// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testInnerBlock struct {
	Height_ uint64
	TS      time.Time
	Payload []byte
}

func (b *testInnerBlock) Bytes() []byte {
	out := make([]byte, 16, 16+len(b.Payload))
	binary.BigEndian.PutUint64(out[0:8], b.Height_)
	binary.BigEndian.PutUint64(out[8:16], uint64(b.TS.UnixMilli()))
	out = append(out, b.Payload...)
	return out
}

func (b *testInnerBlock) Digest() [32]byte {
	bytes := b.Bytes()
	return sha256.Sum256(bytes)
}

func (b *testInnerBlock) Height() uint64                       { return b.Height_ }
func (b *testInnerBlock) Timestamp() time.Time                 { return b.TS }
func (b *testInnerBlock) Verify(context.Context, uint64) error { return nil }

type testInnerBlockDeserializer struct{}

func (ibd *testInnerBlockDeserializer) ParseBlock(_ context.Context, buff []byte) (avalanchego.VMBlock, error) {
	b := &testInnerBlock{}
	b.Height_ = binary.BigEndian.Uint64(buff[0:8])
	b.TS = time.UnixMilli(int64(binary.BigEndian.Uint64(buff[8:16])))
	b.Payload = append([]byte(nil), buff[16:]...)
	return b, nil
}

var (
	genesisPChainHeight uint64 = 0
	genesisBlock               = &testInnerBlock{Height_: genesisPChainHeight, TS: time.Now(), Payload: []byte("genesis")}
)

// epochBlockTime fixes the timestamp of the epoch-defining block
// this ensures a consistent block digest
var epochBlockTime = genesisBlock.TS.Add(time.Millisecond)
var paramConfig = ParameterConfig{
	MaxNetworkDelay: 200 * time.Millisecond,
	MaxRoundWindow:  100,
	WALMaxSizeBytes: 1024,
}

type testPlatformChain struct {
	genesisHeight uint64 // genesis height is the height of the pchain the genesis validator set lives

	// lock guards the sets, which the running instances read while a test installs new ones.
	lock sync.Mutex
	// validatorSetAtHeight maps a P-chain height to the validator set in force from it on.
	validatorSetAtHeight map[uint64]metadata.NodeBLSMappings

	height uint64
	// heightChanged is closed and replaced on every advanceHeight, waking waiters
	// so they re-check the height.
	heightChanged chan struct{}
}

// newTestPChain returns a P-chain holding only the genesis validator set, in force from
// genesisPChainHeight on.
func newTestPChain(genesisSet metadata.NodeBLSMappings) *testPlatformChain {
	return &testPlatformChain{
		genesisHeight: genesisPChainHeight,
		validatorSetAtHeight: map[uint64]metadata.NodeBLSMappings{
			genesisPChainHeight: genesisSet,
		},
		height:        genesisPChainHeight,
		heightChanged: make(chan struct{}),
	}
}

func (pc *testPlatformChain) currentHeight() uint64 {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	return pc.height
}

// GetValidatorSet grabs the validator set at `height`. If one does not directly exist at that height,
// it returns validator set at the first height less than `height`.
func (pc *testPlatformChain) GetValidatorSet(height uint64) (metadata.NodeBLSMappings, error) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	set, ok := pc.validatorSetAtHeight[height]
	if ok {
		return set, nil
	}

	var nextLargestHeight uint64
	for h := range pc.validatorSetAtHeight {
		if h >= height {
			continue
		}

		if h > nextLargestHeight {
			nextLargestHeight = h
		}
	}

	set, ok = pc.validatorSetAtHeight[nextLargestHeight]
	if !ok {
		return nil, fmt.Errorf("no validator set at %d", height)
	}

	return set, nil
}

func (pc *testPlatformChain) GenesisValidatorSet() metadata.NodeBLSMappings {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	return pc.validatorSetAtHeight[pc.genesisHeight]
}

func (pc *testPlatformChain) GetMinimumHeight() uint64 {
	return pc.currentHeight()
}

func (pc *testPlatformChain) GetCurrentHeight() uint64 {
	return pc.currentHeight()
}

// WaitForProgress blocks until the context is cancelled or the P-chain height
// has increased past pChainHeight.
func (pc *testPlatformChain) WaitForProgress(ctx context.Context, pChainHeight uint64) error {
	for {
		pc.lock.Lock()
		if pc.height > pChainHeight {
			pc.lock.Unlock()
			return nil
		}
		ch := pc.heightChanged
		pc.lock.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (pc *testPlatformChain) setValidatorSetAt(height uint64, validatorSet metadata.NodeBLSMappings) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	pc.validatorSetAtHeight[height] = validatorSet
}

// advanceHeight bumps the P-chain height and wakes every WaitForProgress waiter.
func (pc *testPlatformChain) advanceHeight(height uint64) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	if height <= pc.height {
		panic("smaller height")
	}
	pc.height = height
	close(pc.heightChanged)
	pc.heightChanged = make(chan struct{})
}

func (pc *testPlatformChain) LastNonSimplexBlockPChainHeight() uint64 {
	return pc.genesisHeight
}

type testCryptoOps struct{}

func (c *testCryptoOps) Sign(message []byte) ([]byte, error) {
	// A deterministic, non-empty placeholder signature.
	d := sha256.Sum256(message)
	return d[:], nil
}

func (c *testCryptoOps) AggregateKeys(keys ...[]byte) ([]byte, error) {
	var out []byte
	for _, k := range keys {
		out = append(out, k...)
	}
	return out, nil
}

func (c *testCryptoOps) VerifySignature(_ []byte, _ []byte, _ []byte) error {
	return nil
}

func (c *testCryptoOps) CreateSignatureAggregator(nodes []common.Node) common.SignatureAggregator {
	return &testutil.TestSignatureAggregator{N: len(nodes)}
}

func (c *testCryptoOps) DeserializeQuorumCertificate(bytes []byte) (common.QuorumCertificate, error) {
	var qc []common.Signature
	if _, err := asn1.Unmarshal(bytes, &qc); err != nil {
		return nil, err
	}
	return testutil.TestQC(qc), nil
}

type MockStorage struct {
	t *testing.T
	*testutil.InMemStorage
	bd *testInnerBlockDeserializer

	blocksLock sync.Mutex
	blocks     map[uint64]storedBlock
}

type storedBlock struct {
	rawBlock []byte
	fin      common.Finalization
}

func NewMockStorage(t *testing.T, bd *testInnerBlockDeserializer) *MockStorage {
	return &MockStorage{
		t:            t,
		InMemStorage: testutil.NewInMemStorage(),
		blocks:       make(map[uint64]storedBlock),
		bd:           bd,
	}
}

func NewMockStorageWithGenesis(t *testing.T, bd *testInnerBlockDeserializer) *MockStorage {
	s := &MockStorage{
		t:            t,
		InMemStorage: testutil.NewInMemStorage(),
		blocks:       make(map[uint64]storedBlock),
		bd:           bd,
	}

	genesis := &ParsedBlock{StateMachineBlock: metadata.StateMachineBlock{InnerBlock: genesisBlock}}
	require.NoError(t, s.Index(context.Background(), genesis, common.Finalization{}))
	return s
}

func (m *MockStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	// We serialized the block so that the original reference isn't shared with other goroutines that may concurrently mutate it.
	encoded := block.Bytes()
	seq := m.NumBlocks()
	m.blocksLock.Lock()
	m.blocks[seq] = storedBlock{rawBlock: encoded, fin: certificate}
	m.blocksLock.Unlock()
	return m.InMemStorage.Index(ctx, block, certificate)
}

func (m *MockStorage) GetBlock(seq uint64) (metadata.StateMachineBlock, *common.Finalization, error) {
	_, f, err := m.Retrieve(seq)
	if err != nil {
		return metadata.StateMachineBlock{}, nil, err
	}
	sb, ok := m.blockAt(seq)
	if !ok {
		return metadata.StateMachineBlock{}, nil, fmt.Errorf("no snapshot for seq %d", seq)
	}
	return sb, &f, nil
}

// blockAt reconstructs an independent copy of the block at seq from its
// stored bytes. Test-only readers use it instead of GetBlock so they never touch
// the instance's live block objects (whose canoto digest cache the instance keeps
// mutating).
func (m *MockStorage) blockAt(seq uint64) (metadata.StateMachineBlock, bool) {
	m.blocksLock.Lock()
	sb, ok := m.blocks[seq]
	m.blocksLock.Unlock()
	if !ok {
		return metadata.StateMachineBlock{}, false
	}
	return m.parseStored(sb.rawBlock), true
}

func (m *MockStorage) parseStored(encoded []byte) metadata.StateMachineBlock {
	raw := &metadata.RawBlock{}
	require.NoError(m.t, raw.UnmarshalCanoto(encoded))
	var inner avalanchego.VMBlock
	if len(raw.InnerBlockBytes) > 0 {
		parsed, err := m.bd.ParseBlock(context.Background(), raw.InnerBlockBytes)
		require.NoError(m.t, err)
		inner = parsed
	}
	return metadata.StateMachineBlock{InnerBlock: inner, Metadata: raw.Metadata}
}

// newChainStorage builds and indexes the minimum chain a node can start from: genesis plus
// epoch 1's defining block, which carries the descriptor naming the epoch's validator set.
// It returns the storage and the epoch-defining block at its tip.
func newChainStorage(t *testing.T, validators metadata.NodeBLSMappings) (*MockStorage, metadata.StateMachineBlock) {
	storage := NewMockStorageWithGenesis(t, &testInnerBlockDeserializer{})
	genesis, ok := storage.blockAt(0)
	require.True(t, ok)

	epochBlock := metadata.StateMachineBlock{
		InnerBlock: &testInnerBlock{Height_: 1, TS: epochBlockTime, Payload: []byte("epoch")},
		Metadata: metadata.StateMachineMetadata{
			Timestamp:               uint64(epochBlockTime.UnixMilli()),
			SimplexProtocolMetadata: common.ProtocolMetadata{Epoch: 1, Round: 1, Seq: 1, Prev: common.Digest(genesis.Digest())},
			SimplexEpochInfo: metadata.SimplexEpochInfo{
				EpochNumber: 1,
				BlockValidationDescriptor: &metadata.BlockValidationDescriptor{
					AggregatedMembership: metadata.AggregatedMembership{Members: validators},
				},
			},
		},
	}

	block := &ParsedBlock{StateMachineBlock: epochBlock.Clone()}
	finalization, _ := testutil.NewFinalizationRecord(t, &testutil.TestSignatureAggregator{N: len(validators)}, block, validators.NodeIDs())
	require.NoError(t, storage.Index(context.Background(), block, finalization))
	return storage, epochBlock
}

type walCreator struct {
	t *testing.T

	lock sync.Mutex
	wals []*testutil.TestWAL
}

func (w *walCreator) createWAL() (wal.DeletableWAL, error) {
	tw := testutil.NewTestWAL(w.t)
	w.lock.Lock()
	w.wals = append(w.wals, tw)
	w.lock.Unlock()
	return tw, nil
}

// containsNotarization reports whether any WAL this creator handed out holds a
// notarization for the given round.
func (w *walCreator) containsNotarization(round uint64) bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	for _, tw := range w.wals {
		if tw.ContainsNotarization(round) {
			return true
		}
	}
	return false
}

// reparseBlock rebuilds an independent ParsedBlock from a verified block's wire bytes.
// The zero block has no inner block, so its inner bytes stay empty.
func reparseBlock(t *testing.T, vb common.VerifiedBlock) *ParsedBlock {
	var rawBlock metadata.RawBlock
	require.NoError(t, rawBlock.UnmarshalCanoto(vb.Bytes()))

	var inner avalanchego.VMBlock
	if len(rawBlock.InnerBlockBytes) > 0 {
		bd := &testInnerBlockDeserializer{}
		parsed, err := bd.ParseBlock(context.Background(), rawBlock.InnerBlockBytes)
		require.NoError(t, err)
		inner = parsed
	}
	return &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{InnerBlock: inner, Metadata: rawBlock.Metadata},
	}
}

func verifiedQuorumRoundToQuorumRound(t *testing.T, vqr common.VerifiedQuorumRound) common.QuorumRound {
	qr := common.QuorumRound{
		Notarization:      vqr.Notarization,
		Finalization:      vqr.Finalization,
		EmptyNotarization: vqr.EmptyNotarization,
	}
	if vqr.VerifiedBlock != nil {
		qr.Block = reparseBlock(t, vqr.VerifiedBlock)
	}
	return qr
}

type instanceComm struct {
	c *network
	// id is the node this comm belongs to, reported as the sender of every message it sends.
	id common.NodeID
}

func newInstanceComm(c *network, id common.NodeID) *instanceComm {
	return &instanceComm{c: c, id: id}
}

func (c *instanceComm) Send(msg *common.Message, destination common.NodeID) {
	// loop through all nodes in chain directly send to handle message directly via but do it in a separate go routine
	for _, n := range c.c.nodesSnapshot() {
		if !bytes.Equal(n.id, destination) {
			continue
		}

		go func(dst *Instance) {
			require.NotNil(c.c.t, dst, "node %x was sent a message before it was created", destination)
			require.NoError(c.c.t, dst.HandleMessage(translateOutgoingToIncomingMessage(c.c.t, msg), c.id))
		}(n.inst)
		return
	}
}

func (c *instanceComm) Broadcast(msg *common.Message) {
	// send to every node in the chain but ourselves, each on its own go routine
	for _, n := range c.c.nodesSnapshot() {
		if bytes.Equal(n.id, c.id) {
			continue
		}

		go func(dst *Instance) {
			require.NotNil(c.c.t, dst, "node %x was sent a message before it was created", n.id)
			require.NoError(c.c.t, dst.HandleMessage(translateOutgoingToIncomingMessage(c.c.t, msg), c.id))
		}(n.inst)
	}
}

// translateOutgoingToIncomingMessage converts the verified message types an instance
// sends into the wire types a receiver handles, like testutil.TestComm. Each carried
// block is re-parsed into a fresh ParsedBlock because HandleMessage mutates the block
// it receives, so recipients cannot share the sender's live block.
func translateOutgoingToIncomingMessage(t *testing.T, msg *common.Message) *common.Message {
	switch {
	case msg.VerifiedBlockMessage != nil:
		return &common.Message{
			BlockMessage: &common.BlockMessage{
				Vote:  msg.VerifiedBlockMessage.Vote,
				Block: reparseBlock(t, msg.VerifiedBlockMessage.VerifiedBlock),
			},
		}
	case msg.VerifiedReplicationResponse != nil:
		vrr := msg.VerifiedReplicationResponse
		data := make([]common.QuorumRound, 0, len(vrr.Data))
		for _, vqr := range vrr.Data {
			data = append(data, verifiedQuorumRoundToQuorumRound(t, vqr))
		}
		resp := &common.ReplicationResponse{Data: data}
		if vrr.LatestRound != nil {
			qr := verifiedQuorumRoundToQuorumRound(t, *vrr.LatestRound)
			resp.LatestRound = &qr
		}
		if vrr.LatestFinalizedSeq != nil {
			qr := verifiedQuorumRoundToQuorumRound(t, *vrr.LatestFinalizedSeq)
			resp.LatestSeq = &qr
		}
		return &common.Message{ReplicationResponse: resp}
	default:
		return msg
	}
}

// pendingBlockSignal is the network's shared mempool. It stays pending until a block builder consumes it,
// so an offline leader only delays the block: rounds are empty notarized until an online leader claims it.
type pendingBlockSignal struct {
	lock    sync.Mutex
	pending bool
	ch      chan struct{}
}

func newPendingBlockSignal() *pendingBlockSignal {
	return &pendingBlockSignal{ch: make(chan struct{})}
}

// pendingBlockSignal is the network's shared mempool. It stays pending until a block builder consumes it,
// so calling addPendingBlock will always produce a block.
// If the current leader is offline, rounds are empty notarized until an online leader claims it.
func (s *pendingBlockSignal) addPendingBlock() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.pending = true
	close(s.ch)
	s.ch = make(chan struct{})
}

// wait returns once a block is pending or ctx is cancelled. It does not
// consume the block for block building.
func (s *pendingBlockSignal) wait(ctx context.Context) {
	for {
		s.lock.Lock()
		pending, ch := s.pending, s.ch
		s.lock.Unlock()

		if pending {
			return
		}

		select {
		case <-ch:
		case <-ctx.Done():
			return
		}
	}
}

// consume waits for a pending block and claims it, reporting false if ctx is cancelled first.
// called from the block builder.
func (s *pendingBlockSignal) consume(ctx context.Context) bool {
	for {
		s.lock.Lock()
		ch := s.ch
		if s.pending {
			s.pending = false
			s.lock.Unlock()
			return true
		}
		s.lock.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return false
		}
	}
}

// blockBuilderVM builds an inner block only when the test arms a pending block, so the
// chain grows one block per index call.
type blockBuilderVM struct {
	storage *MockStorage
	pending *pendingBlockSignal
}

func newBlockBuilderVM(storage *MockStorage, pending *pendingBlockSignal) *blockBuilderVM {
	return &blockBuilderVM{storage: storage, pending: pending}
}

func (vm *blockBuilderVM) BuildBlock(ctx context.Context, pChainHeight uint64) (avalanchego.VMBlock, error) {
	// Claiming the pending block is what gates block building.
	if !vm.pending.consume(ctx) {
		return nil, ctx.Err()
	}

	// the inner height is the seq of the block being built, which is how many blocks the node
	// has committed so far
	height := vm.storage.NumBlocks()
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, height)
	return &testInnerBlock{Height_: height, TS: time.Now(), Payload: payload}, nil
}

func (vm *blockBuilderVM) ParseBlock(ctx context.Context, bytes []byte) (avalanchego.VMBlock, error) {
	return vm.storage.bd.ParseBlock(ctx, bytes)
}

// WaitForPendingBlock returns while a block is pending, or when ctx is cancelled.
func (vm *blockBuilderVM) WaitForPendingBlock(ctx context.Context) {
	vm.pending.wait(ctx)
}

// noopICMTransition keeps every block in the same ICM epoch, so an epoch only ever changes
// because the validator set did.
func noopICMTransition(_ metadata.ICMEpochInput) metadata.ICMEpochInfo {
	return metadata.ICMEpochInfo{}
}

type node struct {
	t       *testing.T
	id      common.NodeID
	vm      *blockBuilderVM
	inst    *Instance
	storage *MockStorage
	comm    *instanceComm
	wals    *walCreator
}

// role reports whether the node is running a validator rather than a non-validator.
func (n *node) role() (isValidator bool) {
	n.inst.lock.Lock()
	defer n.inst.lock.Unlock()

	return n.inst.e != nil
}

type network struct {
	t *testing.T

	pChain *testPlatformChain
	seq    uint64
	epoch  uint64

	// pending holds the block the network has been asked to build, claimable by any leader.
	pending *pendingBlockSignal

	validatorSets map[uint64]common.Nodes // epoch -> sorted validators

	// lock guards nodes, which comm goroutines read while addNode appends.
	lock  sync.Mutex
	nodes []node
}

func (n *network) nodesSnapshot() []node {
	n.lock.Lock()
	defer n.lock.Unlock()
	return append([]node(nil), n.nodes...)
}

func newNetwork(t *testing.T, pChain *testPlatformChain) *network {
	validatorSets := make(map[uint64]common.Nodes)
	genesisNodes := pChain.GenesisValidatorSet().Nodes()
	common.SortNodes(genesisNodes)
	validatorSets[1] = genesisNodes

	return &network{
		t:             t,
		pChain:        pChain,
		pending:       newPendingBlockSignal(),
		validatorSets: validatorSets,

		// Genesis at seq 0. Then first simplex block is built automatically
		// without a build block notification
		seq:   2,
		epoch: 1,
	}
}

// nodeConfig holds optional overrides for a node added to the network.
type nodeConfig struct {
	// storage the node starts from; defaults to a fresh storage holding only genesis.
	storage *MockStorage
	// wals are pre-existing WALs the instance restores on start.
	wals []wal.DeletableWAL
}

// addNode adds a node to the network and blocks until it catches up with the latest tip
func (n *network) addNode(id common.NodeID) *node {
	node := n.addNodeWithConfig(id, nodeConfig{})
	node.storage.WaitForBlockCommit(n.seq - 1)
	return node
}

// addNodeWithStorage adds a node that starts from the given storage.
func (n *network) addNodeWithStorage(id common.NodeID, storage *MockStorage) *node {
	return n.addNodeWithConfig(id, nodeConfig{storage: storage})
}

// addNodeWithConfig adds a node built from the given config.
func (n *network) addNodeWithConfig(id common.NodeID, cfg nodeConfig) *node {
	storage := cfg.storage
	if storage == nil {
		storage = NewMockStorageWithGenesis(n.t, &testInnerBlockDeserializer{})
	}

	// ensure a unique id; snapshot because nodes may be added concurrently
	for _, node := range n.nodesSnapshot() {
		require.NotEqual(n.t, node.id, id)
	}

	comm := newInstanceComm(n, id)

	vm := newBlockBuilderVM(storage, n.pending)
	wc := &walCreator{t: n.t}
	instance := NewInstance(Config{
		LastNonSimplexInnerBlock: genesisBlock,
		ParameterConfig:          paramConfig,
		PlatformChain:            n.pChain,
		Broadcaster:              comm,
		Sender:                   comm,
		CryptoOps:                &testCryptoOps{},
		WalCreator:               wc.createWAL,
		Storage:                  storage,
		// the first byte of the node id labels the node's log records
		Logger:         testutil.MakeLogger(n.t, int(id[0])),
		WALs:           cfg.wals,
		VM:             vm,
		ICMETransition: noopICMTransition,
		ID:             id,
	})

	node := node{
		t:       n.t,
		id:      id,
		storage: storage,
		comm:    comm,
		vm:      vm,
		inst:    instance,
		wals:    wc,
	}

	n.lock.Lock()
	n.nodes = append(n.nodes, node)
	n.lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	n.t.Cleanup(cancel)

	require.NoError(n.t, node.inst.Start(ctx))
	n.t.Cleanup(node.inst.Stop)

	instance.Config.Logger.Debug("Added a node to the test network", zap.Uint64("Seq", n.seq), zap.Uint64("num block", node.storage.NumBlocks()))
	return &node
}

// waitUntilValidatorsRunning blocks until every node that should be validator in the current epoch
// is actually a validator. This ensures that acceptNewBlock, waits for any nodes that may be transitioning from
// a non-validator actually here about the new proposal.
func (n *network) waitUntilValidatorsReady() {
	validators, ok := n.validatorSets[n.epoch]
	require.True(n.t, ok, fmt.Sprintf("epoch is not set. epoch: %d", n.epoch))

	for _, node := range n.nodesSnapshot() {
		if common.NodeIDs(validators.NodeIDs()).IndexOf(node.id) < 0 {
			continue
		}

		require.Eventually(n.t, node.role, time.Minute, time.Millisecond,
			"node %x never started running a validator", node.id)
	}
}

// acceptNewBlock blocks until every node has accepted a newly indexed block.
func (n *network) acceptNewBlock() (common.VerifiedBlock, common.Finalization) {
	_, ok := n.validatorSets[n.epoch]
	require.True(n.t, ok, fmt.Sprintf("epoch is not set epoch: %d. trying to index seq: %d", n.epoch, n.seq))

	// no nodes have indexed this sequence yet
	for _, node := range n.nodes {
		_, _, err := node.storage.Retrieve(n.seq)
		require.ErrorIs(n.t, err, common.ErrBlockNotFound)
	}

	n.waitUntilValidatorsReady()

	// Adds a pending block to the network, to be built by the next online node.
	n.pending.addPendingBlock()

	var block common.VerifiedBlock
	var finalization common.Finalization
	for _, node := range n.nodes {
		committedBlock := node.storage.WaitForBlockCommit(n.seq)
		if block == nil {
			_, fin, err := node.storage.Retrieve(n.seq)
			require.NoError(n.t, err)
			finalization = fin
			block = committedBlock
		} else {
			require.Equal(n.t, block.Bytes(), committedBlock.Bytes())
		}
	}

	require.Equal(n.t, block.BlockHeader().Seq, n.seq)
	n.seq++

	// check if its a sealing
	if block.SealingBlockInfo() != nil {
		n.epoch = n.seq
		newValidatorSet := block.SealingBlockInfo().ValidatorSet
		common.SortNodes(newValidatorSet)
		n.validatorSets[n.epoch] = newValidatorSet
	}

	return block, finalization
}

// waitUntilSealingBlock waits until every node commits the block at the current seq,
// repeating until that block is a sealing block. It then advances the network into
// the new epoch and returns the sealing block.
// This is useful for when we are transitioning epochs because blocks will be built impatiently
// without a notification from the mempool.
func (n *network) waitUntilSealingBlock() common.VerifiedBlock {
	for {
		var block common.VerifiedBlock
		for _, node := range n.nodes {
			committedBlock := node.storage.WaitForBlockCommit(n.seq)
			if block == nil {
				block = committedBlock
			} else {
				require.Equal(n.t, block.Bytes(), committedBlock.Bytes())
			}
		}

		require.Equal(n.t, block.BlockHeader().Seq, n.seq)
		n.seq++

		if block.SealingBlockInfo() == nil {
			continue
		}

		// add the validator set to the networks memory for block building
		n.epoch = n.seq
		newValidatorSet := block.SealingBlockInfo().ValidatorSet
		common.SortNodes(newValidatorSet)
		n.validatorSets[n.epoch] = newValidatorSet
		return block
	}
}

// newBLSMapping creates a mapping with a nodeID, BLSKey and Weight with a given [id].
// id is passed as an int for consistent logs between runs.
func newBLSMapping(id int) metadata.NodeBLSMapping {
	avaID := [20]byte{byte(id)}

	return metadata.NodeBLSMapping{
		NodeID: avalanchego.NodeID(avaID),
		BLSKey: []byte{avaID[0], byte(id + 1)},
		Weight: 1,
	}
}

// assertExpectedNodeIds asserts the validator set contains exactly the expected node IDs.
func assertExpectedNodeIds(t *testing.T, validatorSet []common.NodeID, expected []common.NodeID) {
	require.ElementsMatch(t, expected, validatorSet)
}
