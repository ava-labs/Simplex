// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"

	"github.com/stretchr/testify/require"
)

func TestInstance(t *testing.T) {
	// One node is a validator at genesis, the other is a non-validator.
	// After some blocks, the second (non-validator) node also becomes a validator.
	// The test ensures that the second node tracks the chain while the first node expands the chain
	// in the first epoch, and that both nodes move to the second epoch and then both are used for consensus together.
	const (
		basePChainHeight        = uint64(1)
		epochChangePChainHeight = uint64(100)
	)

	var id [20]byte
	rand.Read(id[:])
	firstNodeID := common.NodeID(id[:])

	// The peer that joins the validator set in the last epoch. Its ID is chosen
	// to differ from the (random) node under test.
	var peerID [20]byte
	rand.Read(peerID[:])
	secondNodeID := common.NodeID(peerID[:])

	// Epoch 1 is single-validator
	// The last epoch is expanded to two validators.
	validatorSetsAtHeight := map[uint64]metadata.NodeBLSMappings{
		basePChainHeight: {
			{NodeID: id, BLSKey: []byte{0xaa}, Weight: 1},
		},
		epochChangePChainHeight: {
			{NodeID: id, BLSKey: []byte{0xaa}, Weight: 2},
			{NodeID: peerID, BLSKey: []byte{0xbb}, Weight: 2},
		},
	}

	pChain := newTestPlatformChain(basePChainHeight, validatorSetsAtHeight)
	cops := &testCryptoOps{}

	genesisBlock := &testInnerBlock{Height_: 0, TS: time.Now(), Payload: []byte("genesis")}

	net := newInMemNetwork(t)
	t.Cleanup(net.stop)

	// newInstance builds an Instance sharing the common test dependencies but with
	// its own ID, storage and VM.

	// Create the storage for the instances and append the genesis block to each
	storage := NewMockStorage(t)
	smb := metadata.StateMachineBlock{InnerBlock: genesisBlock}
	require.NoError(t, storage.Index(context.Background(), &ParsedBlock{StateMachineBlock: smb}, common.Finalization{}))

	storage2 := NewMockStorage(t)
	smb = metadata.StateMachineBlock{InnerBlock: genesisBlock}
	require.NoError(t, storage2.Index(context.Background(), &ParsedBlock{StateMachineBlock: smb}, common.Finalization{}))

	// Create the instances and register them to the network
	firstInstance := newInstance(t, firstNodeID, storage, net, pChain, cops, genesisBlock)
	secondInstance := newInstance(t, secondNodeID, storage2, net, pChain, cops, genesisBlock)
	net.register(firstNodeID, firstInstance)
	net.register(secondNodeID, secondInstance)

	/// Start the instances
	require.NoError(t, firstInstance.Start(t.Context()))
	require.NoError(t, secondInstance.Start(t.Context()))
	t.Cleanup(firstInstance.Stop)
	t.Cleanup(secondInstance.Stop)

	// Epoch 1: wait until the node has committed a series of normal blocks on its own.
	const epoch1Target = uint64(5) // genesis(0) + zero block(1) + 3 normal blocks
	waitForNumBlocks(t, storage, epoch1Target)
	waitForNumBlocks(t, storage2, epoch1Target)

	require.GreaterOrEqual(t, storage.NumBlocks(), epoch1Target)
	require.GreaterOrEqual(t, storage2.NumBlocks(), epoch1Target)

	// The validator set in force is the one introduced by the most recent block
	// that carries a BlockValidationDescriptor (the zero block in epoch 1).
	require.Equal(t, firstInstance.Config.ID, latestValidatorID(t, storage))
	require.Equal(t, firstInstance.Config.ID, latestValidatorID(t, storage2))

	// Trigger the epoch change: the validator set changes at epochChangePChainHeight,
	// growing from one validator to two.
	pChain.advanceTo(epochChangePChainHeight)
	approval := &metadata.ValidatorSetApproval{
		NodeID:        peerID,
		PChainHeight:  epochChangePChainHeight,
		AuxInfoDigest: sha256.Sum256(nil),
		Signature:     []byte{1, 2, 3},
	}

	// The node seals the epoch once it has a quorum of approvals of the new
	// (two-validator) set. With two validators the node's self-approval is no longer
	// a quorum and the peer is not running yet, so waitForSealingBlock injects the
	// peer's approval on each poll until the sealing block is committed.
	// TODO: Implement this capability in production so we won't need to inject approvals in tests.
	sealingBlockSeq := waitForSealingBlock(t, firstInstance, approval, storage.NumBlocks())
	waitForNumBlocks(t, storage2, sealingBlockSeq) // Ensure the new validator has replicated the sealing block.

	// With both validators live, the two-validator epoch commits more blocks.
	const epoch2Extra = uint64(3)
	waitForNumBlocks(t, storage, sealingBlockSeq+epoch2Extra)

	// Confirm the second epoch has the second validator in the sealing block
	require.Equal(t, secondInstance.Config.ID, latestValidatorID(t, storage))
}

func newInstance(t *testing.T, nodeID common.NodeID, storage *MockStorage, net *inMemNetwork, pChain *testPlatformChain, cops *testCryptoOps, genesisBlock *testInnerBlock) *Instance {
	comm := &networkSender{net: net, self: nodeID}
	return &Instance{
		Config: Config{
			Logger:                   testutil.MakeLogger(t, int(nodeID[0])),
			ID:                       nodeID,
			VM:                       newTestVM(),
			Storage:                  storage,
			Sender:                   comm,
			Broadcaster:              comm,
			PlatformChain:            pChain,
			CryptoOps:                cops,
			LastNonSimplexInnerBlock: genesisBlock,
			ParameterConfig: ParameterConfig{
				MaxNetworkDelay:  500 * time.Millisecond,
				MaxRoundWindow:   100,
				WALMaxEntryCount: 1024,
			},
		},
	}
}

func latestValidatorID(t *testing.T, storage *MockStorage) common.NodeID {
	t.Helper()
	num := storage.NumBlocks()
	// Iterate backwards and find the latest sealing block (a block with a block validation descriptor)
	for seq := int64(num) - 1; seq >= 0; seq-- {
		block, ok := storage.blockAt(uint64(seq))
		if !ok {
			continue
		}
		bvd := block.Metadata.SimplexEpochInfo.BlockValidationDescriptor
		if bvd != nil {
			validatorCount := len(bvd.AggregatedMembership.Members)
			return bvd.AggregatedMembership.Members[validatorCount-1].NodeID[:]
		}
	}
	t.Fatalf("no block with a BlockValidationDescriptor found in storage")
	return nil
}

// waitForNumBlocks waits until the given storage has at least targetHeight blocks.
func waitForNumBlocks(t *testing.T, storage *MockStorage, targetHeight uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return storage.NumBlocks() >= targetHeight
	}, 20*time.Second, 100*time.Millisecond)
}

// waitForSealingBlock waits until a sealing block (a block carrying a BlockValidationDescriptor with the new weight)
// is committed at or after fromSeq. It periodically injects approvals into the given instance.
// Returns the seq of the sealing block.
func waitForSealingBlock(t *testing.T, inst *Instance, approval *metadata.ValidatorSetApproval, fromSeq uint64) uint64 {
	t.Helper()
	var result uint64
	storage := inst.Config.Storage.(*MockStorage)
	require.Eventually(t, func() bool {
		inst.lock.Lock()
		msm := inst.msm
		inst.lock.Unlock()
		if msm != nil {
			require.NoError(t, msm.HandleApproval(approval, 1))
		}

		num := storage.NumBlocks()
		for seq := fromSeq; seq < num; seq++ {
			block, ok := storage.blockAt(seq)
			if !ok {
				continue
			}
			bvd := block.Metadata.SimplexEpochInfo.BlockValidationDescriptor
			if bvd != nil {
				result = seq
				return true
			}
		}
		return false
	}, 20*time.Second, 100*time.Millisecond)
	return result
}

type testInnerBlock struct {
	Height_ uint64
	TS      time.Time
	Payload []byte
}

func (b *testInnerBlock) Bytes() ([]byte, error) {
	out := make([]byte, 16, 16+len(b.Payload))
	binary.BigEndian.PutUint64(out[0:8], b.Height_)
	binary.BigEndian.PutUint64(out[8:16], uint64(b.TS.UnixMilli()))
	out = append(out, b.Payload...)
	return out, nil
}

func (b *testInnerBlock) Digest() [32]byte {
	bytes, _ := b.Bytes()
	return sha256.Sum256(bytes)
}

func (b *testInnerBlock) Height() uint64                       { return b.Height_ }
func (b *testInnerBlock) Timestamp() time.Time                 { return b.TS }
func (b *testInnerBlock) Verify(context.Context, uint64) error { return nil }

func parseTestInnerBlock(buff []byte) (*testInnerBlock, error) {
	b := &testInnerBlock{}
	b.Height_ = binary.BigEndian.Uint64(buff[0:8])
	b.TS = time.UnixMilli(int64(binary.BigEndian.Uint64(buff[8:16])))
	b.Payload = append([]byte(nil), buff[16:]...)
	return b, nil
}

type testVM struct {
	nextHeight atomic.Uint64
}

func newTestVM() *testVM {
	vm := &testVM{}
	vm.nextHeight.Store(2) // genesis inner block is height 1
	return vm
}

func (vm *testVM) BuildBlock(_ context.Context, _ uint64) (metadata.VMBlock, error) {
	h := vm.nextHeight.Add(1) - 1
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, h)
	return &testInnerBlock{Height_: h, TS: time.Now(), Payload: payload}, nil
}

func (vm *testVM) WaitForPendingBlock(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
	}
}

func (vm *testVM) ParseBlock(_ context.Context, b []byte) (metadata.VMBlock, error) {
	return parseTestInnerBlock(b)
}

func (vm *testVM) ComputeICMEpoch(input metadata.ICMEpochInput) metadata.ICMEpochInfo {
	// ACP-181-style transition (mirrors the msm test helper).
	var zero metadata.ICMEpochInfo
	if input.ParentEpoch == zero {
		return metadata.ICMEpochInfo{
			PChainEpochHeight: input.ParentPChainHeight,
			EpochNumber:       1,
			EpochStartTime:    uint64(input.ParentTimestamp.Unix()),
		}
	}
	endTime := time.Unix(int64(input.ParentEpoch.EpochStartTime), 0).Add(time.Second)
	if input.ParentTimestamp.Before(endTime) {
		return input.ParentEpoch
	}
	return metadata.ICMEpochInfo{
		PChainEpochHeight: input.ParentPChainHeight,
		EpochNumber:       input.ParentEpoch.EpochNumber + 1,
		EpochStartTime:    uint64(input.ParentTimestamp.Unix()),
	}
}

type testPlatformChain struct {
	baseHeight           uint64
	validatorSetAtHeight map[uint64]metadata.NodeBLSMappings // height --> validator set
	lock                 sync.Mutex
	cond                 *sync.Cond
	height               uint64
}

func newTestPlatformChain(baseHeight uint64, validatorSetsAtHeight map[uint64]metadata.NodeBLSMappings) *testPlatformChain {
	pc := &testPlatformChain{
		baseHeight:           baseHeight,
		validatorSetAtHeight: validatorSetsAtHeight,
		height:               baseHeight,
	}
	pc.cond = sync.NewCond(&pc.lock)
	return pc
}

func (pc *testPlatformChain) advanceTo(h uint64) {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	pc.height = h
	pc.cond.Broadcast() // wake any WaitForProgress waiters
}

func (pc *testPlatformChain) currentHeight() uint64 {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	return pc.height
}

func (pc *testPlatformChain) validatorSet(height uint64) metadata.NodeBLSMappings {
	heights := make([]uint64, 0, len(pc.validatorSetAtHeight))
	for h := range pc.validatorSetAtHeight {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

	var lastCheckpoint uint64
	for _, h := range heights {
		if h > height {
			break
		}
		lastCheckpoint = h
	}
	// Return a copy instead of the original slice so the reference won't be used in other goroutines concurrently.
	// Since we allocate a nil slice, a new underlying array is allocated and the copy is safe to use concurrently.
	src := pc.validatorSetAtHeight[lastCheckpoint]
	return append(metadata.NodeBLSMappings(nil), src...)
}

func (pc *testPlatformChain) GetValidatorSet(height uint64) (metadata.NodeBLSMappings, error) {
	return pc.validatorSet(height), nil
}

func (pc *testPlatformChain) GenesisValidatorSet() metadata.NodeBLSMappings {
	return pc.validatorSet(pc.baseHeight)
}

func (pc *testPlatformChain) GetMinimumHeight(context.Context) (uint64, error) {
	return pc.currentHeight(), nil
}

func (pc *testPlatformChain) GetCurrentHeight(context.Context) (uint64, error) {
	return pc.currentHeight(), nil
}

func (pc *testPlatformChain) WaitForProgress(ctx context.Context, pChainHeight uint64) error {
	stop := pc.signalWhenContextFinished(ctx)
	defer stop()

	pc.lock.Lock()
	defer pc.lock.Unlock()
	for pc.height == pChainHeight {
		if err := ctx.Err(); err != nil {
			return err
		}
		pc.cond.Wait()
	}
	return nil
}

func (pc *testPlatformChain) signalWhenContextFinished(ctx context.Context) func() bool {
	stop := context.AfterFunc(ctx, func() {
		pc.lock.Lock()
		defer pc.lock.Unlock()
		pc.cond.Broadcast()
	})
	return stop
}

func (pc *testPlatformChain) LastNonSimplexBlockPChainHeight() uint64 {
	return pc.baseHeight
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

	snapLock sync.Mutex
	blocks   map[uint64]storedBlock
}

type storedBlock struct {
	rawBlock []byte
	fin      common.Finalization
}

func NewMockStorage(t *testing.T) *MockStorage {
	return &MockStorage{
		t:            t,
		InMemStorage: testutil.NewInMemStorage(),
		blocks:       make(map[uint64]storedBlock),
	}
}

func (m *MockStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	// We serialized the block so that the original reference isn't shared with other goroutines that may concurrently mutate it.
	encoded, err := block.Bytes()
	if err != nil {
		return err
	}
	seq := m.InMemStorage.NumBlocks()
	m.snapLock.Lock()
	m.blocks[seq] = storedBlock{rawBlock: encoded, fin: certificate}
	m.snapLock.Unlock()
	return m.InMemStorage.Index(ctx, block, certificate)
}

func (m *MockStorage) GetBlock(seq uint64) (metadata.StateMachineBlock, *common.Finalization, error) {
	_, f, err := m.InMemStorage.Retrieve(seq)
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
	m.snapLock.Lock()
	sb, ok := m.blocks[seq]
	m.snapLock.Unlock()
	if !ok {
		return metadata.StateMachineBlock{}, false
	}
	return m.parseStored(sb.rawBlock), true
}

func (m *MockStorage) parseStored(encoded []byte) metadata.StateMachineBlock {
	raw := &RawBlock{}
	require.NoError(m.t, raw.UnmarshalCanoto(encoded))
	var inner metadata.VMBlock
	if len(raw.InnerBlockBytes) > 0 {
		parsed, err := parseTestInnerBlock(raw.InnerBlockBytes)
		require.NoError(m.t, err)
		inner = parsed
	}
	return metadata.StateMachineBlock{InnerBlock: inner, Metadata: raw.Metadata}
}

func (m *MockStorage) CreateWAL() (wal.DeletableWAL, error) {
	return testutil.NewTestWAL(m.t), nil
}

// ---------------------------------------------------------------------------
// inMemNetwork: Routes messages between Instances.
// Delivery happens on a per-node goroutine rather than inline in Send,
// due to locking.
// ---------------------------------------------------------------------------

type netMsg struct {
	from common.NodeID
	msg  *common.Message
}

type netNode struct {
	inst *Instance
	// in is a buffered inbox drained by the delivery goroutine. The channel itself
	// signals that work is available, so no separate wake signal is needed. Sends
	// never block (see enqueue); on the rare chance the buffer fills, a dropped
	// message costs at most an empty round the epoch recovers from.
	in      chan netMsg
	done    chan struct{}
	stopped chan struct{}
}

type inMemNetwork struct {
	t     *testing.T
	lock  sync.Mutex
	nodes map[string]*netNode
}

func newInMemNetwork(t *testing.T) *inMemNetwork {
	return &inMemNetwork{t: t, nodes: make(map[string]*netNode)}
}

// register wires inst into the network and starts delivering messages to it.
// Messages that arrive before the epoch exists are dropped by the instance's
// nil-epoch guard, which at worst costs a few empty rounds the epoch recovers from.
func (n *inMemNetwork) register(id common.NodeID, inst *Instance) {
	node := &netNode{
		inst:    inst,
		in:      make(chan netMsg, 1024),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	n.lock.Lock()
	n.nodes[string(id)] = node
	n.lock.Unlock()
	go n.deliver(node)
}

func (n *inMemNetwork) stop() {
	n.lock.Lock()
	nodes := make([]*netNode, 0, len(n.nodes))
	for _, node := range n.nodes {
		nodes = append(nodes, node)
	}
	n.nodes = make(map[string]*netNode)
	n.lock.Unlock()
	for _, node := range nodes {
		close(node.done)
		<-node.stopped
	}
}

func (n *inMemNetwork) enqueue(dest common.NodeID, m netMsg) {
	n.lock.Lock()
	node := n.nodes[string(dest)]
	n.lock.Unlock()
	if node == nil {
		// Destination not registered; drop. This only happens before an instance
		// is registered, never mid-run.
		return
	}
	select {
	case node.in <- m:
	default:
		// Never block the sender (Send runs under the epoch lock). A dropped message
		// costs at most an empty round the epoch recovers from.
	}
}

func (n *inMemNetwork) deliver(node *netNode) {
	defer close(node.stopped)
	for {
		select {
		case <-node.done:
			return
		case m := <-node.in:
			n.dispatch(node.inst, m)
		}
	}
}

func (n *inMemNetwork) dispatch(inst *Instance, m netMsg) {
	if err := inst.HandleMessage(m.msg, m.from); err != nil {
		n.t.Logf("HandleMessage from %x failed: %v", m.from, err)
	}
}

// toRawBlock re-encodes a verified block into the wire RawBlock the receiving
// instance parses in HandleBlockMessage.
func toRawBlock(t *testing.T, vb common.VerifiedBlock) *RawBlock {
	bytes, err := vb.Bytes()
	require.NoError(t, err)
	raw := &RawBlock{}
	require.NoError(t, raw.UnmarshalCanoto(bytes))
	return raw
}

// reparseBlock reconstructs an independent *ParsedBlock from a verified block's
// wire bytes. Each call yields a fresh object sharing no pointers with the
// sender's live block, so that remaining references to the sender's block don't race with the receiver.
func reparseBlock(t *testing.T, vb common.VerifiedBlock) *ParsedBlock {
	raw := toRawBlock(t, vb)
	var inner metadata.VMBlock
	if len(raw.InnerBlockBytes) > 0 {
		parsed, err := parseTestInnerBlock(raw.InnerBlockBytes)
		require.NoError(t, err)
		inner = parsed
	}
	return &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{InnerBlock: inner, Metadata: raw.Metadata},
	}
}

type networkSender struct {
	net  *inMemNetwork
	self common.NodeID
}

func (s *networkSender) Broadcast(msg *common.Message) {
	for _, n := range s.net.nodes {
		s.Send(msg, n.inst.Config.ID)
	}
}

func (s *networkSender) Send(msg *common.Message, dest common.NodeID) {
	if bytes.Equal(s.self, dest) {
		// Do not send to myself
		return
	}
	m := s.createIngressMessage(msg)
	s.net.enqueue(dest, m)
}

// CreateIngressMessage translates a message into the form the receiving instance expects on the wire.
// For example, a VerifiedBlockMessage is re-encoded as a BlockMessage with a RawBlock.
// A VerifiedReplicationResponse is re-encoded as a ReplicationResponse with independent copies of the carried blocks.
func (s *networkSender) createIngressMessage(msg *common.Message) netMsg {
	m := netMsg{from: s.self}
	switch {
	case msg.VerifiedBlockMessage != nil:
		m.msg = &common.Message{
			BlockMessage: &common.BlockMessage{
				Vote:  msg.VerifiedBlockMessage.Vote,
				Block: reparseBlock(s.net.t, msg.VerifiedBlockMessage.VerifiedBlock),
			},
		}
	case msg.VerifiedReplicationResponse != nil:
		m.msg = &common.Message{ReplicationResponse: toReplicationResponse(s.net.t, msg.VerifiedReplicationResponse)}
	default:
		m.msg = msg
	}
	return m
}

// toReplicationResponse translates a VerifiedReplicationResponse (the sender's
// internal form) into the ReplicationResponse a receiver handles on the wire,
// mirroring testutil.TestComm. Each carried block is reconstructed as an
// independent copy so the delivery goroutine never touches the sender's live
// block object (whose canoto digest cache the sender keeps mutating).
func toReplicationResponse(t *testing.T, vrr *common.VerifiedReplicationResponse) *common.ReplicationResponse {
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
	return resp
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
