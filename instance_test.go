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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// requireTipIsSealing asserts whether the last block in storage is a sealing block.
func requireTipIsSealing(t *testing.T, storage *MockStorage, want bool) {
	t.Helper()
	num := storage.NumBlocks()
	require.Positive(t, num)
	block, ok := storage.blockAt(num - 1)
	require.True(t, ok)
	require.Equal(t, want, block.SealingBlockInfo() != nil)
}

// countSealingBlocks returns the number of sealing blocks (blocks carrying a
// BlockValidationDescriptor) currently in storage.
func countSealingBlocks(t *testing.T, storage *MockStorage) int {
	t.Helper()
	count := 0
	num := storage.NumBlocks()
	for seq := uint64(0); seq < num; seq++ {
		block, ok := storage.blockAt(seq)
		if !ok {
			continue
		}
		if block.SealingBlockInfo() != nil {
			count++
		}
	}
	return count
}

// waitForSealingBlockCount waits until storage holds at least target sealing blocks.
func waitForSealingBlockCount(t *testing.T, storage *MockStorage, target int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return countSealingBlocks(t, storage) >= target
	}, 20*time.Second, 100*time.Millisecond)
}

// newStorageWithGenesis returns storage holding only the genesis block, the ledger every node
// here starts from.
func newStorageWithGenesis(t *testing.T, genesisBlock *testInnerBlock) *MockStorage {
	t.Helper()
	storage := NewMockStorage(t)
	genesis := &ParsedBlock{StateMachineBlock: metadata.StateMachineBlock{InnerBlock: genesisBlock}}
	require.NoError(t, storage.Index(context.Background(), genesis, common.Finalization{}))
	return storage
}

// newInstance builds an Instance sharing the common test dependencies but with its own ID,
// storage and VM.
func newInstance(t *testing.T, nodeID common.NodeID, storage *MockStorage, net *inMemNetwork, pChain *testPlatformChain, cops *testCryptoOps, genesisBlock *testInnerBlock) *Instance {
	return newInstanceWithVM(t, nodeID, storage, net, pChain, cops, genesisBlock, newTestVM())
}

// newInstanceWithVM is like newInstance but uses a caller-supplied VM, so a test
// can share one controllable VM across restarts of the same node.
func newInstanceWithVM(t *testing.T, nodeID common.NodeID, storage *MockStorage, net *inMemNetwork, pChain *testPlatformChain, cops *testCryptoOps, genesisBlock *testInnerBlock, vm *testVM) *Instance {
	comm := &networkSender{net: net, self: nodeID}
	config := Config{
		Logger:                   testutil.MakeLogger(t, int(nodeID[0])),
		ID:                       nodeID,
		VM:                       vm,
		Storage:                  storage,
		ICMETransition:           vm.ComputeICMEpoch,
		Sender:                   comm,
		Broadcaster:              comm,
		PlatformChain:            pChain,
		CryptoOps:                cops,
		LastNonSimplexInnerBlock: genesisBlock,
		WalCreator:               storage.CreateWAL,
		ParameterConfig: ParameterConfig{
			MaxNetworkDelay: 500 * time.Millisecond,
			MaxRoundWindow:  100,
			WALMaxSizeBytes: 1024,
		},
	}
	return NewInstance(config)
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
		if info := block.SealingBlockInfo(); info != nil {
			return info.ValidatorSet[len(info.ValidatorSet)-1].Id
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
	}, 20*time.Second, 100*time.Millisecond, "storage did not commit %d blocks in time", targetHeight)
}

// waitForSealingBlock waits until a sealing block (a block carrying a BlockValidationDescriptor with the new weight)
// is committed at or after fromSeq. It periodically injects approvals into the given instance.
// Returns the seq of the sealing block.
func waitForSealingBlock(t *testing.T, inst *Instance, approval *common.ValidatorSetApproval, fromSeq uint64) uint64 {
	t.Helper()
	var result uint64
	storage := inst.Config.Storage.(*MockStorage)
	require.Eventually(t, func() bool {
		inst.lock.Lock()
		msm := inst.msm
		inst.lock.Unlock()
		if msm != nil {
			msm.HandleApproval(approval, 1)
		}

		num := storage.NumBlocks()
		for seq := fromSeq; seq < num; seq++ {
			block, ok := storage.blockAt(seq)
			if !ok {
				continue
			}
			if block.SealingBlockInfo() != nil {
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

func parseTestInnerBlock(buff []byte) (*testInnerBlock, error) {
	b := &testInnerBlock{}
	b.Height_ = binary.BigEndian.Uint64(buff[0:8])
	b.TS = time.UnixMilli(int64(binary.BigEndian.Uint64(buff[8:16])))
	b.Payload = append([]byte(nil), buff[16:]...)
	return b, nil
}

type testVM struct {
	nextHeight atomic.Uint64
	// When paused, the VM behaves as a chain with no pending transactions:
	// WaitForPendingBlock and BuildBlock block until their context expires, so the
	// epoch stops producing ordinary blocks. The epoch-transition and sealing
	// machinery, which builds its block once the inner build times out, still runs —
	// so pausing before an epoch change leaves the sealing block at the tip with
	// nothing built on top. Lets a test pin the chain tip without touching storage.
	paused atomic.Bool
}

func newTestVM() *testVM {
	vm := &testVM{}
	vm.nextHeight.Store(1) // the genesis inner block is height 0
	return vm
}

func (vm *testVM) pause()  { vm.paused.Store(true) }
func (vm *testVM) resume() { vm.paused.Store(false) }

func (vm *testVM) BuildBlock(ctx context.Context, _ uint64) (avalanchego.VMBlock, error) {
	if vm.paused.Load() {
		<-ctx.Done() // let the caller's impatient build time out
		return nil, ctx.Err()
	}
	h := vm.nextHeight.Add(1) - 1
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, h)
	return &testInnerBlock{Height_: h, TS: time.Now(), Payload: payload}, nil
}

func (vm *testVM) WaitForPendingBlock(ctx context.Context) {
	if vm.paused.Load() {
		<-ctx.Done() // no pending block while paused
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
	}
}

func (vm *testVM) ParseBlock(_ context.Context, b []byte) (avalanchego.VMBlock, error) {
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

func (pc *testPlatformChain) GetMinimumHeight() uint64 {
	return pc.currentHeight()
}

func (pc *testPlatformChain) GetCurrentHeight() uint64 {
	return pc.currentHeight()
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
	wals     []*testutil.TestWAL
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
	encoded := block.Bytes()
	seq := m.NumBlocks()
	m.snapLock.Lock()
	m.blocks[seq] = storedBlock{rawBlock: encoded, fin: certificate}
	m.snapLock.Unlock()
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
	m.snapLock.Lock()
	sb, ok := m.blocks[seq]
	m.snapLock.Unlock()
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
		parsed, err := parseTestInnerBlock(raw.InnerBlockBytes)
		require.NoError(m.t, err)
		inner = parsed
	}
	return metadata.StateMachineBlock{InnerBlock: inner, Metadata: raw.Metadata}
}

func (m *MockStorage) CreateWAL() (wal.DeletableWAL, error) {
	w := testutil.NewTestWAL(m.t)
	m.snapLock.Lock()
	m.wals = append(m.wals, w)
	m.snapLock.Unlock()
	return w, nil
}

// cloneBelow returns a storage holding every block of m below seq - the block at seq itself, and
// anything after it, is left out - so that a test can bring up a node that lags the chain by them.
func (m *MockStorage) cloneBelow(seq uint64) *MockStorage {
	clone := NewMockStorage(m.t)
	for cloned := uint64(0); cloned < seq; cloned++ {
		m.snapLock.Lock()
		stored, ok := m.blocks[cloned]
		m.snapLock.Unlock()
		require.True(m.t, ok)

		block := &ParsedBlock{StateMachineBlock: m.parseStored(stored.rawBlock)}
		require.NoError(m.t, clone.Index(context.Background(), block, stored.fin))
	}
	return clone
}

// containsNotarization reports whether any WAL this storage handed out holds a notarization for
// the given round.
func (m *MockStorage) containsNotarization(round uint64) bool {
	m.snapLock.Lock()
	wals := append([]*testutil.TestWAL(nil), m.wals...)
	m.snapLock.Unlock()

	for _, w := range wals {
		if w.ContainsNotarization(round) {
			return true
		}
	}
	return false
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
	defer n.lock.Unlock()

	// If an instance was previously registered under this id (e.g. a restart replacing
	// the node), stop its delivery goroutine before swapping in the new one.
	old := n.nodes[string(id)]
	if old != nil {
		close(old.done)
		<-old.stopped
	}

	n.nodes[string(id)] = node

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

// registeredIDs returns the nodes currently wired into the network. Broadcasters go through it
// rather than reading the map directly, since nodes register while others are already running.
func (n *inMemNetwork) registeredIDs() []common.NodeID {
	n.lock.Lock()
	defer n.lock.Unlock()
	ids := make([]common.NodeID, 0, len(n.nodes))
	for _, node := range n.nodes {
		ids = append(ids, node.inst.Config.ID)
	}
	return ids
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
func toRawBlock(t *testing.T, vb common.VerifiedBlock) *metadata.RawBlock {
	bytes := vb.Bytes()
	raw := &metadata.RawBlock{}
	require.NoError(t, raw.UnmarshalCanoto(bytes))
	return raw
}

// reparseBlock reconstructs an independent *ParsedBlock from a verified block's
// wire bytes. Each call yields a fresh object sharing no pointers with the
// sender's live block, so that remaining references to the sender's block don't race with the receiver.
func reparseBlock(t *testing.T, vb common.VerifiedBlock) *ParsedBlock {
	raw := toRawBlock(t, vb)
	var inner avalanchego.VMBlock
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
	for _, dest := range s.net.registeredIDs() {
		s.Send(msg, dest)
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
