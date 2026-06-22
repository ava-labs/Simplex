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
	// Two-validator last epoch, end to end:
	//   - The node under test bootstraps from a non-Simplex genesis block and
	//     commits a series of blocks alone in the first (single-validator) epoch.
	//   - It observes a validator-set change on the P-chain that both bumps the
	//     weight and adds a second validator, then seals the epoch and enters the
	//     two-validator epoch.
	//   - A second, real Instance (the peer) is started from a copy of the node's
	//     ledger, and the two validators drive the two-validator epoch together
	//     over an in-memory network: block proposals are delivered through
	//     HandleBlockMessage and votes/finalizations through HandleMessage.
	//
	// Sealing the epoch needs a quorum of approvals of the new (two-validator) set.
	// The peer is not running yet during epoch 1, so its approval is injected
	// directly into the node's approval store (HandleApproval) until the sealing
	// block appears.
	const (
		basePChainHeight        = uint64(1)
		epochChangePChainHeight = uint64(100)
	)

	var id [20]byte
	rand.Read(id[:])
	nodeID := common.NodeID(id[:])

	// The peer that joins the validator set in the last epoch. Its ID is chosen
	// to differ from the (random) node under test.
	var peerID [20]byte
	rand.Read(peerID[:])
	peerNodeID := common.NodeID(peerID[:])

	// Epoch 1 is single-validator (just the node under test). The last epoch is
	// expanded to two validators (the node + the peer).
	validatorSetsAtHeight := map[uint64]metadata.NodeBLSMappings{
		basePChainHeight: {
			{NodeID: id, BLSKey: []byte{0xaa}, Weight: 1},
		},
		epochChangePChainHeight: {
			{NodeID: id, BLSKey: []byte{0xaa}, Weight: 2},
			{NodeID: peerID, BLSKey: []byte{0xbb}, Weight: 2},
		},
	}

	pc := newTestPlatformChain(basePChainHeight, validatorSetsAtHeight)
	cops := &testCryptoOps{}

	// The last non-Simplex block: VM height 1, so the zero (first Simplex) block
	// is built at storage seq 1 on top of the genesis at seq 0.
	lastNonSimplex := &testInnerBlock{Height_: 0, TS: time.Now(), Payload: []byte("genesis")}

	// net routes messages between the two instances asynchronously (see
	// inMemNetwork) so proposals reach HandleBlockMessage and everything else
	// reaches HandleMessage.
	net := newInMemNetwork(t)
	t.Cleanup(net.stop)

	// newInstance builds an Instance sharing the common test dependencies but with
	// its own ID, storage and VM, wired to the in-memory network. Each instance
	// has an independent VM: the inner-block heights it produces are carried
	// verbatim into blocks and never cross-checked, so they need not agree.
	newInstance := func(logID int, nodeID common.NodeID, storage *MockStorage) *Instance {
		return &Instance{
			Config: Config{
				Logger:                   testutil.MakeLogger(t, logID),
				ID:                       nodeID,
				VM:                       newTestVM(),
				Storage:                  storage,
				Sender:                   net.senderFor(nodeID),
				PlatformChain:            pc,
				CryptoOps:                cops,
				LastNonSimplexInnerBlock: lastNonSimplex,
				ParameterConfig: ParameterConfig{
					MaxNetworkDelay:  500 * time.Millisecond,
					MaxRoundWindow:   100,
					WALMaxEntryCount: 1024,
				},
			},
		}
	}

	storage := NewMockStorage(t)
	smb := metadata.StateMachineBlock{InnerBlock: lastNonSimplex}
	require.NoError(t, storage.Index(context.Background(), &ParsedBlock{StateMachineBlock: smb}, common.Finalization{}))

	firstInstance := newInstance(0, nodeID, storage)
	net.register(nodeID, firstInstance)
	require.NoError(t, firstInstance.Start())
	t.Cleanup(firstInstance.Stop)

	// Epoch 1: wait until the node has bootstrapped (genesis + zero block) and
	// committed a series of normal blocks on its own.
	const epoch1Target = uint64(5) // genesis(0) + zero block(1) + 3 normal blocks
	waitForNumBlocks(t, storage, epoch1Target)

	epoch1Blocks := storage.NumBlocks()
	require.GreaterOrEqual(t, epoch1Blocks, epoch1Target)

	// The validator set in force is the one introduced by the most recent block
	// that carries a BlockValidationDescriptor (the zero block in epoch 1).
	require.Equal(t, uint64(1), latestValidatorWeight(t, storage), "epoch 1 should use the original validator weight")

	// Trigger the epoch change: the validator set changes at epochChangePChainHeight,
	// growing from one validator to two.
	pc.advanceTo(epochChangePChainHeight)
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
	waitForSealingBlock(t, firstInstance, approval, epoch1Blocks)

	// The node is now in the two-validator epoch but cannot make progress alone
	// (a single vote/empty-vote is not a quorum). Start the real peer from a copy
	// of the node's ledger so the two validators drive the epoch together.
	peerStorage := cloneStorageDeep(t, storage)
	secondInstance := newInstance(1, peerNodeID, peerStorage)
	net.register(peerNodeID, secondInstance)
	require.NoError(t, secondInstance.Start())
	t.Cleanup(secondInstance.Stop)

	// With both validators live, the two-validator epoch commits more blocks.
	sealedAt := storage.NumBlocks()
	const epoch2Extra = uint64(3)
	waitForNumBlocks(t, storage, sealedAt+epoch2Extra)

	// Confirm the post-change validator set has the new weight and now contains
	// two validators.
	require.Equal(t, uint64(2), latestValidatorWeight(t, storage), "epoch 2 should use the new validator weight")
	require.Equal(t, 2, latestValidatorSetSize(t, storage), "the last epoch should have two validators")
}

func latestValidatorWeight(t *testing.T, storage *MockStorage) uint64 {
	t.Helper()
	num := storage.NumBlocks()
	for seq := int64(num) - 1; seq >= 0; seq-- {
		block, ok := storage.blockSnapshot(uint64(seq))
		if !ok {
			continue
		}
		bvd := block.Metadata.SimplexEpochInfo.BlockValidationDescriptor
		if bvd != nil && len(bvd.AggregatedMembership.Members) > 0 {
			return bvd.AggregatedMembership.Members[0].Weight
		}
	}
	t.Fatalf("no block with a BlockValidationDescriptor found in storage")
	return 0
}

// latestValidatorSetSize returns the number of validators recorded in the most
// recent block that carries a BlockValidationDescriptor.
func latestValidatorSetSize(t *testing.T, storage *MockStorage) int {
	t.Helper()
	num := storage.NumBlocks()
	for seq := int64(num) - 1; seq >= 0; seq-- {
		block, ok := storage.blockSnapshot(uint64(seq))
		if !ok {
			continue
		}
		bvd := block.Metadata.SimplexEpochInfo.BlockValidationDescriptor
		if bvd != nil && len(bvd.AggregatedMembership.Members) > 0 {
			return len(bvd.AggregatedMembership.Members)
		}
	}
	t.Fatalf("no block with a BlockValidationDescriptor found in storage")
	return 0
}

func waitForNumBlocks(t *testing.T, storage *MockStorage, target uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if storage.NumBlocks() >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d blocks, have %d", target, storage.NumBlocks())
}

// waitForSealingBlock waits until a two-validator sealing block (a block carrying a
// BlockValidationDescriptor with the new weight) is committed at or after fromSeq.
//
// Sealing needs a quorum of approvals of the new (two-validator) set, but the peer
// is not running yet, so on every poll it injects the peer's approval into the
// node's approval store. Injecting before the store is initialized is a harmless
// no-op, and re-injecting an existing approval is deduplicated, so this simply
// guarantees the approval lands before the node seals.
func waitForSealingBlock(t *testing.T, inst *Instance, approval *metadata.ValidatorSetApproval, fromSeq uint64) {
	t.Helper()
	storage := inst.Config.Storage.(*MockStorage)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		inst.lock.Lock()
		msm := inst.msm
		inst.lock.Unlock()
		if msm != nil {
			_ = msm.HandleApproval(approval, 1)
		}

		num := storage.NumBlocks()
		for seq := fromSeq; seq < num; seq++ {
			block, ok := storage.blockSnapshot(seq)
			if !ok {
				continue
			}
			bvd := block.Metadata.SimplexEpochInfo.BlockValidationDescriptor
			if bvd != nil && len(bvd.AggregatedMembership.Members) > 0 &&
				bvd.AggregatedMembership.Members[0].Weight == 2 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for sealing block after seq %d", fromSeq)
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
	case <-time.After(10 * time.Millisecond):
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
	lock sync.Mutex
	cond *sync.Cond
	height uint64
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
	return pc.validatorSetAtHeight[lastCheckpoint]
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
	// sync.Cond has no notion of cancellation, so wake the waiter when the context
	// is done and let the loop below observe ctx.Err().
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

// ---------------------------------------------------------------------------
// testCryptoOps: permissive crypto that accepts all signatures and forms
// quorums based on node count (reusing testutil aggregation/QC helpers).
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// MockStorage: in-memory Storage with a real (in-memory) WAL.
// ---------------------------------------------------------------------------

type MockStorage struct {
	t *testing.T
	*testutil.InMemStorage

	// snapLock guards snapshots. Each committed block is snapshotted (its metadata
	// copied into an object the instance never touches again) at Index time, so the
	// test's polling helpers and the peer's storage clone can read block metadata
	// without racing on the block's canoto digest cache — which the running
	// instance keeps mutating as it re-digests blocks (e.g. as proposal parents).
	snapLock  sync.Mutex
	snapshots map[uint64]storedBlock
}

type storedBlock struct {
	encoded []byte
	fin     common.Finalization
}

func NewMockStorage(t *testing.T) *MockStorage {
	return &MockStorage{
		t:            t,
		InMemStorage: testutil.NewInMemStorage(),
		snapshots:    make(map[uint64]storedBlock),
	}
}

func (m *MockStorage) Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error {
	// Serialize the block on the committing goroutine (where it isn't racing with
	// the instance's own use of it) and keep only the bytes. Test readers then
	// reconstruct fully independent block objects from these bytes rather than
	// touching the instance's live block, whose canoto digest cache — including the
	// pointer-shared BlockValidationDescriptor — the instance keeps mutating.
	encoded, err := block.Bytes()
	if err != nil {
		return err
	}
	seq := m.InMemStorage.NumBlocks()
	m.snapLock.Lock()
	m.snapshots[seq] = storedBlock{encoded: encoded, fin: certificate}
	m.snapLock.Unlock()
	return m.InMemStorage.Index(ctx, block, certificate)
}

func (m *MockStorage) GetBlock(seq uint64) (metadata.StateMachineBlock, *common.Finalization, error) {
	vb, f, err := m.InMemStorage.Retrieve(seq)
	if err != nil {
		return metadata.StateMachineBlock{}, nil, err
	}
	return vb.(*ParsedBlock).StateMachineBlock, &f, nil
}

// blockSnapshot reconstructs an independent copy of the block at seq from its
// stored bytes. Test-only readers use it instead of GetBlock so they never touch
// the instance's live block objects (whose canoto digest cache the instance keeps
// mutating).
func (m *MockStorage) blockSnapshot(seq uint64) (metadata.StateMachineBlock, bool) {
	m.snapLock.Lock()
	sb, ok := m.snapshots[seq]
	m.snapLock.Unlock()
	if !ok {
		return metadata.StateMachineBlock{}, false
	}
	return m.parseStored(sb.encoded), true
}

// parseStored reconstructs a StateMachineBlock from the wire bytes recorded at
// Index. Each call yields a fresh object with no pointers shared with any other
// block, so concurrent readers never race.
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

// cloneStorageDeep copies src into a fresh MockStorage holding independent block
// objects, reconstructed from the bytes src recorded at Index. It never touches
// the source instance's live block objects (which would race on their canoto
// digest cache), and the peer gets its own object per block so its instance
// re-digests objects independent of the source node's.
func cloneStorageDeep(t *testing.T, src *MockStorage) *MockStorage {
	dst := NewMockStorage(t)
	num := src.NumBlocks()
	for seq := uint64(0); seq < num; seq++ {
		src.snapLock.Lock()
		sb, ok := src.snapshots[seq]
		src.snapLock.Unlock()
		require.True(t, ok, "missing snapshot for committed block %d", seq)

		pb := &ParsedBlock{StateMachineBlock: src.parseStored(sb.encoded)}
		require.NoError(t, dst.Index(context.Background(), pb, sb.fin))
	}
	return dst
}

// ---------------------------------------------------------------------------
// inMemNetwork: a tiny in-memory network that routes messages between Instances.
// Delivery happens on a per-node goroutine rather than inline in Send, because
// Send is invoked while the sending epoch holds its lock; delivering inline
// (which would take the destination's lock, whose epoch may in turn Send back)
// could deadlock. Block proposals are delivered through HandleBlockMessage and
// all other messages through HandleMessage, matching the real inbound paths.
// ---------------------------------------------------------------------------

type netMsg struct {
	from common.NodeID
	// Exactly one of {msg} or {rawBlock,vote} is set. Proposals are serialized to
	// rawBlock eagerly in Send (under the sender's epoch lock) so the delivery
	// goroutine never touches the sender's live block object, which would race on
	// its lazily-populated canoto digest cache.
	msg      *common.Message
	rawBlock *RawBlock
	vote     *common.Vote
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

// senderFor returns the Sender an instance uses to broadcast; it enqueues to the
// destination's delivery goroutine (skipping the loopback copy to itself).
func (n *inMemNetwork) senderFor(self common.NodeID) Sender {
	return &networkSender{net: n, self: self}
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
	// Proposals are delivered through HandleBlockMessage (which re-parses the raw
	// block), matching the real inbound path; everything else via HandleMessage.
	if m.rawBlock != nil {
		if err := inst.HandleBlockMessage(context.Background(), m.rawBlock, m.vote, m.from); err != nil {
			n.t.Logf("HandleBlockMessage from %x failed: %v", m.from, err)
		}
		return
	}
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

type networkSender struct {
	net  *inMemNetwork
	self common.NodeID
}

func (s *networkSender) Send(msg *common.Message, dest common.NodeID) {
	// An instance broadcasts to every validator including itself; ignore the
	// loopback copy.
	if bytes.Equal(s.self, dest) {
		return
	}
	m := netMsg{from: s.self}
	// Send runs under the sender's epoch lock, so serializing the proposal block
	// here is ordered with the sender's own accesses to it. Shipping the encoded
	// bytes (rather than the live block) keeps the delivery goroutine off the
	// sender's block object.
	if vbm := msg.VerifiedBlockMessage; vbm != nil {
		vote := vbm.Vote
		m.rawBlock = toRawBlock(s.net.t, vbm.VerifiedBlock)
		m.vote = &vote
	} else {
		m.msg = msg
	}
	s.net.enqueue(dest, m)
}
