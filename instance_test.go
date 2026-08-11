// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/nonvalidator"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

// peerApproval builds the approval a node would have signed for the validator set at
// pChainHeight, for tests that inject approvals on behalf of a node that cannot send them.
func peerApproval(nodeID [20]byte, pChainHeight uint64) *common.ValidatorSetApproval {
	return &common.ValidatorSetApproval{
		NodeID:        nodeID,
		PChainHeight:  pChainHeight,
		AuxInfoDigest: sha256.Sum256(nil),
		Signature:     []byte{1, 2, 3},
	}
}

func TestParseBlockSizeMatchesBytes(t *testing.T) {
	// Case 1: Bytes() first, Size() second, size returns the cached length.
	pb := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: common.ProtocolMetadata{
					Version: 1,
					Prev:    common.Digest{},
					Round:   1,
					Epoch:   4,
					Seq:     2,
				},
				SimplexBlacklist: common.Blacklist{
					Updates:   common.BlacklistUpdates{{NodeIndex: 1, Type: 1}},
					NodeCount: 2,
				},
				PChainHeight: 6,
			},
			InnerBlock: &testInnerBlock{
				Height_: 7,
				TS:      time.UnixMilli(8),
				Payload: []byte("payload"),
			},
		},
	}
	bytes := pb.Bytes()
	require.Equal(t, len(bytes), pb.Size())

	// Case 2: Size() first on a non serialized block. it will
	// compute the size and match a later Byte() call.
	pb2 := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: common.ProtocolMetadata{
					Version: 1,
					Prev:    common.Digest{},
					Round:   1,
					Epoch:   4,
					Seq:     2,
				},
				SimplexBlacklist: common.Blacklist{
					Updates:   common.BlacklistUpdates{{NodeIndex: 1, Type: 1}},
					NodeCount: 2,
				},
				PChainHeight: 6,
			},
			InnerBlock: &testInnerBlock{
				Height_: 9,
				TS:      time.UnixMilli(10),
				Payload: []byte("other payload"),
			},
		},
	}
	size := pb2.Size()
	require.NotZero(t, size)
	bytes2 := pb2.Bytes()
	require.Equal(t, len(bytes2), size)

	// case 3: cincurrent Size() calls on a block that was never serialized.
	// the goroutines rase to compute the size, the lock must make this
	// safe and every call must return the correct value

	pb3 := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: common.ProtocolMetadata{
					Version: 1,
					Prev:    common.Digest{},
					Round:   1,
					Epoch:   4,
					Seq:     2,
				},
				SimplexBlacklist: common.Blacklist{
					Updates:   common.BlacklistUpdates{{NodeIndex: 1, Type: 1}},
					NodeCount: 2,
				},
				PChainHeight: 6,
			},
			InnerBlock: &testInnerBlock{
				Height_: 11,
				TS:      time.UnixMilli(12),
				Payload: []byte("concurrent"),
			},
		},
	}
	var wg sync.WaitGroup
	sizes := make([]int, 4)
	for i := range sizes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sizes[i] = pb3.Size()
		}()
	}
	wg.Wait()
	bytes3 := pb3.Bytes()
	for _, size := range sizes {
		require.Equal(t, len(bytes3), size)
	}
}

func TestInstanceDoubleStartFails(t *testing.T) {
	const basePChainHeight = uint64(1)

	var id [20]byte
	rand.Read(id[:])
	nodeID := common.NodeID(id[:])

	// Single-validator set including this node, so Start brings up a validator epoch.
	validatorSetsAtHeight := map[uint64]metadata.NodeBLSMappings{
		basePChainHeight: {
			{NodeID: id, BLSKey: []byte{0xaa}, Weight: 1},
		},
	}

	pChain := newTestPlatformChain(basePChainHeight, validatorSetsAtHeight)
	cops := &testCryptoOps{}
	genesisBlock := &testInnerBlock{Height_: 0, TS: time.Now(), Payload: []byte("genesis")}

	net := newInMemNetwork(t)
	t.Cleanup(net.stop)

	storage := newStorageWithGenesis(t, genesisBlock)

	inst := newInstance(t, nodeID, storage, net, pChain, cops, genesisBlock)

	require.NoError(t, inst.Start(t.Context()))
	t.Cleanup(inst.Stop)

	require.ErrorContains(t, inst.Start(t.Context()), "instance already started")
}

// currentNonValidator returns the non-validator the instance is running, or nil if it
// is running a validator epoch instead. Since a restart installs a freshly created
// non-validator, comparing the returned pointer across time detects one.
func currentNonValidator(inst *Instance) *nonvalidator.NonValidator {
	inst.lock.Lock()
	defer inst.lock.Unlock()
	return inst.nv
}

// waitForNonValidatorRole waits until the instance has replaced its validator epoch with a
// non-validator, and returns that non-validator.
func waitForNonValidatorRole(t *testing.T, inst *Instance) *nonvalidator.NonValidator {
	t.Helper()
	var nv *nonvalidator.NonValidator
	require.Eventually(t, func() bool {
		inst.lock.Lock()
		defer inst.lock.Unlock()
		nv = inst.nv
		return nv != nil && inst.e == nil
	}, 20*time.Second, 100*time.Millisecond)
	return nv
}

// waitForValidatorRole waits until the instance has replaced its non-validator with a
// validator epoch, and returns the epoch it started as a validator.
func waitForValidatorRole(t *testing.T, inst *Instance) uint64 {
	t.Helper()
	var epoch uint64
	require.Eventually(t, func() bool {
		inst.lock.Lock()
		defer inst.lock.Unlock()
		if inst.nv != nil || inst.e == nil {
			return false
		}
		epoch = inst.e.Epoch
		return true
	}, 20*time.Second, 100*time.Millisecond)
	return epoch
}

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
func newInstanceWithVM(t *testing.T, nodeID common.NodeID, storage *MockStorage, net *inMemNetwork, pChain *testPlatformChain, cops *testCryptoOps, genesisBlock *testInnerBlock, vm VM) *Instance {
	comm := &networkSender{net: net, self: nodeID}
	config := Config{
		Logger:                   testutil.MakeLogger(t, int(nodeID[0])),
		ID:                       nodeID,
		VM:                       vm,
		Storage:                  storage,
		Sender:                   comm,
		Broadcaster:              comm,
		PlatformChain:            pChain,
		CryptoOps:                cops,
		LastNonSimplexInnerBlock: genesisBlock,
		WalCreator:               storage.CreateWAL,
		ParameterConfig: ParameterConfig{
			MaxNetworkDelay:  500 * time.Millisecond,
			MaxRoundWindow:   100,
			WALMaxEntryCount: 1024,
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
	}, 20*time.Second, 100*time.Millisecond)
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
			require.NoError(t, msm.HandleApproval(approval, 1))
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
