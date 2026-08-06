// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/ava-labs/simplex/wal"
	"github.com/stretchr/testify/require"
)

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

// pendingBlockSignal broadcasts to every waiter by closing the current channel and
// replacing it with a fresh one for the next generation of waiters.
type pendingBlockSignal struct {
	lock sync.Mutex
	ch   chan struct{}
}

func newPendingBlockSignal() *pendingBlockSignal {
	return &pendingBlockSignal{ch: make(chan struct{})}
}

// wait returns when the signal is broadcast or ctx is cancelled.
func (s *pendingBlockSignal) wait(ctx context.Context) {
	s.lock.Lock()
	ch := s.ch
	s.lock.Unlock()

	select {
	case <-ch:
	case <-ctx.Done():
	}
}

// broadcast wakes every current waiter.
func (s *pendingBlockSignal) broadcast() {
	s.lock.Lock()
	close(s.ch)
	s.ch = make(chan struct{})
	s.lock.Unlock()
}

// blockBuilderVM builds an inner block only when the test triggers one on the block builder, so
// the chain grows one block per index call.
type blockBuilderVM struct {
	bb      *testutil.TestControlledBlockBuilder
	storage *MockStorage
	pending *pendingBlockSignal
}

func newBlockBuilderVM(bb *testutil.TestControlledBlockBuilder, storage *MockStorage, pending *pendingBlockSignal) *blockBuilderVM {
	return &blockBuilderVM{bb: bb, storage: storage, pending: pending}
}

func (vm *blockBuilderVM) BuildBlock(ctx context.Context, pChainHeight uint64) (avalanchego.VMBlock, error) {
	// The builder gates when a block is built; the block it returns is not an inner block, so
	// it is thrown away.
	fmt.Println("hello")
	if _, ok := vm.bb.BuildBlock(ctx, common.ProtocolMetadata{}, common.Blacklist{}); !ok {
		return nil, ctx.Err()
	}

	// the inner height is the seq of the block being built, which is how many blocks the node
	// has committed so far
	height := vm.storage.NumBlocks()
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, height)
	return &testInnerBlock{Height_: height, TS: time.Now(), Payload: payload}, nil
}

// WaitForPendingBlock returns when index broadcasts that a block is being created,
// or when ctx is cancelled.
func (vm *blockBuilderVM) WaitForPendingBlock(ctx context.Context) {
	vm.pending.wait(ctx)
}

type node struct {
	id      common.NodeID
	vm      *blockBuilderVM
	inst    *Instance
	storage *MockStorage
	comm    *instanceComm
}

// noopICMTransition keeps every block in the same ICM epoch, so an epoch only ever changes
// because the validator set did.
func noopICMTransition(_ metadata.ICMEpochInput) metadata.ICMEpochInfo {
	return metadata.ICMEpochInfo{}
}

const genesisPChainHeight uint64 = 0

var genesisBlock = &testInnerBlock{Height_: genesisPChainHeight, TS: time.Now(), Payload: []byte("genesis")}
var paramConfig = ParameterConfig{
	MaxNetworkDelay:  500 * time.Millisecond,
	MaxRoundWindow:   100,
	WALMaxEntryCount: 1024,
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

func (pc *testPlatformChain) GetValidatorSet(height uint64) (metadata.NodeBLSMappings, error) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	set, ok := pc.validatorSetAtHeight[height]
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

	pc.height = height
	close(pc.heightChanged)
	pc.heightChanged = make(chan struct{})
}

func (pc *testPlatformChain) LastNonSimplexBlockPChainHeight() uint64 {
	return pc.genesisHeight
}

type network struct {
	t *testing.T

	pChain *testPlatformChain
	seq    uint64
	epoch  uint64

	// pending wakes every VM blocked in WaitForPendingBlock when index creates a block.
	pending *pendingBlockSignal

	validatorSets map[uint64]common.Nodes // epoch -> sorted validators

	// lock guards nodes, which comm goroutines read while addNode appends.
	lock  sync.Mutex
	nodes []node
}

func (c *network) nodesSnapshot() []node {
	c.lock.Lock()
	defer c.lock.Unlock()
	return append([]node(nil), c.nodes...)
}

func newChain(t *testing.T, pChain *testPlatformChain) *network {
	validatorSets := make(map[uint64]common.Nodes)
	genesisNodes := pChain.GenesisValidatorSet().Nodes()
	common.SortNodes(genesisNodes)
	validatorSets[1] = genesisNodes

	return &network{
		t:             t,
		pChain:        pChain,
		pending:       newPendingBlockSignal(),
		validatorSets: validatorSets,

		seq:   2, // zero block is built automatically, chain should be on seq 2
		epoch: 1,
	}
}

func (c *network) addNode(id common.NodeID) {
	comm := newInstanceComm(c, id)
	bd := &testInnerBlockDeserializer{}
	storage := NewMockStorageWithGenesis(c.t, bd)

	vm := newBlockBuilderVM(testutil.NewTestControlledBlockBuilder(c.t), storage, c.pending)
	wc := &walCreator{t: c.t}
	instance := NewInstance(Config{
		LastNonSimplexInnerBlock: genesisBlock,
		ParameterConfig:          paramConfig,
		PlatformChain:            c.pChain,
		Broadcaster:              comm,
		Sender:                   comm,
		CryptoOps:                &testCryptoOps{},
		WalCreator:               wc.createWAL,
		Storage:                  storage,
		// the first byte of the node id labels the node's log records
		Logger:            testutil.MakeLogger(c.t, int(id[0])),
		WALs:              nil,
		VM:                vm,
		ICMETransition:    noopICMTransition,
		BlockDeserializer: bd,
		ID:                id,
	})

	node := node{
		id:      id,
		storage: storage,
		comm:    comm,
		vm:      vm,
		inst:    instance,
	}

	c.lock.Lock()
	c.nodes = append(c.nodes, node)
	c.lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	c.t.Cleanup(cancel)

	require.NoError(c.t, node.inst.Start(ctx))
	c.t.Cleanup(node.inst.Stop)

	node.storage.WaitForBlockCommit(c.seq - 1)
}

func (c *network) index() (common.VerifiedBlock, error) {
	nodes, ok := c.validatorSets[c.epoch]
	if !ok {
		return nil, fmt.Errorf("epoch is not set epoch: %d. trying to index seq: %d", c.epoch, c.seq)
	}

	// no nodes have indexed this sequence yet
	for _, node := range c.nodes {
		node.storage.EnsureNoBlockCommit(c.t, c.seq)
	}

	leaderID := simplex.LeaderForRound(nodes.NodeIDs(), c.seq)
	for _, node := range c.nodes {
		if bytes.Equal(node.id, leaderID) {
			fmt.Println("woken up leader", node.id[0], node.id)
			node.vm.bb.TriggerNewBlock()
			fmt.Println("triggered")
		}
	}

	fmt.Println("triggered wait for pending block")

	// wake every VM blocked in WaitForPendingBlock
	c.pending.broadcast()
	fmt.Println("bang for pending block")

	var block common.VerifiedBlock
	for _, node := range c.nodes {
		committedBlock := node.storage.WaitForBlockCommit(c.seq)
		if block == nil {
			block = committedBlock
		} else {
			require.Equal(c.t, block.Bytes(), committedBlock.Bytes())
		}
	}

	require.Equal(c.t, block.BlockHeader().Seq, c.seq)
	c.seq++

	// check if its a sealing
	if block.SealingBlockInfo() != nil {
		c.epoch = c.seq
		newValidatorSet := block.SealingBlockInfo().ValidatorSet
		common.SortNodes(newValidatorSet)
		c.validatorSets[c.epoch] = newValidatorSet
	}

	return block, nil
}

// waitUntilSealingBlock waits until every node commits the block at the current seq,
// repeating until that block is a sealing block. It then advances the network into
// the new epoch and returns the sealing block.
func (c *network) waitUntilSealingBlock() common.VerifiedBlock {
	for {
		var block common.VerifiedBlock
		for _, node := range c.nodes {
			committedBlock := node.storage.WaitForBlockCommit(c.seq)
			if block == nil {
				block = committedBlock
			} else {
				require.Equal(c.t, block.Bytes(), committedBlock.Bytes())
			}
		}

		require.Equal(c.t, block.BlockHeader().Seq, c.seq)
		c.seq++

		if block.SealingBlockInfo() == nil {
			continue
		}

		c.epoch = c.seq
		newValidatorSet := block.SealingBlockInfo().ValidatorSet
		common.SortNodes(newValidatorSet)
		c.validatorSets[c.epoch] = newValidatorSet
		return block
	}
}

func generateNodeId() common.NodeID {
	var id [20]byte
	rand.Read(id[:])
	return common.NodeID(id[:])
}

func generateNodeIDMapping(id avalanchego.NodeID) metadata.NodeBLSMapping {
	return metadata.NodeBLSMapping{
		NodeID: avalanchego.NodeID(id),
		BLSKey: []byte{id[0], id[1]},
		Weight: 1,
	}
}

type walCreator struct {
	t *testing.T
}

func (w *walCreator) createWAL() (wal.DeletableWAL, error) {
	return testutil.NewTestWAL(w.t), nil
}
