package nonvalidator

import (
	"context"
	"fmt"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

var testNodes = simplex.Nodes{
	{Node: simplex.NodeID{1}, Weight: 1},
	{Node: simplex.NodeID{2}, Weight: 1},
	{Node: simplex.NodeID{3}, Weight: 1},
	{Node: simplex.NodeID{4}, Weight: 1},
}

var genesis = testutil.NewTestBlock(simplex.ProtocolMetadata{
	Seq:   0,
	Round: 0,
	Epoch: 0,
}, simplex.Blacklist{})

type messageInfo struct {
	msg  *simplex.Message
	from simplex.NodeID
}

// send delivers m to nv via HandleMessage. The error is returned so future
// tests can assert on it; existing call sites discard.
func (m *messageInfo) send(nv *NonValidator) error {
	return nv.HandleMessage(m.msg, m.from)
}

// testChain combines an in-memory storage with chain-tip bookkeeping so a
// test can seed prior history and append successor blocks through a single
// value.
type testChain struct {
	*testutil.InMemStorage
	t             *testing.T
	seq           uint64
	epoch         uint64
	prev          simplex.Digest
	validatorSets map[uint64]simplex.Nodes
}

func (c *testChain) String() string {
	return fmt.Sprintf("TestChain: Current Epoch: %d, Current Seq: %d", c.epoch, c.seq)
}

// newSeededChain returns a testChain whose storage is indexed through seq=lastSeq:
//
//	seq 0           — genesis (epoch 0)
//	seq 1           — sealing block opening epoch 1 (validatorSet = testNodes)
//	seq 2..lastSeq  — epoch 1 blocks
func newSeededChain(t *testing.T, nodes simplex.Nodes, lastSeq uint64) *testChain {
	require.GreaterOrEqual(t, lastSeq, uint64(1), "lastSeq must be >= 1 (0 is genesis, 1 is the first epoch's sealing block)")

	tc := &testChain{
		InMemStorage:  testutil.NewInMemStorage(),
		t:             t,
		prev:          genesis.Digest,
		validatorSets: make(map[uint64]simplex.Nodes),
	}
	require.NoError(t, tc.Index(context.Background(), genesis, simplex.Finalization{}))

	// The first sealing block lives directly in epoch 1 (metadata.Epoch=1,
	// sealingInfo.Epoch=1) — matches the convention used by newStorageWithFirstEpoch
	// historically and by TestNewEpochs.
	sealingBlock := newSealingTestBlock(1, 1, tc.prev, &simplex.SealingBlockInfo{
		Epoch:        1,
		ValidatorSet: nodes,
	})
	require.NoError(t, tc.Index(context.Background(), sealingBlock, simplex.Finalization{}))
	tc.seq = 1
	tc.epoch = 1
	tc.prev = sealingBlock.Digest

	for tc.seq < lastSeq {
		b := tc.appendBlock()
		require.NoError(t, tc.Index(context.Background(), b, simplex.Finalization{}))
	}
	return tc
}

// appendBlock advances the chain by one non-sealing block in the current
// epoch. The block is constructed but NOT indexed.
func (tc *testChain) appendBlock() *testutil.TestBlock {
	tc.seq++
	block := newBlock(tc.seq, tc.epoch, tc.prev)
	tc.prev = block.Digest
	return block
}

// appendSealing advances the chain by one sealing block announcing nextEpoch
// and validatorSet. The sealing block's metadata.Epoch is the current epoch;
// subsequent appendBlock calls live in nextEpoch. Not indexed.
func (tc *testChain) appendSealing(validatorSet simplex.Nodes) *sealingTestBlock {
	tc.seq++
	block := newSealingTestBlock(tc.seq, tc.epoch, tc.prev, &simplex.SealingBlockInfo{
		Epoch:        tc.seq,
		ValidatorSet: validatorSet,
	})
	tc.prev = block.Digest
	tc.epoch = tc.seq
	tc.validatorSets[tc.seq] = validatorSet
	return block
}

func (tc *testChain) signatureAggregatorCreator(nodes []simplex.Node) simplex.SignatureAggregator {
	isQuorumFunc := func(signatures []simplex.NodeID) bool {
		count := 0
		nodeSet := make(map[string]struct{})
		for _, node := range nodes {
			nodeSet[node.Node.String()] = struct{}{}
		}

		for _, sig := range signatures {
			if _, ok := nodeSet[sig.String()]; ok {
				count++
			}
		}

		return count >= simplex.Quorum(len(nodes))
	}
	return &testutil.TestSignatureAggregator{
		IsQuorumFunc: isQuorumFunc,
		N:            len(nodes),
	}
}
