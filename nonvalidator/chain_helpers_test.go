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

// send is a helper that calls HandleMessage on `nv` with `m`.
func (m *messageInfo) send(nv *NonValidator) error {
	return nv.HandleMessage(m.msg, m.from)
}

// testChain is a helper that book-keeps the current chain-tip, alongside any
// blocks and finalizations indexed on the chain. It allows easy creation of block and sealing blocks.
// It also manages validator sets, and the SignatureAggregatorCreator required by non-validators to verify finalizations.
type testChain struct {
	*testutil.InMemStorage
	t *testing.T

	// seq, epoch, prev, sealingBlockHash defines the current tip of testChain.
	// the next block uses seq + 1, epoch, prevDigest = digest, etc...
	seq              uint64
	epoch            uint64
	digest           simplex.Digest
	sealingBlockHash simplex.Digest

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
		digest:        genesis.Digest,
		validatorSets: make(map[uint64]simplex.Nodes),
		seq:           0, // set for clarity
		epoch:         0,
	}
	require.NoError(t, tc.Index(context.Background(), genesis, simplex.Finalization{}))
	tc.validatorSets[0] = nodes

	// firstSimplex comes after genesis
	sealingBlock := tc.appendFirstSimplexAfterGenesis(nodes)
	finalization := tc.newFinalization(sealingBlock)
	require.NoError(t, tc.Index(context.Background(), sealingBlock, finalization))

	for tc.seq < lastSeq {
		b := tc.appendBlock()
		finalization := tc.newFinalization(b)

		require.NoError(t, tc.Index(context.Background(), b, finalization))
	}

	return tc
}

// newSnowToSimplexChain returns a testChain whose storage is indexed through seq=lastSnowSeq:
func newSnowToSimplexChain(t *testing.T, lastSnowSeq uint64) *testChain {
	require.GreaterOrEqual(t, lastSnowSeq, uint64(1), "genesis must be indexed")

	tc := &testChain{
		InMemStorage:  testutil.NewInMemStorage(),
		t:             t,
		digest:        genesis.Digest,
		validatorSets: make(map[uint64]simplex.Nodes),
		seq:           0,
		epoch:         0,
	}

	// genesis
	require.NoError(t, tc.Index(context.Background(), genesis, simplex.Finalization{}))

	for tc.seq < lastSnowSeq {
		b := tc.appendBlock()
		require.NoError(t, tc.Index(context.Background(), b, simplex.Finalization{}))
	}

	require.Equal(t, tc.seq, lastSnowSeq)
	return tc
}

// appendBlock advances the chain by one non-sealing block in the current
// epoch. The block is constructed but NOT indexed.
func (tc *testChain) appendBlock() *testutil.TestBlock {
	tc.seq++
	block := newBlock(tc.seq, tc.epoch, tc.digest)
	tc.digest = block.Digest
	return block
}

func (tc *testChain) newFinalization(b simplex.VerifiedBlock) simplex.Finalization {
	nodes, ok := tc.validatorSets[b.BlockHeader().Epoch]
	require.True(tc.t, ok, "Validator set expected to have before creating a new finalization", b.BlockHeader().Epoch, b.BlockHeader().Seq)
	sigAgg := tc.signatureAggregatorCreator(nodes)
	finalization, _ := testutil.NewFinalizationRecord(tc.t, sigAgg, b, nodes.NodeIDs())

	return finalization
}

// appendSealing advances the chain by one sealing block announcing nextEpoch
// and validatorSet. The sealing block's metadata.Epoch is the current epoch;
// subsequent appendBlock calls live in nextEpoch. Not indexed.
func (tc *testChain) appendSealing(validatorSet simplex.Nodes) *sealingTestBlock {
	tc.seq++

	block := newSealingTestBlock(tc.seq, tc.epoch, tc.digest, &simplex.SealingBlockInfo{
		Epoch:                tc.seq,
		ValidatorSet:         validatorSet,
		PrevSealingBlockHash: tc.sealingBlockHash,
	})
	tc.digest = block.Digest
	tc.epoch = tc.seq
	tc.validatorSets[tc.epoch] = validatorSet
	tc.sealingBlockHash = block.Digest
	return block
}

// the first simplex block must return sealing block information
func (tc *testChain) appendFirstSimplexAfterGenesis(validatorSet simplex.Nodes) *sealingTestBlock {
	lastBlock, _, err := tc.Retrieve(tc.seq)
	tc.seq++
	firstEverEpoch := tc.seq
	require.NoError(tc.t, err)

	block := newSealingTestBlock(tc.seq, firstEverEpoch, tc.digest, &simplex.SealingBlockInfo{
		Epoch:                tc.seq,
		ValidatorSet:         validatorSet,
		PrevSealingBlockHash: lastBlock.BlockHeader().Digest,
	})
	tc.digest = block.Digest
	tc.epoch = firstEverEpoch
	tc.sealingBlockHash = block.Digest
	tc.validatorSets[firstEverEpoch] = validatorSet
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

// addEpochs adds sealing blocks at epochs, and normal blocks in between
func (tc *testChain) addEpochs(epochs ...uint64) {
	// ensure that the new epoch we are adding is not already indexed
	require.Greater(tc.t, epochs[0], tc.seq)

	for _, epoch := range epochs {
		for tc.seq < epoch-1 {
			b := tc.appendBlock()
			finalization := tc.newFinalization(b)
			require.NoError(tc.t, tc.Index(context.Background(), b, finalization))
		}
		validatorSet, ok := tc.validatorSets[tc.epoch]
		require.True(tc.t, ok)

		newNodes := append(validatorSet, simplex.Node{
			Node:   simplex.NodeID{byte(epoch)},
			Weight: 1,
		})
		sealing := tc.appendSealing(newNodes)
		finalization := tc.newFinalization(sealing)
		require.NoError(tc.t, tc.Index(context.Background(), sealing, finalization))
	}
}

func (tc *testChain) nodes() simplex.Nodes {
	latestValidatorSet, ok := tc.validatorSets[tc.epoch]
	require.True(tc.t, ok)

	return latestValidatorSet
}
