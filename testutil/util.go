package testutil

import (
	"encoding/asn1"
	"testing"
	"time"

	"github.com/ava-labs/simplex"
	"github.com/stretchr/testify/require"
)

// DefaultTestNodeEpochConfig returns a default epoch config for a given node.
func DefaultTestNodeEpochConfig(t *testing.T, nodeID simplex.NodeID, comm simplex.Communication, bb simplex.BlockBuilder) (simplex.EpochConfig, *TestWAL, *InMemStorage) {
	l := MakeLogger(t, int(nodeID[0]))
	storage := NewInMemStorage()
	wal := NewTestWAL(t)
	conf := simplex.EpochConfig{
		MaxProposalWait:     simplex.DefaultMaxProposalWaitTime,
		MaxRebroadcastWait:  simplex.DefaultEmptyVoteRebroadcastTimeout,
		Comm:                comm,
		Logger:              l,
		ID:                  nodeID,
		Signer:              &testSigner{},
		WAL:                 wal,
		Verifier:            &testVerifier{},
		Storage:             storage,
		BlockBuilder:        bb,
		SignatureAggregator: &TestSignatureAggregator{},
		BlockDeserializer:   &BlockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
		StartTime:           time.Now(),
	}
	return conf, wal, storage
}

type AnyBlock interface {
	// BlockHeader encodes a succinct and collision-free representation of a block.
	BlockHeader() simplex.BlockHeader
}

func NewTestVote(block AnyBlock, id simplex.NodeID) (*simplex.Vote, error) {
	vote := simplex.ToBeSignedVote{
		BlockHeader: block.BlockHeader(),
	}
	sig, err := vote.Sign(&testSigner{})
	if err != nil {
		return nil, err
	}

	return &simplex.Vote{
		Signature: simplex.Signature{
			Signer: id,
			Value:  sig,
		},
		Vote: vote,
	}, nil
}

func InjectTestVote(t *testing.T, e *simplex.Epoch, block simplex.VerifiedBlock, id simplex.NodeID) {
	vote, err := NewTestVote(block, id)
	require.NoError(t, err)
	err = e.HandleMessage(&simplex.Message{
		VoteMessage: vote,
	}, id)
	require.NoError(t, err)
}

func NewTestFinalizeVote(t *testing.T, block simplex.VerifiedBlock, id simplex.NodeID) *simplex.FinalizeVote {
	f := simplex.ToBeSignedFinalization{BlockHeader: block.BlockHeader()}
	sig, err := f.Sign(&testSigner{})
	require.NoError(t, err)
	return &simplex.FinalizeVote{
		Signature: simplex.Signature{
			Signer: id,
			Value:  sig,
		},
		Finalization: simplex.ToBeSignedFinalization{
			BlockHeader: block.BlockHeader(),
		},
	}
}

func InjectTestFinalization(t *testing.T, e *simplex.Epoch, finalization *simplex.Finalization, from simplex.NodeID) {
	err := e.HandleMessage(&simplex.Message{
		Finalization: finalization,
	}, from)
	require.NoError(t, err)
}

func InjectTestFinalizeVote(t *testing.T, e *simplex.Epoch, block simplex.VerifiedBlock, id simplex.NodeID) {
	err := e.HandleMessage(&simplex.Message{
		FinalizeVote: NewTestFinalizeVote(t, block, id),
	}, id)
	require.NoError(t, err)
}

func InjectTestNotarization(t *testing.T, e *simplex.Epoch, notarization simplex.Notarization, id simplex.NodeID) {
	err := e.HandleMessage(&simplex.Message{
		Notarization: &notarization,
	}, id)
	require.NoError(t, err)
}

type testQCDeserializer struct {
	t *testing.T
}

func (t *testQCDeserializer) DeserializeQuorumCertificate(bytes []byte) (simplex.QuorumCertificate, error) {
	var qc []simplex.Signature
	rest, err := asn1.Unmarshal(bytes, &qc)
	require.NoError(t.t, err)
	require.Empty(t.t, rest)
	return TestQC(qc), err
}

type TestSignatureAggregator struct {
	Err error
}

func (t *TestSignatureAggregator) Aggregate(signatures []simplex.Signature) (simplex.QuorumCertificate, error) {
	return TestQC(signatures), t.Err
}

type TestQC []simplex.Signature

func (t TestQC) Signers() []simplex.NodeID {
	res := make([]simplex.NodeID, 0, len(t))
	for _, sig := range t {
		res = append(res, sig.Signer)
	}
	return res
}

func (t TestQC) Verify(msg []byte) error {
	return nil
}

func (t TestQC) Bytes() []byte {
	bytes, err := asn1.Marshal(t)
	if err != nil {
		panic(err)
	}
	return bytes
}

type testSigner struct {
}

func (t *testSigner) Sign([]byte) ([]byte, error) {
	return []byte{1, 2, 3}, nil
}

type testVerifier struct {
}

func (t *testVerifier) VerifyBlock(simplex.VerifiedBlock) error {
	return nil
}

func (t *testVerifier) Verify(_ []byte, _ []byte, _ simplex.NodeID) error {
	return nil
}

func WaitToEnterRound(t *testing.T, e *simplex.Epoch, round uint64) {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if e.Metadata().Round >= round {
			return
		}

		select {
		case <-time.After(time.Millisecond * 10):
			continue
		case <-timeout.C:
			require.Fail(t, "timed out waiting to enter round", "current round %d, waiting for round %d", e.Metadata().Round, round)
		}
	}
}

func WaitForBlockProposerTimeout(t *testing.T, e *simplex.Epoch, startTime *time.Time, startRound uint64) {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if e.WAL.(*TestWAL).ContainsEmptyVote(startRound) || e.WAL.(*TestWAL).ContainsEmptyNotarization(startRound) {
			return
		}

		// if we are expected to time out for this round, we should not have a notarization
		require.False(t, e.WAL.(*TestWAL).ContainsNotarization(startRound))

		*startTime = startTime.Add(e.EpochConfig.MaxProposalWait / 5)
		e.AdvanceTime(*startTime)
		select {
		case <-time.After(time.Millisecond * 10):
			continue
		case <-timeout.C:
			require.Fail(t, "timed out waiting for event")
		}
	}
}
