package testutil

import (
	"encoding/asn1"
	"encoding/binary"
	"simplex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testNode struct {
	Storage *InMemStorage
	E       *simplex.Epoch
	Wal     *TestWAL
	ingress chan struct {
		msg  *simplex.Message
		from simplex.NodeID
	}
	t *testing.T
}

func (t *testNode) Start() {
	go t.handleMessages()
	require.NoError(t.t, t.E.Start())
}

type TestNodeConfig struct {
	// optional
	InitialStorage     []simplex.VerifiedFinalizedBlock
	Comm               simplex.Communication
	ReplicationEnabled bool
}

// NewSimplexNode creates a new testNode and adds it to [net].
func NewSimplexNode(t *testing.T, nodeID simplex.NodeID, net *inMemNetwork, bb simplex.BlockBuilder, config *TestNodeConfig) *testNode {
	comm := NewTestComm(nodeID, net, AllowAllMessages)

	epochConfig, _, _ := DefaultTestNodeEpochConfig(t, nodeID, comm, bb)

	if config != nil {
		updateEpochConfig(&epochConfig, config)
	}

	e, err := simplex.NewEpoch(epochConfig)
	require.NoError(t, err)
	ti := &testNode{
		Wal:     epochConfig.WAL.(*TestWAL),
		E:       e,
		t:       t,
		Storage: epochConfig.Storage.(*InMemStorage),
		ingress: make(chan struct {
			msg  *simplex.Message
			from simplex.NodeID
		}, 100)}

	net.addNode(ti)
	return ti
}

func updateEpochConfig(epochConfig *simplex.EpochConfig, testConfig *TestNodeConfig) {
	// set the initial storage
	for _, data := range testConfig.InitialStorage {
		epochConfig.Storage.Index(data.VerifiedBlock, data.FCert)
	}

	// TODO: remove optional replication flag
	epochConfig.ReplicationEnabled = testConfig.ReplicationEnabled

	// custom communication
	if testConfig.Comm != nil {
		epochConfig.Comm = testConfig.Comm
	}
}

func DefaultTestNodeEpochConfig(t *testing.T, nodeID simplex.NodeID, comm simplex.Communication, bb simplex.BlockBuilder) (simplex.EpochConfig, *TestWAL, *InMemStorage) {
	l := MakeLogger(t, int(nodeID[0]))
	storage := NewInMemStorage()
	wal := newTestWAL(t)
	conf := simplex.EpochConfig{
		MaxProposalWait:     simplex.DefaultMaxProposalWaitTime,
		Logger:              l,
		ID:                  nodeID,
		Signer:              &TestSigner{},
		WAL:                 newTestWAL(t),
		Verifier:            &testVerifier{},
		Storage:             storage,
		Comm:                comm,
		BlockBuilder:        bb,
		SignatureAggregator: &TestSignatureAggregator{},
		BlockDeserializer:   &blockDeserializer{},
		QCDeserializer:      &testQCDeserializer{t: t},
		StartTime:           time.Now(),
	}
	return conf, wal, storage
}

func (t *testNode) HandleMessage(msg *simplex.Message, from simplex.NodeID) error {
	err := t.E.HandleMessage(msg, from)
	require.NoError(t.t, err)
	return err
}

func (t *testNode) handleMessages() {
	for msg := range t.ingress {
		err := t.HandleMessage(msg.msg, msg.from)
		require.NoError(t.t, err)
		if err != nil {
			return
		}
	}
}

func (n *testNode) WaitToEnterRound(round uint64) {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if n.E.Metadata().Round >= round {
			return
		}

		select {
		case <-time.After(time.Millisecond * 10):
			continue
		case <-timeout.C:
			require.Fail(n.t, "timed out waiting for event")
		}
	}
}

type TestSigner struct {
}

func (t *TestSigner) Sign([]byte) ([]byte, error) {
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

type blockDeserializer struct {
}

func (b *blockDeserializer) DeserializeBlock(buff []byte) (simplex.VerifiedBlock, error) {
	blockLen := binary.BigEndian.Uint32(buff[:4])
	bh := simplex.BlockHeader{}
	if err := bh.FromBytes(buff[4+blockLen:]); err != nil {
		return nil, err
	}

	tb := TestBlock{
		data:     buff[4 : 4+blockLen],
		metadata: bh.ProtocolMetadata,
	}

	tb.computeDigest()

	return &tb, nil
}

func TestBlockDeserializer(t *testing.T) {
	var blockDeserializer blockDeserializer

	tb := NewTestBlock(simplex.ProtocolMetadata{Seq: 1, Round: 2, Epoch: 3})
	tb2, err := blockDeserializer.DeserializeBlock(tb.Bytes())
	require.NoError(t, err)
	require.Equal(t, tb, tb2)
}

func (n *testNode) waitForBlockProposerTimeout(startTime *time.Time, startRound uint64) {
	WaitForBlockProposerTimeout(n.t, n.E, startTime, startRound)
}

func WaitForBlockProposerTimeout(t *testing.T, e *simplex.Epoch, startTime *time.Time, startRound uint64) {
	timeout := time.NewTimer(time.Minute)
	defer timeout.Stop()

	for {
		if e.WAL.(*TestWAL).containsEmptyVote(startRound) || e.WAL.(*TestWAL).containsEmptyNotarization(startRound) {
			return
		}
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
