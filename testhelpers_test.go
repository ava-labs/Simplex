package simplex

import (
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
)

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

	snapLock sync.Mutex
	blocks   map[uint64]storedBlock
}

type storedBlock struct {
	rawBlock []byte
	fin      common.Finalization
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

type testInnerBlockDeserializer struct{}

func (ibd *testInnerBlockDeserializer) ParseBlock(_ context.Context, buff []byte) (avalanchego.VMBlock, error) {
	b := &testInnerBlock{}
	b.Height_ = binary.BigEndian.Uint64(buff[0:8])
	b.TS = time.UnixMilli(int64(binary.BigEndian.Uint64(buff[8:16])))
	b.Payload = append([]byte(nil), buff[16:]...)
	return b, nil
}

func (m *MockStorage) CreateWAL() (wal.DeletableWAL, error) {
	return testutil.NewTestWAL(m.t), nil
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
