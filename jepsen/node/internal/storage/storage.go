// Package storage provides a bbolt-backed implementation of common.Storage.
//   - Tracks lastIndexedDigest for digest validation.
//   - Stores blocks and finalizations in separate bbolt buckets.
//   - Uses atomic.Uint64 for numBlocks.
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.etcd.io/bbolt"

	"github.com/ava-labs/simplex/common"
)

var (
	bucketBlocks = []byte("blocks")
	bucketFins   = []byte("fins")

	errUnexpectedSeq        = errors.New("unexpected sequence number")
	errMismatchedPrevDigest = errors.New("mismatched previous digest")
	errMismatchedDigest     = errors.New("mismatched digest in finalization")
	errInvalidQC            = errors.New("invalid quorum certificate")
)

// Storage implements common.Storage backed by a bbolt database.
type Storage struct {
	db  *bbolt.DB
	bd  common.BlockDeserializer
	qd  common.QCDeserializer
	mu  sync.RWMutex

	numBlocks         atomic.Uint64
	lastIndexedDigest common.Digest // Inspired by AvalancheGo storage.go
}

// New opens (or creates) a bbolt database at dir/simplex.db and recovers the
// numBlocks counter by scanning the blocks bucket.
func New(dir string, bd common.BlockDeserializer, qd common.QCDeserializer) (*Storage, error) {
	db, err := bbolt.Open(dir+"/simplex.db", 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("storage: open bbolt: %w", err)
	}

	// Ensure buckets exist.
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketBlocks); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(bucketFins)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: create buckets: %w", err)
	}

	s := &Storage{db: db, bd: bd, qd: qd}

	// Recover numBlocks by counting keys in the blocks bucket and re-deriving
	// the lastIndexedDigest from the last stored block.
	var count uint64
	var lastDigest common.Digest
	if err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketBlocks)
		c := b.Cursor()
		lastKey, lastVal := c.Last()
		if lastKey == nil {
			// Empty bucket = nothing to recover.
			return nil
		}
		// Count total keys.
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		// Deserialize the last block to recover its digest.
		blk, err := bd.DeserializeBlock(context.Background(), lastVal)
		if err != nil {
			return fmt.Errorf("storage: deserialize last block: %w", err)
		}
		lastDigest = blk.BlockHeader().Digest
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: recover state: %w", err)
	}
	s.numBlocks.Store(count)
	s.lastIndexedDigest = lastDigest

	return s, nil
}

// Close closes the underlying bbolt database.
func (s *Storage) Close() error {
	return s.db.Close()
}

// NumBlocks returns the number of blocks stored.
func (s *Storage) NumBlocks() uint64 {
	return s.numBlocks.Load()
}

// Index stores a block and its finalization.
// Validates sequence and digest chaining, inspired by AvalancheGo's storage.go.
func (s *Storage) Index(_ context.Context, block common.VerifiedBlock, finalization common.Finalization) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bh := block.BlockHeader()
	numBlocks := s.numBlocks.Load()

	// Validate sequence ordering.
	if numBlocks != bh.Seq {
		return fmt.Errorf("%w: expected %d, got %d", errUnexpectedSeq, numBlocks, bh.Seq)
	}

	// Validate digest chain (inspired by AvalancheGo).
	if s.lastIndexedDigest != bh.Prev {
		return fmt.Errorf("%w: expected %s, got %s", errMismatchedPrevDigest, s.lastIndexedDigest, bh.Prev)
	}

	// Validate finalization digest matches block digest.
	if bh.Digest != finalization.Finalization.Digest {
		return fmt.Errorf("%w: block %s, finalization %s", errMismatchedDigest, bh.Digest, finalization.Finalization.Digest)
	}

	if finalization.QC == nil {
		return errInvalidQC
	}

	blockBytes, err := block.Bytes()
	if err != nil {
		return fmt.Errorf("storage: serialize block: %w", err)
	}

	finBytes := encodeFinalization(finalization)
	key := seqKey(bh.Seq)

	if err := s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket(bucketBlocks).Put(key, blockBytes); err != nil {
			return err
		}
		return tx.Bucket(bucketFins).Put(key, finBytes)
	}); err != nil {
		return fmt.Errorf("storage: write to bbolt: %w", err)
	}

	s.numBlocks.Add(1)
	s.lastIndexedDigest = bh.Digest
	return nil
}

// Retrieve returns the block and finalization at seq.
// Returns common.ErrBlockNotFound if seq is not present.
func (s *Storage) Retrieve(seq uint64) (common.VerifiedBlock, common.Finalization, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := seqKey(seq)

	var blockBytes []byte
	var finBytes []byte

	if err := s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketBlocks).Get(key)
		if v == nil {
			return common.ErrBlockNotFound
		}
		blockBytes = make([]byte, len(v))
		copy(blockBytes, v)

		f := tx.Bucket(bucketFins).Get(key)
		if f == nil {
			return fmt.Errorf("finalization not found for seq %d", seq)
		}
		finBytes = make([]byte, len(f))
		copy(finBytes, f)
		return nil
	}); err != nil {
		if errors.Is(err, common.ErrBlockNotFound) {
			return nil, common.Finalization{}, common.ErrBlockNotFound
		}
		return nil, common.Finalization{}, err
	}

	blk, err := s.bd.DeserializeBlock(context.Background(), blockBytes)
	if err != nil {
		return nil, common.Finalization{}, fmt.Errorf("storage: deserialize block seq %d: %w", seq, err)
	}
	vb, err := blk.Verify(context.Background())
	if err != nil {
		return nil, common.Finalization{}, fmt.Errorf("storage: verify block seq %d: %w", seq, err)
	}

	fin, err := decodeFinalization(finBytes, s.qd)
	if err != nil {
		return nil, common.Finalization{}, fmt.Errorf("storage: deserialize finalization seq %d: %w", seq, err)
	}

	return vb, fin, nil
}

// seqKey encodes a uint64 sequence number as an 8-byte big-endian key.
func seqKey(seq uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, seq)
	return key
}

// encodeFinalization serializes a Finalization as:
// [4-byte header length][header bytes][QC bytes]
func encodeFinalization(fin common.Finalization) []byte {
	hdrBytes := fin.Finalization.Bytes()
	qcBytes := fin.QC.Bytes()
	buf := make([]byte, 4+len(hdrBytes)+len(qcBytes))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(hdrBytes)))
	copy(buf[4:], hdrBytes)
	copy(buf[4+len(hdrBytes):], qcBytes)
	return buf
}

// decodeFinalization deserializes bytes produced by encodeFinalization.
func decodeFinalization(data []byte, qd common.QCDeserializer) (common.Finalization, error) {
	if len(data) < 4 {
		return common.Finalization{}, fmt.Errorf("finalization data too short")
	}
	hdrLen := int(binary.BigEndian.Uint32(data[0:4]))
	if len(data) < 4+hdrLen {
		return common.Finalization{}, fmt.Errorf("finalization data truncated at header")
	}
	hdrBytes := data[4 : 4+hdrLen]
	qcBytes := data[4+hdrLen:]

	var tbsf common.ToBeSignedFinalization
	if err := tbsf.FromBytes(hdrBytes); err != nil {
		return common.Finalization{}, fmt.Errorf("decode finalization header: %w", err)
	}
	qc, err := qd.DeserializeQuorumCertificate(qcBytes)
	if err != nil {
		return common.Finalization{}, fmt.Errorf("decode finalization QC: %w", err)
	}
	return common.Finalization{
		Finalization: tbsf,
		QC:           qc,
	}, nil
}
