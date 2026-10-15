// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package wal_test

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/ava-labs/simplex/common"
	"github.com/ava-labs/simplex/testutil"
	wal "github.com/ava-labs/simplex/wal"
	"github.com/stretchr/testify/require"
)

// mockWALReader reads records laid out as 8 bytes of round followed by 8 bytes of epoch.
// The garbage collection tests only ever read the round, so their records carry random
// bytes past the round; makeEpochRecord builds records that carry both.
type mockWALReader struct{}

func (m *mockWALReader) Round(entry []byte) (uint64, error) {
	if len(entry) < 8 {
		return 0, errRecordTooShort
	}
	return binary.BigEndian.Uint64(entry[:8]), nil
}

func (m *mockWALReader) Epoch(entry []byte) (uint64, error) {
	if len(entry) < 16 {
		return 0, errRecordTooShort
	}
	return binary.BigEndian.Uint64(entry[8:16]), nil
}

var errRecordTooShort = errors.New("record too short")

// makeRecord builds a payload of exactly `size` bytes whose first 8 bytes
// encode the round (as read back by mockWALReader).
func makeRecord(t *testing.T, round uint64, size int) []byte {
	require.GreaterOrEqual(t, size, 8)
	buff := make([]byte, size)
	binary.BigEndian.PutUint64(buff[:8], round)
	_, err := rand.Read(buff[8:])
	require.NoError(t, err)
	return buff
}

// makeEpochRecord builds a 16 byte payload encoding the given round and epoch,
// as read back by mockWALReader.
func makeEpochRecord(round, epoch uint64) []byte {
	buff := make([]byte, 16)
	binary.BigEndian.PutUint64(buff[:8], round)
	binary.BigEndian.PutUint64(buff[8:16], epoch)
	return buff
}

// countingCreator returns a Creator that records how many WALs it has created
// and hands back a pointer to the running count.
func countingCreator(t *testing.T) (wal.Creator, *int) {
	count := new(int)
	return func() (wal.DeletableWAL, error) {
		*count++
		return testutil.NewTestWAL(t), nil
	}, count
}

func TestGarbageCollectedWAL(t *testing.T) {

	var testWALs []wal.DeletableWAL
	records := make([][]byte, 10)

	for i := uint64(0); i < 10; i++ {
		buff := make([]byte, 16)
		binary.BigEndian.PutUint64(buff[:8], i)
		_, err := rand.Read(buff[8:])
		require.NoError(t, err)

		testWAL := testutil.NewTestWAL(t)
		require.NoError(t, testWAL.Append(buff))
		testWALs = append(testWALs, testWAL)
		records[i] = buff
	}

	var newWALCreatedCount int

	gcw, err := wal.NewGarbageCollectedWAL(testWALs, func() (wal.DeletableWAL, error) {
		newWALCreatedCount++
		return testutil.NewTestWAL(t), nil
	}, &mockWALReader{}, 100)
	require.NoError(t, err)

	walRecords, err := gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records, walRecords)

	require.NoError(t, gcw.GarbageCollect(6))
	walRecords, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records[6:], walRecords)

	require.NoError(t, gcw.GarbageCollect(10))
	walRecords, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Empty(t, walRecords)

	buff := make([]byte, 16)
	binary.BigEndian.PutUint64(buff[:8], 100)
	_, err = rand.Read(buff[8:])
	require.NoError(t, err)

	require.Zero(t, newWALCreatedCount)
	require.NoError(t, gcw.Append(buff))
	require.Equal(t, 1, newWALCreatedCount)
	walRecords, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]byte{buff}, walRecords)

	buff1 := buff
	buff = make([]byte, 1024)
	binary.BigEndian.PutUint64(buff[:8], 100)
	_, err = rand.Read(buff[8:])
	require.NoError(t, err)

	buffs := make([][]byte, 2)
	buffs[0] = buff1
	buffs[1] = buff

	require.Equal(t, 1, newWALCreatedCount)
	require.NoError(t, gcw.Append(buff))
	walRecords, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, 2, newWALCreatedCount)
	require.Equal(t, buffs, walRecords)
}

func TestGarbageCollectedWALRotatesBySize(t *testing.T) {
	// WAL rotates when the total number of records exceed maxWALSize.
	// Each WAL is expected to contain 2 records, so 6 records should result in 3 WALs.
	const (
		maxWALSize = 100
		recordSize = 40
		numRecords = 6
	)

	creator, newWALCount := countingCreator(t)

	gcw, err := wal.NewGarbageCollectedWAL(nil, creator, &mockWALReader{}, maxWALSize)
	require.NoError(t, err)

	records := make([][]byte, numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = makeRecord(t, uint64(i), recordSize)
		require.NoError(t, gcw.Append(records[i]))
	}

	// Each WAL fits two 40-byte records: 40+40=80 <= 100, but 80+40=120 > 100
	// forces a rotation. So 6 records land in 3 WALs.
	require.Equal(t, 3, *newWALCount)

	// Every record must still be readable, in the original append order,
	// across the rotated WALs.
	got, err := gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records, got)
}

func TestGarbageCollectedWALRotatesOnOversizedPayload(t *testing.T) {
	// WAL rotates if a really big record is inserted.
	const maxWALSize = 100

	creator, newWALCount := countingCreator(t)

	gcw, err := wal.NewGarbageCollectedWAL(nil, creator, &mockWALReader{}, maxWALSize)
	require.NoError(t, err)

	small := makeRecord(t, 0, 16)
	require.NoError(t, gcw.Append(small))
	require.Equal(t, 1, *newWALCount)

	// A single record bigger than maxWALSize rotates into its own WAL.
	big := makeRecord(t, 1, maxWALSize+50)
	require.NoError(t, gcw.Append(big))
	require.Equal(t, 2, *newWALCount)

	got, err := gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]byte{small, big}, got)
}

func TestGarbageCollectedWALGarbageCollectsRotatedWALs(t *testing.T) {
	// Verifies a WAL is removed only once its highest round drops below the cutoff.
	const (
		maxWALSize = 100
		recordSize = 40
		numRecords = 6
	)

	creator, newWALCount := countingCreator(t)

	gcw, err := wal.NewGarbageCollectedWAL(nil, creator, &mockWALReader{}, maxWALSize)
	require.NoError(t, err)

	// Rounds 0..5, two per WAL:
	//   WAL0 -> {0,1} (max 1), WAL1 -> {2,3} (max 3), WAL2 -> {4,5} (max 5)
	records := make([][]byte, numRecords)
	for rt := 0; rt < numRecords; rt++ {
		records[rt] = makeRecord(t, uint64(rt), recordSize)
		require.NoError(t, gcw.Append(records[rt]))
	}
	require.Equal(t, 3, *newWALCount) // 6 records, two per WAL -> 3 WALs.

	// Does not drop any WALs because the cutoff is below the max round of WAL0.
	require.NoError(t, gcw.GarbageCollect(1))
	got, err := gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records, got)

	// Drop WAL 0 (max retention 1 < 2) but keeps WAL1 and WAL2.
	require.NoError(t, gcw.GarbageCollect(2))
	got, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records[2:], got)

	// Does not drop WAL1 because the cutoff is equal to the max round of WAL1.
	require.NoError(t, gcw.GarbageCollect(3))
	got, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records[2:], got)

	// Cutoff 4 drops WAL1 (max retention 3 < 4).
	require.NoError(t, gcw.GarbageCollect(4))
	got, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records[4:], got)

	// Cutoff 6 drops the last WAL.
	require.NoError(t, gcw.GarbageCollect(6))
	got, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Empty(t, got)

	// No new WALs were created by GC or ReadAll.
	require.Equal(t, 3, *newWALCount)
}

func TestGarbageCollectedWALAppendOutOfOrder(t *testing.T) {
	walRecordBlock := func(round uint64) *testutil.TestBlock {
		return testutil.NewTestBlock(common.ProtocolMetadata{Round: round, Seq: round}, common.Blacklist{})
	}
	nodes := []common.NodeID{{1}, {2}, {3}, {4}}
	sigAggr := &testutil.TestSignatureAggregator{N: len(nodes)}

	blockRecord := func(round uint64) []byte {
		b := walRecordBlock(round)
		raw := b.Bytes()
		bytes, err := common.BlockRecord(b.BlockHeader(), raw)
		require.NoError(t, err)
		return bytes
	}
	notarizationRecord := func(round uint64) []byte {
		rec, err := testutil.NewNotarizationRecord(testutil.MakeLogger(t), sigAggr, walRecordBlock(round), nodes)
		require.NoError(t, err)
		return rec
	}
	finalizationRecord := func(round uint64) []byte {
		_, rec := testutil.NewFinalizationRecord(t, sigAggr, walRecordBlock(round), nodes)
		return rec
	}

	// We create a block record for 4 and a finalization record for 3 out of order.
	// The WALReader must be able to read the round from every
	// record type, or GarbageCollectedWAL.Append will fail on the finalization.
	records := [][]byte{
		blockRecord(1), notarizationRecord(1),
		blockRecord(2), notarizationRecord(2),
		blockRecord(3), notarizationRecord(3),
		blockRecord(4),
		finalizationRecord(3),
	}

	// A small maxWALSize forces the stream across several WAL segments so ReadAll must stitch every segment back together.
	creator, newWALCount := countingCreator(t)
	walSize := 512
	gcw, err := wal.NewGarbageCollectedWAL(nil, creator, &common.WALReader{}, walSize)
	require.NoError(t, err)

	var sum int
	for _, rec := range records {
		sum += len(rec)
		require.NoError(t, gcw.Append(rec))
	}
	require.Equal(t, *newWALCount, sum/walSize+1)

	// Every record — including the out-of-order Finalization — reads back in the
	// order it was appended.
	got, err := gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records, got)
}

func TestGarbageCollectedWALAppendAfterRotationAndGC(t *testing.T) {
	// Verifies that appends resume  correctly (and keep rotating)
	// after the WAL has been fully garbage collected down to empty.
	const (
		maxWALSize = 100
		recordSize = 40
	)

	creator, newWALCount := countingCreator(t)

	gcw, err := wal.NewGarbageCollectedWAL(nil, creator, &mockWALReader{}, maxWALSize)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, gcw.Append(makeRecord(t, uint64(i), recordSize)))
	}
	// 4 records, two per WAL -> 2 WALs.
	require.Equal(t, 2, *newWALCount)

	// GC everything away.
	require.NoError(t, gcw.GarbageCollect(100))
	got, err := gcw.ReadAll()
	require.NoError(t, err)
	require.Empty(t, got)

	// Appending again must create a fresh WAL and continue rotating.
	next := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		next[i] = makeRecord(t, uint64(200+i), recordSize)
		require.NoError(t, gcw.Append(next[i]))
	}
	// First append starts a new WAL; the third crosses maxWALSize -> 2 more WALs.
	require.Equal(t, 4, *newWALCount)

	got, err = gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, next, got)
}

// newEpochAwareWAL wraps a fresh garbage collected WAL that hides records of epochs below lowestEpoch.
func newEpochAwareWAL(t *testing.T, lowestEpoch uint64) (*wal.EpochAwareWAL, *wal.GarbageCollectedWAL) {
	creator, _ := countingCreator(t)
	gcw, err := wal.NewGarbageCollectedWAL(nil, creator, &mockWALReader{}, 100)
	require.NoError(t, err)

	return &wal.EpochAwareWAL{
		GarbageCollectableWAL: gcw,
		Reader:                &mockWALReader{},
		LowestEpoch:           lowestEpoch,
	}, gcw
}

func TestEpochAwareWALFiltersOlderEpochs(t *testing.T) {
	// Rounds are not unique across epochs: the records of epoch 4 at rounds 3 and 4 collide
	// with the records of epoch 5 at the same rounds, so only the epoch can tell them apart.
	oldEpochs := [][]byte{
		makeEpochRecord(1, 3),
		makeEpochRecord(2, 3),
		makeEpochRecord(3, 4),
		makeEpochRecord(4, 4),
	}
	currentEpoch := [][]byte{
		makeEpochRecord(3, 5),
		makeEpochRecord(4, 5),
	}
	newerEpoch := [][]byte{
		makeEpochRecord(5, 6),
	}

	eaw, gcw := newEpochAwareWAL(t, 5)
	for _, record := range slices.Concat(oldEpochs, currentEpoch, newerEpoch) {
		require.NoError(t, eaw.Append(record))
	}

	// Reading through the wrapper hides the older epochs and preserves the append order of the rest.
	got, err := eaw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, slices.Concat(currentEpoch, newerEpoch), got)

	// The wrapper only filters what is read; the underlying WAL still holds every record.
	all, err := gcw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, slices.Concat(oldEpochs, currentEpoch, newerEpoch), all)
}

func TestEpochAwareWALLowestEpochZeroKeepsEverything(t *testing.T) {
	records := [][]byte{
		makeEpochRecord(1, 0),
		makeEpochRecord(2, 1),
		makeEpochRecord(3, 7),
	}

	eaw, _ := newEpochAwareWAL(t, 0)
	for _, record := range records {
		require.NoError(t, eaw.Append(record))
	}

	got, err := eaw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records, got)
}

func TestEpochAwareWALReadAllEmpty(t *testing.T) {
	eaw, _ := newEpochAwareWAL(t, 5)

	got, err := eaw.ReadAll()
	require.NoError(t, err)
	require.Empty(t, got)

	// A WAL holding only older epochs reads back as empty too.
	require.NoError(t, eaw.Append(makeEpochRecord(1, 4)))
	got, err = eaw.ReadAll()
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestEpochAwareWALGarbageCollectsByRound(t *testing.T) {
	// Garbage collection is keyed on rounds and filtering on epochs; the two compose.
	eaw, _ := newEpochAwareWAL(t, 5)

	// A tiny WAL size rotates upon every record, so garbage collection can drop records one by one.
	eaw.GarbageCollectableWAL = func() *wal.GarbageCollectedWAL {
		creator, _ := countingCreator(t)
		gcw, err := wal.NewGarbageCollectedWAL(nil, creator, &mockWALReader{}, 16)
		require.NoError(t, err)
		return gcw
	}()

	records := [][]byte{
		makeEpochRecord(1, 4),
		makeEpochRecord(2, 5),
		makeEpochRecord(3, 4),
		makeEpochRecord(3, 5),
		makeEpochRecord(4, 5),
	}
	for _, record := range records {
		require.NoError(t, eaw.Append(record))
	}

	// Collecting below round 3 removes the records of rounds 1 and 2, whatever their epoch.
	require.NoError(t, eaw.GarbageCollect(3))
	got, err := eaw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]byte{makeEpochRecord(3, 5), makeEpochRecord(4, 5)}, got)

	// The older epoch's record of round 3 was garbage collected too, it just was never visible.
	all, err := eaw.GarbageCollectableWAL.ReadAll()
	require.NoError(t, err)
	require.Equal(t, records[2:], all)
}

func TestEpochAwareWALPropagatesReaderError(t *testing.T) {
	eaw, _ := newEpochAwareWAL(t, 5)

	// The record is long enough to carry a round, which Append needs, but not an epoch.
	require.NoError(t, eaw.Append(makeRecord(t, 1, 8)))

	_, err := eaw.ReadAll()
	require.ErrorIs(t, err, errRecordTooShort)
}

// failingWAL fails every read with the given error.
type failingWAL struct {
	wal.GarbageCollectableWAL
	err error
}

func (f *failingWAL) ReadAll() ([][]byte, error) {
	return nil, f.err
}

func TestEpochAwareWALPropagatesReadAllError(t *testing.T) {
	sentinel := errors.New("disk on fire")
	eaw, gcw := newEpochAwareWAL(t, 5)
	eaw.GarbageCollectableWAL = &failingWAL{GarbageCollectableWAL: gcw, err: sentinel}

	_, err := eaw.ReadAll()
	require.ErrorIs(t, err, sentinel)
}

func TestEpochAwareWALWithRealRecords(t *testing.T) {
	// Exercises the wrapper with the real record encodings and reader, across every record type.
	// Epoch 2 was sealed at round 5, but its Telocks kept it going at rounds 6 and 7 while the
	// sealing block finalized. Epoch 3 starts at round 6, reusing those rounds.
	const (
		sealedEpoch = 2
		newEpoch    = 3
	)

	nodes := []common.NodeID{{1}, {2}, {3}, {4}}
	sigAggr := &testutil.TestSignatureAggregator{N: len(nodes)}

	block := func(epoch, round uint64) *testutil.TestBlock {
		return testutil.NewTestBlock(common.ProtocolMetadata{Epoch: epoch, Round: round, Seq: round}, common.Blacklist{})
	}
	blockRecord := func(epoch, round uint64) []byte {
		b := block(epoch, round)
		record, err := common.BlockRecord(b.BlockHeader(), b.Bytes())
		require.NoError(t, err)
		return record
	}
	notarizationRecord := func(epoch, round uint64) []byte {
		record, err := testutil.NewNotarizationRecord(testutil.MakeLogger(t), sigAggr, block(epoch, round), nodes)
		require.NoError(t, err)
		return record
	}
	finalizationRecord := func(epoch, round uint64) []byte {
		_, record := testutil.NewFinalizationRecord(t, sigAggr, block(epoch, round), nodes)
		return record
	}
	emptyNotarizationRecord := func(epoch, round uint64) []byte {
		en := testutil.NewEmptyNotarization(nodes, round)
		en.Vote.Epoch = epoch
		return common.NewEmptyNotarizationRecord(en)
	}
	emptyVoteRecord := func(epoch, round uint64) []byte {
		return common.NewEmptyVoteRecord(common.ToBeSignedEmptyVote{
			EmptyVoteMetadata: common.EmptyVoteMetadata{Epoch: epoch, Round: round},
		})
	}

	sealedEpochRecords := [][]byte{
		blockRecord(sealedEpoch, 5), notarizationRecord(sealedEpoch, 5), finalizationRecord(sealedEpoch, 5),
		blockRecord(sealedEpoch, 6), notarizationRecord(sealedEpoch, 6),
		emptyVoteRecord(sealedEpoch, 7), emptyNotarizationRecord(sealedEpoch, 7),
	}
	newEpochRecords := [][]byte{
		emptyVoteRecord(newEpoch, 6), emptyNotarizationRecord(newEpoch, 6),
		blockRecord(newEpoch, 7), notarizationRecord(newEpoch, 7), finalizationRecord(newEpoch, 7),
	}

	creator, _ := countingCreator(t)
	gcw, err := wal.NewGarbageCollectedWAL(nil, creator, &common.WALReader{}, 512)
	require.NoError(t, err)
	eaw := &wal.EpochAwareWAL{
		GarbageCollectableWAL: gcw,
		Reader:                &common.WALReader{},
		LowestEpoch:           newEpoch,
	}

	for _, record := range slices.Concat(sealedEpochRecords, newEpochRecords) {
		require.NoError(t, eaw.Append(record))
	}

	got, err := eaw.ReadAll()
	require.NoError(t, err)
	require.Equal(t, newEpochRecords, got)
}
