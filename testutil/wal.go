// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/record"
	"github.com/ava-labs/simplex/wal"
	"github.com/stretchr/testify/require"
)

type TestWAL struct {
	simplex.WriteAheadLog
	t      *testing.T
	lock   sync.Mutex
	signal sync.Cond
}

func NewTestWAL(t *testing.T) *TestWAL {
	var tw TestWAL
	tw.WriteAheadLog = wal.NewMemWAL(t)
	tw.signal = sync.Cond{L: &tw.lock}
	tw.t = t
	return &tw
}

func (tw *TestWAL) Clone() *TestWAL {
	rawWAL, err := tw.ReadAll()
	require.NoError(tw.t, err)

	wal := NewTestWAL(tw.t)

	for _, entry := range rawWAL {
		wal.Append(entry)
	}

	return wal
}

func (tw *TestWAL) Delete() error {
	return nil
}

func (tw *TestWAL) Append(b []byte) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	err := tw.WriteAheadLog.Append(b)
	tw.signal.Signal()
	return err
}

func (tw *TestWAL) ReadAll() ([][]byte, error) {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	return tw.WriteAheadLog.ReadAll()
}

func (tw *TestWAL) AssertWALSize(n int) {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	for {
		rawRecords, err := tw.WriteAheadLog.ReadAll()
		require.NoError(tw.t, err)

		if len(rawRecords) == n {
			return
		}

		tw.signal.Wait()
	}
}

func (tw *TestWAL) AssertNotarization(round uint64) uint16 {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	for {
		rawRecords, err := tw.WriteAheadLog.ReadAll()
		require.NoError(tw.t, err)

		for _, rawRecord := range rawRecords {
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.NotarizationRecordType {
				_, vote, err := simplex.ParseNotarizationRecord(rawRecord)
				require.NoError(tw.t, err)

				if vote.Round == round {
					return record.NotarizationRecordType
				}
			}
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyNotarizationRecordType {
				_, vote, err := simplex.ParseEmptyNotarizationRecord(rawRecord)
				require.NoError(tw.t, err)

				if vote.Round == round {
					return record.EmptyNotarizationRecordType
				}
			}
		}

		tw.signal.Wait()
	}

}

func (tw *TestWAL) AssertEmptyVote(round uint64) {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	for {
		rawRecords, err := tw.WriteAheadLog.ReadAll()
		require.NoError(tw.t, err)

		for _, rawRecord := range rawRecords {
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyVoteRecordType {
				vote, err := simplex.ParseEmptyVoteRecord(rawRecord)
				require.NoError(tw.t, err)

				if vote.Round == round {
					return
				}
			}
		}

		tw.signal.Wait()
	}

}

func (tw *TestWAL) AssertBlockProposal(round uint64) {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	for {
		rawRecords, err := tw.WriteAheadLog.ReadAll()
		require.NoError(tw.t, err)

		for _, rawRecord := range rawRecords {
			if binary.BigEndian.Uint16(rawRecord[:2]) == record.BlockRecordType {
				bh, _, err := simplex.ParseBlockRecord(rawRecord)
				require.NoError(tw.t, err)

				if bh.Round == round {
					return
				}
			}
		}

		tw.signal.Wait()
	}

}

func (tw *TestWAL) ContainsNotarization(round uint64) bool {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	rawRecords, err := tw.WriteAheadLog.ReadAll()
	require.NoError(tw.t, err)

	for _, rawRecord := range rawRecords {
		if binary.BigEndian.Uint16(rawRecord[:2]) == record.NotarizationRecordType {
			_, vote, err := simplex.ParseNotarizationRecord(rawRecord)
			require.NoError(tw.t, err)

			if vote.Round == round {
				return true
			}
		}
	}

	return false
}

func (tw *TestWAL) ContainsEmptyVote(round uint64) bool {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	rawRecords, err := tw.WriteAheadLog.ReadAll()
	require.NoError(tw.t, err)

	for _, rawRecord := range rawRecords {
		if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyVoteRecordType {
			vote, err := simplex.ParseEmptyVoteRecord(rawRecord)
			require.NoError(tw.t, err)

			if vote.Round == round {
				return true
			}
		}
	}

	return false
}

func (tw *TestWAL) ContainsEmptyNotarization(round uint64) bool {
	tw.lock.Lock()
	defer tw.lock.Unlock()

	rawRecords, err := tw.WriteAheadLog.ReadAll()
	require.NoError(tw.t, err)

	for _, rawRecord := range rawRecords {
		if binary.BigEndian.Uint16(rawRecord[:2]) == record.EmptyNotarizationRecordType {
			_, vote, err := simplex.ParseEmptyNotarizationRecord(rawRecord)
			require.NoError(tw.t, err)

			if vote.Round == round {
				return true
			}
		}
	}

	return false
}

type walRound struct {
	round                   uint64
	blockRecord             bool
	notarizationRecord      bool
	finalizationRecord      bool
	emptyNotarizationRecord bool
	emptyVoteRecord         bool
}

// AssertHealthy checks that the WAL has at most one of each record type per round.
func (tw *TestWAL) AssertHealthy(bd simplex.BlockDeserializer, qcd simplex.QCDeserializer) {
	ctx := context.Background()
	records, err := tw.WriteAheadLog.ReadAll()
	require.NoError(tw.t, err)

	rounds := make(map[uint64]*walRound)

	for _, r := range records {
		recordType := binary.BigEndian.Uint16(r)

		switch recordType {
		case record.BlockRecordType:
			block, err := simplex.BlockFromRecord(ctx, bd, r)
			require.NoError(tw.t, err)
			round := block.BlockHeader().Round
			if _, exists := rounds[round]; !exists {
				rounds[round] = &walRound{round: round}
			}
			require.False(tw.t, rounds[round].blockRecord, "duplicate block record for round %d", round)
			rounds[round].blockRecord = true
		case record.NotarizationRecordType:
			_, vote, err := simplex.ParseNotarizationRecord(r)
			require.NoError(tw.t, err)
			round := vote.Round
			if _, exists := rounds[round]; !exists {
				rounds[round] = &walRound{round: round}
			}
			require.False(tw.t, rounds[round].notarizationRecord, "duplicate notarization record for round %d", round)
			rounds[round].notarizationRecord = true
		case record.EmptyNotarizationRecordType:
			_, vote, err := simplex.ParseEmptyNotarizationRecord(r)
			require.NoError(tw.t, err)
			round := vote.Round
			if _, exists := rounds[round]; !exists {
				rounds[round] = &walRound{round: round}
			}
			require.False(tw.t, rounds[round].emptyNotarizationRecord, "duplicate empty notarization record for round %d", round)
			rounds[round].emptyNotarizationRecord = true
		case record.EmptyVoteRecordType:
			vote, err := simplex.ParseEmptyVoteRecord(r)
			require.NoError(tw.t, err)
			round := vote.Round
			if _, exists := rounds[round]; !exists {
				rounds[round] = &walRound{round: round}
			}
			require.False(tw.t, rounds[round].emptyVoteRecord, "duplicate empty vote record for round %d", round)
			rounds[round].emptyVoteRecord = true
		case record.FinalizationRecordType:
			finalization, err := simplex.FinalizationFromRecord(r, qcd)
			require.NoError(tw.t, err)
			round := finalization.Finalization.Round
			if _, exists := rounds[round]; !exists {
				rounds[round] = &walRound{round: round}
			}
			require.False(tw.t, rounds[round].finalizationRecord, "duplicate finalization record for round %d", round)
			rounds[round].finalizationRecord = true
		default:
			tw.t.Fatalf("undefined record type: %d", recordType)
		}
	}
}
