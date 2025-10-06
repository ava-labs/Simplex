// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
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
