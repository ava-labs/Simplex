// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"errors"
	"math"
	"testing"

	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

func makeNodeID(seed byte) nodeID {
	var n nodeID
	n[0] = seed
	return n
}

func makeValidators(n int) NodeBLSMappings {
	vdrs := make(NodeBLSMappings, n)
	for i := 0; i < n; i++ {
		vdrs[i] = NodeBLSMapping{
			NodeID: makeNodeID(byte(i + 1)),
			BLSKey: []byte{byte(i + 1)},
			Weight: 1,
		}
	}
	return vdrs
}

func TestApprovalStoreHandleApproval(t *testing.T) {
	for _, tc := range []struct {
		name       string
		validators int
		sigErr     error
		// approvals is the sequence of approvals (and their timestamps) to hand to the
		// store, in order. Each one is expected to be accepted without error. Node IDs are
		// referenced via makeNodeID(i+1), matching the i-th validator from makeValidators.
		approvals []approvalAndTimestamp
		// verify asserts on the store after all approvals have been handled. sent is the
		// approvals slice above, so cases can reference the values they submitted.
		verify func(t *testing.T, as *ApprovalStore, sent []approvalAndTimestamp)
	}{
		{
			// Verifies that an approval from a node that is not in the validator set is silently dropped.
			name:       "approval from unknown node is dropped",
			validators: 3,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(99), PChainHeight: 1}, 1},
			},
			verify: func(t *testing.T, as *ApprovalStore, _ []approvalAndTimestamp) {
				require.Empty(t, as.Approvals())
				require.Equal(t, 0, as.storedCount)
			},
		},
		{
			// Verifies that an approval from a known validator whose signature fails verification is dropped without being stored.
			name:       "approval with invalid signature is dropped",
			validators: 3,
			sigErr:     errors.New("bad sig"),
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 1, Signature: []byte{0xAA}}, 1},
			},
			verify: func(t *testing.T, as *ApprovalStore, _ []approvalAndTimestamp) {
				require.Empty(t, as.Approvals())
				require.Equal(t, 0, as.storedCount)
			},
		},
		{
			// happy path: an approval from a known validator with a valid signature is stored and
			// is retrievable via Approvals() and also increases storedCount.
			name:       "valid approval is stored",
			validators: 3,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0x01}}, 100},
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []approvalAndTimestamp) {
				got := as.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, sent[0].ValidatorSetApproval, got[0])
				require.Equal(t, 1, as.storedCount)
			},
		},
		{
			// Verifies that handing the same (NodeID, PChainHeight, Timestamp) twice is a no-op.
			// The store keeps exactly one copy and storedCount does not double-count.
			name:       "duplicate approval with same timestamp is a no-op",
			validators: 3,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0x01}}, 100},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0x01}}, 100},
			},
			verify: func(t *testing.T, as *ApprovalStore, _ []approvalAndTimestamp) {
				require.Len(t, as.Approvals(), 1)
				require.Equal(t, 1, as.storedCount)
			},
		},
		{
			// Verifies that when a newer approval is already stored for a (NodeID, PChainHeight), a subsequent
			// approval with an older Timestamp is dropped and does not overwrite it.
			name:       "older timestamp is ignored",
			validators: 3,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0x02}}, 200}, // newer, stored first
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0x01}}, 100}, // older, dropped
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []approvalAndTimestamp) {
				got := as.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, sent[0].ValidatorSetApproval, got[0]) // the newer approval is kept
				require.Equal(t, 1, as.storedCount)
			},
		},
		{
			// Verifies that a newer approval at the same (NodeID, PChainHeight) replaces the previously stored
			// one in place without changing storedCount.
			name:       "newer timestamp replaces",
			validators: 3,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0x01}}, 100}, // older, stored first
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0x02}}, 200}, // newer, replaces
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []approvalAndTimestamp) {
				got := as.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, sent[1].ValidatorSetApproval, got[0]) // the newer approval replaces the older one
				require.Equal(t, 1, as.storedCount)
			},
		},
		{
			// Verifies that the store keeps independent entries per (NodeID, PChainHeight) tuple.
			// Different validators and different heights all coexist.
			name:       "multiple nodes and heights coexist",
			validators: 3,
			// 3 validators x 2 heights, with timestamp == i*10 + h and Signature == {i} per validator.
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 1, Signature: []byte{0}}, 1},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 2, Signature: []byte{0}}, 2},
				{ValidatorSetApproval{NodeID: makeNodeID(2), PChainHeight: 1, Signature: []byte{1}}, 11},
				{ValidatorSetApproval{NodeID: makeNodeID(2), PChainHeight: 2, Signature: []byte{1}}, 12},
				{ValidatorSetApproval{NodeID: makeNodeID(3), PChainHeight: 1, Signature: []byte{2}}, 21},
				{ValidatorSetApproval{NodeID: makeNodeID(3), PChainHeight: 2, Signature: []byte{2}}, 22},
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []approvalAndTimestamp) {
				require.Len(t, as.Approvals(), len(sent))
				require.Equal(t, len(sent), as.storedCount)
			},
		},
		{
			// Verifies that once a node accumulates more approvals than len(validators),
			// the entry with the oldest Timestamp is evicted and storedCount stays in sync.
			name:       "prunes oldest when over cap",
			validators: 2,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 1, Signature: []byte{1}}, 10},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 2, Signature: []byte{2}}, 20},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 3, Signature: []byte{3}}, 30},
			},
			verify: func(t *testing.T, as *ApprovalStore, _ []approvalAndTimestamp) {
				got := as.Approvals()
				require.Len(t, got, 2, "store should be pruned down to cap=len(validators)=2")
				require.Equal(t, 2, as.storedCount)

				// Approvals() no longer carries the timestamp, so the pruned entry is identified by
				// its PChainHeight: height 1 had the oldest timestamp (ts=10) and should be gone.
				heights := map[uint64]bool{}
				for _, a := range got {
					heights[a.PChainHeight] = true
				}
				require.False(t, heights[1], "oldest (height=1, ts=10) should have been pruned")
				require.True(t, heights[2])
				require.True(t, heights[3])
			},
		},
		{
			// Verifies that an approval with the maximum uint64 timestamp is stored,
			// and that a subsequent approval at the same (NodeID, PChainHeight) with any
			// smaller timestamp is treated as older and does not replace it.
			name:       "max uint64 timestamp is kept over any smaller timestamp",
			validators: 3,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0xFF}}, math.MaxUint64},     // maxTS
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: []byte{0x01}}, math.MaxUint64 - 1}, // older
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []approvalAndTimestamp) {
				got := as.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, sent[0].ValidatorSetApproval, got[0]) // the max-timestamp approval is kept
				require.Equal(t, 1, as.storedCount)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vdrs := makeValidators(tc.validators)
			as := NewApprovalStore(&signatureVerifier{err: tc.sigErr}, vdrs, testutil.MakeLogger(t))

			for _, a := range tc.approvals {
				require.NoError(t, as.HandleApproval(&a.ValidatorSetApproval, a.Timestamp))
			}

			tc.verify(t, as, tc.approvals)
		})
	}
}

func TestApprovalStoreHandleApprovalStoredCountStaysConsistent(t *testing.T) {
	// Runs a mixed workload (insert, duplicate, replace, new height, prune)
	// and asserts that storedCount equals len(Approvals()) after every step.
	vdrs := makeValidators(2)
	as := NewApprovalStore(&signatureVerifier{}, vdrs, testutil.MakeLogger(t))
	node := vdrs[0].NodeID

	for _, a := range []approvalAndTimestamp{
		{ValidatorSetApproval{NodeID: node, PChainHeight: 1}, 10},
		{ValidatorSetApproval{NodeID: node, PChainHeight: 1}, 10}, // duplicate
		{ValidatorSetApproval{NodeID: node, PChainHeight: 1}, 20}, // replaces
		{ValidatorSetApproval{NodeID: node, PChainHeight: 2}, 30}, // new height
		{ValidatorSetApproval{NodeID: node, PChainHeight: 3}, 40}, // triggers prune
	} {
		require.NoError(t, as.HandleApproval(&a.ValidatorSetApproval, a.Timestamp))
		require.Len(t, as.Approvals(), as.storedCount)
	}
	require.Equal(t, 2, len(as.Approvals()))
}

func TestApprovalStoreHandleApprovalPruningIsPerNode(t *testing.T) {
	// Verifies that the cap is applied per-NodeID, not globally: filling one node up to its cap does not
	// affect another node's approvals.

	vdrs := makeValidators(2)
	as := NewApprovalStore(&signatureVerifier{}, vdrs, testutil.MakeLogger(t))

	require.NoError(t, as.HandleApproval(&ValidatorSetApproval{
		NodeID:       vdrs[1].NodeID,
		PChainHeight: 1,
	}, 100))

	for h := uint64(1); h <= 10; h++ {
		require.NoError(t, as.HandleApproval(&ValidatorSetApproval{
			NodeID:       vdrs[0].NodeID,
			PChainHeight: h,
		}, h))
	}
	require.Len(t, as.Approvals().UniqueByNodeID(), 2)
}
