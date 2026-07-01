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
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 1},
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
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100},
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
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100},
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
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 200}, // newer, stored first
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100}, // older, dropped
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
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100}, // older, stored first
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 200}, // newer, replaces
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
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 1},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 2, Signature: signApproval(2, [32]byte{})}, 2},
				{ValidatorSetApproval{NodeID: makeNodeID(2), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 11},
				{ValidatorSetApproval{NodeID: makeNodeID(2), PChainHeight: 2, Signature: signApproval(2, [32]byte{})}, 12},
				{ValidatorSetApproval{NodeID: makeNodeID(3), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 21},
				{ValidatorSetApproval{NodeID: makeNodeID(3), PChainHeight: 2, Signature: signApproval(2, [32]byte{})}, 22},
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
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 10},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 2, Signature: signApproval(2, [32]byte{})}, 20},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 3, Signature: signApproval(3, [32]byte{})}, 30},
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
			// Verifies that the store keys on (NodeID, PChainHeight, AuxInfoDigest):
			// two approvals from the same node at the same P-chain height but with different
			// auxiliary info digests are kept as independent entries.
			name:       "same height different aux info digest coexist",
			validators: 3,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, AuxInfoDigest: [32]byte{0xAA}, Signature: signApproval(7, [32]byte{0xAA})}, 100},
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, AuxInfoDigest: [32]byte{0xBB}, Signature: signApproval(7, [32]byte{0xBB})}, 100},
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []approvalAndTimestamp) {
				got := as.Approvals()
				require.Len(t, got, 2)
				require.Equal(t, 2, as.storedCount)
				require.ElementsMatch(t, []ValidatorSetApproval{sent[0].ValidatorSetApproval, sent[1].ValidatorSetApproval}, got)
			},
		},
		{
			// Verifies that an approval with the maximum uint64 timestamp is stored,
			// and that a subsequent approval at the same (NodeID, PChainHeight) with any
			// smaller timestamp is treated as older and does not replace it.
			name:       "max uint64 timestamp is kept over any smaller timestamp",
			validators: 3,
			approvals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, math.MaxUint64},     // maxTS
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, math.MaxUint64 - 1}, // older
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

func TestApprovalStorePutApprovals(t *testing.T) {
	// findApproval locates the approval for a given (NodeID, PChainHeight) in a slice returned by
	// Approvals(). It is used to assert which of two competing approvals (source vs. destination)
	// survived a merge, since Approvals() does not carry the timestamp but does carry the Signature,
	// and signApproval produces a distinct signature per call.
	findApproval := func(got ValidatorSetApprovals, node nodeID, height uint64) (ValidatorSetApproval, bool) {
		for _, a := range got {
			if a.NodeID == node && a.PChainHeight == height {
				return a, true
			}
		}
		return ValidatorSetApproval{}, false
	}

	for _, tc := range []struct {
		name string
		// srcValidators/dstValidators size the two stores via makeValidators; NodeIDs are makeNodeID(i+1).
		srcValidators int
		dstValidators int
		// srcApprovals are loaded into the source store; dstApprovals are pre-loaded into the destination
		// store before PutApprovals is called.
		srcApprovals []approvalAndTimestamp
		dstApprovals []approvalAndTimestamp
		// verify asserts on the destination (and source) store after src.PutApprovals(dst).
		verify func(t *testing.T, dst, src *ApprovalStore, srcSent, dstSent []approvalAndTimestamp)
	}{
		{
			// Copies every source approval into an empty destination that shares the same validator set,
			// and leaves the source store untouched (PutApprovals copies, it does not move).
			name:          "copies all approvals into empty destination",
			srcValidators: 3,
			dstValidators: 3,
			srcApprovals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100},
				{ValidatorSetApproval{NodeID: makeNodeID(2), PChainHeight: 8, Signature: signApproval(8, [32]byte{})}, 100},
			},
			verify: func(t *testing.T, dst, src *ApprovalStore, srcSent, _ []approvalAndTimestamp) {
				got := dst.Approvals()
				require.Len(t, got, 2)
				require.Equal(t, 2, dst.storedCount)
				require.ElementsMatch(t, []ValidatorSetApproval{srcSent[0].ValidatorSetApproval, srcSent[1].ValidatorSetApproval}, got)
				// The source store is unchanged.
				require.Len(t, src.Approvals(), 2)
				require.Equal(t, 2, src.storedCount)
			},
		},
		{
			// Approvals from nodes absent from the destination's (smaller) validator set are dropped on
			// carry-over. This is the epoch-change case: a validator that is not part of the new set does
			// not have its approval carried into the new store.
			name:          "drops approvals from nodes not in destination validator set",
			srcValidators: 3,
			dstValidators: 2,
			srcApprovals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 10},
				{ValidatorSetApproval{NodeID: makeNodeID(2), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 10},
				{ValidatorSetApproval{NodeID: makeNodeID(3), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 10},
			},
			verify: func(t *testing.T, dst, _ *ApprovalStore, _, _ []approvalAndTimestamp) {
				got := dst.Approvals()
				require.Len(t, got, 2, "only approvals from nodes in the destination set carry over")
				require.Equal(t, 2, dst.storedCount)
				_, ok := findApproval(got, makeNodeID(3), 1)
				require.False(t, ok, "node 3 is not in the destination validator set")
			},
		},
		{
			// Merges into a non-empty destination: a pre-existing destination approval with a newer
			// timestamp is kept over an older one carried from the source, while a brand-new node's
			// approval from the source is added.
			name:          "keeps newer destination approval and adds new node",
			srcValidators: 2,
			dstValidators: 2,
			dstApprovals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 200}, // newer, already present
			},
			srcApprovals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100}, // older, must not overwrite
				{ValidatorSetApproval{NodeID: makeNodeID(2), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100}, // new node, added
			},
			verify: func(t *testing.T, dst, _ *ApprovalStore, _, dstSent []approvalAndTimestamp) {
				got := dst.Approvals()
				require.Len(t, got, 2)
				require.Equal(t, 2, dst.storedCount)
				kept, ok := findApproval(got, makeNodeID(1), 7)
				require.True(t, ok)
				require.Equal(t, dstSent[0].ValidatorSetApproval, kept, "the newer destination approval is retained")
				_, ok = findApproval(got, makeNodeID(2), 7)
				require.True(t, ok, "the new node's approval is carried over")
			},
		},
		{
			// A source approval with a newer timestamp replaces the stale destination approval at the
			// same (NodeID, PChainHeight).
			name:          "newer source approval replaces stale destination approval",
			srcValidators: 2,
			dstValidators: 2,
			dstApprovals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 100}, // older, present
			},
			srcApprovals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 7, Signature: signApproval(7, [32]byte{})}, 200}, // newer, replaces
			},
			verify: func(t *testing.T, dst, _ *ApprovalStore, srcSent, _ []approvalAndTimestamp) {
				got := dst.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, 1, dst.storedCount)
				require.Equal(t, srcSent[0].ValidatorSetApproval, got[0], "the newer source approval replaces the stale one")
			},
		},
		{
			// An empty source store is a no-op: the destination is left exactly as it was.
			name:          "empty source is a no-op",
			srcValidators: 2,
			dstValidators: 2,
			dstApprovals: []approvalAndTimestamp{
				{ValidatorSetApproval{NodeID: makeNodeID(1), PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 10},
			},
			verify: func(t *testing.T, dst, _ *ApprovalStore, _, dstSent []approvalAndTimestamp) {
				got := dst.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, 1, dst.storedCount)
				require.Equal(t, dstSent[0].ValidatorSetApproval, got[0])
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := NewApprovalStore(&signatureVerifier{}, makeValidators(tc.srcValidators), testutil.MakeLogger(t))
			dst := NewApprovalStore(&signatureVerifier{}, makeValidators(tc.dstValidators), testutil.MakeLogger(t))

			for _, a := range tc.srcApprovals {
				require.NoError(t, src.HandleApproval(&a.ValidatorSetApproval, a.Timestamp))
			}
			for _, a := range tc.dstApprovals {
				require.NoError(t, dst.HandleApproval(&a.ValidatorSetApproval, a.Timestamp))
			}

			src.PutApprovals(dst)

			// storedCount must always stay in sync with the number of retrievable approvals.
			require.Len(t, dst.Approvals(), dst.storedCount)
			tc.verify(t, dst, src, tc.srcApprovals, tc.dstApprovals)
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
		{ValidatorSetApproval{NodeID: node, PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 10},
		{ValidatorSetApproval{NodeID: node, PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 10}, // duplicate
		{ValidatorSetApproval{NodeID: node, PChainHeight: 1, Signature: signApproval(1, [32]byte{})}, 20}, // replaces
		{ValidatorSetApproval{NodeID: node, PChainHeight: 2, Signature: signApproval(2, [32]byte{})}, 30}, // new height
		{ValidatorSetApproval{NodeID: node, PChainHeight: 3, Signature: signApproval(3, [32]byte{})}, 40}, // triggers prune
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
		Signature:    signApproval(1, [32]byte{}),
	}, 100))

	for h := uint64(1); h <= 10; h++ {
		require.NoError(t, as.HandleApproval(&ValidatorSetApproval{
			NodeID:       vdrs[0].NodeID,
			PChainHeight: h,
			Signature:    signApproval(h, [32]byte{}),
		}, h))
	}
	require.Len(t, as.Approvals().UniqueByNodeID(), 2)
}
