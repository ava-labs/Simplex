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
		// approvals is the sequence of approvals to hand to the store, in order.
		// Each one is expected to be accepted without error. Node IDs are referenced
		// via makeNodeID(i+1), matching the i-th validator from makeValidators.
		approvals []*ValidatorSetApproval
		// verify asserts on the store after all approvals have been handled. sent is the
		// approvals slice above, so cases can reference the values they submitted.
		verify func(t *testing.T, as *ApprovalStore, sent []*ValidatorSetApproval)
	}{
		{
			// Verifies that an approval from a node that is not in the validator set is silently dropped.
			name:       "approval from unknown node is dropped",
			validators: 3,
			approvals: []*ValidatorSetApproval{{
				NodeID:       makeNodeID(99),
				PChainHeight: 1,
				Timestamp:    1,
			}},
			verify: func(t *testing.T, as *ApprovalStore, _ []*ValidatorSetApproval) {
				require.Empty(t, as.Approvals())
				require.Equal(t, 0, as.storedCount)
			},
		},
		{
			// Verifies that an approval from a known validator whose signature fails verification is dropped without being stored.
			name:       "approval with invalid signature is dropped",
			validators: 3,
			sigErr:     errors.New("bad sig"),
			approvals: []*ValidatorSetApproval{{
				NodeID:       makeNodeID(1),
				PChainHeight: 1,
				Timestamp:    1,
				Signature:    []byte{0xAA},
			}},
			verify: func(t *testing.T, as *ApprovalStore, _ []*ValidatorSetApproval) {
				require.Empty(t, as.Approvals())
				require.Equal(t, 0, as.storedCount)
			},
		},
		{
			// happy path: an approval from a known validator with a valid signature is stored and
			// is retrievable via Approvals() and also increases storedCount.
			name:       "valid approval is stored",
			validators: 3,
			approvals: []*ValidatorSetApproval{{
				NodeID:       makeNodeID(1),
				PChainHeight: 7,
				Timestamp:    100,
				Signature:    []byte{0x01},
			}},
			verify: func(t *testing.T, as *ApprovalStore, sent []*ValidatorSetApproval) {
				got := as.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, *sent[0], got[0])
				require.Equal(t, 1, as.storedCount)
			},
		},
		{
			// Verifies that handing the same (NodeID, PChainHeight, Timestamp) twice is a no-op.
			// The store keeps exactly one copy and storedCount does not double-count.
			name:       "duplicate approval with same timestamp is a no-op",
			validators: 3,
			approvals: []*ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}},
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}},
			},
			verify: func(t *testing.T, as *ApprovalStore, _ []*ValidatorSetApproval) {
				require.Len(t, as.Approvals(), 1)
				require.Equal(t, 1, as.storedCount)
			},
		},
		{
			// Verifies that when a newer approval is already stored for a (NodeID, PChainHeight), a subsequent
			// approval with an older Timestamp is dropped and does not overwrite it.
			name:       "older timestamp is ignored",
			validators: 3,
			approvals: []*ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 200, Signature: []byte{0x02}}, // newer, stored first
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}}, // older, dropped
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []*ValidatorSetApproval) {
				got := as.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, *sent[0], got[0]) // the newer approval is kept
				require.Equal(t, 1, as.storedCount)
			},
		},
		{
			// Verifies that a newer approval at the same (NodeID, PChainHeight) replaces the previously stored
			// one in place without changing storedCount.
			name:       "newer timestamp replaces",
			validators: 3,
			approvals: []*ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}}, // older, stored first
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 200, Signature: []byte{0x02}}, // newer, replaces
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []*ValidatorSetApproval) {
				got := as.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, *sent[1], got[0]) // the newer approval replaces the older one
				require.Equal(t, 1, as.storedCount)
			},
		},
		{
			// Verifies that the store keeps independent entries per (NodeID, PChainHeight) tuple.
			// Different validators and different heights all coexist.
			name:       "multiple nodes and heights coexist",
			validators: 3,
			// 3 validators x 2 heights, with Timestamp == i*10 + h and Signature == {i} per validator.
			approvals: []*ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 1, Timestamp: 1, Signature: []byte{0}},
				{NodeID: makeNodeID(1), PChainHeight: 2, Timestamp: 2, Signature: []byte{0}},
				{NodeID: makeNodeID(2), PChainHeight: 1, Timestamp: 11, Signature: []byte{1}},
				{NodeID: makeNodeID(2), PChainHeight: 2, Timestamp: 12, Signature: []byte{1}},
				{NodeID: makeNodeID(3), PChainHeight: 1, Timestamp: 21, Signature: []byte{2}},
				{NodeID: makeNodeID(3), PChainHeight: 2, Timestamp: 22, Signature: []byte{2}},
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []*ValidatorSetApproval) {
				require.Len(t, as.Approvals(), len(sent))
				require.Equal(t, len(sent), as.storedCount)
			},
		},
		{
			// Verifies that once a node accumulates more approvals than len(validators),
			// the entry with the oldest Timestamp is evicted and storedCount stays in sync.
			name:       "prunes oldest when over cap",
			validators: 2,
			approvals: []*ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 1, Timestamp: 10, Signature: []byte{1}},
				{NodeID: makeNodeID(1), PChainHeight: 2, Timestamp: 20, Signature: []byte{2}},
				{NodeID: makeNodeID(1), PChainHeight: 3, Timestamp: 30, Signature: []byte{3}},
			},
			verify: func(t *testing.T, as *ApprovalStore, _ []*ValidatorSetApproval) {
				got := as.Approvals()
				require.Len(t, got, 2, "store should be pruned down to cap=len(validators)=2")
				require.Equal(t, 2, as.storedCount)

				timestamps := map[uint64]bool{}
				for _, a := range got {
					timestamps[a.Timestamp] = true
				}
				require.False(t, timestamps[10], "oldest (ts=10) should have been pruned")
				require.True(t, timestamps[20])
				require.True(t, timestamps[30])
			},
		},
		{
			// Verifies that an approval with the maximum uint64 timestamp is stored,
			// and that a subsequent approval at the same (NodeID, PChainHeight) with any
			// smaller timestamp is treated as older and does not replace it.
			name:       "max uint64 timestamp is kept over any smaller timestamp",
			validators: 3,
			approvals: []*ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: math.MaxUint64, Signature: []byte{0xFF}},     // maxTS
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: math.MaxUint64 - 1, Signature: []byte{0x01}}, // older
			},
			verify: func(t *testing.T, as *ApprovalStore, sent []*ValidatorSetApproval) {
				got := as.Approvals()
				require.Len(t, got, 1)
				require.Equal(t, *sent[0], got[0]) // the max-timestamp approval is kept
				require.Equal(t, 1, as.storedCount)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vdrs := makeValidators(tc.validators)
			as := NewApprovalStore(&signatureVerifier{err: tc.sigErr}, vdrs, testutil.MakeLogger(t))

			for _, a := range tc.approvals {
				require.NoError(t, as.HandleApproval(a))
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

	for _, a := range []*ValidatorSetApproval{
		{NodeID: node, PChainHeight: 1, Timestamp: 10},
		{NodeID: node, PChainHeight: 1, Timestamp: 10}, // duplicate
		{NodeID: node, PChainHeight: 1, Timestamp: 20}, // replaces
		{NodeID: node, PChainHeight: 2, Timestamp: 30}, // new height
		{NodeID: node, PChainHeight: 3, Timestamp: 40}, // triggers prune
	} {
		require.NoError(t, as.HandleApproval(a))
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
		Timestamp:    100,
	}))

	for h := uint64(1); h <= 10; h++ {
		require.NoError(t, as.HandleApproval(&ValidatorSetApproval{
			NodeID:       vdrs[0].NodeID,
			PChainHeight: h,
			Timestamp:    h,
		}))
	}
	require.Len(t, as.Approvals().UniqueByNodeID(), 2)
}
