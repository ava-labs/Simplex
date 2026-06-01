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

func newApprovalStoreForTest(t *testing.T, validators NodeBLSMappings, sigErr error) *ApprovalStore {
	t.Helper()
	return NewApprovalStore(&signatureVerifier{err: sigErr}, validators, testutil.MakeLogger(t))
}

func TestApprovalStoreHandleApproval(t *testing.T) {
	tests := []struct {
		name       string
		validators int
		sigErr     error
		// approvals is the sequence of approvals handed to HandleApproval, in order.
		approvals []ValidatorSetApproval
		// want is the expected final set of stored approvals, compared order-independently.
		want []ValidatorSetApproval
	}{
		{
			name:       "approval from a node that is not a validator is dropped",
			validators: 3,
			approvals:  []ValidatorSetApproval{{NodeID: makeNodeID(99), PChainHeight: 1, Timestamp: 1}},
			want:       nil,
		},
		{
			name:       "approval from a known validator with an invalid signature is dropped",
			validators: 3,
			sigErr:     errors.New("bad sig"),
			approvals:  []ValidatorSetApproval{{NodeID: makeNodeID(1), PChainHeight: 1, Timestamp: 1, Signature: []byte{0xAA}}},
			want:       nil,
		},
		{
			name:       "valid approval is stored and retrievable",
			validators: 3,
			approvals:  []ValidatorSetApproval{{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}}},
			want:       []ValidatorSetApproval{{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}}},
		},
		{
			name:       "duplicate approval with same timestamp is a no-op",
			validators: 3,
			approvals: []ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}},
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}},
			},
			want: []ValidatorSetApproval{{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}}},
		},
		{
			name:       "older timestamp does not overwrite a newer stored approval",
			validators: 3,
			approvals: []ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 200, Signature: []byte{0x02}},
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}},
			},
			want: []ValidatorSetApproval{{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 200, Signature: []byte{0x02}}},
		},
		{
			name:       "newer timestamp replaces the stored approval in place",
			validators: 3,
			approvals: []ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 100, Signature: []byte{0x01}},
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 200, Signature: []byte{0x02}},
			},
			want: []ValidatorSetApproval{{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: 200, Signature: []byte{0x02}}},
		},
		{
			name:       "max uint64 timestamp is not replaced by a smaller one",
			validators: 3,
			approvals: []ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: math.MaxUint64, Signature: []byte{0xFF}},
				{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: math.MaxUint64 - 1, Signature: []byte{0x01}},
			},
			want: []ValidatorSetApproval{{NodeID: makeNodeID(1), PChainHeight: 7, Timestamp: math.MaxUint64, Signature: []byte{0xFF}}},
		},
		{
			name:       "store keeps independent entries per (node, height)",
			validators: 3,
			approvals: []ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 1, Timestamp: 1, Signature: []byte{0}},
				{NodeID: makeNodeID(1), PChainHeight: 2, Timestamp: 2, Signature: []byte{0}},
				{NodeID: makeNodeID(2), PChainHeight: 1, Timestamp: 11, Signature: []byte{1}},
				{NodeID: makeNodeID(2), PChainHeight: 2, Timestamp: 12, Signature: []byte{1}},
				{NodeID: makeNodeID(3), PChainHeight: 1, Timestamp: 21, Signature: []byte{2}},
				{NodeID: makeNodeID(3), PChainHeight: 2, Timestamp: 22, Signature: []byte{2}},
			},
			want: []ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 1, Timestamp: 1, Signature: []byte{0}},
				{NodeID: makeNodeID(1), PChainHeight: 2, Timestamp: 2, Signature: []byte{0}},
				{NodeID: makeNodeID(2), PChainHeight: 1, Timestamp: 11, Signature: []byte{1}},
				{NodeID: makeNodeID(2), PChainHeight: 2, Timestamp: 12, Signature: []byte{1}},
				{NodeID: makeNodeID(3), PChainHeight: 1, Timestamp: 21, Signature: []byte{2}},
				{NodeID: makeNodeID(3), PChainHeight: 2, Timestamp: 22, Signature: []byte{2}},
			},
		},
		{
			name:       "oldest approval is evicted once a node exceeds the per-node cap",
			validators: 2,
			approvals: []ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 1, Timestamp: 10, Signature: []byte{1}},
				{NodeID: makeNodeID(1), PChainHeight: 2, Timestamp: 20, Signature: []byte{2}},
				{NodeID: makeNodeID(1), PChainHeight: 3, Timestamp: 30, Signature: []byte{3}},
			},
			want: []ValidatorSetApproval{
				{NodeID: makeNodeID(1), PChainHeight: 2, Timestamp: 20, Signature: []byte{2}},
				{NodeID: makeNodeID(1), PChainHeight: 3, Timestamp: 30, Signature: []byte{3}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			as := newApprovalStoreForTest(t, makeValidators(tc.validators), tc.sigErr)

			for i := range tc.approvals {
				a := tc.approvals[i] // fresh copy: HandleApproval retains the pointer it is given.
				require.NoError(t, as.HandleApproval(&a))
				// storedCount must track the number of stored approvals after every step.
				require.Equal(t, len(as.Approvals()), as.storedCount)
			}

			require.ElementsMatch(t, tc.want, as.Approvals())
			require.Equal(t, len(tc.want), as.storedCount)
		})
	}
}

func TestApprovalStoreHandleApprovalPruningIsPerNode(t *testing.T) {
	// Verifies that the cap is applied per-NodeID, not globally: filling one node up to its cap does not
	// affect another node's approvals. This is kept out of the table because it exercises the cap with
	// many heights for a single node, which reads more clearly as a loop.
	vdrs := makeValidators(2)
	as := newApprovalStoreForTest(t, vdrs, nil)

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
