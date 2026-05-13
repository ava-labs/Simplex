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

func TestApprovalStoreHandleApprovalUnknownNode(t *testing.T) {
	// Verifies that an approval from a node that is not in the validator set is silently dropped.

	as := newApprovalStoreForTest(t, makeValidators(3), nil)
	require.NoError(t, as.HandleApproval(&ValidatorSetApproval{
		NodeID:       makeNodeID(99),
		PChainHeight: 1,
		Timestamp:    1,
	}))
	require.Empty(t, as.Approvals())
	require.Equal(t, 0, as.storedCount)
}

func TestApprovalStoreHandleApprovalInvalidSignature(t *testing.T) {
	// Verifies that an approval from a known validator whose signature fails verification is dropped without being stored.
	vdrs := makeValidators(3)
	as := newApprovalStoreForTest(t, vdrs, errors.New("bad sig"))

	require.NoError(t, as.HandleApproval(&ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 1,
		Timestamp:    1,
		Signature:    []byte{0xAA},
	}))
	require.Empty(t, as.Approvals())
	require.Equal(t, 0, as.storedCount)
}

func TestApprovalStoreHandleApprovalStoresValidApproval(t *testing.T) {
	// happy path: an approval from a known validator with a valid signature is stored and
	//is retrievable via Approvals() and also increases storedCount.
	vdrs := makeValidators(3)
	as := newApprovalStoreForTest(t, vdrs, nil)

	a := &ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 7,
		Timestamp:    100,
		Signature:    []byte{0x01},
	}
	require.NoError(t, as.HandleApproval(a))

	got := as.Approvals()
	require.Len(t, got, 1)
	require.Equal(t, *a, got[0])
	require.Equal(t, 1, as.storedCount)
}

func TestApprovalStoreHandleApprovalDuplicateSameTimestamp(t *testing.T) {
	// Verifies that handing the same (NodeID, PChainHeight, Timestamp) twice is a no-op.
	// The store keeps exactly one copy and storedCount does not double-count.
	vdrs := makeValidators(3)
	as := newApprovalStoreForTest(t, vdrs, nil)

	a := &ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 7,
		Timestamp:    100,
		Signature:    []byte{0x01},
	}
	require.NoError(t, as.HandleApproval(a))
	require.NoError(t, as.HandleApproval(a))
	require.Len(t, as.Approvals(), 1)
	require.Equal(t, 1, as.storedCount)
}

func TestApprovalStoreHandleApprovalOlderTimestampIgnored(t *testing.T) {
	// Verifies that when a newer approval is already stored for a (NodeID, PChainHeight), a subsequent
	// approval with an older Timestamp is dropped and does not overwrite it.
	vdrs := makeValidators(3)
	as := newApprovalStoreForTest(t, vdrs, nil)

	newer := &ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 7,
		Timestamp:    200,
		Signature:    []byte{0x02},
	}
	require.NoError(t, as.HandleApproval(newer))

	older := &ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 7,
		Timestamp:    100,
		Signature:    []byte{0x01},
	}
	require.NoError(t, as.HandleApproval(older))

	got := as.Approvals()
	require.Len(t, got, 1)
	require.Equal(t, *newer, got[0])
	require.Equal(t, 1, as.storedCount)
}

func TestApprovalStoreHandleApprovalNewerTimestampReplaces(t *testing.T) {
	// verifies that a newer approval at the same (NodeID, PChainHeight) replaces the previously stored
	// one in place without changing storedCount.
	vdrs := makeValidators(3)
	as := newApprovalStoreForTest(t, vdrs, nil)

	older := &ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 7,
		Timestamp:    100,
		Signature:    []byte{0x01},
	}
	require.NoError(t, as.HandleApproval(older))

	newer := &ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 7,
		Timestamp:    200,
		Signature:    []byte{0x02},
	}
	require.NoError(t, as.HandleApproval(newer))

	got := as.Approvals()
	require.Len(t, got, 1)
	require.Equal(t, *newer, got[0])
	require.Equal(t, 1, as.storedCount)
}

func TestApprovalStoreHandleApprovalMultipleNodesAndHeights(t *testing.T) {
	// Verifies that the store keeps independent entries per (NodeID, PChainHeight) tuple.
	// Different validators and different heights all coexist.
	vdrs := makeValidators(3)
	as := newApprovalStoreForTest(t, vdrs, nil)

	for i, v := range vdrs {
		for _, h := range []uint64{1, 2} {
			require.NoError(t, as.HandleApproval(&ValidatorSetApproval{
				NodeID:       v.NodeID,
				PChainHeight: h,
				Timestamp:    uint64(i*10) + h,
				Signature:    []byte{byte(i)},
			}))
		}
	}

	require.Len(t, as.Approvals(), len(vdrs)*2)
	require.Equal(t, len(vdrs)*2, as.storedCount)
}

func TestApprovalStoreHandleApprovalPrunesOldestWhenOverCap(t *testing.T) {
	// Verifies that once a node accumulates more approvals than len(validators),
	// the entry with the oldest Timestamp is evicted and storedCount stays in sync.
	vdrs := makeValidators(2)
	as := newApprovalStoreForTest(t, vdrs, nil)

	node := vdrs[0].NodeID
	for _, a := range []*ValidatorSetApproval{
		{NodeID: node, PChainHeight: 1, Timestamp: 10, Signature: []byte{1}},
		{NodeID: node, PChainHeight: 2, Timestamp: 20, Signature: []byte{2}},
		{NodeID: node, PChainHeight: 3, Timestamp: 30, Signature: []byte{3}},
	} {
		require.NoError(t, as.HandleApproval(a))
	}

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
}

func TestApprovalStoreHandleApprovalPruningIsPerNode(t *testing.T) {
	// Verifies that the cap is applied per-NodeID, not globally: filling one node up to its cap does not
	// affect another node's approvals.

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

func TestApprovalStoreHandleApprovalMaxUint64Timestamp(t *testing.T) {
	// Verifies that an approval with the maximum uint64 timestamp is stored,
	// and that a subsequent approval at the same (NodeID, PChainHeight) with any
	// smaller timestamp is treated as older and does not replace it.
	vdrs := makeValidators(3)
	as := newApprovalStoreForTest(t, vdrs, nil)

	maxTS := &ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 7,
		Timestamp:    math.MaxUint64,
		Signature:    []byte{0xFF},
	}
	require.NoError(t, as.HandleApproval(maxTS))

	got := as.Approvals()
	require.Len(t, got, 1)
	require.Equal(t, *maxTS, got[0])
	require.Equal(t, 1, as.storedCount)

	older := &ValidatorSetApproval{
		NodeID:       vdrs[0].NodeID,
		PChainHeight: 7,
		Timestamp:    math.MaxUint64 - 1,
		Signature:    []byte{0x01},
	}
	require.NoError(t, as.HandleApproval(older))

	got = as.Approvals()
	require.Len(t, got, 1)
	require.Equal(t, *maxTS, got[0])
	require.Equal(t, 1, as.storedCount)
}

func TestApprovalStoreHandleApprovalStoredCountStaysConsistent(t *testing.T) {
	// runs a mixed workload (insert, duplicate, replace, new height, prune)
	// and asserts that storedCount equals len(Approvals()) after every step.
	vdrs := makeValidators(2)
	as := newApprovalStoreForTest(t, vdrs, nil)
	node := vdrs[0].NodeID

	for _, a := range []*ValidatorSetApproval{
		{NodeID: node, PChainHeight: 1, Timestamp: 10},
		{NodeID: node, PChainHeight: 1, Timestamp: 10}, // duplicate
		{NodeID: node, PChainHeight: 1, Timestamp: 20}, // replaces
		{NodeID: node, PChainHeight: 2, Timestamp: 30}, // new height
		{NodeID: node, PChainHeight: 3, Timestamp: 40}, // triggers prune
	} {
		require.NoError(t, as.HandleApproval(a))
	}
	require.Equal(t, 2, len(as.Approvals()))
}
