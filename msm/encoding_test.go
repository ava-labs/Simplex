// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metadata

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimplexEpochInfoIsZero(t *testing.T) {
	require.True(t, (&SimplexEpochInfo{}).IsZero())
	require.False(t, (&SimplexEpochInfo{EpochNumber: 1}).IsZero())
	require.False(t, (&SimplexEpochInfo{PChainReferenceHeight: 1}).IsZero())
}

func TestSimplexEpochInfoEqual(t *testing.T) {
	hash1 := [32]byte{1, 2, 3}
	hash2 := [32]byte{4, 5, 6}

	tests := []struct {
		name     string
		a        *SimplexEpochInfo
		b        *SimplexEpochInfo
		expected bool
	}{
		{
			name:     "first nil second nil",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "first non-nil second nil",
			a:        &SimplexEpochInfo{},
			b:        nil,
			expected: false,
		},
		{
			name:     "first nil second non-nil (via nil receiver)",
			a:        nil,
			b:        &SimplexEpochInfo{},
			expected: false,
		},
		{
			name:     "both zero",
			a:        &SimplexEpochInfo{},
			b:        &SimplexEpochInfo{},
			expected: true,
		},
		{
			name: "equal with all fields",
			a: &SimplexEpochInfo{
				PChainReferenceHeight:     10,
				EpochNumber:               2,
				PrevSealingBlockHash:      hash1,
				NextPChainReferenceHeight: 20,
				PrevVMBlockSeq:            5,
				SealingBlockSeq:           15,
			},
			b: &SimplexEpochInfo{
				PChainReferenceHeight:     10,
				EpochNumber:               2,
				PrevSealingBlockHash:      hash1,
				NextPChainReferenceHeight: 20,
				PrevVMBlockSeq:            5,
				SealingBlockSeq:           15,
			},
			expected: true,
		},
		{
			name:     "different PChainReferenceHeight",
			a:        &SimplexEpochInfo{PChainReferenceHeight: 1},
			b:        &SimplexEpochInfo{PChainReferenceHeight: 2},
			expected: false,
		},
		{
			name:     "different EpochNumber",
			a:        &SimplexEpochInfo{EpochNumber: 1},
			b:        &SimplexEpochInfo{EpochNumber: 2},
			expected: false,
		},
		{
			name:     "different PrevSealingBlockHash",
			a:        &SimplexEpochInfo{PrevSealingBlockHash: hash1},
			b:        &SimplexEpochInfo{PrevSealingBlockHash: hash2},
			expected: false,
		},
		{
			name:     "different NextPChainReferenceHeight",
			a:        &SimplexEpochInfo{NextPChainReferenceHeight: 1},
			b:        &SimplexEpochInfo{NextPChainReferenceHeight: 2},
			expected: false,
		},
		{
			name:     "different PrevVMBlockSeq",
			a:        &SimplexEpochInfo{PrevVMBlockSeq: 1},
			b:        &SimplexEpochInfo{PrevVMBlockSeq: 2},
			expected: false,
		},
		{
			name:     "different SealingBlockSeq",
			a:        &SimplexEpochInfo{SealingBlockSeq: 1},
			b:        &SimplexEpochInfo{SealingBlockSeq: 2},
			expected: false,
		},
		{
			name: "with BlockValidationDescriptor equal",
			a: &SimplexEpochInfo{
				BlockValidationDescriptor: &BlockValidationDescriptor{
					AggregatedMembership: AggregatedMembership{
						Members: []NodeBLSMapping{{NodeID: nodeID{1}, Weight: 10}},
					},
				},
			},
			b: &SimplexEpochInfo{
				BlockValidationDescriptor: &BlockValidationDescriptor{
					AggregatedMembership: AggregatedMembership{
						Members: []NodeBLSMapping{{NodeID: nodeID{1}, Weight: 10}},
					},
				},
			},
			expected: true,
		},
		{
			name: "with BlockValidationDescriptor different",
			a: &SimplexEpochInfo{
				BlockValidationDescriptor: &BlockValidationDescriptor{
					AggregatedMembership: AggregatedMembership{
						Members: []NodeBLSMapping{{NodeID: nodeID{1}, Weight: 10}},
					},
				},
			},
			b: &SimplexEpochInfo{
				BlockValidationDescriptor: &BlockValidationDescriptor{
					AggregatedMembership: AggregatedMembership{
						Members: []NodeBLSMapping{{NodeID: nodeID{2}, Weight: 20}},
					},
				},
			},
			expected: false,
		},
		{
			name: "with NextEpochApprovals equal",
			a: &SimplexEpochInfo{
				NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{1, 2}, Signature: []byte{3, 4}},
			},
			b: &SimplexEpochInfo{
				NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{1, 2}, Signature: []byte{3, 4}},
			},
			expected: true,
		},
		{
			name: "with NextEpochApprovals different",
			a: &SimplexEpochInfo{
				NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{1, 2}, Signature: []byte{3, 4}},
			},
			b: &SimplexEpochInfo{
				NextEpochApprovals: &NextEpochApprovals{NodeIDs: []byte{5, 6}, Signature: []byte{7, 8}},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.a.Equal(tt.b))
		})
	}
}

func TestNodeBLSMappingEquals(t *testing.T) {
	tests := []struct {
		name     string
		a        NodeBLSMapping
		b        NodeBLSMapping
		expected bool
	}{
		{
			name:     "both zero",
			expected: true,
		},
		{
			name:     "equal with values",
			a:        NodeBLSMapping{NodeID: nodeID{1, 2, 3}, BLSKey: []byte{4, 5}, Weight: 100},
			b:        NodeBLSMapping{NodeID: nodeID{1, 2, 3}, BLSKey: []byte{4, 5}, Weight: 100},
			expected: true,
		},
		{
			name:     "different NodeID",
			a:        NodeBLSMapping{NodeID: nodeID{1}},
			b:        NodeBLSMapping{NodeID: nodeID{2}},
			expected: false,
		},
		{
			name:     "different BLSKey",
			a:        NodeBLSMapping{BLSKey: []byte{1}},
			b:        NodeBLSMapping{BLSKey: []byte{2}},
			expected: false,
		},
		{
			name:     "different Weight",
			a:        NodeBLSMapping{Weight: 1},
			b:        NodeBLSMapping{Weight: 2},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.a.Equals(&tt.b))
		})
	}
}

func TestBlockValidationDescriptorEquals(t *testing.T) {
	tests := []struct {
		name     string
		a        *BlockValidationDescriptor
		b        *BlockValidationDescriptor
		expected bool
	}{
		{
			name:     "both nil",
			expected: true,
		},
		{
			name:     "first nil second non-nil",
			b:        &BlockValidationDescriptor{},
			expected: false,
		},
		{
			name:     "first non-nil second nil",
			a:        &BlockValidationDescriptor{},
			expected: false,
		},
		{
			name: "equal members",
			a: &BlockValidationDescriptor{
				AggregatedMembership: AggregatedMembership{
					Members: []NodeBLSMapping{{NodeID: nodeID{1}, Weight: 10}},
				},
			},
			b: &BlockValidationDescriptor{
				AggregatedMembership: AggregatedMembership{
					Members: []NodeBLSMapping{{NodeID: nodeID{1}, Weight: 10}},
				},
			},
			expected: true,
		},
		{
			name: "different members",
			a: &BlockValidationDescriptor{
				AggregatedMembership: AggregatedMembership{
					Members: []NodeBLSMapping{{NodeID: nodeID{1}, Weight: 10}},
				},
			},
			b: &BlockValidationDescriptor{
				AggregatedMembership: AggregatedMembership{
					Members: []NodeBLSMapping{{NodeID: nodeID{2}, Weight: 20}},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.a.Equals(tt.b))
		})
	}
}

func TestAggregatedMembershipEquals(t *testing.T) {
	tests := []struct {
		name     string
		members  []NodeBLSMapping
		other    []NodeBLSMapping
		expected bool
	}{
		{
			name:     "both empty",
			expected: true,
		},
		{
			name:     "different lengths",
			members:  []NodeBLSMapping{{Weight: 1}},
			expected: false,
		},
		{
			name:     "equal",
			members:  []NodeBLSMapping{{NodeID: nodeID{1}, BLSKey: []byte{2}, Weight: 3}},
			other:    []NodeBLSMapping{{NodeID: nodeID{1}, BLSKey: []byte{2}, Weight: 3}},
			expected: true,
		},
		{
			name:     "different",
			members:  []NodeBLSMapping{{NodeID: nodeID{1}}},
			other:    []NodeBLSMapping{{NodeID: nodeID{2}}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			am := &AggregatedMembership{Members: tt.members}
			require.Equal(t, tt.expected, am.Equals(tt.other))
		})
	}
}

func TestNextEpochApprovalsEquals(t *testing.T) {
	tests := []struct {
		name     string
		a        *NextEpochApprovals
		b        *NextEpochApprovals
		expected bool
	}{
		{
			name:     "both nil",
			expected: true,
		},
		{
			name:     "first nil second non-nil",
			b:        &NextEpochApprovals{},
			expected: false,
		},
		{
			name:     "first non-nil second nil",
			a:        &NextEpochApprovals{},
			expected: false,
		},
		{
			name:     "equal",
			a:        &NextEpochApprovals{NodeIDs: []byte{1, 2}, Signature: []byte{3, 4}},
			b:        &NextEpochApprovals{NodeIDs: []byte{1, 2}, Signature: []byte{3, 4}},
			expected: true,
		},
		{
			name:     "different NodeIDs",
			a:        &NextEpochApprovals{NodeIDs: []byte{1}},
			b:        &NextEpochApprovals{NodeIDs: []byte{2}},
			expected: false,
		},
		{
			name:     "different Signature",
			a:        &NextEpochApprovals{Signature: []byte{1}},
			b:        &NextEpochApprovals{Signature: []byte{2}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.a.Equals(tt.b))
		})
	}
}

func TestNodeBLSMappingsTotalWeight(t *testing.T) {
	tests := []struct {
		name        string
		mappings    NodeBLSMappings
		expected    uint64
		expectError bool
	}{
		{
			name:     "empty",
			expected: 0,
		},
		{
			name:     "single",
			mappings: NodeBLSMappings{{Weight: 42}},
			expected: 42,
		},
		{
			name:     "multiple",
			mappings: NodeBLSMappings{{Weight: 10}, {Weight: 20}, {Weight: 30}},
			expected: 60,
		},
		{
			name:        "overflow",
			mappings:    NodeBLSMappings{{Weight: math.MaxUint64}, {Weight: 1}},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total, err := tt.mappings.TotalWeight()
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, total)
			}
		})
	}
}

func TestNodeBLSMappingsSumWeights(t *testing.T) {
	mappings := NodeBLSMappings{
		{NodeID: nodeID{1}, Weight: 10},
		{NodeID: nodeID{2}, Weight: 20},
		{NodeID: nodeID{3}, Weight: 30},
	}

	// Select only even indices
	total, err := mappings.SumWeights(func(i int, _ NodeBLSMapping) bool {
		return i%2 == 0
	})
	require.NoError(t, err)
	require.Equal(t, uint64(40), total) // index 0 (10) + index 2 (30)

	// Select none
	total, err = mappings.SumWeights(func(int, NodeBLSMapping) bool {
		return false
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), total)
}

func TestNodeBLSMappingsForEach(t *testing.T) {
	mappings := NodeBLSMappings{
		{Weight: 1},
		{Weight: 2},
		{Weight: 3},
	}

	var visited []uint64
	mappings.ForEach(func(_ int, nbm NodeBLSMapping) {
		visited = append(visited, nbm.Weight)
	})
	require.Equal(t, []uint64{1, 2, 3}, visited)
}

func TestNodeBLSMappingsCompare(t *testing.T) {
	tests := []struct {
		name     string
		a        NodeBLSMappings
		b        NodeBLSMappings
		expected bool
	}{
		{
			name:     "both nil",
			expected: true,
		},
		{
			name:     "different lengths",
			a:        NodeBLSMappings{{Weight: 1}},
			expected: false,
		},
		{
			name:     "equal same order",
			a:        NodeBLSMappings{{NodeID: nodeID{1}, Weight: 10}, {NodeID: nodeID{2}, Weight: 20}},
			b:        NodeBLSMappings{{NodeID: nodeID{1}, Weight: 10}, {NodeID: nodeID{2}, Weight: 20}},
			expected: true,
		},
		{
			name:     "equal different order",
			a:        NodeBLSMappings{{NodeID: nodeID{2}, Weight: 20}, {NodeID: nodeID{1}, Weight: 10}},
			b:        NodeBLSMappings{{NodeID: nodeID{1}, Weight: 10}, {NodeID: nodeID{2}, Weight: 20}},
			expected: true,
		},
		{
			name:     "different values",
			a:        NodeBLSMappings{{NodeID: nodeID{1}, Weight: 10}},
			b:        NodeBLSMappings{{NodeID: nodeID{1}, Weight: 99}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.a.Equal(tt.b))
		})
	}
}

func TestValidatorSetApprovalsForEach(t *testing.T) {
	approvals := ValidatorSetApprovals{
		{NodeID: nodeID{1}, PChainHeight: 10},
		{NodeID: nodeID{2}, PChainHeight: 20},
	}

	var heights []uint64
	approvals.ForEach(func(_ int, v ValidatorSetApproval) {
		heights = append(heights, v.PChainHeight)
	})
	require.Equal(t, []uint64{10, 20}, heights)
}

func TestValidatorSetApprovalsFilter(t *testing.T) {
	approvals := ValidatorSetApprovals{
		{NodeID: nodeID{1}, PChainHeight: 10},
		{NodeID: nodeID{2}, PChainHeight: 20},
		{NodeID: nodeID{3}, PChainHeight: 30},
	}

	filtered := approvals.Filter(func(_ int, v ValidatorSetApproval) bool {
		return v.PChainHeight > 15
	})
	require.Len(t, filtered, 2)
	require.Equal(t, uint64(20), filtered[0].PChainHeight)
	require.Equal(t, uint64(30), filtered[1].PChainHeight)

	// Filter all
	filtered = approvals.Filter(func(int, ValidatorSetApproval) bool {
		return false
	})
	require.Empty(t, filtered)
}
