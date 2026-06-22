// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEpochChangeSupression(t *testing.T) {
	t.Run("Inactive by default", func(t *testing.T) {
		var ecs epochChangeSupression
		require.False(t, ecs.isSupressionActive())
		// When inactive, nothing is prohibited regardless of sequence.
		require.False(t, ecs.sendProhibited(0))
		require.False(t, ecs.sendProhibited(100))
	})

	t.Run("setSupression activates and prohibits sequences after the sealing block", func(t *testing.T) {
		var ecs epochChangeSupression
		const sealingBlockSeq = uint64(10)
		ecs.setSupression(sealingBlockSeq)

		require.True(t, ecs.isSupressionActive())

		// Sequences up to and including the sealing block are allowed.
		require.False(t, ecs.sendProhibited(sealingBlockSeq-1))
		require.False(t, ecs.sendProhibited(sealingBlockSeq))

		// Sequences after the sealing block are prohibited.
		require.True(t, ecs.sendProhibited(sealingBlockSeq+1))
		require.True(t, ecs.sendProhibited(sealingBlockSeq+100))
	})

	t.Run("clearSupression deactivates and allows all sequences", func(t *testing.T) {
		var ecs epochChangeSupression
		const sealingBlockSeq = uint64(10)
		ecs.setSupression(sealingBlockSeq)
		require.True(t, ecs.sendProhibited(sealingBlockSeq+1))

		ecs.clearSupression()

		require.False(t, ecs.isSupressionActive())
		// Once cleared, previously prohibited sequences are allowed again.
		require.False(t, ecs.sendProhibited(sealingBlockSeq+1))
	})

	t.Run("setSupression overwrites the previous sealing block sequence", func(t *testing.T) {
		var ecs epochChangeSupression
		ecs.setSupression(5)
		require.True(t, ecs.sendProhibited(8))

		ecs.setSupression(10)
		require.True(t, ecs.isSupressionActive())
		// The new, higher sealing block sequence now permits what was previously prohibited.
		require.False(t, ecs.sendProhibited(8))
		require.True(t, ecs.sendProhibited(11))
	})
}
