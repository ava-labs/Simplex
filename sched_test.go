// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"crypto/rand"
	"sync"
	"testing"

	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
	"github.com/stretchr/testify/require"
)

var defaultMaxTasks uint64 = 1000

func TestAsyncScheduler(t *testing.T) {
	t.Run("Executes asynchronously", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxTasks, func(d simplex.Digest) {})
		defer as.Close()

		ticks := make(chan struct{})

		var wg sync.WaitGroup
		wg.Add(1)

		dig2 := makeDigest(t)

		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			<-ticks
			return dig2
		})

		ticks <- struct{}{}
		wg.Wait()
	})

	t.Run("Executes onTaskFinished properly", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)

		digest := makeDigest(t)

		wg.Add(1)
		finished := func(d simplex.Digest) {
			require.Equal(t, digest, d)
			wg.Done()
		}

		as := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxTasks, finished)
		defer as.Close()

		as.Schedule(func() simplex.Digest {
			defer wg.Done()
			return digest
		})

		wg.Wait()
	})

	t.Run("Does not execute when closed", func(t *testing.T) {
		as := simplex.NewScheduler(testutil.MakeLogger(t), defaultMaxTasks, func(d simplex.Digest) {})
		ticks := make(chan struct{}, 1)
	
		as.Close()
	
		dig := makeDigest(t)
		as.Schedule(func() simplex.Digest {
			close(ticks)
			return dig
		})
	
		ticks <- struct{}{}
	})
}

func makeDigest(t *testing.T) simplex.Digest {
	var dig simplex.Digest
	_, err := rand.Read(dig[:])
	require.NoError(t, err)
	return dig
}
