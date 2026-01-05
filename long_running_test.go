// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/ava-labs/simplex/testutil"
)

func TestLongRunningSimple(t *testing.T) {
	net := testutil.NewDefaultLongRunningNetwork(t, 5)

	net.StartInstances()
	net.WaitForNodesToEnterRound(40)
	net.StopAndAssert(false)
}

func TestLongRunningReplication(t *testing.T) {
	for range 100 {
		t.Run("replication", func(t *testing.T) {
			t.Parallel()
			fmt.Println("iteration")
			net := testutil.NewDefaultLongRunningNetwork(t, 10)
			for _, instance := range net.Instances {
				instance.SilenceExceptKeywords("WAL", "empty vote", "empty notarization", "Triggering empty block agreement", "Leader is blacklisted, will not wait for it to propose", "I'm blacklisted, cannot propose", "It is time to build a block", "Starting round", "Not triggering empty block", "We have already timed out", "Broadcasting finalize vote", "Starting new round after committing", "Starting new round after committing finalizations", "Moving to a new round", "111Persisting", "111 Assembled","Indexing finalizations", "Progressing rounds due to commit", "111 Persist and broadcast")
			}

			net.StartInstances()

			net.WaitForNodesToEnterRound(40)
			net.NoMoreBlocks()
			net.DisconnectNodes(2)
			net.ContinueBlocks()
			net.WaitForNodesToEnterRound(70, 1, 3, 4, 5, 6)
			net.DisconnectNodes(4)
			net.WaitForNodesToEnterRound(90, 1, 3, 5, 6, 7, 8, 9)
			net.ConnectNodes(2, 4)
			net.WaitForNodesToEnterRound(150)
			net.StopAndAssert(false)
		})
	}
}

func TestLongRunningCrash(t *testing.T) {
	net := testutil.NewDefaultLongRunningNetwork(t, 10)

	net.StartInstances()
	net.WaitForNodesToEnterRound(30)
	net.CrashNodes(3)
	crashedNodeLatestBlock := net.Instances[3].Storage.NumBlocks()

	net.WaitForNodesToEnterRound(80, 1, 2, 4, 5, 6, 7, 8, 9)
	net.StartNodes(3)

	waitForRound := math.Max(float64(crashedNodeLatestBlock*2), 150)
	net.WaitForNodesToEnterRound(uint64(waitForRound))
	net.StopAndAssert(false)
}
