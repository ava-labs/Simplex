package simplex

import (
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/stretchr/testify/require"
)

func TestParseBlockSizeMatchesBytes(t *testing.T) {
	// Case 1: Bytes() first, Size() second, size returns the cached length.
	pb := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: common.ProtocolMetadata{
					Version: 1,
					Prev:    common.Digest{},
					Round:   1,
					Epoch:   4,
					Seq:     2,
				},
				SimplexBlacklist: common.Blacklist{
					Updates:   common.BlacklistUpdates{{NodeIndex: 1, Type: 1}},
					NodeCount: 2,
				},
				PChainHeight: 6,
			},
			InnerBlock: &testInnerBlock{
				Height_: 7,
				TS:      time.UnixMilli(8),
				Payload: []byte("payload"),
			},
		},
	}
	bytes := pb.Bytes()
	require.Equal(t, len(bytes), pb.Size())

	// Case 2: Size() first on a non serialized block. it will
	// compute the size and match a later Byte() call.
	pb2 := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: common.ProtocolMetadata{
					Version: 1,
					Prev:    common.Digest{},
					Round:   1,
					Epoch:   4,
					Seq:     2,
				},
				SimplexBlacklist: common.Blacklist{
					Updates:   common.BlacklistUpdates{{NodeIndex: 1, Type: 1}},
					NodeCount: 2,
				},
				PChainHeight: 6,
			},
			InnerBlock: &testInnerBlock{
				Height_: 9,
				TS:      time.UnixMilli(10),
				Payload: []byte("other payload"),
			},
		},
	}
	size := pb2.Size()
	require.NotZero(t, size)
	bytes2 := pb2.Bytes()
	require.Equal(t, len(bytes2), size)

	// case 3: concurrent Size() calls on a block that was never serialized.
	// the goroutines rase to compute the size, the lock must make this
	// safe and every call must return the correct value

	pb3 := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: common.ProtocolMetadata{
					Version: 1,
					Prev:    common.Digest{},
					Round:   1,
					Epoch:   4,
					Seq:     2,
				},
				SimplexBlacklist: common.Blacklist{
					Updates:   common.BlacklistUpdates{{NodeIndex: 1, Type: 1}},
					NodeCount: 2,
				},
				PChainHeight: 6,
			},
			InnerBlock: &testInnerBlock{
				Height_: 11,
				TS:      time.UnixMilli(12),
				Payload: []byte("concurrent"),
			},
		},
	}
	var wg sync.WaitGroup
	sizes := make([]int, 4)
	for i := range sizes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sizes[i] = pb3.Size()
		}()
	}
	wg.Wait()
	bytes3 := pb3.Bytes()
	for _, size := range sizes {
		require.Equal(t, len(bytes3), size)
	}
}
