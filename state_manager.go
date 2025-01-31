package simplex

import "sync/atomic"

type StateManager struct {
	lastBlock          Block // latest block commited
	canReceiveMessages atomic.Bool
	rounds             map[uint64]*Round
	futureMessages     messagesFromNode
	round              uint64 // The current round we notarize
}
