// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "sync"

type ExecutingCounter interface {
	Increment()
	Decrement()
	Wait()
}

type NoOpExecutingCounter struct{}

func (f NoOpExecutingCounter) Increment() {
}

func (f NoOpExecutingCounter) Decrement() {
}

func (f NoOpExecutingCounter) Wait() {
}

type RealExecutingCounter struct {
	lock sync.Mutex
	signal sync.Cond
	executing int
}

func NewRealExecutingCounter() *RealExecutingCounter {
	var ec RealExecutingCounter
	ec.signal.L = &ec.lock
	return &ec
}

func (ec *RealExecutingCounter) Increment() {
	ec.lock.Lock()
	defer ec.lock.Unlock()

	ec.executing++
	ec.signal.Broadcast()
}

func (ec *RealExecutingCounter) Decrement() {
	ec.lock.Lock()
	defer ec.lock.Unlock()

	ec.executing--
	if ec.executing < 0 {
		panic("negative executing count")
	}
	ec.signal.Broadcast()
}

func (ec *RealExecutingCounter) Wait() {
	ec.lock.Lock()
	defer ec.lock.Unlock()

	for ec.executing > 0 {
		ec.signal.Wait()
	}
}
