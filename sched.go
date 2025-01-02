// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "sync"

type scheduler struct {
	lock    sync.Mutex
	signal  sync.Cond
	pending dependencies[Digest, task]
	ready   []task
	close   bool
}

func NewScheduler() *scheduler {
	var as scheduler
	as.pending = NewDependencies[Digest, task]()
	as.signal = sync.Cond{L: &as.lock}

	go as.run()

	return &as
}

func (as *scheduler) Size() int {
	as.lock.Lock()
	defer as.lock.Unlock()
	return as.pending.Size()
}

func (as *scheduler) Close() {
	as.lock.Lock()
	defer as.lock.Unlock()

	as.close = true

	as.signal.Signal()
}

func (as *scheduler) run() {
	as.lock.Lock()
	defer as.lock.Unlock()

	for !as.close {
		as.maybeExecuteTask()
		if as.close {
			return
		}
		as.signal.Wait()
	}
}

func (as *scheduler) maybeExecuteTask() {
	for len(as.ready) > 0 {
		if as.close {
			return
		}
		var task task
		task, as.ready = as.ready[0], as.ready[1:]
		as.lock.Unlock()
		task.f()
		as.lock.Lock()
	}
}

func (as *scheduler) Schedule(id Digest, f func(), prev Digest, ready bool) {
	as.lock.Lock()
	defer as.lock.Unlock()

	if as.close {
		return
	}

	task := task{
		f:      as.dispatchTaskAndScheduleDependingTasks(id, f),
		parent: prev,
		digest: id,
	}

	if ready {
		as.ready = append(as.ready, task)
	} else {
		as.pending.Insert(task)
	}

	as.signal.Signal()
}

func (as *scheduler) dispatchTaskAndScheduleDependingTasks(id Digest, task func()) func() {
	return func() {
		task()
		as.lock.Lock()
		defer as.lock.Unlock()
		newlyReadyTasks := as.pending.Remove(id)
		as.ready = append(as.ready, newlyReadyTasks...)
		as.signal.Signal()
	}
}

type task struct {
	f      func()
	digest Digest
	parent Digest
}

func (t task) dependsOn() Digest {
	return t.parent
}

func (t task) id() Digest {
	return t.digest
}

type dependent[C comparable] interface {
	dependsOn() C
	id() C
}

type dependencies[C comparable, D dependent[C]] struct {
	dependsOn map[C][]D // values depend on key.
}

func NewDependencies[C comparable, D dependent[C]]() dependencies[C, D] {
	return dependencies[C, D]{
		dependsOn: make(map[C][]D),
	}
}

func (t *dependencies[C, D]) Size() int {
	return len(t.dependsOn)
}

func (t *dependencies[C, D]) Insert(v D) {
	dependency := v.dependsOn()
	t.dependsOn[dependency] = append(t.dependsOn[dependency], v)
}

func (t *dependencies[C, D]) Remove(id C) []D {
	dependents := t.dependsOn[id]
	delete(t.dependsOn, id)
	return dependents
}
