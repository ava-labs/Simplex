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
		as.executeReadyTasks()
		if as.close {
			return
		}
		if len(as.ready) > 0 {
			continue
		}
		as.signal.Wait()
	}
}

func (as *scheduler) executeReadyTasks() {
	for len(as.ready) > 0 {
		if as.close {
			return
		}
		var task task
		task, as.ready = as.ready[0], as.ready[1:]
		as.lock.Unlock()
		task.f()
	}
}

/*

(a) While a task is scheduled, the lock is held and therefore the only instructions that the scheduler thread
can perform is running 'dispatchTaskAndScheduleDependingTasks', as any other line in 'executeReadyTasks' requires
the lock to be held. Inside 'dispatchTaskAndScheduleDependingTasks', the only line that doesn't require the lock
to be held is executing the task itself. It follows from here, that it is not possible to schedule a new task
while moving tasks from pending to the ready queue, and vice versa.

(b) The Epoch schedules new tasks under a lock, and computes whether a task is ready or not, under that lock as well.
Since each task in the Epoch first obtains a lock before proceeding to do anything, if a task A finished its execution before
the task B that depends on it was scheduled, then it cannot be that B was scheduled with a dependency on A and is not ready,
because the computation of whether B is ready to be scheduled or not is mutually exclusive with respect to A's execution.
Therefore, if A finished executing it must be that B is ready to be executed.

(c) If a task is scheduled and is ready to run, it will be executed after a finite set of instructions.
The reason is that a ready task is entered into the ready queue and then the condition variable is signaled.
The scheduler goroutine can be either waiting for the signal, in which case it will wake up and execute the task,
or it can be performing an instruction before waiting for the signal. In the latter case, the only time when
a lock is not held, is when the task is executed. Afterward, the lock is re-acquired in 'dispatchTaskAndScheduleDependingTasks'.
It follows from (a) that if the lock is not held by the scheduler goroutine, then it will check for ready tasks one more time
just before entering the wait for the signal, and therefore even if the signal is given while the scheduler goroutine is not waiting
for it, a scheduling of a task ready to run will run after a finite set of instructions by the scheduler goroutine.

We will show that it cannot be that there exists a task B such that it is scheduled and is not ready to be executed,
and B depends on a task A which finishes, but B is never scheduled once A finishes.

We split into two distinct cases:

1) B is scheduled after A
2) A is scheduled after B

If (1) holds, then when B is scheduled, it is not ready (according to the assumption) and hence it is inserted into pending.
It follows from (b) that A does not finish before B is inserted into pending (otherwise B was ready to be executed).
At some point the task A finishes its execution, after which the scheduler goroutine
enters 'dispatchTaskAndScheduleDependingTasks' where it proceeds to remove the ID of A,
retrieve B from pending, add B to the ready queue, and perform another iteration inside 'executeReadyTasks'.
It will then pop tasks from the ready queue and execute them until it is empty, and one of these tasks will be B.

If (2) holds, then when B is scheduled it is pending on A to finish.
The rest follows trivially from (1).

*/

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
		newlyReadyTasks := as.pending.Remove(id)
		as.ready = append(as.ready, newlyReadyTasks...)
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
