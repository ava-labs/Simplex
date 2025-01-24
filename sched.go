// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import "sync"

type scheduler struct {
	lock    sync.RWMutex
	signal  sync.Cond
	pending dependencies
	ready   []task
	close   bool
}

func NewScheduler() *scheduler {
	var as scheduler
	as.pending = newDependencies()
	as.signal = sync.Cond{L: &as.lock}

	go as.run()

	return &as
}

func (as *scheduler) Size() int {
	as.lock.RLock()
	defer as.lock.RUnlock()

	return as.pending.Size() + len(as.ready)
}

func (as *scheduler) Close() {
	as.lock.Lock()
	defer as.lock.Unlock()

	as.close = true

	as.signal.Broadcast()
}

/*

(a) If a task A finished its execution before some task B that depends on it was scheduled, then when B was scheduled,
it was scheduled as ready.
Proof: The Epoch schedules new tasks under a lock, and computes whether a task is ready or not, under that lock as well.
Since each task in the Epoch obtains that lock as part of its execution, the computation of whether B is ready to be
scheduled or not is mutually exclusive with respect to A's execution. Therefore, if A finished executing it must be
that B is scheduled as ready when it is executed.

(b) If a task is scheduled and is ready to run, it will be executed after a finite set of instructions.
Proof: A ready task is entered into the ready queue (10) and then the condition variable is signaled (11) under a lock.
The scheduler goroutine in the meantime can be either waiting for the signal, in which case it will wake up (2) and perform the next
iteration where it will pop the task (4) and execute it (6), or it can be performing an instruction while not waiting for the signal.
In the latter case, the only time when a lock is not held (5), is when the task is executed (6).
If the lock is not held by the scheduler goroutine, then it will eventually reach the end of the loop and perform the next iteration,
in which it will detect the ready queue is not empty (1) and pop the task (4) and execute it (6).


// main claim (liveness): Tasks that depend on other tasks to finish are eventually executed once the tasks they depend on are executed.

We will show that it cannot be that there exists a task B such that it is scheduled and is not ready to be executed,
and B depends on a task A which finishes, but B is never scheduled once A finishes.

We split into two distinct cases:

I) B is scheduled after A
II) A is scheduled after B

If (I) holds, then when B is scheduled, it is not ready (according to the assumption) and hence it is inserted into pending (9).
It follows from (a) that A does not finish before B is inserted into pending (otherwise B was executed as 'ready').
At some point the task A finishes its execution (6), after which the scheduler goroutine removes the ID of A,
retrieve B from pending (7), add B to the ready queue (8), and perform the next iteration.
It follows from (b) that eventually it will pop B from the ready queue (4) and execute it.

If (II) holds, then when B is scheduled it is pending on A to finish and therefore added to the pending queue(9),
and A is not scheduled yet because scheduling of tasks is done under a lock. The rest follows trivially from (1).

*/

func (as *scheduler) run() {
	as.lock.Lock()
	defer as.lock.Unlock()

	for !as.close {

		if len(as.ready) == 0 { // (1)
			as.signal.Wait() // (2)
			continue         // (3)
		}

		var task task
		task, as.ready = as.ready[0], as.ready[1:] // (4)

		as.lock.Unlock() // (5)
		id := task.f()   // (6)
		as.lock.Lock()

		newlyReadyTasks := as.pending.Remove(id)        // (7)
		as.ready = append(as.ready, newlyReadyTasks...) // (8)
	}
}

func (as *scheduler) Schedule(f func() Digest, prev Digest, ready bool) {
	as.lock.Lock()
	defer as.lock.Unlock()

	if as.close {
		return
	}

	task := task{
		f:      f,
		parent: prev,
	}

	if !ready {
		as.pending.Insert(task) // (9)
		return
	}

	as.ready = append(as.ready, task) // (10)

	as.signal.Signal() // (11)
}

type task struct {
	f      func() Digest
	parent Digest
}

type dependencies struct {
	dependsOn map[Digest][]task // values depend on key.
}

func newDependencies() dependencies {
	return dependencies{
		dependsOn: make(map[Digest][]task),
	}
}

func (d *dependencies) Size() int {
	return len(d.dependsOn)
}

func (d *dependencies) Insert(t task) {
	dependency := t.parent
	d.dependsOn[dependency] = append(d.dependsOn[dependency], t)
}

func (t *dependencies) Remove(id Digest) []task {
	dependents := t.dependsOn[id]
	delete(t.dependsOn, id)
	return dependents
}
