// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"

	"go.uber.org/zap"
)

type Scheduler struct {
	logger  Logger
	lock    sync.Mutex
	signal  sync.Cond
	pending dependencies
	ready   []Task
	close   bool
}

func NewScheduler(logger Logger) *Scheduler {
	var as Scheduler
	as.pending = NewDependencies()
	as.signal = sync.Cond{L: &as.lock}
	as.logger = logger

	go as.run()

	return &as
}

func (as *Scheduler) Size() int {
	as.lock.Lock()
	defer as.lock.Unlock()

	return as.pending.Size() + len(as.ready)
}

func (as *Scheduler) Close() {
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

func (as *Scheduler) run() {
	as.lock.Lock()
	defer as.lock.Unlock()

	for !as.close {

		if len(as.ready) == 0 { // (1)
			as.logger.Trace("No ready tasks, going to sleep")
			as.signal.Wait() // (2)
			as.logger.Trace("Woken up from sleep", zap.Int("ready tasks", len(as.ready)))
			continue // (3)
		}

		taskToRun := as.ready[0]
		as.ready[0] = Task{}    // Cleanup any object references reachable from the closure of the task
		as.ready = as.ready[1:] // (4)
		numReadyTasks := len(as.ready)

		as.lock.Unlock() // (5)
		as.logger.Debug("Running task", zap.Int("remaining ready tasks", numReadyTasks))
		id := taskToRun.F() // (6)
		as.logger.Debug("Task finished execution", zap.Stringer("taskID", id))
		as.lock.Lock()

		newlyReadyTasks := as.pending.Remove(id)        // (7)
		as.ready = append(as.ready, newlyReadyTasks...) // (8)
		as.logger.Trace("Enqueued newly ready tasks", zap.Int("number of ready tasks", len(newlyReadyTasks)))
	}
}

func (as *Scheduler) Schedule(f func() Digest, prev Digest, ready bool) {
	as.lock.Lock()
	defer as.lock.Unlock()

	if as.close {
		return
	}

	task := Task{
		F:      f,
		Parent: prev,
	}

	if !ready {
		as.logger.Debug("Scheduling task", zap.Stringer("dependency", prev))
		as.pending.Insert(task) // (9)
		return
	}

	as.logger.Debug("Scheduling new ready task", zap.Stringer("dependency", prev))

	as.ready = append(as.ready, task) // (10)

	as.signal.Broadcast() // (11)
}

type Task struct {
	F      func() Digest
	Parent Digest
}

type dependencies struct {
	dependsOn map[Digest][]Task // values depend on key.
}

func NewDependencies() dependencies {
	return dependencies{
		dependsOn: make(map[Digest][]Task),
	}
}

func (d *dependencies) Size() int {
	return len(d.dependsOn)
}

func (d *dependencies) Insert(t Task) {
	dependency := t.Parent
	d.dependsOn[dependency] = append(d.dependsOn[dependency], t)
}

func (t *dependencies) Remove(id Digest) []Task {
	dependents := t.dependsOn[id]
	delete(t.dependsOn, id)
	return dependents
}
