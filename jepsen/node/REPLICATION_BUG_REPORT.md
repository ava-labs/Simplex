# Bug report: replication retry livelock — a lagging node can never catch up

**Found by:** Jepsen-style fault injection test `drop-replication-response` (`jepsen/node/cmd/tests/main.go`)
**Component:** block replication (`simplex/requestor.go`, `simplex/epoch.go`)
**Severity:** liveness — a node that falls behind by more than `MaxRoundWindow` sequences and loses a few replication responses gets **permanently stuck**. The triggering fault is transient; the failure is not. Recovery requires a process restart.

Two independent defects combine to cause this. Either one alone is harmless; together they produce an infinite retry loop.

---

## Bug 1 — Off-by-one: the requester's window and the responder's limit disagree

The requester and the responder both believe they are enforcing "at most `MaxRoundWindow` (= 10) sequences per request," but they measure it in different units. The requester limits a **distance**; the responder limits a **count**. An inclusive range with distance 10 contains 11 items.

### The requester side

When a node notices it is behind, `sendMoreReplicationRequests` computes the range of sequences to fetch (`simplex/requestor.go:157-166`):

```go
// it limits the amount of outstanding requests to be at most [maxRoundWindow] ahead of [currentSeqOrRound].
func (r *requestor) sendMoreReplicationRequests(observedSeqOrRound, currentSeqOrRound uint64) {
	start := math.Max(float64(currentSeqOrRound), float64(r.highestRequested))
	// we limit the number of outstanding requests to be at most maxRoundWindow ahead of nextSeqToCommit
	end := math.Min(float64(observedSeqOrRound), float64(r.maxRoundWindow+currentSeqOrRound))
	...
	r.sendReplicationRequests(uint64(start), uint64(end))
}
```

Concrete example from the failing test run: the lagging node's next sequence to commit is **5**, and its peers are at sequence 15+. So:

- `start = 5`
- `end = min(observed, 10 + 5) = 15`

This satisfies the requester's own rule — nothing more than 10 positions ahead of 5 is requested. The range is then materialized into an explicit list of sequences by `sendRequestToNode`, and the loop is **inclusive on both ends** (`simplex/requestor.go:196-205`):

```go
for i := start; i <= end; i++ {     // i <= end, inclusive
	...
	seqsOrRound = append(seqsOrRound, i)
	r.timeoutHandler.AddTask(i)     // retry timeout registered per seq
}
```

The request for `[5, 15]` therefore contains the sequences **5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 — eleven items**. A span of 10 positions has 11 fenceposts.

### The responder side

The receiving node validates the request by **counting items** (`simplex/epoch.go:2973-2980`):

```go
if len(req.Seqs) > int(e.MaxRoundWindow) || len(req.Rounds) > int(e.MaxRoundWindow) {
	e.Logger.Info("Replication request exceeds maximum allowed seqs and rounds", ...)
	return nil
}
```

`len(req.Seqs)` is 11, `MaxRoundWindow` is 10, so `11 > 10` and the request is dropped. Critically, `return nil` sends **nothing back** — no response, no error. The requester cannot distinguish "my request was rejected" from "the packet was lost."

### The contradiction

| | interprets `MaxRoundWindow = 10` as | result for the example |
|---|---|---|
| requester (`requestor.go:162`) | max **distance** ahead: `end − current ≤ 10` | builds `[5..15]` = 11 items — valid by its rule |
| responder (`epoch.go:2973`) | max **item count**: `len(req.Seqs) ≤ 10` | rejects 11 items — invalid by its rule |

So the requester can emit a request that is fully compliant with its own limit, yet is guaranteed to be silently refused by every honest peer. The maximum-size request the replication mechanism is *designed* to produce is exactly one item too large to ever be answered.

---

## Bug 2 — The retry path aggregates what the initial path deliberately splits

Bug 1 asks: "can an 11-item request be built?" Bug 2 answers: "yes — but only on retry." That is why replication works in normal operation and in every partition/crash test, and only wedges after response loss.

### First attempt: the range is split across peers

The initial send path never puts the whole range in one request. `sendReplicationRequests` divides it among the peers that signed the observed quorum certificate (`simplex/requestor.go:171-182`):

```go
func (r *requestor) sendReplicationRequests(start uint64, end uint64) {
	nodes := r.highestObserved.signers
	numNodes := len(nodes)

	seqRequests := DistributeSequenceRequests(start, end, numNodes)
	...
}
```

`DistributeSequenceRequests` (`simplex/util.go:241`) splits `[5, 15]` evenly across the 3 signers:

- peer A ← seqs `[5, 8]` (4 items)
- peer B ← seqs `[9, 12]` (4 items)
- peer C ← seqs `[13, 15]` (3 items)

Every request is well under the limit of 10. Each sequence also gets a retry timeout registered in the timeout handler (`requestor.go:204`), to be cancelled when that sequence arrives.

### Retry: all missing sequences are re-fused into one request

In the failure scenario, the three responses are lost (in our test, dropped by fault injection; in production, a partition flap or restart at the wrong moment). None of the 11 per-sequence timeouts get cancelled. After `DefaultReplicationRequestTimeout` (5 s, `simplex/epoch.go:31`) the timeout handler fires **once with all 11 missing sequences** and calls `resendReplicationRequests` (`simplex/requestor.go:125-136`):

```go
func (r *requestor) resendReplicationRequests(missingIds []uint64) {
	...
	segments := CompressSequences(missingIds)
	r.sendSegments(segments)
	...
}
```

`CompressSequences` (`simplex/util.go:209`) merges consecutive numbers into contiguous segments. Sequences 5..15 are all consecutive, so the output is **a single segment `[5, 15]`**.

Then `sendSegments` (`simplex/requestor.go:184-190`) sends **each segment whole, to one node**:

```go
func (r *requestor) sendSegments(segments []Segment) {
	numNodes := len(r.highestObserved.signers)
	for i, seqsOrRounds := range segments {
		index := (i + r.requestIterator) % numNodes
		r.sendRequestToNode(seqsOrRounds.Start, seqsOrRounds.End, r.highestObserved.signers[index])
	}
}
```

There is one segment, so one peer receives one request for all 11 sequences. Unlike the initial path, **nothing on the retry path caps or splits the segment size** — `DistributeSequenceRequests` is never consulted. The 11-item request hits the responder's `len > 10` check from Bug 1 and is silently dropped.

### The livelock

From here the loop is closed and can never exit:

1. Request `[5..15]` (11 seqs) sent to one peer → rejected silently (`epoch.go:2973`, `return nil` — no response).
2. No response ⇒ none of the 11 timeouts are cancelled.
3. Timeout fires again after 5 s with the same 11 missing seqs.
4. `CompressSequences` produces the same single `[5, 15]` segment (rotating to a different peer via `requestIterator` doesn't help — every peer enforces the same limit).
5. Go to 1.

Observed in the test logs — the identical rejection every ~5 seconds, indefinitely, while the lagging node's sequence number never moves:

```
10:51:01 "Replication request exceeds maximum allowed seqs and rounds","num seqs":11,"max round window":10
10:51:06 "Replication request exceeds maximum allowed seqs and rounds","num seqs":11,"max round window":10
10:51:11 "Replication request exceeds maximum allowed seqs and rounds","num seqs":11,"max round window":10
...
```

The node remained at seq=5 for the full 120 s test timeout (and would remain forever). Note the network was fully healthy during this entire period — the original fault had already been removed.

---

## Impact

- Any node behind by more than `MaxRoundWindow` consecutive sequences that loses one round of replication responses is **wedged permanently**. No later network healing fixes it.
- The trigger requires no test harness: a brief partition, a restart during catch-up, or ordinary packet loss during recovery is sufficient.
- In a 4-node deployment, one wedged node reduces fault tolerance from f=1 to f=0: the next crash or partition halts consensus entirely.
- The failure is silent on the lagging node's side. Only the *responders* log anything (at Info level), so the operator of the stuck node sees no error.

## Suggested fixes

Any one of these breaks the livelock; (2) fixes the actual defect.

1. **Fix the fencepost** (`requestor.go:162`): express the window as a count — `end = min(observed, current + maxRoundWindow − 1)` — so the maximum inclusive range holds exactly `maxRoundWindow` items.
2. **Chunk segments on the retry path** (`requestor.go:184-190`): in `sendSegments`, split any segment longer than `maxRoundWindow` (or reuse `DistributeSequenceRequests`) before sending, so retries obey the same size discipline as initial requests.
3. **Make the responder tolerant** (`epoch.go:2973`): truncate an oversized request to the first `MaxRoundWindow` entries and answer that, instead of silently dropping it. Partial progress cancels timeouts and unsticks the requester.

Independent of the fix chosen, the silent `return nil` on rejection is worth revisiting — a rejected request being indistinguishable from a lost packet is what turned an off-by-one into an infinite loop.
