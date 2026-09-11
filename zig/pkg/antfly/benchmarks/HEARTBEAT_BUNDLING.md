# Node-level heartbeat bundling investigation

Antfly would benefit from completing transport batching across groups that share
a node. This is a separate transport change from normalizing metadata status
reports. The catalog PR includes a reproducible framing experiment; it does not
change Raft's heartbeat or retry semantics.

CockroachDB's [2015 MultiRaft explanation](https://www.cockroachlabs.com/blog/scaling-raft/)
describes exchanging heartbeats once per node pair per tick rather than once per
range. This establishes the architectural motivation, not a claim about every
version of CockroachDB's current implementation.

## What Antfly does today

- DataServer sends a store's group/runtime statuses in one `reportNodeStatus`
  call. Its cached heartbeat preserves observation clocks and status generation;
  it does not manufacture fresh Raft evidence or renew embedding activity.
- MultiRaft's Ready drain accumulates an outbox and groups messages by peer.
  Its `TransportOutbox.flush` currently searches peer/group builders linearly.
- `CodecTransportHost.sendPeerBatches` then **splits multi-group batches back into
  single-group frames**, even when all routes point to the same endpoint. The
  `codec transport host sends multi-group peer batches as isolated frames` test
  explicitly asserts this behavior.
- The binary codec, HTTP `/raft/v1/batch` receiver, and inbound host already carry
  multiple groups. The codec retry record, however, identifies one group, and
  endpoint refresh is keyed by `(group_id, peer_id)`. Removing the split alone
  would let one group's route control retries for unrelated groups.

## Production implementation

Use bounded frames keyed by destination node, source/authentication identity,
protocol, endpoint address, and endpoint metadata. Resolve every group before
bundling; a shared numeric peer ID alone does not establish a shared route.
Retain each group's ID, term, commit index, heartbeat response and read-index
context. A generic node liveness ping must never stand in for quorum evidence.

Flush the ready work at the existing round boundary, with byte/message/group
limits and a short maximum residence time. Start by batching existing messages;
do not add a timer that delays proposals, votes or read-index requests. Give
control messages an appropriate budget alongside bulk replication and snapshots.
Use hash-indexed builders so grouping is proportional to messages, rather than
quadratic in the number of groups sharing a peer.

Queue/retry ownership must retain the member groups, bounded retained bytes,
and route information. On route change/removal, re-resolve and partition pending
work by current routes instead of retrying an old mixed frame through its first
group. Preserve per-group message order, isolate blocked peers, and make partial
inbound admission/retransmission safe. Context-bearing heartbeats cannot be
silently deduplicated with ordinary liveness messages. Use existing transport
backpressure; do not introduce an unbounded per-peer aggregation queue.

Initially retain per-group ticking and election/read semantics. Quiescence and
true node-liveness coalescing are later changes requiring distinct correctness
proofs; bounded wire batching can deliver request-count savings first.

## Measurements and acceptance workloads

From `zig/lib/raft`:

```sh
zig build heartbeat-bench -Doptimize=ReleaseFast
```

The benchmark encodes one heartbeat for each of 100, 1,000 and 10,000 idle groups
sharing one remote peer. It compares the current one-group framing with proposed
caps of 64 and 256 groups, and checks decoded group/term/commit identity outside
timing. It reports seven-sample encoding medians, encoded bytes and frame counts.
It uses the existing binary codec and page allocator, not the live HTTP driver;
encoding timings are not RPC latency or measured production throughput. Requests
in the opposite direction and additional peers add corresponding work.

The frame-count/byte results are deterministic: at 1,000 groups, one-group frames
produce 1,000 frames / 106,000 bytes; a 64-group cap produces 16 / 88,288; a
256-group cap produces 4 / 88,072. At 10,000 groups, a 256-group cap produces 40
frames / 880,720 bytes versus 10,000 / 1,060,000. Thus the main expected benefit
is fewer HTTP requests, queue items, wakeups and allocations; payload bytes fall
about 17%. Per-group consensus processing and metadata report proposals remain.

Before enabling live bundling, measure a three-node cluster with 100/1,000/10,000
mostly idle tenant ranges, then repeat with a small hot subset doing writes and
linearizable reads. Record actual frames/bytes per peer, allocation/CPU, queue
residence p95/p99, proposal/read latency, elections and retry retention. Repeat
with one slow or partitioned peer, reconnects, endpoint changes, group removal,
leadership churn, rolling versions and concurrent snapshots. Add deterministic
failure tests for mixed-route batches and route changes during retry. A request
count reduction is insufficient if it increases hot-group tail latency or
changes election behavior.
