# Node-level heartbeat bundling

Antfly batches ready Raft heartbeats across groups with the same complete route.
This complements normalized metadata status reports and runtime references.
Consensus still processes every group's original message, term and read context.

CockroachDB's [2015 MultiRaft explanation](https://www.cockroachlabs.com/blog/scaling-raft/)
describes exchanging heartbeats once per node pair per tick rather than once per
range. This establishes the architectural motivation, not a claim about every
version of CockroachDB's current implementation.

## Paths and ownership

- DataServer sends fresh group/runtime statuses in one `reportNodeStatus` call.
  Cached heartbeats negotiate a distinct endpoint and reference committed runtime
  observations by reporter incarnation and status generation. They preserve
  observation clocks and do not renew embedding activity. Current group Raft facts
  still travel with each heartbeat; missing support or bases require a full report.
- MultiRaft's Ready drain accumulates an outbox and groups messages by peer.
  Its `TransportOutbox.flush` uses hash-indexed peer/group builders.
- `CodecTransportHost.sendPeerBatches` bundles heartbeat-only groups whose peer,
  source identity, protocol, endpoint address and endpoint metadata all match.
  Append, vote and snapshot work keeps the existing scheduling path. Mixed message
  groups are sent individually; buffered work for the same group is flushed first.
- The binary codec, HTTP `/raft/v1/batch` receiver, and inbound host already carry
  multiple groups. A failed bundle becomes independently owned per-group retries.
  Each retry resolves its current `(group_id, peer_id)` route; a removed or moved
  group cannot redirect another group's messages. Retransmission after partial
  receiver admission may duplicate Raft messages, as with ordinary transport loss.

## Production implementation

Every group is resolved before bundling. Frames retain each group's ID, term,
commit index, heartbeat response and read-index context. A generic node liveness
ping never stands in for quorum evidence.

Ready work flushes at the existing round boundary, with no added timer. Bundles
cap at 256 groups, 1,024 messages and 1 MiB of encoded bytes; oversized multi-group
frames split recursively. A single group's existing driver size contract remains.
Route builders are hash indexed. Mixed-message duplicate-group ordering checks
may scan the bounded pending builders; the normal heartbeat grouping path is linear.

The codec transport owns all retries, including failed asynchronous HTTP attempts.
The driver performs one HTTP attempt and returns failed frame ownership on the
next transport round. Unsent bundles are invalidated on endpoint changes/removal;
in-flight attempts may finish on the previously admitted endpoint. The transport
splits failed bundles by group and resolves current routes before retrying.

Codec retry retention caps at 4,096 frames and 8 MiB per host, in addition to
attempt/backoff limits. HTTP queue admission reserves bytes before allocation;
queued, in-flight and failed completions all retain the reservation. Defaults
allow four maximum 32 MiB requests globally and one per peer, plus 64 KiB routing
metadata per request: 128.25 MiB globally, 32.0625 MiB per peer. Existing 4,096/256
global/per-peer frame caps count all retained states too. These HTTP limits are
independent of the codec's 8 MiB retry budget; they are not one combined 8 MiB cap.
`antfly_raft_async_send_retained_bytes` and `antfly_raft_async_send_retained_frames`
include failed completions awaiting transfer. The cap excludes bounded queue/map
metadata and transient encoding/decoding buffers. Exhaustion discards transport work and increments the
exhaustion metric; Raft remains responsible for retransmission. Context-bearing
heartbeats are never deduplicated. New traffic and old retries retain the existing
lossy, potentially reordered delivery contract.

Per-group ticking and election/read semantics remain. Quiescence and true
node-liveness coalescing would require separate correctness work.

## Measurements and acceptance workloads

From `zig/lib/raft`:

```sh
zig build heartbeat-bench -Doptimize=ReleaseFast
```

The benchmark sends one heartbeat for each of 100, 1,000 and 10,000 idle groups
sharing one remote peer through the production routing/encoding host and a
counting driver (`HEARTBEAT_HOST_BENCH`). It separately compares isolated codec
caps of 1, 64 and 256 groups, checking decoded group/term/commit identity outside
timing. Both report seven-sample medians, encoded bytes and frame counts, using
the page allocator. They exclude HTTP, queue residence and network latency;
these are not distributed throughput measurements. Requests in the opposite
direction and additional peers add corresponding work.

The frame-count/byte results are deterministic: at 1,000 groups, one-group frames
produce 1,000 frames / 106,000 bytes; a 64-group cap produces 16 / 88,288; a
256-group cap produces 4 / 88,072. At 10,000 groups, a 256-group cap produces 40
frames / 880,720 bytes versus 10,000 / 1,060,000. Thus the main expected benefit
is fewer HTTP requests, queue items, wakeups and allocations; payload bytes fall
about 17%. Per-group consensus processing and metadata report proposals remain.
See the [recorded observations](system_catalog_report_workloads_2026_09_11.json)
and [measurement limitations](SYSTEM_CATALOG_RESULTS.md#node-level-heartbeat-framing)
for the isolated encoding run; these are not live transport measurements.

For production capacity validation, measure a three-node cluster with
100/1,000/10,000 mostly idle tenant ranges, then repeat with a small hot subset doing writes and
linearizable reads. Record actual frames/bytes per peer, allocation/CPU, queue
residence p95/p99, proposal/read latency, elections and retry retention. Repeat
with one slow or partitioned peer, reconnects, endpoint changes, group removal,
leadership churn, rolling versions and concurrent snapshots. Deterministic tests
cover route changes/removal during retry, retained-byte exhaustion, source and
endpoint metadata isolation, encoded byte limits, group caps, message ordering,
and read-context preservation. A request
count reduction is insufficient if it increases hot-group tail latency or
changes election behavior.

Retry recovery and HTTP scheduling have separate reproducible targets:
`zig build retry-bench -Doptimize=ReleaseFast` in `lib/raft`, and
`ANTFLY_HTTP_SCHEDULER_BENCH=1 zig build antfly-http-scheduler-bench -Doptimize=ReleaseFast`
from `zig`. Retry draining compacts survivors in order in one pass. HTTP scheduling
uses per-peer FIFO queues and a ready-peer list, with one in-flight request per peer
and condition-variable wakeups. These tests retain byte/frame reservations across
queued, in-flight and failed-completion states and cover route invalidation under
backlog. Their timings exclude network latency; use the production capacity
scenarios above to assess hot-group tails under real slow-peer behavior.
