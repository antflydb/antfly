# Workload scheduling qualification

This is the release matrix and acceptance policy selected before measuring the
new scheduling policies. No row is qualified yet. Admission unit tests and the
HTTP C80 fixture do not substitute for these workloads.

## Cloud resource envelopes

The Cloud tier catalog at revision `bdc9fe51`, inspected September 17, 2026,
defines these packages in `catalog.go` (`starterIncludedNodeConfig`,
`standardIncludedNodeConfig`, and `proIncludedNodeConfig`):

| Package | CPU per node | Memory per node | Data disk per data node |
| --- | ---: | ---: | ---: |
| Starter | 1 vCPU | 4 GiB | 50 GiB |
| Standard | 2 vCPU | 4 GiB | 100 GiB |
| Pro | 4 vCPU | 8 GiB | 200 GiB |

Each single deployment has one combined node and replication factor 1. Each
replicated deployment has three metadata and three data nodes, replication
factor 3, and a separate 2 GiB metadata disk per metadata node. The selected CPU
and memory values apply per node, not to the six-node cluster as a whole.
Optional hot standby adds a data node; it does not increase primary capacity.

Gate all three single-node packages. Gate the Starter and Pro replicated
topologies for coordination, fan-out, cancellation, fencing, recovery, and
rolling-upgrade behavior. Gate Standard with hot standby for drain/failover and
irrevocable-write recovery. Record actual pod requests/limits, CPU quota,
architecture, disk class, and resource-manager budgets for each run. A local
development configuration is not evidence for a paid package. Container CPU
quota must govern automatic sizing; host CPU count is not its substitute.

The 1-vCPU package cannot promise simultaneous non-yielding execution in both
interactive lanes. Qualify cooperative operators separately, and publish the
overlap exclusion for unaudited/non-yielding operators rather than claiming
latency isolation from weights alone.

## Workloads and measurements

Use the same binary optimization, data, query semantics, and recall setting for
baseline and candidate. Pin their revisions and configuration in every receipt.
The reference is this branch's source baseline with fixed admission explicitly
configured for the load under test; also retain the untouched legacy defaults
as a separate UX comparison. Never count a failed request as low latency.

- Bounded document lookups and selective full-text queries, alone and alongside
  long graph traversals, aggregations, and scans.
- The retained 50K × 1,536-dimensional and 1M × 768-dimensional vector workloads,
  at the same measured recall floor. Select the fastest independently calibrated
  setting that qualifies; retain actual search-work counters. Run warm and cold
  storage cases. If the working set exceeds the package's RAM, retain that fact
  and measure disk-backed execution instead of silently reducing the dataset.
- Mixed reads/writes with ingestion, indexing, and compaction; use both 90/10
  and 50/50 offered operation mixes and report each class separately.
- Slow output consumers, stalled/disconnected streams, slow inference, and
  large valid request bodies; verify count and byte pressure independently.
- Partitioned workers, delayed/canceled attempts, partial fan-out failure,
  coordinator restart, rolling peer versions, and writes interrupted immediately
  after a durable decision. Count unknown outcomes and recovery obligations.

Run C1/5/10/20/30/40/60/80 closed-loop sweeps. Also run open-loop arrivals at
50%, 80%, 100%, 125%, and 200% of the fixed baseline's sustainable completed
rate. Use 60-second warmup followed by 5-minute measurement windows, with three
independent runs per point. Follow a 60-second overload interval with 60 seconds
at 50% baseline rate. Repeat cold-start cases independently of warm runs.

Report offered/completed rates, rejects, retries, timeouts, and unknown outcomes.
Measure p50/p95/p99 from original submission, including all local/server waits
and retries, plus stage timings and per-class completions. Retain cgroup memory
peak, process RSS, memory-manager reservations, CPU throttling, queue peaks,
uncertain attempts, recovery backlog/age, and drain duration. Retain raw samples
and configuration checksums, not just percentiles.

## Numerical gates

These are engineering release gates, not customer latency SLAs. Evaluate each
workload/package pair against its own baseline. The absolute addition below
prevents sub-millisecond measurements from making tiny timer variation dominant.

| Gate | Acceptance threshold |
| --- | --- |
| Low-load p99, C1 and 50% offered rate | Candidate ≤ baseline × 1.10 + 1 ms |
| Useful completed throughput at saturation | Candidate ≥ 95% of qualifying fixed baseline, with identical recall/semantics |
| Fairness under sustained mixed load | Every continuously eligible nonempty class completes work in each 5-second window; separately reserved control/recovery paths continue progressing |
| Protected bounded-read p99 under general-lane saturation | ≤ isolated bounded-read baseline × 2 + 5 ms where concurrent lane isolation is supported |
| Memory safety | No OOM, no hard reservation/count/byte ceiling overshoot; cgroup peak ≤ 90% of memory limit with the qualification resource policy |
| Queue retirement | No execution after deadline; cancellation/expiry removes queue ownership within 50 ms of an observed checkpoint under the tested CPU quota |
| Return from overload | Within 10 seconds, queued client work returns to its pre-burst bound and 50%-load p99 returns to baseline × 1.10 + 1 ms |
| Correctness and cleanup | Zero double releases, leaked local leases, late queued execution, silent partial results, or lost durable obligations; all remote uncertainty reconciled by evidence |

An expected, bounded overload rejection is reported separately from unexpected
request failures. A lower latency achieved by dropping more useful work does not
pass throughput or fairness gates. Quarantined remote uncertainty may outlive the
10-second local recovery target; it must remain bounded, visible, and isolated
from healthy destinations. Never expire its charge merely to pass the metric.

Select fixed limits first. Automatic sizing must pass the same matrix; adaptive
mode must additionally beat or match the qualifying fixed baseline within these
allowances, settle after pressure removal, and fall back safely with missing or
stale samples. Do not enable a new default while any applicable row is untested.
