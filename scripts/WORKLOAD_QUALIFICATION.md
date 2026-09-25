# Local workload qualification harness

`workload_qualification.py` runs paired, fresh Antfly lifecycles using direct
processes or Docker. It currently exercises bounded document lookups, small
match-all queries, identical-value writes with deterministic offered mixes, and
independently calibrated vector queries. It does not qualify graph/aggregation isolation, remote ownership,
durable recovery, or the full release matrix in `zig/WORKLOAD_SCHEDULING_QUALIFICATION.md`.

```sh
python3 scripts/workload_qualification.py template --runtime process --output /tmp/workload-plan.json
# Edit both arms: binary paths, source revisions, optimization mode, and configs.
python3 scripts/workload_qualification.py run /tmp/workload-plan.json --output /tmp/workload-receipts
```

The command exits 1 for request correctness or process failures, including
failures during warmup; it still retains summaries, raw samples, and checksums.
Exit 2 means generator-invalid evidence without a detected correctness failure.
Exit 3 means otherwise-clean requests and load generation lack valid Prometheus
snapshots. Correctness and generator failures take precedence if telemetry also
fails. Expected overload 429s remain separate from unexpected HTTP/transport
failures. Exit 0 establishes clean evidence and valid metrics snapshots for the
exercised subset; it never marks the full release matrix qualified. Inspect
`correctness_failures`, `generator_valid`, `shutdown_clean`, `telemetry_complete`,
and `telemetry_failures` in `summary.json` together.

For a harness smoke test with an existing binary whose build provenance is
unknown, set both arms' `revision` and `optimization` to `"unknown"`. This is
permitted only for `purpose: "smoke"`; artifact SHA-256 hashes are still retained.
The template intentionally uses short smoke windows and explicit fixed admission
capacity. Edit the candidate's `config` to enable the mechanism being tested.
Do not compare different datasets, query semantics, optimization modes, or
unrelated resource-manager policies as if they isolated a scheduler change.

For Docker, create the template with `--runtime docker`, set each arm's image,
and describe the actual host disk/filesystem. The harness resolves image IDs
before launching, sets the selected Starter/Standard/Pro CPU and RAM limits,
disables swap, and verifies effective cgroup-v2 values. The image must include
`/antfly` (or an explicit `container_binary`) and `cat`. CPU and RAM are enforced;
the package's disk size is recorded but this bind-mounted fixture does not enforce
a disk quota. Process runs are always unconstrained host correctness evidence.
The harness disables inherited Docker image health checks and polls its configured
API readiness endpoint. Both process and Docker launches enable the dedicated
health/metrics listener. Process runs choose a separate free host port; Docker
publishes container port 4200 on a separate loopback host port. `runtime.json`
retains the API `port`, `metrics_port`, and exact launch `command`.

Snapshots fetch `/metrics` only from that dedicated listener, validate the content
type and Prometheus sample text, and retain valid responses as `.prom`. HTTP 200
with dashboard HTML is invalid: the raw body is retained as `.metrics-invalid.body`
and the corresponding `.resources.json` records `metrics_valid: false` and the
error. Every snapshot contributes to `telemetry_complete`; missing snapshots are
listed as unmeasured evidence. This flag establishes format and collection coverage,
not the correctness or availability of every metric. Before using ownership or
recovery counters as release evidence, verify the configured capacities and
`antfly_admission_query_diagnostics_available` against the retained configuration.
Historical receipts are never rewritten by these checks.

The native health endpoint refreshes its metrics cache asynchronously every five
seconds. Short smoke points can therefore collect the same cached body despite
intervening requests. For cleanup checks, first observe admission peaks from known
completed work before interpreting idle zeroes; retain the polling observations.
Snapshot collection time is not proof of the underlying measurements' freshness.
The cache cannot establish the subsecond ownership-retirement release gate.

`purpose: "qualification"` requires Docker, matching ReleaseFast builds with
declared full revisions, at least 60-second warmup, 300-second measurement,
three lifecycles, C1/5/10/20/30/40/60/80, open factors .5/.8/1/1.25/2, and
60-second overload/recovery intervals. Even then, the output remains partial
matrix evidence. A revision/mode label is a declaration; retain build receipts
separately to establish that an artifact was built from the claimed source.

Prepared paired plans in `scripts/workload-qualification-plans/` cover Starter
(1 CPU, 4 GiB), Standard (2 CPU, 4 GiB), and Pro (4 CPU, 8 GiB). They declare
baseline source `64f1afbb373d5da0a932f08e456116da139e9e9a`, ReleaseFast for both
arms, all required windows/concurrency points, and three fresh pairs per tier.
Candidate source and both images are intentionally unresolved; validation rejects
the plans until the final committed candidate and independently built artifacts
are supplied. Generate executable copies after retaining source/build receipts:

```sh
python3 scripts/workload_qualification.py release-plans --output /tmp/release-plans \
  --baseline-image antfly:baseline-releasefast \
  --candidate-image antfly:candidate-releasefast \
  --candidate-revision FULL_COMMITTED_CANDIDATE_SHA
python3 scripts/workload_qualification.py run /tmp/release-plans/starter.json --output /tmp/starter-receipts
```

Run the Standard and Pro files separately on an otherwise idle Docker host with
sufficient resources. Record the host storage details in each plan first. These
plans use identical explicit fixed admission envelopes in both arms: query and
write limits of 80 active/160 queued, a 1-second queue wait, queued request bytes
of RAM/64, and retained bytes of RAM/8. This isolates implementation overhead
for document lookups, small queries, and 90/10 and 50/50 read/write mixes.
It does not qualify automatic policy or the legacy default configuration, and
does not exercise vector execution. Do not substitute a convenient existing
binary for a declared build. Generator drops, including drops during overload,
invalidate coverage and require a generator with sufficient capacity before
using the results as qualification evidence.

For a separate deterministic vector smoke fixture:

```sh
python3 scripts/workload_qualification.py template --runtime docker --vector --output /tmp/vector-plan.json
# Pin both image arms, revision/mode declarations and host storage description.
python3 scripts/workload_qualification.py run /tmp/vector-plan.json --output /tmp/vector-receipts
```

The synthetic fixture generates normalized float32 vectors and separate query
vectors, computes exact cosine neighbors over the entire small corpus, and
retains the corpus digest, query vectors, ground truth and split indices. This
fixture is explicitly separate from the retained benchmark datasets and cannot
be used with `purpose: qualification`.

Prepared vector plans in `scripts/workload-vector-qualification-plans/` cover
50K × 1,536 and 1M × 768, both at top-100, on each of the three tiers. Generate copies with
`release-plans --vectors` and the same image/revision arguments above. These
plans additionally require paths and SHA-256 hashes for the original retained
`shuffle_train.parquet` (`id`, `emb`), `test.parquet` (`emb`), and
`neighbors.parquet` (`neighbors_id`) files plus their original receipt reference.
Use an environment with PyArrow installed, as the existing VectorDBBench
profilers do. The harness streams training batches, verifies complete training
ID coverage and exact row counts, and checks file hashes before and after the
experiment. It retains the selected query/neighbor rows and input provenance.
Do not substitute another dataset with the same dimensions. The large plans
are specifications, not evidence that these workloads were run.

Each fresh vector lifecycle tests an explicit effort grid including 0, .01,
.025, .05 and higher values through 1. Every effort receives warmup and at least
three timing trials (at least 10 seconds each in full plans); order rotates/reverses between trials. Selection uses the
highest median completed QPS among efforts with zero errors and at least 95%
recall in **every** calibration trial. Trial rates and ranges expose timing
noise. This is the fastest observed grid setting at the declared calibration
concurrency (30 in full plans), not proof of the globally optimal setting or of
generator headroom. Calibration uses bounded closed-loop workers and records
every attempted query; failed/rejected requests invalidate that setting.

The chosen setting must then pass a disjoint held-out query split at 95% recall;
failure stops the lifecycle without retuning against the held-out answers.
Performance points reuse only held-out queries and freeze the chosen effort for
that arm/lifecycle. Raw responses retain result IDs and measured recall; point
comparisons also require the recall floor. Separate profiled probes retain
actual search-work counters without including profiling overhead in calibration
timings. Flat recall, identical sampled IDs, and identical available work counters
are reported separately; they are diagnostic observations, not a product-bug
verdict. Current vector plans measure warm storage only. Cold storage, mixed
vector/ingestion isolation and the remaining release matrix still require
separate experiments.

The first baseline's fastest error-free measured closed-loop rate fixes all
open-loop offered rates for that workload. Later pairs alternate arm order.
Open arrivals retain their original scheduled submission time. The generator
bounds active and queued work; dropped arrivals invalidate generator coverage
and are reported rather than hidden. Successful latencies exclude rejected
requests, while rejects, transport errors, invalid results, late responses, and
unknown write outcomes remain separate counts. No automatic retries occur.
The overload-to-recovery transition retains the same executor, connections, and
outstanding requests; recovery arrivals start without first draining old work.
Smoke runs do not evaluate numerical acceptance gates.

Receipts include the immutable plan, harness copy, artifact/config checksums,
raw per-request JSONL samples, per-class latency/completion summaries, raw
Prometheus snapshots, cgroup resource snapshots, logs, and shutdown outcomes.
`checksums.json` covers evidence files; retained database files are excluded.
The harness's closed-loop comparisons only assess observed C1 latency and
error-free completed throughput. Overload and recovery samples require further
analysis against queue/ownership metrics before claiming the recovery gate.

Run the offline harness checks with:

```sh
python3 -B -m unittest discover -s scripts -p 'test_workload*qualification.py'
```

## Explicit operator and output scenarios

A workload with `kind: "scenario"` provides `setup` requests and an `operations`
list. Each operation declares a unique class, integer weight, `is_write`, method,
local `/db/v1/` path, body, and an exact successful HTTP status plus JSON checks.
Checks use a list of object keys/array indices in `path` and one of `equals`,
`length`, or `sorted_equals`. Query error payloads fail even when HTTP is 200.
Setup writes run once; failed or ambiguous writes are never replayed. Operation
weights determine offered mix independently of completion. Unknown outcomes from
custom write class names retain `unknown_write_outcome`.

`workload_scenarios.mixed_fixture(rows=4096, read_percent=90, graph_depth=32)`
generates a separate deterministic fixture with selective text, graph-chain
traversal, sum aggregation, document lookup, and identical-value indexed writes.
Use `read_percent=50` for the other declared mix. The request shapes come from
repository integration tests; exact preflight assertions verify the installed
fixture before measurements. This fixture is not the retained vector/graph
benchmark and its graph operation is not certified long-running by its name.
Choose and freeze row count/depth before qualification, retain observed operator
latency, and pair identical fixture plans. Generated setup and request assertions
are part of the checksummed plan. The generic scenario schema also accepts
explicit checked operations over externally prepared workload-specific fixtures.

A scan operation can declare `stream` with `mode: "drain"` or `"disconnect"`,
`chunk_bytes`, `pause_seconds`, and `max_bytes`. Drain additionally requires the
predeclared exact `sha256` of the expected complete response. Only the keys scan
endpoint accepts this transport mode. Connect, body reads, and pauses consume
the original submission deadline. A deliberate disconnect is recorded separately
and never counts as completed useful work. It does not itself prove server-side
retirement; inspect the retained ownership metrics or deterministic fault tests.
Dedicated disconnect-only scenarios are correctness probes, not a source of a
sustainable throughput baseline.

## Periodic resource and recovery evidence

Opt into periodic evidence with a plan `telemetry` object:

```json
{
  "interval_seconds": 1,
  "ceilings": {"EXACT_EXPORTED_SERIES_WITH_LABELS": 32},
  "queue_metrics": ["EXACT_EXPORTED_QUEUE_SERIES_WITH_LABELS"],
  "recovery_queue_bound": 0
}
```

Replace the names and limits using the frozen resource policy and actual exported
series; missing names fail closed. Queue bounds must be chosen before the run
from the declared pre-burst policy, not fitted to observed overload results.
Every point retains `.telemetry.jsonl` with raw Prometheus text, raw cgroup data,
monotonic polling timestamps, and failures. A separate sampler polls the dedicated
health listener and Docker cgroup `memory.current`, kernel `memory.peak`, enforced
`memory.max`, and `memory.events` at intervals no longer than one second. The
kernel peak catches memory spikes between polls. Process runs remain useful for
correctness, but have no claimed cgroup qualification coverage. Missing samples
or coverage gaps over two seconds produce unavailable evidence. A peak above
90% of the enforced memory limit or any OOM event fails the memory gate. Declared
series ceilings apply to observed samples; invisible gauge excursions still need
runtime invariants/counters and cannot be disproved by sampling.

HTTP poll frequency does not establish underlying metric freshness: the health
server historically caches metrics for five seconds. Ownership/recovery timing
requires `X-Antfly-Metrics-Age-Ms` (plus full scrape duration) or an actual
`antfly_metrics_collected_timestamp_seconds` source timestamp and sample age no
older than one second. Missing/stale freshness marks those gates unavailable.
Raw body hashes, source-age evidence, and collection-versus-receipt times are
retained; identical values alone never prove either freshness or staleness.
The independent kernel cgroup peak verdict remains available when metrics are
cached. A faster HTTP polling loop cannot qualify a five-second cached source.

The recovery evaluator attributes latency to original submission and requires
every one-second window from ten seconds after pressure removal through the end
of recovery to meet baseline 50%-load p99 × 1.10 + 1 ms and the declared summed
queue bound. Rejections do not improve the verdict; missing completions or queue
samples are unavailable. Class progress reports continuously backlogged client
windows separately from server eligibility, which requires runtime evidence.
These results are per-point evidence, never a full-matrix qualification verdict.
Missing periodic gate evidence uses exit 3; measured numerical gate failure uses
exit 4. Request correctness failures (exit 1) and invalid generators (exit 2)
retain precedence. Fault schedules and remote/durable reconciliation evidence
belong to the separate cluster correctness runner; no Cloud-sized machine is
required to execute those correctness schedules.

## Small process fault correctness

`workload_cluster_qualification.py` is a separate correctness runner. It requires
no Cloud-sized resources, containers, or Kubernetes. Its default local topology
uses one metadata process, one data owner, and one API-only process, following
the native integration fixture. Each node receives separate ports, config,
persistent data directory, and logs. Plans may also use standalone processes or
other bounded metadata/data arrangements; the template does not establish a
replicated Raft topology by merely increasing the node count.

```sh
python3 scripts/workload_cluster_qualification.py template --output /tmp/fault-plan.json
# Pin each binary path, actual SHA256, source revision, optimization and node config.
# Add exact checked setup operations and predeclared fault/request actions.
python3 scripts/workload_cluster_qualification.py run /tmp/fault-plan.json --output /tmp/fault-receipts
```

`setup` contains synchronous `request` actions with node, method, path, body,
`is_write`, and exact expected status/semantic checks, using the scenario check
format above. Timed `actions` have a monotonic `at` offset after setup and support
`request`, `submit` (with a unique `id`), `await` (that ID), `pause`, `resume`,
`stop`, `kill`, `restart`, `ready`, and `discover`. Restart preserves the data
directory; an optional artifact name selects another pinned binary for rolling
version tests. No arbitrary shell commands or external process identifiers are
accepted. A process is killed only when owned by the current run. Cleanup resumes
paused processes before graceful termination, records every exit, and fails on
unexpected crashes or forced shutdown. Every asynchronous request is joined and
its original timeout preserved. Schedule lateness beyond the declared bound
invalidates the run instead of pretending the planned overlap occurred.

Discovery sends an internal service identity `node:<coordinator>` and verifies
the signed response's issuer, coordinator, destination, worker incarnation,
protocol version, and fresh nonce. It requires a binary implementing the node
control route and explicit worker configuration; the template leaves that
configuration disabled. The runner mints a disposable fixture-only credential,
passes it to its children, and retains it in a mode-0600 receipt for offline
verification. Do not reuse this credential outside the local experiment.

A deliberately expected transport failure does not resolve an unknown write
outcome: these remain explicit unresolved obligations and fail correctness until
a dedicated durable-evidence validator is added. Pausing a process is distinct
from partitioning a network path. A kill scheduled by elapsed time is distinct
from a kill proven immediately after a durable decision. Those unexercised
properties stay in `unmeasured`, and `performance_qualified` and
`release_qualified` always remain false. Receipts retain artifact checksums,
commands, PIDs/generations, exact fault and request times, responses, node logs,
shutdown outcomes, and a checksum manifest even when setup or assertions fail.

### Advertised worker traffic faults and signed evidence

For a bounded local diagnostic, set `proxy_capture_response_bytes: 16384`
on a node with `proxy_api: true` (accepted range 1–16384; disabled by default).
The relay retains only the first response prefix per connection, including a
status line, allowlisted content headers and signed workload evidence, and a
bounded body prefix. It records received/captured byte counts and truncation;
incomplete headers are not emitted. Authorization and cookie headers are
excluded, and the disposable local signing secret is redacted. Capture also
observes discarded responses, but is not a parser for persistent/pipelined
exchanges and does not itself verify signatures or prove terminal retirement.
Complete single-message captures also retain an independently capped first-request
prefix with only its method/target, signed attempt header, and exact tiny body.
Authorization remains excluded. `workload_proxy_evidence.verify_exchange` checks
request HMAC/digest, expected coordinator/destination, nonce-bound discovery, and
exact terminal or fence proof. Its `verify_generation_closure` requires matching
namespace/epoch, a fence covering the old generation, and a later generation.
Truncation, duplicate headers, chunked framing and pipelined exchanges are rejected
for proof purposes. Observing a terminal response that the relay discarded proves
worker termination; it does not prove that the coordinator received or retired it.

Set `proxy_api: true` on a data node to bind a separate owned relay and pass its
URL as `--api-advertise-url`. Metadata then advertises the relay to coordinators;
the real API listener remains separate for readiness and direct diagnostic
requests. `proxy_checkpoint` records counters under an `id`. `assert_proxy`
requires that checkpoint plus positive `minimums` deltas (for example
`forwarded_upstream_bytes` and `forwarded_downstream_bytes`) and optional
`paths_include`/`path_prefixes_include`. This distinguishes actual coordinator
routing from a proxy that was launched but never used. A request's explicit
`via_proxy: true` selects the same relay directly; receipts distinguish these
manual requests from public coordinator requests.

Timed network actions target that advertised worker API:

- `partition` resets existing relay connections and rejects new ones.
- `delay` with `delay_ms` delays newly received chunks in both directions.
- `drop_response` forwards requests but discards worker response bytes. Those
  bytes and the forwarded request bytes are counted separately.
- `heal` removes all policies; it cannot restore bytes already discarded.

Policy application is acknowledged by the relay before its action completes.
One event-loop thread owns each relay, with at most64 connection pairs and64KiB
pending per direction per pair. Backpressure stops reading at that bound.
Healthy delayed connections drain queued bytes before forwarding EOF. Source
addresses, observed first HTTP request lines, connection events, fault times,
and counter snapshots are retained. This is an API endpoint fault, not a Raft
network partition or a per-packet latency model.

A `discover` action can save its verified result under `id: "worker"`. A later
`request` or `submit` can declare:

```json
{
  "id": "attempt-one",
  "attempt": {
    "from_discovery": "worker",
    "generation": 1,
    "sequence": 1,
    "operation": 7
  }
}
```

The runner signs the exact method, target and body using its disposable local
credential. Protocol3 binds the worker's durable namespace and monotonic epoch,
plus coordinator, destination, generation, sequence and operation. Lost responses
leave the fixture attempt explicitly `unproven`. `attempt_status` or
`close_generation` with `attempt_ref: "attempt-one"` verifies the returned
terminal/fence signature before retiring that fixture debt. Terminal evidence
also binds the HTTP status and exact response-body digest; fences must cover the
requested generation as quiescent in the same namespace and epoch. A successful
unsigned control response fails. A changed namespace, restart, elapsed deadline,
or healthy endpoint never clears debt. Any remaining unproven fixture attempt
fails the run. These manual protocol obligations do not attest the production
coordinator's ledger; that needs a public distributed query and its own retained
metrics/retirement evidence. Protocol3 control behavior requires the rebuilt
candidate; healthy advertised routing and transport faults can be checked on the
older frozen binary independently.

### Preselected candidate policy and overhead comparisons

`scripts/workload-qualification-plans/` retains the identical-config overhead
comparison: both arms have 80 foreground slots and 160 queued requests per class.
It does not enable the shared read scheduler. The separate
`scripts/workload-fixed-policy-plans/` contains preselected candidate policies
against that same fixed baseline 64f. These are prepared inputs, not results;
null image/revision fields deliberately prevent execution until artifacts are
pinned. No production default is changed.

| Candidate setting | Starter | Standard | Pro |
| --- | ---: | ---: | ---: |
| Enforced container CPU / memory |1 /4 GiB |2 /4 GiB |4 /8 GiB |
| Read runnable / outstanding / queued |1 /160 /160 |2 /160 /160 |4 /160 /160 |
| Participating read workspace |64 MiB |64 MiB |128 MiB |
| Audited suspended native reads |8 |16 |32 |
| Ingress total count / bytes |512 /512 MiB |512 /512 MiB |512 /1 GiB |
| Reserved recovery count / bytes |4 /4 MiB |4 /4 MiB |4 /4 MiB |
| Session bytes / transaction completion bytes |64 MiB /16 MiB |64 MiB /16 MiB |64 MiB /16 MiB |

Ingress totals include the nonborrowable control floor (2 requests /256 KiB)
and recovery floor. Foreground query/write limits remain identical between
arms. Read queuing has a fixed 1000ms ceiling. The native default backend is LSM;
these cells explicitly leave protected LMDB probes and scan snapshot suspension
disabled. They cannot qualify those features. Single-node remote coordination
is also disabled; separate correctness topology cells opt in.

Generate the retained lookup/mixed plans, or materialize separate 4096-row
checked graph/text/aggregation 90/10 and 50/50 mixes:

```sh
python 3 scripts/workload_fixed_policy.py --output /tmp/fixed-policy-plans
python 3 scripts/workload_fixed_policy.py --operators --output /tmp/operator-policy-plans
```

The operator fixtures have exact semantic expectations, but are not declared
long-running until measurements demonstrate that property. Repeated identical
writes do not certify sustained ingestion or compaction. These cells do not
replace retained 50 K×1536 /1 M×768 vector datasets, cold-storage, slow-consumer,
protected-lane or durable-decision fault qualification. Both performance arms
still require ReleaseFast, three fresh lifecycles,60s warmup,300s measurement,
the full concurrency/rate schedule and the unchanged latency, throughput,
90%-memory and 10s recovery gates.

Telemetry can be declared per arm (`arms.baseline.telemetry` and
`arms.candidate.telemetry`), overriding the common top-level specification.
`expected` maps exact exported policy gauges to required values; a disabled or
differently configured scheduler fails even when usage remains below all
`ceilings`. Missing series remain unavailable. Candidate collection is 250ms,
but every sample still needs observed source age at most 1s. Legacy baseline
cache freshness may remain unavailable; selecting different series never
waives freshness or qualifies that missing timing evidence. Recovery requires
all declared queue series to return to the preselected zero bound. Sampled
ceilings do not prove absence of excursions between samples.

`local-correctness.json` is a separate three-process metadata/data/API template
with tiny finite budgets,2 runnable /8 outstanding read tasks,8 worker records,
4 coordinator attempts,2 MiB journals and reserved recovery ingress. Worker
capacity includes coordinator-closure identities; retained terminal tombstones
need not return to zero. The fixture table declares immutable recovery protocol 1
with 4 obligations /4 MiB total /1 MiB per transaction; the maximum transaction
plus 64 KiB fits each half of its 8 MiB completion reserve. The runner creates only
disposable local credentials and persistent directories owned by the run.

After pinning a fresh homogeneous binary, its hash and source revision, run it
with `workload_cluster_qualification.py run ... --output ...`. It checks signed
worker discovery, real advertised API proxy routing, partition/heal and API
restart with previously committed data. It remains a correctness cell: no
Cloud resource envelope is required, no performance result is inferred, and it
does not establish coordinator reconciliation or interruption immediately after
a durable decision. Those require distinct fault schedules and evidence. The
prepared inputs have offline structural tests; native config parsing and live
candidate execution remain pending until a matching build is supplied.

### Production coordinator reconciliation cell

`workload-fixed-policy-plans/local-reconciliation.json` adds a distinct
production-path schedule. It uses public reads through the API coordinator and
advertised worker proxy, with one coordinator attempt available. Each response
loss must produce HTTP 503, forwarded requests and discarded response bytes,
and a fresh sampled coordinator-record count of one. Healing must restore the
exact document and a coordinator-record count of zero. Separate cycles restart
the worker and then the API process on their existing persistent roots. This
plan still requires a newly pinned candidate build and has not been run.

A `metrics` action polls the node's dedicated health listener. It declares
`timeout_seconds` (at most 30), `interval_seconds` (0.1–1),
`max_age_seconds` (at most 1), `stable_seconds`, exact `expected` series and
optional hard `ceilings`. Its absolute deadline starts at the original scheduled
action time, so dispatch delay consumes its budget. Missing, malformed or stale
samples cannot satisfy the stability window. A hard ceiling violation fails
immediately. Every raw body and age observation is retained in `events.jsonl`,
including HTML and failed samples; this is sampled state, not proof of unseen
transitions between samples. The candidate requests 250 ms metric collection;
source age plus scrape duration must still meet the declared limit.

A signed `discover` action may include:

```json
{
  "compare": {
    "previous": "initial",
    "namespace": "same",
    "epoch": "increased"
  }
}
```

The previous result must already have a verified signature. Coordinator,
destination, protocol and durable namespace must match exactly; the epoch must
increase after worker restart or remain `same` after API-only restart. A new
namespace or epoch rollback fails instead of manufacturing retirement evidence.

The production schedule reserves 21 seconds between restart dispatch and its
next assertion; the existing lateness gate remains enforced. Its observed
production ledger is separate from the runner's manual
`fixture_protocol_obligations`. Fresh aggregate counts and signed discovery do
not bind individual attempts or independently prove signed generation closure;
that requires per-attempt/fence receipts. Durable write decision interruption,
namespace-loss recovery and replicated failover remain separate cells. Worker
terminal tombstones are intentionally not required to return to zero.

For a diagnostic cell, `observe_unknown_setup: true` retains bounded read-only
catalog observations before cleanup when setup returns an unknown write outcome.
It makes at most three GET requests, each with a two-second ceiling, against the
public catalog, affected table and owned metadata catalog. These observations
never retry the mutation or classify its outcome as resolved. Explicit
`X-Antfly-Raft-Mutation-Outcome: unknown-v1` responses remain unknown even when
the configured HTTP status was expected; unknown setup stops the run.
