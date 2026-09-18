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
Expected overload 429s remain separate from unexpected HTTP/transport failures.
Exit 0 establishes only clean evidence for the exercised subset; it never marks
the full release matrix qualified. Inspect `correctness_failures`,
`generator_valid`, and `shutdown_clean` in `summary.json` together.

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
The harness disables inherited image health checks and polls its configured API
readiness endpoint; the production image's separate health port is disabled in
these fixtures.

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
