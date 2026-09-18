# Local workload qualification harness

`workload_qualification.py` runs paired, fresh Antfly lifecycles using direct
processes or Docker. It currently exercises bounded document lookups, small
match-all queries, and identical-value writes with deterministic offered mixes.
It does not qualify vector recall, graph/aggregation isolation, remote ownership,
durable recovery, or the full release matrix in `zig/WORKLOAD_SCHEDULING_QUALIFICATION.md`.

```sh
python3 scripts/workload_qualification.py template --runtime process --output /tmp/workload-plan.json
# Edit both arms: binary paths, source revisions, optimization mode, and configs.
python3 scripts/workload_qualification.py run /tmp/workload-plan.json --output /tmp/workload-receipts
```

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

`purpose: "qualification"` requires Docker, matching ReleaseFast builds with
declared full revisions, at least 60-second warmup, 300-second measurement,
three lifecycles, C1/5/10/20/30/40/60/80, open factors .5/.8/1/1.25/2, and
60-second overload/recovery intervals. Even then, the output remains partial
matrix evidence. A revision/mode label is a declaration; retain build receipts
separately to establish that an artifact was built from the claimed source.

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
python3 -m unittest discover -s scripts -p test_workload_qualification.py
```
