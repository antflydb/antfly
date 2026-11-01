# Independent Cloudflare HTTP measurements

The small approved-control HTTP diagnostic demonstrated a real connection
bottleneck in its Cloudflare load driver and incomplete service tracing. Its
results remain preserved separately. This follow-up implements an external
Node offered-load client and complete service evidence before another hosted
performance comparison. It does not change native serving code, resource
ceilings, approved control modules or the original gate policy.

## Actual targets and previous misses

The original `qualification/gate-policy.mjs` requires read/write/cold p95 at
most **1,000/3,000/5,000 ms**, with paired read/write non-regression. Numeric
readiness alone cannot pass the original three one-hour trials, corpus,
recovery-sample, queue/lag/fault/ANN and independent-coverage requirements.

| Previous candidate population | Result | Actual numeric miss |
| --- | ---: | --- |
| Reverse mixed read | 1,594 ms | Above 1,000 ms |
| Reverse warm read | 510 ms versus control 483 ms | Regression |
| Cold read, both orders | 1,424 / 1,721 ms | Neither exceeds 5,000 ms |
| Mixed write, both orders | 1,703 / 2,326 ms | Neither exceeds 3,000 ms |

The separate design goals are 10× cold and 2× warm/burst/mixed improvement.
Cold missed both relative goals (938.1 / 823.1 ms); warm missed both
(342 / 241.5 ms); reverse burst missed 281.5 ms at 394 ms. Forward burst,
mixed reads and mixed writes met their respective 2× goals. These are small
samples, not a stable speedup guarantee. `http-latency-targets.mjs` reports
absolute limits, paired non-regression and design goals separately.

## Client and execution identity

`node-http-transport.mjs` owns separate native HTTP agents, with 64 connections
per side. It returns headers before consuming the bounded response body, keeps
status/ray/version evidence when the body fails, rejects redirects, and records
physical completion and pool queueing. Tests overlap sixteen real HTTP requests
and exercise oversized, broken and canceled response streams.

`independent-http-runner.mjs` precomputes UUIDs and arrival offsets and records
intent before workload I/O. Both sides share a start clock. Slow replies cannot
shift later arrivals: overload, excessive dispatch lag, transport queueing,
failed body/version/fence checks and journal errors fail the run. Offered
latency includes dispatch delay; service latency is reported separately.
There are no workload retries. Acknowledged writes advance the receipt only
when their LSN is newer. Post-load reads verify the highest acknowledged write
and exact document count; these verification attempts also enter reconciliation.

`independent-http-trial.mjs` runs on a GitHub-hosted Linux machine. An explicit
`--local-diagnostic` supports local debugging but cannot produce an independent
pass. Public plans pin both stacks, versions, source/module/config hashes,
corpus and a fresh execution UUID. Each CI plan expires after one hour. The
workflow claims that UUID with a retained artifact **before workload I/O**,
rejects previously claimed UUIDs and rejects workflow retries. Failed and
partial results remain artifacts. Two side executions are joined even after
failure. The initial profile offers 256 reads at 32 reads/s, with one write per
sixteen reads, across four tenants. Supported corpus sizes are 64/128/512 per
tenant. This bounded profile is not the original hour-long gate.

## Service and resource closure

The observer accepts the explicit `cold-pair-v1` layout: exactly two cold stack
names and app/writer/reader/builder roles for each. Existing gate layouts retain
their original checks. Attaching observation can change only observability
metadata and Tail consumers; runtime limits, vars, storage/bindings and every
sealed JavaScript/WASM payload remain pinned. The approved control still uses
exact `c44896ac8c` modules.

Before workload admission, all eight sources must pass the existing positive
witness readiness protocol. Before/after private boundary witnesses, provider
Tail counts, canonical log/trace records, immutable windows, stable repeat
observations and the app's complete physical-attempt ledger are reconciled.
Quiet services still need their independent boundary evidence. Collection waits
for settlement and is bounded: missing evidence fails, rather than being
replaced by successful client responses. Snapshot paths become relative for
portable artifact download and are checked on restore.

Resource scanning now includes `error_message` and nested `errors` resource
flags, and canonical compaction retains them. A recovered `SQLITE_NOMEM` inside
a final `ok` event fails qualification. This corrects a detection gap; it does
not establish the root cause or resolution of the historical platform-memory
failure. Linear WASM memory still does not measure total isolate/SQLite memory.

The hosted result requires numeric readiness and complete canonical/native
proofs. **It always reports `qualification_ready: false` and
`full_gate_passed: false`.** Account export-delivery history remains local and
must be joined afterward. `reconcile-independent-http.mjs` recomputes all eight
proofs and delivery-history health from downloaded artifacts; missing windows,
errors, changed versions or gaps cannot become complete. The local monitor's
earlier window is conservatively required too, so pre-workload errors are not
discarded. Polling cannot rule out provider errors cleared entirely between
samples. Neither CLI substitutes for the full original gate.

## Operator procedure

Use fresh disposable sealed stacks; the previous trial's stacks were deleted.
Keep approved control payloads immutable. The steps below name local manifests
and output files, and contain no credentials:

```sh
node qualification/prepare-independent-http.mjs pair BASELINE_RUN CANDIDATE_RUN PAIR_JSON
node qualification/paired-observer-lifecycle.mjs prepare PAIR_JSON 1 --windowed-evidence
# Use the emitted OBSERVER_RUN path for seal, deploy, upload-token,
# deploy-reader, deploy-receiver, then attach, in that order.
node qualification/prepare-independent-http.mjs plan BASELINE_RUN CANDIDATE_RUN PUBLIC_PLAN --observer=OBSERVER_RUN
node qualification/monitor-independent-delivery.mjs OBSERVER_RUN DELIVERY_HISTORY
```

Seed the equal corpus and capture final serving versions/receipts after observer
attachment; export the public plan only after that preparation. Plan preparation
requires deployed, seeded, credential-ready, sealed stacks. Avoid an overlapping
workload. Set repository variable `ANTFLY_HTTP_TRIAL_PLAN` to the public JSON.
Configure only the newly generated disposable baseline/candidate/observer tokens
as the three `ANTFLY_{BASELINE,CANDIDATE,OBSERVER}_API_TOKEN` secrets. Do not upload
Wrangler/account credentials or local credential files. Account destination API
calls use official Wrangler authentication transiently in local memory, with no
legacy credential-file fallback.

The optional `ci:cloudflare` suite uses the repository's existing approval and
source-pinning system. **Its workflow and suite registration must first land
on the default branch**; manual workflow dispatch cannot bootstrap a workflow
that exists only on this feature branch. The controller dispatches default-branch
workflows, and normal PR execution needs the existing `/ci run <sha>` approval.
No approval bypass or default-branch merge is performed by these changes.

Keep the local delivery monitor running from before CI readiness through export
settlement. After CI finishes, interrupt it once to join the final status poll,
download the result artifacts, then run:

```sh
node qualification/reconcile-independent-http.mjs DOWNLOADED_RESULT/report.json DELIVERY_HISTORY CLOSURE_JSON
```

A monitor budget of 256 observations and 90 minutes bounds this diagnostic;
window readers retain their original one-hour bounds. This is intentionally
not presented as a full one-hour-plus gate runner. Preserve failed plans,
claims, reports and original evidence. Retire only the explicitly authorized
disposable resources afterward. This implementation/test session created no
Cloudflare resources, account credentials or hosted measurements.


## Validation

613 Node tests and 210 CI policy tests pass. Eleven focused actual-workerd
checks pass across the observer, export receiver and HTTP driver files. An
initial parallel-file run hit an existing Tail burst fixture response-body reuse
error; its failed log is retained, and the isolated test and full observer file
subsequently pass. This is not a claim that every actual-workerd suite ran.
The new workflow passes `actionlint`. Linting the existing orchestrator also
reports its pre-existing `cache-mode`/`concurrency.queue` syntax unsupported by
the installed linter; this change does not add those keys. License and diff
checks pass. Guarded `make gc` skips cleanup while native builds are active or
process inspection is unavailable.

Logs are separate from the previous sealed hosted evidence, under
`examples/cloudflare-serverless/dist/qualification/independent-http-implementation/`.
All failed intermediate logs remain there. No latency improvement is inferred
from these correctness/integration tests.
