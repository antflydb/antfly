# Cloudflare qualification procedure

This is the current operator and acceptance reference for #1278. See
[architecture](cloudflare-serverless.md), [durability](antfly-object-storage-backend.md),
[performance findings](cloudflare-platform-baselines.md) and
[unresolved memory investigation](cloudflare-sqlite-memory.md). Local correctness,
short hosted diagnostics and the full performance gate are separate verdicts.
The full hosted gate remains unpassed.

## Immutable control and acceptance policy

Use corrected control `c44896ac8c13898c549b7896ab633b1b8b25c04c` with its original
retained modules, not a rebuild. Its full/compact SHA-256 values are:

- `563936db3958bdab50c404db4a6ccef15e095e798e49198ab2304d87322dac97`
- `789469d743cf69bb9cdcc3e3c59caa4f05663fe4e73958908800d780b1d85f77`

The approved corrections preserve caller context and suspension-aware clock behavior,
resource ceilings, batching policy and native algorithms. Actor admission remains
one active job, eight pending jobs/four MiB/one MiB per job, with 30-second deadlines.
Mutation admission remains sixteen pending jobs/four MiB/one MiB per request; groups
have sixteen callers/768 KiB/1,024 records, a two-ms window and five-second aging.
The caller patch and machine-readable hashes remain under `qualification/` because
control verification and safe local artifact retention depend on their provenance.
They are not permission to modify a frozen control or repair failed results.

`qualification/gate-policy.mjs` is the executable policy. It requires:

| Requirement | Acceptance |
| --- | --- |
| Trials | Three distinct paired one-hour executions with fresh identities/corpora |
| Corpus | Four tenants, 512 documents each, exact bodies and replay receipts |
| Reads | At least 1,000 per side; candidate p95 ≤1,000 ms; paired non-regression |
| Writes | At least 300 per side; durable ACK p95 ≤3,000 ms; paired non-regression |
| Recovery | 100 distinct candidate epochs per trial; one attempt each, correct count, p95 ≤5,000 ms |
| Maintenance | Publication convergence; lag/backlog/age within policy; zero DLQ |
| Errors | No client/resource failures, including recovered or late allocation errors |
| Coverage | All eight role streams and complete canonical/native population reconciliation |
| Faults/ANN | Required failure evidence and equal-budget synthetic/model ANN evidence |

The fixed 30-second arrival cadence, publication lag ≤4,096, backlog ≤128 and
oldest age ≤900,000 ms remain unchanged. Per-tenant lag coverage spans all 60
minutes and queue growth must remain bounded. ANN compares probes 8/32/all on
at least 10,000 synthetic and 1,000 model-embedding documents, with required sample,
recall and physical I/O checks. `evaluateWarmPrerequisites` defers only cold samples;
it does not relax the other requirements or produce a final pass.

Tenfold cold and twofold warm/burst/mixed improvement are separate design goals.
Report them separately from absolute limits and paired non-regression.

## Retained tooling

Use one primary path per task:

- `read-state-latency.mjs`: local actual-workerd serving comparisons and inline profiles.
- `cold-pilot.mjs`: bounded sealed control/candidate stack lifecycle for short HTTP trials.
- `prepare-independent-http.mjs`, `independent-http-trial.mjs`,
  `reconcile-independent-http.mjs`: public plan, dedicated external client and offline closure.
- `paired-observer-lifecycle.mjs`: eight-role observation/export lifecycle and positive witnesses.
- `prepare-gate.mjs`, `gate-lifecycle.mjs`, `run-gate.mjs`, `evaluate-gate.mjs`:
  original full-gate preparation, bounded operations, execution and evaluation.
- `platform-baseline-lifecycle.mjs`: isolated primitive/pressure diagnosis, not an Antfly gate.
- `provider-contracts.mjs`, native tests, `r2-scale.mjs` and ANN fixtures:
  separate provider, scale and quality qualification.
- `prune-local-artifacts.py`: dry-run-first local artifact retention; never credential discovery.

Shared modules implement workload scheduling, transport, source sealing, error
classification, collection, independent coverage, source/API scoping and cleanup.
Their small modules and regression tests are maintained implementation dependencies,
not separate experiment entry points. Superseded one-off programs and chronological
design reports are not maintained in the current tree. Git history preserves source;
local failed evidence remains immutable and rebuilds are not assumed byte-identical.

## Short rehearsal before the gate

Use fresh sealed stacks and a quiet dedicated client. Preserve exact source/module,
configuration, corpus, role versions and approved-control hashes. Prepare the control
from its separate clean checkout and retained original modules; create the candidate
from the reviewed current source. Both have equal corpus size and unchanged ceilings.
Do not reuse consumed execution IDs or reserved cold epochs.

From `examples/cloudflare-serverless`, `cold-pilot.mjs prepare ACCOUNT_ID
--documents=512` prepares the candidate; add `--control-source=CONTROL_EXAMPLE_DIR`
for the exact approved control. Prepare emits the scoped `run.json` path. Run
`seal`, then `deploy` and the explicitly authorized disposable `upload-token` action
with that manifest. These create paid resources; preparation/sealing alone are local.
The lifecycle removes queues/triggers/AI bindings for this bounded diagnostic.

```sh
node qualification/prepare-independent-http.mjs pair BASELINE_RUN CANDIDATE_RUN PAIR_JSON
node qualification/paired-observer-lifecycle.mjs prepare PAIR_JSON 1 --windowed-evidence
```

Using the emitted observer manifest, seal/deploy it, install its authorized new token,
deploy reader/receiver and attach observation in lifecycle order. Observe all eight
roles with positive readiness witnesses before workload admission. Seed both equal
corpora after attachment, verify versions/receipts, then export the fresh public plan:

```sh
node qualification/prepare-independent-http.mjs plan BASELINE_RUN CANDIDATE_RUN PUBLIC_PLAN --observer=OBSERVER_RUN
node qualification/monitor-independent-delivery.mjs OBSERVER_RUN DELIVERY_HISTORY
```

The initial independent profile offers 256 reads at 32 reads/s, with a write every
sixteen reads, across four tenants. Each side owns a 64-connection native HTTP pool.
Arrival times are precomputed; slow responses cannot shift later arrivals. Record
intent durably before workload I/O and terminal outcomes afterward. Overload,
dispatch lag >25 ms, transport queueing, failed bodies/fences/versions and journal
errors fail the population. No workload retries. Join all physical peers on failure.
Final verification checks the latest acknowledged mutations and exact counts.

A dedicated GitHub-hosted Linux runner is supported by
`.github/workflows/cloudflare-qualification.yml` and the existing `ci:cloudflare`
admission system. **The workflow/suite registration must first land on main**;
GitHub cannot dispatch the workflow while it exists only on this feature branch.
Normal source approval and pinned checkout still apply. Do not bypass admission
or merge the unfinished runtime just to enable a diagnostic.

Set `ANTFLY_HTTP_TRIAL_PLAN` to the fresh public plan and install only newly generated
disposable baseline/candidate/observer API tokens in the named CI secrets. Never
upload account credentials, Wrangler tokens or a legacy token file. The plan is
source-pinned, expires after an hour and is claimed in a retained artifact before
workload I/O. Retries/replays reject the consumed execution ID. Failed/partial runs
remain artifacts. Local `--local-diagnostic` execution cannot claim independence.

Keep provider delivery monitoring active from before readiness through settlement.
After execution, join its final poll, download results and run:

```sh
node qualification/reconcile-independent-http.mjs DOWNLOADED_RESULT/report.json DELIVERY_HISTORY CLOSURE_JSON
```

Both orders must meet numeric limits and paired non-regression with complete
observation before scaling duration. Inline profiles are required only on the
candidate when its sealed config enables them; the historical control remains
unmodified. Zero/error/drop flags and phase activation must be retained.
The short runner always reports `qualification_ready:false` and
`full_gate_passed:false`, even when its own diagnostic checks pass.

## Coverage, memory and final trials

Reconcile source/role versions, physical attempt IDs, HTTP terminals, intermediate
witnesses, quiet-role boundaries, provider Tail counts, canonical/native windows,
source rejection prefixes and receiver/export delivery health. ETag-pinned immutable
windows require stable repeat observation; downloaded paths must remain portable.
Missing or sampled populations are not complete. Resource errors survive canonical
compaction and later success. Polling cannot exclude an error cleared entirely
between observations, so state this limitation rather than claiming absolute absence.

Only after short warm prerequisites pass, run the original gate lifecycle and
three distinct trials. Preserve the 100 fresh single-attempt recovery epochs per
candidate for valid executions. Finish exact body/receipt, faults, publication/queue,
ANN and complete independent coverage checks before evaluating the final gate.
A cold reconstructed actor is not automatically a proven physically cold isolate.

Retain report provenance and all failures. Tokens never enter evidence seals.
Cleanup only the named authorized disposable resources: stop/freeze private actors,
await physical/maintenance drain, drain the exact bucket with bounded requests,
remove app/private Workers, queues/DLQs/namespaces/buckets and disposable secrets,
then verify scoped absence and save receipts. Never use broad account-wide deletion.
`make gc` is guarded; do not force it past active builds or unknown process state.
