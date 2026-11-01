# Paired Cloudflare HTTP diagnostic trial

This proposal uses the approved corrected control `c44896ac8c13898c549b7896ab633b1b8b25c04c`
and current candidate runtime from `4212a8452d`. Control source is unchanged;
its full/compact modules are the exact retained approved bytes, not rebuilds:

- Full: `563936db3958bdab50c404db4a6ccef15e095e798e49198ab2304d87322dac97`
- Compact: `789469d743cf69bb9cdcc3e3c59caa4f05663fe4e73958908800d780b1d85f77`

The diagnostic wrappers add identity/state observations outside the timed API
path. Control execution, batching, capacities and native algorithms remain
unchanged. Both stacks keep their original platform CPU limit and four-role
layout. Both use four tenants, 64 identical 1 KiB documents per tenant and explicit
publication; queues, cron and automatic GC are disabled for this small diagnostic.
This is not the original large, continuously maintained, whole-window gate.

## Exact disposable scope

| Role | Worker |
| --- | --- |
| Approved control app | `antfly-1278-cold-mvx8bv81-0a4b` |
| Control private roles | `antfly-1278-cold-mvx8bv81-0a4b-{writer,reader,builder}` |
| Candidate app | `antfly-1278-cold-mvx8bvda-f8b1` |
| Candidate private roles | `antfly-1278-cold-mvx8bvda-f8b1-{writer,reader,builder}` |
| Remote HTTP request driver | `antfly-1278-driver-mvx8irf0-905b` |

The Antfly account is `e7e005e208ec9363efaca673271d2dbd`. The two R2 buckets
have the control/candidate app names. Their six actor namespaces belong to the
six private Workers. No queues or observability destinations are created.

All nine Workers and the two buckets were deployed before authorization. Automatic
approval review initially rejected credential installation because previous
approvals named different deployments. The user then explicitly answered **yes**
to this exact token and cleanup scope; installation is now authorized and complete.

## Credential installation requiring authorization

Three newly generated disposable tokens exist in mode-0600 files in these new
run directories. They are not existing account credentials. Install them only as:

- Control app token → `API_TOKEN` on the control app and `BASELINE_TOKEN` on the
  request driver.
- Candidate app token → `API_TOKEN` on the candidate app and `CANDIDATE_TOKEN` on
  the request driver.
- Driver trigger token → `API_TOKEN` on the request driver.

The driver receives no Cloudflare account credentials. Its two outbound origins
are sealed to these disposable app Workers. Public fetch uses
`global_fetch_strictly_public`; redirects are rejected manually before credentials
can be forwarded. The authenticated driver accepts only bounded generated test
operations, at most 16 calls per round and eight rounds per invocation, and
streams/parses at most 16 KiB of input. Reports and sanitized tails omit token
values and headers. A token grants access only to the corresponding disposable
app's test data or the driver's bounded test operations. Private roles receive
no API credentials.

## Workload and failure policy

After credential installation, seed/publish the same corpus on both sides.
Restart all four roles on both sides and wait passively for rollout without
warming the new serving actors. The control serves reads through reader actors;
restarting only its writer would be an unfair cold comparison. Perform a single timed 12-read cold population for each side; repeat
with a new reconstruction in reverse order. Every sample must match its expected
app version. After the timed population, check that all owner and reader instances changed
and all owner/reader versions match. Retain and reject mixed-version populations.

Then run warm (four concurrent reads), burst (twelve concurrent reads) and mixed
(twelve reads plus four durable writes) profiles, eight rounds each, in both
orders. All rounds execute inside the remote driver so local host scheduling
cannot alter within-profile request spacing. Each later round's read fence
advances only after the preceding durable write completes. Writes use unique
keys and are never automatically replayed. Verify final bodies, visibility
incarnations/fences and counts; manually publish remaining work with bounded
continuations.

The driver records I/O-advanced Cloudflare wall time around awaited public HTTP
requests. It includes the remote app's compute and transit while the fetch is
pending, and excludes the local Mac and external internet-client transit.
Cloudflare CPU-only phase clocks are not CPU profilers. Keep these results
separate from prior external-client p95 and original gate measurements. See
[Cloudflare fetch](https://developers.cloudflare.com/workers/runtime-apis/fetch/)
and [timer semantics](https://developers.cloudflare.com/workers/runtime-apis/performance/).

Capture all nine Workers' sanitized diagnostic Tail streams, per-attempt IDs,
HTTP status/version/outcome, resource failures, pool limits and actor identities.
All members settle before a batch returns; failures stop later rounds and remain
in reports. Tail is diagnostic, not exhaustive independent coverage. No isolated
local performance guard, tenfold claim, historical `SQLITE_NOMEM` closure or full
gate pass can be inferred from this diagnostic alone.

## Teardown

After capture, permanently remove this exact test's nine Workers, six associated
actor namespaces and two R2 buckets with test data. Stop the driver first, detach
each app from actors, stop private roles, wait for quiescence, and drain R2 until
an empty invocation before deleting buckets. Remove all three disposable token
files. Preserve manifests, sealed configurations, hashes, source, reports and
sanitized journals locally; deduplicate native module payloads.

The prepared pair manifest is
`examples/cloudflare-serverless/dist/qualification/antfly-1278-driver-mvx8irf0-905b/pair.json`.
Each side has a sealed `run.json` in its same-name qualification directory.
The driver has a sealed configuration and bundle hashes. Changes to the sealed
configuration or bytes cause preflight verification to fail before workload
requests. Scripts: `qualification/cold-pilot.mjs`,
`qualification/paired-http-trial.mjs` and `qualification/http-trial-driver.mjs`.

## Completed paired result

The approved trial completed on 2026-11-01 UTC. Sixteen timed populations run
both orders, with **944 timed reads (including 48 reconstruction reads), 128
durable single-document writes, sixteen final verification reads, sixteen count
checks and eight publication calls**. All 1,112 driver calls succeed. Every known
attempt UUID has exactly one matching sanitized driver Tail terminal with equal
status, version and latency; no missing, unexpected or duplicate IDs occur.
This reconciles known driver calls, not independent platform invocation coverage.
Service trace coverage is demonstrably partial: of 480 candidate lookup IDs, only
93 have matching HTTP traces and 66 have matching owner/cohort traces in the saved
journals. The cause of these missing service traces is not established; no zero-
error conclusion can be drawn for the missing intervals. `latency-attribution.json`
retains the matched and unmatched populations.

Both cold populations on each side pass app-version, four owner-identity/version
and eight reader-identity/version checks. All source versions are pinned, and no
mixed-version population is accepted. Read bodies, visibility incarnations,
prior-round read fences, strictly increasing write fences and final counts are
verified. Every write uses a distinct keyed sequence; none is retried. Publication
converges in one four-tenant batch per side; post-publication verification passes.

| Profile, p95 milliseconds | Control, forward | Candidate, forward | Candidate, reverse | Control, reverse |
| --- | ---: | ---: | ---: | ---: |
| Cold read | 9,381 | 1,424 | 1,721 | 8,231 |
| Warm read, four concurrent | 684 | 456 | 510 | 483 |
| Burst read, twelve submitted | 488 | 173 | 394 | 563 |
| Mixed read, twelve reads/four writes | 1,248 | 539 | 1,594 | 4,840 |
| Mixed durable write | 3,918 | 1,703 | 2,326 | 6,351 |

The remote driver reports SJC for all populations. Within each paired order,
cold improves **4.8–6.6×**, burst **1.4–2.8×**, mixed reads **2.3–3.0×** and
mixed writes **2.3–2.7×**. Warm improvement is inconsistent: the reverse candidate
is slightly slower. These are closed-loop batches of hot points on a small
corpus, with platform transport/connection queueing included, not a fixed offered
arrival rate or broad workload distribution. Cloudflare limits a request to
[six connections waiting for response headers](https://developers.cloudflare.com/changelog/post/2026-04-09-relaxed-connection-limiting/);
submitting twelve/sixteen fetch promises does not prove twelve/sixteen concurrent
server executions. The platform can queue this driver, and that queueing is
included in these timings. Do not combine these populations
with internet-client latency or the original gate results.

Available journals contain **zero observed non-OK outcomes, resource failures,
durable-storage/checkpoint errors, nonempty trace error lists or truncated
events**. Peak observed WASM pool linear memory is 10,027,008 bytes for control
and 22,872,064 bytes for candidate; both retain their original pool ceilings
(maximum logged ceiling 67,108,864 bytes). These numbers exclude JavaScript,
compiled code and platform/SQLite memory. No independent total-isolate-memory
measurement or full-scale soak is supplied. Historical `SQLITE_NOMEM` remains
open despite this successful small run.

**The full gate does not pass.** Warm gain is not stable, cold is below the
tenfold relative target, the reverse mixed candidate read p95 exceeds the
1,000 ms target, and the original large-corpus/error-window/independent-coverage
requirements are unrun. Do not lower thresholds or infer a universal speedup.
The next performance work should attribute owner/network and R2 dependency
waiting separately, replace the driver bottleneck with a genuinely independent offered-
load client, improve complete service/resource export, and repeat both
orders and expand corpus/concurrency with complete resource windows. Low linear
memory alone cannot identify or rule out the historical platform-memory failure.

All nine Workers and both buckets have been deleted under the explicit approval;
the private actor namespaces are retired with their owning Workers. No queues,
cron jobs or observability destinations were created. All three disposable token
files are removed. Scoped read-only checks confirm missing Workers and absence
of the two buckets from the account inventory. Reports, sealed configurations,
bundles, hashes, audits and sanitized logs remain local. Twenty archived WASM
paths share five immutable payloads; mutable control build output is detached
from the retained approved binaries so a later build cannot overwrite them.

Evidence root:
`examples/cloudflare-serverless/dist/qualification/antfly-1278-driver-mvx8irf0-905b/`.
`pair.json` contains ordered populations and approval; `summary.json` and
`audit.py` independently check the saved sample/fence/version/identity/hash and
known-attempt reconciliation; `absence-check.json` records scoped retirement.
Each side's run directory preserves its original sealed configuration snapshots
and namespace/version histories. All original failed reports and approved
control bytes are unchanged.

Validation: **594 Node tests pass**. Twenty-six focused checks, including the
actual-workerd driver test, pass. They cover public-fetch compatibility, manual
redirect rejection, body/fence/version checks, sibling resource failures without
retries, round-to-round fence progression, bounded streamed input, and sanitized
per-attempt/resource diagnostics. Native modules and production serving source
are unchanged by this follow-up.
