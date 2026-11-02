# Serverless performance findings and next measurements

The native serving design is in [architecture](cloudflare-serverless.md), commit
semantics in [durability](antfly-object-storage-backend.md), and acceptance/operator
procedures in [qualification](cloudflare-independent-http.md). Measurements below
are diagnostic evidence with stated scope. They do not establish a full gate pass,
universal speedup or inherent platform latency floor.

## Measured improvements and misses

The approved-control paired Cloudflare HTTP experiment used exact control
`c44896ac8c` and both execution orders. It measured 944 timed reads and 128 writes,
plus verification/publication calls. Exact bodies, counts, fences and role reconstruction
checks passed. The full original gate did not run and independent trace coverage was incomplete.

| p95 milliseconds, both orders | Approved control | Candidate |
| --- | ---: | ---: |
| Cold read | 8,231–9,381 | 1,424–1,721 |
| Warm read | 483–684 | 456–510 |
| Burst read | 488–563 | 173–394 |
| Mixed read | 1,248–4,840 | 539–1,594 |
| Mixed write | 3,918–6,351 | 1,703–2,326 |

Cold improved 4.8–6.6× and mixed reads/writes approximately 2–3× within those
orders. Reverse mixed reads missed the 1,000 ms limit; reverse warm reads regressed
510 versus 483 ms. Warm improvement is inconsistent. The driver could queue waiting
for headers, and missing service traces prevent an exhaustive error claim. These
pre-main results do not qualify the current rebuilt modules.

## Platform envelopes

Three separate Cloudflare fixtures measured gateway/service/owner RPC, local storage
and conditional R2 operations with 1 KiB, 256 KiB and synthetic-pressure profiles.
Pressure retained 32 MiB WASM plus 8 MiB bytes; no Antfly engine was loaded. Each
unloaded cell has 24 successful samples. Client measurements came from the local
Mac; app colos were SEA/SJC, while actual owner/R2 placement was unverified.

| Client p95, ms | 1 KiB | 256 KiB | Pressure |
| --- | ---: | ---: | ---: |
| Gateway echo | 44.7 | — | — |
| Service echo | 68.8 | — | — |
| Owner echo | 77.4 | 98.9 | 250.3 |
| Owner GET | 73.8 | 105.6 | 100.1 |
| Owner PUT + sync | 116.7 | 145.2 | 291.8 |
| Owner R2 GET | 265.0 | 256.7 | 203.0 |
| Owner conditional R2 PUT | 1,010.8 | 901.0 | 539.7 |
| Gateway R2 GET | 215.5 | 390.9 | 103.0 |
| Gateway conditional R2 PUT | 1,202.5 | 866.5 | 465.1 |

All three overall reports failed offered-load/client checks: 43 explicit unsent
overloads, dispatch-bound failures and one pre-header transport failure remain.
All 1,707 known attempts have intent/terminal pairs; all 27 write-verification
batches pass. The 16-in-flight client cap is not a global R2 QPS limit. Partial
Tail recorded 1,003 events and no observed resource failure; it is not exhaustive.
Run order, different buckets/placement and host load prevent causal pressure claims.

New-object creation had 24 samples, p95 468.6 ms. Existing-owner reconstruction
first reads were 118.0/87.0/196.3 ms, **one sample per profile**, followed by seven
warm reads each. Changed instance/version and persisted seed verified reconstruction;
physical cold isolates were not proven. These are not cold p95 populations.

All six fixture Workers, three namespaces and three buckets were removed, with
12 exact scoped API 404 checks. New disposable tokens were removed. Reports,
failed samples and receipts remain local. No fixture queues/schedules/exports remain.

## Attribution and experimental policy

`QUALIFICATION_PHASE_PROFILE=true` carries bounded same-request HTTP/owner profiles
through existing RPC and emits an authenticated `X-Antfly-Profile` response header.
Profiles exclude keys, bodies, arbitrary error strings and credentials. Span labels,
header size and dropped/error-bearing profiles are checked. The default is off.
The HTTP driver and local runner reject incomplete required profiles.
`analyze-request-phases.mjs` aggregates them offline.

Calculate residuals within individual requests. Do not subtract unrelated percentiles
or timestamps from different isolates; nested spans and shared groups are not additive.
Worker clocks advance around I/O, so a zero span is not zero CPU. Inspect the slow
requests' admission, owner RPC, native execution, bootstrap, cache activation and
physical dependency waves before changing architecture.

The elapsed batching-window experiment completed both orders with 384 reads,
128 writes and 48 reconstruction reads. All 512 mixed-request profiles validate.
Read p95 forward regressed 155.0→176.7 ms; reverse was 128.3→128.2 ms. Write p95
changed 275.7→271.2 and 281.5→255.6 ms. There is no twofold gain or paired read
non-regression. `MUTATION_ELAPSED_BATCH_WINDOW` stays off by default.

## Current modules and next sequence

Latest integration incorporated main's lazy navigation and reclaimer-aware quotas.
Rebuilt full/compact/observe module SHA-256 values are:

- `a5622fe8f6fc2565943b8fe8f2c9f77db5d5c3be8701a73ce136b27de47ba9dd`
- `b8602aace3749f706c9074e1b789bea609add3d5914e40f901f9c0680eff3e0f`
- `8d144f61f0e26ea4c402f54cf1c753144182809599905e045e51d7882e5d8cbd`

Those modules pass 623 JavaScript tests, 209 object-store checks (two provider skips),
96 graph/page, 16 read-state, 14 publication/plan, 27 native host contracts and six
actual-workerd checks. This is functional evidence, not a new latency comparison.
The approved control remains immutable; rebuilt modules do not replace its bytes.

Run a short 512-document comparison on the current source before longer tests.
For local diagnosis use `read-state-latency.mjs`, separate fresh output directories,
explicit variant flags, both orders and complete phase profiles. An incumbent
resident variant is not the approved historical control. Avoid overlapping builds
or latency workloads. Preserve negative results and verify actual activation.

For hosted acceptance, use a dedicated independent client and exact approved control.
First eliminate client overload, missing observation and mixed versions. Then fix
the dominant mixed/warm read wait while preserving freshness and writer progress.
Accept improvements only if both orders meet absolute limits and paired non-regression.
Expand cold recovery only after warm prerequisites pass.

Reduce required serial R2 commit waves before trying larger write concurrency.
Group commit amortizes throughput costs but cannot remove required durable latency.
An authoritative provider-local log with asynchronous R2 checkpointing changes
acknowledgment/takeover/replication semantics and needs separate protocol qualification.
Keep provider placement, whole-isolate memory and client scheduling as measured
variables; the baseline table is not an inherent lower bound or product SLO.
