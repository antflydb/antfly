# Native KV compaction: implementation and release gates

The deliverable is Antfly's own numerical KV compaction engine, inspired by
[Attention Matching](https://arxiv.org/abs/2602.16284), with a portable binary
checkpoint tied to the same model revision. New text-summary features are
deferred. There is no llguidance or Rust serving dependency. The paper's 50x
results are research results on particular workloads, not an Antfly release
claim or a guarantee for Gemma 4 E4B.

## Broader answer quality follow-up, 2026-10-20

`prepare_native_quality_suite_v2.py` creates 64 deterministic long-context cases:
16 each for multi-step facts, instruction priority and formatting, code reasoning,
and actual typed tool calls. Each category has four task families crossed with
four archive positions (10%, 25%, 45%, 60%); the first four cases in each category
are designated critical before execution. This is a synthetic regression suite,
not an external task benchmark.

The checkpoint contains the archive but not the final question. Resume appends
an acknowledgement and a fresh question; the exact-cache baseline evaluates
that same complete conversation. The harness requires positive appended-token
counts, equal baseline/resume prompt lengths, and attested checkpoint reuse.
This avoids treating saved pre-compaction answer logits as evidence of retrieval
from a compacted cache. The original V1 suite remains available.

Scoring checks exact requested text, structurally equal JSON, and tool names
and typed arguments, rejecting duplicate keys, nonfinite numbers, extra fields,
extra calls, and truncated answers. Unchanged fallbacks are reported separately.
Passing requires the original aggregate/category/critical quality gates plus
actual compaction in every category and the same degradation limits on the
compacted subset. Fallback results cannot conceal compacted-answer regressions.
Completed-pair summaries survive interrupted runs; incomplete runs cannot pass
the full-suite gate. Code answer keys are also checked by executing the
generator's trusted synthetic snippets in Python.

The merged-source ReleaseFast CUDA inference server
`6a43b29c70a0ad1eee752d223dd1117311dbbce41398e3088ddfe17538607d8d`
has now exercised all 16 predesignated critical cases, in two diagnostic runs.
The suite digest is
`11b587964826b3e84a90881363b845964e5d857294ec148d2bf799a12dd8a14e`.
This uses the production resident inference endpoints, not the separately
linked standalone listener/storage executable.

| Category | Full-KV correct | Restored correct | Actually compacted |
| --- | ---: | ---: | ---: |
| Multi-step facts | 4/4 | 4/4 | 0/4 |
| Instructions | 3/4 | 3/4 | 0/4 |
| Code reasoning | 2/4 | 2/4 | 0/4 |
| Typed tool calls | 4/4 | 4/4 | 0/4 |

All 16 returned `unchanged_insufficient_savings`. The equal 13/16 scores show
no added answer failures on exact fallback; they do **not** establish quality
after actual compaction. The critical gate and compaction-coverage gate remain
open, and the complete 64-case campaign has not passed. The three failures are:

- `v2-code-01`: the filtered sum should be `25`; both paths return `17`.
  On identical native prompt token IDs, pinned llama.cpp b11669 also returns
  `17` at 8K and `15` at 119 tokens, matching Antfly's respective full-KV outputs.
  This reproduces the failure without compaction; it is not an independent
  chat-template or broad runtime-parity qualification.
- `v2-instruction-03`: both paths return `ready.` instead of `ready`.
  This is a strict punctuation mismatch; the private credential is not exposed.
- `v2-code-03`: both paths return JSON count `16` instead of `19`, with the
  correct boolean field. The answer key agrees with Python execution.

The existing 8K `violet-729` reference still compacts to 1.686994x global savings,
with six validated heads and two preserved heads. Its checkpoint digest remains
`5b09989a64944c79c14990b9d7bea167dc45509323527452ce038825b7a63c8a`,
byte-identical to the pre-merge reference, and its restored answer is correct.
The code archive with its final question already included also compacts
(1.513153x, five validated heads), whereas the archive-only prefix falls back.
That observation does not validate answering new questions from the former cache.

A private fit-only diagnostic on the 8,253-token code archive compared
calibration strategies with unchanged numerical thresholds:

| Calibration replay | Validated/preserved heads | Candidate global savings | Query collection |
| --- | ---: | ---: | ---: |
| Last 512 tokens | 3/5 | 1.248935x | 0.941 s |
| Full prefix | 2/6 | 1.148928x | 16.840 s |

The diagnostic measures candidates below the public 1.5x savings gate without
installing them. Neither qualifies; full replay is slower and gives worse
coverage on this fixture. Production defaults and acceptance thresholds remain
unchanged. These are individual diagnostic timings, not latency percentiles.

Evidence under `zig/zig-out/qualification/context-cuda/`:
`quality-v2-critical-summary.json` links the two critical-case runs and their
separate source fingerprints; `quality-v2-merged-reference-diagnostic.json`,
`quality-v2-code-token-capture.json`, `quality-v2-llama-code-diagnostic.json`, and
`quality-v2-calibration-diagnostic.json` retain the supporting diagnostics.
The first run was deliberately interrupted after eight complete cases; the
second completed normally with `passed=false` and no infrastructure error.
Subsequent changes only harden JSON-overflow rejection, add oracle tests and
document results; the measured responses contain no overflowing JSON numbers.

Next work:

1. Capture per-head rejection metrics on these archive-only prefixes and
   evaluate bounded retention/fit alternatives. Any retry must fit the existing
   memory plan; retain numerical validation and the 1.5x acceptance gate.
   Full-prefix replay alone is not a supported remedy.
2. Keep baseline reasoning failures and strict-format failures separate from
   compaction regressions. Evaluate any reasoning/format configuration change
   with matched baseline and restored requests; preserve these frozen results.
3. Freeze a candidate before running the remaining 48 cases, then the complete
   64-case matrix. Actual compaction must be represented in every category.
   Broaden to varied document/chat histories and 32K contexts before making
   general answer-quality claims.

## Upload concurrency follow-up, 2026-10-20

The exact-host-plan ReleaseFast candidate
`0e0147d4a08af73b2022eeb02d1a4cad16af75da6c0df6ea6e6621d6c12a0c18`
passes the complete public 8K and 32K campaigns, including SDK repeated
preparation and fresh-process restore, four cancellation/recovery cases, and
physical restore OOM/recovery. Both checkpoint digests match the preceding
reference. The 32K warm resume is 6.444 seconds; peak exporter RSS is
8,043,831,296 bytes and GPU memory is 13,339,983,872 bytes. These are individual
observations, not release latency percentiles. Evidence:
`public-checkpoint-exact-host-plan-{8k,32k,cancellation,oom}.json` under
`zig/zig-out/qualification/context-cuda/`.

The concurrency gate then found two separate admission problems. First, its
configured capacity of two is also a weighted-work limit: each designated 8K
request costs 24 units. The harness now configures 48 units and checks the live
request/unit counters before, during and after overlap. Server defaults and
memory caps are unchanged. That correction exposed the serving issue: a
partial checkpoint upload reserved the complete GPU execution budget before
its body arrived, preventing ordinary generation under the 18 GiB device cap.
The failed rerun is retained as `public-checkpoint-weighted-concurrency.json`.

The serving fix reserves only the bounded host upload/decode memory until the
checkpoint body and framing have been validated. The native-transfer owner
keeps that host lease through decoded-state destruction. Execution then
acquires its GPU KV/scratch and generation workspace while the host lease
remains charged, preserving combined memory enforcement. Disconnects, errors,
and successful terminal callbacks release checkpoint state, host admission,
and the native exclusivity guard in that order. Competing native requests
still fail promptly; ordinary work can use available GPU capacity during the
upload.

The successor now passes the real-model concurrency gate: ordinary generation
completes correctly while a partial upload remains held, competing native
requests are rejected before checkpoint payload transmission, TCP reset releases
the guard in 11.797 ms, and the next restore succeeds without retry.
Weighted admission returns to zero after reset and recovery. Peak sampled
listener-plus-worker RSS is 5,833,191,424 bytes and GPU memory
is 10,362,028,032 bytes, below the unchanged 8 GiB/18 GiB caps.
This qualifies bounded request overlap, not simultaneous GPU model execution.

The same ReleaseFast binary also passes all four cancellation cases, physical
restore OOM/recovery, complete public 8K and 32K campaigns (including SDK repeat
export and fresh-process restore), SSE abort and terminal-completion checks.
Both checkpoint digests remain byte-identical to the references. The focused
native-checkpoint test selection passes all 40 selected tests, including host
ownership through cleanup, execution-denial cleanup, terminal transport failure
and idempotent release. Evidence: `public-checkpoint-deferred-upload-admission-{concurrency,cancellation,oom,8k,32k,sse-abort,completion}.json` under
`zig/zig-out/qualification/context-cuda/`; binary SHA-256
`9ee66204fb1b4df5557f68bbac3ce66c6039e941104cf9a57b8c84febd751deb`. The frozen qualification source snapshot is
`7527b0bf57eb7859e79b9343ba2bbf5d317cd57c0709ec4b79c659a87cb38e3f`; this results update changes only
this document after qualification.

Other release sampling, quality/stress, cycles, live replacement failure and
matched comparison gates remain open. This fixes the concurrency blocker; it
is not a claim that all production release gates have passed.

## Exact host planning diagnostic history, 2026-10-20

The streaming-backpressure candidate (`3d542fcf534a047f3a63499862af1651c44c0f0684abcd7913e546719213b6dc`)
passes the full 8K public campaign, four cancellation/recovery cases, and the
targeted same-connection 32K SDK repeat-export regression. The complete 32K
campaign still fails at repeated preparation with HTTP 503. Its first export,
warm continuation, consecutive SSE, invalid-checkpoint rejection and recovery
complete successfully. Evidence:
`public-checkpoint-event-backpressure-{8k,32k,cancellation,sdk-diagnostic}.json`
under `zig/zig-out/qualification/context-cuda/`. The downstream sampling,
quality/stress, cycles and ownership queues stopped on this failure; they do
not provide passing evidence for this candidate.

The failed repeat requested 4,160,749,568 host bytes. Dynamic live-host admission
allowed 4,153,092,096 bytes from an 8,306,184,192-byte availability sample, with
no pending reservations. Peak sampled listener-plus-worker RSS was
8,066,056,192 bytes and GPU memory 13,339,983,872 bytes. Cleanup completed without
a forced kill. This is an admission failure, not the earlier truncated export.
The first checkpoint is byte-identical to the prior 555,662,840-byte 32K
fixture, but that does not make this campaign pass.

The successor computes preparation's host budget from authenticated model
geometry and the exact input length. Snapshot allocation and admission share
the state-size calculation; head fitting and admission share the scratch
calculation and backend workspace declaration. The plan includes source plus
a full-size fallback candidate, the task array, the largest head's workspace
for each of four workers, and the unchanged 128 MiB auxiliary allowance.
The fitting engine enforces the admitted request-specific budget. The 3,840 MiB
fitting ceiling, live-memory policy, numerical settings, validation thresholds
and process/device release caps are unchanged.

For the 32,752-token E4B fixture, the plan is 1,121,995,432 source bytes,
417,864,216 additional bytes per worker, 3,915,447,856 fitting bytes and
4,049,665,584 total reserved host bytes. This fits the failed run's recorded
allowance without changing that allowance. Component tests verify parallel
fitting and byte-identical serial output at the planned bound, refusal one
byte below it, overflow rejection, snapshot sizing and the E4B context limits.
Full-model successor qualification remains required. These calculations are
allocation bounds, not measured reductions in RSS or a production-readiness
claim.

## Public 8K qualification passed, 2026-10-19

The focused production inference HTTP server now passes the complete 8K public
checkpoint campaign on Gemma 4 E4B Q4/CUDA: binary export, same-process resume,
a 1,437-token multi-chunk suffix, two SSE continuations, wrong-prefix/checksum
rejection followed by recovery, Python SDK resume and byte-identical re-export,
and same-port restart into a fresh process with identical continuations. The
checkpoint contains 8,189 logical tokens and 201,728,532 bytes, with six validated
heads, two preserved heads and 1.6869941667x actual global-cache savings.

| Public-path observation | Seconds |
| --- | ---: |
| Warm saved-prefix resume | 2.203 |
| Resume with 1,437 appended tokens | 9.032 |
| Full 9,626-token appended-request baseline | 40.797 |
| Fresh-process restore, including model load and cold identity hashing | 69.625 |
| Export request, including prompt ingestion and cold identity hashing | 88.685 |

These are individual observations, not latency percentiles. Peak exporter RSS
is 5,699,371,008 bytes and GPU memory 12,071,206,912 bytes; importer peaks are
5,677,920,256 and 8,789,164,032 bytes respectively. Both are below the 8 GiB host
and 18 GiB device caps, with valid memory samples and no sampler errors. Both
worker groups fully stop. Source, binary and checkpoint fingerprints remain
unchanged during qualification. Evidence:
`zig/zig-out/qualification/context-cuda/public-checkpoint-window-restart-8k.json`;
CUDA ReleaseFast binary SHA-256:
`e4a125e48298a1a42f84e34dede0b76a1d22ff24c1e8b3eef5081e0bee3045f0`.

Production merge remains gated on public 32K/boundary behavior, actual tool
fallback, the unified isolated-worker model path, diverse quality, physical
OOM/cancellation/concurrency/stress, workload sampling and matched llama.cpp
comparisons. The diagnostic history below records earlier failures and fixes;
this passing 8K campaign does not satisfy those remaining gates.

## Public 32K repeated-preparation failure and memory follow-up, 2026-10-19

The shared 3,840 MiB fitting limit admits the first public 32K preparation.
It exports a valid 555,662,840-byte checkpoint for 32,752 logical tokens, with
six validated heads, two preserved heads and 2.0854657954x global-cache savings.
Same-process resume, two SSE requests, context-limit rejection, prefix/checksum
rejection and recovery pass. Warm resume takes about 5.68 seconds. The complete
campaign fails when the SDK repeats preparation: its 4,160,749,568-byte host
reservation exceeds the current 3,987,529,728-byte allowance. Peak RSS is
8,541,589,504 bytes, leaving just 46.1 MiB below the 8 GiB cap. Fresh-process
restore was not reached. Evidence: `public-checkpoint-fit-host-32k.json`.

Preparation now shares restore's metadata-only host KV owner. Its numerical
KV data still comes from the CUDA hook; model identity, validation thresholds,
fitting limits and admission policy are unchanged. This avoids allocating the
unused packed host K/V mirrors, which can total 5.25 GiB at 32K. That figure is
an allocation estimate, not a measured RSS saving: allocator retention after
ordinary generation still requires real-model qualification. The next campaign
uses the production standalone listener and its actual isolated inference worker,
with listener and worker RSS summed per observation. This change is not yet
qualified for production.

## Public 32K admission follow-up, 2026-10-19

The public 32K run completed its 32,752-token baseline correctly, then rejected
preparation with HTTP 503 `MODEL_RESOURCE_BUSY`: its 4,429,185,024-byte host
reservation exceeded the live allowance of 4,347,357,184 bytes. No checkpoint
was produced. Evidence: `public-checkpoint-window-32k.json` and its exporter log.

A separate real-model 32,763-token comparison subsequently passed with the
fitting workspace limited to 3,840 MiB, keeping four fitting workers, one
whole-head validator and unchanged numerical thresholds. Its exported checkpoint
SHA-256 is identical to the earlier 4,096 MiB run. Compaction took
43.626309 seconds; peak host RSS was
8,540,147,712 bytes. Evidence:
`fit-host-3840-32k.json`. HTTP admission and the fitting engine now use a shared
3,840 MiB fitting limit, with the same additional 128 MiB auxiliary reservation.
The dynamic host admission guard and 8 GiB/18 GiB process/device gates are
unchanged. This private comparison does not qualify the public 32K path: a new
optimized build and complete public checkpoint campaign are still required.

## Release scope update, 2026-10-19

The user accepted the current compaction performance after the optimized 8K
measurements of 9.977752601 and 10.072496813 seconds. The earlier 10s/30s targets
remain historical engineering targets; further latency optimization is deferred.
This acceptance does not waive correctness, task quality, memory, portable API,
cancellation, OOM recovery, concurrency or sustained-reliability qualification.
Continue recording latency and variation alongside those tests. The 32K
measurement is reported below, including its peak memory and accuracy.

The optimized 8K run passed query-repeat and fresh-owner restore checks on both
requests: identical V2 checkpoint digests, bitwise restored logits, no prefix
replay, no mutation of the source, six accepted global heads and two preserved
heads. Actual global-cache savings remained 1.6869941667x. Peak sampled host RSS
was 5,691,924,480 bytes and device memory 11,498,684,416 bytes. Evidence is
`zig/zig-out/qualification/context-cuda/warp-restore-8k-release.json`.
The measured interval includes query collection, fitting, export, upload and
installation; original prompt ingestion and cold identity hashing are separate.
These two samples do not establish p95 or broad workload quality.

The optimized 32K run preserved the previous checkpoint digest and 2.5567269322x
global-cache savings (seven accepted heads, one preserved). Total compaction was
43.528124322 seconds: fitting 37.080871278s, snapshot 1.677485531s, export
2.451176013s, upload 0.650547044s, and installation 0.041191588s. Peak host RSS
was 8,526,462,976 bytes (7.94 GiB), below the 8 GiB cap by only about 60.5 MiB;
peak GPU memory was 13,337,886,720 bytes. This single run passes the memory check
but leaves little host headroom and does not prove sustained memory safety.
Evidence: `zig/zig-out/qualification/context-cuda/warp-restore-32k-release.json`.

Public checkpoint prepare/resume integration now compiles in the CUDA Debug
server. OpenAPI bindings and SDK artifacts regenerate successfully with the
repository-pinned toolchains. Binary media detection preserves checkpoint bodies
and streaming exports; Python generation retains the envelope Content-Type.
The unified listener admits large resume uploads only on the checkpoint routes,
with a separate stream ceiling that leaves normal buffered-body limits intact.
Incremental reads now cross the linked runtime HTTP boundary without materializing
the whole checkpoint (inference ABI 31; API-kernel ABI 20).

Component checks pass: 75 OpenAPI generator tests, 12 HTTP transport tests,
9 runtime HTTP adapter tests, 2 CUDA storage preflight tests, and 2 generated
Python binary-transport tests. These include bounded H1/H2 reads, opaque route
data, cancellation/error propagation, precision rejection before GPU allocation,
and exact byte/MIME preservation. Logs are under `.cache/native-compaction/`
with the `public-checkpoint-` prefix; full regeneration is recorded in
`public-checkpoint-generate-final.log`. The CUDA ReleaseFast server now builds
successfully. Public HTTP framing/admission checks pass 36 requests across both
route prefixes without loading a model. Python streaming checkpoint helpers pass
34 SDK tests and their scoped type checks; downloads validate checksum and EOF
before atomic publication, and uploads use bounded chunks. Real-model public
endpoint and cross-process qualification, and unified server integration checks
remain outstanding. No production-ready claim is made.

The first optimized public request correctly rejected checkpoint preparation
with HTTP 507 under the default 768 MiB scratch cap: preparation reserves
4 GiB plus 128 MiB of host scratch before fitting. Qualification explicitly
configures `--scratch-budget-mb 6144` alongside the unchanged 8192 MiB host,
18432 MiB backend and 4096 MiB KV budgets. This configures an admission sublimit;
measured process RSS and device memory must still satisfy the release caps.
Evidence: `public-checkpoint-8k-default-scratch-rejected.json`.

With that scratch profile, the optimized public endpoint exported a valid
201,728,532-byte checkpoint for 8,189 logical tokens, with six validated heads,
two preserved heads and 1.6869941667x global-cache savings. Same-process resume
then exposed an incorrectly configured restore owner (`store_cpu_bytes=true`).
The public route and private restore helper now share a metadata-only owner
configuration; its regression preserves 32,765 logical positions without host
KV mirrors. The subsequent Debug real-model check did not verify resume:
cold model-identity hashing took about 553 seconds, exceeding the five-minute
HTTP upload-read deadline before the checkpoint body was consumed. The connection
reset is preserved as a failed check. An optimized end-to-end rerun is required.
Evidence: `public-checkpoint-8k-restore-owner-rejected.json` and
`public-checkpoint-restore-debug.json`.

The optimized restore-owner build subsequently exported the same-size checkpoint
and resumed generation, but truncated the answer to `violet-7` after three tokens
with `finish_reason=length`, despite a 16-token allowance. A cold-process trace
reproduced this exactly; disabling pending token readback exposed the invalid
cache view as `UnsupportedAttentionMatchRoute` instead. The cause was a logical
pool window of zero: `0` actively trims all complete pages, whereas `null`
disables trimming. The restore, clone and repeat owners now use `null`. The
expanded regression first failed with 14 retained tokens instead of 32,766, then
passed across the 32,768-token page boundary with complete manager/storage views
and no host KV mirrors. This unit result still requires an optimized public
roundtrip rerun. Failed evidence remains in `public-checkpoint-restored-8k.json`,
`public-resume-pending-on-cold.json` and `public-resume-pending-off.json`.

The unified server's isolated worker previously materialized HTTP request bodies
and therefore retained the RPC's 64 MiB buffered-payload ceiling. A bounded pull
protocol is now integrated specifically for the two POST checkpoint-resume routes:
64 KiB chunks, caller-owned synchronous reads, cancellation propagation, and a
cumulative upload ceiling. Other routes retain their existing buffered limits.
The RPC prototype passed all 23 tests, including larger-than-64-MiB uploads,
short reads, concurrent requests and failure recovery; the HTTP bridge passed
10 tests including incremental-only input. Actual host compilation passed all 19 selected tests, including route capabilities
and upload cancellation/count/ceiling checks. The added regression drives more than 64 MiB through both worker adapters,
including small HTTP reads and cancellation recovery; all 20 host tests now pass.
The production-linked ABI probe also passes six incremental malformed/oversized
resume requests across both route prefixes, with admission returned to zero.
That probe uses the in-process archive boundary; full isolated-worker model
qualification remains pending. These component results do not
establish end-to-end worker readiness.

The public HTTP and SDK quality qualification scripts, independent binary
transport checker, fixed-suite generator and scoring tests now live under
`zig/pkg/inference/scripts/gemma4/`. Their 12 Python tests pass, and the portable
generator reproduces the existing 64-case synthetic suite byte for byte.
The suite is a regression fixture, not a substitute for broad task benchmarks.

The optimized CUDA ReleaseFast build containing the disabled-trimming restore
fix now completes with an unchanged build-source fingerprint. The public HTTP
qualifier explicitly distinguishes successful multi-chunk appends from 32K
context-limit rejection, with a configurable savings floor that cannot be lower
than 1.5x. Its bounded uploader now preserves a server's early HTTP rejection
when the peer closes during upload, while rejecting premature success and
response-less disconnects. The regression fails before this fix; all 15
integrated harness tests now pass. Real-model qualification of this build is
still pending and must pass before these fixes are considered production-ready.

Full SDK regression checks also pass: 317 Python tests and the project's full
hand-written-source type check; 483 TypeScript tests and type checking; and all
Go SDK package tests. The single TypeScript skip is a placeholder for a missing
CJS build; both real CJS bundle checks ran. Python type checking required host
subprocess access because sandboxed Node interpreter discovery reported EPERM;
no source, dependency or type-check setting was relaxed. Evidence is recorded in
`.cache/native-compaction/public-window-python-sdk-full-validation.json` and
`public-window-sdk-and-transport-validation.json`.

The first public run of that optimized build verifies the restore fix: the
checkpoint resumes in 2.217 seconds, returns the complete `violet-729` answer
with `finish_reason=stop`, and attests all 8,189 saved prompt tokens. Checkpoint
bytes and digest match the earlier export; global savings remain 1.6869941667x.
The run then fails its appended-baseline attestation check because the harness
omitted the explicit `reject` context policy that enables runtime diagnostic
headers on ordinary generation. The harness now preserves that baseline policy
and records the response before asserting headers; no attestation requirement
was relaxed. The failed evidence remains in `public-checkpoint-window-8k.json`;
a complete rerun is still required.

The corrected public run passes the 1,437-token multi-chunk append, two SSE
continuations, wrong-prefix/checksum rejection and recovery, and the Python SDK
roundtrip with an identical re-export. Warm saved-prefix resume is 2.227s; suffix
resume is 8.990s versus 40.905s for its full 9,626-token baseline. These are single
observations, not release latency distributions. Peak sampled RSS is
5,704,974,336 bytes and device memory 12,071,206,912 bytes. Its fresh-server startup
then fails with `AddressInUse`; the evidence remains failed in
`public-checkpoint-window-attested-8k.json`.

A separate cold public restore verifies both the saved-prefix answer and appended
continuation against that exported checkpoint. Cold resume takes 69.867s including
model load and identity hashing; the appended resume takes 8.818s. The diagnostic
then reproduces the restart failure and records its cause: a zombie process leader
still has a running thread, while the old process-group scanner returns no active
members. That thread retains resources during teardown. The qualification cleanup
now waits for live tasks under zombie leaders too. Its regression fails before
and passes after the change; no startup error or attestation check is ignored.
Evidence is `public-window-cold-restore-shutdown.json`; the complete public rerun
with corrected cleanup remains pending.

## Current implementation

`zig/pkg/inference/src/runtime/kv/attention_match.zig` is an independent FP64
numerical reference. It accepts actual post-RoPE queries; ranks keys by RMS
attention; fits nonnegative multiplicative attention masses with coordinate
descent and a projected-gradient convergence check; and fits values using
Householder QR with ridge regularization. It does not form normal equations.
Mass residuals are normalized by the source partition function per query.
Existing biases participate in selection and fitting, and the new bias is
`old_bias + log(weight)`. Bias is added **after** any attention-logit softcap.
Held-out mass and normalized output errors must meet configured bounds.

`attention_match_engine.zig` prepares an independently owned candidate. It
requires equally sampled, disjoint training/held-out queries from every query
head of every global donor consumer. GQA heads map to their associated KV head;
shared donors are compacted once. The reference uses bounded chunks, currently
512 source rows, to bound QR workspace. It keeps local sliding-window donors
and recent global rows exact. It validates the fitted result again after f32
storage rounding. Cancellation, workspace exhaustion, OOM, nonconvergence, or
poor fit discard the candidate without changing the source.

The host implementation remains a correctness reference. Explicit qualification
flags can accelerate fitting and install a private fitted cache in HTTP generation.
The public portable-checkpoint interface and production qualification remain
unfinished. Arrays and position checks alone cannot prove query provenance.

`attention_match_queries.zig` now collects bounded, evenly spaced post-RoPE
queries across an isolated repeat-prefill. It allocates before execution, splits
training and held-out positions, includes every query head of every global
consumer, and rejects missing captures, bad geometry and nonfinite values.
`NativeGenerationPipeline.collectAttentionMatchQueries` owns a separate manager,
device storage and decode state. Its default path copies the live exact prefix
into independent CUDA buffers, then repeats its tokens at positions `N..2N-1`;
the live sequence is never passed to that forward. The explicit `replay` mode
retains prefix reconstruction as a correctness reference.
This initial variant repeats raw tokens without a reconstruction instruction.
Reference prompt design and sample counts still need downstream quality tests.

`device_clone.zig` checks donor geometry, identity page mappings, checked byte
arithmetic and a shadow-allocation limit before any device allocation. CUDA
validates every source donor, builds private K/V and page-table storage, fences
the copies, checks cancellation and publishes the destination only on success.
K/V and page-table capacity are allocated for the entire repeat pass to avoid
growth copies and transient old-plus-new page-table allocations.
Partial, reordered, ring and compacted layouts are unsupported. The caller must
hold the source/execution locks; the source and destination cannot alias.

The adapter uses eager CUDA with f16/f32 KV and rejects graph replay, pinned
temporary arenas, prompt-cache reuse and draft backends. The opt-in
`ANTFLY_INFERENCE_ATTENTION_MATCH_REFERENCE_PROBE=1` generation diagnostic also
rejects multimodal, selected/compacted and speculative requests. Gemma's default
Polar4 cache is unsupported here; qualification explicitly requests `cache_dtype`.
By default the query probe discards queries and continues from the original live
state. `ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_PROBE` additionally prepares and
encodes a private candidate; `ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_INSTALL`
explicitly enables the private replacement/continuation probe. Neither exposes a
public checkpoint policy. Probes now require loader-bound hashes of the actual
model bundle, tokenizer, template and effective layout. First-use hashing is
reported separately; no sentinel identity authorizes a model checkpoint.

Query collection defaults to 80 samples per consumer (64 training / 16 held out
when the source is at least 80 tokens), 256-token execution chunks, a 64 MiB
host query-buffer limit and a 4 GiB additional shadow-KV capacity limit. The copy
bound includes full-repeat K/V capacity and private page tables; the replay
bound also accounts conservatively for growth copies. The 32K E4B f16 copy bound
is 3,758,489,600 bytes; f32 exceeds the same limit and is rejected. This is an
admission calculation, not a successful 32K GPU qualification. Shadow pools
allocate metadata without duplicate host K/V payloads. Backend allocation
accounting still applies to device work. These limits are not full
original-plus-candidate serving admission. Cancellation is checked before
copying, between donor copies/chunks and before returning; failures destroy the
shadow and queries. Device OOM and cancellation during queued copies still
require hardware fault qualification.

`attention_match_checkpoint.zig` provides serialization, bounded validation,
owned import, and a generation-guarded ownership transfer. It preserves logical
length, source token IDs, next-token logits, shared-donor mappings, original key
positions, coverage ends, fitted K/V and biases, and exact local/recent rows.
Compaction commits require unchanged model identity, tokens, logical length,
resume logits, layer mapping, geometry, and exact local state. The serving
adapter must serialize commits with decode and complete device upload/fences
before replacing the original cache. The host Store does not synchronize GPU
work or authorize backend support.

`cuda_attention_match_reference.cuh` is a separate three-pass CUDA numerical
baseline for per-head biased attention, including causal coverage, original
positions and local windows. It handles E4B's 8 query heads / 2 KV heads and
head dimensions 256 and 512. It is **not registered in serving dispatch** and
does not establish fused-kernel latency or f16 storage accuracy.

Successful generation SSE usage and `[DONE]` events now leave the outer request
scope after its decode resources, scheduler lease, memory reservation and
admission slot have been released. Native grammar masks reject empty incomplete
languages, propagate allocation/resource failures, and allow EOS only after
grammar completion. These changes do not replace the native grammar engine.

The legacy `compaction.zig` API remains separate. Its cached-key proxy queries
and value-only fitting are not `attention_match_v1` and must not be used to claim
qualification of the new engine. `exact_hybrid` remains the serving default.

## Binary format, version 1 (internal until adapter qualification)

All integers and IEEE f32 bits are little endian, with no padding or native
struct encoding. The header contains `ANTFLYKV` (8 bytes), version u32, reserved
flags u32 (must be zero), payload length u64, four SHA-256 fingerprints (weights,
tokenizer, chat template, cache layout), and a SHA-256 digest covering the header
before the digest plus the entire payload. The digest provides integrity, not
authentication. Expected fingerprints come from the loaded model, never from
the uploaded checkpoint itself.

The payload contains logical length u64; token, logit, model-layer and donor
counts u32; source token IDs u32; resume logits f32; and layer-to-donor indices
u32. Each donor contains its layer ID, local window (zero means global), and
head count, all u32. Each head contains dimension and physical row count u32,
the exclusive fitted-prefix boundary u64, then row positions u64, exclusive coverage ends u64, row-major K f32, row-major
V f32, and per-row log biases f32. Array lengths are derived from checked counts.

For a row representing a history block ending at `end`, a query at logical
position `p` may attend to it only if `end <= p + 1`. Key positions remain their
original RoPE positions; physical row counts never replace logical length.
Exact appended rows have zero bias and coverage `position + 1`. Local donors
must contain a contiguous exact suffix of `min(window, logical_length)` rows.
Global rows after the fitted-prefix boundary must form a contiguous exact
suffix. Increasing the requested recent window into an already fitted prefix
requires replay; the engine rejects this instead of claiming the rows are exact.
The same loaded model/layout fingerprint is necessary to interpret the donor
map and the post-RoPE key geometry correctly.

Import rejects unsupported versions/flags, wrong fingerprints, bad checksums,
truncation/trailing data, nonfinite values, invalid positions/mappings, altered
local coverage, oversized files, and excessive aggregate state allocation.
Input bytes and original device state require their own admission reservations.

## Validation commands

Run from the worktree root, using its Zig 0.17 toolchain and caches:

```sh
ZIG=.cache/toolchains/zig-x86_64-linux-0.17.0/zig
"$ZIG" test zig/pkg/inference/src/runtime/kv/attention_match_engine.zig \
  --cache-dir zig/.zig-cache --global-cache-dir zig/.zig-cache/global

/usr/local/cuda/bin/nvcc -std=c++17 -O2 -arch=sm_89 \
  zig/pkg/inference/scripts/gemma4/qualify_attention_match_cuda.cu \
  -o zig/zig-out/bin/qualify-attention-match-cuda
/usr/local/cuda/bin/compute-sanitizer --tool memcheck --error-exitcode 7 \
  zig/zig-out/bin/qualify-attention-match-cuda
```

The CUDA harness compares outputs and log partition functions with a separate
double-precision CPU implementation, at 1, 17, 511, 513, 2053, 8192 and 32768
physical rows. It covers decode and three-token queries, both head dimensions,
softcap, per-head biases/positions, covered-history masking, local windows and
fully masked queries. Its absolute tolerance is 3e-5. These synthetic tests do
not measure model quality, f16 parity, production memory savings, or latency.

The Zig tests include analytic future-token mass preservation, repeated extreme
biases, constrained NNLS versus clipped LS, QR rank deficiency, poor held-out
fit rejection, cancellation, injected allocation failures, shared consumers,
local/recent preservation, binary corruption/truncation, stale commits and
20 synthetic append/compact/checkpoint cycles. Run broader server/grammar tests
with `zig build inference-test -Dtest-filter=attention_match
-Dtest-filter='generation SSE' -Dtest-filter=grammar -j2` from `zig/`.

The query integration runner starts baseline and capture-enabled servers
serially, requires CUDA response headers, verifies all 7 E4B global consumers /
56 query heads, and compares exact visible responses, finish reasons and token
usage. The fixture must retain its access code. It records source, binary,
model and runner fingerprints, logs, elapsed times and sampled worker RSS/GPU
memory after the first request. It checks continuation parity, not a bytewise
hash of the entire live device cache or compaction quality.
`--prefix-mode clone` is the default; `replay` retains the old path.
`--verify-clone` additionally reconstructs the prefix in the same request and
requires bitwise equality of all captured query values and sampled positions.
It records that reference time separately from copy-path collection time; HTTP
latency includes both when verification is enabled. Captures include a query
SHA-256 digest, copy/repeat timings and device-copy/allocation byte counts.

```sh
python3 zig/pkg/inference/scripts/gemma4/qualify_attention_match_queries.py \
  --server zig/zig-out/debug-bin/antfly-inference-bench-server \
  --models-dir .models --dtype f32 --verify-clone \
  --output zig/zig-out/qualification/context-cuda/native-attention-match-clone-f32.json

python3 zig/pkg/inference/scripts/gemma4/qualify_attention_match_queries.py \
  --server zig/zig-out/debug-bin/antfly-inference-bench-server \
  --models-dir .models --dtype f16 --padding-lines 0 114 742 --repetitions 1 \
  --output zig/zig-out/qualification/context-cuda/native-attention-match-clone-f16-long.json
```

### L4 capture evidence, 2026-10-17

The Debug CUDA build passed 17 baseline/probe pairs (34 successful generation
requests): six f16 pairs at 27/247/907 tokens, eight f32 pairs at
27/247/907/1281 tokens, and three f16 pairs at 27/1281/8189 tokens. The 1281-token
case exercises a one-token final chunk. Every probe captured all 56 query-head
batches and preserved the visible continuation and usage. Evidence is under
`zig/zig-out/qualification/context-cuda/native-attention-match-queries-{f16,f32,f16-long}.json`.
The focused inference suite passed 81 tests, including collector allocation
failure and missing-consumer tests; the Python qualifier suite passed 14.

For the 8189-token f16 case, isolated query collection took **96.406 seconds**,
including shadow-prefix reconstruction, repeat execution, capture and shadow
teardown. The whole request took 131.009 seconds versus 34.121 seconds for the
baseline. The planned host query-buffer bound was 17,570,940 bytes. Across the
warm long-context campaign, sampled probe peaks were 10,179,575,808 bytes GPU
memory (9.48 GiB) and 3,469,377,536 bytes host RSS (3.23 GiB), with no sampling
errors. Sampling does not certify instantaneous allocation peaks or cold model
load memory. This is a single long-context observation on a **Debug host build**,
not a production p95 benchmark, and includes no fitting/checkpoint work.
Latency remains an explicit release blocker; these results do not qualify the
10-second 8K compaction target.

### L4 prefix-copy evidence, 2026-10-17

The copy implementation passed another 19 baseline/probe pairs (38 generation
requests): eight f16 and eight f32 pairs at 27/247/907/1281 tokens, plus three
f16 pairs at 27/1281/8189 tokens. All 16 short/boundary pairs required bitwise
query equality with same-request replay. All 19 pairs covered all 56 global
query heads and preserved the fixture fact, exact visible choices and usage.
Evidence is under
`zig/zig-out/qualification/context-cuda/native-attention-match-clone-{f16,f32,f16-long}.json`.

At 8189 tokens, collection took **62.072 seconds**, versus the earlier replay
observation of 96.406 seconds (35.6% lower). Copying 469,590,016 bytes took
**10.621 ms**; the repeat pass took **61.760 seconds**. The shadow allocation
bound was 939,622,400 bytes. The full request took 97.848 seconds versus a
34.418-second baseline. Sampled warm probe peaks were 10,062,135,296 bytes GPU
memory (9.37 GiB) and 3,475,738,624 bytes host RSS (3.24 GiB), with no sampling
errors. The long run did not include the additional replay verification pass.

After reserving full page-table capacity to eliminate transient growth
allocations, the final CUDA build passed three more baseline/probe pairs at
27/1281/8189 tokens, all with bitwise replay verification. At 8189 tokens,
collection took **62.326 seconds** versus **96.451 seconds** for same-request
replay (35.4% lower); copying took **11.049 ms** and repeat execution took
**62.013 seconds**. All query values, sampled positions, visible choices and
token usage matched. This brings this copy campaign to 22 pairs / 44 successful
generation requests. Final evidence is
`zig/zig-out/qualification/context-cuda/native-attention-match-clone-final-f16.json`.
Its full HTTP time includes replay verification and is not the copy-only latency.

These are single long-context observations from Debug host builds, not p95 or
release-performance evidence. Copying removes reconstruction cost but leaves
repeat execution far above the 10-second target, before fitting/export. The
next latency work must profile and optimize repeat prefill/capture, qualify
the optimized host build and cover 32K; this change does not establish native
compaction quality, serving support or production readiness.

The expanded focused inference suite passed 93 tests, including clone alias,
cross-backend, unsupported-hook and cancellation rejection, checked admission,
the 32K f16/f32 budget boundary, fitting/checkpoint tests, SSE cleanup and native
grammar cases. The Python qualification suite passed 27 tests.

## Repeat-attention acceleration (opt-in qualification)

The E4B collector has a separate `exact` repeat route for SM89 and exact f16
KV, with eight query heads and two KV heads. Eight independent warps process
eight queries per block. Each warp preserves the reference's FP32 reduction
tree, chronological online softmax, and multiply/FMA rounding. It removes
block-wide synchronization without changing the numerical policy. The existing
one-KV-head E2B routes remain separate.

The override is held only during the isolated repeat pass, under the session
execution lock. Success, cancellation and errors restore the previous prefill
profile. Shape/storage mismatches and absent symbols fail closed. A launch
counter prevents qualification from accepting a silent fallback. This route
is not enabled by default and does not replace the live cache.

Use `qualify_attention_match_queries.py --repeat-mode exact --verify-repeat`
to require bitwise equality of every collected train/held-out query against
the reference in the same request, with identical consumer geometry and sample
positions. `reference_ns` includes verification collection; `elapsed_ns`
excludes it. `--profile-prefill` uses SSE and requires a CUDA operator profile
per request, while still checking unchanged visible continuation.
`--verify-attention --padding-lines 0` provides bounded per-layer output
comparisons on identical input tensors; those diagnostic runs are not latency
measurements.

The standalone `qualify_repeat_attention.cu` runner compares all output rows
against the serving F32 kernel, requires bitwise equality for the repeat
candidate, and checks independent FP64 spots from both KV groups. Run normal
and `--boundary` comparisons, then repeat with `--memcheck` under NVIDIA
Compute Sanitizer. The latter launches only the candidate and checks finite
outputs plus FP64 spots; its report explicitly says no full reference was
compared. Memory-boundary cases exercise maximum query count and maximum KV
length separately. Instrumented timings are not performance measurements.

Earlier tensor-core candidates passed isolated numerical and memory checks,
but failed the real E4B Q4 query gate: worst query relative L2 error exceeded
3%, against a predeclared 0.2% limit. Splitting both queries and probabilities
into high/residual components did not fix that failure. Direct layer checks
found attention differences of only a few parts per million, which still grew
through the model. Those candidates were rejected; the replacement preserves
the reference's FP32 operations exactly.

### Measured repeat results, 2026-10-17

The final Debug CUDA server passed six baseline/probe pairs on the L4 with
Gemma 4 E4B Q4 and f16 KV: two consecutive rounds of 27, 1281 and 8189 source
tokens. Every train/held-out query matched the same-request reference bitwise
across all seven global consumers / 56 query heads. Visible continuation and
token usage also matched. The 1281-token fixture exercises a one-token final
chunk through the existing decode route. The accelerated route launched
42/210/1344 kernels per collection respectively, excluding that decode tail.

| Source tokens | Exact collection, two runs | Reference collection, two runs | Mean time reduction |
| --- | --- | --- | --- |
| 27 | 0.133 / 0.136 s | 0.131 / 0.133 s | No improvement (1.7% higher) |
| 1281 | 3.128 / 3.174 s | 4.025 / 4.057 s | 22.0% |
| 8189 | 37.385 / 37.367 s | 62.178 / 62.293 s | 39.9% (1.67x faster) |

At 8189 tokens, repeat execution itself took 37.216 / 37.196 seconds;
prefix copying took 10.739 / 13.707 ms. Collection timings exclude original
ingestion and the additional reference verification pass. The probe's full
HTTP requests took about 135 seconds because they include both. Sampled warm
probe peaks were 10,062,135,296 bytes GPU memory (9.37 GiB) and 3,517,214,720
bytes host RSS (3.28 GiB), with no sampling errors. Two rounds do not establish
leak freedom or latency percentiles. These profiled, fixed-order observations
are correctness and optimization evidence, not the release performance gate.

Evidence is `zig/zig-out/qualification/context-cuda/repeat-exact-model-final.json`,
including source, model, binary and runner fingerprints. The source snapshot
was unchanged during the run. The final SM89 cubin is byte-for-byte embedded
in that server; both kernel symbols are present in portable PTX and all seven
SASS targets. The earlier ReleaseFast reference run still took 62.087 seconds
at 8189 tokens, so host optimization alone did not remove the bottleneck.

Standalone checks passed 40 ordinary and 16 maximum-boundary cases with
bitwise full-output equality, plus 24 Compute Sanitizer memory-boundary cases
with zero errors and independent FP64 spot checks. Evidence is
`repeat-exact-xor-address.jsonl`, `repeat-exact-final-boundary.jsonl` and
`repeat-exact-memory.log` in the same qualification directory. The focused
host suites passed 36 CPU tests and 38 CUDA-enabled tests (13/14 skipped);
the Python qualification suites passed 32 tests. Two preliminary short
requests also verified clone versus replay and every attention output, but
are not latency evidence.

This still misses the 10-second 8K target before fitting/export. Keep the
route opt-in. Full-model 32K, optimized-build percentiles, cancellation/OOM,
real cache replacement and compaction quality remain unqualified.

## Exact split repeat execution (2026-10-18)

`ANTFLY_INFERENCE_ATTENTION_MATCH_REPEAT_MODE=exact_split` is an opt-in
repeat-query route for E4B on SM89 with f16 KV. It retains the existing exact
local-window kernel and splits global HD512 attention into three kernels:
parallel FP32 scores, chronological softmax coefficients, and parallel value
recurrences. Each stage preserves the reference reduction and rounding order;
no tensor-core approximation or query tolerance is introduced. Original
positions, prefix-clone semantics, chunk256 and 64 training/16 held-out samples
per head remain unchanged. The default remains `reference`.

The score kernel assigns a key to each half-warp and a 32-dimension group to
each lane. It evaluates the reference's XOR reduction tree locally, then reduces
the 16 group sums within the half-warp. Explicit round-to-nearest multiply/add
operations, the zero-padded final-tree addition and runtime reciprocal square
root preserve the reference's score bits. Loading and the first addition level
are combined to keep the SM89 score kernel at 64 registers with no spills.
This reduces the representative 256-query/8192-offset global-attention case
from roughly 45ms for the first split implementation to 16.4ms. That is a
standalone case, not a model-level latency percentile.

Global attention processes slabs of at most 4096 keys. Scores become alpha
in place; beta and running maximum/denominator share one scope-owned allocation.
At chunk256 the allocation is 67,125,248 bytes, independent of context length;
the accepted query range is 2–512 and scratch remains below 160 MiB. Admission
occurs before route mutation. Cancellation is checked before each slab, and
scope cleanup restores the prior profile and frees scratch after queued use.
The live KV cache remains untouched. Single-token tails use the existing decode
route. The candidate is not a compacted-cache serving implementation.

The standalone L4 matrix passes 56 full-output bitwise comparisons (including
28 global intermediate score/alpha/beta/max/denominator checks), 16 cases at
maximum boundaries, and 24 Compute Sanitizer cases with zero errors. Cases
include permuted physical pages, sharp logits, slab crossings, 8K/32K and the
65536-token kernel limit. The hardware failure test exercises budget rejection,
20 cancellations after the first slab was queued, full original-cache hashing,
profile restoration, scratch accounting and a successful retry. It does not
inject a physical CUDA allocator failure. The packaged SM89 score instructions
match the tested prototype exactly; all three exports are present in portable
PTX and all seven SASS targets. The focused host suite passes 37 tests (15
skipped), the hardware failure test passes, and the four Python qualification
suites pass 40 tests. Evidence:

- `zig/zig-out/qualification/context-cuda/repeat-split-local-tree-early-matrix.jsonl`
- `zig/zig-out/qualification/context-cuda/repeat-split-local-tree-early-boundary.jsonl`
- `zig/zig-out/qualification/context-cuda/repeat-split-local-tree-early-memory.log`
- `.cache/native-compaction/repeat-split-local-tree-cuda-fault-tests.log`

The Debug full-model correctness run passes at 27, 1281, 8189 and 32763
tokens, including the one-token tail. Every collected training/held-out query
sample matches the reference bit for bit, and visible continuation and usage
remain unchanged. With the first
split score implementation, collection takes 32.75s at 8K versus a 62.04s
reference verification pass, and 318.28s at 32K versus 795.71s reference. These
profiled correctness measurements are not paired release-build performance
results. Evidence: `repeat-split-model-correctness.json` in the qualification
directory. The 32K query SHA256 is
`01f001db7333b6b487b63d79864cc057acef098ca47a3e17a7c394e461c98633`.

The initial phase report missed Q4 gate/up projection intervals; that reporting
hole is corrected. Other-GPU timing still excludes uninstrumented operations
such as the Q4×Q8 projection path. Capture host-wait time includes preceding
GPU execution and must not be described as removable transfer overhead. The separate
`capture_callback_ns` timer measures the collector after download completion.
A host-only collector microbenchmark scans the 8K query volume in 0.47s in
ReleaseFast. The complete model trace below resolves the 5% optimization gate;
no device-gather optimization is justified by the measured transfer/host costs.

`qualify_attention_match_queries.py` now separates three modes:

- `correctness`: baseline/probe visible continuation parity and full captured
  query comparison against reference for both exact routes. Optional short
  per-layer diagnostics and clone/replay comparison remain available.
- `profile`: per-request and repeat/prefix phase GPU timings, download counts
  and bytes, device allocation calls and capture host waits. Q4 pair projections
  are included without double-counting scalar fallback intervals.
- `performance`: verification/profiling prohibited, AB/BA route order, three
  warmups per block and ten measurements per route by default. Reports p50/p95,
  coefficient of variation, query digests and visible-continuation parity.
  A development speed gate requires three warmups, ten samples and at least 15%
  reduction in both p50 and p95. This report never changes runtime defaults.
  Release sampling still requires five warmups and 100 measurements.

All modes fingerprint source, binary and model and reject source changes during
qualification. Fact, configuration-code and tool-result-shaped text fixtures
are supported; the latter does not qualify actual tool-call API behavior.
The runner sets a 4 GiB KV sub-budget within the unchanged 18 GiB total GPU
budget so the 32K reservation is not rejected by the default 1.5 GiB KV cap.
Memory sampling starts after the first response and covers subsequent warm
requests, not cold-start peak memory. Example commands from the worktree root:

```bash
python3 zig/pkg/inference/scripts/gemma4/qualify_attention_match_queries.py \
  --server zig/zig-out/bin/antfly-inference-bench-server --models-dir .models \
  --mode correctness --repeat-mode exact_split --padding-lines 0 114 742 \
  --output zig/zig-out/qualification/context-cuda/repeat-split-correctness.json
python3 zig/pkg/inference/scripts/gemma4/qualify_attention_match_queries.py \
  --server zig/zig-out/bin/antfly-inference-bench-server --models-dir .models \
  --mode performance --repeat-mode exact_split --compare-repeat-mode exact \
  --padding-lines 742 --warmups 3 --repetitions 10 \
  --output zig/zig-out/qualification/context-cuda/repeat-split-performance.json
```

The first 32K attempt was rejected by the default KV sub-budget before
execution. With the explicit 4 GiB KV budget, original ingestion was admitted,
but the Debug worker was killed by the host OOM killer before reaching query
collection. Its anonymous RSS was 11,282,996 KiB (about 10.76 GiB), already over
the 8 GiB release target. Compilation was running concurrently and also consumed
host memory. This is a failed diagnostic, not numerical or memory qualification;
subsequent long-context checks must run without concurrent compilation and
include a short initial request so the sampler covers the entire long request.
Evidence: `repeat-split-model-32k-kv-budget.json` and
`.cache/native-compaction/repeat-split-32k-host-oom.log`. The request-local CUDA
manager now uses metadata-only pages: `KvStorageRuntime` retains ownership of
actual cache storage and existing host fallback bytes. Previously both manager
and storage allocated full host KV arrays. Shared prompt-cache pools retain
their existing ownership. The regression test appends and rolls back 20 times,
verifies stored KV values and page reuse, and rejects reading bytes from the
metadata-only manager. This removes redundant bookkeeping storage; it is not
numerical KV compression.

The subsequent full-model correctness run, without concurrent compilation,
passes through 32763 tokens. Its sampler covers the long request after a short
initial request: peak host RSS is 6,326,910,976 bytes (5.89 GiB), and device
memory is 14,432,600,064 bytes (13.44 GiB), with 5826 samples and no sampling
errors. Both fit the provisional warm-memory limits. Cold peaks, sustained
concurrency, physical allocator failure and release latency remain separate
gates.

Performance mode reports the warm 8 GiB host/18 GiB device gates
and fails those gates when memory samples are missing or sampling reports errors.

Use `--padding-lines 2976` for the 32763-token fact fixture, or `2950` when
combining all fixtures near 32K. Longer prompt headers otherwise exceed 32K.
The provisional 10s target covers collection **plus fitting/export** and remains
unmet; it is not a measured llama.cpp/MLX baseline. Promotion also requires the
release latency and reliability gates, independent of standalone kernel success
and full-model correctness.

### ReleaseFast model and latency qualification

The final SM89 server embeds the audited cubin byte for byte. Fresh direct
reference comparisons pass at 27, 1281 and 8189 tokens and on 39-token code /
47-token tool-result-shaped text fixtures. All collected training and held-out
query samples match bit for bit; visible continuation and token counts are
unchanged. The 8K profiled correctness pass takes 22.640s for collection versus
61.872s for the same-request reference. Evidence:
`repeat-split-local-tree-model-correctness.json` and
`repeat-split-local-tree-fixtures.json` in the qualification directory.

At 32763 tokens, the new binary's collected query SHA256 matches the samples
from the earlier full bitwise reference check. The comparison also requires
identical model-file hashes, request bodies, sample geometry, continuation and
token counts. This is digest agreement with previously verified samples, not
a new full reference pass. Collection takes 166.647s; peak host RSS is 5.84 GiB
and GPU memory is 13.42 GiB, with 1775 samples and no sampling errors.
Evidence: `repeat-split-local-tree-model-32k.json`, which fingerprints its golden
`repeat-split-model-correctness.json` evidence.

The unprofiled 8K AB/BA comparison uses three warmups per block and ten measured
samples per route. Both routes use the same ReleaseFast binary, model revision,
f16 KV, chunk256 and 64 training/16 held-out samples per head. Profiling and
reference-verification work are disabled. Query digests, continuation and token
counts must agree across every measured and warmup request.

| Repeat route | Measured samples | Collection p50 | Collection p95 | CV |
| --- | --- | --- | --- | --- |
| `exact` | 10 | 36.539s | 36.568s | 0.038% |
| `exact_split` | 10 | 22.409s | 22.428s | 0.037% |

The initial attempt stopped after its first block with `AddressInUse` while
restarting a worker; `repeat-split-local-tree-performance.json` preserves that
failed run. The runner now uses a distinct listener per block and requires a
successful health response before sending requests. Its four-block short-input
restart smoke passes; it is not latency qualification. The focused Python
suites pass 40 tests, including startup failure and connection-reset coverage.

The development speed gate is met: p50 and p95 both fall by 38.67%. The
warm-memory gate is met, with sampled peaks of 1.92 GiB host and 9.55 GiB device.

Evidence: `repeat-split-local-tree-performance-final.json`. These are component
measurements and exclude fitting/export. Ten measurements do not satisfy the
release requirement of five warmups and 100 measurements; 32K latency
percentiles and sustained concurrency remain unqualified. The provisional
10s/30s full-compaction targets remain unmet, and the runtime default remains
`reference`.

The complete Nsight trace accounts for all 791 calls per split stage across the
short and 8K requests, plus all 224 / 939,180,032-byte query downloads in the 8K
repeat pass. Its 22.805s collection contains 13.920s of quantized projections
(including 11.216s in the BF16 tensor-core path and 2.524s in the Q4×Q8 tile path), 7.881s
of attention, and 0.267s of other model kernels. Query downloads take 0.071s;
the CPU collector callback takes 0.205s. The interval from completed downloads
to the next kernel launch totals 0.466s and already includes the callback plus
other preparation; do not add those two values together.

The union of traced model kernel intervals is 22.067s. Subtracting it from full
collection time leaves at most 0.738s (3.24%) for host/transfer work, assuming
the retained model kernels are unchanged. This overestimates that headroom by
also including initial/final model kernels outside the traced repeat interior.
Consequently the planned 5% gate for query-gather optimization is not met, and
no device-gather route is added. Continue with projection and attention work.
Evidence: `repeat-split-release-timeline.analysis.json` and its `.sqlite` /
`.nsys-rep` trace. The per-phase `other_gpu_us` counter remains partial; use the
complete trace for total projection attribution.

### Staged Q4 projection qualification (2026-10-18)

The next measured bottleneck is the existing BF16 Q4 tensor-core projection:
11.216s of the 22.805s traced 8K collection. The new canonical
`cuda_q4_0_bf16_staged.cuh` keeps its 64x32 output tile, BF16 input/weight
rounding, and chronological 16-wide WMMA accumulation. It loads Q4 weights
along their contiguous K dimension, stages two K slices per barrier pair,
pads shared-memory strides, and reuses the input tile storage for output.
It requires no persistent or per-request device allocation.

Runtime selection is restricted to SM89 bundled SASS, `exact_split` repeat
scope, and 2–512 input rows. Ordinary ingestion/decode and reference/exact
comparisons retain the original projection. Missing optional symbols fall
back to the original BF16 kernel. Portable compute_75 PTX must not select the
empty BF16 compatibility entry.

The standalone harness `qualify_q4_bf16_staged.cu` compares all output bits
against the saved pre-change packaged cubin, not against another instantiation
of the new kernel. Four staging widths across 14 shapes and three input
patterns pass 168 comparisons. Shapes include row/output tails, K-stage tails,
2560→10240 FFN projections, and 2048/4096→2560 attention outputs. Patterns
include negative scales, all Q4 nibble values, activations beyond F16 range,
and negative zero. The selected width is 32; larger staging did not produce
a consistent improvement. Kernel timings are diagnostic, not model latency.
The reduced sanitizer matrix passes 72 comparisons with zero memcheck errors
and zero racecheck hazards/errors/warnings.

Evidence is under `zig/zig-out/qualification/context-cuda/`:
`q4-staged-matrix.jsonl`, `q4-staged-memcheck.log`, and
`q4-staged-racecheck.log`. The original module is `q4-staged-baseline.cubin`
(SHA-256 `664e0dd975996fe410731297b62de3f0f382fecc227a0884c7d2055359ba21b0`);
the saved ReleaseFast baseline server is `q4-staged-baseline-server`
(SHA-256 `e015d3605a60a7b556daf669001dc543a97597ef44b02e60054c3ac2fee3f4d6`).
Keep these immutable when regenerating artifacts. The regenerated SM89 export
passes all 42 shape/pattern comparisons and 18 packaged memcheck cases with
zero errors. Its 40 registers and 8 KiB shared memory require no local-memory
spills. All seven SASS targets contain the export; the fatbin's SM89 instruction
encodings match the numerically qualified standalone cubin. Other architectures
are compiled, not hardware-qualified for this route.

The query qualification runner now supports `--compare-server` in performance
mode, including the same repeat route in both binaries. It uses separate
variant labels, AB/BA ordering, distinct ports, per-block warmups, and checks
both binary fingerprints after execution. Query digests, continuations and
usage still have to agree. All 42 focused Python qualification tests pass,
including same-route aggregation and detecting a changed baseline binary.
This permits isolating projection changes from the already-qualified split
attention change:

```sh
python3 zig/pkg/inference/scripts/gemma4/qualify_attention_match_queries.py \
  --server zig/zig-out/bin/antfly-inference-bench-server \
  --compare-server zig/zig-out/qualification/context-cuda/q4-staged-baseline-server \
  --models-dir .models --mode performance \
  --repeat-mode exact_split --compare-repeat-mode exact_split \
  --padding-lines 742 --warmups 3 --repetitions 10 --port 8212 \
  --output zig/zig-out/qualification/context-cuda/q4-staged-performance.json
```

#### Model and paired latency results

The new ReleaseFast server is SHA-256
`700a025ff935ca289e639a1907e796148e62b8b042d6799b6aafa8ad38b8de30`.
Its embedded SM89 module was verified byte-for-byte against the regenerated
cubin, SHA-256 `6719c9dabbf1158ca709483b079d666ffdd02f9d8b3159f7e98078eb697d66f2`.

The fresh model comparison passes at 27, 1,281 and 8,189 tokens, including the
one-token chunk tail. All retained training/held-out query values agree bitwise
with a fresh reference pass, and visible continuation and usage are unchanged.
The 8K query digest remains
`876653d3465b022a75553e408ecde3d12243b7629af6fbb88d42a4908c4fa990`.
Short code and tool-shaped text fixtures also pass. These are query-collection
checks, not compacted-cache quality or actual tool-API qualification.
Evidence: `q4-staged-model-correctness.json` and `q4-staged-fixtures.json`.

The paired 8K benchmark uses the same `exact_split` route in both binaries,
Gemma 4 E4B Q4, F16 KV, 256-token chunks and 64/16 samples per head. Four fresh
servers run AB/BA, with three warmups per block and ten measured requests per
binary. Profiling and reference verification are disabled during timing.
Every query digest, continuation and token count agrees across all 32 requests.
Both binary fingerprints and the source snapshot remain unchanged during the run.

| Binary | Collection p50 | Collection p95 | CV |
| --- | ---: | ---: | ---: |
| Previous exact split | 22.397116s | 22.435740s | 0.0740% |
| Staged Q4 exact split | 17.794373s | 17.845984s | 0.1204% |

The reductions are 20.55% at p50 and 20.46% at p95, passing the 15% development
speed gate. Warm peak RSS is 2,058,899,456 bytes (1.92 GiB) and GPU memory is
10,255,073,280 bytes (9.55 GiB), passing the memory gate with complete sampling.
This excludes cold-start peak memory and original ingestion latency. The
five-warmup/100-sample release gate is **not met**. Evidence:
`q4-staged-performance.json` and its four `.round*.log` files.

The 32K golden comparison also passes at 32,763 tokens, with unchanged
continuation/usage and digest
`01f001db7333b6b487b63d79864cc057acef098ca47a3e17a7c394e461c98633`.
Collection takes 148.540852s in this single profiled request. This checks the
digest against the previously full-bitwise-verified reference; it is not a new
full 32K reference comparison or a percentile measurement. The short control
request starts sampling before the entire long request. Peak RSS is
6,268,841,984 bytes (5.84 GiB), GPU memory is 14,407,434,240 bytes (13.42 GiB),
and all 1,708 memory samples are error-free. Evidence: `q4-staged-model-32k.json`.

The complete new Nsight trace confirms 8,249 staged projection calls in the
8K repeat interior, taking 4.975870s. Total quantized projection work takes
7.945991s, including 2.773727s in the unchanged Q4×Q8 down projection and
0.196393s in its activation preparation. Attention now takes 9.073322s:
2.208106s local, 2.603240s scores, 0.961647s coefficients, and 3.300329s values.
The trace has all 791 calls per split stage across the short and 8K requests,
and all 224 query downloads / 939,180,032 bytes in the 8K interior.
Attention and Q4×Q8 instruction encodings were checked against the saved
pre-change cubin and are unchanged. Use the fresh paired results above for the
speedup, rather than subtracting individual timings from historical traces.

Downloads take 0.071475s; collector callbacks take 0.276340s. The conservative
non-kernel upper bound is 0.798774s / 4.41% of the 18.100460s traced collection,
so the 5% device-gather gate is still not met. Callback time is already included
in the measured post-download gap and must not be counted twice. Evidence:
`q4-staged-timeline.analysis.json`, `.sqlite` and `.nsys-rep`.

The 10s/30s full-compaction targets remain unmet by query collection alone.
Fitting, export and safe live-cache replacement remain unintegrated. The
default repeat route remains `reference`; this optimization remains opt-in
through `exact_split`.

## Fitting, export and replacement work (2026-10-18)

The native host engine now assembles candidates head by head instead of encoding
and decoding a full copy of the source. Admission reserves source plus candidate
before allocation and checks query conversion, replacement and solver workspace
at each fitted head. Local donors and the exact recent suffix retain their
original values. An explicit `storage_precision = .f16` option rounds fitted K/V
before rerunning held-out quality gates; bias and portable payloads remain f32.
A fit that passes at f32 but fails after f16 rounding is rejected without changing
the source.

`attention_match_checkpoint.exportTo` writes the existing v1 binary format with
a fixed 16 KiB staging buffer. It computes the header checksum in a first pass
and writes header plus payload in a second pass, checking cancellation at every
flush. The caller must hold the state immutable and publish its temporary output
only after success. Streaming output is byte-identical to the existing encoder;
this is not a new wire format or an atomic filesystem publication API.

`attention_match_snapshot` reads authoritative device rows through a dedicated
reader, with preallocation admission and bounded scratch. Shared donors are read
once, local donors export only their exact logical suffix, and resume tokens,
logits, original positions and donor mapping are retained. The CUDA reader
supports complete identity-mapped f16/f32 prefixes, fences each read, and rejects
missing, stale, shifted, remapped and unsupported encoded storage. It has no host
pool fallback. `NativeGenerationPipeline.snapshotAttentionMatchState` derives
the donor geometry from the loaded model configuration; its caller must supply
trusted fingerprints from loaded artifacts and lock the source through capture.
This adapter does not support already compacted caches or arbitrary page maps.

`attention_match_transaction.Cache(Device)` stages fitting and a backend upload
into separately owned host/device state. Commit checks generation and all logical
state invariants, fences the candidate and checks cancellation again before
publishing both owners together. The old entry is returned still alive so the
serving owner can refresh borrowed views before retiring it. Host tests inject upload/fence errors, cancellation, stale work and allocation
failure. `ops/cuda/attention_match_storage.zig` provides an independent CUDA
staging owner with f16/f32 K/V, per-head positions, coverage ends and f32 bias.
It checks the live context allocation ledger plus the complete candidate before
allocating, rejects values that were not already rounded to the selected storage
precision, fences uploads and retires partial allocations on error. It is a
storage backend, not a serving binding: bias/coverage-aware attention and borrowed
view integration are still required before replacing a live serving cache.

At this stage the attention value-load prototype was reproducible in
`scripts/gemma4/qualify_repeat_attention_kernel.cu` with
`-DANTFLY_QUALIFY_VECTOR_VALUES` (superseded by canonical-template qualification
in the full-model iteration below). It changes the lane-to-dimension assignment and
loads V in aligned uint4 packs while retaining every dimension's chronological
FMUL/FMA recurrence. It was qualification-only in that iteration: packaged kernels, dispatch,
and the last qualified server were unchanged. An alternating-order 256-query/8192-offset comparison (four observations per
variant) against a freshly built current-source baseline measured median
13.0519 ms versus 16.1638 ms for the three-pass attention operation: 19.25% lower.
These are standalone CUDA-event observations, not end-to-end latency or release
percentiles. The results and source/module hashes are in
`zig/zig-out/qualification/context-cuda/repeat-vector-paired-kernels.json`. Do not compare against
older `repeat-split-local-tree.cubin` artifacts: one of those predates the current
qualified score kernel and gives a misleading baseline.

Validation for this iteration is recorded in
`zig/zig-out/qualification/context-cuda/fit-export-staging-summary.json`, including
source/test-binary hashes and evidence hashes:

- 32 distinct host tests pass, including exhaustive allocation failures, f16
  rounding rejection, bounded streaming equivalence/cancellation, shared donor
  snapshots, partial read chunks and transaction rollback/retirement.
- Four real CUDA checks pass (the existing repeat cancellation check plus new
  snapshot, fit/export/swap and HD256/HD512 transfer checks). Both f16 and f32 are
  covered. Staging tests reject unvalidated f16 conversion, exercise a one-byte
  admission shortfall and mid-upload allocation-ledger failure, preserve the old
  buffers after cancellation and prove they remain readable until retirement.
- CUDA memcheck on the three new hardware tests reports zero errors and zero
  bytes leaked. Full 4096-element upload chunks and final partial chunks are
  covered; the boundary test caught and fixed a narrowed-integer byte-count
  overflow before completion.
- The final attention prototype passes 72 full bitwise comparisons, 24 boundary
  memcheck cases and one racecheck case, with zero errors/hazards. This does not
  replace full-model query/continuation checks or release-sized sampling.
- The pipeline snapshot adapter compiles in the focused CUDA test build. Use
  `-Dtest-filter` to narrow compilation in this repository; arguments after `--`
  only filter runtime execution.

These are synthetic device-state and numerical qualification results. Native
compacted-cache serving, real-model fitting latency/quality, checkpoint API
attachments, physical cuMemAlloc failure and sustained concurrency remain open.
The last qualified full-model query-collection measurement remains 17.794s p50 /
17.846s p95 at 8K; this iteration does not claim a new model latency result or
completion of the 10s/30s full-compaction targets.

## Vector value loads: full-model qualification, 2026-10-19

The vector value implementation now lives in the canonical
`cuda_gqa_repeat_split_f16.cuh` template and is available through the existing
opt-in `exact_split` repeat route. The standalone qualifier includes that template
directly; the test-only `ANTFLY_QUALIFY_VECTOR_VALUES` switch is removed. Runtime
routing guards and the default `reference` route are unchanged.

Portable PTX, the seven-target fatbin and the SM89 cubin were regenerated. An
instruction audit proves that the packaged L4 value kernel matches the previously
qualified prototype, the other 382 SM89 functions are unchanged, and the fatbin's
SM89 function matches the cubin. Register use remains 40 per thread, with no stack
or local-memory spills. Other GPU targets were compiled, not hardware-qualified.
A real ReleaseFast server was built and its embedded cubin checked; no executable
patching was used. The preserved baseline server is
`700a025ff935ca289e639a1907e796148e62b8b042d6799b6aafa8ad38b8de30`, and the
candidate is `7b40c44d921378ba5504820fa76737fa99b6f19afd3e3f84dae0570903477c50`.

The runner now reports query-collection and full HTTP request latency separately,
rejecting mismatched request/capture counts and invalid timing observations.
Query collection excludes original ingestion, fitting and export. HTTP timing
includes original ingestion, query collection and continuation; it still excludes
fitting, export and cache replacement. Neither metric is full-compaction latency.

Both binaries used the same L4, model artifacts, Q4 tuning, f16 KV, `exact_split`
route, chunk256 and 64 training/16 held-out samples per head. Profiling and
verification were disabled during timing. Four fresh processes ran in ABBA order.
At 8K, each block had three warmups and five measured requests, giving ten measured
requests per binary. At 32K, each process first ran a short prompt to load the
model and start memory sampling, then one long request: two long observations per
binary, with no long-context warmups. The 32K comparison is diagnostic and does
not qualify steady-state percentiles.

| Context | Metric | Baseline median | Candidate median | Reduction |
| --- | --- | --- | --- | --- |
| 8,189 tokens | Query collection | 17.8028s | 16.4482s | 7.61% |
| 8,189 tokens | Full HTTP request | 50.4752s | 49.1041s | 2.72% |
| 32,763 tokens | Query collection | 148.1401s | 127.0963s | 14.21% |
| 32,763 tokens | Full HTTP request | 460.5436s | 439.5756s | 4.55% |

At 8K, empirical query p95 falls from 17.8295s to 16.4744s (7.60%); HTTP p95
falls from 50.5087s to 49.1598s (2.67%). Query-time variation is about 0.10–0.12%
CV. The 15% development speed gate is **not met** despite satisfying its 8K
sample/warmup counts. The 32K run also lacks those counts. The 10s/30s
full-compaction targets remain unmet: query collection alone exceeds them.
The optimization remains opt-in, and no release gate or default was relaxed.

Memory sampling completed without errors in every block. Across both binaries,
8K peaks were 1.92 GiB host RSS and 9.55 GiB GPU; 32K peaks were 5.84 GiB host and
13.42 GiB GPU. GPU peaks were identical between variants at each size. These
measurements cover requests after the first response in each process, excluding
cold-start peak and future fitting/replacement allocations.

Fresh full captured-query comparisons against reference pass for fact prompts
at 27, 1,281 and 8,189 tokens, code text at 39 and 1,293 tokens, and tool-result-
shaped text at 47 and 1,301 tokens. Every query is bitwise equal, and continuation
and token usage are unchanged. Tool-shaped text is not a tool-call API test.
All 40 requests in the two performance runs, including short prompts and warmups,
also match the previous reference-verified query digests, continuations and usage
with identical model artifacts. That is golden-sample agreement at 32K, not a new
full 32K reference execution. The 43 focused Python qualification tests pass,
including the new timing-scope and malformed-observation checks.

Evidence, configuration, source/model/binary fingerprints and log hashes are
recorded in `zig/zig-out/qualification/context-cuda/vector-model-summary.json`.
The adjacent `vector-model-correctness*.json`, `vector-model-performance-8k.json`,
`vector-model-performance-32k.json`, `vector-model-golden-agreement.json` and
`vector-model-artifact-audit.json` retain the underlying observations. Their
`passed` fields describe execution/correctness; speed, memory and release-sample
qualification are separate fields.

The saved baseline profile also identifies an end-to-end priority. Its per-request
prefill attention counter is 369.703405s, including 113.241679s in repeat-query
attention: 256.461726s remains in original-prefill attention. Decode rows are
excluded from this counter. This subtraction uses the previously recorded profile
from the same baseline binary/model, not a fresh candidate profile or a prediction
of attainable speedup. Its provenance is in `vector-model-baseline-attribution.json`.
Optimizing only repeat attention leaves this large ordinary-prefill cost intact.

## Remaining integration, in order

1. Qualify the public prepare/resume endpoints on the optimized server, including
   real-model export, import after process restart, empty-suffix saved logits,
   multi-chunk suffix execution, SSE and unchanged tool-workload fallback. CUDA
   Debug compilation, API/SDK regeneration and 36 malformed/oversized HTTP
   rejection checks pass. Full-model HTTP portability is still unproven. The
   linked unified-server route/ABI changes also need integration qualification.
2. Stress peak memory and ownership lifetimes on the public path. The optimized
   private 32K run now passes the 8 GiB RSS cap at 7.94 GiB, with only about
   60.5 MiB headroom. That single run is insufficient evidence for concurrent
   uploads, error recovery or sustained operation.
3. Verify the explicit `unchanged_insufficient_savings` outcome with actual tool
   requests. Preserve whole heads that fail validation and preserve the original
   state whenever fitting cannot meet the threshold. Do not count an unchanged
   fallback as successful compaction. Further latency optimization is deferred
   following the user's acceptance of current performance.
4. Preserve the numerical query/continuation checks when qualifying the public
   path. Its CUDA fitting, export, import and suffix dispatch must retain the
   same position, coverage, bias, precision and model-identity invariants as the
   previously tested private replacement and fresh-owner restore paths.
5. Complete diverse fact/instruction/code/actual-tool quality tests, full-model
   physical OOM recovery, cancellation, consecutive SSE, concurrency and sustained
   request/compaction counts. The twenty-cycle real-model fixture passes, but
   does not substitute for these tests.
6. Complete the documented workload sampling and same-workload llama.cpp
   comparisons. Report latency variation and memory alongside quality, without
   reinstating the accepted 10s/30s compaction targets as release blockers.
   Automatic mid-generation compaction remains separate qualification.

## Release gates

These are initial engineering targets, not measured claims. Use the same model
revision, quantization, tokenizer, token IDs, KV precision, input/output lengths,
sampling settings and hardware for full-KV, compacted and llama.cpp comparisons.
Report ingestion, query generation, selection, mass fitting, value fitting,
upload/export/import, TTFT, decode and total latency separately, plus amortized
break-even after repeated continuations. Record exact source/binary/model hashes.

| Area | Required evidence |
| --- | --- |
| Scope | Gemma 4 E4B Q4 on L4; 8K and 32K logical contexts; at most 8K newly appended inputs within the cumulative limit |
| Compression | Qualify 2x and 4x first; 4x eligible old global rows and at least 2x actual global-cache bytes on the designated 32K workload; count local/recent rows and all metadata separately |
| Quality | At most 2 percentage points aggregate and 5 points per category versus full KV; all designated critical fact/instruction/tool fixtures pass |
| Generation latency | Same-workload p50 at most 1.25x and p95 at most 1.5x llama.cpp |
| Compaction latency | Current latency accepted by the user on 2026-10-19; record query generation, fitting and export separately from ingestion. Historical 10s/30s targets no longer block this release. |
| Memory | Peak device memory at most 18 GiB and host RSS at most 8 GiB, including temporary original/replacement/fitting allocations |
| Sampling | At least 5 warmups and 100 measured runs per designated workload |
| Reliability | At least 1000 mixed requests, 100 compactions and 20 successive real-model compaction cycles; no state corruption, leaked admission, or unbounded growth |

8–50x compression, automatic mid-generation compaction, additional models and
additional backends remain experimental until independently qualified. The
prior text-summary benchmarks and their latency/recall failures do not qualify
this numerical engine. Synthetic unit and kernel results cannot satisfy these
release gates.

## Real-model fitting diagnostic, 2026-10-19

Production remains blocked by actual fitting quality as well as serving
integration and latency. The first diagnostic with the real E4B CUDA cache
passed authoritative snapshot, bounded encoding, independent f16 upload, and
unchanged continuation for a 27-token prompt. It fitted zero chunks because the
prompt was inside the exact 2048-row recent window; this is an adapter check,
not evidence of successful compression.

The 2117-token prompt rejected its first fit with `CompactionFitRejected`:
donor layer 5, KV head 0, chunk offset 0, one attempted and zero completed
chunks. Follow-up diagnostics establish that this is a pre-storage fitting
failure: mass error 0.870884 and output RMSE 0.261765. The 8189-token workload
also rejects its first full 512-row chunk before rounding: mass error 0.533733,
output RMSE 0.096788, 903 NNLS sweeps. Both are donor layer 5, KV head 0.
Do not relax the 5% fitting thresholds or infer readiness from query-only timings.

Evidence: `zig/zig-out/qualification/context-cuda/fit-model-debug.json` and its
baseline/probe logs. Debug server SHA-256:
`75ddf306060b064bc9bd132aa86833e572b0ccc67673062f250e91079b442ef0`.
This run used profiling and is not latency qualification. The failed request is
recorded as failed; the short successful check does not make the run pass.
The subsequent `fit-model-diagnostic.json`, `fit-model-8k-diagnostic.json`, and
`fit-model-8k-full-head.json` record the reproducible failures and model/source/
binary hashes. The 8K first-chunk diagnostic binary is
`d9c368c7c09902fb15dc149278b8d841d489b6384ba86207b641178dedd085e0`.

The opt-in `--fit-probe` qualification mode now exercises the entire preparation
chain after query collection: snapshot with resume logits, f16-rounded fitting,
bounded portable encoding into a counting/hash sink, and independent device
upload. It reports phase times and allocation sizes. It never installs the
candidate or emits an importable checkpoint; the diagnostic uses sentinel
identities only in its internal sink. Loader-authenticated artifact identity and
the serving owner remain required before exposing export/import to clients.
`--fit-fixture PATH` saves the first global head's first eligible chunk and its
pooled queries for offline numerical diagnosis. This is a diagnostic data file,
not the checkpoint interface, and its I/O invalidates latency comparisons.
`--fit-fixture-full-head` includes all rows of that head, enabling a comparison
with its exact recent suffix. The qualification runner validates fixture shape
and pool sizes and hashes the captured file. It preserves query/fit diagnostics
even when the request fails and retains the error payload from SSE error events.

Offline experiments on the captured 8K head, using f16-rounded candidates and
the original held-out queries, also fail the complete-head mass gate:

| Selection / exact prefix / chunk rows | Max relative mass error | Output RMSE |
| --- | ---: | ---: |
| RMS / 0 / 512 | 0.513251 | 0.073075 |
| RMS / 128 / 512 | 0.195810 | 0.039034 |
| RMS / 512 / 512 | 0.282519 | 0.043471 |
| Residual selection prototype / 128 / 512 | 0.144210 | 0.013706 |
| RMS / 128 / 2048 | 0.338858 | 0.040325 |

These isolated experiments defer per-chunk rejection only to measure the combined
error. They do not alter runtime acceptance, install a cache, or establish
model-level quality. The alternative selection and fitting scripts remain
diagnostic prototypes. Neither bounded weights nor residual selection passed
the original first-chunk gate.

Increasing capture from 80 to 320 positions per query head (256 training, 64
held out) preserves bitwise query agreement against the reference repeat route:
`fit-model-8k-dense-queries.json`, query SHA-256
`79be282e85feab583d2ad1fcf2f24403307ae461887c3df05c6c5f59dd91d6dd`.
Its first-chunk fit still fails (mass 0.439040, output 0.062837, 1806 sweeps).
The diagnostic query peak is 45,105,420 bytes, within the 64 MiB collector cap.
Offline dense-query experiments with an exact 128-row prefix hit the existing
4096-sweep convergence limit for both 512- and 2048-row chunks.

`--query-samples` and `--repeat-chunk-size` make capture experiments explicit;
the reference verification uses the same sampling and chunk geometry. Defaults
remain 80 samples and 256-row repeat chunks. A 512-row repeat path is not yet
qualified by the results above.

Next: improve mass-fit convergence using the captured fixtures, then resolve
held-out quality without weakening the acceptance gates. Recheck all real-model
donors and the designated 8K/32K workloads before qualifying serving or latency.
The paper's [Appendix C](https://arxiv.org/html/2602.16284v2#A3.SS2) motivates
testing a least-squares warm start; our native solver must still meet its KKT
convergence and held-out checks. No replacement dependency is introduced.

## Recorded checks, 2026-10-17

- L4 CUDA reference: 96 cases passed, including 8K/32K physical caches; maximum
  absolute output error 2.72e-7 and log-mass error 9.90e-7 versus FP64, with zero
  Compute Sanitizer memcheck errors. Evidence is in
  `zig/zig-out/qualification/context-cuda/native-attention-match-reference.json`.
- SSE lifecycle: 20 immediate follow-up requests to the rebuilt Debug CUDA
  server, concurrency one, all HTTP 200 with `[DONE]`; usage alternately enabled
  and disabled. No admission 503 occurred. Evidence and binary/controller/server
  hashes are in `zig/zig-out/qualification/context-cuda/native-sse-release.json`.
  This run is not a release latency benchmark.
- Host reference: 21 tests pass, including fault injection and 20 synthetic
  append/compact/checkpoint cycles. Qualification-controller tests also pass.

These checks cover the implemented components, not the remaining serving
integration or the real-model release gates above.

### Residual selection and complete-head validation

The follow-up independent numerical oracle found a passing basis for the first
real 8189-token global head. With 256 training and 64 held-out positions per
query head, an exact 128-token prefix, 512-row old chunks, 4x old-row retention,
and f16-rounded K/V, residual orthogonal matching pursuit (OMP) produces a
complete-head mass error of 0.0282452761 and output RMSE of 0.0065909127. The
native active-set solver reproduces these errors to floating-point tolerance.
This is one head on one fixture, not model-level or serving qualification.

The independent oracle uses SciPy only as an offline cross-check. No external
compaction engine is imported into the runtime. The native implementation now
has explicit `residual_omp` selection and `active_set` NNLS options. It uses
scaled Householder QR for active-set subproblems, twice-reorthogonalized
incremental QR for feasible OMP steps, and the original direct normalized KKT
convergence tolerance. Negative or inaccurate unconstrained solutions fall back
to the constrained solver. The RMS/coordinate reference defaults remain intact.
Workspace admission includes the selection transpose and incremental QR arrays.

`prefix_rows` preserves initial logical positions exactly and rejects a request
for an exact prefix already lost to earlier compaction. The explicit
`whole_head` validation option assembles private chunks, rounds storage, and
applies the unchanged 5% mass/output limits to the complete head before returning
a candidate. Chunk errors remain diagnostic in this mode. There is no path
that returns an unchecked rounded head. Query buffers, replacement buffers,
full-head validation conversions, and solver scratch are admitted before
allocation. Focused tests exercise constraints, rank deficiency, insufficient
iterations, prefix preservation, f16 rejection, and allocation failure cleanup.

Evidence for the independent/full-repeat result:
`fit-model-dense-scipy-omp-oracle.json`,
`fit-model-8k-dense-full-head.f32` (SHA-256
`e7e9b8d0a46c8b30e21b65d467e405d6cf12745fbd54d08da9c2bc1c8dbeedb4`),
and `.cache/native-compaction/fit-full-head-8k-dense-active-prefix128.log`.
The slow native reference took about 59.7 seconds for this one head; it does not
meet the full-compaction latency target.

### Bounded repeat and all-head diagnostics

`--reference-suffix-tokens 512` is an opt-in experiment: repeat only the last
512 tokens over the complete cloned source prefix, at true future positions.
It does not truncate the original cache or synthesize queries from keys. With
320 total sample positions per query head, query collection matched the
corresponding reference repeat bit for bit and took about 0.99 seconds in a
Debug/profiling run. This is not ReleaseFast latency qualification or a
full-compaction timing. Full-repeat behavior remains the default.

For the same first head, the independent bounded-repeat oracle passes with
1024-row chunks: mass error 0.0363989100, output RMSE 0.0062053530, and 3680
physical rows from 8189 (exact prefix + fitted old rows + exact recent suffix).
The native engine reproduces this result. The bounded-repeat fixture SHA-256 is
`ebc716da97140a702cfe5f0e49d57b7d60ad1e011d18eba0401f15714f9a4f08`.
Evidence: `fit-model-suffix-scipy-omp-chunk1024-oracle.json` and
`fit-model-8k-suffix-queries.json` under
`zig/zig-out/qualification/context-cuda/`.

`--fit-fixture-all-heads` captures every global donor/head for numerical
reproduction; its payloads remain diagnostic data, not portable checkpoints.
`--fit-residual` selects native residual selection, active NNLS, exact prefix128,
chunk1024, and whole-head validation inside the preparation probe. Neither
option authorizes live replacement. Full-head capture covers donor layers
5, 11, 17, and 23, with two KV heads each; layer23 pools 4096 training and 1024
held-out queries per head, reflecting its shared consumers.

The first all-head capture still uses RMS fitting to demonstrate the original
rejection and leave live state untouched. Capture server SHA-256:
`891ffad482578c02061e86fdba2dc769145e19c363996bdfda11235017a7cfab`.
Its query digest and all source/model hashes are in
`fit-all-heads-suffix.json`; the expected failed fit does not make that run pass.
Offline native per-head preparation is recorded separately in
`fit-all-heads-native-summary.json`. Already, layer5/head1 rejects at 0.0575706
mass error while layer5/head0, both layer11 heads, and layer17/head0 pass. Thus
the passing first-head result must not be generalized to the model. Preserve
these rejection gates while investigating basis/prefix/query choices, then
qualify on separate prompts and continuation horizons.

The immediate work remains: achieve quality across all real heads and diverse
held-out workloads; accelerate fitting with bounded, independently checked CUDA
numerics; then bind bias/coverage-aware attention and transactional live cache
replacement, loader-authenticated identities, and portable API import/export.
After that, run full-model quality, memory, latency, cancellation/OOM/concurrency,
and successive-cycle qualification against the release table above. Query-only
speedups and per-head passes cannot substitute for those gates.

### All-head acceptance and native GPU fitting (2026-10-19)

The bounded-repeat 8189-token capture now has complete native numerical results
for all eight global heads. The table reports maximum held-out mass error and
relative output RMSE after f16 rounding, using exact prefix128, recent2048,
chunk1024 and nominal 4x old-row retention. The unchanged limits are 0.05 each.

| Donor/head | Mass error | Output RMSE | Result |
|---|---:|---:|---|
| 5/0 | 0.036398910 | 0.006205353 | Accept |
| 5/1 | 0.108489570 | 0.009199345 | Reject |
| 11/0 | 0.036573890 | 0.004886257 | Accept |
| 11/1 | 0.007879650 | 0.002654814 | Accept |
| 17/0 | 0.042297602 | 0.009878374 | Accept |
| 17/1 | 0.084324445 | 0.011216658 | Reject |
| 23/0 | 0.035784171 | 0.008849926 | Accept |
| 23/1 | 0.029437702 | 0.006842215 | Accept |

Earlier rejection numbers such as 0.0575706 were the first threshold crossing,
not the maximum over the complete held-out set. The validator now computes the
complete finite maximum before rejecting. Full-repeat queries also reject 5/1
(mass error 0.090546342); the suffix experiment alone does not explain this.

An explicit `preserve_original` rejection policy retains a failed head's source
unchanged. It catches quality/convergence failure only; cancellation, allocation,
layout and validation failures still propagate. Defaults still reject the whole
candidate. An optional minimum actual global byte saving is an acceptance gate.
On this capture, six accepted heads and two preserved heads use 79,531,144 global
device bytes versus 134,168,576 exact source bytes: **1.686994x actual saving**,
including candidate metadata. This is not 4x achieved compression and does not
qualify the unmeasured 32K minimum-saving gate. Qualification rejects a long
probe that prepared private chunks but ultimately saved no global bytes.

The original CPU all-head preparation took about 815 seconds. A qualification
CUDA backend with our incremental active-set QR additions and Givens deletions
reduced a single-process all-head run to 12.019 seconds; fused column norm and
projection reduced it to **11.120 seconds**. Both reproduce the table above.
These timings include fixture-based preparation only and exclude real query
collection, checkpoint export, upload and serving. They are neither p95 nor a
passing 10-second full-compaction result. Evidence is in
`fit-all-heads-{native-summary,updates-preserve,fused-preserve}.json`.

The diagnostic CUDA implementation uses cuBLAS/cuSOLVER FP64 matrix primitives;
selection, constraints, convergence and acceptance remain native code. Eleven
independent numerical cases plus cancellation/work limits pass. Fused-kernel
memcheck with explicit CUDA API errors reports zero errors/leaks, and racecheck
reports zero hazards. An earlier extended API-error run reported cuBLAS internal
kernel-discovery messages; its failed log is retained, not counted as a pass.
SciPy is an offline oracle dependency only.

The numerical engine now accepts synchronous backend operations while retaining
native final NNLS convergence and rounded whole-head quality checks. Validation
batches are capped at 64 queries independently of training count. Admission
includes their scratch and backend-declared host workspace. Tests cover invalid
accelerator indices, nonfinite results, workspace refusal before dispatch and
allocation-failure cleanup. A context-owned Zig CUDA backend is being qualified
separately; prototype timings do not qualify its integration. Serving dispatch,
real portable checkpoint identities, atomic live replacement and the complete
release matrix remain outstanding.

### Integrated preparation probe and synchronization repair

The context-owned Zig fitter now uses the model CUDA context/stream and tracked
DeviceBuffers, with explicit device admission and a fixed 4 MiB cuBLAS workspace.
Its kernels have a separate reproducible fatbin/manifest for the repository's
seven architecture targets. No runtime NVRTC compilation is needed (the locally
installed toolkit's PTX version exceeds the driver JIT version).

Expanded native racecheck found a shared-memory reuse race in fitting softmax:
all warps must read the maximum before the same array is reused for sums. The
kernel now has that barrier. The pre-fix failed log is retained; earlier
prototype timing/quality samples do not qualify this repaired implementation.
Post-fix checks cover eight attention geometries/softcaps with bias, eight
selection fixtures against native FP64/KKT, cancellation, work limits and OOM
after partial device allocation. Memcheck reports zero errors/leaks and
racecheck zero hazards. Source, artifact and binary hashes are recorded in
`native-cuda-fit-safety.json`.

`native-fit-model-8k-debug.json` is the first successful integrated real-model
preparation probe. It uses CUDA fitting with six accepted/two unchanged heads,
passes the unchanged quality limits, encodes 201,728,532 checkpoint bytes, and
uploads 100,912,264 device bytes. Query capture is bitwise identical to the
reference and the original continuation remains unchanged. The encoded state
has private sentinel identities and is not a publishable attachment; the probe
still reports `installed=0`. Actual global storage saving is 1.686994x.

Debug preparation timings, excluding original ingestion: queries 0.986 s,
snapshot 1.315 s, fit 41.437 s, export 44.321 s, upload 1.951 s, total 90.009 s.
This correctness run is not a ReleaseFast or p95 qualification. Its memory
sampler started only after the first response, so its reported retained memory
is not a transient peak. The runner now resolves the private worker before the
first request and confirms that PID against response headers, allowing subsequent
runs to cover initial loading and every preparation phase.

A ReleaseFast one-head Nsight diagnostic shows fitting is dominated by repeated
host/device control transfers and kernel launch overhead: 7,894 device-to-host
copies, 3,496 host-to-device copies and 11,520 stream synchronizations. Copy API
time is about 58% of measured CUDA API time; actual kernel work totals about
0.258 s in a 1.475 s fitted-head run. This supports investigating device-resident
selection/NNLS control while retaining the direct convergence and held-out gates.
Evidence: `native-fit-head0-profile.nsys-rep` and its API/kernel summary.

### Optimized integrated measurement and ragged attention foundation

`native-fit-model-8k-release.json` records a single ReleaseFast correctness run
(binary SHA256 `8339b424a678510f9789a78cada205cffb5b5943d9139602ac0ce0e97b5bafca`).
The 8,189-token candidate reproduces the Debug checkpoint digest, the six
accepted/two preserved heads and 1.686994x global byte saving. Timings excluding
original ingestion are query collection 0.943 s, snapshot 0.670 s, fitting
20.566 s, export 2.247 s, upload 1.193 s, total **25.620 s**. This is neither a
passing 10-second result nor a p95 measurement. The original continuation is
unchanged; the candidate is still private and is not installed.

Sampling starts before the first request and covers loading and preparation.
The probe's sampled peaks are 5,670,539,264 host RSS bytes and 10,309,599,232 GPU
bytes, with 353 observations and no sampler errors. These are sampled peaks,
not a proof that shorter transients cannot exceed them.

A separate ragged attention kernel now supports independently sized heads,
f16/f32 K/V, mass bias after softcap, causal coverage and original logical
positions, and wrapped exact local windows. Split reductions bound attention
scratch without padding every head to the longest head's K/V allocation. Its
standalone differential harness passes 337 cases against an independent FP64
oracle, including positions above 2^32, all-masked queries and 32K rows. The
193-case racecheck subset reports zero hazards. Compilation and these checks
alone do not qualify model serving. The native context-owned wrapper and live
replacement integration are being qualified separately.

### First live fitted-cache continuation

`ragged-live-model-8k-debug.json` passes an actual private cache replacement and
continuation: the 8,189-token candidate is installed (`installed=1`) and generation
returns the same `violet-729` fact and five completion tokens as the exact baseline.
The checkpoint digest and six accepted/two preserved heads are unchanged. This
is one fixture, not downstream quality qualification. The measured Debug server
SHA256 is `950975c31812f9cc37354cf853f23261eb17933e8dce96eae3cfa738dcdd5099`.
Its 90.887-second preparation includes a 0.813-second installation; it is not an
optimized latency result. Sampled probe peaks are 5,732,687,872 host bytes and
10,286,530,560 GPU bytes.

The private binding fences uploads, rechecks the exact-source revision and
geometry, and transfers ownership before retiring dense device buffers. Failed
forward passes invalidate the binding; unsupported masks, graph capture,
rollback/speculation, and legacy exact reads cannot fall back to dense storage.
Appended rows keep logical positions and zero mass bias. Local rows stay available
for every shared consumer of a multi-token chunk before window retention.

Native qualification covers f16/f32 attention, bounded scratch and device
admission, source/output alias refusal, nonfinite append rejection, cancellation
after the append kernel, 600 local-window steps per precision, wrapped-buffer
growth, and authoritative device readback through encode/decode/re-upload.
Memcheck and racecheck report no errors/leaks/hazards on the exercised paths.
The wrapper and artifact are separate from the older dense f32 reference kernel.

Further runs require explicit serving-use counters in addition to `installed=1`.
Public loader-authenticated identity, checkpoint transport/resume, repeat
compaction of an already fitted state, broader model quality, and the full
latency/reliability release matrix remain open.


### Optimized live run exposed EOS-lookahead rollback

The stronger serving-use gate rejects `ragged-live-model-8k-release.json`, even
though both the exact and installed-cache responses are `violet-729` (five
completion tokens). The installed cache executed 252 attention calls and 144
donor appends, but ended at 8195 logical tokens with `invalid=1`. Ordinary CUDA
asynchronous token readback computes an EOS lookahead row and then truncates it;
the initial fitted binding rejected every truncation. This is a serving
integration failure, not a passing release result. The earlier Debug response
check did not inspect that final validity state.

That ReleaseFast run measured 25.734200s including reference queries, snapshot,
fitting, export, upload and installation: 0.950466s, 0.670913s, 20.675825s,
2.120339s, 1.128950s and 0.187687s respectively. It retained the same six
accepted/two exact heads and checkpoint digest as the preparation-only run.
Sampled peak RSS was 5,667,950,592 bytes and GPU memory 10,007,609,344 bytes.
The preserved binary is `ragged-live-model-release-server`, SHA256
`8109c03e99ecbd6f9765abfa096b969e977469c3ce9d5aaaae755163a702eb7c`.
This single correctness run also misses the 10s latency target.

The repair tracks the earliest recoverable position of the latest exact append,
validates all donor heads, fences, and removes only that exact tail. Evicting
local history advances the rollback floor; rollback across fitted/evicted
history or a failed forward stays rejected. The standalone native suite verifies
cancellation before publication, multi-row rollback/reappend, local-window
rejection after eviction and rollback through wrapped storage, for f16 and f32.
Independent device clones preserve bias, coverage and ring order, admit their
full simultaneous allocation, and leave source bytes unchanged when mutated.
Both paths pass memcheck (zero errors/leaks) and racecheck (zero hazards);
`native-cuda-clone-rollback-safety.json` fingerprints this component evidence.
The repaired live-model run remains required.

Parallel numerical experiments have a corrected cooperative NNLS control kernel
and independent worker streams. All eight captured heads complete with the same
quality gates: four workers with ordinary allocations take 8.828834s; a prototype
using stream-ordered allocation takes 6.140481s. The latter passes the independent
11-case numerical oracle, cancellation/work limits, memcheck and racecheck.
These runs overlapped server compilation, measure fitting only, and use an
unintegrated qualification library. They establish an implementation direction,
not production latency evidence. Native integration still needs owned streams,
bounded/accounted scratch reuse, aggregate host admission and cancellation.

The repaired 8K Debug model run, `ragged-rollback-model-8k-debug.json`, now passes
the stronger serving gate: source 8189, final logical length 8194, 252 attention
calls, 144 donor appends, one exact-tail rollback, and `invalid=0`. Both responses
remain `violet-729`; checkpoint SHA256 remains
`d81dcf133d35bc3c18dd53866eee70d5206b88dc9c34670865304411ae45dd03`.
The preserved binary is `ragged-rollback-model-debug-server`, SHA256
`7f972ec8f77e6a39873c27848f949962e93b74be5bcdd0911b66d7c4a092f552`.
Its 91.601613s Debug compaction timing is not an optimized latency measurement.
The installation test also passes 40 f16/f32 cycles covering stale revisions,
post-fence cancellation/reentrant mutation, allocation failure, transfer and
retirement. The containing unfiltered build later failed on an unrelated CLI
compile due to disk exhaustion; that is not a full-suite pass. Use
`-Dtest-filter='attention match CUDA live install'` for compile-time selection on
the current toolchain, in addition to the runtime filter after `--`.

The native cooperative selector now has an explicit private benchmark option,
`--fit-cooperative`. Captured eight-head fitting completes in 14.296764s with
12,032 cooperative launches, zero rank-deficiency restarts, six accepted heads
and two preserved exact heads. This is a numerical fixture measurement, not
full-model or percentile latency. `native-cooperative-eight-heads.json` retains
the binary and input hashes. The harness requires actual launch counters and
continues to enforce the original whole-head gates.

Both compaction fatbins were rebuilt with NVIDIA nvcc 13.2.86 (CUDA 13.2 Update 2).
Its [release notes](https://docs.nvidia.com/cuda/archive/13.2.2/cuda-toolkit-release-notes/index.html)
report a compiler reconvergence fix relevant to divergent kernels. Native
cooperative selection passes memcheck and racecheck; the updated ragged-cache
binary passes memcheck including rollback, clone and f16 conversion. The exact
integer f16 conversion is exhaustively checked over all 65,536 half bit patterns
and additional f32 admission cases.

Full native fitter memcheck and racecheck now pass with zero reported errors,
hazards or leaks using Compute Sanitizer 2026.3.0.0 build38855100, the system
580.159.03 driver and system cuBLAS 13.4.0.1. No API-error filters or suppressions
are applied. The previous 48 cuBLASLt internal loader reports also disappear in
the independent cuBLAS-only reproducer. The versioned sanitizer resolves that
component-qualification blocker; it does not establish full-model release
qualification. See `native-fit-full-safety-sanitizer-2026.3.json` and NVIDIA's
[Compute Sanitizer release notes](https://docs.nvidia.com/compute-sanitizer/ReleaseNotes/index.html).

Parallel native fitting is under qualification. It uses independently owned
streams/BLAS handles, an atomic context allocation ledger, bounded retained
scratch, serialized caller cancellation, private per-worker diagnostics and
joined worker lifetimes. The host scheduler reserves source plus worst-case
candidate once, and divides the remaining workspace budget among workers.
Forty-one engine/checkpoint tests pass, including serial/parallel checkpoint
identity and worker failure/cancellation admission. A worker selection memcheck
passes with repeated scratch reuse. Initial eight-head parallel runs correctly
reject excessive retained scratch; scratch right-sizing and eviction are being
validated before the next live-model measurement. This does not close the
production latency or public-interface gates.

The bounded native four-worker fixture run now passes in 8.832056s, including
worker initialization and retirement: six validated heads, two exact preserved
heads, 12,032 cooperative launches, zero restarts and 7,776 scratch reuses.
Peak charged device allocations are 1,678,934,208 bytes. Idle-slot eviction and
right-sizing QR capacity avoid the earlier workspace rejection without raising
the per-worker 512 MiB limit. See `native-parallel-bounded-four.json`.

The full 8K Debug model run `native-parallel-model-8k-debug.json` passes query
agreement, original quality gates and installed-cache continuation. It reports
252 attention calls, 144 donor appends, one rollback and `invalid=0`, with final
logical length 8194 and the same `violet-729` answer. Debug fitting is 15.116618s
versus the previous 41.317511s; total Debug compaction is 64.000207s and does not
qualify optimized latency. Peak sampled host RSS is 5,707,915,264 bytes and GPU
memory 10,729,029,632 bytes. The preserved binary
`native-parallel-model-debug-server` has SHA256
`68060ffdcb98c5da0c478af97e2f4b8e69ec4877ce5e1b2ac7ceea847cbe2b9c`.
The encoded checkpoint digest changes to
`0d8eb7fcc0df0bc8dee95990dcbd4389ba8dfd4d18faccd097b12563482affff`.
Although all reported whole-head metrics match, array-level serial/cooperative
comparison remains required to explain that digest difference.

The eight-head array comparison now resolves that digest difference:
`native-parallel-array-comparison.json` shows bit-for-bit identical parallel and
serial cooperative K/V, bias, positions and coverage. Against the previous
reference selector, only 16 signed-zero value representations and one bias value
(difference 1.4901161193847656e-8) change. All unchanged whole-head quality gates
pass. This explains why encoded hashes differ without showing a parallel-state
corruption; it is not a replacement for broader downstream quality tests.

The optimized full 8K run `native-parallel-model-8k-release.json` passes visible
continuation and installed-cache checks with `invalid=0`, one exact-tail rollback,
252 attention calls and 144 donor appends. Total compaction is **15.225862s**:
query collection 0.953911s, snapshot 0.514582s, fitting 11.145479s, export 2.134505s,
upload 0.300730s and installation 0.176624s. This is one correctness run, not a
percentile or release-latency qualification, and exceeds the 10s target. Six heads
pass the unchanged quality gates and two remain exact, giving 1.686994x actual
global savings. The encoded digest matches the Debug parallel run. The saved
ReleaseFast binary SHA256 is
`f21c6525752a83b20f9d4cf2e087f38866ed4c7a89229d8c798b310f5ef4a5ab`.

Four-column launch batching passes full-fitter and partial-batch memcheck and
racecheck in an isolated prototype. It reduces selector launches from 12,032 to
3,008 but has not demonstrated lower fitting latency; it is not integrated.
The 512-row chunk experiment also remains unintegrated because its 1.248x savings
fail the 1.5x gate. Recompaction work now preserves prior fitted history exactly
and fits only newly eligible exact rows, with 43 passing engine/checkpoint tests.
CUDA replacement, authoritative readback and independent fitted-shadow adapters
are undergoing qualification; real-model repeated cycles remain required.

CUDA replacement/shadow component qualification now passes on f16 and f32:
40 initial installations and 20 successive replacements per precision, independent
shadow copies, authoritative readback, post-fence cancellation/invalidation,
stale readback and partial clone-allocation failure. Tracked device memory returns
to baseline. The final binary passes memcheck with zero errors/leaks and racecheck
with zero hazards; see `recompaction-cuda-ownership-safety.json`. These component
cycles do not replace the required twenty successive real-model compactions.

The integrated fitting candidate now keeps separate bounded scratch slots for
attention, selection and value fitting, retaining the large QR workspace across
callbacks. Four workers and the 512 MiB per-worker cap remain unchanged. The
scheduler starts higher-cost heads first using their row/query work estimate;
its task array is included in host admission. Prototype cached-stage memcheck and
racecheck pass, and reordered arrays match serial cooperative results exactly.
The 6.67s cost-ordered fixture profile used a 768 MiB experimental cap and is not
an integrated full-model timing. Eight-worker experiments showed no useful gain
and remain unintegrated. Main inference PTX/fatbin/sm89 artifacts were regenerated
with nvcc 13.2.86; the next model run must qualify those artifacts as well.

`--fit-host-mib` explicitly bounds benchmark aggregate fitting host workspace
(default 2048, maximum 4096 MiB). The server reports the effective limit and the
harness verifies it. This does not change the separate 8 GiB sampled process-RSS
release gate. Installed fitted shadows use the reference repeat route and report
ragged attention activity; dense exact-repeat options reject that source layout.

### Streaming export and banked fitting follow-up (2026-10-19)

The integrated stage-specific scratch cache and cost-ordered four-worker fitting
passed the 8K Debug real-model installation/continuation check, with the same v1
checkpoint SHA-256 as the prior validated model run. Canonical full-stage
memcheck and racecheck both passed with zero errors/hazards. This is one model
request, not repeated-cycle or latency qualification.

The checkpoint encoder now offers explicit wire version 2: the 152-byte header
is followed by the unchanged payload and a 32-byte integrity footer. The footer
hashes header plus payload. Copying the running hash before finalization lets
export compute the complete-file SHA-256 after appending the footer without
rereading the payload. Total file overhead remains 184 bytes. The decoder
accepts both v1 and v2; existing v1 encoding remains available. Sinks stay private
until successful completion. No authentication is implied by either checksum.
Independent encoding, full-file hashing, v1 round-trip equivalence, every
truncation, corruption, cancellation, failed writes and allocation failures are
covered. In a 197,616,292-byte synthetic ReleaseFast export benchmark (one warmup
and five measured samples), v1 export plus file digest had median 1.792s and v2
0.927s. These are component timings, not full-model latency. The private model
probe now uses v2 and reports its wire version. Public import/export and
loader-derived identity remain release blockers.


### Full-model V2 measurements and follow-up (2026-10-19)

The optimized banked-cache/V2 server passed the protected-prefix fact fixture at
8K and 32K, and an additional 8K fixture whose fact is in the compacted middle
of the prompt. Each is one baseline/probe pair, with bitwise reference-query
verification and the same visible answer. These are correctness measurements,
not p95 qualification. The regenerated main CUDA artifacts are included.

| Fixture | Compaction total | Fitting | Export | Actual global savings |
| --- | ---: | ---: | ---: | ---: |
| 8K prefix fact | 11.925 s | 8.864 s | 1.088 s | 1.687× |
| 8K middle fact | 11.638 s | 8.575 s | 1.078 s | 1.687× |
| 32K prefix fact | 48.154 s | 40.822 s | 2.447 s | 2.557× |

Evidence: `banked-v2-model-8k-release.json`,
`banked-v2-model-middle-8k-release-rerun.json`, and
`banked-v2-model-32k-release.json` under the qualification directory. The first
middle-fixture runner attempt failed before model startup because its isolated
wrapper path was missing; its failed artifact remains available. Binary SHA-256:
`db0ec4cc9dc08777a6804f6ced040850cc100af50ab96a43184abfe450965912`.

Both latency targets remain unmet. At 32K, sampled host RSS peaked at
8,847,634,432 bytes (8.24 GiB), exceeding the 8 GiB release gate; GPU memory peaked
at 13,337,886,720 bytes and passed. Functional `passed` in these probe reports
must not be interpreted as passing the release memory or latency gates.
The host peak coincides with whole-head FP64 validation near the end of fitting.
Ordinary 32K prompt ingestion took about 312 seconds and remains a separate
end-to-end serving bottleneck.

The next integrated candidate uses 256-query held-out batches in the CUDA fitter.
The default/reference backend contract remains bounded at 64. A 257-query CUDA
oracle, sanitizer checks and 515-query CPU tail/allocation checks pass; saved
8K numerical fixtures retain identical cooperative fitted arrays. The candidate
also adds an opt-in successive-cycle diagnostic with independent original-exact
and fitted cache owners, actual V2 encode/decode at each replacement, identical
appended tokens, and exact-reference logit/greedy comparisons. These new model
paths still require a server build and real-model qualification. No public
portable checkpoint interface or production-ready claim is made.

Whole-head validation now admits at most two simultaneous FP64 conversions in
parallel fitting, while keeping four fit workers. Cancellation before or after
acquiring a permit preserves the gate, and serial callers need no gate. The
47-test engine/codec suite passes. Saved suffix-query fixtures match serial
cooperative arrays bitwise and retain the prior host-reference rounding
comparison; see `validation-gate-component.json`. The isolated fit took 5.161s
without another model/compiler running, but a full-model memory measurement is
still required before claiming the 32K RSS regression fixed.

The integrated Debug server now passes two successive real-model cycles from
8,189 initial tokens through 9,221 tokens. Both V2 checkpoints encode, decode,
and replace the same private cache owner. All eight compared greedy choices
match the independent original-exact branch. The second cycle collects 84 ragged
attention calls from fitted state and saves an additional 1.110× global storage;
the original source remains unchanged. Final ordinary installed continuation
also passes. Evidence: `validation256-two-cycles-8k-debug.json`. Debug timings
include redundant binary round-trip checks and one brief debugger attachment;
they are not latency evidence. Twenty cycles, task diversity and public portable
resume remain open gates.

Output-only CUDA validation is now integrated: training still receives complete
probabilities, while held-out validation downloads only attention outputs and
log-mass totals. The FP64 matrix operations and reductions are unchanged. A
257-query oracle compares both routes bitwise; memcheck reports zero errors or
leaks and racecheck reports zero hazards. The 49-test engine/codec suite passes,
and the eight-head suffix fixtures preserve fitted arrays. See
`validation-summary-component-safety.json`; full-model performance is pending.

The private exact-prefill probe also passes one 8K Debug model run with bitwise
query agreement against a separately replayed ordinary prefix, unchanged fitted
checkpoint digest, and baseline continuation parity. `exact-prefill-8k-debug.json`
records 1,008 split launches. Approximate log intervals are 23s candidate versus
41s ordinary ingestion, not optimized release measurements. This path remains
explicitly opt-in and is not the serving default.

Portable identity primitives and a loader bridge are integrated for compilation
and model qualification. Before tokenizer/weight loading the loader captures a
file-generation marker; content hashes are computed only against that unchanged
generation and cached only for that loaded model. Identity binds the full GGUF
bundle, actual tokenizer route/compatibility behavior, effective chat template,
KV precision and effective model layout. Writable digest memo files are not
trusted. Unsupported identity bundles retain ordinary inference, while native
checkpoint operations fail closed. Five identity tests cover copied artifacts,
mutation, cancellation, limits and tokenizer contract changes. Diagnostic probes
now require this identity; the server reports cold hash time separately. This
bridge still needs a real-model run, and does not expose public checkpoint resume.


### Loader identity, repeated cycles and fresh restore (2026-10-19)

The optimized `summary-identity-release-server` has SHA-256
`530a3321594ffd898a0e496ccbf84b6abdcc19a6a81c4a64337144c54c5d46d5`.
It binds real loaded-artifact identities and includes output-only held-out
validation with 256-row batches and at most two whole-head validations.

| Evidence | Result | Qualification limit |
| --- | --- | --- |
| `summary-identity-8k-release.json` | 9.9880s and 9.5790s total compaction; 1.687x global savings; correct continuation | Two samples, not p95 qualification |
| `summary-identity-32k-release.json` | 43.4659s total, 35.6973s fitting; 2.5567x global savings; correct continuation and bitwise repeat queries | Fails 30s target and 8 GiB RSS cap (8.2483 GiB peak) |
| `summary-identity-20-cycles-release.json` | 20 actual V2 encode/decode/replacement cycles on the same owner; 250/250 greedy comparisons; 8,189 to 27,888 logical tokens | One fixture; not broad quality or sustained reliability |
| `summary-identity-tool-call-release.json` | Baseline tool call correct; fit rejected its 1.5x requested savings | Required failure retained, not reclassified as a pass |
| `summary-identity-tool-call-retention-release.json` | Correct structured tool call after compaction; three heads accepted, five preserved; 1.2501x global savings; 10.9960s total | Diagnostic minimum of 1.000001x; does not pass the earlier 1.5x gate |

The 20-cycle run peaks at 5,671,825,408 host-RSS bytes and 15,076,425,728 GPU
bytes. Its cycle timings include extra binary round trips and cannot be compared
directly with the ordinary compaction timer. Cold real-identity hashing takes
about 35.9s on the optimized server; a cached lookup takes about 0.2ms.

`fresh-owner-restore-8k-debug.json` qualifies a private fresh cache owner against
an independent clone of the fitted cache, using actual V2 bytes and the trusted
loaded-model identity. All logits match bitwise through six greedy comparisons
and five actual appends, with zero replayed prefix tokens and an unchanged source.
The ordinary answer also remains correct. The checkpoint digest remains
`5b09989a64944c79c14990b9d7bea167dc45509323527452ce038825b7a63c8a`.
Peak RSS is 5,736,984,576 bytes and GPU memory is 11,586,764,800 bytes.
The Debug binary SHA-256 is
`785033172f8fda5d93672153e21e42536cf9bab0f02f81f8ad793d894c5a8d46`.
This is correctness evidence, not latency or cross-process/public-API evidence;
Debug identity hashing alone takes 559.6s.

The integrated restore code validates exact loaded-model geometry and token
prefixes before upload, and checks the fresh owner again after the final device
fence. Seven CUDA integration tests pass, including large f16/f32 snapshot reads,
allocation failure, cancellation, stale state and successful ownership transfer.
Memcheck reports zero errors and zero leaked allocations. Snapshot reads now use
64K-element batches with in-place half expansion. The host source snapshot is
retired after replacement validation, before export/upload. These changes need
optimized full-model latency and 32K memory measurements.

The codec now also provides bounded streaming import, consuming one declared
file directly into owned arrays with 16 KiB reads. It shares payload decoding
with buffered import, supports V1/V2 integrity checks and returns state only
after checksum and semantic validation. Tests cover fragmented reads, every
truncated prefix, injected I/O/allocation failures, cancellation and partial large
arrays. HTTP streaming wiring and transport admission remain unfinished.

Three measured kernel experiments were rejected: a four-column kernel loop,
skipping used/zero-weight work, and reducing cooperative grids to 32/16 blocks.
They preserved the tested numerical contracts but ran slower in isolated
comparisons. The canonical fitter retains the original 64-block selection path.
A separately queued-launch experiment remains unqualified.
