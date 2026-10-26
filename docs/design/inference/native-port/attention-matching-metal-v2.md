# Native attention matching on Metal, version 2

This implementation carries the numerical approach from
`feat/context-management` (`b14eea52c7783e8b19c021f6aec646595f1f1a8b`)
into the Metal production worktree. CUDA is the working implementation reference;
no CUDA result files or remote workloads were inspected. The original Metal
worktree was backed up before changes. Nothing is committed or pushed.

**Production qualification remains open.** The release registry is empty. Passing
source tests, a synthetic whole-head fixture, or an internal development capture
does not authorize approximate serving.

## Implementation

`runtime/kv/attention_match.zig` contains the portable FP64 solver derived from
the CUDA branch. Both its source-based oracle and its new matrix interface use
the same residual OMP, active-set NNLS, direct residual/KKT checks, and ridge
Householder QR. Values are fitted with the CUDA reference's zero-centered ridge
objective. The implementation does not form normal equations or learn new keys.

The Metal adapter, `backends/context_attention_match.{zig,metal}`, computes
normalized probabilities and exact outputs from immutable f16 device KV and
real f32 post-RoPE queries. Only these bounded derived matrices are read by the
FP64 host solver. Selected original keys are gathered on the GPU. Fitted values
are rounded to f16 and log-mass bias to f32 before acceptance. The production
adapter does not read source KV back or snapshot the cache onto the host.

The FP32 Metal frontend is an explicit precision boundary. Qualification compares
the stored whole-head representation against both the Metal implementation and
the portable FP64 oracle. The numerical CUDA reference remains FP64 throughout;
GPU source-probability parity must be measured on actual Gemma inputs.

| Contract | Existing block v1 | Native whole-head v2 |
|---|---|---|
| Selection | RMS / periodic-refit matching pursuit | Per-step residual OMP |
| Mass | FP32 coordinate descent | FP64 active-set NNLS, direct KKT verification |
| Values | FP32 Gram/Cholesky, seed-anchored ridge | FP64 augmented Householder QR, zero-centered ridge |
| Acceptance unit | Independent 512-row block | Assembled KV head, including exact prefix/recent rows |
| Serving representation | f16 K/V + f32 bias | f16 K/V + f32 bias |
| Mass limit | 0.05 | 0.05 |
| Pooled output RRMSE limit | 0.10 | 0.10 |
| Worst physical-consumer output RRMSE limit | 0.10 | 0.10 |

Whole-head acceptance is a new versioned contract. It never changes the result
of a failed v1 experiment. A weak consumer cannot hide behind another consumer's
larger output energy. Softcap precedes the mass-bias correction.

`Maintenance.publishNativeCampaignV2` retains the established Metal transaction:

1. Validate query campaign generation, complete consumer coverage, payload
   eligibility, retained rows, and joint staging admission.
2. Keep all original global owners and local windows alive; stage private owner
   generations and copy protected rows exactly.
3. Fit each eligible block with the native solver, then validate every assembled
   rounded head against all held consumer strata.
4. Check cancellation and commit all owners in one `Batch.commit`.

Failures retire staging and keep the original owners installed. Failed attempts
consume publication epochs; an epoch is never reused. Cancellation during fit
or immediately before commit, nonfinite queries, allocation failures, and a
simulated GPU completion error have separate tests. Submitted work is drained
before buffers are released.

Serving selection carries `Profile.engine` and the corresponding implementation
identity through single requests and serial mixed batches. Native v2 uses
algorithm version 2, explicit rounded-whole-head acceptance, and a distinct solver
tag. Profile domain v3 and implementation domain v4 bind this difference.
Zig/Python profile digest vectors agree. Host FP64 scratch and Metal staging are
admitted separately. The model execution pool's added native pipeline/runtime
uses reservation version 2, 80 MiB, disclosed separately from request savings.

Telemetry distinguishes derived P/Y readback from source-KV readback and
validation-status readback. The current implementation is synchronous and
serial within an owner transaction; fitter throughput is unqualified.

## Complete-head diagnostics

Existing calibration captures contain only eligible 512-row blocks. They omit
the protected prefix and recent tail, so they cannot establish whole-head error.

The opt-in installed-model capture supports:

```
ANTFLY_CONTEXT_CAPTURE_NATIVE_FULL_HEADS=1
ANTFLY_CONTEXT_CAPTURE_NATIVE_PREFILL_ONLY=1
```

The second switch exports original complete heads after exact packed prefill
without running a new query study. It requires the first switch. Artifacts use
a separate `-native-head-` stem and `antfly.context.native.full-head-source.v2`
index. They bind the model, prompt, build, binary, owner/consumer geometry, and
all f16 source bytes. Export is test-only and has no publication authority.

`scripts/gemma4/native_attention_matching.py` seals an `AMHEAD02` input only
after checking the full release build, explicit training-authority membership,
complete consumer geometry, disjoint training/validation source IDs, and a
byte-exact FP16 KV witness linking the query capture to the complete head.
It hashes the same regular-file bytes it consumes and rejects final symlinks.
It copies held bytes without using their numerical values for selection.
The input manifest records the preparer's SHA-256 separately from the source
build that produced the captured tensors. Schema v2 calibration witnesses use
little-endian FP16; FP32 witness byte counts are rejected before query loading.

The default protected recent window is at least 2048 rows. Eligible blocks
intersecting that window remain exact; the existing protected prefix also stays
exact. Retention attempts are fixed in advance at 4:1, 3:1, and 2:1, all against
the same limits and continuation-inclusive retained-row cap. The input is
explicitly labeled internal development, never fresh unseen qualification.

`context_native_head_qualify.zig` exercises this input with:

```
ANTFLY_CONTEXT_NATIVE_HEAD_INPUT=/absolute/path/input.bin
ANTFLY_CONTEXT_NATIVE_HEAD_SHA256=<sealed-input-sha256>
ANTFLY_CONTEXT_NATIVE_HEAD_OUTPUT=/absolute/path/new-output-prefix
```

Run the compiled test named `context captured native whole-head Gemma
qualification`. It writes a JSON receipt and, on acceptance, the complete f16
K/V + f32 bias candidate. It compares Metal with FP64 pooled and worst-consumer
metrics to absolute tolerance 2e-5. If every admitted attempt fails, it reports
preservation of the original and fails qualification. Outputs are created
exclusively. One-head receipts always retain `serving_qualified=false` and
`full_context_memory_qualified=false`.

Production requests currently use the artifact's frozen retention decision.
The bounded adaptive retry driver above is a qualification tool. Enabling
adaptive retries in serving still requires a hash-bound profile policy,
admission for its largest attempt and plan metadata, and explicit unchanged
outcomes when no admissible candidate passes.

## Local verification

From `zig/`, using worktree-owned caches:

```
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build inference-test-context-native \
  -j1 -Dmetal=true -Dcuda=false -Doptimize=ReleaseSafe -Dstrip=true
```

Run this inside the existing resource guard on constrained hosts. The focused
suite covers the native CPU numerical core, Metal parity after storage rounding,
consumer gates, source immutability, admission, identity separation, all-owner
rollback/publication/continuation, and request-scoped faults.

The sealed-input pipeline has also been exercised on a synthetic complete head.
That fixture is a harness check and supplies no Gemma numerical evidence.

On 2026-10-22 the source-bound full release build selected 607 tests: 562 passed,
45 opt-in checks skipped, and no failures or leaks. Build process-group RSS
peaked at 4316.4 MiB. Swap grew by 16.4375 MiB, within the unchanged 512 MiB
build guard; this is build evidence and supplies no clean-swap performance
claim. The receipt is at
`/Volumes/Extreme/antfly/qualification/context-native-v2-full-release-build-20261022-v1/full-release-build.json`
(SHA-256 `0bebf60b73368ee6745cb7ae6adcdfbb3231cceeb86db4f502c89bc1c8c0a320`).

The same executable completed an exact-prefill-only E2B capture of all three
global owners (layers 4, 9, 14) at 8160 prompt tokens. Each source has one KV
head of dimension 512. The run took 102.12 seconds, peaked at 4181.61 MiB
process-group RSS, grew swap by zero bytes, and drained before exit. Its index
is at
`/Volumes/Extreme/antfly/qualification/context-native-v2-full-head-source-coverage-20261022-v4/native-source-native-head-index.json`
(SHA-256 `a2c8d8b665d701fca2c826f6eb680d97d388ea1593b78677f23961d6e69cfca7`).
The layer-4 block-2048 source K/V bytes exactly match the existing authorized
training capture. The preparer's FP16 format correction and rejection checks
passed nine focused Python tests after this build; the correction does not
change the compiled Metal executable or its sealed source archive.

All three complete heads were then evaluated against the existing separate
gate-study population. Each of the fixed 4:1, 3:1, and 2:1 attempts was rejected.
At 2:1 with 2528 exact recent rows, the independent FP64 results were:

| Global owner | Physical consumer strata | Worst mass error | Pooled output RRMSE | Worst-consumer output RRMSE |
|---|---:|---:|---:|---:|
| 4 | 8 | 20.81% | 6.58% | 12.59% |
| 9 | 8 | 51.29% | 9.95% | 18.15% |
| 14 | 40 | 22.79% | 6.72% | 17.52% |

Metal/FP64 differences met the fixed absolute 2e-5 parity limit. All runs grew
swap by zero bytes and preserved the original. The receipts are in
`/Volumes/Extreme/antfly/qualification/context-native-v2-real-head-20261022-v2/`.
These are rejected internal development fits, not qualified profiles.

A predeclared six-cell development sweep increased the minimum protected
recent window to 3072 and 4096 rows for each owner. None passed the unchanged
limits. Inadmissible attempts were explicitly skipped under the existing 6144
row cap including 32 continuation rows. The closed plan and results are at
`/Volumes/Extreme/antfly/qualification/context-native-v2-protected-tail-study-20261022-v1/`.
For example, protecting more rows reduced owner 4's worst-consumer RRMSE to
5.76% at 2:1, while mass error remained 19.07%. This motivates work on query
coverage; it does not justify relaxing a mass or consumer gate.

## Representative replay development protocol

The CUDA implementation selects a bounded study from history: infrequent-token
64-row windows supplement a long exact recent suffix. It partitions sampled
positions within that replay. The older Metal campaign instead withholds a
complete, distinct gate-study branch. These are different calibration and
validation protocols; their results are never interchangeable.

`runtime/kv/attention_match_study.zig` carries the CUDA history-only selector
verbatim, including allocation-failure tests. It consumes no query values or
validation results. The opt-in installed capture now also accepts:

```
ANTFLY_CONTEXT_CAPTURE_NATIVE_FULL_HEADS=1
ANTFLY_CONTEXT_CAPTURE_NATIVE_REPEAT=1
```

This is mutually exclusive with `ANTFLY_CONTEXT_CAPTURE_NATIVE_PREFILL_ONLY`.
It runs a 512-token representative replay in an owned private branch, captures
every physical consumer, seals and drains the branch, and verifies the original
descriptor, KV payload, publication epoch and logical position are unchanged.
Its distinct `-native-repeat-index.json` uses schema
`antfly.context.native.repeat-source.v1`. Training and validation positions use
the existing disjoint hashed-quartet sampler, uniformly across the complete
replay. It never opens or reassigns the older gate-study queries.

`scripts/gemma4/native_repeat_attention_matching.py` requires the full build,
successful clean-swap capture command and completion evidence, original
training-document authority, exact FP16 source witness, full consumer coverage,
and both position partitions before opening the new query tensors. It verifies
the study is an ordered source-history subsequence and preserves the exact
384-token recent suffix. It binds preparer hashes separately and retains
`fresh_unseen_qualified=false`, `serving_qualified=false`, and
`full_context_memory_qualified=false`. A passing result here would establish an
internal recipe; independent unseen quality, physical savings and serving
qualification remain required.

The new selector and exporter passed 45 focused native tests with zero swap
growth. The existing and new input-contract checks passed 15 Python tests,
including rejection of overlapping positions, missing consumers, non-history
tokens, failed captures, and attempts to substitute the old capture protocol.

The subsequent source-bound full release build on 2026-10-23 selected 610
tests: 565 passed, 45 opt-in checks skipped, no failures or leaks. It completed
in 481.44 seconds, peaked at 5064.30 MiB process-group RSS, and grew swap by
zero bytes. Source verification after the build passed. Its receipt is at
`/Volumes/Extreme/antfly/qualification/context-native-v2-repeat-full-release-build-20261023-v1/full-release-build.json`
(SHA-256 `89d48a48b92225e3d0f2dfc86989478a99312fcf6d23ca8d3d08a9cd6e7e2509`).
The executable SHA-256 is
`b07b40e3a1385bafaf13ff4a2e322dc7d5dd7625c88c8a8325d5d2910d58aa1d`.
This is full build evidence; the representative replay capture and numerical
gates must still run against that executable.

## E4B continuation, 2026-10-23

Recovery found that the native repeat path still rejected multiple KV heads.
That excluded the installed E4B geometry: global owners 5, 11, 17 and 23, each
with two KV heads of dimension 512. The portable CUDA numerical core was already
present; this continuation repairs the capture and qualification boundary.

The repeat exporter now emits `antfly.context.native.repeat-source.v2`, with one
entry per `(owner_layer, kv_head)`. Each entry preserves the physical GQA query
head labels and the shared consumers' head-local row order. Training queries,
validation queries and position arrays have distinct per-head files. The full
source stays in its original `[logical row][KV head][head dim]` f16 order.
The preparer verifies that complete interleaved witness before extracting one
physical head into the unchanged `AMHEAD02` format. Legacy one-head inputs remain
supported. The repeat index digest is emitted by the actual capture process and
checked against its canonical filename and resource logs before query use.

`native_repeat_training_authority.py` adds a separate fresh-data workflow:

1. Freeze a calibration-only plan against completed coverage, the full build,
   model/document/prompt identities, eligible blocks, all owner/head coordinates,
   the history selector and the disjoint position protocol.
2. Bind that plan digest in the native capture command and repeat index.
3. Seal the complete source/repeat/resource graph after a clean capture, then
   revalidate it before preparing a selected head.

This path does not require, open or relabel the old five-source validation query
tensor. Validation f32 bytes are hashed and copied; their numeric values are not
interpreted or used for selection. The same-replay partition remains internal
development evidence. Neither the plan nor its authority grants fresh unseen,
publication, serving or release authority.

The opt-in test `context Metal installed Gemma compaction identity bootstrap`
records actual loaded model identities and global-owner geometry with zero model
forward steps. This avoids guessing E4B tokenizer, template or layout hashes
when preparing its coverage request.

Verification completed on the current source:

- Native Metal suite: 47 selected and passed, zero skips or leaks; 33.98 seconds,
  539.34 MiB peak combined RSS, zero swap growth and complete process drain.
- Python preparer/authority suite: 35 passed, including distinct two-head byte
  extraction, GQA ordering, stale/missing heads, plan/resource/index binding,
  failure precedence and opaque held-byte handling.
- Full stripped ReleaseFast context build: 614 selected, 568 passed, 46 opt-in
  skips, zero failures or leaks; 466.24 seconds, 4984.52 MiB peak process-group
  RSS and zero swap growth. Both Zig caches stayed in the worktree.
- Independent source auditing matches all seven resumed Zig/Python files to the
  sealed build archive. Existing uncommitted work was preserved.

The durable local evidence root is
`.qualification/gemma4-e4b-resume-20261023-v1/`. Its full build receipt is
`build/full-release-build.json`, SHA-256
`5ff4b6ff625edd0e0094fce1c9a1f286a82b88c43c399496ae91ba4463f8d3e8`;
the executable SHA-256 is
`655aed2caa8ae9ab0224a69fa6630094e56942d5f3f46579f86b49ca11a6e301`.
`source-audit.json` and `resume-delta.patch` distinguish this continuation from
the recovered dirty worktree.

The real-model launch prechecks reported 60%, 63% and 61% free memory,
below the preserved 64% requirement. All stopped before loading a model. No
current E4B numerical candidate or memory-savings result is established by
these code tests. When headroom permits, run these phases serially from the
worktree root; outputs are exclusive and failures remain preserved:

```sh
python3 .qualification/gemma4-e4b-resume-20261023-v1/run_e4b.py bootstrap
python3 .qualification/gemma4-e4b-resume-20261023-v1/run_e4b.py coverage
python3 .qualification/gemma4-e4b-resume-20261023-v1/run_e4b.py capture
python3 .qualification/gemma4-e4b-resume-20261023-v1/run_e4b.py fit
```

For a fresh full release build, set `XDG_CACHE_HOME="$PWD/zig/.zig-cache"`
when invoking `record_context_release_build.py` from the worktree root. Its
sanitized environment clears `ZIG_*` variables; the inherited XDG setting keeps
the global cache in the worktree as well as the normal local cache.

The fixed fit schedule remains 4:1, 3:1 and 2:1 at mass/output/worst-consumer
limits 0.05/0.10/0.10. A completed eight-head diagnostic still requires separate
unseen evaluation and validation through the actual packed serving kernels.
The legacy stored-artifact qualification bridge's fixed 36-coordinate contract
was not expanded; native v2's whole-head path is separate, and its block count
must be derived from the fresh eligible-block set. The release registry remains
empty.

## Recovery of the interrupted production campaign, 2026-10-23

The earlier `run_e4b.py` handoff above is historical. Its temporary resource
helper no longer exists. The repository-backed continuation uses
`native_repeat_campaign.py` for identity, coverage, plan and capture, and
`native_repeat_fit_resume.py` for explicit immutable physical-head attempts.
The last completed v2 build selected 617 tests: 571 passed and 46 opt-in checks
skipped. The v3 build was intentionally stopped before later collector edits;
it has no qualifying build receipt.

Recovery checked the native profile projection together with the paired
performance and loaded-provenance collectors. A repeat campaign calibrates the
retention schedule. The candidate qualification driver subsequently refits
live request KV using its separate compiled five-source query protocol; it
does not serve the repeat campaign's candidate bytes. Repeat acceptance alone
cannot establish runtime numerical coverage or fresh unseen quality.

The bootstrap validator now rejects zero artifact, tokenizer, template, layout
and implementation digests before a profile can be frozen. All 168 focused
Python checks passed under the unchanged zero-new-swap guard, including the
collector integration, immutable fit resume and new identity regressions.
The tests peaked at 172.22 MiB process-group RSS and drained completely.

The v4 full build stopped before tests after system swapouts grew 576.3125 MiB,
exceeding the preserved 512 MiB build allowance. Its compiler group drained.
A subsequent read-only host snapshot recorded 20 concurrent Go processes
using 5355.94 MiB RSS outside that build. The failed attempt remains in
`.qualification/gemma4-e4b-production-20261023-v4/`; it supplies no build or
model evidence. After the competing compilers drained, two new headroom
samples passed at 69% and 70% free memory.

The fresh continuation root is
`.qualification/gemma4-e4b-production-20261023-v5/`. Its local `run_v5.py`
uses repository-owned resource helpers, preserves the original calibration
input bytes, and refuses to freeze a campaign spec before validating a complete
source-bound full build. Model phases run serially; each physical-head fit uses
an explicit owner, KV head and six-digit attempt ID. Failed attempts are never
overwritten or automatically selected. `resume-delta.patch` isolates this
recovery's bootstrap hardening from the inherited dirty worktree.

The v5 stripped ReleaseFast Metal build completed in 440.18 seconds: 625
selected, 578 passed, 47 opt-in skips, no failures or leaks. Peak process-group
RSS was 4201.00 MiB, with zero new swapouts and complete process drain. The
independent audit matched all 7365 Git-visible Zig input files and their archived
bytes to the sealed build. The receipt is `build/full-release-build.json`
(SHA-256 `402b6fbf26251bec3660e95a2cc2b40b746f5cf5f2161b2a8c132179ed8c46a3`),
and the executable SHA-256 is
`fe0f5e2963de92fada350b70cdc44f3b3879fdf35bf59eff8c5dc3fcb531443f`.
This verifies the current source and test executable; real-model capture and
numerical acceptance remain separate evidence.

## Main integration and portable f16/FP8 checkpoints, 2026-10-25

This worktree incorporates `origin/main` at
`9d152a4c78311bcbcf1438d9ae1ac893ea1cde42`. Main now supplies the shared
attention-matching engine, portable checkpoint codecs, exact-prefix resume
validation, and native checkpoint HTTP contracts. Its implementation supersedes
the older `feat/context-management` checkout as the CUDA reference. The original
45 modified and 257 untracked files were archived and hash-verified before the
fast-forward. Git stash `15bbd736d13e14a5f50cb2d30be3266c8b2e4592` remains
available, with the file archive and hash manifest in
`.qualification/merge-main-20261025-v1/`.

The Metal adapter implements the common f16/FP8 checkpoint path in addition to
the request-local v2 path documented above:

| Component | Metal implementation |
|---|---|
| Numerical fitting | GPU FP32 probabilities, outputs and log normalizers; shared FP64 residual OMP, active NNLS and ridge QR |
| Acceptance | Common engine's 5% mass and 5% output limits after serving-precision rounding; FP8 has its additional decoded-value validation |
| Retry/fallback | Immutable-source 4:1, 3:1, 2:1 retries and exact preservation on numerical rejection; cancellation/OOM/corruption propagate |
| Storage | GPU-owned exact f16 rows and resident E4M3FN FP8 history, with fitted bias and absolute position/coverage metadata |
| Attention | Metal GQA with causal coverage, local windows, and mass bias after the model softcap |
| Continuation | Exact f16 appends, local rings, owner-scoped cloning, download, rollback and teardown |
| Checkpoints | Common V3 f16/FP8 codec and identity/prefix/layout limits; rotated4 is unsupported on this Metal adapter |

`ops/metal_attention_match_fit.{zig,metal}` supplies the common solver backend.
`ops/metal_attention_match_storage.{zig,metal}` owns the portable device state.
MetalCompute/KvStorage bindings validate source revisions, fence device work,
and transfer a complete candidate atomically. Clone admission validates the
common page-map/geometry contract and reserves future capacity, growth overlap
and attention scratch before allocating a destination owner.
Ordinary exact prefixes clone through two GPU buffer blits per donor into
independently leased, budgeted slots. Publication waits for all donors;
cancellation, allocation failure and device errors roll back staging. The
pipeline drains provider frames and pending KV seeds before clone admission,
and checkpoint preparation uses full-history source storage rather than a
local donor ring. Exact snapshot downloads report their raw f16 transfer bytes.
Fitter shared buffers are charged to the source request's allocation tracker
and bounded by its admitted fit plan. Fitting and attention fence at most 16
query rows per dispatch before checking cancellation. FP8 validation polls
between bounded value chunks; readback validates metadata and buffer geometry
before host allocation.

Query collection disables the pre-projection fused decoder block so every
global consumer reaches post-RoPE capture. A declined residual fusion suppresses
duplicate capture in the fallback. Metal uses the ordinary `.reference` repeat
route, including for already compacted owners; `.exact` and `.exact_split`
remain explicitly CUDA-only.

The portable path's global savings gate uses Metal's storage costs. Ordinary
exact sources use a conservative f16 payload lower bound; installed and
candidate states charge the actual page-rounded shared-buffer representation,
including FP8 buffers and metadata. The source denominator omits geometric
spare capacity. The Mac allocation probe confirmed that `MTLBuffer.allocatedSize`
can equal a four-byte requested length despite a 16 KiB host page, so rounding
ordinary slots as if they used the adapter's shared buffers would inflate
savings. A failed physical savings gate returns the unchanged original.

This portable path uses the common CUDA query/acceptance contract. The earlier
v2 campaign still has its separately frozen five-source query protocol and
10% pooled/worst-consumer output limits. Its existing profile and release gates
remain intact. Portable export performs explicit bounded device KV readback;
the request-local v2 fitter retains its derived-matrix interface.

Metal native checkpoint/resume remains available through qualification ingress
in test binaries. `GenerationConfig.metal_native_checkpoint_qualification` is
a boolean only in test builds and cannot enable Metal production serving.
The production release registry remains empty. CUDA availability is unchanged.
Verification logs for the merged source are retained in
`.qualification/merge-main-20261025-v1/`; earlier v5 build receipts are historical
and do not qualify this merged implementation.

The final merged-source validation passed:

- Source-bound Metal ReleaseFast suite: 782 selected, 734 passed, 48 opt-in
  skips, no failures or leaks; 431.82 seconds, 4,748.2 MiB peak process-group
  RSS, zero new swap. The receipt binds the executable and 7,875 Zig inputs.
- Focused Metal ReleaseSafe regressions: 12 passed, including resident FP8,
  common-engine/checkpoint roundtrips, ordinary exact clone rollback, and
  provider-frame retirement. The separate five-format selected-key scorer
  oracle passes before and after repacking.
- Backend-disabled ReleaseSafe core: 123 passed, 10 GPU-specific skips.
- Standalone production Metal semantic check, 200 Python qualification/build
  tests, Go SDK tests, TypeScript SDK typecheck, OpenAPI consistency, and Rust
  bundled-spec consistency passed.

The final receipt is
`.qualification/merge-main-20261025-v1/build-final/full-release-build.json`.
`final-artifact-audit.json` independently verifies the recorded file hashes and
current source closure. These results cover implementation and synthetic GPU
fixtures; no E4B model quality, performance, or serving campaign ran on this
merged source. The remaining release gates below still apply.

## Remaining release gates

- Capture a complete Gemma4 head and obtain a passing native recipe within the
  retained-row and staging budgets. Freeze its candidate before independent
  development or unseen evaluation; preserve the existing consumed/reserved
  prompt roles.
- Qualify all global owners and shared consumers, continuation, and repeated
  compaction. Integrate any serving retry policy and test cancellation/OOM at
  its boundaries without changing the frozen acceptance limits.
- Bind native v2 candidate, query, source, build, solver, storage precision, and
  acceptance identities in a dedicated release contract. The existing v1
  independent-block payload validator cannot authorize a v2 whole-head fit.
- Measure at least 25% total committed context physical savings after teardown,
  including local windows, metadata, protected rows, and reserved continuation.
  Row counts, global-KV estimates, and a one-head file are insufficient.
- Run matched exact/compacted Gemma E2B/E4B quality and performance campaigns at
  the required context sizes. Preserve the exact-path regression, TTFT, decode,
  quality, perplexity, clean-swap, and destination-hardware requirements.
- Exercise native/CLI/HTTP/stream/serial mixed-batch serving, reload, allocation
  faults and cancellation with the native implementation selected. Complete the
  four-hour persistent-owner soak per artifact before registry enrollment.

No gate is waived by the shared CUDA algorithm or by a successful unit fixture.
