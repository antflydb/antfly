# Gemma4 generative OCR optimization

Gemma4 OCR generates text from page images. Its decode costs and qualification
are separate from image/audio **embedding** throughput. This document describes
the current contracts and qualified local result. The dated experiments,
rejected approaches and source-pinned reports are summarized in
[OCR history](history/gemma4/ocr-throughput-2026-10.md); the
[Apple comparison](gemma4-ocr-comparison.md) defines the original corpus.

## Serving policy and output qualification

Ordinary Metal multimodal generation remains serialized. Experimental native
OCR cohorts preserve the **complete prepared singleton operation family** by
default, including QKV, attention/KV publication, PLE, normalization and head
arithmetic. Setting `ANTFLY_GEMMA4_METAL_OCR_MULTIROW=1` alone does not select
matrix arithmetic; that research lane additionally requires
`ANTFLY_GEMMA4_METAL_OCR_MATRIX_ARITHMETIC=1`. Scoped rollbacks, normalization
changes and a canonical final EOS check do not guarantee equivalent history.

The matrix lane intermittently omitted a paragraph by selecting EOS early.
A free-running trace at sequence length 1,011 reproduced newline 108 changing
to EOS 106, dropping 204 characters/41 tokens. The request finished normally.
Fixed-prefix diagnostics restored canonical KV and hid accumulated drift;
passing those diagnostics was insufficient to qualify free-running output.
The complete singleton family is the serving correction. No EOS suppression
or forced text extension substitutes for that correction.

Every throughput candidate must preserve page identity, complete transcripts,
completion counts and normal finishes, with no generation caps. Qualify repeated
fragile pages, the original eight pages, sixteen text pages, reversed order and
allocation instrumentation. Passing means agreement with ordinary output on
these fixtures; it does not certify OCR accuracy on arbitrary PDFs. The expanded
corpus excludes an independently failing diagram page, whose ordinary output
repeated text to the cap; retain that failure separately.

## Coordinated independent-page decode

`ANTFLY_GEMMA4_METAL_OCR_BATCH=1` enables the opt-in Metal GGUF lane;
`ANTFLY_GEMMA4_METAL_OCR_BATCH_ROWS` bounds GPU cohorts at four.
`ANTFLY_GEMMA4_METAL_OCR_CONTINUOUS=1` admits a resident request window:
`ANTFLY_GEMMA4_METAL_OCR_WINDOW` defaults to eight and is capped at sixteen.
These are distinct from HTTP envelope size and cross-request embedding brokers.

An outer model/provider lease covers the batch. A separate step mutex serializes
projector, prefill and coordinated decode work. Admission reserves **all live**
KV caches and scratch. Each page has its own position, RoPE and paged KV identity;
finished leases retire immediately and ready pages fill subsequent cohorts.
Only token IDs return during eligible greedy decode. Unsupported/nongreedy work
retains compatibility execution; an error after KV writes fails the step rather
than replaying mutated state as a fallback. Owned frames cancel on failure;
borrowed frames remain owned by their caller. Singleton tails retain the direct
optimized decoder.

## Resident generative image preparation

The GGUF projector returns an owned `ResidentProjectedImages` tensor. Backend
embedding scaling and row copies pack its visual rows into the prefill prompt,
using the existing marker validation and PLE padding. Mixed audio features can
be uploaded into the same prompt. Row concatenation, copying and scalar
multiplication are required; `ANTFLY_GEMMA4_GENERATION_IMAGE_HOST=1` restores
the host path. Decode/resize and tokenization remain CPU work. The HF embedding
path has its own resident handoff. Ownership and cancellation extend through
the last prompt consumer.

## Singleton-invariant Q4 cohort dispatch

`TERMITE_METAL_Q4_0_BATCH_INVARIANT=1` uses the singleton MMV pipeline and
reduction geometry for Q4_0 F32 projections with two through eight rows, while
dispatching the actual rows. Row-dependent portfolio/JIT routes are bypassed.
Fused gate/up keeps its singleton arithmetic. Attention and FFN tails must also
retain the singleton normalization/residual family. Optional sum-of-squares and
F16 projection remain governed by their existing policies; sum-of-squares is off
by default. Kernel equality alone does not qualify the complete decoder.

## Shared Q4 tiles, batched tails and carrier reuse

The following switches default off and are independently measurable:

| Switch | Behavior and constraints |
| --- | --- |
| `TERMITE_METAL_Q4_0_BATCH_SHARED` | Load packed Q4 tiles once for 2–4 rows; pad row three with uniform barriers. Preserve scalar accumulation/reduction; unsupported threadgroup geometry falls back to invariant MMV. Mostly slower in the operator sweep; excluded from the faster configurations. |
| `TERMITE_METAL_Q4_0_BATCH_FUSED_TAILS` | With invariant dispatch, project all rows then use one singleton norm/residual threadgroup per row: two dispatches per cohort. Optional F16 projection keeps per-row fallback. |
| `ANTFLY_GEMMA4_METAL_OCR_BATCH_QKV` | Reuse prepared Q + paired K/V scratch for eligible zero-bias Q4 slots, avoiding row-packing copies. Other formats keep the canonical route. |
| `ANTFLY_GEMMA4_METAL_OCR_REUSE_CARRIERS` | Cache private buffers by semantic role at four-row capacity; grow by width, retaining old storage through live views. Frame dependency tracking orders overwrites. Differential diagnostics disable reuse. |
| `ANTFLY_GEMMA4_METAL_OCR_HEAD_FRAME` | After the backbone, group eligible quantized heads in one additional frame using actual one-row norm/full-logit operations, GPU packing and suppressed argmax. Dense/final singleton heads retain the legacy route. No approximate head or shared-weight head kernel. |

PLE remains canonical per row. The head-frame path reopens its planned encoder
after packing before argmax, so the frame owns the actual readout. Bitwise
Q4/Q6 full-logit fixtures check partial tiles, suppression, choices and ownership.
No CUDA, audio generation or PDF-rendering gain follows from this OCR campaign.

### Current whole-request result

Frozen runtime `7f4d7fa577`, Apple M4 Max (32 GPU cores, 36 GiB), Q4_0 E2B
GGUF target and Q8_0 projector, greedy OCR, thinking disabled, 2,048-token cap.
All rows below use the **entire eight-page corpus**, 4,057 completion tokens per
pass, and the same binary. Recognition includes image preparation, prefill,
decode and HTTP; PDF rendering and startup are excluded.

| Configuration | Seconds per eight-page pass |
| --- | --- |
| Ordinary before | 57.572 / 56.526 |
| Invariant tails + QKV + carrier reuse (`pool`) | 52.843 / 52.206 |
| Above + canonical head frame (`head`) | 49.304 / 48.819 |
| Ordinary after | 55.524 / 55.339 |

`head` reduces mean time **12.8%** (14.6% higher throughput); `pool` reduces it
6.6%. Both candidate ranges beat every bracketed control. Weight sharing is
**disabled**. This is a local campaign result requiring independent replication;
all experimental switches remain off. The approximately 6.1 seconds/page is
amortized throughput, not measured individual-page latency in the shared wave.
The earlier Apple accurate baseline was 2.087 seconds for all eight pages and
excluded PNG preparation/HTTP; it is not a fresh bracketed compute comparison.

The head configuration also matches repeated Attention page 6 (2,968 tokens/pass),
sixteen text pages forward/reversed (9,126 tokens), and the allocation probe.
Those conditions lack fresh ordinary timing brackets. The probe samples 3.710 GiB
Metal allocations and 3.457 GiB process-tree RSS: overlapping domains, never add
them; post-wait samples do not establish an instantaneous peak. Probe timing is
excluded. Across the recorded native-cohort conditions, 128 page observations
and 79,222 completion tokens pass full output gates, including diagnostics.
The final runtime passed 66 experiment-enabled checks, all 101 compiled checks
at default flags, the optional sum-of-squares check and nine sweep tests.
[Shared-batching evidence](gemma4-ocr-shared-batching.json) retains exact pins,
flags, hashes, output gates, counters and excluded conditions.

## Speculative image OCR

`ANTFLY_GEMMA4_MTP_MULTIMODAL=1` is a separate experiment for a compatible
assistant with a qualified Metal GGUF target and final-hidden contract. Seed it
from actual image prefill hidden/KV state; unavailable hidden state fails rather
than reconstructing a text-only placeholder prompt. Target verification controls
acceptance and completion budget. Existing automatic calibration remains intact.

No Antfly full-corpus MTP speedup is qualified. Fewer assistant calls or frame
submissions are not enough: compare drafting + verification + recovery per
committed token. K=1 without bonus cannot amortize target steps; larger K can
increase rejected work. Cached target seeds count known target choices, not
independent predictor accuracy. A reduced seven-page F32 MLX result cannot rank
Antfly against MLX because quantization, outputs and workload differ.

Experimental controls and eligibility remain explicit:

| Controls | Contract |
| --- | --- |
| `ANTFLY_GEMMA4_MTP_LONG_CONTEXT_FRAME` | Canonical one-token hidden/choice recovery beyond the sliding window; retain the existing fallback when unavailable. |
| `TERMITE_METAL_ENABLE_GEMMA4_MTP_VERIFY_{INPUT,TAIL}_FRAME` | Extend eligible dense 2–4-token verification frames to embedding/PLE and readout. Corresponding `DISABLE` switches override enables; other shapes/families retain existing behavior. |
| `ANTFLY_GEMMA4_MTP_SEED_CACHED_TARGET` | Trusted recovery hidden/choice pair only; fixed K=2, resident greedy, final hidden, constant position, adaptive K and deferred recovery disabled. Original full-corpus lane failed paragraph parity; remains off. |
| `ANTFLY_GEMMA4_MTP_ENABLE_MASKED_EMBEDDING` + `TERMITE_METAL_ENABLE_GEMMA4_MTP_CLUSTER_HEAD` | GPU centroid/cluster/candidate scoring for the assistant; target still verifies with its full vocabulary head. Unsupported layouts retain fallback. |
| `TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_REPLAY` | Replay only the clustered drafter-head commands; buffer/layout/dtype/weight changes invalidate the template. Released slots drop cached bindings. |
| `TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME` | Append clustered-head scoring to the assistant frame before its scalar boundary; preserve caller ownership of an empty frame afterward. |
| `ANTFLY_GEMMA4_MTP_RECOVERY_CHOICE_ONLY` | Skip unused recovery snapshots; retain them when an activation is needed. |
| `TERMITE_METAL_ENABLE_GEMMA4_DECODE_SCRATCH_ENVELOPE` | Reserve singleton scratch once at layer maxima only for uniform KV-head geometry. |

The target's vocabulary head is already Q8_0. The formerly attributed BF16
projection is PLE (1,536→8,960), not the 262,144-output head. Head-format changes
must use actual checkpoint/dispatch evidence.

## Matrix research controls

The separate matrix opt-in batches QKV/FFN/PLE/head projections while retaining
per-page attention, RoPE and frame-local F32/F16 KV suffix publication.
`ANTFLY_GEMMA4_METAL_BATCHED_Q4_FFN=1` exposes the FFN route independently;
`TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN=1` overrides either enable.
`TERMITE_METAL_ENABLE_Q4_0_SHORT_ROWS_SPECIALIZED=1` selects exact-column
two-/three-row F32 kernels; existing small-batch/wide disables override it,
and other row counts/formats retain fallback. Short matrix RMS uses parallel
reduction at eligible widths; `TERMITE_METAL_DISABLE_SMALL_ROWS_NORM_REDUCE=1`
restores prior selection. An isolated kernel gain does not qualify OCR output.

`ANTFLY_GEMMA4_METAL_OCR_EAGER=1` selects the compatibility path for diagnosis.
`ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1` and the paired
`ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE{,_TARGET_ACTIVATION}=1` controls alter
speculative recovery work; they have separate transcript/work gates and do not
inherit the canonical cohort's qualification. Fixed-K experiments explicitly
disable `ANTFLY_GEMMA4_MTP_ADAPTIVE_K` and preserve original full-context guards
unless their qualified recovery opt-in is supplied.

## Runtime control and profiling

`ANTFLY_GEMMA4_OCR_PROFILE=1` records optimized hits/fallbacks, actual occupancy,
submissions, waits and tensor mirror downloads without extra synchronization.
Scalar token downloads are outside that tensor counter. MTP phase/draft profiles
are enclosing wall times; nested phase counters must not be added.

`ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL_STEP=1` compares complete matrix steps at
canonical KV prefixes; `ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL=1` compares and
restores each layer. These are mutually exclusive, add readbacks/work and cannot
qualify throughput. Scoped `ANTFLY_GEMMA4_METAL_OCR_CANONICAL_{QKV,PLE,TAIL}`
rollbacks are diagnostic alternatives, not a parity guarantee. Canonical tail
rollback is incompatible with differential modes. Use
`TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN=1` to override matrix FFN enables;
setting an enable to zero alone does not isolate that work.
`ANTFLY_GEMMA4_METAL_OCR_CONFIRM_EOS=1` checks the same reserved KV position;
it does not validate earlier history. `TERMITE_METAL_TRACE_GEMMA4_OCR_STOP=1`
traces its choices. All diagnostics/rollbacks default off.

Stage sampling and allocation probes run separately from throughput. Baseline
body-frame samples and mixed body/head samples are not equal-work averages.
The optional probe measures worker Metal allocations after waits; process-tree
RSS can double-count shared mappings. Neither is total nonoverlapping memory.

## Reproduction

Use the model/corpus preparation and server budgets in the
[comparison report](gemma4-ocr-comparison.md#reproduction). Build the ReleaseFast
server with Zig 0.17, freeze the binary/checksum and source revision, then run
[`gemma4_ocr_sweep.py`](../../../zig/pkg/inference/tools/bench/gemma4_ocr_sweep.py).
It starts fresh servers, clears inherited experiment flags, records source/binary
pins and rejects incomplete outputs before ranking time per committed token.
`tails`, `pool` and `head` isolate controls without shared weights; `share`,
`fused`, `reuse` and `optimized` additionally enable sharing. `invariantprofile`,
`headprofile` and `optimizedprofile` are diagnostics, not throughput cases.

Compare candidates with same-binary ordinary controls before and after; record
host contention and keep GPU jobs exclusive. Full repeated output gates precede
speed claims; cross-campaign timings are not interchangeable. Large inline
requests may require `--http-max-body-mb 32` (run JSON `http_max_body_mb`): default
10 MiB, finite/overflow-checked, independently bounded by HTTP aggregate retention
and model/KV admission. A connection reset before decode is not a GPU capacity
measurement. Preserve complete outputs, not only token totals or `stop` status.

## Remaining work

Replicate the local head-frame gain on an idle independent host and broader
manually checked PDFs before promotion. Measure preparation, prefill and decode
separately; preserve singleton arithmetic while reducing remaining PLE/head
synchronization. Matrix and cached-seed research must first pass free-running
paragraph/order/memory gates. PDF and audio workloads need their own end-to-end
qualification. Apple generation has not been benchmarked in these reports.
