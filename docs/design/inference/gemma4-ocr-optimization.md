# Gemma4 generative OCR optimization

[Compact results](gemma4-ocr-optimization.json).

## Clustered drafting, recovery and continuous cohorts

The next implementation separates four opt-in opportunities:

- `ANTFLY_GEMMA4_MTP_ENABLE_MASKED_EMBEDDING=1` plus
  `TERMITE_METAL_ENABLE_GEMMA4_MTP_CLUSTER_HEAD=1` runs centroid scoring,
  stable cluster selection, indexed candidate scoring and token selection on
  Metal. The E2B assistant has BF16 matrices of 2,048×256 centroids and
  262,144×256 output embeddings; top-32 selection scores 4,096 vocabulary rows.
  This is distinct from the target's Q8_0 262,144×1,536 vocabulary head.
  The target still verifies proposals with its existing full vocabulary head.
  F32/BF16/F16/Q8_0 candidate weights are supported; unsupported/inverse-order
  layouts retain the existing fallback. One bounded native workspace and one
  immutable ordering upload are reused. Quantized candidate slots avoid an
  unnecessary full dense mirror.
- `ANTFLY_GEMMA4_MTP_RECOVERY_CHOICE_ONLY=1` avoids producing a normalized
  hidden snapshot when recovery needs only KV writes and the next target choice.
  It keeps the canonical prepared singleton forward, and leaves the hidden-copy
  route intact whenever an activation is actually needed.
  `TERMITE_METAL_ENABLE_GEMMA4_DECODE_SCRATCH_ENVELOPE=1` reserves the shared
  singleton scratch capacity once at the layer maxima instead of repeating
  reservation for each layer. It requires uniform KV-head geometry; other
  layouts keep the original reservation loop. These are capacity checks, not
  changes to decoder arithmetic.
- `TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_REPLAY=1` stores the four clustered-head
  dispatches in a Metal indirect command buffer. A blit refreshes the current
  hidden input at a stable address before each execution. Weight/ordering
  buffer identities, offsets, dimensions and dtypes invalidate the command
  template. Direct dispatch remains available for A/B comparisons and fallback.
  This first replay scope covers the drafter head, not whole target layers;
  target KV/RoPE/attention commands still use their existing encoding.
- `ANTFLY_GEMMA4_METAL_OCR_CONTINUOUS=1` with the existing OCR batch opt-in
  admits a bounded resident window (default eight, maximum sixteen) larger than
  the GPU cohort (maximum four). The native scheduler packs ready decoders
  with independent positions, retires finished leases, and fills subsequent
  cohorts from the remaining window. `ANTFLY_GEMMA4_METAL_OCR_WINDOW` controls
  the resident window; `ANTFLY_GEMMA4_METAL_OCR_BATCH_ROWS` controls cohort size.
  Manifest/media ceilings still bound materialization and admission reserves
  all live KV caches. This operates within an explicit batch request, rather
  than establishing a global cross-request image batching service. Both the
  existing scalar coordinated lane and experimental matrix lane can be tested.

An isolated probe uses the actual BF16 assistant head/centroid weights and token
ordering, with a fixed synthetic 256-element hidden vector. Both variants use
ICB replay and alternate order over 24 pairs; the first six pairs are warmup.
The serial selector has median GPU time 750.45 µs (746.8–1,092.5), versus
302.45 µs (299.8–428.6) for parallel ranking with eight SIMD rows per threadgroup.
Selected token IDs match in every pair. This isolates the selector implementation
within the clustered path; it does not compare against the original full-vocab
head or qualify whole OCR throughput. Repeated fixed inputs favor cache reuse.
The implementation drops cached command bindings when their prepared weight
slot is released, preventing templates from retaining evicted weights.

The operator fixtures cover dense/Q8 weights, nonzero input/ordering offsets,
changing inputs, workspace/template reuse, invalid shapes and replay on/off.
The scheduler fixtures cover eight unequal page positions, four-row limits,
lease retirement and a newly admitted page. All options remain off by default;
complete-output and repeated whole-request performance gates decide qualification.

### Whole-request qualification

[Clustered-head results](gemma4-ocr-clustered-head.json) and
[continuous-cohort results](gemma4-ocr-continuous-cohorts.json) pin runtime
`849a025887`, binary, model, flags and corpus. Each full run uses eight
pre-rendered pages, one warm page and two passes. Rendering and model startup
are excluded. Timings measure recognition, including image preparation,
prefill, decoding and HTTP.

| Mode | Eight-page seconds, two passes | Output gate |
| --- | --- | --- |
| Ordinary singleton | 57.816 / 56.257 | 4,057 tokens, all eight transcripts match |
| MTP control before | 66.496 / 67.058 | Same output |
| Clustered head + replay + recovery + scratch | 64.163 / 64.738 | Same output |
| MTP control after | 63.698 / 64.778 | Same output |
| Explicit batch-eight serial control before | 54.308 / 54.012 | Same output |
| Continuous window-eight, scalar cohorts | 59.878 / 57.147 | Same output |
| Continuous window-eight, matrix cohorts | 34.296 / 34.639 | First pass omits Attention page 6's final paragraph; second matches |
| Explicit batch-eight serial control after | 53.583 / 53.802 | Same output |

The clustered changes preserve output but overlap the final MTP control;
MTP remains slower than ordinary decoding. Scalar continuous cohorts preserve
output but do not improve throughput. Matrix batching is substantially faster,
but one pass emits 4,016 rather than 4,057 tokens: Attention page 6 stops at
701 rather than 742. It does not hit the completion cap. The other seven pages
match in that pass and all eight match in the second. This is an intermittent
quality failure, so the faster timings do not qualify a repeatable OCR speedup.
Per-page times are unavailable inside the shared eight-page wave.

Earlier prototype pilots are excluded: they silently used the old masked
fallback because the synchronization helper reopened an empty active frame.
The integration now accepts that empty borrowed frame while rejecting pending
work or unawaited submitted work, and preserves caller ownership. Regression
fixtures cover those states and released-slot template invalidation. A separate
instrumented serving run confirms the native clustered kernels execute; its
timings are diagnostic only. The final regression selection passes 54 tests
with zero skips, including actual Metal GPU execution.

`ANTFLY_GEMMA4_METAL_OCR_CONFIRM_EOS=1` adds an opt-in canonical singleton
check of a batched stop decision at the same reserved KV position. It does not
append another position or validate earlier matrix arithmetic. With
`TERMITE_METAL_TRACE_GEMMA4_OCR_STOP=1`, a final single-pass diagnostic at
runtime `e58cba6333` matches all 4,057 tokens and transcripts. All seven stops
encountered in matrix cohorts agree with canonical token 106; none is replaced
(the remaining page finishes in a singleton cohort). Thus the passing run does
not establish that confirmation fixes the prior intermittent omission. Its
34.339 s timing is diagnostic only, and no option is promoted.

## Paragraph omission: free-running trace and serving correction

The [paragraph correctness investigation](gemma4-ocr-paragraph-correctness.json)
captures the first changed decision while preserving independently accumulated
KV history. Four copies of Attention page 6 all reproduce the ordinary 742-token
transcript in both singleton passes. The old matrix/QKV-rollback lane omits the
paragraph in one uninstrumented pass, and the free-running trace reproduces it
in a separate pass. The coordinator still uses two-, three- and four-row steps
as those identical requests become ready; duplicate inputs do not freeze the
scheduler's cohort geometry.

At total sequence length 1,011, with input token 236761, ordinary selects newline
108 and the failing matrix row selects EOS 106. The same input/token prefix
matches in all 5,410 traced matrix-row comparisons, with exactly this one changed
choice. The singleton newline/EOS margin is +0.0032997; the failing matrix favors
EOS by 0.0056114. Its response ends normally with `finish_reason=stop`, at an
exact prefix 204 characters/41 completion tokens shorter than ordinary. This
establishes an early model stop decision rather than lost HTTP/stream bytes or
a generation-cap failure. The trace does not restore canonical KV after each
step, unlike the earlier differential mode that masked accumulated drift.

The same-input projection differences, row-dependent operation selection and
free-running margin movement support accumulated numerical state drift. They
do not identify one invalid KV index or prove a single offending kernel.
[Batch-invariant inference research](https://thinkingmachines.ai/blog/defeating-nondeterminism-in-llm-inference/)
explains why deterministic kernels can still change an individual request's
answer when batching changes the arithmetic; it is background, not independent
proof of this particular failure.

New attention-output and complete post-attention block rollbacks also fail the
repeated-page gate. Combining QKV/block/PLE/tail rollbacks at `145ba9f996` still
fails: the `strict` sweep case was only a collection of scoped switches, not the
complete canonical operation family. In particular, the old QKV rollback used
three standalone row projections, while ordinary dense Gemma4 uses the fused
QKV family with paired K/V reduction. `7736ff8f80` corrects that reference and
rollback to call the singleton fused family, copying each result before scratch
can be reused. Other singleton KV/attention/fusion choices still matter, so no
partial rollback is promoted as a parity guarantee.

Native OCR batching now retains the complete prepared singleton operation
family by default, including independent page KV, paired projections, KV
publication/attention and layer/tail arithmetic. Layers remain coordinated in
one GPU frame and only token IDs return to the host. Setting the older
`ANTFLY_GEMMA4_METAL_OCR_MULTIROW=1` alone no longer selects the unqualified
matrix arithmetic. It requires an additional explicit
`ANTFLY_GEMMA4_METAL_OCR_MATRIX_ARITHMETIC=1`, or an explicit canonical-prefix
or layer differential diagnostic. The sweep's `cohort4w8stable` case verifies
the normal path with the legacy multirow flag still set and the new matrix flag
zero; matrix research cases explicitly set the new flag. Matrix timing gains
in earlier source-pinned reports remain conditional research observations.
All normal serving defaults and model/KV admission limits remain bounded.

The final `8ed689d285` serving correction matches every transcript/count in
72 page observations (41,428 completion tokens total), including the former
allocation-instrumented failure condition. No generation reaches its cap.

| Canonical native cohort check | Recognition seconds | Completion tokens/pass |
| --- | --- | --- |
| Four copies of Attention page 6, two passes | 41.351 / 41.114 | 2,968 / 2,968 |
| Original eight pages, two passes | 65.967 / 62.340 | 4,057 / 4,057 |
| Sixteen text pages, forward | 152.964 | 9,126 |
| Sixteen text pages, reversed | 160.773 | 9,126 |
| Sixteen text pages, allocation-instrumented | Timing excluded | 9,126 |

The occupancy logs contain actual two-, three- and four-row native cohorts.
The runner derives these separately from matrix-frame counters and removes
preload activity; explicit warmups may remain in occupancy counts, which are
not committed output-token counts. The memory stress run samples 3.750 GiB
of Metal resources and 1.466 GiB process-tree RSS as separate domains.
These final runs verify correctness against unchanged ordinary references;
they lack new ordinary timing brackets and do not qualify a throughput gain.
The earlier matrix speedup cannot be retained as a normal serving claim.

The corrected fused-QKV research rollback matches both repeated-page passes,
but combining it with the other scoped rollbacks still omits paragraphs
(2,927/2,886 versus 2,968 tokens/pass). This reinforces the distinction between
individual row operations and the full prepared singleton family. The guard
retains the latter, rather than changing EOS policy or forcing extra text.

Final validation selects 61 native Metal/MTP/decoder/scheduler/QKV/HTTP tests:
all pass with head-frame/replay both enabled and explicitly disabled, zero skips.
Eight sweep tests, ReleaseFast serving build, Zig/Python formatting, license and
whitespace checks pass. Model/corpus/source/binary/harness/probe pins and all
failed scoped gates remain in the correctness report.

## Fixed-prefix parity and stage isolation

The next qualification tools distinguish complete-step drift from individual
operation differences. `ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL_STEP=1` executes a
matrix step first, recomputes the same input and reserved KV positions through
the canonical singleton, and emits the canonical choice. The singleton restores
the current KV positions before the next step, so every comparison starts from
a canonical committed prefix. It records both choices and top-two logit margins;
canonical hidden capture is checked against the canonical head choice.

`ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL=1` instead compares embedding/PLE inputs,
attention normalization, Q/K/V projections, complete layer outputs and final
normalization/logits. Local projection comparisons use exactly the same normalized
input. Every layer's singleton result and current KV writes replace the matrix
result before proceeding, isolating local errors from accumulated drift. Both
modes are diagnostics with host readbacks and extra GPU work, and cannot be used
together. Their timings never qualify throughput.

An initial eight-token, eight-page probe at runtime `14445c818b` records 6,811
stage samples and 49 matrix choice comparisons, with no nonfinite values or
changed choices. Embeddings, PLE inputs, attention norms and final norms match
bit-for-bit in every sample. The first difference is the layer-zero Q projection:
maximum absolute difference 6.10e-5. Across all samples Q projection differences
reach 1.22e-4, complete local layer differences 1.48e-3, and logits 2.29e-5.
This establishes differing projection arithmetic, not a KV/scheduler bug or the
cause of a later token change. It also corrects the initial normalization lead:
two passing normalization-rollback runs do not prove normalization caused the
intermittent omission. Disabling small-row reduction takes 77.220/76.423 s
versus the matrix control's 40.446/42.764 s, which reproduces one omission.

Two full canonical-prefix passes at `920765db79` check 7,800 matrix choices,
with zero changed choices and zero captured-head choice mismatches. Both emit
all eight ordinary transcripts (4,057 tokens/pass). Diagnostic times are
96.637/101.334 s and are excluded from performance comparisons. The smallest
canonical top-two margin is 0.0032997 logits: newline token 108 versus EOS 106
at sequence position 1,011, immediately around the omitted paragraph boundary.
The earlier shortened transcript is an exact 2,810-character prefix of the
3,014-character ordinary transcript; it omits 204 characters/41 tokens.
This is consistent with accumulated numerical state perturbations flipping a
fragile stop decision. Identical-state probes do not establish a deterministic
kernel/KV indexing defect, and restoring only the final step cannot repair past
state differences. The scoped rollbacks and free-running corpus/order tests
are needed to qualify an actual remedy.

Scoped rollback switches keep other matrix work available:

- `ANTFLY_GEMMA4_METAL_OCR_CANONICAL_QKV=1` uses prepared singleton projections
  per row, sharing the frame and preserving independent positions/attention.
- `TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN=1` explicitly overrides the
  multirow Q4 FFN opt-ins. Setting the enable flag to zero alone is insufficient
  because the OCR matrix opt-in also enables this path.
- `ANTFLY_GEMMA4_METAL_OCR_CANONICAL_PLE=1` uses singleton PLE tails per row.
- `ANTFLY_GEMMA4_METAL_OCR_CANONICAL_TAIL=1` completes the matrix backbone
  frame and uses the existing fused singleton norm/head/argmax tail per row.
  It exports only scalar IDs. Tail commands are included in wall time but
  excluded from matrix backbone frame counters. It cannot be combined with
  either differential mode.

The first packed row norm/linear tail prototype is rejected: runtime
`920765db79` encounters an encoder assertion, and `eccf5bfc65` avoids that
assertion but produces incorrect transcripts, including a capped response.
Those timings are excluded. Runtime `7f531ca852` replaces that prototype with
the existing fused canonical singleton tail after completing the backbone.
This avoids treating unqualified generic in-frame row packing as a reference.

The assertion also exposes a general Metal copy issue. Buffer-copy/pair
transitions now use their explicit owning runtime rather than ambient thread-local
state to close the compute encoder and account for blits. Shared-to-shared copies
use the GPU when a frame is active/submitted, instead of immediate CPU memcpy
before queued producers complete. GPU fixtures remove the ambient association,
queue a compute producer and check exact copied values, offsets, frame counts,
shared/private destinations and paired copies. These correctness fixes apply to
the shared Metal copy primitive; they do not establish the cause of the original
intermittent paragraph omission or qualify an audio speedup.

All switches are off by default. The earlier enable-zero FFN experiment is
excluded from FFN isolation: it retained the matrix kernel. Stage rollbacks
are alternatives to blanket singleton decoding; their complete-output and
performance gates must both pass before promotion.

`TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME=1` appends clustered-head work to
the assistant's existing frame before the scalar boundary. The hidden producer
and head then require one submission/wait rather than a producer flush followed
by an independent head command. A flush preserves caller frame ownership by
reopening an empty frame, which the caller can cancel. The native fixture tests
pending input writes, selected token correctness, exactly one submission and
empty borrowed-frame preservation, with direct dispatch and ICB replay.

`tools/bench/gemma4_ocr_sweep.py` starts a fresh server per case, pins the binary
hash and explicit compilation revision, clears inherited experiment flags,
checks every transcript/token count and generation cap, and records the sum
of server/worker process-tree RSS (shared mappings can be double-counted).
Earlier prototype RSS sampled only the lightweight parent process and is
excluded from memory comparisons. An optional local `metal_memory_probe.m`
uses [Metal's resource allocation counter](https://developer.apple.com/documentation/metal/mtldevice/currentallocatedsize?language=objc)
after command-buffer waits in the inference worker. Those are sampled maxima,
not instantaneous peaks. Injected memory-probe timings are diagnostic only;
RSS and Metal allocations are different domains and must not be added. It supports row-order reversal, separate
stage rollbacks, cohorts of two/four with windows four/eight/sixteen, and fixed
MTP K=1/2/3/4. It excludes warm requests from MTP acceptance/phase counters and
compares time per committed completion token, including drafting and recovery.
The runner never promotes a diagnostic or incomplete-output case. Repeat ranges
must beat the ordinary controls before it reports a campaign speedup; independent
replication is still needed on this shared machine.

### Larger inline media envelopes

The sixteen-page PNG request contains about 11.5 MB of base64 media, exceeding
the standalone HTTP listener's default 10 MiB body cap. Its connection resets
before any batch decoding, so that failure does not measure GPU window capacity.
`--http-max-body-mb 32` (or run JSON `http_max_body_mb`) now sets an explicit
encoded-payload cap. `NodeConfig.http_max_body_bytes` feeds the common listener
configuration. The default remains 10 MiB; zero and conversion overflow are
rejected. The HTTP library's 64 MiB aggregate retained-body budget covers this
32 MiB cap, independently from model/KV admission and GPU cohort limits. Tests
cover defaults, units, overflow, invalid capacity and aggregate retention.

The first expanded corpus independently fails ordinary OCR on Attention page 4:
its diagram repeats “Linear” to the 2,048-token cap in both passes. That failed
quality case is retained separately, with no throughput qualification. A new
sixteen-page text corpus replaces it with NIST page 11 and produces a stable
9,126-token ordinary reference, with no caps. No claim of general diagram OCR
correctness follows from the text-corpus results.

### Repeated cohort/window qualification

The [source-pinned parity investigation](gemma4-ocr-parity-investigation.json)
retains both successful and rejected runs. Each timing below covers one complete
recognition pass over pre-rendered pages; ordinary runs bracket each campaign.
All throughput candidates must match every ordinary transcript and completion
count in both passes, without reaching the generation cap.

| Corpus / configuration | Two passes, seconds | Output gate |
| --- | --- | --- |
| Eight pages, ordinary controls (`7f531ca852`) | 58.628–65.369 | 4,057 tokens/pass |
| Eight pages, four-row/eight-window canonical QKV | 44.476 / 44.229 | Pass |
| Sixteen text pages, ordinary controls (`7f531ca852`) | 123.357–129.584 | 9,126 tokens/pass |
| Sixteen text pages, two-row/four-window | 96.493 / 97.193 | Pass in this order |
| Sixteen text pages, two-row/eight-window | 106.690 / 104.316 | One paragraph omitted in pass two |
| Sixteen text pages, four-row/four-window canonical QKV | 98.194 / 95.389 | One paragraph omitted in pass one |
| Sixteen text pages, four-row/eight-window canonical QKV | 93.548 / 92.430 | Pass |
| Sixteen text pages, ordinary controls (`90a6fd9f5b`, 32 MiB HTTP cap) | 124.716–146.310 | 9,126 tokens/pass |
| Sixteen text pages, two-row/sixteen-window | 132.126 / 128.953 | Pass; overlaps controls |
| Sixteen text pages, four-row/sixteen-window canonical QKV | 107.780 / 96.145 | Pass |
| Reversed eight pages, ordinary controls (`90a6fd9f5b`) | 55.297–61.729 | 4,057 tokens/pass |
| Reversed eight pages, two-row/four-window | 46.768 / 45.893 | One paragraph omitted in pass two |
| Reversed eight pages, four-row/eight-window canonical QKV | 49.893 / 46.659 | Pass |

Four-row/eight-window canonical QKV is the stronger measured candidate: it
passes both ordinary eight-page runs, both expanded text runs and both reversed
runs. In the sixteen-text-page campaign it reduces median recognition time by
about 27% versus its own ordinary controls. Increasing the request window to
sixteen does not establish a further gain. The smaller cohort fails under
reordering, and QKV rollback itself fails with a four-page window; neither
cohort size nor a single scoped rollback guarantees canonical output.
A subsequent allocation-instrumented pass also reproduces the paragraph
omission with four-row/eight-window canonical QKV (9,085 versus 9,126 tokens).
Although instrumentation excludes its timing, that output failure is retained
as evidence that QKV rollback is not a robust parity fix. The matrix lane and
rollback switches therefore remain experimental and off by default. Timing ranges across campaigns should not be ranked directly on
this shared machine; the per-campaign ordinary brackets are the comparisons.

The `90a6fd9f5b` sixteen-page campaign also records native generation pipeline
latency: ordinary p95 is 10.594–12.120 seconds, while four-row/sixteen-window
canonical QKV p95 is 106.448 seconds. These per-page pipeline durations include
waits but exclude queueing before pipeline start and HTTP return; the full batch
returns as a wave. Better aggregate completion time therefore does not imply
better individual-page latency. Earlier `7f531ca852` profile summaries still
include a preload dummy generation and must not be used for precise per-request
latency or MTP work comparisons; `90a6fd9f5b` removes it before explicit warmup
removal. Recognition wall times are unaffected by that profile correction.

### Fixed-K and synchronization calibration

Runtime `90a6fd9f5b` repeats Attention pages 1 and 3 twice per case, with a fresh
server and one warmup page. Every variant reproduces the same 841 tokens/pass.
The pilot is qualified against those two pages from the unchanged eight-page
ordinary reference; supplying the full reference initially marked the six
out-of-corpus pages missing. The report records this post-measurement reference
subset correction, without changing outputs or timings.

| Configuration | Two passes, seconds | ms / committed token | Matched / drafted |
| --- | --- | --- | --- |
| Ordinary controls | 11.990–12.766 | 14.257–15.180 | — |
| MTP K=1 | 15.165 / 15.688 | 18.032 / 18.654 | 590 / 1,092 |
| MTP K=2 | 15.993 / 15.420 | 19.016 / 18.336 | 802 / 1,380 |
| MTP K=3 | 17.345 / 17.270 | 20.624 / 20.536 | 864 / 1,674 |
| MTP K=4 | 16.917 / 16.876 | 20.115 / 20.066 | 926 / 1,962 |
| K=2, assistant head frame | 15.171 / 15.128 | 18.040 / 17.988 | 802 / 1,380 |
| K=2, verification input/tail frames | 15.181 / 16.626 | 18.051 / 19.769 | 802 / 1,380 |
| K=2, all three frames | 15.102 / 15.321 | 17.957 / 18.217 | 802 / 1,380 |
| K=2, cached seed, legacy reductions, head frame | 14.394 / 13.854 | 17.116 / 16.473 | 1,050 / 1,262 |

None beats ordinary. K=2's head frame reduces total measured draft phase time
across both passes from 5.926 to 5.471 seconds, while verification and correction
still dominate: 9.181 and 10.731 seconds respectively. Increasing K reduces
rounds but adds draft/verification work; K=4 matches only about 47% of its draft
slots. Cached-seed acceptance counts include a target-supplied first slot,
so its 83% ratio is not an independent predictor accuracy measurement.
Only the strongest complete-output pilot candidate is selected for the separate
full eight-page gate; the four K values are pilot measurements, not full-corpus
qualifications. All MTP experiments remain off by default.

The selected cached-seed/reduction/head-frame candidate also preserves all
4,057 tokens in both full eight-page passes at `90a6fd9f5b`: 68.715/66.461 s,
versus ordinary brackets 64.252/68.694 and 61.370/64.393 s. Its ranges overlap
the controls, so **no overall MTP speedup is qualified**. Unlike the earlier
cached-seed configuration, this combined configuration passes this full-corpus
quality check. It does not isolate which reduction rollback changes the fragile
stop decision, or establish parity under every cohort/order. Across the two
passes it spends 13.589 s drafting, 59.580 s verifying and 39.087 s materializing
corrections; 5,106/6,010 draft slots match (including supplied seed slots).
Reducing assistant work alone leaves target verification/correction as the
larger costs. The measured matrix cohort path is the current throughput lead.

### GPU allocation diagnostics and final validation

A separate one-pass sixteen-text-page run injects the local wait probe at runtime
`90a6fd9f5b`. Every timing from this campaign is excluded from throughput ranking.
The reported allocation maximum includes preload/prefill and recognition; it is
sampled after waits and does not resolve short-lived peaks.

| Configuration | Sampled Metal resources, GiB | Sampled process-tree RSS, GiB | Output |
| --- | --- | --- | --- |
| Two-row/four-window | 3.781 | 1.313 | 9,085 tokens; Attention page 6 short |
| Four-row/eight-window canonical QKV | 3.767 | 1.868 | 9,085 tokens; Attention page 6 short |
| Four-row/sixteen-window canonical QKV | 3.720 | 3.533 | All 9,126 tokens match |

There are 135/102/92 allocation samples respectively. Similar sampled GPU
maxima do not establish identical live KV usage: the maximum also includes
common preload/prefill resources. Host RSS increases with the larger media
window, and may double-count shared mappings. Do not add RSS to Metal bytes.
The instrumented QKV failure reinforces the parity limit; it does not establish
that instrumentation itself causes the numerical divergence. All matrix and
MTP experiments remain opt-in, with no new audio or CUDA speedup claim.

Final `90a6fd9f5b` validation selects 56 native Metal/MTP/decoder/scheduler/parity/
HTTP-capacity tests: all pass, zero skips, with head-frame/replay both enabled
and both disabled. ReleaseFast serving build, seven sweep tests, Zig formatting,
license and whitespace checks pass. The committed report pins each measured
binary, harness, corpus PNG and diagnostic probe hash.

## Verification framing and vocabulary-head audit

[The framing measurements](gemma4-ocr-verification-framing.json) retain the
original Q4_0 target/Q8_0 projector and identical eight-page greedy output
(4,057 completion tokens per pass, no truncation). The target vocabulary head
already uses Q8_0 and the prepared-slot Q8 matrix-vector kernel. The previously
attributed BF16 head is actually the per-layer embedding projection (1,536 to
8,960); the head projects 1,536 to 262,144. Exploratory wider Q8 output groups
and short-row kernels have overlapping timings at K=2 and are excluded.

Two independent opt-ins extend the existing verification frame:
`TERMITE_METAL_ENABLE_GEMMA4_MTP_VERIFY_INPUT_FRAME=1` includes embedding/PLE
preparation for supported dense Gemma4 2–4-row suffixes, and
`TERMITE_METAL_ENABLE_GEMMA4_MTP_VERIFY_TAIL_FRAME=1` includes the prepared head
and argmax. Each corresponding `DISABLE` flag overrides its enable flag.
Owned frames cancel on early exit, borrowed frames remain owned by their caller,
and a decoder that consumes the frame prevents a second cancellation.
Full prompts, singleton rows, other families and MoE retain existing behavior.

Combined framing reduces tracked target submissions from 4,601 to 3,350 per
full pass: one saved submission for each of 1,251 verification calls (27.2%).
The combined wall times are 63.446/65.822 s; controls before are 69.971/66.999 s
and after are 63.946/63.612 s. Ordinary decoding is 54.106/54.585 s. Shared-machine
timing ranges overlap, so fewer submissions do not qualify an overall speed
win and MTP remains slower here. Changed frame scope also changes profiled GPU
time attribution; those counters are not equal-work kernel timings.
The frame ownership/shape regression selection passes all 12 tests, and the
ReleaseFast serving build passes. Both flags remain off by default.

## Cached target prefixes: reduced work, failed full-corpus parity

[The seed experiment](gemma4-ocr-cached-target-seeding.json) adds
`ANTFLY_GEMMA4_MTP_SEED_CACHED_TARGET=1`, off by default. Canonical recovery
retains the normalized target hidden state paired with its cached next choice.
The next K=2 round uses that known choice as slot zero and runs one assistant
prediction for slot one. Initial/predictor-only activations do not qualify.
The experimental lane requires resident greedy Metal drafting, final hidden
source, constant position, adaptive K disabled and deferred recovery disabled.
Other K values keep the existing recovery path. Target verification, completion
budget and bonus acceptance still govern every emitted token. `cached_seeds`
counts known target prefixes; `draft_steps` counts physical assistant calls.

The two-page pilot preserves all 841 tokens and transcripts: ordinary 12.626 s,
control 14.458 s, seed 12.814 s. Assistant steps fall 682 to 317, recovery
forwards 427 to 314, while verification rows increase 506 to 630.

Full eight-page work counters repeat across both passes: assistant steps fall
3,206 to 1,488 (53.6% fewer), recovery forwards 1,947 to 1,475 (24.2% fewer), and
verification rows rise 2,502 to 2,960 (18.3% more). However, Attention page 6
omits its final paragraph in both passes: 701 rather than 742 tokens. Its output
ends normally, rather than hitting the completion cap. All seven other page
transcripts/counts match. Full candidate times 61.950/58.824 s therefore fail
the matched-work gate; ordinary is 59.550/57.122 s and bracketed MTP controls
range 68.943–80.742 s. The seven matching pages take 51.860/49.438 s versus
57.190–66.382 s with unseeded MTP and 49.369/47.269 s ordinary. That is a
post-hoc restricted scope, not full-corpus OCR qualification; seeded MTP remains
slower than ordinary in those matching-page measurements too.

A focused full-generation check on the failing page reproduces the same
701-token omission with both input/tail frames disabled, input only, and tail
only. The framing changes do not explain the failure. Changing the proposal
schedule exposes a broader target-output parity limit; these tests do not
isolate its numerical/cache cause. No inference default or full-corpus speed
claim is promoted. Next work should compare canonical one-row and two-row
verification at identical committed prefixes/KV, including prepared Q4 FFN,
attention and final-normalization/head choices, before qualifying more drafting
optimizations. The tests cover trusted-pair eligibility and invalidation when
an activation is replaced. The combined MTP/decoder regression selection passes
48 tests with no skips, and the final ReleaseFast serving build passes.

The benchmark freezes runtime `ed4e54d641` with SHA-256
`4c14dde0a7b3fdc7036b30825d606c87c199b3a0cc1bb351bf55416074497492`.
Subsequent guard/lifetime-test changes restrict recovery to the tested K=2 lane
without changing that lane. Reproduce using the existing `pdf_ocr.py` harness
with batch one, two iterations, one warm page, `--draft-model assistant
--speculative-k 2`; retain the report's explicit server flags and budget/model
configuration. Start fresh servers for ordinary, unseeded MTP, seeded MTP and
unseeded MTP controls. Rendering/model loading remain excluded.

## MLX MTP reference and pending-token follow-up

[The reference measurements](gemma4-ocr-mlx-reference.json) compare MTP on/off
within a pinned MLX target on the same M4 Max and eight pre-rendered OCR pages.
Google reports [up to threefold faster generation](https://blog.google/innovation-and-ai/technology/developers-tools/multi-token-prediction-gemma-4/)
with its specialized drafters. This experiment uses the Google-supported MLX
ecosystem's community port, not Google's benchmark configuration. The checkout
is `fb4b1e2f340249ca2ff17ba2e70e61049d6d4d33`; MLX is 0.32.3 and Transformers
is 5.19.0. The target is `mlx-community/gemma-4-E2B-it-4bit`, revision
`238767527555cb75a05732a84dff5d6ba0dd6809`, paired with the BF16 assistant at
`a7770799b560135ebdbfae8b7f468947415003bc`. Its affine 4-bit/group-64 weights
and vision encoder differ from Antfly's Q4_0 target/Q8_0 projector. Available
disk space did not accommodate the 10.2 GB BF16 reference checkpoint.
These results cannot rank the two OCR runtimes.

The benchmark keeps both models loaded, warms each mode with 16 tokens, starts
each request with independent KV, and brackets MTP with ordinary controls.
It uses the existing OCR prompt, temperature zero, thinking disabled, a
280-image-token budget, and a 2,048-token completion cap. MLX block size includes
the pending bonus token: size three drafts two candidates. Size six is a ceiling
that the reference can adapt down. Recognition timing includes image file
loading/processing, vision encoding, prefill, decoding and detokenization;
PDF rendering, model loading and HTTP are excluded.

The initial 64-token image smoke matched output and took about 0.65 seconds
ordinary versus 0.56 seconds with MTP. That short result did not generalize to
the full corpus. Original floating precision changed multiple transcripts,
and NIST page 5 repeated text to the completion cap with MTP. Ordinary controls
repeated their output exactly despite wall times ranging from 23.1 to 38.8
seconds. Those MTP timings fail the matched-output/work gate.

A benchmark-local adapter verifies each target input as a singleton while
keeping the MTP drafting/acceptance loop. It restores parity in the four-page,
64-token pilot and eliminates the NIST page 5 repetition at the original
precision. Full-corpus singleton verification still changes four pages in both
passes; first differences occur at total contexts 603, 642, 634 and 538, beyond
the 512-token sliding window. This narrows the investigation to verification
geometry and the speculative cache/attention path; it does not isolate a
particular arithmetic or cache bug. The reference replaces rotating caches
with buffered temporal caches for speculation. Its default Gemma4 MTP path
calls the model directly and does not select the optional exact-verifier branch.

Promoting floating parameters/scales to F32, while retaining the same packed
quantized integers, restores exact text **and token-ID** parity between ordinary
and MTP decoding across both full passes:

| F32 MLX scope | Ordinary median | MTP block 3 median | Work and quality |
| --- | ---: | ---: | --- |
| All eight pages | 50.292 s | 42.395 s | Same 5,439 tokens; NIST page 5 repeats/truncates in both modes |
| Seven completed pages, excluding NIST page 5 | 31.723 s | 29.575 s | Same 3,391 tokens; no truncation; mean word-F1 proxy 0.853 |

The seven-page observation is 6.8% less wall time, or 7.3% higher throughput.
Including the repeating page inflates the apparent OCR benefit. All ordinary
and MTP transcripts repeat within their precision configuration. F32 ordinary
output differs from original floating precision, so precision variants are not
matched-work speed comparisons. Unrelated CPU compilation/tests and system
services ran concurrently; the seven-page timing ranges overlap. This is a
local, paired observation, not dedicated-machine or saturated-serving
qualification. Word-multiset F1 measures coverage against `pdftotext`; it does
not establish reading order or manually verified OCR correctness.

The reproducible harness is
[`gemma4_mlx_mtp.py`](../../../zig/pkg/inference/tools/bench/gemma4_mlx_mtp.py).
It records transcripts/token IDs, timings, acceptance counts, checkpoint/source
metadata and quality gates. `--singleton-verify` is a diagnostic adapter, and
`--float32` changes floating execution precision; neither modifies the upstream
checkout. With the pinned checkout/dependencies and local model snapshots:

```sh
PYTHONPATH=/tmp/antfly-gemma4-mlx-vlm HF_HUB_OFFLINE=1 \
  TOKENIZERS_PARALLELISM=false \
  /tmp/antfly-gemma4-mlx-venv/bin/python \
  zig/pkg/inference/tools/bench/gemma4_mlx_mtp.py \
  --model /tmp/antfly-gemma4-mlx-models/target \
  --assistant /tmp/antfly-gemma4-mlx-models/assistant \
  --corpus .benchmark-results/pdf-ocr-comparison-2026-10-11/ocr-corpus.json \
  --output /tmp/mlx-mtp-reference.json --blocks 0,3,0 --passes 2 --float32
```

Antfly's frozen `8af0e0904aa27ae40c427f7c7aeef18fb6486905` build also tests
pending-token verification **with bonus acceptance enabled**, rather than the
earlier no-bonus deferred variant. Two Attention pages, forced K=2, and explicit
short-row-specialization-off flags produce the same 841 tokens and identical
transcripts in every control/candidate run:

| Antfly two-page configuration | Measured seconds |
| --- | --- |
| Ordinary resident | 11.310, 11.306, 11.363 |
| Canonical MTP recovery with bonus | 13.541, 13.819 |
| Pending-token verification with bonus | 16.520, 16.560 |

The first canonical MTP pass spends 2.626 seconds drafting, 4.205 verifying and
4.700 materializing correction/bonus state. Pending-token verification removes
the separate materialization, but spends 3.205 seconds drafting and 11.227
verifying. It processes 1,238 verification rows in 413 calls versus 506 rows in
253 calls. The canonical path's cached next-target choice can reject a first
draft without a verification forward and can stop drafting early; the pending
path loses those shortcuts and includes the pending token as another row.
Its draft steps grow from 682 to 827. This explains why removing a recovery
forward is not, by itself, a speed improvement. The second candidate repeats
the measured work and output. These are two-page pilot observations, not a new
full-corpus qualification. Phase times enclose queued work and are not additive
independent GPU kernel measurements.

For Antfly, the next optimization targets remain cheap verification/readout
and preserving useful target state across rounds. The reference demonstrates
that this E2B OCR workload can benefit from MTP; it does not justify copying
its unqualified low-precision matrix path. The subsequent dispatch audit
corrects the earlier head diagnosis: Q8_0 head storage is already preserved.
Default inference policies remain unchanged.

## Upstream research and short-row specialization

[The follow-up qualification](gemma4-ocr-q4-specialization.json) pins the new
runtime, source research and measurements. The research checkout is llama.cpp
`8bb3cf0d216a5ae48360179f2931daf284be4d01`. Its small-batch dispatch uses an
exact three-column tile. The proposed
[upstream multi-column kernels](https://github.com/ggml-org/llama.cpp/pull/29110)
share activation slices and weight blocks, specialize the column count, and
report gains on larger models. Our existing kernels already share weights;
the useful differences are register-sized accumulators and removing the padded
fourth column for three-row batches.

`TERMITE_METAL_ENABLE_Q4_0_SHORT_ROWS_SPECIALIZED=1` selects new two-/three-row
Q4_0 kernels for F32 input/output. They retain the existing accumulation order,
unroll fixed-size loops and use two SIMD groups for three rows, four for two.
The prior kernels remain the fallback when disabled or unavailable, and for
other row counts/formats. Existing small-batch/wide disable switches also
prevent this path. Borrowed FFN runtimes copy the new pipeline handles.
The four-row specialization was slower in the isolated probes and is excluded.
On the actual backend, eight alternating off/on runs with 16 resident weight
slots measured 33.43 versus 29.01 microseconds per operation for the two-row
1,536-by-6,144 FFN projection (13.2% less GPU time). Every enabled sample was
faster than every control sample, and output hashes matched. Three-row results
had overlapping/drifting timing ranges and are not performance-qualified.
Runtime teardown releases both new and prior wide-batch pipeline handles.

The existing operator benchmark now accepts `--planned-linear-scope`, grouping
independent linear operations into the decoder's tracked encoder scope. Use
`--weight-slots 16` to rotate resident weights and avoid relying solely on a hot
single matrix. Compare the same binary with the specialization switch at 0/1:

```sh
zig build inference-metal-bench -Doptimize=ReleaseFast -- \
  --mode linear --rows 3 --in 6144 --out 1536 \
  --warmup 4 --iters 15 --ops-per-frame 64 --weight-slots 16 \
  --planned-linear-scope
```

A separate prototype adapted the narrow 64-output-by-8-row matrix tile discussed
in [upstream issue 25250](https://github.com/ggml-org/llama.cpp/issues/25250).
At 1,536-by-6,144 with rotating weights it took about 44 microseconds per op,
versus 22/36/36 for the existing two-/three-/four-row kernels. FP16 input
narrowing also changed the outputs. This prototype is excluded from serving;
the larger-model proposal does not establish a gain at E2B's shapes.

The assistant protocol audit agrees with pinned upstream
[speculative decoding](https://github.com/ggml-org/llama.cpp/blob/8bb3cf0d216a5ae48360179f2931daf284be4d01/common/speculative.cpp)
and [Gemma4 hidden-state output](https://github.com/ggml-org/llama.cpp/blob/8bb3cf0d216a5ae48360179f2931daf284be4d01/src/models/gemma4.cpp):
fixed positions within a draft round, shared target KV, and post-final-norm
target hidden states.
The GGUF's tied `token_embd.weight` and Antfly's prepared vocabulary head are
Q8_0. A subsequent dispatch audit corrected the earlier BF16-head attribution:
the traced BF16 projection has dimensions 1,536 by 8,960 and belongs to PLE.
The Q8_0 MMV head dispatch covers 262,144 outputs. A Q6_K switch cannot optimize
this checkpoint's head, and original-head preservation is already implemented.

The latest complete corpus campaign exposes a broader parity limit: four-row
controls with specialization **disabled** also changed the diagram-page
transcript (4,016 versus 4,057 total tokens). Enabling specialization matched
one pass and changed the other. The earlier 40.139-second campaign remains a
record of that run, but does not establish repeatable transcript parity under
broader scheduling conditions. Both the matrix lane and this new kernel option
remain opt-in. The mismatch's numerical/scheduling cause is not yet isolated.

Full-corpus MTP output matched the ordinary control in both variants. Timing
swung substantially with unrelated compilation on the shared machine, including
roughly 17 to 48 seconds for identical two-page pilot work. These timings cannot
establish an additional end-to-end speedup. Kernel-level results, all measured
work/quality gates and the synthetic llama.cpp reference are recorded in JSON.
The installed llama.cpp reference (0.5.0, build 11146, `7fe450e19`) used the
same target GGUF with 310 synthetic prompt tokens and 128 generated positions
per sequence, at batches 1/2/3/4. Two runs observed 103–118, 178–204, 208–213,
and 215–235 generated tokens/second respectively. This excludes multimodal
preparation, HTTP, sampling and transcription accuracy, and uses shorter
contexts than much of the OCR corpus. It demonstrates small-batch decoder
scaling on this hardware; it cannot rank complete OCR pipelines.

Six focused checks pass with specialization enabled and again disabled,
including F64 linear references over rows 2/3/4/5, widths 32/512/1536, offset
inputs, a 257-column tail and zero rows; the FFN comparison now includes row 3.

## Parallel normalization and matrix scopes

[The next qualification](gemma4-ocr-parallel-qualification.json) measures the
follow-up on the same eight pages with two complete passes per configuration.
The matrix path was normalizing each hidden row serially on one GPU thread.
It now selects the existing parallel row RMS kernel for two-to-four-row
matrices with widths at least 1,024 when
`ANTFLY_GEMMA4_METAL_OCR_MULTIROW=1`. This also accelerates short MTP verification
matrices in that configuration. `TERMITE_METAL_DISABLE_SMALL_ROWS_NORM_REDUCE=1`
retains the previous normalization selection.

QKV projections, the shared attention-output/FFN block, PLE and final head now
use tracked compute scopes inside the existing frame. These reuse compute
encoders and retain the runtime's dependency barriers. The existing Metal
counter sampler profiles the frame without intermediate submissions or host
waits. A separate complete two-page diagnostic sampled a median 12.49 ms per
two-row step across 404 steps; the previous two-row smoke sampled about
39.5 ms. Stage sampling was disabled for the timed corpus runs. Some shared
block work remains attributed to `other`; these are not per-kernel roofline
measurements.

| Configuration | Median seconds, eight pages | Tokens per pass | Exact control text in both passes |
| --- | ---: | ---: | --- |
| Resident ordinary greedy | 54.501 | 4,057 / 4,057 | Yes |
| Matrix batching, two rows | 42.675 | 4,057 / 4,016 | No: one diagram-page repeat changed |
| Matrix batching, four rows | 40.139 | 4,057 / 4,057 | Yes |
| MTP K=2, canonical recovery and bonus | 63.273 | 4,057 / 4,057 | Yes |
| MTP K=2, deferred recovery, no bonus | 76.087 | 4,057 / 4,057 | Yes |

Four-row batching delivered 1.36 times the throughput of ordinary generation,
a 26.4% reduction in recognition time, while preserving all transcripts in
both passes. The previous-version control measured 74.382 seconds for four-row
batching and 79.470 seconds for bonus MTP, versus 40.139 and 63.273 seconds
with this patch. In the first measured MTP pass, verification fell from 36.993
to 20.103 seconds with identical 1,251 calls and 2,502 rows; draft and recovery
counts were identical too. The previous-version controls include the CPU
activity described below. The two-row variant still fails the strict parity/repeat gate.
MTP verification is cheaper, but MTP still loses to ordinary decoding on this
checkpoint and hardware. Experimental matrix batching and Metal MTP remain
opt-in; this corpus does not establish broader document/checkpoint coverage.

The recovery audit found that fully accepted drafts already reuse verified
hidden states. Corrections and bonus tokens still need their own KV written.
The existing deferred-recovery path folds this work into the next verification
batch, removing the separate forward but increasing verification work and
changing the assistant's donor context. Its complete-corpus result was slower
than canonical recovery with bonus acceptance, so it remains off. In the
first measured complete pass, canonical/bonus verification processed 2,502 rows
in 20.103 seconds and materialization took 21.626 seconds. Deferred recovery
removed materialization but processed 5,469 verification rows in 49.283
seconds; drafting grew from 12.881 to 17.760 seconds. Warmup phases are excluded. These are enclosing phase
wall times, not independent GPU kernel timings.

These timings include recognition preparation, prefill/decode and HTTP over
identical pre-rendered PNGs. PDF rasterization, model startup and image file
reads are excluded. The machine was not dedicated: unrelated CPU compilation
appeared during the second deferred-recovery pass and the previous-version
controls. The ordinary, new matrix and bonus-MTP cases completed before that
activity appeared. Source/executable pins, flags, quality proxies and phase
counters are recorded in the qualification JSON. Seven focused checks passed,
including independent F64 normalization references at the model dimensions
and frame/KV lifecycle checks.

## Implemented matrix batching and MTP follow-up

[Eight-page qualification](gemma4-ocr-matrix-qualification.json) records the
follow-up implementation. Earlier sections below describe the preceding
implementations and measurements.

The new prepared Metal decoder applies QKV projections, attention output,
feed-forward projections, PLE projections and the vocabulary head to a cohort
of rows. Each page retains its own position, RoPE and paged KV identity.
Embedding/PLE initialization is batched too. The Q4 feed-forward wrapper now
has a bounded two-to-eight-row route to the existing batched device encoder,
instead of silently looping over singleton calls.

A new frame-local KV suffix seed reserves capacity before acquiring its
encoder, handles F32/F16 storage and encodes directly on the same runtime as
the cohort. This avoids the generic paged-attention writer's intermediate
frame flushes. The final argmax returns the cohort's token IDs together.
All 1,314 measured multi-row decode steps in the eight-page four-row run
submitted exactly one frame. Singleton tails continue through the ordinary
optimized decoder. Cancellation/error cleanup retains the existing frame and
request ownership rules; unsupported KV formats/runtimes fail closed.

MTP's preprojection now joins its assistant/postprojection/selection frame.
`ANTFLY_GEMMA4_MTP_LONG_CONTEXT_FRAME=1` permits the existing fully owned
one-token decoder to return both the greedy choice and normalized hidden
state beyond the local-attention window. It retains the prior hidden-only
fallback when that route is unavailable; leaving the switch off preserves
the earlier long-context guard. This recovery path restored exact transcripts
with K=2 on the complete eight-page corpus, including bonus acceptance.

The original BF16 vocabulary-head attribution was incorrect; subsequent shape
and dispatch tracing identified that projection as PLE, with an existing Q8_0
vocabulary head. The earlier Q6_K switch did not control the head. The shared
BF16 linear kernel improvement applies to eligible BF16 projections: it keeps
two-to-four-row accumulators in registers and reduces with SIMD shuffles using
the existing binary addition tree. Larger row counts retain the prior path.
Tests cover small rows, tails, offsets and BF16 range.

| Configuration | Seconds, one eight-page pass | Completion tokens | Exact control text |
| --- | ---: | ---: | --- |
| Resident ordinary greedy | 54.012 | 4,057 | Yes |
| True matrix batching, four rows | 76.750 | 4,057 | Yes |
| MTP K=2, full-context recovery/draft frame | 88.222 | 4,057 | Yes |
| Same MTP K=2, bonus acceptance | 81.657 | 4,057 | Yes |

These are single complete passes with one page warmed outside measurement.
The final serving build and other CPU compilation overlapped the campaign;
these observations establish transcript agreement on the corpus and argue
against promotion, not a precise speedup estimate. The timed executable is
pinned to `65dbefeb706e1eb1c21aa57d3d5819ae067d3a90`; the final serving revision
removes only its disabled synchronized diagnostic. A diagnostic experiment
inserted intermediate waits and exposed an encoder-lifecycle assertion; that
mode was removed and its interrupted request is excluded from qualification.

Four-page pilots also showed that two-row matrix batching and forced MTP K=4
could shorten/change the diagram-page transcript. They failed the parity gate.
Four-row batching and K=2 passed all eight transcript checks, but their elapsed
times still failed the performance gate. Serving defaults therefore retain the
previous batch path and Metal MTP qualification policy.

To reproduce this historical matrix page path on current builds, additionally
set `ANTFLY_GEMMA4_METAL_OCR_MATRIX_ARITHMETIC=1`, then set
`ANTFLY_GEMMA4_METAL_OCR_BATCH=1`,
`ANTFLY_GEMMA4_METAL_OCR_BATCH_ROWS=4` and
`ANTFLY_GEMMA4_METAL_OCR_MULTIROW=1`. The last switch also selects batched Q4
feed-forward processing; `ANTFLY_GEMMA4_METAL_BATCHED_Q4_FFN=1` exposes that
route independently for backend qualification. The corpus MTP cases used
`ANTFLY_GEMMA4_MTP_MULTIMODAL=1`, disabled adaptive K, explicitly forced K=2,
enabled full-context recovery and
`TERMITE_METAL_ENABLE_DONATED_SLOT_ATTENTION_ON_FRAME=1`. Bonus acceptance
remains a separate switch. Complete flags and source/binary pins are in JSON.

`ANTFLY_GEMMA4_OCR_PROFILE=1` now logs `metal_ocr_matrix_frame` with row count,
actual submission count and wait/GPU duration, without extra synchronization.
The remaining throughput work is kernel efficiency: sharing weight reads alone
did not overcome this implementation's additional matrix/operator cost.

## Why the expected speedups have not appeared

The current result qualifies these implementations, not the potential of GPU
batching or speculative decoding. There are three different limits:

- Resident image preparation addresses only a small part of OCR time. In the
  paired resident run, decode took 43.416 seconds versus 6.335 seconds for image
  preparation. Even eliminating preparation entirely would save roughly 12%
  of the measured total; reducing its transfers cannot make decode faster.
- Coordinated page decode is still **one-row arithmetic inside a shared
  frame**. `runCoordinatedGemma4Rows` loops layers, then pages, invoking
  `runActiveGatedLayerFromAttentionInputDeviceMt` separately for every row.
  Embedding/PLE initialization runs separately before the frame; final
  RMS normalization and the vocabulary head also run separately for each
  page after it. This adds copies, dispatches and synchronization without
  explicitly sharing weight loads/dequantization across input rows. Local
  cache reuse is possible, but a multi-row matrix kernel is needed to obtain
  the expected bandwidth benefit. Increasing scheduler occupancy alone
  cannot resolve this limitation.
- MTP saves target rounds only if drafting, batch verification and KV/hidden
  recovery together cost less than the ordinary target steps they replace.
  The measured K=2 run accepted 839 tokens in 543 rounds: about 1.55 tokens
  per round, not two. That includes correction tokens; the draft-match rate
  alone does not describe speed. Metal bonus acceptance defaults off. With
  K=1 and no bonus, each round emits at most one token, so drafting cannot
  amortize the number of target steps at all.
  Corrections can require another target forward, and past the 512-token
  sliding window the active hidden-materialization guard selects the
  existing hidden-only route. Ordinary greedy generation retains its
  optimized singleton path throughout these pages.

The MTP phase counters are now available in logs with
`ANTFLY_GEMMA4_MTP_PROFILE=1`. `mtp_phase_profile` reports drafting,
verification, materialization and activation-copy wall times and counts;
`mtp_draft_profile` breaks down assistant stages. Leave
`ANTFLY_GEMMA4_MTP_PROFILE_SYNC` disabled for performance comparisons: the
logging reads existing counters and does not add GPU synchronization.
These are host-observed phase times, not a Metal kernel trace, and nested
draft-stage times must not be added to their enclosing draft total.

### Follow-up controlled experiments

[Phase timings and transcript checks](gemma4-ocr-bottlenecks.json) pin the
diagnostic source revision and executable SHA-256. These tests use the first
two Attention pages, fresh servers, one page warmup, and two measured passes
per configuration on one frozen executable. The diagnostic build completed
before these comparisons; only one benchmark server used the GPU at a time.
Small differences between consecutively run cases remain subject to drift.

| Configuration | Median seconds, two pages | Completion tokens/pass | Exact control text |
| --- | ---: | ---: | --- |
| Resident ordinary greedy | 11.211 | 841 | Yes |
| MTP K=2, existing defaults | 22.609 | 841 | No |
| MTP K=2, enable Q6_K two-row head switch | 22.751 | 841 | No |
| MTP K=2, accept bonus tokens | 21.182 | 796 | No |
| MTP K=2, defer materialization and reuse target activation | 24.459 | 841 | Yes |
| Deferred MTP K=2 plus two-row head switch | 24.737 | 841 | Yes |

Ordinary decode took 9.134 and 9.226 seconds. The first default MTP pass spent
4.844 seconds drafting, 9.103 verifying and 6.511 materializing corrections,
plus 0.035 copying activations. It made 345 separate correction
materializations. All 841 draft steps were resident, with no host draft steps
or target-embedding cross-backend copies. GPU residency in the draft is
already present; the extra target work and assistant computation dominate.

`TERMITE_METAL_ENABLE_Q6_K_R2_REDUCE=1` permits an existing two-row shared-weight
Q6_K head kernel on eligible routes/shapes. Enabling the switch did not produce
an end-to-end gain; this test does not assert that every head operation selected
that kernel. Its source dispatch requires the switch; the prepared multi-row head helper's
comment now states that requirement explicitly.

`ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1` reduced target rounds, but required 150 bonus
materializations in addition to 256 correction materializations. Recovery took
7.706 seconds, and output shortened and changed. It therefore fails both the
ordinary-runtime comparison and exact-transcript qualification.

Together, `ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE=1` and
`ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE_TARGET_ACTIVATION=1` removed the separate
materialization phase and matched all four measured pilot transcripts. They
also increased verification from 596 to 1,219 target rows per pass and from
298 to 448 calls. Verification alone took 16.986 seconds, with 5.352 seconds
drafting. The transcript match is useful qualification evidence on these two
pages, but does not establish correctness on the full corpus or a speedup.
Enabling the two-row head switch on that deferred path also retained the
pilot transcripts and gave no timing improvement.

The [speculative decoding paper](https://arxiv.org/abs/2211.17192) and
[llama.cpp documentation](https://github.com/ggml-org/llama.cpp/blob/master/docs/speculative.md)
describe verification that amortizes a target forward across multiple tokens.
Our current failure to amortize enough work is an implementation/qualification
problem; it does not establish that this approach cannot help Gemma4 OCR.

The next implementation priority is a true multi-row prepared decoder:
batch the QKV and feed-forward linear operations and vocabulary head, retain
per-page paged attention and KV identities, and read all token choices after
one owned frame. Qualify two rows before increasing the active set. For MTP,
first restore agreement with ordinary greedy generation at the first divergent
token, then improve verifier and correction-materialization cost. Keep the
existing sliding-window guard until a safe optimized hidden-state route is
qualified across that boundary. Neither output shortening nor a changed
transcript counts as an exact-control speedup.

This follows the [OCR comparison](gemma4-ocr-comparison.md), using the same
Gemma4 E2B Q4_0 checkpoint, Q8_0 image projector and eight PDF pages. It addresses
four separate questions: runtime efficiency, independent-page decode batching,
speculative decoding and resident image-to-prompt preparation. These are text
generation measurements; EmbeddingGemma2 and CLAP embedding results are separate.

## Runtime control and profiling

The installed llama.cpp build is 11146 (`7fe450e19`, Homebrew 0.5.0). It consumes
the identical model, projector, PNGs, transcription prompt, temperature zero and
2,048-token ceiling, with Metal offload, Flash attention and reasoning disabled.
`--image-max-tokens 280` matches Antfly's 310 prompt tokens on the technical and
modern report pages; scanned pages use 317. Leaving llama.cpp's image budget at
its default produced 844 prompt tokens, so that pilot is excluded.

Both llama.cpp passes produced identical transcripts to one another, but they
did not match Antfly. The diagram/table page reached the output ceiling in both
passes. Mean normalized word F1 against the PDF text layer was 0.847, compared
with Antfly's 0.912. The first pass decoded around 81–99 tokens/s by llama.cpp's
own decoder timing. Whole-corpus times were 64.460 and 83.986 seconds. Other local
compilation jobs were active, and these passes generated more tokens than
Antfly; they do not establish an end-to-end runtime speedup.

`ANTFLY_GEMMA4_OCR_PROFILE=1` records actual optimized decode attempts, hits and
fallbacks, command-frame submissions, wait/GPU durations and tensor host-mirror
download counters. It reads counters without forcing additional synchronization.
Serialized requests take before/after snapshots. Coordinated decode reports
actual row occupancy under its execution mutex instead of racing per-request
snapshots on a shared backend. Tiny raw token-result downloads are outside the
tensor host-mirror counter, so that counter is not total PCIe/unified-memory
traffic.

## Resident generative image preparation

Previously the GGUF projector exported its visual features to a host float
array. Prompt assembly separately downloaded language embeddings, scaled and
overwrote them on CPU, then uploaded the entire prompt again.

The new path prepares bounded image geometry cohorts and returns an owned
`ResidentProjectedImages` tensor. Prompt assembly shares the original marker
validation and PLE padding rules, performs embedding scaling on the backend and
copies visual rows into the resident prompt. Mixed audio features can be uploaded
once into that prompt. The prefill consumes the prompt directly. Features and
weights retain their normal request/model ownership and execution control.

Backends must expose row concatenation, row copying and scalar multiplication.
`ANTFLY_GEMMA4_GENERATION_IMAGE_HOST=1` restores the host path for comparison.
This changes the generative GGUF path; the existing HF embedding path already
keeps these features resident.

The paired control uses one frozen executable, fresh server instances and two
complete eight-page passes per case, with one page warmed outside measurement.
Host packing took 51.626 and 51.982 seconds; resident packing took 51.391 and
51.364 seconds. Their medians are 51.804 and 51.377 seconds, a 0.8% difference
that should not be treated as a substantial throughput gain. All 32 measured
transcripts matched the earlier control exactly and completed normally.

Tensor host-mirror downloads fell from 4,587,520 bytes on technical/modern pages
and 4,673,536 bytes on scans to 1,048,576 bytes per page. The remaining download
is the final prefill vocabulary row. In the second corpus passes, preparation
summed to 6.746 seconds on the host path and 6.335 seconds resident, while decode
summed to 43.603 and 43.416 seconds. These timings support a preparation/transfer
improvement, with autoregressive decoding dominating overall cost.

## Coordinated independent-page decode

`ANTFLY_GEMMA4_METAL_OCR_BATCH=1` enables an experimental GGUF Gemma4 Metal batch
lane. `ANTFLY_GEMMA4_METAL_OCR_BATCH_ROWS=2` or `4` sets its bounded active set.
The ordinary serving defaults stay serialized. Groups above the active-set
ceiling retain their ordinary execution path.

One shared backend holds the outer model mutex and provider lease for the whole
batch. A distinct step mutex protects projector, prefill and decode operations
within that batch. Resource admission reserves all live KV caches and scratch,
instead of the maximum per-item reservation used by serial execution.

Each page has its own paged KV identity and position. Projector and image prefill
operations remain serialized. The model-specific scheduler policy allows mixed
positions and bounded coalescing; CUDA retains its existing homogeneous C2
policy. Completed pages leave the coordinator promptly.

The prepared decoder processes page rows layer by layer within one owned frame,
using the same quantized singleton kernels and private hidden/PLE carriers. This
keeps weight reuse local to a layer and preserves singleton arithmetic. Final
norm/head selection also uses the singleton route; only scalar token IDs return
to request workers. There is no hidden-state or vocabulary-logit export during
coordinated greedy decode. Ordinary singletons keep their established optimized
path. Errors after KV writes fail the step rather than replaying it as a fallback.

An eager compatibility path remains for unsupported or nongreedy work.
`ANTFLY_GEMMA4_METAL_OCR_EAGER=1` selects it explicitly for A/B diagnosis. Per-layer
frames coalesce its dispatches, and mixed paged-attention outputs use device row
copies instead of host float arrays. The eager pilot still failed qualification:
four pages took 144 seconds at batch 2, with one shortened transcript, and 493
seconds at batch 4, with very low coalescing occupancy. Merely routing work to
batched eager matrix operations was insufficient.

The prepared-kernel four-page pilot restored all control transcripts and reduced
those eager times to 27.0/25.4 seconds, near serial cost. The final eight-page
qualification also corrects coalescing readiness for unequal positions.

| Path | Eight-page passes (seconds) | Median | Exact control transcripts |
|---|---|---|---|
| Host prompt packing, serial | 51.626, 51.982 | 51.804 | Yes |
| Resident prompt packing, serial | 51.391, 51.364 | 51.377 | Yes |
| Prepared cohort, two rows | 55.506, 55.709 | 55.608 | Yes |
| Prepared cohort, four rows | 54.648, 56.791 | 55.719 | Yes |

All 64 Antfly transcripts completed normally, with the same 4,057 completion
tokens per corpus and mean word F1 proxy of 0.912. Two-row coordinated steps
had occupancy `{1: 18, 2: 3156}` across both passes. Four-row steps had
`{1: 698, 2: 1549, 3: 644, 4: 379}`. These histograms count coordinated decode
steps, excluding the direct singleton tail after peers finish. They establish
real two-/four-row execution rather than inference from envelope size.

The cohorts are 8.2%/8.5% slower than resident serial generation on this corpus.
They execute prepared singleton matrix-vector kernels layer by layer; they do
not yet share weight loads through a fused multi-row quantized matrix kernel.
Extra embedding/PLE preparation, row carriers, scalar head selection and
scheduling offset local weight reuse. Correctness and bounded residency passed;
throughput promotion did not. The ordinary serving defaults remain serial.
Further performance work should target the quantized two-/four-row kernel and
batched device token handoff, retaining these transcript and occupancy controls.

## Speculative image OCR

The compatible draft evaluated here is
[Google's E2B assistant](https://huggingface.co/google/gemma-4-E2B-it-assistant),
revision `2d874ef7d29f9a30599a1e4b3c1cbc9595f005df`, downloaded as its original
SafeTensors checkpoint. Its 157,565,344-byte weights are separate from the target
GGUF model; the target tokenizer remains authoritative.

Antfly previously rejected every multimodal speculative request. The experimental
`ANTFLY_GEMMA4_MTP_MULTIMODAL=1` lane admits a Gemma4 MTP assistant only with a
qualified Metal GGUF target and the final-hidden activation contract. Image
prefill retains the last post-norm hidden row to seed the assistant and leaves
the image-derived target KV cache intact. If that hidden state cannot be
captured, the request fails rather than recreating a text-only prompt from image
placeholder IDs. Target verification remains responsible for accepted tokens.

The final warm sweep uses two paper pages (PDF pages 1 and 3), one warmup page,
one measured two-page pass per K and adaptive K disabled. K is a cap: a cached
first-token rejection can stop drafting early. These smaller tests diagnose MTP;
they do not establish full-corpus document throughput.

| Draft cap | Two-page seconds | Matched/drafted | Acceptance | Exact control transcripts |
|---|---|---|---|---|
| 1 | 21.051 | 464/798 | 58.1% | 0/2 |
| 2 | 21.919 | 494/841 | 58.7% | 1/2 |
| 4 | 22.990 | 500/1055 | 47.4% | 1/2 |

Ordinary recognition of those pages took about 11 seconds in the paired control.
All speculative outputs finished normally, but every K failed exact transcript
parity and throughput promotion. K=1 produced 408/390 completion tokens versus
405/436 in the control; K=2/4 produced 405/436 but changed wording on the first
page. The configured K must not be confused with realized draft length, and
accepted-token counts include target corrections; the acceptance column uses
matched draft tokens only.

The sweep also exposed a pre-existing long-context K=1 failure: active hidden
materialization intentionally declines beyond the local attention window, but
its caller treated that decline as an unsupported backend. The caller now uses
the existing full one-token hidden path and retains KV rollback on actual
errors. Its fallback has an allocation/hidden-value test and the sweep completes
past the window. A corrupt image remains a permanent per-item failure; a sibling
page keeps its complete transcript and original index.

The MTP path stays experimental. Existing calibration requirements and Metal
automatic-policy defaults remain in place. A compatible assistant and substantial
draft acceptance alone do not establish a speed or quality win.

## Reproduction

Use the model files, corpus and raster-preparation method in the original report.
The benchmark server requires the larger host/media budget from that report.
Keep GPU jobs exclusive and record other host activity. Freeze the executable
after the build completes; retain its checksum and source revision/diff.

The OCR driver now supports a matched llama.cpp control and explicit drafts:

```sh
python3 zig/pkg/inference/tools/bench/pdf_ocr.py \
  --runtime llama --base-url http://127.0.0.1:18098 \
  --model gemma4 --corpus "$corpus" --output-dir "$out/llama" \
  --batches 1 --iterations 2

python3 zig/pkg/inference/tools/bench/pdf_ocr.py \
  --model "$target" --draft-model assistant --speculative-k 2 \
  --corpus "$corpus" --output-dir "$out/mtp-k2" \
  --batches 1 --iterations 2
```

The driver preserves complete transcripts, identities, finish reasons, token
usage and speculation status. `pdf_ocr_optimization_report.py --case NAME=JSON`
compares complete cases against the first transcript control and checks output
order, repeat equivalence, truncations and word coverage. PDF text layers remain
noisy proxy references, especially for the historical scans; these scores do not
measure reading order, tables or manually labelled character error rates.

## Validation and provenance

The final runtime qualification uses `b27e83321918a5396e915e58bd29f3294cee283f`,
Zig 0.17.0 ReleaseFast and executable SHA-256
`3fabeab275df026fecc9925d7d9e7fb59ead44c6d910a81ddf5173f97abd4276`.
The paired host/resident control was frozen before the final scheduler/MTP
corrections, at SHA-256
`ccd2c55d036106ec152df6f8c55bd22f45f445dcfdcc83caba7ed9e880d6c232`;
those corrections do not change the serial image packing path. Its base source
was `63e995d19d` plus the recorded prepared-row/ragged-policy patch. Raw inputs,
outputs, server flags, full logs and intermediate failed pilots are retained
locally under `.benchmark-results/gemma4-ocr-optim-2026-10-11/`.

Eighteen focused checks pass, covering model/step lock separation, all-live KV
admission, mixed-position scheduling/coalescing with the CUDA gate unchanged,
Metal mixed-attention parity, per-page choices/publication order, prompt tensor
cleanup, unavailable image-hidden rejection, canonical MTP fallback and permanent
image versus transient allocation failures. Real-model qualification adds full
ordered transcripts and actual occupancy at both two and four rows. Formatting,
license headers, Python checks and the serving build pass. CUDA, E4B and audio
text-generation throughput are not qualified by these OCR runs.
