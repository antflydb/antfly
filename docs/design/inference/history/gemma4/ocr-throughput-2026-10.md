# Gemma4 OCR throughput investigation, October 2026

> Summary of a dated investigation. Full text: [gemma4-ocr-optimization.md@144f7dc](https://github.com/antflydb/antfly/blob/144f7dc49c17bb4b9b747b58d8ae2a81b918f3e6/docs/design/inference/gemma4-ocr-optimization.md). Living contracts, controls and current qualification: [OCR optimization](../../gemma4-ocr-optimization.md). Original headings are retained below for historical anchors.

Outcome: a canonical Q4 cohort path with batched tails/QKV, reusable buffers and
one additional head frame reduced eight-page recognition time by 12.8% locally.
Matrix arithmetic and speculative decoding remain unqualified for promotion.
All times are source-pinned observations on an M4 Max/36 GiB, not an SLA.

## Clustered drafting, recovery and continuous cohorts

GPU clustered scoring, stable top-cluster selection, candidate scoring and ICB
replay were implemented for the BF16 assistant; target verification still uses
the full head. A repeated fixed-vector probe reduced selector GPU time from
750.45 to 302.45 µs with identical choices. That is an isolated selector result,
not a full-head or OCR speedup. Recovery snapshot avoidance and scratch-envelope
reservation are independent controls. Continuous windows admit up to sixteen
pages with four-row GPU cohorts inside one explicit request.

### Whole-request qualification

At `849a025887`, ordinary eight-page recognition took 57.816/56.257 s.
Cluster/replay/recovery/scratch took 64.163/64.738 s, overlapping the later MTP
control and losing to ordinary. Scalar continuous cohorts preserved output but
were slower; matrix cohorts took 34.296/34.639 s but omitted 41 tokens in one
pass. EOS confirmation in a passing diagnostic replaced no stops and did not
qualify a remedy. [Cluster evidence](../../gemma4-ocr-clustered-head.json),
[continuous evidence](../../gemma4-ocr-continuous-cohorts.json).

## Paragraph omission: free-running trace and serving correction

Four copies of Attention page 6 exposed a premature EOS at sequence length
1,011: ordinary newline margin +0.0032997 became an EOS preference of 0.0056114.
The result was an exact prefix, 204 characters/41 tokens short, with normal stop
status. Scoped QKV/block/PLE/tail rollbacks still failed. The older QKV rollback
used standalone projections rather than ordinary's fused Q + paired K/V family.
Correcting that rollback alone was insufficient. Runtime `8ed689d285` retained
the entire singleton family by default and matched 72 page observations/41,428
tokens, including memory instrumentation. Matrix arithmetic acquired a separate
explicit opt-in. No fresh ordinary brackets qualified those corrected timings.
[Correctness evidence](../../gemma4-ocr-paragraph-correctness.json).

## Fixed-prefix parity and stage isolation

Layer diagnostics first differed at Q projection (up to 1.22e-4); complete
local layer differences reached 1.48e-3. Two full canonical-prefix passes checked
7,800 choices without differences, including the fragile newline/EOS boundary.
Restoring canonical history concealed accumulated drift; these passing traces
were not free-running qualification. The initial packed tail asserted or changed
output; it was replaced by canonical singleton tail work after the backbone.
Metal copies were also corrected to use their explicit runtime and honor queued
shared-buffer producers. Those fixes did not isolate the original omission's
kernel cause. [Parity investigation](../../gemma4-ocr-parity-investigation.json).

### Larger inline media envelopes

The sixteen-page base64 payload exceeded the default 10 MiB HTTP body cap;
that reset happened before GPU decoding. An explicit 32 MiB cap enabled testing,
independent of KV/window admission. Ordinary OCR on the first expanded corpus
repeated a diagram to the cap; the qualified sixteen-page text corpus instead
contains 9,126 completion tokens. The diagram failure remains separate evidence.

### Repeated cohort/window qualification

Four-row/eight-window canonical-QKV matrix cohorts initially matched ordinary,
expanded and reversed corpus runs and appeared ~27% faster in one bracketed
campaign. The allocation probe later reproduced the omission (9,085 vs 9,126
tokens). Two-row and smaller-window configurations also failed order/output
gates. Neither a scoped rollback nor a larger window established robust parity.
The apparent matrix gains are superseded as serving claims.

### Fixed-K and synchronization calibration

K=1/2/3/4 on two Attention pages all preserved 841 tokens but took 15–17 s
against ordinary 12–13 s. K=2 head/input/tail frames did not win. Cached seed +
legacy reductions + head frame passed a later eight-page check at 68.715/66.461 s,
but overlapped ordinary brackets, so no full-corpus MTP speedup was qualified.
Known target seed choices must not count as independent draft accuracy.

### GPU allocation diagnostics and final validation

Worker Metal allocations are sampled after waits; process-tree RSS can
include shared mappings more than once. These domains overlap and cannot be
added. Allocation-instrumented times are excluded, while output failures remain
valid evidence. Earlier parent-only RSS estimates are excluded.

## Verification framing and vocabulary-head audit

Input/tail framing cut target submissions 4,601→3,350 (27.2%), one per
verification call, with all 4,057 tokens preserved. Candidate 63.446/65.822 s
overlap the later MTP controls; ordinary was 54.106/54.585 s. Fewer submissions
did not establish a speed win. The target head is Q8_0; the traced BF16
1,536→8,960 projection is PLE, not the 262,144-output head.
[Framing evidence](../../gemma4-ocr-verification-framing.json).

## Cached target prefixes: reduced work, failed full-corpus parity

Trusted target seeds cut assistant steps by 53.6% and recovery forwards by
24.2%, while verification rows rose 18.3%. Both full-corpus passes omitted the
same paragraph, including framing-disabled probes. Seven matching pages were
a post-hoc restricted scope, not a full-corpus speed gate. The original seeded
lane remains off. [Seed evidence](../../gemma4-ocr-cached-target-seeding.json).

## MLX MTP reference and pending-token follow-up

Pinned MLX (`fb4b1e2f`, MLX 0.32.3) used affine 4-bit/group-64 weights,
not Antfly's Q4 GGUF. Original-precision MTP changed text/repeated to the cap;
singleton verification alone did not fix all pages past the sliding window.
F32 floating parameters/scales restored token parity. Eight pages took
50.292→42.395 s but included a capped page; seven completed pages took
31.723→29.575 s (6.8% reduction) with overlapping ranges on a shared machine.
This cannot rank the runtimes. Antfly pending-token verification with bonus
was slower on the two-page pilot (16.52 s vs canonical MTP 13.54–13.82 s and
ordinary 11.31 s): removing recovery increased verification rows/calls and
lost cached-choice shortcuts. [MLX evidence](../../gemma4-ocr-mlx-reference.json).

## Upstream research and short-row specialization

Pinned llama.cpp research motivated exact two-/three-row Q4 specialization.
A rotating-weight two-row FFN probe improved 33.43→29.01 µs (13.2%) with matching
hashes; three-row ranges overlapped and a four-row prototype was excluded.
A narrow tile was slower and changed output via F16 narrowing. Full OCR with
specialization disabled still omitted text, so this kernel was not the isolated
cause. Synthetic llama.cpp decode scaled with batches but excluded images,
HTTP, sampling and OCR accuracy. [Specialization evidence](../../gemma4-ocr-q4-specialization.json).

## Parallel normalization and matrix scopes

Parallel small-row RMS and tracked scopes initially reduced four-row corpus
time to 40.139 s vs ordinary 54.501 s, with two matching passes. Subsequent
matrix runs changed a transcript, including with specialization disabled.
The earlier result is conditional historical evidence, not repeatable output
qualification. Canonical/bonus MTP and deferred recovery remained slower.
[Parallel evidence](../../gemma4-ocr-parallel-qualification.json).

## Implemented matrix batching and MTP follow-up

Prepared projections span rows while each page keeps independent KV/RoPE;
frame-local KV suffix publication removed intermediate flushes. All 1,314
measured multirow steps in one run submitted one frame. That early matrix run
was slower (76.750 s vs ordinary 54.012 s) under compilation contention.
Long-context canonical MTP recovery restored output but also lost the speed gate.
[Matrix evidence](../../gemma4-ocr-matrix-qualification.json).

## Why the expected speedups have not appeared

Resident packing improves transfers in a small preparation fraction;
autoregressive decode dominates. Layer-coordinated singleton MMVs do not
explicitly share weight loads and add carriers/dispatches/synchronization.
Speculation only helps when drafting, verification and recovery together cost
less per committed token than ordinary steps. Occupancy and acceptance alone
are insufficient evidence.

### Follow-up controlled experiments

Two-page ordinary took 11.211 s; default MTP 22.609 s changed output.
Deferred recovery preserved output but took 24.459 s: separate materialization
vanished while verification rows rose 596→1,219 and calls 298→448. Bonus
acceptance shortened output. A Q6 head switch could not address this target's
Q8 head. [Bottleneck evidence](../../gemma4-ocr-bottlenecks.json).

## Runtime control and profiling

Matched-model llama.cpp OCR produced different transcripts and capped a
page; its 64.460/83.986 s corpus times were not matched-work runtime rankings.
The default image token budget also produced a larger prompt; that pilot was
excluded. Non-synchronizing hit/frame/occupancy counters distinguish native
execution from envelope batching. Scalar token reads are outside mirror-byte
counters. [Initial evidence](../../gemma4-ocr-optimization.json).

## Resident generative image preparation

Resident prompt packing reduced per-page tensor mirror downloads from
~4.59–4.67 MB to 1 MiB (remaining prefill vocabulary row). Recognition changed
51.804→51.377 s (0.8%); decode still took ~43 s while preparation took ~6 s.
All 32 measured transcripts matched. Transfer savings did not imply a large
OCR throughput gain.

## Coordinated independent-page decode

Initial prepared two-/four-row cohorts matched all outputs but were about
8% slower than resident serial (~55.6–55.7 s vs 51.4 s). Occupancy counters
confirmed actual cohorts. Earlier eager pilots were far slower and changed
output. These steps justified prepared arithmetic and later tail/head work,
not promotion based on envelope size.

## Speculative image OCR

The initial K=1/2/4 image MTP pilots each failed text parity and took
21–23 s vs ordinary ~11 s for two pages. Long-context hidden fallback was fixed
so the pilot could finish; residency and compatible assistant weights alone did
not qualify speculative performance.

## Reproduction

The live [OCR design](../../gemma4-ocr-optimization.md#reproduction) documents
current controls. Frozen binaries, exact flags, model/corpus hashes and complete
outputs remain in the linked JSON reports. Raw artifacts are local under
`.benchmark-results/gemma4-ocr-optim-2026-10-11/`. Never compare unlike workload,
precision or instrumented timing as matched throughput.

## Validation and provenance

Historical tests/build counts apply to their pinned runtimes, not to a
later merge. Native/Metal fixtures cover residency, row identity, KV publication,
frame/copy ownership and failure cleanup. CUDA, E4B and audio text-generation
throughput were not qualified by this OCR campaign.

## Singleton-invariant Q4 cohort dispatch

Using singleton projection/gate-up kernels alone still omitted text;
retaining canonical attention tails at `874788fba0` restored the gates.
The final invariant-only campaign matched 104 page observations/58,668 tokens,
but original-corpus timing ranges failed the speed gate. Complete operation
families matter beyond matrix multiply. [Invariant evidence](../../gemma4-ocr-batch-invariant.json).

## Shared Q4 tiles, batched tails and carrier reuse

Runtime `7f4d7fa577` kept invariant arithmetic while batching QKV/tails,
reusing carriers and grouping canonical heads. The full eight-page `head` case
took **48.819/49.304 s vs ordinary brackets 55.339–57.572 s**: 12.8% less mean
time, 14.6% more throughput, all 4,057 tokens exact. Pool without head framing
saved 6.6%. Shared weights were disabled: 48 matching operator hashes mostly
showed slower sharing; widened accumulators were rejected for 16-ULP changes.
The fixed four-page, forward/reverse sixteen-page and memory conditions matched
output but lack fresh timing controls. Sampled Metal/RSS maxima were 3.710/3.457
GiB, overlapping resources; probe timing excluded. Diagnostic counts reached
8 carrier allocations/135,160 reuses. Across recorded native conditions,
128 page observations/79,222 tokens passed. The final runtime passed 66 selected
experiment-enabled tests, 101 default compiled tests and nine sweep tests.
The earlier six-case repeated-page campaign's varying load invalidated its
timings; correctness evidence was retained. [Final evidence](../../gemma4-ocr-shared-batching.json).

