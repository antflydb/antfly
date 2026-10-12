# Gemma4 generative OCR optimization

[Compact results](gemma4-ocr-optimization.json).

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
