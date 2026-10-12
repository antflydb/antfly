# Gemma4 generative OCR optimization

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

The initial resident run completed all 16 measured transcripts identically to
the original control, with no truncation. Eight-page times were recorded in the
local raw report; their median was 56.715 seconds. The original control median
was 59.638 seconds, but a paired host-path run is needed to isolate this change
from host timing drift. Resident tensor host-mirror downloads were 1,048,576
bytes per page, solely the final prefill vocabulary row.

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
operations remain serialized. The scheduler groups ready decoder rows with
independent sequence lengths, batches language-model matrix operations and uses
device row argmax for eligible greedy requests. Per-layer Metal frames coalesce
the eager dispatches, and mixed paged-attention results are assembled with device
row copies instead of host float arrays. Finished pages leave the coordinator
promptly so a remaining singleton can use the direct decoder. Only token IDs return to the
request workers. Mixed or nongreedy steps retain the full-logits compatibility
path. The existing optimized singleton Metal decoder remains separate from
this batched eager path; a function named `decoderRuntimeDecodeBatch` alone did
not provide independent-page batching.

Promotion requires complete ordered transcripts at both two and four rows,
observed multi-row decode occupancy, bounded memory, per-item failure isolation
and a measured throughput improvement. An HTTP envelope size is not evidence
that a native decoder batch executed.

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

The first full-transcript pilot matched ordinary OCR exactly, but took 14.388
seconds including first assistant/model setup versus roughly 5.9 seconds for
ordinary generation on that page. Warm two-page repeats then took 24.519 and
24.506 seconds. The paper's third page matched the control; its first page
changed “state-of-the-art score” to “state of the art BLEU score” in both repeats.
Those warm measurements fail the speed and exact-transcript promotion gates.
This path stays experimental. Existing calibration requirements and Metal
automatic-policy defaults remain in place. Draft acceptance and realized lengths
must accompany any subsequent tuning; the configured K alone is not the
realized adaptive draft length.

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
