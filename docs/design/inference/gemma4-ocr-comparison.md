# Gemma4 OCR text generation versus Apple Vision

[Compact measurement report](gemma4-ocr-comparison.json).

The follow-up [runtime, resident prompt, decode batching and MTP investigation](gemma4-ocr-optimization.md) uses this pinned corpus and keeps experimental generation paths separate from these baseline measurements.

This compares **OCR text generation**, rather than PDF/image embeddings. The
Gemma4 model is the available multimodal `gemma-4-E2B-it-Q4_0.gguf`, with its
matching Q8_0 projector. These results do not transfer to the HF
`google/embeddinggemma-2` image embedding measurements in this PR.

## Corpus and timing

Eight distinct pages selected from three real PDFs:

- [Attention Is All You Need](https://arxiv.org/pdf/1706.03762), PDF pages 1, 3
  and 6: dense technical text, a diagram, equations and a table.
- [NIST's FY2021 Cybersecurity and Privacy Annual Report](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-220.pdf),
  PDF pages 5, 6 and 9: modern report typography, photos and text.
- [1907 Bureau of Standards annual report](https://nvlpubs.nist.gov/nistpubs/Legacy/MP/nbsmiscellaneouspub27.pdf),
  PDF pages 8 and 12: historical scans with yellowed paper and dense serif text.

Poppler renders every page once at 150 DPI, capped at 1,536 pixels on its long
edge. Both engines consume those identical raster pixels. The model's own image
processor subsequently resizes them according to its configured vision geometry.
Rendering, disk reads and model loading are outside recognition timing.

Gemma4 uses `/ai/v1/generate/batch`, request batches 1/2/4/8, temperature zero,
`enable_thinking=false`, a 2,048-token output ceiling and a transcription-only
prompt. Every case warms one page, then processes the complete eight-page corpus
twice. Timings include HTTP/base64 decoding, image preparation, prefill,
autoregressive generation and complete text response. Every page retains its
identity, full transcript, token usage and finish reason in the local raw report.

Apple uses the repository's existing `antfly_vision_read` adapter on borrowed
RGBA pixels, accurate and fast recognition, language correction enabled and
`en-US`. Each mode warms two whole-corpus passes and measures three. PNG decoding
and RGBA preparation are outside recognition timing. The different input
transport means small HTTP/decode costs remain in Gemma4's measurement; it is
not a pure-kernel comparison.

Apple's [Vision text recognition](https://developer.apple.com/documentation/vision/recognizing-text-in-images)
is allowed to choose its compute devices. The benchmark does not force CPU-only
operation or claim a specific GPU/Neural Engine assignment.

The shared eight-page Poppler preparation took 5.835 seconds at the median
of three runs (5.700, 5.835, 6.148 seconds). This uses a separate subprocess per
selected page and includes parsing, rasterization, PNG encoding and filesystem
output. It is a reproducible corpus-preparation measurement, not Antfly GPU
renderer throughput. Adding it to warm recognition gives **serial work
estimates** of 65.5 seconds for initial Gemma4 batch 1 and
7.9 seconds for Apple accurate (~8×), without overlapping stages.
They are not independently measured end-to-end ingestion latencies.

## What batch size means here

The current Antfly multimodal generation path executes GPU requests under
`BatchExecutionMode.shared_serial`; it performs each item's projector, prefill
and decode before moving to the next. Its continuous decode batching gate only
qualifies opted-in CUDA text-only requests, excluding multimodal input. Increasing
the HTTP envelope does **not** create a native Gemma4 OCR batch on Metal.

The Apple adapter also serializes recognition with its existing mutex. Its
baseline is per-page recognition, not an invented native Vision batch. This
experiment tests the shipping paths. A future native multimodal decoder batch
would need separate qualification; image embedding batching does not establish
its performance or transcript equivalence.

## Quality checks

Tokenize normalized text into words, case-fold and join hyphenated PDF line
breaks. Compute multiset precision/recall/F1 against `pdftotext -layout`. This
checks text coverage while tolerating different whitespace and table formatting;
it does not score reading order or reconstructed tables. The scanned report's
embedded OCR text layer is imperfect, so its score is agreement with a noisy
reference, not ground-truth accuracy. No manual CER/WER claim is made.

Check Gemma4 transcripts across repeats and request batch sizes, page ordering,
per-item failures and output token caps. Report missed text even when the model
finishes normally; `finish_reason=stop` does not establish a complete transcript.

## Warm recognition results

Apple M4 Max, 36 GiB, macOS 27.0.1. Times below are the median total for the
same eight pages; Gemma4 has two measured passes, Apple three.

| Engine / request batch | Eight-page time | Pages/s | Mean word F1 |
| --- | ---: | ---: | ---: |
| Gemma4 E2B Q4_0 / 1 | 59.638 s | 0.134 | 0.912 |
| Gemma4 E2B Q4_0 / 2 | 67.508 s | 0.119 | 0.912 |
| Gemma4 E2B Q4_0 / 4 | 73.911 s | 0.108 | 0.912 |
| Gemma4 E2B Q4_0 / 8 | 73.216 s | 0.109 | 0.912 |
| Apple Vision accurate / 1 | 2.087 s | 3.834 | 0.973 |
| Apple Vision fast / 1 | 3.234 s | 2.473 | 0.678 |

Gemma4 transcripts match exactly across repeats and all request batch sizes.
All finish reasons are `stop`; total completion tokens are 4,057 per corpus.
Despite normal finishes, dense technical pages lose text: mean word F1 is
0.824 for the technical paper, versus Apple accurate 0.955. Modern-report
scores are 0.970 versus 0.997; historical-scan scores are 0.957 versus 0.966.
These are noisy-reference coverage proxies, not certified OCR accuracy.

Apple accurate is about 29× faster than the initial Gemma4 batch-1 result.
The fast-mode label does not imply superior performance on this corpus: its
scanned-report mean word F1 is only 0.078. Its measured eight-page time is also
higher than accurate.

The first Apple accurate warmup took 47.859 seconds for the corpus, followed
by 2.067 seconds for the next warmup. Report that one-off initialization
observation separately from warm throughput; no repeatable cold-start claim
is established. The fast warmup took 3.518 seconds.

A late batch-1 control took 67.776 seconds, with identical
transcripts. The initial batch-1 versus larger-batch spread therefore includes
timing drift and cannot establish a causal batch-size penalty. The code and
measurements establish no native batching benefit. The late profile totals
10.878 s of image preparation, 2.129 s
of prefill and 54.202 s of decode; token generation accounts for
80.6% of reported model time. Native multimodal
decode batching is the relevant throughput work for this path.

The production paths have different output/layout behavior. This does not
establish table reconstruction quality, reading-order quality, retrieval
quality, other Gemma4 variants, or a native GPU multimodal batching speedup.

## Reproduction

Build the production server and Apple baseline:

```sh
cd zig/pkg/inference
zig build bench-server build-pdf-ocr-comparison -Doptimize=ReleaseFast
zig-out/bin/antfly-inference-bench-server run \
  --port 18097 --models-dir "$MODEL_ROOT" --max-concurrent-requests 512 \
  --host-budget-mb 8192 --backend-budget-mb 16384 \
  --combined-budget-mb 24576 --scratch-budget-mb 4096 --kv-budget-mb 8192 \
  --preload-model "generator:metal:$MODEL"
```

The local default 3 GiB host policy cannot fit this roughly 2.8 GiB resident
model plus the admitted image preprocessing workspace; the override avoids
measuring admission failures as inference speed.

```sh
python3 tools/bench/pdf_ocr.py --corpus "$CORPUS" --model "$MODEL" \
  --output-dir "$RESULTS" --batches 1 2 4 8 --iterations 2
zig-out/bin/antfly-pdf-ocr-comparison --corpus "$CORPUS" \
  --recognition accurate --batch 1 --warmups 2 --iterations 3 \
  --output "$RESULTS/apple-accurate.json"
```

Compute timing and quality summaries from the captured transcripts:

```sh
python3 tools/bench/pdf_ocr_report.py --corpus "$CORPUS" \
  --gemma-dir "$RESULTS" --apple-accurate "$RESULTS/apple-accurate.json" \
  --apple-fast "$RESULTS/apple-fast.json" --output "$RESULTS/summary.json"
```

Repeat the Apple command with `--recognition fast`. Do not run the recognition
campaigns simultaneously. Corpus JSON contains `pages`, each with `path`,
`document` and one-based `page`. Raw reports are ignored local artifacts under
`.benchmark-results/pdf-ocr-comparison-2026-10-11/`; compact results include PDF
hashes and page selection for reproduction.
