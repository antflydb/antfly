# Readers Parity

This document tracks reader and OCR parity between the Zig server in this repo and the Go server in `~/go/src/github.com/antflydb/antfly/termite`.

## Goal

Bring Zig reader support up to Go parity for:

- Vision2Seq reader families
- structured reader outputs
- metadata-driven multi-stage OCR
- PaddleOCR and Surya-style pipelines

This is intentionally narrower than the repo-root `TODO.md` parity sections. It
is the working plan for `/api/read` and reader model support.

## Current State

### Go already has

- a reader abstraction that returns:
  - `text`
  - `fields`
  - `regions`
- pooled Vision2Seq readers for:
  - TrOCR
  - Donut
  - Florence-2
  - Pix2Struct
  - Moondream2
- metadata-driven multi-stage OCR readers for:
  - PaddleOCR
  - Surya-style pipelines

Key Go references:

- `pkg/inference/lib/reading/reader.go`
- `pkg/inference/lib/reading/multistage_reader.go`
- `pkg/inference/lib/pipelines/multistage_ocr.go`
- `pkg/inference/lib/pipelines/multistage_ocr_loader.go`
- `pkg/inference/reader_registry.go`
- `specs/008-ocr-models-v2.md`

### Zig currently has

- a new reader wrapper in `src/readers/reader.zig` for the current Vision2Seq/native Florence path
- `/api/read` routed through that reader layer
- a single `ReadingPipeline` in `src/pipelines/reading.zig`
- support centered on:
  - split encoder/decoder readers
  - native Florence directories backed by `safetensors`
- response items that can now expose:
  - `text`
  - optional `fields`

Key Zig references:

- `src/readers/reader.zig`
- `src/server/server.zig`
- `src/pipelines/reading.zig`
- `src/native_read.zig`
- `src/models/manifest.zig`

## Main Gaps

### 1. Reader abstraction gap

Go has a reader layer and registry dispatch. Zig currently wires `/api/read` directly to one pipeline inside the server.

Impact:

- model-family-specific behavior accumulates in the server
- there is no clean dispatch point for multi-stage OCR
- response-shape parity is harder to add cleanly

### 2. Output structure gap

Go reader results can include `fields` and `regions`. Zig results are text-only today.

Impact:

- no Donut-style structured extraction parity
- no PaddleOCR or Surya region parity
- no stable response shape for richer readers

### 3. Metadata-driven OCR gap

Go can load multi-stage OCR from `termite_metadata.json`. Zig does not currently parse or route on that metadata.

Impact:

- no PaddleOCR parity
- no Surya parity
- no stage-based detection/recognition/layout/order orchestration

### 4. Reader family coverage gap

Missing or incomplete in Zig:

- Pix2Struct
- Moondream-style decoder-only reading
- Donut structured output handling
- multi-stage OCR families

## Recommended Rollout

## Phase 1: Introduce a Zig reader layer

Purpose:

- get `/api/read` out of inline server wiring
- create a stable place for family-specific parsing and future dispatch

Concrete work:

- add a reader result type with:
  - `text`
  - optional `fields`
  - optional `regions`
- add a reader interface or equivalent dispatch abstraction
- move current Vision2Seq reading flow out of `src/server/server.zig`
- keep existing TrOCR/Florence behavior working through the new layer

Likely Zig files:

- `src/server/server.zig`
- new `src/readers/` module or equivalent
- `src/pipelines/reading.zig`

Acceptance criteria:

- `/api/read` still works for current reader models
- server no longer owns model-family-specific reader logic
- response encoder can emit `text` now and extend to `fields` and `regions` later without another shape rewrite

Status:

- done

## Phase 2: Add structured reader outputs for current Vision2Seq readers

Purpose:

- reach near-term parity without waiting for multi-stage OCR

Concrete work:

- extend Zig read results to include optional `fields` and `regions`
- add output parsing for:
  - Donut structured tags to flattened `fields`
  - Florence prompt normalization and better text cleanup
  - any current reader-family-specific parsing that does not require new architecture
- update `/api/read` response serialization
- update e2e expectations to allow richer results without regressing plain text readers

Likely Zig files:

- `src/pipelines/reading.zig`
- `src/server/server.zig`
- new reader parser helpers
- `../../e2e/inference/test_read.py`

Acceptance criteria:

- existing text-only readers still return `text`
- Donut-style readers can populate `fields`
- response shape matches Go direction: `text` plus optional richer outputs

Status:

- in progress

Implemented in this pass:

- Donut tag parsing to flattened `fields`
- Florence text post-processing in the reader layer
- optional `fields` emission from `/api/read`
- the Donut `/api/read` e2e now prefers the shared `sample-page-1.png` fixture when available and exercises the DocVQA prompt path instead of only the tiny placeholder image
- the Donut model-gated `/api/read` e2e now supports env-driven phrase and field assertions, so a local real-model run can tighten it without another code change

Still remaining in this phase:

- confirm behavior against real Donut models rather than parser-only unit coverage

## Phase 3: Add metadata-driven reader dispatch

Purpose:

- unlock multi-stage OCR without baking special cases into the server

Concrete work:

- add Zig parsing for `termite_metadata.json`
- add a detector for multi-stage reader directories
- route reader model loading between:
  - Vision2Seq reader path
  - multi-stage OCR path
- decide whether this belongs in:
  - `src/models/manifest.zig`, or
  - a new OCR metadata module

Recommendation:

- keep `termite_metadata.json` separate from generic model manifest parsing unless there is a strong reason to merge them

Likely Zig files:

- `src/models/manifest.zig`
- new `src/readers/multistage_metadata.zig` or equivalent
- new reader loader/dispatch module

Acceptance criteria:

- Zig can identify a multi-stage OCR model directory from metadata
- standard Vision2Seq readers and multi-stage readers can coexist behind one read endpoint

Status:

- done

Implemented in this pass:

- reader-specific parsing for `termite_metadata.json`
- explicit detection of `multistage_ocr` model directories
- reader loading routed between Vision2Seq and multi-stage OCR implementations
- actual multi-stage OCR pipeline and stage loading wired behind the reader abstraction

## Phase 4: Pix2Struct parity

Purpose:

- add the cheapest missing family that still fits the Vision2Seq shape

Concrete work:

- support Pix2Struct-specific prompting and preprocessing expectations
- add Pix2Struct output handling through the reader layer

Why now:

- low structural risk
- no multi-stage OCR machinery required

Likely Zig files:

- `src/pipelines/reading.zig`
- new Pix2Struct prompt/helper module
- possibly exporter or model-discovery support if needed later

Acceptance criteria:

- Pix2Struct models load through the normal reader path
- prompt-based DocVQA-style reads work

Status:

- in progress

Implemented in this pass:

- explicit Pix2Struct reader-family detection in the shared reader layer
- Pix2Struct prompt helpers for DocVQA/ChartQA/Infographics-style natural-language questions
- API prompt docs now call out Pix2Struct's natural-language prompt contract directly instead of only the tag-based Donut/Florence formats
- the existing model-gated Pix2Struct `/api/read` e2e now matches that contract cleanly: natural-language prompt in, plain text answer out
- the same Pix2Struct e2e now supports an env-driven phrase assertion, so a local installed model can promote the current smoke check into a stronger regression
- the local `Xenova/pix2struct-docvqa-base` and `Xenova/donut-base-finetuned-cord-v2` paths now have stronger model-specific assertions on the shared sample-page fixture instead of only generic non-empty text checks

Still remaining in this phase:

- validate against a real Pix2Struct model instead of unit coverage plus the existing model-gated e2e path alone
- confirm whether any exported Pix2Struct variants need additional preprocessing beyond the current Vision2Seq image path

## Phase 5: Shared multi-stage OCR infrastructure

Purpose:

- build the common machinery both PaddleOCR and Surya need

Concrete work:

- multi-stage OCR pipeline coordinator
- detection post-processor interface
- crop and reading-order helpers
- region result assembly
- optional layout and ordering stages

New Zig modules likely needed:

- `src/pipelines/multistage_ocr.zig`
- `src/pipelines/connected_components.zig`
- `src/pipelines/db_postprocess.zig`
- `src/pipelines/ctc_decode.zig`
- `src/pipelines/crop.zig`

Acceptance criteria:

- a metadata-described multi-stage model can run detection and recognition
- the pipeline can return `regions`
- full text is assembled from recognized regions in reading order

Status:

- in progress

Implemented in this pass:

- shared `TextRegion`, `RecognizedRegion`, and `LayoutRegion` types
- reading-order sort and full-text assembly helpers
- bbox crop utility for region recognition
- greedy CTC decode and character-dictionary loading helpers
- connected-components detection utilities
- multi-stage OCR coordinator with detection plus CTC recognition
- metadata-driven stage loading into runnable detection and recognition sessions

Still remaining in this phase:

- optional layout and reading-order model support
- broader model-validated tests beyond unit coverage

## Phase 6: PaddleOCR parity

Purpose:

- land the first full multi-stage OCR family on top of the shared infrastructure

Concrete work:

- DB post-processing
- CTC recognition
- character dictionary loading
- region text and confidence output

Why before Surya:

- it exercises the core multi-stage pieces directly
- it does not require Surya layout/order support to deliver value

Acceptance criteria:

- PaddleOCR model directories load through metadata-driven dispatch
- `/api/read` returns:
  - `text`
  - `regions`
- region bounding boxes and recognized text are stable enough for e2e tests

Status:

- in progress

Implemented in this pass:

- DB and heatmap detection post-processing in the shared OCR coordinator
- CTC recognition session loading with character-dictionary support
- `/api/read` multistage dispatch now produces `text` plus `regions` for CTC-based OCR readers
- native `antfly inference read` now emits `regions` too, so local reader output matches the richer API result shape more closely
- PaddleOCR recognition preprocessing now keeps aspect ratio and pads crops instead of always stretching them to the recognition width
- multistage OCR preprocessing now honors explicit `rescale_factor` values instead of assuming `/255` normalization in every case
- multistage `preprocessor_config.json` parsing now accepts additional Hugging Face-style size variants such as `[width, height]` arrays and `{ "shortest_edge": N }`
- detection-stage preprocessing now matches the Go loader more closely by preserving explicit `preprocessor_config.json` dimensions instead of always overriding them from the ONNX input shape

Still remaining in this phase:

- tune defaults and thresholds against model outputs instead of code-level defaults alone
- keep tightening PaddleOCR output quality against real fixtures instead of settling for shape-level parity

Verified in this pass:

- `/api/version` now exposes built backend capabilities so e2e can gate ONNX-backed OCR checks without shelling out
- the installed `monkt/paddleocr-onnx` reader now has a model-backed e2e assertion on the generated bitmap fixture
- `antfly inference pull hf:monkt/paddleocr-onnx --models-dir models/readers` now works against the raw upstream Hugging Face repo by selecting the nested detection/English recognition assets and synthesizing a local `termite_metadata.json`
- the freshly pulled local `monkt/paddleocr-onnx` bundle now passes the multistage `/api/read` e2e, so PaddleOCR is no longer blocked on manual repackaging
- plain `zig build` now auto-enables ONNX when a matching local `onnxruntime/<platform>-<arch>` bundle is present, so the default local binary keeps the PaddleOCR reader path live
- the default build now passes `zig build test`, `e2e/inference/test_version.py`, and the `monkt/paddleocr-onnx` multistage `/api/read` e2e without extra `-Donnx=true` flags
- the rebuilt default binary still passes the model-backed `monkt/paddleocr-onnx` `/api/read` e2e after the preprocessing-config parity changes
- the current tree still passes `zig build` after the new pull fallback; `zig build test` is currently hitting an unrelated graph test crash (`fused_embedding_lookup`) outside the reader path

## Phase 7: Surya parity

Purpose:

- add the richer multi-stage OCR family once the shared infrastructure is proven

Concrete work:

- heatmap-based detection post-processing
- Vision2Seq recognizer reuse for cropped regions
- optional layout stage
- optional reading-order stage

Acceptance criteria:

- Surya-style model directories load from metadata
- `/api/read` returns ordered text regions
- layout labels are attached where available

Status:

- in progress

Implemented in this pass:

- multistage reader loading now accepts optional `layout` and `order` stages from `termite_metadata.json`
- reader results now surface multistage layout labels through the existing `regions[].label` field
- the shared Vision2Seq reader load/run path now lives in a reusable module so both top-level readers and multistage OCR can use the same encoder-decoder loading and preprocessing rules
- `ReadingPipeline` now supports already-decoded image crops, which lets multistage OCR reuse the existing Vision2Seq stack without re-encoding intermediate regions
- multistage OCR now supports `recognition.type = "vision2seq"` from `termite_metadata.json`, loading stage-specific encoder/decoder files and running Vision2Seq recognition on cropped text regions
- `/api/read` e2e now has a Surya-specific, model-gated test path so installed Surya readers exercise the new multistage `vision2seq` recognizer separately from the PaddleOCR CTC checks
- that Surya e2e now prefers the Go repo's existing `sample-page-1.png` fixture when available, so real-model validation can cover document structure and OCR phrases without duplicating binary testdata here
- the Surya e2e also accepts env-driven stricter assertions for expected OCR phrases and layout labels, so a local real-model run can validate more than just non-empty regions without another code change
- the Surya e2e can now also assert ordered region texts and ordered region labels from env vars, which lets a local real-model run validate reading order behavior directly
- the same Surya e2e now accepts `ANTFLY_INFERENCE_SURYA_EXPECTATIONS_JSON`, so a local pinned expectation set can capture phrase, label, and ordered-region checks in one file instead of many env vars
- the same test can now write an observed expectation snapshot via `ANTFLY_INFERENCE_SURYA_WRITE_EXPECTATIONS_JSON`, and `e2e/testdata/surya_expectations.sample.json` documents the expected file shape for promoting a local Surya run into a regression
- if `e2e/testdata/surya_expectations.local.json` exists, the Surya e2e will now pick it up automatically; that file is gitignored so one machine can pin a real local Surya expectation set without affecting the shared repo

Still remaining in this phase:

- validate layout/order behavior against a real Surya reader model instead of unit tests alone
- capture and pin a stable local Surya fixture/model expectation JSON from a real run so the new e2e assertion path can become a default regression instead of an opt-in local check

Verified in this pass:

- `zig build` still passes after the shared Vision2Seq refactor and multistage `vision2seq` recognizer wiring
- the existing model-backed `monkt/paddleocr-onnx` `/api/read` e2e still passes after adding the new recognizer type, confirming the CTC path still works

## Phase 8: Moondream-style reader parity

Purpose:

- close the remaining non-classical OCR reader gap

Concrete work:

- support decoder-only VLM reading behavior
- parse structured JSON-like output into `fields` where applicable

Why last:

- useful, but not blocking classical OCR parity
- architecturally distinct from the main OCR gap

Acceptance criteria:

- Moondream-style readers work through the reader layer
- structured fields can be surfaced from model output

Status:

- in progress

Implemented in this pass:

- Moondream readers load through the shared reader layer via both ONNX decoder-only VLM and ORT GenAI paths
- Moondream JSON-like outputs are parsed into text, optional flattened `fields`, and an internal structured payload used by `/extract`
- the `/api/read` boundary still accepts ordinary natural-language prompts, but the Moondream reader now restores the Go-style internal JSON-guided prompt wrapper for better local-model behavior
- the normal Moondream reader path now prefers the working decoder-only VLM runtime again, so local `/read` and CLI flows do not pay an ORT GenAI load failure before succeeding
- the model-gated Moondream `/api/read` e2e now uses a simple generated bitmap-text image aligned with Go's reader test instead of treating document-page OCR as the baseline regression
- the same Moondream e2e now supports env-driven phrase and field assertions, so a local installed model can promote the current smoke check into a stronger regression
- the forced ORT GenAI overlay package for Moondream now normalizes `config.json`, preserves the `onnx/` layout, and includes tokenizer merge/vocab files so the local `Xenova/moondream2` path loads successfully in the single-process reader flow

Still remaining in this phase:

- improve Moondream output quality beyond the current non-empty simple-image regression, especially on document-style fixtures like `sample-page-1.png`
- decide whether the repaired ORT GenAI overlay path is worth keeping on the reader side at all, since the decoder-only VLM path is currently the reliable default
- decide whether any additional model-specific fields beyond the current `mood` / `possible_source` / `tags` projection should be surfaced publicly

## Verification Plan

Current Zig coverage is very thin for readers. We should add both unit coverage and API/e2e coverage as each phase lands.

### Unit coverage to add

- reader output parsing
- `termite_metadata.json` parsing
- detection post-processors
- CTC decoding
- reading-order assembly

### API and e2e coverage to add

- extend `e2e/inference/test_read.py` for:
  - optional `fields`
  - optional `regions`
- add model-gated OCR tests for:
  - Pix2Struct
  - PaddleOCR
  - Surya

Use Go tests as reference:

- `e2e/reader_test.go`
- `e2e/paddleocr_test.go`

## Risks and Notes

- The biggest design risk is skipping the reader abstraction and pushing feature branches directly into `src/server/server.zig`.
- `termite_metadata.json` is likely better handled as reader-specific metadata rather than overloaded into the generic manifest path.
- Surya has licensing considerations; PaddleOCR and Pix2Struct are cleaner first targets.
- Response-shape changes should be introduced in a backwards-compatible way:
  - keep `text`
  - add `fields` and `regions` only when present
- schema-driven document understanding should live in `/extract`, not `/read`:
  - `/read` remains the low-level perception API for text, regions, and optional native reader structure
  - `/extract` can now own image-backed schema extraction by selecting a compatible reader internally, reading document text first, and then running the extraction pipeline
  - `reader_model` should stay an internal routing detail unless and until we expose true modality-aware extraction capabilities in model metadata
  - `/extract` now also has a native reader-backed image branch for models that explicitly advertise `image` + `extraction`, mapping structured reader `fields` directly into schema instances instead of forcing the GLiNER text fallback
  - reader results now carry an internal structured payload, and the native image-extraction branch consumes that structured value first, falling back to flattened `fields` only for older readers
  - extractor selection and image/text dispatch now live under `src/extractors/`, so `/extract` is no longer carrying its own parallel resolver/runtime path inside `src/server/server.zig`

## Suggested Next Implementation Step

Start with Phase 1 and Phase 2 together:

- introduce the Zig reader layer
- switch `/api/read` to it
- add response support for optional `fields`

That gives us a clean seam for everything else and delivers useful parity before the multi-stage OCR work begins.
