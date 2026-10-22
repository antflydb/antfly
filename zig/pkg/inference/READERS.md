# Readers Parity

This document records the implemented Zig reader/OCR surface and the remaining
model-qualification work. Historical Go parity goals are retained only where
they explain a current implementation boundary.

## Goal

Bring Zig reader support up to Go parity for:

- Vision2Seq reader families
- structured reader outputs
- metadata-driven multi-stage OCR
- PaddleOCR and Surya-style pipelines

This is intentionally narrower than the repo-root `TODO.md` parity sections. It
is the working plan for `/api/read` and reader model support.

**Status**: reader dispatch, structured `fields`/`regions`, metadata-driven
multi-stage OCR routing, detection/CTC recognition, and optional layout/order and
Vision2Seq recognition stages are implemented. Model coverage and output quality
remain family-specific: the sections below retain real Donut, Pix2Struct and
Surya qualification limits. Implemented routing does not certify every upstream
model export.

## Current State

### Zig currently has

- a new reader wrapper in `src/readers/reader.zig` for the current Vision2Seq/native Florence path
- `/api/read` routed through that reader layer
- a shared `ReadingPipeline` plus multi-stage OCR coordination
- support centered on:
  - split encoder/decoder readers
  - native Florence directories backed by `safetensors`
- response items that can now expose:
  - `text`
  - optional `fields`, `regions`, and internal structured output

Key Zig references:

- `src/readers/reader.zig`
- `src/server/server.zig`
- `src/pipelines/reading.zig`
- `src/native_read.zig`
- `src/models/manifest.zig`

## Remaining qualification

The reader abstraction dispatches Vision2Seq and metadata-described multi-stage
OCR outside the server. Results support text, optional fields/regions, and an
internal structured payload used by image-backed extraction. `termite_metadata.json`
is parsed by `src/readers/multistage_metadata.zig`; the loader selects stages in
`src/readers/multistage_reader.zig` and `src/pipelines/multistage_ocr.zig`.

Remaining work is validating supported model exports, quality thresholds and
reading order against real fixtures. Pix2Struct, Donut, and Surya sections below
identify their specific validation gaps. Moondream's current CPU/crop limits
remain applicable.

## Zig Reader Layer

`/api/read` is out of inline server wiring, with a stable place for
family-specific parsing and dispatch:

- a reader result type with `text`, optional `fields`, and optional `regions`
- a reader interface / dispatch abstraction
- the Vision2Seq reading flow lives outside `src/server/server.zig`
- existing TrOCR/Florence behavior works through this layer

Zig files:

- `src/server/server.zig`
- `src/readers/` (or equivalent)
- `src/pipelines/reading.zig`

This meets its original acceptance bar: `/api/read` works for current reader
models, the server does not own model-family-specific reader logic, and the
response encoder can emit `text` now and extend to `fields`/`regions` without
another shape rewrite.

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

## Metadata-Driven Reader Dispatch

Multi-stage OCR is unlocked without baking special cases into the server:

- Zig parses `termite_metadata.json` (kept separate from generic model
  manifest parsing)
- a detector identifies multi-stage reader directories
- reader model loading routes between the Vision2Seq reader path and the
  multi-stage OCR path

Zig files:

- `src/models/manifest.zig`
- `src/readers/multistage_metadata.zig` (or equivalent)
- the reader loader/dispatch module

Implemented:

- reader-specific parsing for `termite_metadata.json`
- explicit detection of `multistage_ocr` model directories
- reader loading routed between Vision2Seq and multi-stage OCR implementations
- the multi-stage OCR pipeline and stage loading, wired behind the reader
  abstraction

This meets its original acceptance bar: Zig identifies a multi-stage OCR
model directory from metadata, and standard Vision2Seq readers and
multi-stage readers coexist behind one read endpoint.

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

- model-backed qualification of optional layout and reading-order stages
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

- `/api/version` exposes built backend capabilities
- the installed `monkt/paddleocr-onnx` reader now has a model-backed e2e assertion on the generated bitmap fixture
- `antfly inference pull hf:monkt/paddleocr-onnx --models-dir models/readers` now works against the raw upstream Hugging Face repo by selecting the nested detection/English recognition assets and synthesizing a local `termite_metadata.json`
- the freshly pulled local `monkt/paddleocr-onnx` bundle now passes the multistage `/api/read` e2e, so PaddleOCR is no longer blocked on manual repackaging
- PaddleOCR runs on the native ONNX implementation; the default build needs no external runtime for the `monkt/paddleocr-onnx` multistage `/api/read` e2e
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

## Moondream Reader

Moondream2 (`vikhyatk/moondream2@2024-03-04`, `model_type: "moondream1"`)
runs natively from its safetensors checkpoint, with no ONNX Runtime. The
design, prompt contract, parity qualification, performance, and limits are in
[models/moondream/MOONDREAM.md](models/moondream/MOONDREAM.md).

- `src/readers/reader.zig` routes Moondream directories (path name contains
  `moondream`) through the `native_moondream` seam to the native reader
  (`src/readers/moondream_reader.zig`) when the directory has a `moondream1`
  `config.json`, `tokenizer.json`, and `model.safetensors`; other Moondream
  directories (such as the removed ONNX exports) still fail with
  `UnsupportedMoondreamArtifacts` (HTTP 400 `INVALID_MODEL`). `probeManifest` reports native
  directories as `moondream`, and the manifest classifies `moondream1`
  configs as readers.
- The `/read` prompt is the question in upstream's
  `<image>...Question: {q}\n\nAnswer: ` template; an empty prompt asks
  `Describe this image.`. Answers go through the existing Moondream parser:
  `text` always, `fields` and the structured payload when the answer is JSON.
- The model-gated `/api/read` e2e
  (`test_read_moondream_model_exposes_optional_fields`) runs without any
  ONNX Runtime gate.
- The pipeline matches the upstream float32 reference token-for-token on the
  e2e HELLO fixture (`src/pipelines/moondream_parity_test.zig`).

Still remaining:

- CPU only; Metal/CUDA need device rotary and KV seeding.
- One 378x378 crop per image, so document-page OCR quality is limited, as it
  is upstream at this revision; newer multi-crop moondream2 revisions are not
  supported.

## Verification coverage and remaining qualification

Unit tests already cover reader output parsing, `termite_metadata.json` parsing,
detection post-processing, CTC decoding, and reading-order assembly. The current
`e2e/inference/test_read.py` also includes model-gated Donut field checks,
PaddleOCR multistage region checks, Surya region/layout/order checks, and
Pix2Struct prompt checks. These tests are the executable reference; the retired
external Go checkout is not required.

Existing test paths do not certify every model export or document shape.
Remaining qualification work is to pin real model/fixture expectations for
Donut, Pix2Struct, and Surya, tighten OCR quality and ordering assertions, and
validate supported exports against those expectations. Model-gated tests require
the corresponding installed models; their presence alone does not establish
that those model-backed checks ran in a particular test invocation.

## Risks and Notes

- Keep family-specific parsing and routing in the implemented reader abstraction, outside `src/server/server.zig`.
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

## Next qualification work

Pin real model/fixture expectations for the remaining Donut, Pix2Struct and
Surya cases. Extend model-gated coverage of structured fields, layout labels,
and ordering without replacing the implemented reader dispatch or stage loader.
