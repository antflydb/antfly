# Termite Parity

This document tracks parity between the Zig server in this repo and the Go server in `~/go/src/github.com/antflydb/antfly/termite`.

## Current Summary

The Zig server now covers the main classifier / extractor / recognizer parity slice, including GLiNER2 classification / extraction / relations, REBEL relation extraction, resolver support, and native `safetensors` classifier / recognizer sessions for generic BERT-family and DeBERTa models.

The biggest remaining parity gaps are:

- OCR runtime coverage beyond the currently validated TrOCR, Donut, and PaddleOCR families.
- Broader non-GLiNER recognizer coverage beyond the initial REBEL seq2seq path.

Additional native reranker status:

- the base-model-backed native BERT/RoBERTa cross-encoder reranker path now has
  distributed Metal tensor-parallel support and standalone verification for
  `bge-reranker-base`-style checkpoints
- the current repeated-request in-process benchmark on that path shows:
  - BLAS warm average around `3056 ms`
  - 2-rank Metal TP warm average around `297 ms`
  - score parity within the current verifier tolerance
- the real `/api/rerank` server path is now also verified on the same native
  reranker path:
  - BLAS average around `3075 ms`
  - 2-rank Metal TP average around `170 ms`
  - 2-rank Metal TP warm average around `166 ms`
  - server-path score parity matches the standalone verifier, with score diff
    around `0.000261`

## Verification Status

### Tested Here

- `zig build test` previously passed on this tree after the GLiNER classifier/extractor work, the GLiNER/REBEL relation work, the native BERT-family classifier-head work, the generic native DeBERTa session work, and the native token-classifier session work for BERT/DeBERTa recognizers.
- A rebuilt local Zig binary with `onnx=true`, `metal=true`, and `native=true` was run against the real local Go registry at `../antfly/termite/registry/models`.
- Live E2E results against local models:
  - `e2e/termite/test_classify.py`: 3/3 passed
  - `e2e/termite/test_recognize.py`: 5/5 passed
  - `e2e/termite/test_extract.py`: 3/3 passed
  - `e2e/termite/test_models.py`: 3/3 passed
  - `e2e/termite/test_read.py`: 4 passed, 4 skipped
- Those live runs exercised:
  - ONNX classifier path via `MoritzLaurer/mDeBERTa-v3-base-mnli-xnli`
  - native `safetensors` GLiNER recognizer / extractor / recognizer-backed relation path via `fastino/gliner2-base-v1`
  - ONNX REBEL relation path via `Babelscape/rebel-large`
  - ONNX reader path via explicit `Xenova/trocr-base-printed`
  - ONNX multistage OCR path via explicit `monkt/paddleocr-onnx`
- Additional live reader validation outside the Go registry:
  - explicit `Xenova/donut-base-finetuned-cord-v2` coverage now passes against a local Zig-pulled snapshot
  - explicit `Xenova/moondream2` coverage now passes against a local Zig-pulled snapshot
  - focused Moondream E2E coverage passed:
    - `env TERMITE_BIN=./zig-out/bin/termite TERMITE_MODELS_DIR=models TERMITE_MOONDREAM_MODEL=Xenova/moondream2 uv run --project e2e/termite pytest -q e2e/termite/test_read.py -k moondream -rs`
    - result: `1 passed`
- A local native-only `safetensors` classifier was pulled with the Zig CLI:
  - `./zig-out/bin/termite pull hf:cross-encoder/nli-distilroberta-base:native --models-dir models/classifiers`
  - the resulting model was exercised through `/api/classify` with `models` as the server model root
  - server logs confirmed the path fell through ONNX, selected `metal`, and returned live classification scores from the native session
- A local native-only `safetensors` DeBERTa classifier was also pulled with the Zig CLI:
  - `./zig-out/bin/termite pull hf:MoritzLaurer/mDeBERTa-v3-base-mnli-xnli:native --models-dir models/classifiers`
  - the resulting model was exercised through `/api/classify` with `models` as the server model root
  - server logs confirmed the path fell through ONNX, selected `metal`, and returned live classification scores from the native DeBERTa session
- A local native-only `safetensors` BERT token-classifier recognizer was also pulled with the Zig CLI:
  - `./zig-out/bin/termite pull hf:dslim/bert-base-NER:native --models-dir models/recognizers`
  - the resulting model was exercised through `/api/recognize` with `models` as the server model root
  - server logs confirmed the path fell through ONNX, selected `metal`, and returned live entity spans from the native token-classifier session
  - focused E2E coverage passed:
    - `TERMITE_URL=http://127.0.0.1:8097 uv run --project e2e/termite pytest -q e2e/termite/test_recognize.py -k native_safetensors_bert_token_classifier -rs`
    - result: `1 passed`
- A local native-only `safetensors` DeBERTa token-classifier recognizer was also pulled with the Zig CLI:
  - `./zig-out/bin/termite pull hf:mukuls9971/pii-deberta-v3-xsmall:native --models-dir models/recognizers`
  - the resulting model was exercised through `/api/recognize` with `models` as the server model root
  - server logs confirmed the path fell through ONNX, selected `metal`, and returned live entity spans from the native DeBERTa token-classifier session
  - this also validated two raw-Hugging-Face gaps:
    - `num_labels` inference from `id2label` / `label2id` when `config.json` omits `num_labels`
    - offset-aware `encodeForModel(...)` for `Unigram + Metaspace(split=true)` tokenizers
- Raw `hf:` pulls under task directories now infer `model_type` from the directory path even without a generated `model_manifest.json`, which is what made the native classifier validation work for the Zig-pulled snapshot.
- Reader/parser coverage now also includes:
  - Donut field parsing
  - Florence text cleanup
  - Moondream prompt building and JSON-field parsing
- Native BERT-family classifier support has direct unit coverage for:
  - BERT/DistilBERT/RoBERTa classifier-head weight mapping
  - `makeBertConfig(...)` carrying `num_labels`
- Native DeBERTa session support has direct unit coverage for:
  - `isDebertaModel(...)`
  - DeBERTa config parsing with `num_labels`
  - `detectArchitecture(...)` recognizing generic DeBERTa classifier configs
- The raw Hugging Face WordPiece tokenizer fallback now has direct unit coverage in `lib/tokenizer/src/hf_tokenizer.zig` for:
  - synthesized `tokenizer.json` parsing via `vocab.txt`-style metadata
  - offset-aware `encodeForModel(...)` for WordPiece tokenizers
- REBEL routing was exercised against a live local server far enough to confirm the request path reaches the REBEL codepath and fails on this machine only because no ONNX backend is available.

### Implemented But Not Fully Exercised Here

- Native DeBERTa classifier inference is now validated end to end for a local `safetensors` model via `MoritzLaurer/mDeBERTa-v3-base-mnli-xnli`.
- Native BERT-family classifier inference is now validated end to end for a local RoBERTa-family `safetensors` classifier via `cross-encoder/nli-distilroberta-base`, but coverage is still narrow and not yet broad across BERT and DistilBERT classifier families.
- Native BERT/DeBERTa token-classifier inference is now runtime-validated end to end for a local `dslim/bert-base-NER` `safetensors` recognizer, including:
  - raw Hugging Face `vocab.txt` tokenizer fallback
  - native backend selection
  - offset-aware span reconstruction through `/api/recognize`
- Native DeBERTa token-classifier inference is now also runtime-validated end to end for a local `mukuls9971/pii-deberta-v3-xsmall` `safetensors` recognizer, including:
  - `num_labels` inference from label maps when not explicitly present in `config.json`
  - offset-aware span reconstruction for Hugging Face `Unigram + Metaspace` tokenizer layouts used by DeBERTa-v3
  - aggregation of fragmented adjacent subtype labels like `IP` and `IPV4` into a single `/api/recognize` span
  - focused E2E assertions that verify merged `jane.smith@example.org` and `203.0.113.42` entities
- Native classifier/recognizer sessions are now preferred by the model manager when native weights are present, but they still have less runtime coverage than the long-standing ONNX paths.
- `/api/read` live coverage still depends heavily on which reader families are installed locally:
  - generic read requests passed here
  - explicit `Xenova/trocr-base-printed` coverage now passes against the local Go registry
  - explicit `monkt/paddleocr-onnx` multistage OCR coverage now passes against the local Go registry
  - explicit `Xenova/donut-base-finetuned-cord-v2` coverage now passes against a local Zig-pulled snapshot
  - explicit `Xenova/moondream2` coverage now passes against a local Zig-pulled snapshot through the decoder-only VLM path
  - Surya remains unvalidated live here because no local Surya snapshot was present
  - the local `google/pix2struct-docvqa-base` snapshot is a native `safetensors` checkpoint, not an ONNX encoder/decoder export
- `/api/models` now filters reader directories that the current build cannot actually load, which means unsupported native-only Pix2Struct checkpoints are no longer advertised as ready-to-run reader models.
- Pix2Struct prompt / parser handling exists in Zig, but real native Pix2Struct model loading is still missing. That is a larger architecture project than the Moondream parity slice because the local HF checkpoint is not a generic split encoder/decoder ONNX export.

## Classifier / Extractor / Recognizer Parity

### What Go Has

- `/api/classify` can use either:
  - dedicated classifier models from `models_dir/classifiers/`
  - recognizer models that advertise `classification`
- `/api/recognize` can use:
  - standard BIO NER models
  - zero-shot GLiNER models with custom labels
  - relation-capable recognizers when `relation_labels` are provided
- `/api/extract` is capability-driven:
  - extractor models are recognizers with `extraction`
  - extraction is not a separate model family on disk
- `/api/models` exposes recognizer capabilities and populates `extractors` from recognizers with `extraction`

Concrete Go references:

- `pkg/termite/api.go`
- `pkg/termite/ner_registry.go`
- `pkg/termite/classifier_registry.go`
- `e2e/classifier_test.go`
- `e2e/gliner2_test.go`
- `e2e/rebel_test.go`

### What Zig Has

- `/api/classify` resolves dedicated classifiers and recognizer-backed GLiNER2 classification
- `/api/recognize` supports:
  - BIO NER
  - GLiNER2 entities
  - GLiNER2 relations
  - REBEL-style seq2seq relation extraction
  - resolver flattening for both entity-only and entity+relation flows
- `/api/extract` supports structured GLiNER2 extraction from Go-style schemas
- `/api/models` exposes recognizer capabilities and populates `extractors` from recognizers with `extraction`

Concrete Zig references:

- `src/server/server.zig`
- `src/server/model_manager.zig`
- `src/pipelines/classification.zig`
- `src/pipelines/gliner.zig`
- `src/architectures/session_factory.zig`
- `src/registry/registry.zig`

### Missing Parity

#### 1. Relation extraction

Go `/api/recognize` accepts `relation_labels` and returns relation edges for models that support them.

Zig now has parity for:

- GLiNER2 relation extraction
- REBEL-style encoder/decoder relation extraction
- resolver handling on top of relation results

Remaining impact:

- broader non-GLiNER relation recognizers still need model-specific implementations

#### 2. Structured extraction

Go has a proper extractor interface and extraction config. Zig now has structured GLiNER-based extraction from Go-style schemas, but it still does not have broad non-GLiNER extractor-family parity.

Impact:

- extraction is still effectively GLiNER-centric today
- broader extractor-family coverage still needs model-specific work

#### 3. Capability-driven model listing

Go derives `extractors` from recognizers with `extraction` and returns capabilities in `/api/models`.

Zig now derives `extractors` from recognizers with extraction capability and includes capability metadata in `/api/models`.

Remaining impact:

- capability routing outside the GLiNER-centric extractor path is still limited

#### 4. Native `safetensors` classifier/token-classifier heads

Zig already has native `safetensors` support for:

- GLiNER2-style DeBERTa + span head
- Florence
- Whisper
- CLIP
- GGUF/generative paths

But generic native BERT/DeBERTa native-task parity still has some edges:

- native BERT-family sessions now support sequence-classification logits for classifier manifests
- native BERT-family weight mapping now includes common HF classifier head layouts:
  - BERT `classifier.*`
  - RoBERTa `classifier.dense` + `classifier.out_proj`
  - DistilBERT `pre_classifier` + `classifier`
- native generic DeBERTa sessions now support sequence-classification logits for classifier manifests
- native generic BERT/DeBERTa sessions now support token-classification logits for recognizer manifests
- model loading now prefers native sessions for classifier/recognizer manifests when native weights are present, instead of defaulting to ONNX first

Remaining impact:

- `safetensors` now has a real native path for generic BERT-family and DeBERTa classifier/recognizer sessions, but those paths are still less runtime-tested than the GLiNER and ONNX flows.
- Raw Hugging Face WordPiece tokenizers without `tokenizer.json` are now handled via a synthesized tokenizer path from `vocab.txt` + tokenizer metadata, and that path is now exercised both by tokenizer-level tests and a live `/api/recognize` run for `dslim/bert-base-NER`.
- Hugging Face `Unigram + Metaspace` tokenizer layouts used by DeBERTa-v3 now also preserve offsets through `encodeForModel(...)`, and that path is exercised by a live `/api/recognize` run for `mukuls9971/pii-deberta-v3-xsmall`.

### Current Build Status

- A fresh `zig build -Donnx=true -Dshared-lib-root=../antfly-zig` succeeds on the current tree.
- Repo-wide `zig build test` coverage is still thinner than the targeted live E2E coverage recorded above; the parity work here has been validated primarily through focused unit tests plus live `/api/*` requests against local model snapshots.

## OCR Parity

### What Go Has

Go supports a broader reader surface:

- TrOCR
- Donut
- Florence-2
- Pix2Struct
- Moondream2
- Multi-stage OCR:
  - PaddleOCR
  - Surya-style pipelines

Go readers can return:

- `text`
- structured `fields`
- spatial `regions`

Go uses reader dispatch:

- Vision2Seq-style readers use pooled encoder/decoder pipelines
- Multi-stage OCR uses metadata-driven stage loading

### What Zig Has

Zig reading support now includes:

- split encoder/decoder readers, which covers TrOCR-style models and other standard vision-encoder/decoder packages
- Donut-style field parsing
- native Florence directories backed by `safetensors`
- metadata-driven multi-stage OCR loading
- CTC recognition + region output for multi-stage readers
- Moondream-style decoder-only ONNX VLM reading through the existing multimodal generation pipeline
- promptable generic vision readers that should also cover Pix2Struct-style encoder/decoder exports when the model layout matches Go’s generic path

Zig read results can now return:

- `text`
- `fields`
- `regions`

### Missing OCR Parity

#### 1. Reader family coverage

Zig still does not yet have Go-level parity for:

- any Nougat-style model-specific reader handling if Go relies on it
- broader decoder-only VLM reader coverage beyond the initial Moondream-style path

Pix2Struct note:

- Go appears to use the same generic vision reader path for Pix2Struct, not a dedicated lower-level pipeline.
- Zig now has a matching model-gated E2E test for that path, but live model validation is still pending in this environment.

#### 2. Multi-stage OCR breadth

Zig has the core multi-stage OCR path now:

- `termite_metadata.json` stage loading
- detection post-processors
- CTC recognition
- region outputs

Remaining gaps are around breadth and robustness:

- unsupported stage types / model layouts still return `MultiStageReaderNotYetSupported`
- layout-specific labeling beyond plain text regions remains limited

#### 3. Runtime coverage

The Zig OCR surface has much better implementation parity than it did at the start of this work, but runtime coverage is still thinner than Go:

- E2E OCR tests are largely model-availability-gated in this environment
- native Florence still needs more live-model validation
- Moondream now has live local ONNX validation via `Xenova/moondream2`, but Pix2Struct and Surya are still unvalidated here

## Recommended Rollout Order

### Phase 1: Classifier / Extractor / Recognizer capability plumbing

Do first:

- Make `/api/models` capability-aware for recognizers
- Populate `extractors` from recognizers with `extraction`
- Stop treating extractor parity as a separate on-disk model family

Why first:

- Low risk
- Aligns API semantics with Go
- Unlocks accurate capability reporting before larger feature work

### Phase 2: GLiNER2 parity in Zig

Add next:

- recognizer-backed classification for GLiNER2
- structured extraction on top of GLiNER2
- capability-gated routing for classify/extract flows

Why next:

- Zig already has native GLiNER2 NER infrastructure
- Best cost/benefit path to immediate recognizer parity

Status:

- done

### Phase 3: REBEL seq2seq relation parity

Add next:

- route `/api/recognize` through a seq2seq relation extractor for REBEL-style recognizers
- parse generated triplets from special-token and plain-text fallback output
- return entities + relations through the same recognize response shape

Why:

- Go already treats REBEL as a recognizer + relation extractor
- The Zig repo already has shared encoder/decoder generation infrastructure and a local `Babelscape/rebel-large` model checkout

Status:

- done for the initial REBEL path

Remaining:

- other non-GLiNER relation-capable recognizers still need explicit support

### Phase 3: Relation extraction parity

Add after GLiNER classification/extraction:

- GLiNER2 relation extraction
- `/api/recognize` relation response plumbing
- optional REBEL parity

Why later:

- More API and output-shape work
- Higher implementation and verification cost

### Phase 4: Native `safetensors` classifier/token-classifier heads

Add after API parity work:

- BERT/RoBERTa/DistilBERT sequence-classification head
- BERT/RoBERTa/DistilBERT token-classification head
- DeBERTa sequence-classification head
- DeBERTa token-classification head

Recommended target order:

1. GLiNER2 native capability expansion
2. mDeBERTa MNLI native classifier
3. generic BERT token-classifier parity
4. BART MNLI native path if still worth the complexity

Why:

- GLiNER2 already has native Zig architecture support
- DeBERTa infrastructure already exists
- BART is a larger lift than BERT/DeBERTa classifier heads

Status:

- BERT/RoBERTa/DistilBERT sequence-classification: done
- BERT/RoBERTa/DistilBERT token-classification: done
- generic DeBERTa sequence-classification: done
- generic DeBERTa token-classification: done
- end-to-end runtime coverage against local safetensors classifier/recognizer models: still incomplete

## Immediate Next Work

The classifier / extractor / recognizer parity slice is largely in place.

Next highest-value work:

1. Runtime validation of the new native `safetensors` classifier / recognizer paths against real local models
2. OCR family expansion, with Pix2Struct the most obvious remaining reader gap
3. broader non-GLiNER recognizer coverage beyond the current REBEL path
