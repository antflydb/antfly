# Learned Entity Cleanup

This document defines a learned post-GLiNER cleanup path for OCR-heavy entity extraction.

The target problem is:
- remove junk entities produced from OCR noise
- merge duplicate mentions such as `TimKaye`, `Tim Kaye`, and `TIM KAYE`
- select one representative surface form per duplicate cluster

This is intentionally not a canonical-ID retrieval system.

## Goal

Add a second-stage learned cleanup model after GLiNER2 detection:

1. OCR / reader produces text and regions
2. GLiNER2 detects entity spans
3. cleanup model predicts:
   - `validity_score`: keep or drop the detected mention
   - `dedup_embedding`: learned vector used for duplicate clustering
   - `representative_score`: quality score for choosing the best surface form inside a duplicate cluster
4. cleanup pipeline returns:
   - dropped mentions
   - kept mentions
   - resolved entities with provenance

## Why This Design

The existing GLiNER2 training path in `antfly-inference-zig` is span-centric:
- detect spans
- assign entity labels
- materialize LoRA adapters

That makes it a good detector, but not a direct fit for:
- canonical entity retrieval
- free-form rewrite generation

The cleanup task stays mention-centric and uses the current strengths of the codebase:
- span detection from GLiNER2
- lightweight learned post-processing
- embedding-style duplicate clustering
- future upgrade path to GLiNER span-state features

The first complete implementation in this repo uses learned mention text, label,
and local-context features so the entire path can run end-to-end in serving
without modifying the GLiNER runtime. A later version can swap the feature
extractor to GLiNER span states while keeping the cleanup contract unchanged.

## Model Outputs

For each detected mention span, the cleanup model should emit:

- `validity_score: f32`
  - probability that the mention is a real entity worth keeping
- `representative_score: f32`
  - quality score used to choose a surface form when multiple mentions refer to the same entity
- `dedup_embedding: [D]f32`
  - learned embedding used for duplicate clustering

These outputs are independent from the original GLiNER span label score.

## Inference Contract

The cleanup stage consumes detected spans plus learned outputs.

Suggested logical input:

```json
{
  "text": "T1mKaye met Tim Kaye at Apple.",
  "mentions": [
    {
      "text": "T1mKaye",
      "label": "person",
      "start": 0,
      "end": 8,
      "detect_score": 0.81,
      "validity_score": 0.10,
      "representative_score": 0.05,
      "embedding": [0.1, 0.2, 0.3]
    },
    {
      "text": "Tim Kaye",
      "label": "person",
      "start": 13,
      "end": 21,
      "detect_score": 0.97,
      "validity_score": 0.98,
      "representative_score": 0.96,
      "embedding": [0.8, 0.1, 0.0]
    }
  ]
}
```

Suggested logical output:

```json
{
  "dropped_mentions": [
    {
      "text": "T1mKaye",
      "label": "person"
    }
  ],
  "resolved_entities": [
    {
      "text": "Tim Kaye",
      "label": "person",
      "mentions": ["Tim Kaye"]
    }
  ]
}
```

## Training Dataset Contract

Use a JSONL schema dedicated to entity cleanup.

Each row should contain the original text and mention-level supervision:

```json
{
  "schema": "entity_cleanup/v1",
  "id": "doc-1",
  "split": "train",
  "text": "T1mKaye met Tim Kaye at Apple.",
  "mentions": [
    {
      "start": 0,
      "end": 8,
      "label": "person",
      "keep": false
    },
    {
      "start": 13,
      "end": 21,
      "label": "person",
      "keep": true,
      "group_id": "person:tim_kaye",
      "preferred_surface": true
    },
    {
      "start": 25,
      "end": 30,
      "label": "organization",
      "keep": true,
      "group_id": "org:apple",
      "preferred_surface": true
    }
  ]
}
```

Fields:
- `schema`
  - must be `entity_cleanup/v1`
- `id`
  - optional row identifier
- `split`
  - optional train/eval split marker when loading without a filter
  - required when a train/eval split filter is requested
- `text`
  - original text
- `mentions[]`
  - span-level supervision
- `mentions[].keep`
  - whether the mention should survive cleanup
- `mentions[].group_id`
  - required for kept mentions; duplicate cluster identifier used for positive / negative pairing
- `mentions[].preferred_surface`
  - required for kept mentions; whether this mention is the best cluster representative

Validation rules:
- every row must declare `schema: "entity_cleanup/v1"`
- mention spans must satisfy `0 <= start < end <= text.len`
- labels must be non-empty
- dropped mentions must not set `group_id` or `preferred_surface`
- kept mentions must provide a non-empty `group_id`
- each `group_id` must have exactly one `preferred_surface=true` mention
- the same `group_id` may not mix labels across the loaded dataset
- train/eval cache inputs with the same explicit split are rejected

## Training Plan

### Phase 1: Data + Offline Cleanup Pipeline

- add dataset loader for `entity_cleanup/v1`
- add in-memory cleanup pipeline that accepts learned scores and embeddings
- add unit tests for:
  - keep/drop filtering
  - duplicate clustering
  - representative selection

### Phase 2: Cached Training Surface

- add `prepare-entity-cleanup-cache`
- build learned mention features from text, label, and local context
- add cleanup trainer:
  - BCE loss for `validity_score`
  - BCE or pairwise ranking loss for `representative_score`
  - pairwise learned embedding loss for `dedup_embedding`

### Phase 3: Serving Integration

- add optional cleanup stage after GLiNER2 recognition
- carry OCR provenance forward so representative selection can later use reader confidence and region evidence

## Task List

- [x] Define cleanup architecture and dataset contract
- [x] Implement cleanup dataset loader
- [x] Implement learned cleanup clustering pipeline
- [x] Add cache-prep tool for cleanup training
- [x] Add trainer for validity, dedup embedding, and representative scoring
- [x] Integrate cleanup stage into recognize serving path
- [x] Integrate cleanup stage into native recognize CLI
- [x] Integrate cleanup into GLiNER extraction path
- [x] Add workflow docs and one bundled GLiNER2 cleanup smoke workflow

## Current Code Surface

The current implementation in this repo adds:
- `src/finetune/entity_cleanup_data.zig`
- `src/pipelines/entity_cleanup.zig`
- `src/finetune/entity_cleanup_model.zig`
- `src/finetune/tools/prepare_entity_cleanup_cache.zig`
- `src/finetune/train/train_eval_entity_cleanup_head.zig`
- server integration in `src/server/server.zig`
- local CLI integration in `src/native_recognize.zig`

The learned cleanup head is intentionally lightweight:
- hashed mention/context features
- linear validity head
- linear representative head
- learned embedding projection for duplicate clustering

This keeps the training and serving loop cheap while preserving a clean upgrade
path to richer GLiNER span-state features later.

## Current Serving Behavior

If a model directory contains `entity_cleanup_head.json`, the recognize serving
and GLiNER extraction paths automatically apply cleanup after entity detection:

1. run NER / GLiNER detection
2. score mentions with the cleanup head
3. drop low-validity mentions
4. cluster duplicates by learned embedding similarity
5. emit the best learned representative per cluster

If no cleanup artifact is present, recognition behaves exactly as before.

Recognize and extraction responses preserve the representative mention span
chosen by the cleanup stage. A bundled GLiNER2 cleanup smoke workflow now
exists for cache prep, cleanup-head training, and materialization, but there is
still not a separate OCR-to-recognize request-path smoke test.

## Training Workflow

Prepare caches from an `entity_cleanup/v1` dataset:

```bash
zig build prepare-entity-cleanup-cache -- /tmp/entity_cleanup.jsonl /tmp/entity_cleanup_train_cache.json train 128 24
zig build prepare-entity-cleanup-cache -- /tmp/entity_cleanup.jsonl /tmp/entity_cleanup_eval_cache.json eval 128 24
```

Train and write the cleanup head artifact:

```bash
zig build train-eval-entity-cleanup-head -- /tmp/entity_cleanup_train_cache.json /tmp/entity_cleanup_eval_cache.json /tmp/entity_cleanup_out --epochs 5 --learning-rate 0.05 --embedding-learning-rate 0.02 --embedding-dim 8
```

Copy or materialize the resulting `entity_cleanup_head.json` into the target
model directory to enable the learned cleanup stage at inference time.

## Recommended Production Path

The best product shape for this repo is:

- ship a `GLiNER2 + cleanup head` bundle as one recognizer product
- keep the cleanup head architecturally separate from the core GLiNER2 detector
- train the cleanup head on GLiNER2-native span or boundary features, not the
  current hashed text/context features

This keeps the user-facing experience simple:

- one recognizer model
- one inference API
- built-in cleanup behavior

But it avoids the highest-risk option of fully joint cleanup + GLiNER2 training.

## Why This Is The Right Next Step

The current implementation is already good enough to prove the cleanup task and
start data collection. The main weakness is only the feature source.

Current state:

- cleanup artifact and serving hooks exist
- cleanup training works today
- cleanup currently learns from hashed mention/context features

Best next state:

- cleanup artifact stays separate
- cleanup features come from cached GLiNER2 span or boundary representations
- GLiNER2 bundles materialize and carry the cleanup artifact automatically

This is the lowest-risk path that still upgrades the cleanup head into a
production-ready GLiNER2-native component.

## Target End State

The desired production deployment shape is:

1. `prepare-gliner2-entity-cleanup-cache`
   - load a GLiNER2 base or adapter model
   - run boundary or span feature extraction for annotated cleanup mentions
   - persist a cleanup cache derived from GLiNER2-native representations

2. `train-eval-entity-cleanup-head`
   - train the cleanup head on GLiNER2-derived mention features
   - emit `entity_cleanup_head.json`
   - report validity / preferred-surface / pairwise dedup metrics

3. `materialize-gliner2-lora`
   - copy `entity_cleanup_head.json` into the final materialized model dir

4. inference
   - `recognize`
   - `extract`
   - `antfly inference recognize`
   all automatically load and apply the bundled cleanup head

From the user's perspective, this is one GLiNER2 model with built-in cleanup.

## GLiNER2 Integration

### Step 1: GLiNER2-Native Cleanup Cache

Implemented:
- `src/finetune/entity_cleanup_gliner_cache.zig`
- `src/finetune/tools/prepare_gliner2_entity_cleanup_cache.zig`

Recommended cache contents per mention:
- `text`
- `label`
- `start`
- `end`
- `detect_score`
- `keep`
- `preferred_surface`
- `group_id`
- `features`
  - precomputed GLiNER2 mention vector, preferably derived from cached boundary
    or span states

Recommendation:
- store precomputed mention vectors in the cleanup cache instead of replaying
  the full GLiNER2 encoder during cleanup-head training

Why:
- training stays cheap
- artifact format stays simple
- cleanup trainer remains decoupled from GLiNER2 runtime details

### Step 2: Reuse GLiNER2 Boundary Cache Infrastructure

Reuse these existing surfaces:

- `src/finetune/gliner2_boundary.zig`
- `src/finetune/text_encoder_boundary.zig`
- `src/finetune/tools/prepare_gliner2_top_layer_boundary_cache.zig`
- `src/finetune/train/train_eval_gliner2_lora_bundle.zig`

The intended pattern is:

- start from annotated `entity_cleanup/v1` examples
- align each cleanup mention with GLiNER2 token/span indices
- extract a pooled mention representation from the GLiNER2 boundary cache path
- store that vector into the cleanup cache

This gives the cleanup model GLiNER2-native features without changing the core
cleanup contract.

### Step 3: GLiNER2 Cleanup Trainer Entry Point

Implemented:
- `entity_cleanup_model.zig` remains the cleanup-head definition
- `train-eval-entity-cleanup-head` trains the same head on GLiNER2-derived caches

Suggested command shape:

```bash
zig build prepare-gliner2-entity-cleanup-cache -- <base_or_adapter_model_dir> <jsonl_or_dir> <out_json> [split] [backend] [max_examples] [max_length] [max_span_width] [top_layer_count]
zig build train-eval-entity-cleanup-head -- <train_cache.json> <eval_cache.json> <out_dir> [--epochs N] [--learning-rate LR] [--embedding-learning-rate LR] [--embedding-dim N] [--max-mentions N]
```

Current `prepare-gliner2-entity-cleanup-cache` defaults:
- `backend=blas`
- `max_examples=128`
- `max_length=256`
- `max_span_width=8`
- `top_layer_count=1`

`max_examples` must currently be greater than zero. Passing `0` does not mean
"all examples".

### Step 4: Treat Cleanup As A Standard GLiNER2 Supporting Artifact

Expected behavior:
- adapter bundle may contain `entity_cleanup_head.json`
- materialized model output also contains `entity_cleanup_head.json`
- serving continues to load it automatically from model dir

### Step 5: End-To-End Smoke Workflow

Implemented workflow:
- prepare train/eval GLiNER2 cleanup caches
- train/eval the cleanup head
- materialize the recognizer bundle
- emit a single JSON summary

Command:

```bash
zig build run-gliner2-entity-cleanup-smoke-workflow -- \
  /tmp/gliner2_base /tmp/gliner2_adapter \
  /tmp/train_cleanup.jsonl /tmp/eval_cleanup.jsonl /tmp/out \
  train eval blas 64 32 256 8 1 3 0.05 0.01 32
```

## File-Level Task List

- [x] Add `src/finetune/entity_cleanup_gliner_cache.zig`
- [x] Add `src/finetune/tools/prepare_gliner2_entity_cleanup_cache.zig`
- [x] Export the new module from `src/finetune/root.zig`
- [x] Export the new module from `src/termite_internal.zig`
- [x] Add build step `prepare-gliner2-entity-cleanup-cache` in `pkg/inference/build.zig`
- [x] Reuse existing `train-eval-entity-cleanup-head` on GLiNER2-derived caches
- [x] Add delegated root-build steps in repo-root `build.zig`
- [x] Extend `src/finetune/gliner2.zig` to copy `entity_cleanup_head.json` as a supporting artifact
- [x] Add one smoke workflow command for bundled cleanup-head training
- [x] Add focused tests for GLiNER2 cleanup cache generation
- [x] Add focused tests for bundle/materialization propagation of cleanup artifacts

## Training-Ready Definition

This feature is "ready enough to start training on GLiNER2 features" when all
of the following are true:

- `prepare-gliner2-entity-cleanup-cache` exists and emits a stable cache
- the cleanup trainer can consume that cache and emit `entity_cleanup_head.json`
- `materialize-gliner2-lora` preserves `entity_cleanup_head.json`
- recognize / extract / native recognize all apply cleanup from the materialized model dir
- one smoke workflow validates cache prep, cleanup-head training, and materialization

At that point, the cleanup head is no longer just a sidecar experiment. It is a
first-class bundled auxiliary head for GLiNER2.
