# Laya typed decisions

Laya models answer classification questions without generating text. Antfly serves
prepared Laya checkpoints through `/ai/v1/extract` with `schema_version: 2` and
extraction provider `antfly`. They appear under extractors in model discovery.

Prepare a checkpoint from a local upstream download, or from a pinned Hugging
Face revision:

```sh
uv run scripts/prepare_laya.py convaiinnovations/laya \
  --revision <full-hugging-face-commit-sha> \
  --output ./models/extractors/laya
antfly standalone --models-dir ./models
```

The importer combines the encoder and decision configuration, preserves the
weights and calibration, and copies the tokenizer. It refuses to overwrite an
existing directory. Native support requires a ModernBERT-backed checkpoint with
the Laya decision heads. The runtime validates tensor shapes before execution.
Unprepared upstream checkpoints are not automatically converted by `inference pull`.

Submit a text request to the server's AI endpoint:

```json
{
  "model": "laya",
  "schema_version": 2,
  "inputs": [{"id": "request-1", "content": "Find the document about refunds."}],
  "schema": {
    "classifications": [
      {
        "name": "tool",
        "mode": "single",
        "instruction": "Which tool should handle the request?",
        "labels": ["search", "fetch_document", "no_tool"],
        "label_definitions": {
          "search": {"description": "Find documents matching a topic"},
          "fetch_document": {"description": "Retrieve a document with a known ID"},
          "no_tool": {"description": "Respond without a tool"}
        }
      },
      {
        "name": "urgency",
        "mode": "ordinal",
        "instruction": "How urgent is this request?",
        "labels": ["routine", "soon", "immediate"]
      },
      {
        "name": "tool_needed",
        "mode": "boolean",
        "instruction": "Does answering require retrieving external information?",
        "labels": ["false", "true"]
      }
    ]
  }
}
```

`single` maps to Laya `choice`, `ordinal` to `score`, and `boolean` to `noul`.
Boolean labels must be exactly `false`, then `true`. Each task requires a name,
an instruction (`prompt` is an alias), and 2–20 distinct labels. Ordinal labels
are ordered from lowest to highest; descriptions, when present, supply rubric
text. Per-input `schema` and `options` replace the corresponding shared values.

Each output contains compatible `classifications` entries with the selected
label and its probability, plus `decisions` with:

- The full probability distribution in request label order.
- `expected_value` for ordinal tasks, on the zero-based level scale.
- `true_probability` for boolean tasks.
- `confidence` and `confidence_method`. Choice and ordinal confidence measures
  normalized inverse entropy; boolean confidence is the larger class probability.
- `act_probability`, the auxiliary action-head output.

The action probability does not execute a tool. An agent can consume these
results to select a tool or a bounded argument; application state, another
extractor, or a generator supplies free-form arguments. The application validates
and executes the resulting call.

The current Laya executor accepts text-only classification schemas. It rejects
multi-label tasks, entities, relations, structures, examples, constraints,
windowing, and unsupported options. `top_k`, when supplied, must be 1. Full
probabilities are always available in `decisions`. Entire batches are validated
and tokenized before inference. Inputs that exceed the checkpoint token budget,
including question and option tokens, are rejected instead of silently truncated.
Up to 128 inputs, 64 tasks per input, and 512 total tasks are accepted, subject
to the server's memory and executor limits.

Checkpoint selection is explicit. Benchmark application-specific accuracy and
calibration before choosing thresholds; model confidence does not establish that
a tool choice is correct. This integration does not change the GLiNER v2 executor.

## Reference validation

The deterministic fixture exercises the complete encoder, decision heads,
preprocessing, mixed question batches, calibration, HTTP, and embedded extraction.
It uses upstream source with small randomly initialized weights:

```sh
curl -fsSL https://raw.githubusercontent.com/NandhaKishorM/laya/6a5819129eb220570792e417e49723d697efd76f/laya/common.py -o /tmp/laya-common.py
uv run scripts/laya_reference.py --common /tmp/laya-common.py --output /tmp/laya-reference
cd zig
ANTFLY_LAYA_REFERENCE=/tmp/laya-reference python3 tools/run_bounded_zig_build.py \
  build inference-test -Dmetal=false -Dcuda=false -Donnx=false -- --test-filter 'laya '
```

Parity tests skip when `ANTFLY_LAYA_REFERENCE` is unset. Set `ANTFLY_LAYA_METAL=1`
and build with `-Dmetal=true` to require Metal, including the managed extraction
route. A missing GPU fails the test rather than silently selecting CPU.

The synthetic fixture tests independent and reordered batches through the
512-question pipeline limit. Released-checkpoint qualification additionally
compares token IDs and probabilities against upstream PyTorch on labeled data,
checks accuracy, and measures warm batches through 128 rows. See
[qualification results and reproduction](../design/laya-qualification.md).

These checks qualify native CPU and Metal. CUDA remains unqualified.

## Native finetuning

`antfly inference finetune train laya <job.json>` trains the ModernBERT encoder,
question-type embeddings, transformer head, and marker scorer in FP32. Choose
`cpu` or `metal` explicitly. The default `rlcd` objective follows the upstream
[typed-decisions notebook](https://github.com/NandhaKishorM/laya/blob/main/notebooks/laya_finetune_typed_decisions_2xT4_kaggle.ipynb):
soft-target cross-entropy plus a centered Gaussian policy-gradient estimator
with log, spherical, and ordinal ranked-probability rewards. `soft_ce` selects
cross-entropy alone. This is full finetuning; it does not produce a LoRA adapter.

Prepare the base checkpoint with `scripts/prepare_laya.py` as above. Supply
separate train and evaluation JSONL files, with one decision per line:

```json
{"id":"case-1/tool","group_id":"case-1","text":"Find the refund policy.","kind":"choice","instruction":"Which tool should handle the request?","labels":["search","fetch_document","no_tool"],"descriptions":["Find documents by topic","Retrieve a known document ID","Answer without a tool"],"target":[0.95,0.03,0.02]}
```

`kind` is `choice`, `score`, or `noul`. Targets are probabilities in label order
and must sum to one; one-hot labels are also valid. Ordinal labels run from
lowest to highest. Boolean labels must be `false`, then `true`. Descriptions
are optional. Preprocessing shares the inference tokenizer and formatting,
including explicit rejection of overlength text. No training examples are
silently truncated or discarded.

To convert a JSONL export of `LocalLLaMA/typed-decisions`, including its
JSON-encoded `state`, `questions`, and `gold` columns:

```sh
python3 scripts/prepare_laya_finetune.py typed-decisions-train.jsonl --output train.jsonl
python3 scripts/prepare_laya_finetune.py typed-decisions-eval.jsonl --output eval.jsonl
```

Keep every question from a source case in the same split. The trainer rejects
cross-split ID, group, source-text, and token-sequence overlap. Unlike the
notebook, calibration uses a separate optional `calibration_file`; it never
fits on the training or evaluation examples.

Example `job.json` (all paths must be absolute):

```json
{
  "version": 1,
  "model_dir": "/models/extractors/laya",
  "train_file": "/data/laya/train.jsonl",
  "eval_file": "/data/laya/eval.jsonl",
  "output_dir": "/runs/laya-domain-v1",
  "backend": "metal",
  "objective": "rlcd",
  "epochs": 4,
  "batch_size": 1,
  "gradient_accumulation": 4,
  "encoder_lr": 0.000025,
  "head_lr": 0.0001,
  "head_dropout": 0.1,
  "seed": 42
}
```

The output directory must be new. The run writes `metrics.jsonl`, resumable
`latest.safetensors`, and a final `report.json` containing data/run hashes,
initial/final evaluation metrics, and optimizer progress. A completed run
publishes `model/`, which can be loaded as an ordinary Laya extractor. The
report's `complete` status means training and export completed; assess its
held-out results before deploying the checkpoint.

To resume, keep the original job settings, set `resume_from` to the previous
`latest.safetensors`, and choose a new output directory. Checkpoints bind the
source weights, tokenizer, configuration, datasets, and training settings.
They preserve AdamW moments and partial gradient accumulation. The optional
`stop_after_microbatches` setting saves a resumable checkpoint at a safe boundary
without exporting a serving model. Checkpoints are otherwise saved every
`checkpoint_every_steps` microbatches (default 100) and at epoch boundaries.

The action head remains frozen because these targets do not supervise it.
Changing the encoder can still change action probabilities, so validate those
separately. Old calibration buckets are removed on export. Without a calibration
split, temperatures reset to one. With one, a bounded per-type temperature
search fits only types having at least ten calibration examples.

This training path uses materialized attention and a bounded host
allocator (`max_host_bytes`, default 24 GiB). Metal gradients pass through
explicit host staging before resident AdamW updates; this is not a fully
device-resident training graph. CPU matrix products use system BLAS with FP64
accumulation when available, retaining the portable fallback. Start with small
batches. It does
not implement CUDA/DDP, mixed precision, encoder dropout, or activation
recomputation. The small-model parity and lifecycle tests below are distinct
from application-specific accuracy or throughput qualification.
Full-checkpoint CPU and Metal gradients pass the original tolerance against
a reference using FP64 encoder/head arithmetic and upstream FP32 logits/loss.
The earlier FP32 reference discrepancies were traced to rounding across ReLU
boundaries; the compatibility results are retained. See the
[production qualification report](../design/laya-production-qualification.md)
for held-out quality, resource limits, lifecycle, and serving evidence.

### Training parity and lifecycle checks

```sh
python3 scripts/laya_training_reference.py --common /tmp/laya-common.py --fixture /tmp/laya-reference
cd zig
ANTFLY_LAYA_REFERENCE=/tmp/laya-reference python3 tools/run_bounded_zig_build.py \
  --zig /path/to/zig-0.16.0 build inference-test -Dmetal=false -Dcuda=false -Donnx=false \
  -- --test-filter 'laya training'
```

Create `/tmp/laya-reference` with the forward reference command above first.
Set `ANTFLY_LAYA_METAL=1` and `-Dmetal=true` for the GPU checks; unavailable
Metal fails explicitly. The fixture checks forward logits, the notebook loss,
every parameter gradient, partial-accumulation resume equivalence, and reopening
the exported artifact through the serving loader.

See [measured training validation and limitations](../design/laya-finetuning-validation.md)
for the released-checkpoint smoke run and exported-model parity reproduction.

Training snapshots source weights and tokenizer assets during admission, so
later source-file changes cannot alter the serving export. Optional
`tokenizer_config.json` and `special_tokens_map.json` are preserved when present.
Admission checks the largest padded batch across every split; an oversized
later example fails before a run directory is created. Resume validates both
the optimizer cursor and partial accumulation state. `stop_after_microbatches`
is an absolute position and must be ahead of a resumed checkpoint and within
the configured epochs.
