# Laya typed decisions

Laya models answer classification questions without generating text. Antfly serves
prepared Laya checkpoints through `/ai/v1/decisions` using the model-independent
[typed decision contract](../../reference/decisions.md). They appear under deciders in model
discovery. Standalone typed decisions use `/decisions`; extraction keeps ordinary
classification alongside entities, relations, attributes and structured records.

Prepare a checkpoint from a local upstream download, or from a pinned Hugging
Face revision:

```sh
uv run scripts/laya/prepare_laya.py convaiinnovations/laya \
  --revision <full-hugging-face-commit-sha> \
  --output ./models/extractors/laya
antfly standalone --models-dir ./models
```

The importer combines the encoder and decision configuration, preserves the
weights and calibration, and copies the tokenizer. It refuses to overwrite an
existing directory. Native support requires a ModernBERT-backed checkpoint with
the Laya decision heads. The runtime validates tensor shapes before execution.
Unprepared upstream checkpoints are not automatically converted by `inference pull`.

CUDA execution is available for Laya checkpoints with sequence lengths up to 512
and encoder head dimensions up to 128. It uses FP32 resident weights and bounded
internal batches. The English checkpoint was validated on NVIDIA L4 using fatbin
artifacts. Portable PTX requires a driver compatible with the CUDA toolkit used
to generate it.

For decisions, submit the following to `/ai/v1/decisions` (or pass the same JSON to
`antfly_inference_decide_json` / Rust `Inference::decide`):

```json
{
  "model": "laya",
  "questions": [
    {
      "name": "tool",
      "type": "choice",
      "instructions": "Which tool should handle the request?",
      "choices": [
        {
          "value": "search",
          "description": "Find documents matching a topic"
        },
        {
          "value": "fetch_document",
          "description": "Retrieve a document with a known ID"
        },
        {
          "value": "no_tool",
          "description": "Respond without a tool"
        }
      ]
    },
    {
      "name": "urgency",
      "type": "score",
      "instructions": "How urgent is this request?",
      "levels": [
        {
          "label": "0",
          "description": "Routine"
        },
        {
          "label": "1",
          "description": "Soon"
        },
        {
          "label": "2",
          "description": "Immediate"
        }
      ]
    },
    {
      "name": "tool_needed",
      "type": "predicate",
      "instructions": "Answering requires retrieving external information."
    }
  ],
  "input": "Find the document about refunds."
}
```

The response has a named `answers` array: `choice` and its probability
array, an expected zero-based `score` and labeled probability array, and a
`predicate` true `probability`. Confidence and `confidence_method` remain model
diagnostics. `act_probability` preserves the auxiliary action-head output for
Laya checkpoints; OpenDecider-nano has no action head. Decisions do not execute
a tool. Applications validate arguments and apply their action policy.

Batches use `inputs` with optional IDs; results preserve input order and IDs in
`data` with aggregate token usage. See the [decision guide](../../reference/decisions.md) for
HTTP, CLI, embedded inference and SQL examples. The unreleased extraction
`decisions` output and Boolean decision modes have been removed.

## OpenDecider-nano

[OpenDecider-nano](https://huggingface.co/manjunathshiva/opendecider-nano)
(Apache 2.0) is a 400M decision model on an Ettin encoder, a ModernBERT
architecture, with Laya's per-option `[MASK]` scoring. Antfly serves it
through the same pipeline and endpoints:

```sh
uv run scripts/laya/prepare_opendecider.py manjunathshiva/opendecider-nano \
  --revision <full-hugging-face-commit-sha> \
  --output ./models/extractors/opendecider-nano
```

The importer merges the encoder and the separate head into one checkpoint and
sets `laya.format: "opendecider"`. That format uses OpenDecider's prompt
(`question:` and `input:` prefixes, its option text, "yes" before "no", no
per-option token cap) and its marker head. It has no type embedding, head
layers or action head, so decisions omit `act_probability`. It runs unpacked,
with a 2,048-token budget; longer inputs are rejected, as for Laya.

## Typed-decisions benchmark

The community benchmark for typed-decision models is the
`LocalLLaMA/typed-decisions` test split (400 cases, 2,000 decisions), scored
per decision against the case's gold label. `scripts/laya/prepare_laya_training_data.sh`
writes it as native records (`td/eval.jsonl`) and cases (`td/eval-cases.jsonl`).

```sh
antfly inference finetune eval laya ./models/extractors/opendecider-nano td/eval.jsonl \
  --backend metal --truncate-state --predictions nano.jsonl
uv run scripts/laya/typed_decisions_bench.py report nano.jsonl other.jsonl \
  --gold td/eval-cases.jsonl
```

`--truncate-state` cuts states that do not fit the checkpoint, as upstream
Laya and OpenDecider do; 160 of the 2,000 decisions exceed Laya's 512 tokens.
Serving never truncates. `report` prints accuracy per question type, Brier
score, ECE and a paired bootstrap interval against the first file.
`typed_decisions_bench.py reference` runs OpenDecider-nano's own PyTorch code
for comparison. On Metal, Antfly matches it to 3e-6 in probability and
reproduces its published 0.796.

## Resident Metal inference

Set `ANTFLY_LAYA_METAL_RESIDENT=1` in the serving process environment before
loading the model to keep the encoder, heads, and decision decoding on the GPU.
Restart the process after changing it. Size model and request memory budgets
for the checkpoint precision and padded batch size; once resident loading is
attempted, failures are returned rather than retried on CPU.

## Native finetuning

`antfly inference finetune train laya <job.json>` trains the ModernBERT encoder,
question-type embeddings, transformer head, and marker scorer in FP32. Choose
`cpu` or `metal` explicitly. The default `rlcd` objective follows the upstream
[typed-decisions notebook](https://github.com/NandhaKishorM/laya/blob/main/notebooks/laya_finetune_typed_decisions_2xT4_kaggle.ipynb):
soft-target cross-entropy plus a centered Gaussian policy-gradient estimator
with log, spherical, and ordinal ranked-probability rewards. `soft_ce` selects
cross-entropy alone. This is full finetuning; it does not produce a LoRA adapter.

Prepare the base checkpoint with `scripts/laya/prepare_laya.py` as above. Supply
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
python3 scripts/laya/prepare_laya_finetune.py typed-decisions-train.jsonl --output train.jsonl
python3 scripts/laya/prepare_laya_finetune.py typed-decisions-eval.jsonl --output eval.jsonl
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

Training uses a bounded host allocator (`max_host_bytes`, default 24 GiB);
start with small batches. CUDA/DDP and mixed-precision training are not
implemented.

Reference validation, resident-path measurements, and training parity results
are in [Laya validation and measurements](../../design/inference/history/laya/2026-10-12-validation-and-measurements.md).
