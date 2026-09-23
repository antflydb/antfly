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

CUDA execution is available for Laya checkpoints with sequence lengths up to 512
and encoder head dimensions up to 128. It uses FP32 resident weights and bounded
internal batches. The English checkpoint was validated on NVIDIA L4 using fatbin
artifacts. Portable PTX requires a driver compatible with the CUDA toolkit used
to generate it.

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

These checks cover native CPU and Metal. CUDA has a separate
[qualification script](../../scripts/laya_cuda_qualify.py) and
[matched PyTorch performance gate](../../scripts/laya_cuda_performance.py), run
by the [L4 CI workflow](../../.github/workflows/zig-inference-l4-spot.yml).
