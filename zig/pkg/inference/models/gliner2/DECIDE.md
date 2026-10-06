# GLiNER2.5 Decide artifact contract

Antfly's decision runtime targets the published legacy span checkpoint
`fastino/GLiNER2.5-Decide`, Hugging Face revision
`7ee5da4c2415e32259bcdc0b1a7367c32ce8d6f6`.

The reviewed `model.safetensors` is 1,945,828,140 bytes with SHA-256
`40a5a23ff860dc3dff426cecd1048cacdd29c648c96db209dad818e9686dc997`.
Managed pull grants the decision contract only when the weights, DeBERTa-large
geometry, and the five pinned sidecars (`config.json`,
`encoder_config/config.json`, `special_tokens_map.json`, `tokenizer.json`, and
`tokenizer_config.json`) match. A renamed or modified checkpoint does not
inherit support from its path. Pulling the named Decide source with changed
bytes fails rather than falling back to generic GLiNER2 scoring.

## Manifest

Managed pull writes the following executable metadata:

```json
{
  "type": "extractor",
  "tasks": ["extract"],
  "capabilities": ["classification"],
  "inputs": ["text"],
  "gliner_classification_head": "label_marker_mlp"
}
```

An explicitly managed local copy must declare the same fields. The tokenizer
must resolve `[L]` as `gliner_token_l` and `[SEP_STRUCT]` as
`gliner_token_sep_struct`; `[C]` remains the separate structured-choice marker.
The head declaration never implies entity extraction or relation support.

The artifact validator requires the DeBERTa-large geometry (hidden size 1024,
intermediate size 4096, 24 layers, 16 heads, vocabulary 128011) and exactly:

| Tensor | Shape |
|---|---:|
| `classifier.0.weight` | `[2048, 1024]` |
| `classifier.0.bias` | `[2048]` |
| `classifier.2.weight` | `[1, 2048]` |
| `classifier.2.bias` | `[1]` |

Missing, additional, or differently shaped `classifier.*` tensors fail closed.

## Extraction V2 request

The model remains in the `extractors` route because classification is an
Extraction V2 schema head:

```json
{
  "model": "fastino/GLiNER2.5-Decide",
  "schema_version": 2,
  "inputs": [{"content": "Please refund the duplicate charge today."}],
  "schema": {
    "classifications": [
      {"name": "intent", "mode": "single", "labels": ["refund", "support", "sales"]},
      {"name": "urgency", "mode": "single", "labels": ["low", "medium", "high"]}
    ]
  }
}
```

Native and CUDA backends support this head. Each inference row is limited to
512 tokens. The planner keeps every classification task whole and greedily
packs tasks in request order across rows. A task that cannot fit beside the
complete input text is rejected instead of being truncated or split.

For CUDA deployment, configure inference admission above the model's measured
load peak: approximately 1.95 GB host and 4.38 GB backend for the FP32
checkpoint. The default 1.5 GB host budget rejects this model at load time
with `MODEL_RESOURCE_LIMIT`. The handler qualification test supplies explicit
capacity and verifies the endpoint on both native and CUDA.
For the inference server, `--host-budget-mb 3072 --backend-budget-mb 6144
--combined-budget-mb 9216` gives this single model room above the measured
load peak; size shared deployments for all resident models and concurrent work.

## Split GGUF export

```sh
antfly inference export /absolute/path/to/GLiNER2.5-Decide \
  --target gguf \
  --output /absolute/path/to/decide/encoder.gguf
```

The command writes the encoder GGUF plus `gliner_head.gguf`, retains all four
classifier tensors, copies tokenizer/config/manifest sidecars, and writes the
split bundle marker. The same validation runs for `--dry-run` and before any
export output is created.

## FP32 oracle

`scripts/gliner25/generate_decide_oracle.py` generates a canonical fixture from
the reviewed model revision and refuses changed weight bytes. It records the
encoded token IDs, `[L]` positions, raw logits, probabilities, and winners for
two multi-task requests:

```sh
python scripts/gliner25/generate_decide_oracle.py \
  --source-root /absolute/path/to/reviewed/GLiNER2 \
  --model-dir /absolute/path/to/GLiNER2.5-Decide \
  --output /tmp/gliner25-decide-oracle.json

python scripts/gliner25/generate_decide_oracle.py \
  --source-root /absolute/path/to/reviewed/GLiNER2 \
  --model-dir /absolute/path/to/GLiNER2.5-Decide \
  --output /tmp/gliner25-decide-oracle.json \
  --check
```

The checked-in fixture is `src/pipelines/testdata/decide_oracle.json`. Set
`ANTFLY_GLINER_DECIDE_MODEL` to a local copy of the pinned model with the
manifest above to run the parity test. Set `ANTFLY_GLINER_DECIDE_BACKEND=cuda`
to check CUDA; the default backend is native. The fixture records the source
tree digest so regenerated outputs can be tied to the upstream code used.
