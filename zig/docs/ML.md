# Traditional ML Inference

Antfly's Zig inference subsystem can serve "traditional" ML models —
tree ensembles (XGBoost, LightGBM, ONNX-ML), linear / logistic regression,
and SVMs — alongside the neural
network paths (ONNX, MLX, CUDA, Native).

The implementation lives in two layers:

- `zig/lib/ml/tabular/` — the engine. Pure, allocator-driven, WASM-clean.
  No I/O. Consumable from antfly DB indexes and enrichers.
- `zig/pkg/inference/src/tabular/` — registry, discovery, HTTP handlers,
  CLI subcommand. Glues the engine into the inference server.

## On-disk IR — `tabular_model.json`

A single, framework-agnostic JSON file describing the full pipeline:

```jsonc
{
  "schema_version": 1,
  "metadata": {
    "name": "iris-classifier",
    "source_framework": "xgboost",
    "task": "multiclass",
    "num_features": 4,
    "num_classes": 3,
    "feature_names": ["sepal_length", "sepal_width", "petal_length", "petal_width"]
  },
  "output": { "activation": "softmax", "num_outputs": 3 },
  "pipeline": [
    { "type": "scaler",  "scaler":  { /* StandardScaler / MinMaxScaler / ... */ } },
    { "type": "imputer", "imputer": { /* mean / median / most_frequent / constant */ } },
    { "type": "tree_ensemble", "tree_ensemble": {
        "objective": "multi:softprob",
        "base_score": 0.0,
        "num_trees": 100,
        "num_features": 4,
        "nodes": {
          "feature_index": [0, -1, -1, ...],
          "threshold":     [2.45, 0, 0, ...],
          "left_child":    [1, -1, -1, ...],
          "right_child":   [2, -1, -1, ...],
          "leaf_value":    [0, -0.5, 0.7, ...],
          "default_left":  [true, false, false, ...],
          "tree_starts":   [0, 17, 34, ...]
        }
      } }
  ]
}
```

The on-disk format is Antfly Inference's framework-agnostic tabular IR. Models
produced by the converter or installed from a hosted IR run unchanged in the
inference runtime.

## Producing models

Two paths, depending on the source format:

| Source | Tool | Status |
| --- | --- | --- |
| XGBoost (JSON) | `antfly inference convert <model.json> -o <dir> --framework xgboost` | Native Zig |
| LightGBM (text) | `antfly inference convert <model.txt> -o <dir> --framework lightgbm` | Native Zig |
| ONNX-ML | `antfly inference convert <model.onnx> -o <dir> --framework onnx` | Native Zig |
| Hosted `tabular_model.json` IR | `antfly inference pull <url> --name <name>` | Validated CLI pull |

`--optimize` runs the dead-leaf-elimination + threshold-precision passes before
writing the IR.

## Serving

Models are auto-discovered from `~/.antfly/inference/ml/<name>/`.
On first run a built-in iris classifier is seeded (`@embedFile`-backed) so
the catalog is non-empty even on a fresh install.

HTTP routes:

```text
POST /ml/v1/predict          # batched prediction
GET  /ml/v1/models           # predictor catalog
```

```sh
curl -s -X POST http://localhost:8080/ml/v1/predict \
  -H 'content-type: application/json' \
  -d '{"model":"iris-classifier","input":[[5.1,3.5,1.4,0.2]]}'
# → {"model":"iris-classifier","task":"multiclass","predictions":[[0.97,0.02,0.01]]}
```

Install hosted IRs with the CLI:

```sh
antfly inference pull https://example.com/models/iris/tabular_model.json \
  --name iris-classifier
```

Install predictors from Hugging Face repos with `--type predictor`:

```sh
antfly inference pull hf:author/repo --type predictor
antfly inference pull hf:author/repo --type predictor --name iris-classifier
antfly inference pull hf:author/repo --type predictor --file nested/model.onnx --framework onnx
```

The Hugging Face pull path installs safe Zig-native formats only:
`tabular_model.json`, ONNX-ML `.onnx`, XGBoost JSON, and LightGBM text.
Pickle, joblib, cloudpickle, and skops artifacts are detected but not loaded;
export them to ONNX-ML or a native tree format before serving them in Antfly
Inference.

Use `--ml-dir <dir>` or `ANTFLY_INFERENCE_ML_DIR` to override the ML predictor
root. `--models-dir` / `ANTFLY_INFERENCE_MODELS_DIR` remains reserved for the
AI model bundle catalog used by `/ai/v1/*`.

Limits: max batch 10 000 rows, max installed model JSON 256 MB, max source
artifact 512 MB for CLI conversion or Hugging Face predictor pulls. These caps
are ingestion guards for the current buffered parsers, not tree engine
constraints. Names are restricted to `[A-Za-z0-9_-]+`. The HTTP API does not
accept model uploads; conversion and pulling are CLI responsibilities.

## In-process use from antfly DB

`lib/ml/tabular` exposes the same `Predictor` interface used by the HTTP
layer. Antfly indexes / enrichers can avoid the HTTP hop and run inference
in-process — see `Predictor.predict` / `Predictor.predictSingle`. A
follow-up will add a `predictor` enricher type analogous to the existing
embedding / summary enrichers.

## Performance

- SoA layout for tree nodes (parallel arrays) keeps a tree walk cache-warm.
- Thresholds pre-converted to `f32` at load time — no f64→f32 in the hot loop.
- Batched single-output predictions use `@Vector(L, f32)` where
  `L = std.simd.suggestVectorLength(f32)` (4 on arm64, 8 on AVX2,
  16 on AVX-512). Lane divergence handled by `@select`.
- NaN tested with `fv != fv` (IEEE-754 property) — no `@call`/branch into
  libm.
- Optimiser passes: dead-leaf elimination, threshold-precision annotation.

## Build / test commands

```sh
cd zig

# Run the engine's unit tests.
zig build lib-ml-tabular-test

zig build inference-test -Dmetal=false     # registry + HTTP handlers
zig build fuzz-tabular-loader              # loader fuzz target
```

## What's wired today

Engine layer (`lib/ml/tabular/`) — **complete and tested.**
- IR types, JSON loader with structural validation
- Scalar + SIMD tree engine (`@Vector` batch path with lane divergence)
- Linear engine, SVM engine (linear / RBF / poly / sigmoid)
- Scaler (standard / minmax / robust / maxabs), Imputer (4 strategies)
- Activations (identity / sigmoid / softmax / exp; numerically-stable softmax)
- Optimiser passes (dead-leaf elimination, threshold-precision annotation)
- Converter modules (XGBoost JSON, LightGBM text, native-Zig ONNX-ML
  parser with inline protobuf reader, auto-detect)
- Top-level build wiring (`ml_tabular` module, `lib-ml-tabular-test` step,
  `fuzz-tabular-loader` step)

Service layer (`pkg/inference/src/tabular/`) — **fully wired, tested end-to-end.**
- `registry.zig` — TTL-based eviction + atomic ref-count + orphan-on-evict;
  predictor lifetime owned by the IR arena (no leak under load/evict)
- `discovery.zig` — scans `<ml-dir>/<name>/tabular_model.json`,
  seeds the builtin iris classifier via `@embedFile`
- `manifest.zig` — optional `model_manifest.json` reader
- `http.zig` — predict handler logic
- `cli.zig` — `antfly inference convert` and URL `pull` subcommands
- OpenAPI extensions (`specs/openapi/inference/api.yaml`) plus matching
  type definitions in the checked-in generated `inference_api` module
- Prometheus metrics: `antfly_inference_endpoint_requests_predict`,
  `_predict_errors_total`, `_predictor_load_total`, `_predictor_evict_total`

End-to-end coverage:
- `zig build lib-ml-tabular-test` — IR / loader / scalar+SIMD tree /
  linear / SVM / preprocessing / optimiser / converter tests
- `zig build inference-test` — registry, http handler logic, name allowlist
- `zig build fuzz-tabular-loader` — loader fuzz target
- `e2e/inference/test_tabular.py` — Python pytest suite that spins up a
  real `antfly inference run`, runs the iris classifier end-to-end, and
  converts + predicts tiny XGBoost / LightGBM fixtures, plus an ONNX-ML
  linear-regressor conversion path.
