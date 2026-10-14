# ONNX

Antfly inference runs ONNX models on its own implementation. It does not link
ONNX Runtime or ONNX Runtime GenAI. An `.onnx` file is parsed, converted into
Antfly inference's graph IR, and executed by the same native CPU, Metal, CUDA,
and WASM kernels that serve GGUF and safetensors models.

```
ONNX file → ml.graph.Graph → generic graph runtime → selected backend policy
```

The ONNX protobuf parser and operator converters live in `zig/lib/onnx`
(`onnx_graph` and `onnx_data` modules). The session that wraps a converted
graph is `src/backends/imported_onnx_session.zig`.

## Backend Selection

There is no `onnx` backend. `.onnx` files are always loaded through
`ImportedOnnxSession` on the backend the session manager selects (`native`,
`metal`, `cuda`, or `wasm`). The `onnx` backend name that previously selected
ONNX Runtime is rejected:

- `ANTFLY_INFERENCE_PREFERRED_BACKEND=onnx`, `TERMITE_PREFERRED_BACKEND=onnx`,
  and `ANTFLY_INFERENCE_REQUIRED_BACKEND=onnx` fail server startup with an
  explanation.
- The public `ModelBackend` enum no longer offers `onnx`.
- `--backend onnx` is no longer accepted by the CLI commands.

`ModelFormat` still accepts `onnx`: it selects the ONNX artifact set of a model
repository, which then runs on the native implementation.

## Imported ONNX Execution

`ImportedOnnxSession` does not own an ONNX-specific graph runtime. Its job is to
parse and convert ONNX into Antfly inference's graph IR, build input/output
metadata, and delegate execution to the generic graph runtime described in
[GRAPH.md](GRAPH.md). That runtime lives in `src/graph/runtime.zig`.

The default imported-ONNX path is interpreter-backed for correctness.
Opt-in partitioned execution is available for validating the generic runtime
seam:

```sh
antfly inference embed ~/.antfly/inference/models/antflydb/clipclap --backend metal --graph-runtime partitioned --text "hello world"
```

That path routes the converted graph through the generic partition executor
machinery. It is not yet a fully compiled Metal-resident graph because the
generic Metal partition executor and full op coverage are still graph runtime
work, not ONNX-specific work. `TERMITE_GRAPH_RUNTIME` and
`TERMITE_ONNX_GRAPH_RUNTIME` remain compatibility/default fallbacks for
imported-ONNX tests and local scripts.

For benchmark and CI validation of imported resident paths, use the graph
runtime fail-closed gates documented in [GRAPH.md](GRAPH.md). The CLIPCLAP
benchmark (`zig build bench-clipclap-e2e`) also accepts
`--resident-projection-required`, which rejects encoder/projection fallback
inside the embedding pipeline.

`ModelManager` carries `SessionManager.graph_runtime_strategy` through the
cloned managers used for main model-session loading. This is required for
ClipClap-style bundles: the main text encoder and the projection sidecar must
both use the same graph runtime selection for partition reports, residency
counters, and Metal graph debugging to be meaningful.

The stable session contract exports imported-ONNX outputs as host `Tensor`
values, but imported graph sessions have an internal resident extension for
graph composition. `runResident` returns backend-owned graph tensors and
`runResidentInputs` accepts compatible backend-owned graph tensors. ClipClap
embedding paths use this to keep encoder outputs resident through their
projection sidecars: text performs resident masked mean pooling before
projection, and image/audio perform resident CLS selection before projection.
Only the final projected embedding is exported to host.

Resident values only cross sessions when the backend identity matches exactly.
Imported ONNX sessions make that possible for GPU sidecars by using a
ref-counted backend context. `LoadedModel` loads compatible ONNX sidecars with
the main imported session's context, so Metal encoder outputs can be passed to
projection sidecars without constructing a second unrelated backend owner.

Imported graph partitioning seeds uploadable parameters and constants as
resident for the selected graph backend. For Metal, runtime inputs are also
promoted to device-resident tensors when a Metal partition starts. This removes
storage-only native fallback islands from CLIPCLAP-style ONNX imports; remaining
Metal gaps appear as `metal_host_assisted_ops`, meaning the graph planner can
keep the partition intact but the backend still needs native Metal kernels for
those accepted ops.

## Admission

ONNX artifacts are admitted by encoded size. The native and WASM backends
reserve two host copies while the protobuf is read and converted, then keep one
resident copy. Metal and CUDA reserve one host copy and one backend copy while
device parameters are constructed, then keep the backend copy resident. See
`onnxModelLoadAdmission` in `src/server/model_manager.zig`.

## Removed: ONNX Runtime

Antfly inference previously had an opt-in `-Donnx=true` build that linked ONNX
Runtime and ONNX Runtime GenAI. Release builds never enabled it. It provided:

- direct ONNX Runtime sessions for `.onnx` files, with a CUDA execution provider
- `compile-artifact --backend onnx` whole-model and partition artifacts, and
  `run-artifact` / `generate --backend onnx --compiled-target whole-model`
  execution of those artifacts
- generation from ONNX Runtime GenAI packages (`genai_config.json`) and from
  decoder-only ONNX VLM exports
- Moondream reading through those two paths

All of it was removed with ONNX Runtime. Moondream now reads natively from the
upstream safetensors checkpoint ([models/moondream/MOONDREAM.md](models/moondream/MOONDREAM.md));
ONNX-only Moondream exports fail with `UnsupportedMoondreamArtifacts`. Compiled
whole-model artifacts remain available through the PJRT/XLA backend
([PJRT.md](PJRT.md)) and the Metal whole-model executor ([METAL.md](METAL.md)).
The ONNX quantized-export campaign is preserved in
[docs/design/inference/history/onnx-quantized-status-history.md](../../../docs/design/inference/history/onnx-quantized-status-history.md).
