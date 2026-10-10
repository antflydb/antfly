# @antfly/web

Local browser inference backed by Antfly's Zig WASM runtime and optional WebGPU.
Import `Inference` from `@antfly/web/inference`. This entrypoint loads inference
assets independently; a future `@antfly/web/db` entrypoint can expose `Database`
without coupling database consumers to inference assets. Local resources use
`Inference` / `Database`; `Client` names are reserved for remote service clients.
The root entrypoint currently re-exports inference for convenience.

The public API separates extraction from decisions. Internal adapters select
execution from configuration and artifact contracts, never model names. Native
Antfly decision parsing, request lowering, response presentation and capability
checks are reused in WASM. Advertised metadata, browser execution support and
production qualification are separate: browser execution is always
`qualified: false` today.

## Build and test

From `ts/`, run `pnpm --filter @antfly/web build`, `typecheck`, and
`test`. The package emits ESM JavaScript and declarations into `dist/`, together
with matching runtime JavaScript, workers, shaders and a compatibility manifest.
`typecheck` also checks positive and negative consumer contract examples.

The existing npm workflow accepts `ts/antfly/web/v*` after an
`@antfly/web` package has been published once and its trusted publisher
is configured for `.github/workflows/ts-npm-publish.yml` (environment `npm`)
with direct publish permission. An npm maintainer must bootstrap that first
release. No package or runtime release is implied by this source change.

## Prepare runtime assets

The npm package bundles runtime JavaScript and shaders. Prepare a complete
runtime directory with the package's command and a matching source checkout:

```sh
pnpm exec antfly-web-prepare \
  --zig-root /path/to/antfly/zig \
  --out ./public/inference \
  --zig /path/to/pinned/zig
```

In this workspace the equivalent is
`pnpm --filter @antfly/web prepare:runtime --zig-root ../../../zig --out /absolute/public/inference --zig /path/to/pinned/zig`.
The compiler must match `scripts/ci/toolchain-policy.json` (currently Zig 0.17.0).
The command checks the checkout's source fingerprint against the package,
builds CPU and WebGPU WASM serially with `-j1`, checks extraction ABI version 2,
and copies all matching assets into
`<out>/<package-version>-<runtime-id-prefix>/`. It checks packaged asset hashes,
checks the source fingerprint again after compilation, and writes the final
manifest with hashes and sizes for both WASM files. It refuses to replace an
existing versioned directory. Build caches and `zig-out` are written in the
source checkout; model weights are acquired separately.

Serve that directory at an immutable, same-origin URL. The client checks the
manifest version, package version, runtime content identity, JavaScript protocol
version and extraction ABI version before loading. Runtime entrypoints carry
the same identity, and the worker rejects a different client identity before
fetching WASM. Mixed client/runtime/worker releases produce
`RUNTIME_INCOMPATIBLE`. The identity covers client source, runtime JavaScript,
shaders and Zig sources; a WASM ABI match alone does not establish compatibility.
The manifest hashes support deployment integrity checks. Preparation establishes
asset compatibility; it does not establish numerical or production qualification.

```ts
import { Inference, downloadCatalogModel } from "@antfly/web/inference";
const assets = "/inference/0.1.0-<runtime-id-prefix>/";
const client = await Inference.create({ assets });
const unsubscribe = client.subscribe((state) => {
  renderStatus(state.status, state.error?.code, state.recovery);
});
// Use the same directory for the cache's hashing worker.
const files = await downloadCatalogModel(catalogEntry, { assets });
await client.loadModel(files, { backend: "auto", precision: catalogEntry.precision });
const result = await client.extract({
  schema_version: 2,
  model: "local",
  inputs: [{ content: "John works at Apple." }],
  schema: { entities: ["person", "organization"] },
  options: { include_spans: true, offset_unit: "utf16_codeunits" },
});
unsubscribe();
client.dispose();
```

Use HTTPS (or localhost) and same-origin assets. WebGPU requires COOP
`same-origin` and COEP `require-corp`; otherwise the loaded model's
`backend` and `fallbackReason` describe its WASM CPU fallback. A CSP needs
`script-src 'self' 'wasm-unsafe-eval'`, `worker-src 'self'` and `connect-src` for
model download hosts. Inference input and local bundles are not uploaded.
Downloaded files are hash checked before reuse from OPFS.

## Tasks and model support

`extract()` / `validateExtraction()` accept extraction requests and return
extraction output / validation geometry. Legacy GLiNER2 uses schema version 1;
boundary and marker-head extraction use version 2. Typed ordinal and predicate
questions belong to `decide()` / `validateDecision()`, using the same named-array
contract as native `/decisions`:

```ts
const result = await client.decide({
  model: "local",
  input: "Please look up the latest documentation.",
  questions: [
    { name: "tool", type: "choice", instructions: "Which tool is needed?",
      choices: [{ value: "search" }, { value: "none" }] },
    { name: "urgency", type: "score", instructions: "How urgent is this?",
      levels: [{ label: "low" }, { label: "high" }] },
    { name: "act", type: "predicate", instructions: "Should we act now?" },
  ],
});
// Single-input responses have answers; inputs: [...] produces data: [...].
if (result.value.answers) renderAnswers(result.value.answers);
```

A trained head returns `decision_method: "typed"` with probabilities/confidence.
The shared contract also describes `embedding_similarity` answers, whose cosine
similarities and abstention policies have distinct semantics. The browser does
not substitute trained heads for embedding-similarity decisions.

`inspectModel(files)` reads model configuration sidecars and optional
`model_manifest.json` without allocating model weights. It fetches the prepared
CPU WASM asset and checks configuration with the same parser used by loading,
in an isolated instance that leaves the active model untouched.
Its `tasks` and `capabilities` retain advertised
metadata, including native config-owned role corrections for Laya and embedding
models; `execution` intersects declared support with the browser adapter.
`availability` reports known architecture/budget blocks with a reason.
`inspectBundle()` additionally checks artifact layout/precision; `loadModel()`
validates tokenizer, tensors and inventory before a model becomes usable.
`model.execution` describes supported tasks, decision kinds and request limits.
A declaration cannot enable an unimplemented browser route. None of these
inspection results establishes numerical or native production qualification.
When a bundle includes `antfly_inference_bundle.json`, loading verifies every
receipt pin, including capability manifests, embedding sidecars and calibration
policies. Missing, duplicated or mismatched pinned files reject the bundle.

| Configuration / model family | Browser tasks | Limits / unavailable routes |
| --- | --- | --- |
| Legacy GLiNER2 span | extract | Supported DeBERTa base geometry; v1 wire |
| GLiNER boundary (including multilingual DeBERTa Decide) | extract, decide | DeBERTa and FP32 ModernBERT backbones, including the optional linear neck |
| GLiNER marker-head Decide | extract, decide | DeBERTa and ModernBERT marker encoders; unused legacy heads are skipped |
| Laya / OpenDecider | decide | ModernBERT vocabulary up to 262,144, including multilingual models; bounded geometry/sequence sizes |
| EmbeddingGemma2 | decide | Text choice and multi-choice, exact artifact identity, prototypes and qualified calibrations; FP32/BF16 |

Support depends on configuration, declared capabilities, tensor layout, selected
precision and request geometry. This table describes adapters, not qualification
of every checkpoint bearing a family name. Browser limits include one input per
request, 256 KiB text, 512 KiB JSON, 64 KiB lowered schema and 16 decision questions.
Trained decisions support choice, score and predicate. EmbeddingGemma2 supports
choice and multi-choice with descriptions/examples, cosine scoring, abstention,
model identity and optional qualified calibration files (`calibrations/<id>.json`).
Multi-choice requires per-choice similarity thresholds or a matching calibration.
Embedding validation tokenizes inputs/prototypes without running the encoder.
The text adapter loads no media towers and does not expose an `embed` method.
Embedding requests allow at most 128 encoded texts, 2,048 tokens per text and
8,192 tokens in total. Source bundles/files are bounded to 3 GiB; model weights
and scratch share a 1.5 GiB host budget within WASM32's 2 GiB memory ceiling.
Large FP32 checkpoints may need compact artifacts to fit that resident budget.
BF16 stays packed and is converted to exact FP32 per matrix or embedding row.

The request's `model` string labels the response; `loadModel()` selects the bundle.
V1 extraction offsets are UTF-8 bytes; v2 reports `offset_unit` per input.
`validateExtraction()` and `validateDecision()` return `{ valid: true,
encoded_tokens }` inside a `ValidationResult`. Invalid requests leave the model
usable. `InferenceClient`, `run()`, `validateRequest()`, `runExtension()` and
`validateExtension()` remain compatibility APIs for the earlier model-specific
wire; new consumers should use `Inference` and the public task methods. Compatibility
methods enforce the same declared task and capability restrictions. Catalog
downloads retain their declarations in `antfly_catalog.json`; when a pinned
model manifest also declares a field, execution uses their intersection.

## Lifecycle and recovery

`client.state` is an immutable snapshot and `client.model` is a readonly getter.
`subscribe(listener)` immediately reports the current snapshot and reports each
change; the returned function unsubscribes. Snapshots describe status, operation,
loaded model, last error and recovery. `operation: "reload"` and progress stage
`reload` make automatic recovery visible. Progress stage names are a stable union.
Listener exceptions are reported through `reportError` where available and do
not fail model operations.

An explicit model switch destroys the active model and clears its saved reload
configuration before inspecting the replacement. If the replacement fails or is
cancelled, no model remains selected and recovery is `none`. A subsequent run
rejects with `MODEL_NOT_LOADED`; retry the switch with `loadModel()`.

`cancel()` destroys the worker and active device. Cancellation during GPU
initialization keeps the GPU locally owned until its initializer settles, then
destroys any device it created. Cancellation of an already loaded model, device
loss, or fatal worker/WASM failure retains that model's bundle for a visible
reload on the next run. Failed automatic reloads retain their own bundle for
another attempt. `unloadModel()` clears the bundle and `dispose()` also disables
the client. Only one model operation may run at a time.

`InferenceError.code` is stable across rejected operations and state snapshots:
`CANCELLED`, `DEVICE_LOST`, `GPU_FAILED`, `RUNTIME_FAILED`,
`RUNTIME_INCOMPATIBLE`, `MODEL_LOAD_FAILED`, `INVALID_REQUEST`,
`MODEL_NOT_LOADED`, `UNSUPPORTED_TASK`, `BUSY`, and `DISPOSED`. Cancellation errors retain the
`AbortError` name. Error messages provide detail, while UI decisions should use
codes and `state.recovery`.

## Qualification

Catalog `qualification: "passed"` describes the catalog publisher's verification
of that pinned checkpoint and its associated evidence. It is independent of
browser runtime qualification and should be presented as **catalog verification
passed**. It does not establish parity, GPU support, performance, or production
readiness on the user's browser/device.

`ModelInfo.qualified` is always `false` because this browser runtime does not
currently have an admitted production qualification profile. Loading a catalog
entry with `qualification: "passed"` does not promote it to `qualified: true`.
Present these as separate facts, for example: “Catalog verification passed;
browser execution unqualified.” Asset compatibility checks also do not change
either qualification status.

Model-free contract tests run with `test`. CPU fixture tests exercise the actual
WASM adapter after preparation:

```sh
cd zig
EXTRACTION_WASM=zig-out/antfly-extraction-cpu.wasm \
  node --test pkg/inference/web/test-{extraction,laya,decide}-runtime.mjs
```

Full checkpoint parity requires `LAYA_MODEL`, `EXTRACTION_MODEL` or
`DECIDE_MODEL` and relevant oracle fixtures; skipped checks do not qualify a
model. GPU kernel/parity tests use a standalone runtime harness, without the
Colony UI:

```sh
cd ts
pnpm --filter @antfly/web exec playwright install chromium
EXTRACTION_GPU=1 pnpm --filter @antfly/web test:browser
```

These GPU tests currently request Chromium's Metal WebGPU backend on macOS.
They fail rather than count CPU fallback as successful GPU execution, except
where the test explicitly verifies a supported fallback. Hardware, full model
and Colony rendered UX qualification remain separate from compatibility and
model-free tests.
