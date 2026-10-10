# @antfly/inference-web

Browser orchestration for GLiNER2, GLiNER2.5, GLiNER2.5-Decide and Laya /
OpenDecider. Tokenization and inference stay in Antfly's Zig runtime.
The playground UI and model catalog live in Colony at
`ts/apps/www-antfly/content/labs/tim-kaye/gliner-playground`.

## Build and test

From `ts/`, run `pnpm --filter @antfly/inference-web build`, `typecheck`, and
`test`. The package emits ESM JavaScript and declarations into `dist/`, together
with matching runtime JavaScript, workers, shaders and a compatibility manifest.
`typecheck` also checks positive and negative consumer contract examples.

The existing npm workflow accepts `ts/antfly/inference-web/v*` after an
`@antfly/inference-web` package has been published once and its trusted publisher
is configured for `.github/workflows/ts-npm-publish.yml` (environment `npm`)
with direct publish permission. An npm maintainer must bootstrap that first
release. No package or runtime release is implied by this source change.

## Prepare runtime assets

The npm package bundles runtime JavaScript and shaders. Prepare a complete
runtime directory with the package's command and a matching source checkout:

```sh
pnpm exec antfly-inference-prepare \
  --zig-root /path/to/antfly/zig \
  --out ./public/inference \
  --zig /path/to/pinned/zig
```

In this workspace the equivalent is
`pnpm --filter @antfly/inference-web prepare:runtime --zig-root ../../../zig --out /absolute/public/inference --zig /path/to/pinned/zig`.
The compiler must match `scripts/ci/toolchain-policy.json` (currently Zig 0.17.0).
The command checks the checkout's source fingerprint against the package,
builds CPU and WebGPU WASM serially with `-j1`, checks extraction ABI version 1,
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
import { InferenceClient, downloadCatalogModel } from "@antfly/inference-web";
const assets = "/inference/0.1.0-<runtime-id-prefix>/";
const client = new InferenceClient(assets);
const unsubscribe = client.subscribe((state) => {
  renderStatus(state.status, state.error?.code, state.recovery);
});
// Use the same directory for the cache's hashing worker.
const files = await downloadCatalogModel(catalogEntry, { assets });
await client.loadModel(files, { backend: "auto", precision: catalogEntry.precision });
const result = await client.run({
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

## Requests, results and model names

`InferenceRequest` and `InferenceResponse` are versioned unions. GLiNER2 uses
`InferenceRequestV1` / `InferenceResponseV1`; GLiNER2.5 uses
`ExtractionRequestV2` / `ExtractionResponseV2`; Decide uses
`DecideRequestV2`; Laya and OpenDecider use `LayaRequestV2` / `LayaResponseV2`.
V1 inference responses use UTF-8 byte offsets. V2 extraction outputs report
`offset_unit` for each input. A v2 result is a union because the loaded model
selects the runtime: test for `"decisions" in result.value.data[0]` to access
Laya's typed choice, score and boolean decisions. Runtime validation rejects
unsupported features for the loaded family.

`validateRequest()` returns a separate `ValidationResult`, whose `value` is
`{ valid: true, encoded_tokens: number }`. Validation failures reject with
`INVALID_REQUEST` and leave the loaded model usable. `runExtension()` and
`validateExtension()` accept `ExtensionInferenceRequest` for advanced or future
wire fields that the public types do not yet describe. Extensions still undergo
strict Zig validation; the escape hatch does not enable unsupported features.

The request's `model` string labels the response. It never selects a bundle or
switches models; call `loadModel()` to select one. `ModelInfo.family` distinguishes
`gliner2`, `gliner25`, `decide`, `laya` and `opendecider`.
`ModelInfo.architecture` describes the runtime as `span`, `boundary` or
`modernbert`. Catalog entries use the same separate fields.

`WeightPrecision` and `InferenceProgress` replace the old generic `Precision`
and `Progress` names. `Request`, `Progress` and `Precision` remain deprecated
type aliases. Update catalog entries to add `family`, and change old architecture
values `decide` to `span`, and `laya` to `modernbert`. This also removes writable
`client.model` and the old third `run(..., validateOnly)` argument; use
`validateRequest()` for validation.

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
`MODEL_NOT_LOADED`, `BUSY`, and `DISPOSED`. Cancellation errors retain the
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
pnpm --filter @antfly/inference-web exec playwright install chromium
EXTRACTION_GPU=1 pnpm --filter @antfly/inference-web test:browser
```

These GPU tests currently request Chromium's Metal WebGPU backend on macOS.
They fail rather than count CPU fallback as successful GPU execution, except
where the test explicitly verifies a supported fallback. Hardware, full model
and Colony rendered UX qualification remain separate from compatibility and
model-free tests.
