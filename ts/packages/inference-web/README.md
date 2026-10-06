# @antfly/inference-web

Browser orchestration for GLiNER2, GLiNER2.5, GLiNER2.5-Decide and Laya /
OpenDecider. Tokenization and inference stay in Antfly's Zig runtime.
The playground UI and model catalog live in Colony at
`ts/apps/www-antfly/content/labs/tim-kaye/gliner-playground`.

## Build and test

From `ts/`, run `pnpm --filter @antfly/inference-web build`, `typecheck`, and
`test`. The package emits ESM JavaScript and declarations into `dist/`.
The existing npm workflow accepts `ts/antfly/inference-web/v*` after its npm
trusted publisher is configured. No package or runtime release is implied by
this source change.

The client and runtime assets must come from the same revision. Build both
artifacts serially, from `zig/` with the repository's pinned Zig toolchain:

```sh
python3 tools/run_bounded_zig_build.py --max-rss-cap 8589934592 -- build inference-wasm -j1
python3 tools/run_bounded_zig_build.py --max-rss-cap 8589934592 -- build inference-wasm -Dwebgpu=true -j1
EXTRACTION_WASM=zig-out/antfly-extraction-cpu.wasm node --test pkg/inference/web/test-{extraction,laya,decide}-runtime.mjs
```

Serve `inference-web.js`, `inference-worker.js`, `webgpu-ops.js`, `runtime/`
and `shaders/` from `zig/pkg/inference/web`, alongside both
`zig-out/antfly-extraction-{cpu,webgpu}.wasm` binaries. Use a versioned asset
directory; do not use unrelated pre-existing binaries as release artifacts.
The npm package contains the client only, not model weights or WASM binaries.

```ts
import { InferenceClient, downloadCatalogModel } from "@antfly/inference-web";
const assets = "/inference/<revision>/";
const client = new InferenceClient(assets);
// Pass the same asset directory to the cache's hashing worker.
const files = await downloadCatalogModel(catalogEntry, { assets });
await client.loadModel(files, { backend: "auto", precision: catalogEntry.precision });
const result = await client.run(request);
client.dispose();
```

Use HTTPS (or localhost) and same-origin assets. WebGPU requires COOP
`same-origin` and COEP `require-corp`; otherwise the client reports a WASM CPU
fallback. A CSP needs `script-src 'self' 'wasm-unsafe-eval'`, `worker-src 'self'`
and `connect-src` for the model download hosts. Inference input and local
bundles are not uploaded. Cancellation destroys the worker/device and reloads
on the next run. Downloaded files are hash checked before reuse from OPFS.

## Qualification

Model-free contract tests run with `test`. CPU fixture tests above exercise the
actual WASM adapter. Full checkpoint parity requires `LAYA_MODEL`,
`EXTRACTION_MODEL` or `DECIDE_MODEL` and relevant oracle fixtures; skipped
checks do not qualify a model. GPU kernel/parity tests use a standalone runtime
harness, without the Colony UI:

```sh
cd ts
pnpm --filter @antfly/inference-web exec playwright install chromium
EXTRACTION_GPU=1 pnpm --filter @antfly/inference-web test:browser
```

These GPU tests currently request Chromium's Metal WebGPU backend on macOS.
They fail rather than count CPU fallback as successful GPU execution, except
where the test explicitly verifies a supported fallback. Hardware and full
model/browser qualification remain separate from the CI WASM build gate.
