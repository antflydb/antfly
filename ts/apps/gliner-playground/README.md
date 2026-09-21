# GLiNER and Laya browser playground

Standalone React/Vite app using `@antfly/design-system` and
`@antfly/inference-web`. Tokenization, schema compilation, encoder execution,
task heads, constraints, long-document merging, and decoding stay in Zig.
No inference server, login, text telemetry, or local-file upload is involved.

## Run locally

From `zig/`, build the two **distinct** browser artifacts serially:

```sh
python3 tools/run_bounded_zig_build.py --max-rss-cap 8589934592 -- build inference-wasm -j1
python3 tools/run_bounded_zig_build.py --max-rss-cap 8589934592 -- build inference-wasm -Dwebgpu=true -j1
```

From `ts/`:

```sh
pnpm install --frozen-lockfile
pnpm --filter @antfly/gliner-playground dev
```

Open `http://127.0.0.1:3101/`. The existing `apps/playground` remains the design
system gallery; this application is separate. `prepare:runtime` copies the
authoritative JS, WGSL, and WASM assets; generated runtime files are ignored.
Build the design-system package first if its `dist/` is absent in a fresh clone.

## Models and qualification

Supported compatibility paths:

| Family | Browser execution | Bundle |
| --- | --- | --- |
| GLiNER2 base-v1 | WASM CPU or WebGPU + WASM | Complete split encoder/head GGUF pair, or full SafeTensors |
| GLiNER2.5 Small / Base | WASM CPU or WebGPU encoder + WASM task heads | Mixed-precision boundary GGUF or FP32 SafeTensors |
| Laya English | WASM CPU; explicit CPU fallback when WebGPU is requested | Dense FP16/FP32 SafeTensors, upstream or native-importer folder |

GLiNER bundles require `config.json`, `encoder_config/config.json`,
`tokenizer.json`, and `tokenizer_config.json`. Preserve nested paths when
selecting the model folder. Boundary GGUFs use the existing native converter
and its integrity receipt; exact tensor inventories, shapes, precision, byte
lengths, duplicate names, and receipt hashes are checked before execution.
GLiNER2 supports the pinned base-v1 inventory, not arbitrary DeBERTa checkpoints.

### Laya typed decisions

Select **Laya English (reference)** for an explicit, hash-verified download
(about 807 MiB including tokenizer files), or select a local Laya folder.
The catalog pins `convaiinnovations/laya` revision
`c5d78730f3493e4fe16d61507ef4b78eef7318cf` (Apache-2.0).
Upstream folders contain `model.safetensors`, `encoder/config.json`,
`rl_agent_config.json`, and `tokenizer/{tokenizer,tokenizer_config}.json`.
The native importer layout has a combined `config.json` containing `laya`,
plus tokenizer files at the root; both layouts use the same inference path.
No Python or remote model code runs in the browser.

The Laya builder supports single choice, ordinal, and boolean questions.
Advanced JSON accepts PR #815's extraction-v2 `classifications` schema,
including instructions and label descriptions. The result view shows the
complete distribution, confidence method, zero-based expected ordinal level,
true probability, and action probability. It never executes tools.
One input and at most 16 questions are accepted per browser request;
questions run serially. The checkpoint's 512-token budget includes the
formatted question and options. Overlength state text is rejected; question
and option token caps follow upstream preprocessing. Entities, relations,
windowing, multi-label decisions, GGUF/Q8/Q4, BF16, and multilingual Laya
are not supported by this browser adapter. WebGPU runs the encoder and decision
heads, including RoPE, sliding-window attention, erf-based GELU, and row gathering.
Tokenization, token/type embedding-row lookup, confidence/action feature assembly,
and final calibration remain on CPU. Only small final features/logits are read
back; encoder activations stay on-device. On macOS Chromium,
this runs through the browser's Metal-backed WebGPU adapter; it does not execute
the native Metal backend or compile Metal shaders to WASM.

Laya retains packed FP16 projection weights on the GPU across requests. The
shader unpacks FP16 storage and accumulates in FP32, without requiring the
optional `shader-f16` feature. Norm/bias vectors remain FP32. GPU weight uploads
are lazy on the first inference, not repeated per layer/request. The existing
1 GiB tracked GPU budget is unchanged; the released checkpoint retains about
705 MiB of GPU buffers. CPU/WASM retains the original weights for ownership and
fallback, so total memory is higher. FP32 Laya bundles and unsupported WebGPU/
isolation explicitly fall back to WASM CPU. Pure WASM CPU remains supported,
but this optimization targets GPU latency rather than CPU throughput.

Shared Laya files and the ModernBERT exact-GELU correction were taken from
[PR #815](https://github.com/antflydb/antfly/pull/815), head
`2d4cf3038972e3e5c75b3beb247483c68a4030e5`. The PR was open when integrated;
the initial integration did not merge its server, SDK, or Metal changes. Native/browser tensor
validation shares the PR's shape contract. Browser adapters and limits are
separate from native serving qualification.
The subsequent residency work imports the native session wiring and FP16 Metal
dispatch fix from PR head `a39a66469dd9af8d2314f97d8d99ec4248f3aa5a`, without
merging server/SDK changes. Native Metal uses provider-owned encoder and head
projection slots across requests, device-side marker gathering, and a batched
head command frame. The same packed checkpoint is used by both backends.

Repeated browser requests also exposed a shared tensor-constructor alignment
bug: typed data must be allocated with its element alignment, not as unaligned
bytes in a request arena. The fix includes an odd-offset arena regression and
a real-model repeatability test.

The intended default catalog is Small Q8, with Base Q8 and GLiNER2 Base Q8.
**These Q8 catalog rows deliberately have no download URLs until immutable
converted artifacts are published.** They are not fake downloads. Local Q8
bundles work now. Pinned upstream FP32 reference downloads are available for
development. Arbitrary Hugging Face IDs and arbitrary remote model URLs are
not accepted by the UI. Downloading is always explicit.

Local compatibility and catalog qualification are different. No model is
marked browser-qualified by this implementation. The native serving release
gate and its policy table are unchanged. GLiNER2.5 enables GPU computation
only within the encoder; task heads, constrained decisions and document
merging stay on CPU. Unsupported adapters or missing isolation headers fall
back to CPU at load time, with the reason displayed.

For an offline Q8 reference bundle, fetch and convert (commands from `zig/`):

```sh
node ../ts/apps/gliner-playground/scripts/fetch-reference.mjs gliner25-small-fp32 /absolute/new/source
zig build inference-gliner25-convert-build -j1
zig-out/bin/antfly-inference-gliner25-convert --model-dir /absolute/new/source --output-dir /absolute/new/small-q8 --precision q8_0
```

The converter never overwrites an existing destination. Check upstream model
licenses and include required attribution before distributing converted files.
Catalog source revisions/hashes come from
`zig/pkg/inference/scripts/gliner25/oracle_manifest.json`; advanced examples
come from the pinned `testdata/gliner25/pipeline_cases.json` fixture.

## API and lifecycle

```ts
import { InferenceClient, detectCapabilities } from '@antfly/inference-web';
const client = new InferenceClient('/inference/');
await detectCapabilities();
await client.inspectBundle(files, 'q8_0');
await client.loadModel(files, { precision: 'q8_0', backend: 'auto', onProgress });
await client.validateRequest(request);
const result = await client.run(request, { signal, onProgress });
client.cancel();
client.unloadModel();
client.dispose();
```

One model and one operation are admitted at a time. Cancel terminates the
worker, rejects pending promises, releases its GPU device, and invalidates the
generation. A subsequent run reloads retained **in-memory file references**;
unload/dispose removes those references. Worker traps or GPU failures also
invalidate the session. Device-loss recovery is a reload, not a silent retry
of the same input on a different backend.

GLiNER2's browser v1 envelope contains `model`, `text`, `task`, `labels`,
`relation_labels`, and optional structure `schema`/threshold controls. It is a
browser adapter to the native pipeline, **not an HTTP v1 wire-compatible SDK**.
GLiNER2.5 accepts the canonical native v2 request unchanged. Advanced JSON
preserves fields and rejects unsupported options rather than stripping them.
Results keep their original offset unit; highlighting converts to UTF-16 only
for presentation. Raw JSON export preserves original offsets.

Limits: WASM32 maximum 2 GiB; model/request allocator 1.5 GiB, leaving staging
headroom; one staged tensor at most 512 MiB; GPU tracked buffers at most 1 GiB
and additionally limited by the device; text 256 KiB; schema 64 KiB; JSON
request 512 KiB; response 4 MiB. Encoded windows are capped at 2048 tokens and
further constrained by the model (GLiNER2 base is 512). Oversize input is
rejected without silent truncation. GLiNER2.5's explicit UI window preset is
256 source words / 32-word overlap / at most 32 windows, using the native
global merger and constraint checks.

Catalog downloads are hashed incrementally in a separate WASM worker. OPFS
stores files and IndexedDB stores verified metadata; cached bytes are
reverified before use. Restricted storage falls back to session-only loading.
Local files and text are never persisted by the app. No SharedArrayBuffer is
needed for CPU mode. WebGPU's synchronous readback bridge needs COOP/COEP and
uses a finite wait timeout.

## Verification

From `zig/` (no model downloads occur in these tests):

```sh
node --test pkg/inference/web/test-extraction-runtime.mjs pkg/inference/web/test-extraction-support.mjs pkg/inference/web/test-model-discovery.mjs pkg/inference/web/test-webgpu-worker-transfers.mjs
EXTRACTION_WASM=zig-out/antfly-extraction-cpu.wasm EXTRACTION_MODEL=/absolute/small-q8 EXTRACTION_PRECISION=q8_0 EXTRACTION_CYCLES=20 EXTRACTION_ORACLE=pkg/inference/testdata/gliner25/pipeline_cases.json node --test pkg/inference/web/test-extraction-runtime.mjs
```

From `ts/`, while the localhost app is running:

```sh
pnpm --filter @antfly/inference-web test
EXTRACTION_MODEL=/absolute/small-q8 EXTRACTION_PRECISION=q8_0 pnpm --filter @antfly/gliner-playground test:browser
EXTRACTION_MODEL=/absolute/gliner2-base-bundle EXTRACTION_PRECISION=q4_k EXTRACTION_GPU=1 pnpm --filter @antfly/gliner-playground test:browser
EXTRACTION_MODEL=/absolute/small-q8 EXTRACTION_PRECISION=q8_0 EXTRACTION_GPU=1 EXTRACTION_ORACLE=/absolute/antfly/zig/pkg/inference/testdata/gliner25/pipeline_cases.json pnpm --filter @antfly/gliner-playground test:browser
pnpm --filter @antfly/gliner-playground build
```

Laya-specific checks (from `zig/`, model files remain outside the repository):

```sh
node ../ts/apps/gliner-playground/scripts/fetch-reference.mjs laya-english-fp16 /absolute/new/laya
EXTRACTION_WASM=zig-out/antfly-extraction-cpu.wasm LAYA_MODEL=/absolute/new/laya node --test pkg/inference/web/test-laya-runtime.mjs
```

For independent numerical comparison, download upstream `laya/common.py` at
revision `6a5819129eb220570792e417e49723d697efd76f`, then run
`uv run pkg/inference/web/laya-reference.py --model /absolute/new/laya --common /absolute/common.py --output /absolute/new/oracle.json`.
Set `LAYA_ORACLE=/absolute/new/oracle.json` on the WASM test to compare the
three typed probability distributions and action probabilities within `5e-5`.
From the app directory, `LAYA_MODEL=/absolute/new/laya node --test test/laya-browser.test.mjs`
checks CPU without isolation, explicit GPU fallback, all three builders,
advanced-JSON preservation, cancellation/reload, privacy and mobile overflow.
Add `EXTRACTION_GPU=1 LAYA_ORACLE=/absolute/new/oracle.json` to require real
WebGPU dispatch, repeated numerical parity, bounded tracked GPU buffers, and
device-loss/reload recovery. Generate a second reference using `--long` on
`laya-reference.py` to exercise sequences beyond the local attention window;
the browser test uses the reference's text automatically.
This focused coverage is not the PR's 192-example native qualification.

Development validation on September 19, 2026: the released English checkpoint
passed repeated WASM requests, with maximum probability error `7.90e-7`
against the independent three-question PyTorch oracle. The Chromium Laya
workflow passed all three decision builders, cancellation/reload, and CPU
execution without isolation headers. Existing GLiNER2 and GLiNER2.5 WebGPU
browser regressions also passed. This was CPU-only evidence; subsequent WebGPU
validation is separate. Laya remains experimental and is not yet cross-browser
or task-accuracy qualified.

Historical streaming-path validation, September 21, 2026 (Chromium, macOS Metal adapter): short
three-question PyTorch reference maximum probability error `6.85e-7`; a second
three-question reference with 272–284 tokens reached `7.75e-7`, including
confidence, ordinal expectation, and action-probability checks at `5e-5`.
Repeated GPU outputs were identical. Tracked GPU buffers peaked at 58,788,864
bytes (56.1 MiB) and returned to zero between requests; this excludes browser,
driver, transient uniform/staging allocations, and retained WASM/CPU weights.
Cancellation/reload and forced device-loss/reload passed. The longer case exposed
and now guards a missing workgroup barrier in shared attention reduction storage.
The model-free `EXTRACTION_GPU=1 node --test test/attention-browser.test.mjs`
checks masked, batched, multi-head attention at lengths 33, 256, 257, 284, and
512 with three repeats each. All 15 cases passed (maximum error `7.30e-8`).
These are focused numerical/lifecycle checks, not full native Metal, Safari,
cross-device, maximum-context model qualification, or a performance benchmark.

Resident-path measurements on the same machine, same English checkpoint and
fixed 32-token choice fixture: the original streaming WebGPU path had a warm
median of 2,500 ms, 1,268,973,568 uploaded bytes, and 113 downloads per request.
The resident path measured 73–76 ms (three warm samples per run), 274,208 uploaded bytes
(embedding activations and small metadata), and three downloads totaling 4,116
bytes. Retained GPU buffers plateaued at 739,604,492 bytes. The 272-token choice
fixture measured 568 ms with 2,269,088 uploaded bytes and the same small readback.
These are local observations, not universal latency promises. GPU probability
error was at most `1.26e-6` on the six short/long decision references. All three
long-input decisions, exact repeated outputs, cancellation/device-loss recovery
passed; tracked GPU peak was 770,289,676 bytes (734.6 MiB).

Reproduce browser timings from the app directory:
`LAYA_MODEL=/absolute/laya LAYA_ORACLE=/absolute/oracle.json LAYA_RESIDENT=1 node --test test/laya-performance.test.mjs`.
The test rejects warm bulk-weight uploads, excess readbacks, residency growth,
and probability errors above `5e-5`. `EXTRACTION_GPU=1 node --test test/laya-kernels.test.mjs test/attention-browser.test.mjs`
adds FP16 odd/tile-tail shapes, RoPE layouts, erf GELU, gather/slice, and masked
30 masked global/local attention cases.

Native Metal probe (from `zig/`, with a prepared native folder containing
combined `config.json`, `model.safetensors`, and tokenizer files):
`ANTFLY_LAYA_MODEL=/absolute/native-laya ANTFLY_LAYA_ORACLE=/absolute/oracle.json ANTFLY_LAYA_BENCH=1 python3 tools/run_bounded_zig_build.py --max-rss-cap 9663676416 -- build inference-test -j1 -- --test-filter 'laya resident Metal'`.
`ANTFLY_LAYA_BENCH=1` uses the C allocator for timing; omit it to use Zig's test
allocator/leak checks. Native timings use pretokenized inputs and exclude model
loading; browser timings include tokenization, so they are not an exact
cross-backend benchmark. Short native Metal cases measured 60–62 ms warm median
(five samples each), with maximum probability error `1.46e-6`. The 272–284-token
native cases measured 408–432 ms, with maximum probability error `4.8e-7`.

`BROWSER=firefox` or `BROWSER=webkit` selects the corresponding installed
Playwright engine. The GPU test on macOS requests the Metal-backed adapter and
fails if execution silently falls back to CPU. CPU tests remove isolation
headers, exercise local inference and cancellation/reload, check responsive
overflow, and assert that no local-workflow request contains a body or goes
off-origin. All four basic task builders are exercised. Unset
`EXTRACTION_MODEL` runs shell and cache checks only. Cache checks cover a
verified hit, corruption/redownload, session-only loading, and cancellation.
These browser tests use the development server for the cache-module import.

Current evidence is development smoke/regression coverage: real GLiNER2 Q4_K
CPU and WebGPU; GLiNER2.5 Small FP32 CPU plus Small/Base Q8 CPU and WebGPU;
ten pinned oracle cases compare entity spans/classification decisions and record/relation
counts; supplementary-Unicode offsets and long documents; 20-cycle Q8 Small
memory plateau. The oracle check is **not** full score/record-field parity or
a launch qualification report.

Remaining launch gates: publish/license-check pinned Q8 artifacts; full
numeric and all-field parity, Q4 qualification, Windows/Edge and
Safari/Firefox runs, accessibility audit, quota/device-loss fault
injection, automated browser qualification CI, and cold-vs-warm
64/256/512-token performance reports. Do not infer release readiness from a passing
smoke test or native Metal benchmark.

## Static deployment

`pnpm --filter @antfly/gliner-playground build` produces `dist/`. The supplied
`public/_headers` describes COOP/COEP, CSP, MIME and privacy headers for a host
supporting that convention. Configure equivalent headers on other hosts and
serve WASM as `application/wasm`; verify CORS on all model download redirects.
There is no app backend to deploy. No public deployment is performed here.
