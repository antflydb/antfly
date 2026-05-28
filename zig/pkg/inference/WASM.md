# WASM + WebGPU Backend for Termite-Zig

## Context

Termite-zig is a Zig ML inference engine with a clean backend abstraction: `ComputeBackend` VTable in `src/ops/ops.zig` (~35 required ops) and `Session` VTable in `src/backends/session.zig`. Model architectures (BERT, T5, GPT) call ops through the VTable without knowing the backend. Currently three backends exist: BLAS (CPU), MLX (Metal/Apple Silicon), ONNX (runtime).

Goal: enable termite to run in the browser via WASM, with WASM SIMD as the foundation and WebGPU compute shaders as an acceleration layer. Client-side embedding, reranking, and eventually generation without a server.

## Architecture: WASM SIMD Foundation + WebGPU Acceleration

**Key insight**: The existing codebase already has everything needed for a WASM SIMD CPU backend:
- `src/backends/activations.zig` — layerNorm, rmsNorm, gelu, relu, silu, sigmoid, softmax, argmax using `@Vector(8, f32)` which compiles to WASM SIMD automatically
- `src/backends/native.zig` — naive matmul/sgemm fallbacks when `enable_native = false`, pure Zig
- `src/gguf/` — entire GGUF parser is pure Zig, no C deps
- `src/gguf/quant_codec.zig` — dequantization routines, pure Zig
- `src/architectures/bert.zig` — BERT forward pass, pure ComputeBackend calls

**Design**: The WASM backend operates in two tiers:
1. **WASM SIMD (always available)**: All ops implemented in pure Zig. Activations/norms via `activations.zig`. Matmul via vectorized SIMD (improved over current naive fallback). This works everywhere WASM runs.
2. **WebGPU (optional acceleration)**: When `navigator.gpu` is available, heavy ops (matmul, attention) are offloaded to GPU compute shaders via JS bridge. Small ops still run on WASM SIMD (cheaper than GPU round-trip).

This means the WASM build works without WebGPU at all — WebGPU just makes it faster.

## Shared GGUF Quant Dispatch Target

WebGPU should use the same ggml-shaped GGUF quantized matmul dispatch model as
Metal and CUDA. This means Termite owns the generic dispatch policy in Zig; the
browser backend only maps that selected operation to WGSL pipelines.

The target is not to link or port `libggml` into the browser runtime. The target
is to mirror ggml's useful execution shape inside Termite:

- GGUF tensor type and block layout come from `src/gguf/`.
- A backend-agnostic selector maps
  `QuantMatmulShape{ rows, in_dim, out_dim, qtype }` to
  `QuantMatmulKind{ scalar, mmv, small_batch, mm }`.
- WebGPU records both the preferred bucket and the actual shader used, so
  benchmarks can prove that qLen=1 decode is hitting MMV instead of tiled GEMM.
- Every GGUF quant format should flow through one descriptor-driven
  `mul_mat`-style surface, not through independent per-format policy.

WebGPU may have a broader shader table than Metal or CUDA at any given moment,
but the selection vocabulary should stay shared across backends.

## Native Dawn/WebGPU Backend Plan

The browser WebGPU backend and a native Dawn backend should share the same
shader vocabulary and graph planning model, but they are different runtime
targets:

- browser WebGPU: `-Dwasm=true -Dwebgpu=true`, Termite runs as WASM and imports
  synchronous-looking `webgpu` functions supplied by JavaScript
- native Dawn WebGPU: Termite stays a native binary and links Dawn's WebGPU C
  API, using Dawn to target Metal, Vulkan, or D3D12 under the hood

Do not link Chromium into the `termite` binary for this path. Chromium/Electron
remains the hosted app or sidecar path. The native CLI backend should link Dawn
directly.

### Goals

- Add `termite --backend webgpu` for native builds when Dawn is linked.
- Reuse existing WGSL kernels from `web/shaders/`.
- Reuse existing `.webgpu` graph capability and partition-planning logic where
  possible.
- Keep native CPU fallback for unsupported ops and for machines where Dawn
  cannot acquire an adapter/device.
- Keep WebGPU as a portable GPU path, not a CUDA replacement. NVIDIA hardware
  is reached through Vulkan or D3D12, not CUDA/cuBLAS.

### Build Options

Add native Dawn options instead of overloading the current WASM-only
`-Dwebgpu=true` meaning:

```text
-Ddawn=true
-Ddawn-root=/path/to/dawn/install
```

Recommended build option mapping:

- `build_options.enable_webgpu`: keep this as the existing WASM WebGPU import
  flag for now
- `build_options.enable_dawn`: new native Dawn flag
- optional later cleanup: rename the WASM option internally to
  `enable_wasm_webgpu` and keep `-Dwebgpu=true` as the user-facing browser flag

Native link work in `build.zig` / `pkg/inference/build.zig`:

1. Detect `dawn-root` include and lib directories.
2. Add Dawn headers, typically `webgpu/webgpu.h` plus Dawn native headers if
   adapter discovery needs Dawn-specific helpers.
3. Link Dawn shared/static libraries.
4. Link platform GPU dependencies only when required by the packaged Dawn build:
   Metal/Foundation on macOS, Vulkan loader on Linux, D3D12/DXGI on Windows.
5. Add RPATH/install handling for dynamic Dawn builds.

### Backend Plumbing

Files to update:

| File | Change |
|---|---|
| `src/backends/backends.zig` | Add `BackendType.dawn` or map native Dawn to a `webgpu` backend type; include availability, priority, and direct session loading. |
| `src/native_backend_choice.zig` | Allow `webgpu` on native builds when `enable_dawn` is true, or add an explicit `dawn` choice. |
| `src/termite.zig` / `src/termite_internal.zig` | Import the native Dawn compute/backend modules when enabled. |
| `src/architectures/session_factory.zig` | Add `createDawnSession(...)`. |
| `src/backends/session.zig` users | Ensure sessions can report the native WebGPU backend consistently. |
| `src/native_backend_guard.zig` | Add friendly errors for Dawn not built, no adapter, no device, and unsupported platform. |

Preferred user surface:

```text
termite generate MODEL "prompt" --backend webgpu
termite embed MODEL --text "hello" --backend webgpu
```

If ambiguity between WASM and native backends becomes confusing, add
`--backend dawn` as an alias while still reporting graph partitions as
`BackendKind.webgpu`.

### Native Runtime Modules

Add a native compute backend with Dawn-owned resources:

```text
src/ops/dawn_compute.zig
src/backends/dawn_runtime.zig
src/backends/dawn_session.zig
```

`dawn_runtime.zig` should own:

- `WGPUInstance`
- selected `WGPUAdapter`
- `WGPUDevice`
- `WGPUQueue`
- shader modules
- compute pipelines
- bind group layouts
- reusable staging buffers
- persistent weight buffers
- scratch/graph-plan buffers

`dawn_compute.zig` should implement `ops.ComputeBackend` with a native tensor
handle roughly shaped like:

```zig
const DawnTensor = struct {
    buffer: WGPUBuffer,
    byte_len: usize,
    dtype: ops.GraphDType,
    shape: []i64,
    strides: ?[]i64,
    quant_type: ?tensor_types.TensorType,
    owned: bool,
    host_data_valid: bool,
};
```

Required first-pass `ComputeBackend` methods:

- `backendKind` returns `.webgpu`
- `getWeight`
- `freeTensor`
- `toFloat32`
- `fromFloat32Shape`
- `tensorShape`
- `tensorDType`
- `exportTensorData`
- `embeddingLookup`
- `linearNoBias`
- `linear`
- `rmsNorm`
- `layerNorm`
- `gelu` or generic unary activation
- `scaledDotProductAttention` / GQA attention path used by current models
- `reshape2D`, `sliceRows2D`, `sliceLastDim`, `splitLastDim3` as metadata or
  device-copy operations where possible
- `reserveGraphPlanSlots` if the graph partition executor will allocate
  backend-owned scratch buffers

Unsupported hooks should return null or fall back through the existing generic
host path. Correctness first; residency and fusion can come after parity.

### Reusing Existing WebGPU Work

Reusable with minimal changes:

- `web/shaders/*.wgsl`
- `src/graph/webgpu_capabilities.zig`
- `src/graph/webgpu_partition_executor.zig`, if its executor path remains
  backend-interface based rather than JS-handle based
- `src/graph/compiled_webgpu.zig`
- `src/graph/compiled_registry.zig` entries for `.webgpu`
- WebGPU quant dispatch vocabulary and thresholds from `src/ops/wasm_compute.zig`

Reference-only, not directly reusable:

- `web/webgpu-ops.js`: translate its device, pipeline, bind group, upload,
  dispatch, and download logic into Dawn C API calls
- `src/ops/wasm_extern.zig`: browser import ABI only
- `src/ops/wasm_compute.zig`: useful for policy and tensor-residency behavior,
  but its concrete buffer handles are JS-managed `u32` ids
- `web/inference-worker.js`: browser-only workaround for async WebGPU readback

The Dawn backend should compile WGSL at runtime through Dawn/Tint. A later
optimization can pre-package WGSL sources as embedded strings or generated Zig
arrays so the native binary does not need shader files next to it.

### Dawn Operation Mapping

Native equivalents for the current JS `WebGPUOps` responsibilities:

| JS/browser operation | Dawn-native equivalent |
|---|---|
| `navigator.gpu.requestAdapter()` | `wgpuInstanceRequestAdapter` |
| `adapter.requestDevice()` | `wgpuAdapterRequestDevice` |
| `device.createShaderModule({ code })` | `wgpuDeviceCreateShaderModule` with WGSL descriptor |
| `device.createComputePipeline(...)` | `wgpuDeviceCreateComputePipeline` |
| `device.createBuffer(...)` | `wgpuDeviceCreateBuffer` |
| `queue.writeBuffer(...)` | `wgpuQueueWriteBuffer` |
| command encoder + compute pass | `wgpuDeviceCreateCommandEncoder`, `wgpuCommandEncoderBeginComputePass` |
| dispatch | `wgpuComputePassEncoderDispatchWorkgroups` |
| submit | `wgpuQueueSubmit` |
| readback | copy to `MAP_READ` staging buffer, map asynchronously, wait/poll, copy bytes |

Dawn still has async map callbacks. The native backend needs a blocking helper
that submits the copy, polls/waits for completion, and returns only after bytes
are visible to Zig. This is conceptually the native equivalent of the current
browser worker `SharedArrayBuffer + Atomics.wait()` bridge, but it should be
implemented with Dawn's native wait/poll mechanisms.

### Weight Loading and Residency

First-pass model loading can mirror Metal and WASM behavior:

1. Load GGUF/SafeTensors metadata on the host.
2. Upload supported dense and quantized weights into Dawn buffers.
3. Keep weight tensors GPU-resident for hot linear/norm/attention paths.
4. Preserve host metadata and minimal fallback data for unsupported ops.
5. Drop duplicate host quantized payloads only after parity tests prove the
   Dawn path can run the model family without needing that copy.

The target long-term behavior is GPU-primary for supported GGUF quantized
weights, matching the browser WebGPU direction.

### Milestones

1. **Dawn smoke executable**
   - Link Dawn from Zig.
   - Acquire adapter/device.
   - Run one trivial WGSL compute shader.
   - Verify macOS first because local Metal-backed Dawn is easiest to compare
     against the existing Metal backend.

2. **Standalone compute backend skeleton**
   - Add `dawn_compute.zig`.
   - Implement buffer allocation, upload, download, `toFloat32`, and
     `freeTensor`.
   - Add unit tests with small tensors and no model loader.

3. **Matmul parity**
   - Port `matmul_transb.wgsl` and one quantized matmul shader.
   - Implement `linearNoBias` and `linear`.
   - Compare outputs against native CPU for f32/f16 and one GGUF quant format.

4. **Norm and activation parity**
   - Port RMSNorm/LayerNorm/GELU dispatch.
   - Keep activation tensors resident across norm -> linear chains.

5. **Session integration**
   - Add `createDawnSession`.
   - Load a small BERT/encoder model with `--backend webgpu`.
   - Validate embeddings against native/WASM tolerances.

6. **Decoder path**
   - Add GQA/cached-attention support.
   - Keep KV cache buffers resident.
   - Validate a small GPT/Gemma GGUF prompt against native/Metal tolerances.

7. **Graph partition integration**
   - Enable `.webgpu` compiled partition selection on native Dawn builds.
   - Reuse `webgpu_capabilities.zig` for partition decisions.
   - Verify fallback partitions remain native when an op is unsupported.

8. **Packaging**
   - Decide static vs dynamic Dawn distribution.
   - Strip release artifacts and record binary-size deltas.
   - Add install/RPATH handling and platform docs.

### Validation Matrix

Required tests before treating Dawn as more than experimental:

- Dawn device smoke test
- WGSL shader smoke test for each shader family
- `linearNoBias` f32/f16 parity vs native CPU
- one quantized matmul parity test per supported GGUF quant family
- RMSNorm/LayerNorm parity
- attention and GQA cached-attention parity
- BERT embedding end-to-end parity
- small decoder generation parity for qLen=1 and prefill
- fallback behavior when no adapter/device is available
- memory leak/resource lifetime checks for repeated load/unload

Initial status should be explicitly experimental until at least one encoder and
one decoder model family are running end-to-end without accidental host
round-trips in the hot path.

## MVP Scope: BERT Embeddings

BERT (`src/architectures/bert.zig`) uses only **9 ops**:
`getWeight`, `embeddingLookup`, `linear`, `add`, `layerNorm`, `gelu`, `scaledDotProductAttention`, `toFloat32`, `freeTensor`

---

## Implementation Phases

### Phase 1–7: WASM SIMD Foundation ✅ COMPLETE

Phases 1–7 are fully implemented and working. The WASM SIMD backend runs BERT inference end-to-end in the browser.

**Build**: `~/bin/zig build -Dwasm=true wasm` → `zig-out/bin/termite.wasm` (1.2 MB, ReleaseSafe)

**What was built:**

| Phase | Summary |
|---|---|
| 1. Build system | WASM target in `build.zig` with `wasm32-freestanding` / `wasm64-freestanding` + SIMD128. Separate module tree, no libc. |
| 2. Backend registration | `.wasm` added to `BackendKind`. System deps guarded behind `!enable_wasm`. |
| 3. SIMD matmul | `native.zig` naive fallbacks upgraded with target-aware `@Vector` SIMD (4-wide on wasm32, 8-wide native). |
| 4. WASM ComputeBackend | `src/ops/wasm_compute.zig` — full `ComputeBackend` VTable with 30+ ops. Weight storage via `StringHashMap`. |
| 5. Buffer-backed weights | Weight loading from in-memory `[]const u8` buffers in the wasm export shims. Supports SafeTensors (f32/f16/bf16) and GGUF (f32/f16 + quantized via `quant_codec`). |
| 6. WASM entry point | `src/wasm_entry_wasm32.zig` / `src/wasm_entry_wasm64.zig` — profile roots that import the shared export shims. |
| 7. JS glue | `web/termite-web.js` — `TermiteWeb` class with high-level async API. |

**WASM exports:**

| Export | Purpose |
|---|---|
| `init` | Initialize global state |
| `wasm_alloc` / `wasm_dealloc` | WASM linear memory management for JS interop |
| `load_model_gguf` | Parse GGUF model + config JSON, return handle |
| `load_model_safetensors` | Parse SafeTensors model + config JSON, return handle |
| `embed` | BERT forward pass → embeddings |
| `unload_model` | Free model resources |
| `load_tokenizer` | Parse tokenizer.json, return handle |
| `load_tokenizer_gguf` | Derive tokenizer from GGUF metadata, return handle |
| `decode_tokens` | Decode token IDs back to UTF-8 text |
| `tokenize` | Single text → `[CLS] text [SEP]` |
| `tokenize_pair` | Pair → `[CLS] a [SEP] b [SEP]` with token type IDs |
| `unload_tokenizer` | Free tokenizer resources |
| `rerank` | BERT forward + classifier head + sigmoid/softmax scoring |

**JS API (`TermiteWeb`):**

```javascript
const t = new TermiteWeb();
await t.init();                          // load WASM module
const model = await t.loadModel(source); // File, Uint8Array, or URL
const tok = await t.loadTokenizer(src);  // tokenizer.json
const tok2 = await t.loadTokenizer(ggufBytes, { format: 'gguf' }); // derive from GGUF metadata
const config = await t.inferGptConfigFromGguf(ggufBytes);
const text = t.decodeTokens(tok2, [1, 2, 3]);
const emb = t.embed(model, ids, mask);   // Float32Array
const scores = t.rerank(model, tok, query, docs); // Float32Array
t.unloadModel(model);
t.unloadTokenizer(tok);
```

**Browser demo:** `web/index.html` is now a directory-first browser page. It supports:
- local decoder GGUF bundles from a picked `~/.termite/models` directory
- local Hugging Face SafeTensors GPT bundles (`config.json` + `tokenizer.json` + `model.safetensors`)
- local T5/MT5/LongT5 text-to-text bundles
- local BERT-style encoder bundles for single-text embeddings
- remote GGUF URL loading

The page derives GGUF config/tokenizer/chat-template metadata when available, loads local HF tokenizer/config sidecars directly from the picked directory, and switches its action UI between GPT generation, T5-style generation, and encoder embeddings based on the loaded bundle type.

For browser worker-mode WebGPU, serve `pkg/inference/web` with COOP/COEP headers. A minimal local helper lives at `web/dev-server.mjs`; run `node web/dev-server.mjs` from `pkg/inference` and open `http://localhost:8000/index.html`.

**Projector support status:** the web runtime now has in-memory GGUF projector plumbing too: `tensor_store.GgufStore` can own GGUF bytes directly, the web runtime has projector handle slots, `projector_format` can classify projector bytes without a filesystem path, and `TermiteWeb` exposes direct-mode projector helpers:
- `loadProjectorGguf(...)` / `unloadProjector(...)`
- `gptProjectorVisionEncode(...)` for external Gemma 3 projector image tokens
- `gptProjectorImageEncode(...)` for external Gemma 4 image projectors
- `gptProjectorAudioEncode(...)` for external Gemma 4 audio projectors
- `gptGenerateMultimodalGemma4(...)` for projector-backed Gemma 4 image/audio multimodal prefill + decode

Those projector helpers are now wired through the web worker path too, so `TermiteWeb({ worker: true })` can load projector GGUFs and run projector-backed Gemma 3 / Gemma 4 multimodal encode + generation RPCs instead of being limited to direct mode.

`web/index.html` now uses that surface for local discovered GGUF bundles with external `mmproj` sidecars:
- external Gemma 3 projector + image generation
- external Gemma 4 image-projector + image generation
- external Gemma 4 audio-projector + audio generation
- external Gemma 4 image+audio projector + combined multimodal generation

Current limits:
- the Gemma 4 browser page path currently supports one selected image and one selected audio clip at a time
- projector-backed multimodal HF bundle UIs are still follow-on work

### Phase 8: WebGPU Acceleration Layer ✅ COMPLETE (matmul + attention)

Optional GPU acceleration for heavy ops (matmul, attention). Build with `-Dwebgpu=true` to enable.

**Build**: `~/bin/zig build -Dwasm=true -Dwebgpu=true wasm`

Without `-Dwebgpu=true`, the binary has no WebGPU imports and runs pure SIMD. With it, the binary probes `gpu_is_available()` at init; if no GPU, falls back to SIMD transparently.

**How it works:**

1. `wasm_compute.zig` checks `build_options.enable_webgpu and self.use_gpu and size >= threshold` in `linearOp`/`linearNoBiasOp` and `scaledDotProductAttentionOp`
2. Large matmuls dispatch to `gpuSgemmTransB()` which uploads A+B, runs the tiled WGSL shader, downloads result
3. Large attention ops dispatch to `gpuAttention()` which uploads Q+K+V+mask, runs the fused attention WGSL shader (Q@K^T, softmax, @V in one pass), downloads result
4. Small ops and all activations/norms stay on WASM SIMD (cheaper than GPU round-trip)
5. Matmul threshold: 64×768 = ~50K elements; attention threshold: ~50K output elements, max seq_len 512

**Files:**

| File | Purpose |
|---|---|
| `src/ops/wasm_extern.zig` | Zig extern declarations for WebGPU JS bridge (`gpu_attention`, etc.) |
| `src/ops/wasm_compute.zig` | Threshold routing in linear/attention ops, `gpuSgemmTransB` + `gpuAttention` helpers |
| `web/webgpu-ops.js` | `WebGPUOps` class — device init, shader compilation, compute dispatch, worker command handler |
| `web/termite-web.js` | `TermiteWeb` class — direct mode + worker mode with sync GPU downloads |
| `web/inference-worker.js` | Web Worker that runs WASM with sync GPU downloads via `SharedArrayBuffer` + `Atomics.wait()` |
| `web/shaders/matmul.wgsl` | Tiled 16×16 matmul: `C[M,N] = A[M,K] @ B[K,N]` |
| `web/shaders/matmul_transb.wgsl` | Tiled 16×16 matmul with B transposed (used by BERT linear layers) |
| `web/shaders/attention.wgsl` | Fused attention: Q@K^T, scale, mask, softmax, @V — one workgroup per (head, batch, query) |

Activations, norms, and element-wise ops stay on WASM SIMD (faster than GPU round-trip for these sizes).

### Phase 9: Synchronous GPU Downloads ✅ COMPLETE

Worker-based architecture for correct synchronous GPU buffer reads.

**Problem:** WebGPU's `staging.mapAsync()` is async, but WASM is synchronous — `gpu_download()` must return data immediately. The previous `flush()` approach couldn't work because the Zig code reads the download result within the same synchronous call stack.

**Solution:** Run WASM in a dedicated Web Worker. GPU device stays on the main thread. Downloads use `SharedArrayBuffer` + `Atomics.wait()`:

```
WASM Worker                          Main Thread (GPU)
===========                          ================
gpu_download(id, ptr, size)
  → reset sync flag
  → postMessage({cmd:'download'})
  → Atomics.wait(flag, 0)           receives message
     [blocked]                       creates staging buffer
                                     submits copy command
                                     await mapAsync()
                                     copies to SharedArrayBuffer
                                     Atomics.store(flag, 1)
                                     Atomics.notify(flag)
  ← wakes up                        
  ← copies SAB → WASM memory
  ← returns to Zig code
```

**Requirements for worker mode:**
- Page must be cross-origin isolated (COOP/COEP headers) for `SharedArrayBuffer`
- WebGPU available on main thread

**Usage (worker mode with sync GPU):**

```javascript
import { WebGPUOps } from './webgpu-ops.js';
import { TermiteWeb } from './termite-web.js';

const gpu = new WebGPUOps();
await gpu.init();

const t = new TermiteWeb();
await t.init('termite.wasm', { gpu, worker: true });

const model = await t.loadModel('model.gguf', config);
const emb = await t.embed(model, ids, mask, 1, 128);  // async in worker mode
```

**Direct mode (no worker, SIMD only) still works:**

```javascript
const t = new TermiteWeb();
await t.init('termite.wasm');  // no gpu, no worker — pure SIMD
const emb = t.embed(model, ids, mask, 1, 128);  // sync
```

### Phase 10: CLIP Embeddings ✅ COMPLETE

Multimodal text+image semantic search in the browser.

**New op:** `causalSelfAttention` — per-head attention with causal mask (`if (ki > qi) score = -inf`). Optional additive `attn_bias`. SIMD dot products.

**WASM exports:** `load_model_clip`, `clip_embed_text`, `clip_embed_image`, `preprocess_image`

**JS API:** `loadClipModel()`, `clipEmbedText()`, `clipEmbedImage()`, `preprocessImage()`

**Image preprocessing:** `lib/image/processing.zig` handles resize (bilinear, SIMD-optimized) + normalize → CHW f32 in WASM. JS does decode only (Canvas API → RGBA bytes).

**E2E test:** `web/test-clip-wasm.mjs` — load model, text embedding, image embedding, cosine similarity.

### Phase 11: Whisper Transcription ✅ COMPLETE

Client-side speech-to-text.

**New ops:** `conv1d` (mel frontend), `crossAttention` (asymmetric Q[dec_seq] × K/V[enc_seq] with encoder padding mask).

**WASM exports:** `load_model_whisper`, `whisper_encode`, `whisper_decode`, `audio_whisper_mel_interleaved`, `audio_whisper_mel_size`

**JS API:** `loadWhisperModel()`, `whisperEncode()`, `whisperDecode()`, `preprocessWhisperAudio()`

**Audio preprocessing:** `lib/audio/` provides pure Zig WAV decode, resample, log-mel spectrogram with configurable `AudioConfig`. Browser path: JS `AudioContext.decodeAudioData()` for MP3/AAC/OGG, then PCM to WASM for mel.

**Autoregressive decode:** JS-managed loop — `whisperEncode` once, then `whisperDecode` repeatedly with greedy argmax. Enables progress callbacks, cancellation, streaming partial transcripts.

**E2E test:** `web/test-whisper-wasm.mjs` — load model, encoder forward, decoder forward, greedy decode loop (5 steps).

### Phase 12: CLAP Audio Embeddings ✅ COMPLETE

Audio+text multimodal search in the browser.

**New op:** `conv2d` — Two paths: groups=1 (im2col + matmul), depthwise (direct convolution with SIMD accumulation).

**WASM exports:** `load_model_clap`, `clap_embed_text`, `clap_embed_audio`, `audio_clap_features_interleaved`, `audio_clap_feature_size`

**JS API:** `loadClapModel()`, `clapEmbedText()`, `clapEmbedAudio()`, `preprocessClapAudio()`

**E2E test:** `web/test-clap-wasm.mjs` — load model, text embedding, audio embedding, cosine similarity.

### Phase 13: Florence-2 OCR ✅ COMPLETE

Client-side OCR, image captioning, object detection.

**New ops:** `tokenGridConv2d` (reshape + conv2d for DaViT patch embed), `windowedSelfAttention` (pad → window → per-window multi-head attention → unpad for DaViT spatial blocks), `channelSelfAttention` (attention over channel dimension for DaViT channel blocks).

**WASM exports:** `load_model_florence`, `florence_encode`, `florence_decode`

**JS API:** `loadFlorenceModel()`, `florenceEncode()`, `florenceDecode()`

**Autoregressive decode:** Same JS-managed loop as Whisper — `florenceEncode` once, then `florenceDecode` repeatedly.

**E2E test:** `web/test-florence-wasm.mjs` — load model, encoder forward (vision + prompt), decoder forward, greedy decode loop (5 steps).

### Phase 14: WebGPU Acceleration for New Ops ✅ COMPLETE

GPU shader dispatch for the bottleneck ops added in Phases 10-13.

**New WGSL shaders:**

| Shader | Purpose |
|---|---|
| `web/shaders/causal_attention.wgsl` | Causal self-attention with built-in `if (ki > qi) → -inf` mask. No external mask buffer. Used by CLIP text encoder, Whisper/Florence decoder. |
| `web/shaders/cross_attention.wgsl` | Asymmetric encoder-decoder attention. Q from `[batch, dec_seq, H]`, K/V from `[batch, enc_seq, H]`. Encoder padding mask as u32 array. Used by Whisper/Florence decoder. |

**GPU dispatch in `wasm_compute.zig`:**
- `causalSelfAttentionOp` → GPU when output >= threshold, seq_len <= 512, no attn_bias
- `crossAttentionOp` → GPU when output >= threshold, enc_seq <= 512
- `conv2dOp` (groups=1) → im2col on CPU, matmul via `gpuSgemmTransB` when output >= threshold

**New externs in `wasm_extern.zig`:** `gpu_causal_attention` (no mask param), `gpu_cross_attention` (asymmetric dec_seq/enc_seq).

### Phase 15: ONNX Runtime Web Integration ✅ COMPLETE

Optional support for running ONNX-only models (e.g. mxbai-rerank-base-v1) alongside native `termite.wasm`. Purely JS-side — no Zig changes needed.

**Architecture:** Two independent WASM modules on the same page:
- `termite.wasm` (~1.2 MB) — native Zig backend for SafeTensors/GGUF models
- `ort-wasm-simd-threaded.wasm` (~6 MB gzipped) — pre-built ONNX Runtime from npm `onnxruntime-web`

**JS API (lazy, optional — zero cost if not used):**
- `initOnnx(ort, options)` — takes the `onnxruntime-web` module as argument
- `loadOnnxModel(source, options)` — loads `.onnx` into `InferenceSession`, returns handle
- `onnxInfer(handle, feeds)` — runs inference, accepts `{data, dims, type}` objects
- `unloadOnnxModel(handle)` — releases session

```javascript
import * as ort from 'onnxruntime-web';
const t = new TermiteWeb();
await t.init('termite.wasm');
await t.initOnnx(ort, { wasmPaths: '/wasm/' });
const session = await t.loadOnnxModel('reranker.onnx', {
  executionProviders: ['webgpu', 'wasm'],
});
const result = await t.onnxInfer(session, {
  input_ids: { data: ids, dims: [1, 128], type: 'int64' },
});
```

### Phase 16: GPT Generative Support ✅ COMPLETE

Browser-based text generation for GPT-2, LLaMA, Mistral, Phi, Qwen2, Gemma, Falcon, OPT, BLOOM.

**New ops:** `rope` (rotary position embeddings), `ropePerItem` (per-item positions for KV-cached decode), `gqaCausalAttention` (grouped query attention with causal mask), `gqaPagedAttention` (paged attention wrapper).

**WASM exports:** `load_model_gpt` (SafeTensors), `load_model_gpt_gguf` (GGUF), `gpt_forward` (full recompute, no KV cache)

**JS API:** `loadGptModel()`, `gptForward()`, `gptGenerate()` (autoregressive decode loop with JS-side sampling: temperature, top-k, top-p, repetition penalty, EOS stopping, `onToken` callback). When loading GGUF from a whole buffer, `loadGptModel()` can now derive GPT config JSON directly from GGUF metadata if `config` is omitted.

**Key implementation details:**
- GPT-2 Conv1D weights (`c_attn.weight`, `c_fc.weight`, etc.) are stored as `[in_dim, out_dim]` in SafeTensors but the architecture expects `[out_dim, in_dim]`. Transposed during loading via `isConv1dWeight` check.
- `gpt.zig` required freestanding guards: `monotonicNowNs()` returns 0, `getenvBool()` returns false, debug print functions early-return on `wasm32-freestanding`.
- GPT-2 pre-computed causal masks (`h.*.attn.bias`) are skipped during weight loading (48MB saved).
- Full recompute mode (no KV cache) — each generation step recomputes the entire sequence. Adequate for small models (GPT-2). Phase 19 will add KV cache.

**E2E test:** `web/test-gpt-wasm.mjs` — load GPT-2 model + tokenizer, forward pass shape check, greedy decode, multi-step generation.

### Phase 17: f16 Weight Storage ✅ COMPLETE

Halves weight memory for f16 SafeTensors models.

**Design:** `WasmBuf` extended with `f16_data: ?[]f16` field. `registerF16Weight()` stores raw f16 values. `viewF32()` returns `{ data: []const f32, allocated: bool }` — borrowed for f32 buffers, heap-allocated dequant copy for f16/quantized. All ops that read weights (`linear`, `linearNoBias`, `embeddingLookup`, `layerNorm`, `rmsNorm`, `toFloat32`) use `viewF32()`.

**Loading:** SafeTensors `.f16` weights are stored as-is via `copyToF16()` (zero-cost at load time). bf16 weights are converted to f32 (lossless f16 would lose precision). All model loaders (BERT, CLIP, Whisper, CLAP, Florence, GPT) updated via shared `registerSafetensorsWeight()` helper.

**Memory savings:** GPT-2 small: 496MB → 248MB. LLaMA-3.2-1B: 4.8GB → 2.4GB (now fits WASM).

### Phase 18: Quantized GPU Kernels ✅ COMPLETE

Run Q4_0/Q8_0 GGUF models without f32 expansion.

**Design:** `WasmBuf` extended with `quant_raw: ?[]const u8` and `quant_type: ?TensorType` fields. `registerQuantizedWeight()` stores raw quantized bytes. `viewF32()` dequantizes on-the-fly via `quant_codec.dequantizeToFloat32()` as a CPU fallback.

**GPU path:** When WebGPU is available, quantized weight matmuls skip CPU dequantization entirely — raw quantized bytes are uploaded to GPU, and WGSL shaders dequantize in-register during the matmul inner loop.

The long-term WebGPU path should not choose shaders from private size
thresholds. It should consume the shared GGUF quant matmul selector used by
Metal and CUDA, then choose the available WGSL pipeline for that
`QuantMatmulKind`. For example, qLen=1 decode should prefer an MMV shader such
as `matmul_transb_q4_0_mmv.wgsl` once available; wider prompt shapes should
continue to use tiled MM/GEMM shaders.

**New WGSL shaders:**

| Shader | Purpose |
|---|---|
| `web/shaders/matmul_transb_q4_0.wgsl` | Q4_0 dequant+matmul. 18 bytes/block (f16 scale + 32 × 4-bit nibbles). |
| `web/shaders/matmul_transb_q8_0.wgsl` | Q8_0 dequant+matmul. 34 bytes/block (f16 scale + 32 × int8). |

**New externs:** `gpu_matmul_transb_q4_0`, `gpu_matmul_transb_q8_0`

**Memory savings:** LLaMA-3.2-1B (Q4_0): 4.8GB → ~670MB (7x). Phi-2 (Q4_0): 10.8GB → ~1.5GB.

### Phase 19: KV Cache ✅ COMPLETE

Constant-time per-token decode instead of O(N) full recompute.

**Design:** `WasmKvCache` struct — contiguous per-layer K/V arrays pre-allocated to `max_len`. During forward pass, each layer's `gqaPagedAttentionOp` appends new K/V to the cache and attends against the full history. `gqaCachedAttention` handles asymmetric Q (1 token) vs KV (full history) lengths.

**Flow:**
1. **Prefill**: `gpt_forward_cached(model, cache, prompt, 1, prompt_len)` — processes all prompt tokens, stores K/V in cache, returns logits for all positions.
2. **Decode**: `gpt_forward_cached(model, cache, [token], 1, 1)` — processes single token, appends K/V, attends against full cache, returns logits for new token only.

**WASM exports:** `gpt_create_kv_cache(model, max_len)`, `gpt_forward_cached(model, cache, ids, ...)`, `gpt_reset_kv_cache(cache)`, `gpt_free_kv_cache(cache)`

**JS API:** `gptCreateKvCache()`, `gptForwardCached()`, `gptResetKvCache()`, `gptFreeKvCache()`, `gptGenerateCached()` (autoregressive loop with KV cache)

**Performance:** GPT-2 decode step: ~29ms (cached) vs 95-160ms (full recompute, growing with seq_len). KV cache memory: ~2MB for GPT-2 (12 layers, 12 heads, 64 head_dim, 256 max_len).

**E2E test:** `web/test-gpt-wasm.mjs` Test 6 — verifies cached generation produces identical tokens to full recompute.

### Phase 20: Weight Streaming ✅ COMPLETE

Progressive model loading for large models.

**Design:** Two-phase model creation:
1. `create_model_gpt(config_json)` — creates empty model with config only, returns handle
2. `register_weight(handle, name, data, rows, cols, dtype)` — registers single weight tensor by name

**dtype values:** 0=f32, 1=f16, 2=bf16, 3=f16_transposed (Conv1D), 4=f32_transposed (Conv1D)

**JS API:** `streamLoadGptModel(url, config, options)` — parses the SafeTensors header first, then streams and registers one weight at a time with progress callbacks instead of materializing the whole file in JS memory when `ReadableStream` is available. This path is shared by both direct mode and the worker/WebGPU host path.

```javascript
const model = await t.streamLoadGptModel('/model.safetensors', config, {
  onProgress: ({ loaded, total, currentWeight, weightsLoaded, weightsTotal }) => {
    console.log(`${weightsLoaded}/${weightsTotal}: ${currentWeight}`);
  },
  signal: abortController.signal,
});
```

**WASM exports:** `create_model_gpt(config_ptr, config_len)`, `register_weight(handle, name_ptr, name_len, data_ptr, data_len, rows, cols, dtype)`

**E2E test:** `web/test-gpt-wasm.mjs` Test 7 — incrementally registers all GPT-2 weights, verifies forward pass matches bulk-loaded model.

#### Streaming GGUF Registration

Header-first GGUF registration for decoder-only models.

**Design:** `create_model_gpt(config_json)` still creates the empty model, but GGUF weights are now streamed and registered tensor-by-tensor through `register_weight_gguf(...)`, preserving quantized tensor storage instead of dequantizing in JS. The browser runtime parses GGUF metadata/tensor info just far enough to compute alignment, tensor offsets, tensor types, and byte lengths, then streams the tensor payloads incrementally.

**JS API:** `streamLoadGgufModel(url, config, options)` — available in both direct mode and worker mode, with progress callbacks. The stream path now also surfaces GGUF header metadata through `options.onMetadata`, including `tokenizerJson` and `chatTemplate` when the header contains enough tokenizer metadata to reconstruct them without a second fetch.

**Tokenizer convenience:** `streamLoadGgufModel(..., { autoLoadTokenizer: true, onTokenizerLoaded })` now uses that derived `tokenizerJson` to install the tokenizer automatically and reports the resulting `tokenizerHandle` without a second GGUF fetch.

**Current limitation:** streamed GGUF still requires explicit config JSON.

### Phase 21: Tiled Quantized Matmul Shaders ✅ COMPLETE

3-5x GPU matmul speedup for Q4_0/Q8_0 models via shared memory tiling.

**Design:** Rewrote `matmul_transb_q4_0.wgsl` and `matmul_transb_q8_0.wgsl` with `TILE_M=16, TILE_N=16, TILE_K=32` tiling. `TILE_K=32` matches exactly one quantization block (32 values). Each K-tile step loads A (f32) and B (quantized) cooperatively into shared memory, dequantizing B on load. Inner loop accumulates `sum += tile_a[row][i] * tile_b[col][i]` for 32 iterations.

**Shared memory:** `tile_a[16*32] + tile_b[16*32] = 1024 f32 = 4KB`. Well under the 16KB workgroup limit.

No changes to `webgpu-ops.js` — pipeline names, bind group layout, and dispatch grid are unchanged.

### Phase 22: WebGPU GQA Attention Shader ✅ COMPLETE

GPU-accelerated grouped-query attention for LLaMA, Mistral, Phi, Qwen2, Gemma.

**New shader:** `web/shaders/gqa_causal_attention.wgsl` — 256-thread workgroups, MAX_SEQ=512. Mirrors `causal_attention.wgsl` structure but with asymmetric head counts: Q stride = `num_heads * head_dim`, K/V stride = `num_kv_heads * head_dim`, K/V head index = `h / heads_per_group`.

**Params:** `seq_len, num_heads, num_kv_heads, head_dim, scale, _pad` (24 bytes). Dispatch: `(num_heads, batch, seq_len)` workgroups.

**GPU dispatch:** `gqaCausalAttentionOp` dispatches to GPU when `batch * seq_len * num_heads * head_dim >= threshold`, `seq_len <= 512`, and `attn_bias == null`. MHA fast-path (num_kv_heads == num_heads) uses existing `causalSelfAttention` shader.

**New externs:** `gpu_gqa_causal_attention` in `wasm_extern.zig`.

### Phase 23: GPU-Resident KV Cache ✅ COMPLETE

Eliminates O(seq_len) K/V upload per decode step. ~10x speedup for long-sequence generation.

**Problem:** Every GPU attention call during generation uploaded the full K/V history. For seq_len=1024, kv_dim=4096: ~32MB uploaded per token.

**Design:** `GpuKvCache` struct in `wasm_compute.zig` holds persistent GPU buffer IDs (`k_gpu: []u32`, `v_gpu: []u32`) per layer. Pre-allocated to `max_len * kv_dim * 4` bytes.

**New extern:** `gpu_write_buffer_at_offset(id, offset_bytes, ptr, size_bytes)` — writes to a specific offset within an existing GPU buffer (maps to `device.queue.writeBuffer(buf, offset, src)`).

**New shader:** `web/shaders/gqa_cached_attention.wgsl` — asymmetric Q/KV lengths. `MAX_KV=2048`, shared memory `scores[2048] + reduce_buf[256]` = 9KB. Causal mask: query at position `qi` has absolute position `kv_len - q_len + qi`.

**Flow:**
- **Prefill**: Create GPU cache buffers, upload full K/V, dispatch cached attention, set `cached_len`
- **Decode**: Upload only new token's K/V at offset `cached_len * kv_dim * 4`, dispatch with `q_len=1, kv_len=cached_len+1`, increment `cached_len`

Falls back to CPU for kv_len > 2048.

### Phase 24: T5 Encoder-Decoder ✅ COMPLETE

Text-to-text models (translation, summarization) in the browser.

**New ops in `wasm_compute.zig`:**
- `relativePositionBiasOp` — T5 relative position bias. Reads `table[num_heads, num_buckets]`, maps `(qi, ki)` pairs through log-spaced `t5RelativePositionBucket()` function, outputs `[num_heads, q_len, k_len]` bias. Supports bidirectional (encoder) and unidirectional (decoder).
- `scaledDotProductAttentionOp` updated to apply `attn_bias` (was previously ignored as `_: ?CT`). After computing `scores[qi * seq_len + ki] = dot * scale`, adds `bias[h * seq_len * seq_len + qi * seq_len + ki]`.

**WASM exports:** `load_model_t5`, `t5_encode`, `t5_decode`

**JS API:** `loadT5Model()`, `t5Encode()`, `t5Decode()`, `t5Generate()` (encode once, then autoregressive decode loop with greedy sampling)

No decoder KV cache — full recompute per step. Correctness first.

**E2E test:** `web/test-t5-wasm.mjs` — load T5-small, encoder forward, decoder forward (single step), greedy generation (5 tokens).

### Phase 25: Vision-Language Models (Gemma3 Multimodal) ✅ COMPLETE

Image understanding + text generation in the browser.

**Three JS-orchestrated stages:**
1. **Image preprocess** (JS): `preprocessImageBrowser()` — canvas resize + normalize → CHW f32
2. **Vision encode** (WASM): `gpt_vision_encode()` — SigLIP tower → projected image embeddings
3. **Multimodal decode** (WASM): Embed tokens, inject image features at placeholder positions, build bidirectional attention mask for image segments, run GPT forward

**WASM exports:**
- `gpt_vision_encode(model, pixel_values, len, batch, out)` — runs `gemma3_vision.encodeProjectedImageTokens()`
- `gpt_forward_multimodal(model, ids, img_emb, offsets, batch, seq, out)` — embed + scale + inject images + attention mask + `forwardFromEmbeddings()`
- `gpt_forward_cached_multimodal(model, cache, ids, img_emb, offsets, batch, seq, out)` — same with KV cache for generation

**JS API:** `gptVisionEncode()`, `gptForwardMultimodal()`, `gptForwardCachedMultimodal()`, `gptGenerateMultimodal()` (prefill with images, then standard decode loop)

**E2E test:** `web/test-gemma3-vision-wasm.mjs` — export checks, image preprocessing, vision encode, multimodal forward, cached generation (gracefully handles models too large for 4GB WASM).

### Phase 26: T5 Decoder KV Cache ✅ COMPLETE

O(1) per-token T5 decoding instead of O(n²) full recompute.

**Design:** T5 has two attention types per decoder layer — self-attention (Q/K/V from decoder) and cross-attention (Q from decoder, K/V from encoder). Self-attention reuses the existing `WasmKvCache` + `gqaPagedAttention` (MHA is GQA with `num_kv_heads == num_heads`). Cross-attention K/V are constant across all decode steps (they depend only on encoder output), so they're computed once on the first decode step and stored in a new `T5CrossCache`.

**New structs in `t5.zig`:**
- `T5CrossCache` — per-layer `?[]f32` slots for cross-attention K/V. `init()`, `deinit()`, `reset()`.
- `T5DecodeContext` — `cached_len`, `total_kv_len`, pointer to `T5CrossCache`.

**Position bias with offset:** During cached decode, the query is at absolute position `cached_len`, requiring `rel_pos = ki - (qi + q_offset)`. Implemented as `t5RelativePositionBucketLocal` + `computeDecoderBiasWithOffset` in `t5.zig` (avoids modifying the `ComputeBackend` vtable).

**WASM exports:** `t5_create_kv_cache(model, max_len)`, `t5_forward_cached(model, cache, enc_out, enc_mask, dec_ids, batch, dec_seq, enc_seq, out)`, `t5_reset_kv_cache(cache)`, `t5_free_kv_cache(cache)`

**E2E test:** `web/test-t5-wasm.mjs` Test 4 — verifies cached generation produces identical tokens to full recompute (Test 3).

### Phase 27: Q5_K / Q6_K WebGPU Shaders ✅ COMPLETE

GPU-accelerated matmul for Q5_K/Q6_K quantized models (Llama 3 Q5_K_M, Mistral Q6_K).

**Block layouts:**
- **Q5_K** (176 bytes / 256 values): f16 d, f16 dmin, 12 bytes packed 6-bit scales, 32 bytes qh (high bits), 128 bytes ql (low 4-bit). `value = d * sc * (ql | (qh_bit << 4)) - dmin * mn`
- **Q6_K** (210 bytes / 256 values): 128 bytes ql, 64 bytes qh, 16 bytes scales (i8), f16 d. `value = d * scales[sub] * ((ql | (qh << 4)) - 32)`

**New WGSL shaders:**

| Shader | Purpose |
|---|---|
| `web/shaders/matmul_transb_q5_k.wgsl` | Q5_K dequant+matmul. 16×16×32 tiling. `unpack_scale_min` for 6-bit packed scale format. |
| `web/shaders/matmul_transb_q6_k.wgsl` | Q6_K dequant+matmul. 16×16×32 tiling. 16 sub-blocks of 16 with ql/qh/scales/d. |

**New externs:** `gpu_matmul_transb_q5_k`, `gpu_matmul_transb_q6_k`

### Phase 28: mT5 Validation ✅ COMPLETE

Confirmed mT5 (multilingual T5) works with existing `load_model_t5` + `t5_encode` + `t5_decode`. mT5 uses the same T5 architecture with a larger vocabulary (250,112 tokens covering 101 languages).

**E2E test:** `web/test-mt5-wasm.mjs` — load mT5-small, encoder forward, decoder forward (single step), greedy generation (5 tokens).

**mBART:** Out of scope — BART uses learned position embeddings, LayerNorm, and bias, requiring a separate `bart.zig` architecture.

### Phase 29: Speculative Decoding ✅ COMPLETE

KV cache truncation support for draft/verify rollback.

**Design:** Added `truncateTo(new_len)` to both `WasmKvCache` and `GpuKvCache` — sets `cached_len = min(new_len, cached_len)`. This enables the JS-side speculative decoding loop: draft K tokens with a small model, verify against the target, rollback rejected tokens via truncation.

**WASM export:** `gpt_truncate_kv_cache(cache, new_len)`

**JS API:** `gptTruncateKvCache()` + `gptGenerateSpeculative()` (draft model prefill, verify batch, accept prefix, rollback at mismatch)

### Phase 30: Batched Generation ✅ COMPLETE

Generate multiple sequences simultaneously using independent KV caches.

**Design:** Zero Zig changes — JS manages multiple independent caches. Each sequence gets its own KV cache, prefills independently, and decode loop advances all active sequences per step. Sequences that hit EOS stop while others continue.

**JS API:** `gptGenerateBatch(modelHandle, vocabSize, promptIdsBatch, options)` — creates one cache per sequence, prefills each, then decodes in lock-step with per-sequence EOS handling.

**E2E test:** `web/test-gpt-wasm.mjs` Test 8 — verifies batch output matches individual `gptGenerateCached()` calls for two different prompts.

### Phase 31: WebGPU Norms/Activations ✅ COMPLETE

GPU-accelerated RMSNorm and LayerNorm for large hidden dimensions.

**Design:** Threshold-based GPU dispatch: `dim >= 4096 and total_elements >= 65536`. One workgroup (256 threads) per row. Tree reduction in shared memory.

**New WGSL shaders:**

| Shader | Purpose |
|---|---|
| `web/shaders/rms_norm.wgsl` | RMSNorm: sum of squares → inverseSqrt(mean_sq + eps) → scale by weight. 4-binding layout. |
| `web/shaders/layer_norm.wgsl` | LayerNorm: mean + variance → inv_std → normalize + scale + bias. 5-binding layout. |

**New externs:** `gpu_rms_norm`, `gpu_layer_norm`

### Phase 32: Gemma3 Q4_0 Validation ✅ COMPLETE

End-to-end test for Gemma3-2B with Q4_0 GGUF weights.

**E2E test:** `web/test-gemma3-q4-wasm.mjs` — load Gemma3-2B Q4_0 GGUF via `load_model_gpt_gguf`, cached forward pass, 32-token greedy generation, token validity checks.

### Bug Fix: f16 Weight Support in Conv/Attention Ops

Fixed out-of-bounds crashes when running models stored as float16 (e.g., Florence-2). Multiple ops in `wasm_compute.zig` accessed weight data via `toBuf(ct).data` which returns an empty slice for f16 weights. Changed all affected ops to use `toBuf(ct).viewF32(allocator)` which properly handles f16→f32 conversion.

**Affected ops:** `conv2dOp`, `conv1dOp`, `tokenGridConv2dOp`, `windowedSelfAttentionOp`, `channelSelfAttentionOp`, `scaledDotProductAttentionOp`, `causalSelfAttentionOp`, `gqaCausalAttentionOp`, `relativePositionBiasOp`

---

## Files

### Modified (Existing)

| File | Change |
|---|---|
| `build.zig` | `enable_wasm` option, WASM executable target with ReleaseSafe, tokenizer modules |
| `src/backends/backends.zig` | `.wasm` backend enum, system deps guarded behind `!enable_wasm` |
| `src/ops/ops.zig` | `.wasm` added to `BackendKind` |
| `src/termite.zig` | Conditional `wasm_compute` import |
| `src/backends/native.zig` | Target-aware SIMD vector width (4-wide on wasm32, 8-wide native) |
| `src/backends/activations.zig` | Target-aware SIMD vector width |
| `src/models/bert.zig` | `num_labels` field for reranker config |

Architecture code (`bert.zig`, `clip.zig`, `whisper.zig`, `clap.zig`, `florence.zig`) required **zero changes** — they call ops through the `ComputeBackend` VTable. `gpt.zig` required minor freestanding guards (`monotonicNowNs`, `@cImport`/`getenv`, `std.debug.print`).

### Created (New)

| File | Purpose |
|---|---|
| `src/wasm_entry.zig` | Thin freestanding compatibility root used during the refactor |
| `src/wasm_entry_wasm32.zig` | Explicit wasm32 root that imports `src/web/exports_wasm32.zig` |
| `src/wasm_entry_wasm64.zig` | Explicit wasm64 root that imports `src/web/exports_wasm64.zig` |
| `src/web/entry_context.zig` | Shared runtime/cache state and tiny ABI helpers used by the export shims |
| `src/web/exports_core.zig` | Shared non-generative browser export surface |
| `src/web/exports_generation.zig` | Shared GPT / T5 / multimodal browser export surface |
| `src/web/exports_wasm32.zig` | wasm32 profile shim over the shared export surface |
| `src/web/exports_wasm64.zig` | wasm64 profile shim over the shared export surface |
| `src/ops/wasm_compute.zig` | ComputeBackend VTable — all ops via WASM SIMD, optional GPU dispatch |
| `src/ops/wasm_extern.zig` | Extern declarations for WebGPU JS bridge (matmul, attention, causal attention, cross attention) |
| `web/termite-web.js` | JS glue, `TermiteWeb` class — direct/worker mode, all model APIs, optional ONNX |
| `web/test-wasm-path.mjs` | Shared test helper for resolving `termite-wasm32.wasm` / `termite-wasm64.wasm` with legacy fallback |
| `web/test-wasm-runtime.mjs` | Shared Node test helper for ABI-safe WASM instantiation, alloc/free, and typed-array access |
| `web/webgpu-ops.js` | `WebGPUOps` class — device init, 5 shader pipelines, compute dispatch, worker handler |
| `web/inference-worker.js` | Web Worker for WASM with sync GPU downloads via `Atomics.wait()`, ONNX handlers |
| `web/shaders/matmul.wgsl` | Tiled 16x16 matmul: `C[M,N] = A[M,K] @ B[K,N]` |
| `web/shaders/matmul_transb.wgsl` | Tiled 16x16 matmul with B transposed |
| `web/shaders/attention.wgsl` | Fused attention: Q@K^T + scale + mask + softmax + @V |
| `web/shaders/causal_attention.wgsl` | Causal self-attention: built-in future-position masking, no mask buffer |
| `web/shaders/cross_attention.wgsl` | Cross-attention: asymmetric Q[dec_seq] x K/V[enc_seq] with encoder mask |
| `web/shaders/matmul_transb_q4_0.wgsl` | Q4_0 tiled dequant+matmul: 16x16x32 shared memory tiling, cooperative block dequant |
| `web/shaders/matmul_transb_q8_0.wgsl` | Q8_0 tiled dequant+matmul: 16x16x32 shared memory tiling, cooperative block dequant |
| `web/shaders/gqa_causal_attention.wgsl` | GQA causal attention: asymmetric Q/KV head counts, 256-thread workgroups, MAX_SEQ=512 |
| `web/shaders/gqa_cached_attention.wgsl` | GPU-resident KV cache attention: asymmetric Q/KV lengths, MAX_KV=2048 |
| `web/index.html` | Demo page with embedding + reranking UI |
| `web/test-rerank-wasm.mjs` | E2E test: BERT tokenization + reranking |
| `web/test-clip-wasm.mjs` | E2E test: CLIP text/image embedding + cosine similarity |
| `web/test-whisper-wasm.mjs` | E2E test: Whisper encoder + decoder + greedy decode loop |
| `web/test-clap-wasm.mjs` | E2E test: CLAP text/audio embedding + cosine similarity |
| `web/test-florence-wasm.mjs` | E2E test: Florence-2 encoder + decoder + greedy decode loop |
| `web/test-gpt-wasm.mjs` | E2E test: GPT-2 model load + forward pass + multi-step greedy generation |
| `web/test-t5-wasm.mjs` | E2E test: T5-small encoder + decoder + greedy generation |
| `web/test-gemma3-vision-wasm.mjs` | E2E test: Gemma3 vision encode + multimodal forward + cached generation |
| `web/test-mt5-wasm.mjs` | E2E test: mT5-small encoder + decoder + greedy generation |
| `web/test-gemma3-q4-wasm.mjs` | E2E test: Gemma3-2B Q4_0 GGUF load + cached forward + greedy generation |
| `web/shaders/matmul_transb_q5_k.wgsl` | Q5_K tiled dequant+matmul: 16x16x32, 6-bit packed scale unpacking |
| `web/shaders/matmul_transb_q6_k.wgsl` | Q6_K tiled dequant+matmul: 16x16x32, ql/qh/scales/d sub-blocks |
| `web/shaders/rms_norm.wgsl` | RMSNorm: tree reduction, 256-thread workgroups, 4-binding layout |
| `web/shaders/layer_norm.wgsl` | LayerNorm: mean + variance + normalize, 256-thread workgroups, 5-binding layout |

### Reused Unchanged

| File | What it provides |
|---|---|
| `src/backends/activations.zig` | layerNorm, rmsNorm, gelu, relu, silu, sigmoid, softmax, argmax — `@Vector` → WASM SIMD |
| `src/backends/native.zig` | sgemm/sgemmTransB/sgemmTransA with SIMD-vectorized fallbacks |
| `src/gguf/format.zig` | GGUF parser (operates on `[]const u8`) |
| `src/gguf/quant_codec.zig` | Dequantization routines (pure Zig) |
| `src/models/safetensors.zig` | SafeTensors parser |
| `src/architectures/bert.zig` | BERT forward pass |
| `src/architectures/clip.zig` | CLIP text + vision encoder |
| `src/architectures/whisper.zig` | Whisper encoder (conv1d frontend + transformer) + decoder |
| `src/architectures/clap.zig` | CLAP text (RoBERTa) + audio (HTS-AT) encoder |
| `src/architectures/florence.zig` | Florence-2 DaViT vision + BART text encoder/decoder |
| `src/backends/tensor.zig` | Tensor/DType types |
| `lib/tokenizer/` | HuggingFace tokenizer (WordPiece/BPE/Unigram, pure Zig) |
| `lib/audio/` | WAV decode, resample, log-mel spectrogram (Whisper + CLAP configs) |
| `lib/image/processing.zig` | SIMD-optimized bilinear resize + normalize → CHW f32 |

## WASM Exports

| Export | Purpose |
|---|---|
| `init` | Initialize global state |
| `wasm_alloc` / `wasm_dealloc` | WASM linear memory management for JS interop |
| `load_model_gguf` | Parse GGUF model + config JSON, return handle |
| `load_model_safetensors` | Parse SafeTensors model + config JSON, return handle |
| `embed` | BERT forward pass → embeddings |
| `rerank` | BERT forward + classifier head + sigmoid/softmax scoring |
| `gliner` | DeBERTa forward + span classifier → NER logits |
| `load_model_clip` | Load CLIP model from SafeTensors |
| `clip_embed_text` | CLIP text encoder → text embeddings |
| `clip_embed_image` | CLIP vision encoder → image embeddings |
| `preprocess_image` | RGBA bytes → resize + normalize → CHW f32 |
| `load_model_whisper` | Load Whisper model from SafeTensors |
| `whisper_encode` | Whisper encoder (mel → hidden states) |
| `whisper_decode` | Whisper decoder (hidden + token IDs → logits) |
| `audio_whisper_mel_interleaved` | Interleaved PCM → Whisper log-mel spectrogram |
| `audio_whisper_mel_size` | Max mel output buffer size |
| `load_model_clap` | Load CLAP model from SafeTensors |
| `clap_embed_text` | CLAP text encoder → text embeddings |
| `clap_embed_audio` | CLAP audio encoder → audio embeddings |
| `audio_clap_features_interleaved` | Interleaved PCM → CLAP mel features |
| `audio_clap_feature_size` | Max CLAP feature output buffer size |
| `load_model_florence` | Load Florence-2 model from SafeTensors |
| `florence_encode` | Florence-2 vision + text encoder → hidden states |
| `florence_decode` | Florence-2 decoder (hidden + token IDs → logits) |
| `load_model_gpt` | Load GPT model from SafeTensors (with Conv1D transpose) |
| `load_model_gpt_gguf` | Load GPT model from GGUF |
| `gpt_forward` | GPT forward pass → logits `[batch, seq_len, vocab_size]` |
| `gpt_create_kv_cache` | Create KV cache for a GPT model, return cache handle |
| `gpt_forward_cached` | GPT forward with KV cache (prefill or decode) |
| `gpt_reset_kv_cache` | Reset cache for new sequence |
| `gpt_free_kv_cache` | Free cache memory |
| `create_model_gpt` | Create empty GPT model (config only, no weights) |
| `register_weight` | Register single weight tensor by name on existing model |
| `load_tokenizer` | Parse tokenizer.json, return handle |
| `tokenize` | Single text → `[CLS] text [SEP]` |
| `tokenize_pair` | Pair → `[CLS] a [SEP] b [SEP]` with token type IDs |
| `tokenize_raw` | Raw encode without special tokens |
| `load_model_t5` | Load T5 model from SafeTensors |
| `t5_encode` | T5 encoder → hidden states `[batch, seq_len, d_model]` |
| `t5_decode` | T5 decoder → logits `[batch, dec_seq, vocab_size]` |
| `t5_create_kv_cache` | Create T5 decoder KV cache (self-attention + cross-attention) |
| `t5_forward_cached` | T5 cached decode (O(1) per token) |
| `t5_reset_kv_cache` | Reset T5 KV cache for new sequence |
| `t5_free_kv_cache` | Free T5 KV cache memory |
| `gpt_truncate_kv_cache` | Truncate GPT KV cache to given length (for speculative decoding rollback) |
| `gpt_vision_encode` | Gemma3 vision tower → projected image embeddings |
| `gpt_forward_multimodal` | GPT forward with injected image embeddings + bidirectional attention mask |
| `gpt_forward_cached_multimodal` | Same with KV cache (prefill with images) |
| `unload_model` / `unload_tokenizer` | Free resources |

## Ops Implemented in wasm_compute.zig

| Op | Used By | GPU Dispatch |
|---|---|---|
| `getWeight`, `freeTensor` | All | — |
| `embeddingLookup` | All | — |
| `linear`, `linearNoBias` | All | matmul_transb.wgsl when output >= 50K; matmul_transb_q4_0/q8_0/q5_k/q6_k.wgsl for quantized weights |
| `layerNorm` | All | layer_norm.wgsl when dim >= 4096 and total_elements >= 65536 |
| `rmsNorm` | GPT, T5, Gemma | rms_norm.wgsl when dim >= 4096 and total_elements >= 65536 |
| `gelu`, `relu`, `silu`, `quickGelu`, `sigmoid`, `tanh` | Various | — (SIMD) |
| `add`, `multiply`, `concat` | Various | — (SIMD) |
| `scaledDotProductAttention` | BERT, CLIP vision, CLAP | attention.wgsl when output >= 50K, seq <= 512 |
| `disentangledRelativeAttention` | DeBERTa/GLiNER | — (SIMD) |
| `causalSelfAttention` | CLIP text, Whisper/Florence decoder | causal_attention.wgsl when output >= 50K, seq <= 512 |
| `crossAttention` | Whisper/Florence decoder | cross_attention.wgsl when output >= 50K, enc_seq <= 512 |
| `conv1d` | Whisper (mel frontend) | — (SIMD) |
| `conv2d` | CLAP (HTS-AT), Florence (DaViT) | im2col on CPU + matmul_transb.wgsl for groups=1 |
| `tokenGridConv2d` | Florence (DaViT patch embed) | — (SIMD) |
| `windowedSelfAttention` | Florence (DaViT spatial blocks) | — (SIMD) |
| `channelSelfAttention` | Florence (DaViT channel blocks) | — (SIMD) |
| `rope`, `ropePerItem` | GPT (RoPE models) | — (SIMD) |
| `relativePositionBias` | T5 (encoder + decoder) | — (SIMD) |
| `gqaCausalAttention` | GPT (grouped query attention) | gqa_causal_attention.wgsl when output >= threshold, seq <= 512, no attn_bias |
| `gqaPagedAttention` | GPT (paged attention wrapper) | gqa_cached_attention.wgsl when GPU KV cache active, kv_len <= 2048 |
| `fromFloat32`, `fromFloat32Shape`, `toFloat32` | Weight loading | — |

## Known Issues

| Issue | Status |
|---|---|
| LLVM WASM backend miscompilation at `-Os`/`-O3` | NaN in BERT FFN linear ops. Workaround: build with ReleaseSafe (`-O2`). Binary is ~1.2 MB — acceptable. |
| WASM 4GB memory limit | Mitigated by f16 storage (Phase 17) and quantized weights (Phase 18). LLaMA-3.2-1B fits as f16 (2.4GB) or Q4_0 (~670MB). |
| Safari WebGPU gaps | WASM SIMD works everywhere — WebGPU is optional acceleration. |
| Worker mode requires COOP/COEP | `SharedArrayBuffer` needs cross-origin isolation headers. Without them, fall back to direct mode (SIMD only). |
| Attention shader max seq_len 512 | Shared memory limit in WGSL. Falls back to WASM SIMD for longer sequences. GQA cached attention supports up to 2048 KV tokens. |
| Gemma3-4B too large for WASM | BF16 weights (~8 GB) exceed 4 GB WASM address space. Requires quantized (Q4_0/Q8_0) weights or a smaller vision-language model. |
| I64 tensors in SafeTensors | Some models (CLAP) contain I64 index tables. Skipped during weight loading with `catch continue`. |

## Dual wasm32/wasm64 Architecture

The browser path now needs two explicit deployment profiles:

1. `wasm32` for broad browser compatibility and the current worker/WebGPU path
2. `wasm64` / `memory64` for Electron-class runtimes and future browsers that can host models beyond the 4GB linear-memory ceiling

The architectural rule for supporting both is:

- keep one shared inference/runtime core
- isolate pointer-width and host-ABI differences behind thin profile-specific seams

### Layering

#### Shared semantic runtime

Shared Zig code should continue to operate on native Zig types:

- `usize`
- slices (`[]u8`, `[]const u8`, `[]f32`, `[]i64`)
- semantic model loading / generation APIs

This layer owns:

- model parsing and construction
- tokenizer extraction and decode
- generation / embedding / multimodal logic
- `ComputeBackend` and architecture execution

It should not know whether the host ABI is `wasm32` or `wasm64`.

#### Web profile module

Introduce a small shared profile module that defines:

- configured memory model (`wasm32` or `wasm64`)
- host-facing integer types for sizes and offsets
- common target properties like SIMD lane width

Current starting point:

- `src/web/profile.zig`

Everything else should depend on that profile module instead of open-coding architecture checks.

#### Export shims

The current `src/wasm_entry.zig` mixes:

- semantic operations
- global runtime state
- host ABI wire types

That should continue to evolve toward:

- shared semantic operations
- thin `wasm32` export shim
- thin `wasm64` export shim

The shim layer should only do:

- pointer reconstruction
- length conversion
- output marshalling

It should not own model logic.

#### JS ABI adapters

The JS host should not directly assume:

- pointers are always JS `number`
- sizes are always `u32`
- typed-array views can always be built the same way

Instead, `termite-web.js` should eventually route through ABI adapters:

- `abi32.js`
- `abi64.js`

Responsibilities:

- normalize `number` vs `bigint`
- hide `wasm_alloc` / `wasm_dealloc` wire details
- centralize memory-view construction and growth handling
- isolate `wasm32` / `wasm64` calling differences from the public `TermiteWeb` API

#### Loader split: buffered vs streaming

This is the real unlock for large models.

`memory64` by itself is not enough if the host still:

- `fetch(...).arrayBuffer()` the whole GGUF in JS
- allocates another full copy inside WASM

The architecture should support two loaders behind one semantic API:

- buffered loader for small/medium `wasm32` assets
- streaming / chunked loader for `wasm64` and large GGUFs

The long-term design should prefer:

- `create_model_*`
- incremental registration / chunk ingestion
- GGUF-derived config and tokenizer extraction in Zig

over host-side full-buffer staging.

### Build Profiles

The build now has explicit memory-model plumbing:

- `zig build -Dwasm=true -Dwasm-memory-model=wasm32 wasm`
- `zig build -Dwasm=true -Dwasm-memory-model=wasm64 wasm`

Current status:

- `wasm32` remains the supported path
- `wasm64` target selection and shared profile plumbing are in place
- model/tokenizer handle state has started moving out of `src/wasm_entry.zig` into `src/web/runtime_state.zig`
- GPT/T5 KV-cache slot state and lifecycle have started moving out of `src/wasm_entry.zig` into `src/web/cache_state.zig`
- host-facing buffer-length conversion is now centralized in `src/web/host_abi.zig`, and the main JS-facing pointer/slice exports in `src/wasm_entry.zig` now route host buffer lengths through `HostSize` instead of hardcoded `u32`
- tokenizer semantics and early task-specific execution paths have started moving out of `src/wasm_entry.zig` into `src/web/audio_api.zig`, `src/web/tokenizer_api.zig`, `src/web/bert_api.zig`, `src/web/rerank_api.zig`, `src/web/gliner_api.zig`, `src/web/clip_api.zig`, `src/web/whisper_api.zig`, `src/web/clap_api.zig`, `src/web/florence_api.zig`, and `src/web/t5_api.zig`
- shared SafeTensors conversion and registration logic is now centralized in `src/web/weight_loader.zig`
- JS host ABI adaptation is now centralized in `web/runtime/wasm-abi.js` and wired through the current `termite-web.js` and `inference-worker.js` host surface, including generic model loading, GPT, tokenizer, rerank, GLiNER, CLIP, Whisper, CLAP, Florence, T5, and multimodal vision paths
- WebGPU host imports now explicitly coerce host pointers/sizes to JS-safe indices instead of assuming raw `wasm32` numbers
- the Termite wasm root now provides freestanding `PATH_MAX` / `NAME_MAX` overrides for Zig stdlib code that already consults `root.os`
- shared wasm runtime state / cache state access and GPU KV-format parsing are now centralized in `src/web/entry_context.zig`
- generic BERT GGUF / SafeTensors load and embedding now route through `src/web/bert_api.zig`, and CLIP / GLiNER model loading now route through their helper modules
- GPT decoder-only loading / registration / forward / cached-forward / multimodal forward has started moving out of `src/wasm_entry.zig` into `src/web/gpt_api.zig`
- T5 encoder-decoder loading / encode / decode / cached-decode has started moving out of `src/wasm_entry.zig` into `src/web/t5_api.zig`
- the export surface is now split into `src/web/exports_core.zig` and `src/web/exports_generation.zig`, with `src/wasm_entry.zig` reduced to a thin freestanding root that imports those shims
- build-time profile selection now uses explicit `src/wasm_entry_wasm32.zig` / `src/wasm_entry_wasm64.zig` roots and `src/web/exports_wasm32.zig` / `src/web/exports_wasm64.zig` shims instead of a single generic root
- the build now emits `zig-out/bin/termite-wasm32.wasm` and `zig-out/bin/termite-wasm64.wasm`; the `wasm32` build also installs `zig-out/bin/termite.wasm` as a compatibility alias
- `web/termite-web.js` and `web/inference-worker.js` now understand profile-specific artifact names, default `wasmMemoryModel` to `auto`, probe `memory64` support before instantiation, and fall back from `termite-wasm64.wasm` / `termite-wasm32.wasm` to legacy `termite.wasm`
- the local Node-based wasm tests now share `web/test-wasm-path.mjs`, which resolves profile-specific artifacts first and can be steered with `TERMITE_WASM_MEMORY_MODEL=wasm32|wasm64`
- the raw Node tests now share `web/test-wasm-runtime.mjs`, including `test-wasm.mjs`, `test-t5-wasm.mjs`, `test-mt5-wasm.mjs`, `test-rerank-wasm.mjs`, `test-gpt-wasm.mjs`, `test-gliner-wasm.mjs`, `test-clip-wasm.mjs`, `test-whisper-wasm.mjs`, `test-clap-wasm.mjs`, `test-florence-wasm.mjs`, `test-gemma3-q4-wasm.mjs`, and `test-gemma3-vision-wasm.mjs`, so their alloc/free, pointer sizes, and typed-array views are ABI-safe for both `wasm32` and `wasm64`
- `web/runtime/safetensors-stream.js` now provides the true header-first SafeTensors streaming path for `streamLoadGptModel()`, and both `web/termite-web.js` and `web/inference-worker.js` use it to register one weight at a time from the fetch stream instead of concatenating the entire file before load
- `web/runtime/gguf-stream.js` now provides the matching header-first GGUF streaming path for `streamLoadGgufModel()`, backed by the new `register_weight_gguf` export so quantized GGUF tensors can be registered incrementally in direct mode or worker mode
- streamed GGUF tensor ingestion no longer requires a full JS `Uint8Array` for each tensor: the loader now allocates the tensor buffer directly in WASM, streams file chunks into that buffer, and finalizes registration with `register_weight_gguf_owned`
- non-streamed tokenizer loading can now derive Hugging Face tokenizer state directly from GGUF metadata through `load_tokenizer_gguf`, and `web/termite-web.js` / `web/inference-worker.js` route `loadTokenizer(..., { format: 'gguf' })` through that path
- streamed GGUF loading now parses tokenizer-related header metadata in `web/runtime/gguf-stream.js` and surfaces `{ tokenizerJson, chatTemplate }` through `streamLoadGgufModel(..., { onMetadata })`; `streamLoadGgufModel(..., { autoLoadTokenizer: true, onTokenizerLoaded })` can now also install that tokenizer automatically without a second whole-file fetch
- the web runtime now exposes `gguf_chat_template` and `render_chat_prompt`, so direct mode and worker mode can extract a GGUF chat template and render a single-turn system/user prompt through Zig's existing Jinja chat-template engine instead of relying on raw prompt text
- GPU-resident weights have now started as an explicit runtime feature: `WasmCompute` now owns a `GpuWeightStore`, WebGPU-enabled registration eagerly uploads those weights during model load, and the WebGPU matmul / LayerNorm / RMSNorm paths now reuse those resident GPU buffers instead of re-uploading long-lived `B`, `gamma`, and `beta` tensors on every dispatch
- local build verification now passes for both `wasm32` and `wasm64`, but this currently depends on a local Zig freestanding `std.Io.Threaded` workaround tracked in the repo root `ZIG.md`
- large-model browser bring-up remains follow-on work

### Initial Refactor Slice

The first implemented slice of this architecture is intentionally small:

- add explicit `-Dwasm-memory-model`
- centralize memory-model knowledge in `src/web/profile.zig`
- extract model/tokenizer runtime-state management into `src/web/runtime_state.zig`
- start routing JS-side alloc/free, length conversion, and typed-array views through `web/runtime/wasm-abi.js`
- normalize `bigint` pointer/size arguments at the WebGPU JS boundary before building typed-array views or GPU buffer commands
- route shared SIMD-width decisions through that profile module
- start moving host-facing allocation ABI through the profile layer

That keeps the current `wasm32` path stable while creating a real seam for future `wasm64` work.

## Expansion Path

1. ~~**Rerankers**~~ ✅ Done — BERT arch + classifier head + sigmoid/softmax
2. ~~**Wire WebGPU into hot path**~~ ✅ Done — threshold-based routing in `wasm_compute.zig`
3. ~~**Fused attention shader**~~ ✅ Done — `attention.wgsl`
4. ~~**Synchronous GPU downloads**~~ ✅ Done — Web Worker + `SharedArrayBuffer` + `Atomics.wait()`
5. ~~**DeBERTa / GLiNER NER**~~ ✅ Done — disentangled attention + span classifier
6. ~~**CLIP**~~ ✅ Done — text + vision encoder, image preprocessing in WASM
7. ~~**Whisper**~~ ✅ Done — conv1d + cross-attention, mel spectrogram in WASM
8. ~~**CLAP**~~ ✅ Done — HTS-AT audio encoder with conv2d, mel in WASM
9. ~~**Florence-2**~~ ✅ Done — DaViT vision (windowed + channel attention) + BART decoder
10. ~~**WebGPU for new ops**~~ ✅ Done — causal attention, cross attention, conv2d GPU dispatch
11. ~~**ONNX Runtime Web**~~ ✅ Done — optional lazy integration for ONNX-only models
12. ~~**GPT Generative**~~ ✅ Done — rope, GQA, autoregressive decode, GPT-2 Conv1D transpose
13. ~~**f16 weight storage**~~ ✅ Done — on-the-fly dequant, halves memory
14. ~~**Quantized GPU kernels**~~ ✅ Done — Q4_0/Q8_0 WGSL dequant shaders + CPU fallback
15. ~~**KV cache**~~ ✅ Done — O(1) per-token decode, 3-5x speedup
16. ~~**Weight streaming**~~ ✅ Done — `create_model_gpt` + `register_weight` plus true header-first SafeTensors streaming in `web/termite-web.js`
17. ~~**Tiled quantized matmul**~~ ✅ Done — 16x16x32 shared memory tiling for Q4_0/Q8_0
18. ~~**WebGPU GQA attention**~~ ✅ Done — GPU-accelerated grouped-query attention
19. ~~**GPU-resident KV cache**~~ ✅ Done — persistent GPU buffers, incremental K/V upload
20. ~~**T5 encoder-decoder**~~ ✅ Done — relative position bias, cross-attention, encode/decode exports
21. ~~**Vision-language (Gemma3)**~~ ✅ Done — vision encode, multimodal forward, image injection
22. ~~**T5 decoder KV cache**~~ ✅ Done — O(1) decode with self-attention + cross-attention caching
23. ~~**Q5_K / Q6_K GPU shaders**~~ ✅ Done — tiled dequant+matmul for k-quant models
24. ~~**mT5 validation**~~ ✅ Done — multilingual T5 works with existing T5 architecture
25. ~~**Speculative decoding**~~ ✅ Done — KV cache truncation + draft/verify JS loop
26. ~~**Batched generation**~~ ✅ Done — independent KV caches per sequence, JS-managed
27. ~~**WebGPU norms/activations**~~ ✅ Done — RMSNorm + LayerNorm shaders with threshold dispatch
28. ~~**Gemma3 Q4_0 validation**~~ ✅ Done — end-to-end generation with quantized GGUF

### What's Next

| Feature | Benefit | Complexity |
|---|---|---|
| **GPU tensor residence** | Keep tensors in GPU memory between ops (matmul→norm→matmul without download/upload) — eliminates transfer overhead | In progress — persistent GPU buffers for registered weights are in; activation/intermediate residence and GPU-native compute graph are still pending |
| **Streaming tokenizer** | Progressive token output for long documents | Low — expose partial decode from `lib/tokenizer/` |
| **mBART architecture** | Multilingual translation with BART-style model (learned pos embeddings, LayerNorm, bias) | Medium — needs separate `bart.zig` architecture |
| **WebTransport model streaming** | Server-push model weights over WebTransport for faster load | Medium — replaces fetch-based streaming, needs server support |
| **WASM 64-bit memory** | Break 4GB limit for large models (Gemma3-4B BF16) | Target-selection + profile plumbing landed; JS host ABI plus SafeTensors/GGUF streaming, GGUF config inference, non-streamed GGUF tokenizer loading, and streamed GGUF metadata extraction/installation are in, but large-model browser bring-up is still required |
| **Fused GPU norm+matmul** | Chain norm → matmul in a single GPU pass without intermediate download | Medium — custom shader, only worthwhile for dim >= 4096 |

### GPU-Resident Weight Direction

The long-term direction for large local models is now explicitly GPU-first rather than host-paged CPU/WASM-first.

Rationale:

- decoder inference touches nearly every layer on every token, so a host-side demand pager would still churn most of the decoder weights through WASM memory on each decode step
- the current bottleneck for very large GGUFs is no longer just file ingest; it is resident memory pressure once the decoder is loaded
- GPU-resident weights let the runtime keep long-lived decoder parameters and KV cache near the kernels that consume them, instead of treating WASM memory as the permanent home for the whole model

Execution order:

1. Persist registered weight tensors in GPU buffers and reuse them across dispatches
2. Teach GGUF / SafeTensors streaming paths to upload supported weights directly into GPU buffers during registration
3. Keep activations / intermediates on GPU across chains of hot ops instead of forcing download after every dispatch
4. Introduce a real GPU weight store / manifest so the runtime can distinguish host metadata from GPU-resident tensor payloads
5. Only after that, consider true on-demand paging / eviction for oversized models

Current implementation status:

- registered weight tensors in `src/ops/wasm_compute.zig` now eagerly upload into an explicit `GpuWeightStore` when WebGPU is active, and hot kernels look up those resident GPU buffers from the store instead of hiding GPU state directly inside each `WasmBuf`
- GPU weight residence preserves the stored representation when possible: quantized GGUF weights stay quantized in the GPU store for quantized matmul shaders, and host-side dequantization remains a fallback rather than the target architecture
- quantized weights with supported GPU matmul shaders can now drop their host-side raw quantized copy after successful GPU upload, so resident GPU storage becomes the primary long-lived copy instead of a duplicate
- WebGPU `matmulTransB`, quantized `matmulTransB`, `LayerNorm`, and `RMSNorm` now reuse those resident weight buffers instead of re-uploading weight tensors every call, and both `linearNoBias` and `linear + bias` honor the quantized-resident path
- transient activation tensors from GPU `LayerNorm`, `RMSNorm`, and `linearNoBias` now keep an attached GPU buffer alongside their host copy, so later GPU linears/norms can reuse that activation buffer instead of re-uploading the same input tensor on every dispatch
- the GPU attention fast paths now consume attached `Q/K/V` activation buffers when present and return GPU-backed outputs, so the main decoder-side `linear -> attention` path avoids another round of activation uploads
- when the GPU KV cache uses plain `f32` layout, cached-attention now copies freshly produced GPU-backed `K/V` activations directly into the GPU cache via buffer-to-buffer copy instead of round-tripping those writes through host uploads
- `reshape2d` is now a real WASM backend op instead of a null capability, and both `reshape2d` and primitive reshape preserve attached GPU activation buffers via device-to-device copy; that unlocks the GPT/Gemma reshape-gated RMSNorm fast paths without forcing those tensors back through host upload
- the richer 2D shape helpers (`splitLastDim3`, `reshape2D`, `concatRows2D`, `sliceRows2D`) are now real WASM backend ops too, and when their inputs are already GPU-backed they preserve residence with device-to-device copies instead of falling back through `toFloat32`; this removes several decoder/runtime and vision-side host detours
- WASM tensors now retain explicit shape metadata when they are created through shape-aware paths (`fromFloat32Shape`, 2D reshape/slice helpers, primitive reshape), and the backend now exposes `tensorShape` plus a real `sliceLastDim`; that lets GPT fused-QKV and other last-dimension slice paths stay on backend-owned tensors instead of immediately dropping to generic host exports
- the main decoder-side tensor producers now propagate that shape metadata too: embedding lookup, linear/linearNoBias, norm, concat, attention, and RoPE outputs keep 2D shape when available, so `sliceLastDim` and related fast paths can actually fire on hot tensors instead of only on explicitly reshaped ones
- the WASM backend now also fills in several decoder/runtime null hooks directly: `zeroTensor`, `argmaxLastRow`, `sampleLastRow`, `linearNoBiasArgmaxLastRow`, `linearNoBiasArgmaxLastRowTensor`, and `takeRows` are implemented, which removes more of the generic generation and grouped-MoE fallback surface from GPT paths while keeping sampling semantics aligned with the shared host sampler
- the WASM backend now has basic integer-tensor interop too: `fromInt32Shape`, `embeddingLookupTensor`, `tensorDType`, `exportTensorData`, and `evalTensor` are implemented for the common `i32`/`f32` cases, which enables the device-token handoff path to stay inside the backend instead of immediately converting token ids back to host arrays
- the prepared decoder-runtime seam is now partially implemented in `src/ops/wasm_compute.zig` too: absolute token/position embeddings, prepared layer norm / RMS norm slots, prepared linear slots, fused norm+linear argmax/sample helpers, backend-side activation/add, and the dense FFN residual strip can all stay inside the WASM backend instead of immediately falling back to the generic host decode path
- the prepared attention seam now exists too: `runAttention` and `runAttentionResidual` are implemented in the WASM backend on top of the existing dense/paged GQA attention kernels plus prepared linear/RMS-norm slots, so the qLen=1 decoder residual path no longer has to peel back immediately to the generic host composition path
- the block-level decoder seam now covers the split fallback path as well: `runDenseDecoderBlock` and `runGatedDecoderBlock` are implemented in the WASM backend by composing prepared attention-residual, optional FFN norm, and the existing dense/gated FFN residual helpers, which removes another layer of host-side orchestration from decoder runtime execution even before any larger fused-kernel path exists
- the gated block `attention_input` path now routes through the shared prepared QKV helper as one backend operation instead of projecting `K/V` as a pair and `Q` separately, which keeps that orchestration inside the backend and gives the focused WASM test step coverage for both explicit `q/k/v` and projected-input block entrypoints
- the WebGPU dispatch heuristics in `src/ops/wasm_compute.zig` are now chain-aware for prepared decode: when a decoder-runtime linear or norm receives an already GPU-backed input tensor, or a weight is already resident in `GpuWeightStore`, the backend prefers the GPU path even below the old bulk thresholds, which reduces avoidable CPU fallbacks in qLen=1 decode chains without changing the non-GPU surface
- streamed GGUF loading can now register supported quantized tensors directly into GPU buffers in both direct mode and worker mode, bypassing WASM tensor allocation for those weights and registering them as GPU-primary quantized weights through `register_weight_gguf_gpu`
- GLiNER’s disentangled relative-attention hotspot is no longer forced through the host path when WebGPU is available: `disentangledRelativeAttention` now has a real WebGPU kernel/JS bridge in the embedded runtime, so the self-attention step can stay on the GPU instead of falling back to `debertaDisentangledAttentionHost`
- activations and outputs are still transient GPU buffers today, so this is the first seam, not the full architecture

## Electron Shell

The browser demo now has a minimal Electron shell under `pkg/inference/electron/` that reuses the existing `web/index.html` UI rather than introducing a second desktop-specific frontend.

Current behavior:

- Electron serves the same web assets behind a local loopback HTTP server with `COOP/COEP` enabled, so the page can use the `worker + WebGPU` runtime path without the browser-dev-server setup
- the preload bridge exposes the default local models directory (`~/.termite/models`), a directory chooser, and recursive model-file scanning to the page
- the page now detects that Electron bridge and auto-scans `~/.termite/models` on startup, then loads discovered local GGUF / HF SafeTensors bundles by local file URL instead of browser `FileSystemHandle`
- the local loopback file endpoint supports HTTP byte ranges, so GGUF metadata probing in the page can read just the front of a large local model instead of fetching the entire file before streamed load starts

Run:

1. `cd pkg/inference/electron`
2. `npm install`
3. `npm start`

The current Electron shell is intentionally thin. It exists to give the existing embedded-WASM page direct access to the default local model registry and a predictable cross-origin-isolated runtime, not to fork the UI into a separate desktop app.

## Verification

1. **Build**: `~/bin/zig build -Dwasm=true wasm` → `zig-out/bin/termite.wasm` (~1.2 MB, ReleaseSafe)
2. **Build (WebGPU)**: `~/bin/zig build -Dwasm=true -Dwebgpu=true wasm`
3. **Build (Experimental wasm64 target)**: `~/bin/zig build -Dwasm=true -Dwasm-memory-model=wasm64 wasm`
4. **E2E tests** (all pass for the current wasm32 path):
   - `node web/test-rerank-wasm.mjs` — BERT tokenization + reranking
   - `node --max-old-space-size=4096 web/test-clip-wasm.mjs` — CLIP text/image embedding
   - `node --max-old-space-size=4096 web/test-whisper-wasm.mjs` — Whisper encoder + decoder + greedy decode
   - `node --max-old-space-size=4096 web/test-clap-wasm.mjs` — CLAP text/audio embedding
   - `node --max-old-space-size=4096 web/test-florence-wasm.mjs` — Florence-2 encoder + decoder + greedy decode
   - `node --max-old-space-size=4096 web/test-gpt-wasm.mjs` — GPT-2 model load + forward pass + greedy generation + KV cache + weight streaming
   - `node --max-old-space-size=4096 web/test-t5-wasm.mjs` — T5-small encoder + decoder + greedy generation + cached generation
   - `node --max-old-space-size=4096 web/test-mt5-wasm.mjs` — mT5-small encoder + decoder + greedy generation
   - `node --max-old-space-size=8192 web/test-gemma3-vision-wasm.mjs` — Gemma3 vision encode + multimodal forward + cached generation
   - `node --max-old-space-size=4096 web/test-gemma3-q4-wasm.mjs` — Gemma3-2B Q4_0 GGUF load + cached forward + generation
   - `node web/test-model-discovery.mjs` — local `~/.termite/models` bundle discovery/classification coverage for GGUF + `mmproj`, HF GPT/T5/Whisper/GLiNER/REBEL/encoder/CLIP, and unsupported HF bundles
   - `node web/test-page-behavior.mjs` — page-level multimodal action-label, media-availability, prompt-expansion, and integrated image-offset logic coverage
   - `node web/test-page-controller.mjs` — page-level source-mode and load-button/controller-state coverage for local bundles vs remote GGUF URL mode
   - `node web/test-page-prompt.mjs` — page-level loaded-mode prompt-meta and chat-template-control coverage for unloaded, encoder, GLiNER extraction, REBEL relation extraction, seq2seq, Whisper transcription, and generate/chat-template states
   - `node web/test-page-summary.mjs` — page-level load-summary coverage for metrics, large-model warnings, projector readiness, chat-template/raw-prompt notices, and task-mode warnings including extraction/relation/transcription
   - `node web/test-extraction-support.mjs` — GLiNER page-helper coverage for tokenizer special-token parsing, label normalization, and runtime option derivation
   - `node web/test-relation-support.mjs` — REBEL page-helper coverage for relation-config defaults, generated-triplet parsing, and structured relation output conversion
   - `node web/test-projector-wasm.mjs` — projector export coverage plus worker-mode RPC dispatch and Gemma4 cached multimodal generation-wrapper checks for the external Gemma3/Gemma4 browser multimodal path
5. **Focused backend unit tests**:
   - `zig build test-wasm-compute -Dskip-openapi=true` — targeted `wasm_compute` tests for backend ops, integer-token handoff, sampling, prepared decoder-runtime slots, and dense FFN residual coverage
   - `zig build test-web-projector` — focused projector store/runtime-handle tests in normal package context
6. **Browser test**: Open `web/index.html`, load a GGUF, verify tokenizer/chat-template inference, then run generation
7. **WebGPU test**: Same with `{ gpu, worker: true }`, verify results match SIMD-only path
8. **Browser matrix**: Chrome, Firefox, Safari — SIMD works in all, WebGPU where available
