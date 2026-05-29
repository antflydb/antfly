// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { createWasmAbi } from './runtime/wasm-abi.js';
import { resolvePreferredMemoryModel } from './runtime/wasm-capabilities.js';
import { streamRegisterSafetensors } from './runtime/safetensors-stream.js';
import { streamRegisterGguf } from './runtime/gguf-stream.js';

// Antfly Inference Web: browser-based ML inference via WASM SIMD.
// Optionally accelerates heavy ops (matmul, attention) via WebGPU compute
// shaders when the WASM module is built with -Dwebgpu=true.
//
// Two modes:
//
// 1. Direct mode (default): WASM runs on the calling thread. GPU downloads are
//    async (require flush). Best for simple usage without WebGPU.
//
//    const t = new InferenceWeb();
//    await t.init();
//    const model = await t.loadModel(source, config);
//    const emb = t.embed(model, ids, mask, batch, seq);
//
// 2. Worker mode: WASM runs in a dedicated Web Worker. GPU downloads are
//    synchronous via SharedArrayBuffer + Atomics.wait(). Required for correct
//    WebGPU acceleration. Requires cross-origin isolation (COOP/COEP headers).
//
//    const gpu = new WebGPUOps();
//    await gpu.init();
//    const t = new InferenceWeb();
//    await t.init({ gpu, worker: true });
//    const model = await t.loadModel(source, config);
//    const emb = await t.embed(model, ids, mask, batch, seq);  // async in worker mode

// Default SharedArrayBuffer size for GPU data transfers (32 MB).
const DEFAULT_SAB_SIZE = 32 * 1024 * 1024;
const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

function normalizeInitArgs(wasmUrlOrOptions, options) {
  if (
    options === undefined &&
    wasmUrlOrOptions &&
    typeof wasmUrlOrOptions === 'object' &&
    !(wasmUrlOrOptions instanceof URL)
  ) {
    return { wasmUrl: undefined, options: wasmUrlOrOptions };
  }
  return { wasmUrl: wasmUrlOrOptions, options: options ?? {} };
}

function defaultWasmCandidates(memoryModel, requestedMemoryModel) {
  if (requestedMemoryModel === 'auto' && memoryModel === 'wasm64') {
    return ['antfly-inference-wasm64.wasm', 'antfly-inference-wasm32.wasm', 'antfly-inference.wasm'];
  }
  if (memoryModel === 'wasm64') {
    return ['antfly-inference-wasm64.wasm', 'antfly-inference.wasm'];
  }
  return ['antfly-inference-wasm32.wasm', 'antfly-inference.wasm'];
}

function toWasmCandidates(wasmUrl, requestedMemoryModel, resolvedMemoryModel) {
  if (wasmUrl == null) {
    return defaultWasmCandidates(resolvedMemoryModel, requestedMemoryModel);
  }

  const candidate = wasmUrl;
  const asText = typeof candidate === 'string' ? candidate : candidate.href;
  if (asText.endsWith('/antfly-inference.wasm') || asText === 'antfly-inference.wasm') {
    const profileNames = requestedMemoryModel === 'auto' && resolvedMemoryModel === 'wasm64'
      ? ['antfly-inference-wasm64.wasm', 'antfly-inference-wasm32.wasm']
      : [resolvedMemoryModel === 'wasm64' ? 'antfly-inference-wasm64.wasm' : 'antfly-inference-wasm32.wasm'];
    const profiled = profileNames.map((profileName) =>
      typeof candidate === 'string'
        ? candidate.replace(/antfly-inference\.wasm$/, profileName)
        : new URL(candidate.href.replace(/antfly-inference\.wasm$/, profileName)),
    );
    return [...profiled, candidate];
  }

  return [candidate];
}

async function instantiateWasmCandidate(candidate, importObject) {
  if (candidate instanceof URL) {
    return WebAssembly.instantiateStreaming(fetch(candidate), importObject);
  }

  if (typeof candidate === 'string' && !candidate.startsWith('http')) {
    try {
      const fs = await import('fs');
      if (fs.existsSync(candidate)) {
        const buf = fs.readFileSync(candidate);
        return WebAssembly.instantiate(buf, importObject);
      }
    } catch (_) {
      // Fall through to fetch for browser environments.
    }
  }

  return WebAssembly.instantiateStreaming(fetch(candidate), importObject);
}

async function instantiateWasmFromCandidates(candidates, importObject) {
  let lastError = null;
  for (const candidate of candidates) {
    try {
      const source = await instantiateWasmCandidate(candidate, importObject);
      return { source, candidate };
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError ?? new Error('Unable to instantiate Antfly inference WASM module');
}

function toStreamResponse(source) {
  if (typeof Response !== 'undefined' && source instanceof Response) {
    return source;
  }
  if (typeof Blob !== 'undefined' && source instanceof Blob) {
    return new Response(source.stream(), {
      headers: source.size > 0 ? { 'content-length': String(source.size) } : undefined,
    });
  }
  return null;
}

export class InferenceWeb {
  constructor() {
    this.wasm = null;
    this.abi = null;
    this.memoryModel = null;
    this.gpu = null;
    this._worker = null;       // inference Worker (worker mode)
    this._sab = null;          // SharedArrayBuffer (worker mode)
    this._pendingCalls = null;  // Map<id, {resolve, reject}> (worker mode)
    this._nextCallId = 1;
  }

  /**
   * Initialize the WASM module.
   * @param {string|URL|object} [wasmUrl] - URL to the Antfly inference WASM artifact, or init options
   * @param {object} [options]
   * @param {object} [options.gpu] - WebGPUOps instance (from webgpu-ops.js), already init'd
   * @param {boolean} [options.worker] - Run WASM in a Web Worker for sync GPU downloads
   * @param {string|URL} [options.workerUrl] - URL to inference-worker.js (defaults to same dir)
   * @param {'auto'|'wasm32'|'wasm64'} [options.wasmMemoryModel] - Preferred WASM artifact/profile
   * @param {number} [options.sabSize] - SharedArrayBuffer size in bytes (default 32 MB)
   */
  async init(wasmUrl, options) {
    const normalized = normalizeInitArgs(wasmUrl, options);
    const requestedMemoryModel = normalized.options?.wasmMemoryModel ?? 'auto';
    const resolvedMemoryModel = resolvePreferredMemoryModel(requestedMemoryModel);
    const wasmCandidates = toWasmCandidates(normalized.wasmUrl, requestedMemoryModel, resolvedMemoryModel);

    this.gpu = normalized.options?.gpu ?? null;

    if (normalized.options?.worker) {
      return this._initWorker(wasmCandidates, normalized.options);
    }

    return this._initDirect(wasmCandidates);
  }

  // --- Direct mode (WASM on calling thread) ---

  async _initDirect(wasmCandidates) {
    const importObject = { env: {} };

    let wasmMemory = null;
    const memoryProxy = { get buffer() { return wasmMemory?.buffer; } };

    if (this.gpu) {
      importObject.webgpu = this.gpu.getImports(memoryProxy);
    } else {
      importObject.webgpu = _gpuStubs();
    }

    const { source } = await instantiateWasmFromCandidates(wasmCandidates, importObject);

    this.wasm = source.instance.exports;
    this.abi = createWasmAbi(this.wasm);
    this.memoryModel = this.abi.memoryModel;
    wasmMemory = this.wasm.memory;
    this.wasm.init();
  }

  // --- Worker mode (WASM in Web Worker, sync GPU downloads) ---

  async _initWorker(wasmCandidates, options) {
    const sabSize = options.sabSize ?? DEFAULT_SAB_SIZE;
    this._sab = new SharedArrayBuffer(sabSize);
    this._pendingCalls = new Map();

    const baseWasmUrl = wasmCandidates[0];

    // Resolve worker script URL
    const workerUrl = options.workerUrl ??
      new URL('inference-worker.js', typeof baseWasmUrl === 'string' ? new URL(baseWasmUrl, location.href) : baseWasmUrl);

    this._worker = new Worker(workerUrl, { type: 'module' });

    this._worker.onmessage = (e) => this._onWorkerMessage(e);

    // Resolve wasm candidates to absolute URLs for the worker's fetch() fallback chain.
    const absWasmUrls = wasmCandidates.map((candidate) =>
      typeof candidate === 'string'
        ? new URL(candidate, location.href).href
        : candidate.href,
    );

    await this._workerCall('init', {
      wasmUrls: absWasmUrls,
      sharedBuffer: this._sab,
      hasGpu: !!this.gpu,
    });
  }

  _onWorkerMessage(e) {
    const { type, id } = e.data;

    // GPU command from worker — route to WebGPUOps on main thread
    if (type === 'gpu-sync' || type === 'gpu') {
      if (this.gpu) {
        // For gpu-sync, handleWorkerCommand signals the worker when done
        // For gpu (fire-and-forget), we still process it through the handler
        // but the worker isn't blocking
        this.gpu.handleWorkerCommand(e.data, this._sab);
      }
      return;
    }

    // Response to a pending call
    if (type === 'error') {
      const pending = this._pendingCalls.get(id);
      if (pending) {
        this._pendingCalls.delete(id);
        pending.reject(new Error(e.data.message));
      }
      return;
    }

    if (type === 'progress') {
      const pending = this._pendingCalls.get(id);
      if (pending?.onProgress) {
        pending.onProgress(e.data);
      }
      return;
    }

    if (type === 'metadata') {
      const pending = this._pendingCalls.get(id);
      if (pending?.onMetadata) {
        pending.onMetadata(e.data);
      }
      return;
    }

    // Match response type to pending call
    const pending = this._pendingCalls.get(id);
    if (pending) {
      this._pendingCalls.delete(id);
      pending.resolve(e.data);
    }
  }

  _workerCall(type, extra = {}, transfer = [], callbacks = {}) {
    return new Promise((resolve, reject) => {
      const id = this._nextCallId++;
      this._pendingCalls.set(id, { resolve, reject, ...callbacks });
      this._worker.postMessage({ type, id, ...extra }, transfer);
    });
  }

  _getAbi() {
    if (!this.abi || !this.wasm) {
      throw new Error('InferenceWeb direct-mode ABI is not initialized');
    }
    return this.abi;
  }

  _inferGptConfigJsonFromGgufBytes(modelBytes) {
    const abi = this._getAbi();
    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed for GGUF data');
    abi.copyBytesIn(modelPtr, modelBytes);

    const outLenPtr = abi.alloc(4);
    if (!outLenPtr) {
      abi.free(modelPtr, modelBytes.length);
      throw new Error('WASM allocation failed for GGUF config metadata');
    }

    const jsonPtr = this.wasm.gpt_config_json_from_gguf(modelPtr, abi.size(modelBytes.length), outLenPtr);
    const jsonLen = abi.readCopy(Uint32Array, outLenPtr, 1)[0];
    abi.free(outLenPtr, 4);
    abi.free(modelPtr, modelBytes.length);

    if (!jsonPtr || jsonLen === 0) {
      throw new Error('Failed to derive GPT config from GGUF metadata');
    }

    const jsonBytes = abi.readCopy(Uint8Array, jsonPtr, jsonLen);
    abi.free(jsonPtr, jsonLen);
    return TEXT_DECODER.decode(jsonBytes);
  }

  _inferGgufChatTemplateFromBytes(modelBytes) {
    const abi = this._getAbi();
    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed for GGUF data');
    abi.copyBytesIn(modelPtr, modelBytes);

    const outLenPtr = abi.alloc(4);
    if (!outLenPtr) {
      abi.free(modelPtr, modelBytes.length);
      throw new Error('WASM allocation failed for GGUF chat template metadata');
    }

    const templatePtr = this.wasm.gguf_chat_template(modelPtr, abi.size(modelBytes.length), outLenPtr);
    const templateLen = abi.readCopy(Uint32Array, outLenPtr, 1)[0];
    abi.free(outLenPtr, 4);
    abi.free(modelPtr, modelBytes.length);

    if (!templatePtr || templateLen === 0) {
      return null;
    }

    const templateBytes = abi.readCopy(Uint8Array, templatePtr, templateLen);
    abi.free(templatePtr, templateLen);
    return TEXT_DECODER.decode(templateBytes);
  }

  // --- Public API (works in both modes) ---

  /**
   * Load a model into WASM memory.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource
   * @param {object|string} config - BERT config object or JSON string
   * @param {object} [options]
   * @param {'gguf'|'safetensors'} [options.format]
   * @returns {Promise<number>} model handle
   */
  async loadModel(modelSource, config, options) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }

    let format = options?.format;
    if (!format) {
      const magic = new TextDecoder().decode(modelBytes.slice(0, 4));
      format = magic === 'GGUF' ? 'gguf' : 'safetensors';
    }

    const configJson = typeof config === 'string' ? config : JSON.stringify(config);

    if (this._worker) {
      const resp = await this._workerCall('load-model', {
        modelBytes, configJson, format,
      });
      return resp.handle;
    }

    // Direct mode
    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);

    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed for model data');
    abi.copyBytesIn(modelPtr, modelBytes);

    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) {
      abi.free(modelPtr, modelBytes.length);
      throw new Error('WASM allocation failed for config data');
    }
    abi.copyBytesIn(configPtr, configBytes);

    const loadFn = format === 'gguf'
      ? this.wasm.load_model_gguf
      : this.wasm.load_model_safetensors;
    const handle = loadFn(
      modelPtr,
      abi.size(modelBytes.length),
      configPtr,
      abi.size(configBytes.length),
    );

    abi.free(modelPtr, modelBytes.length);
    abi.free(configPtr, configBytes.length);

    if (handle === 0) throw new Error('Failed to load model');
    return handle;
  }

  /**
   * Run embedding inference.
   * In worker mode, returns a Promise. In direct mode, returns synchronously.
   * @param {number} modelHandle
   * @param {number[]|BigInt64Array} tokenIds
   * @param {number[]|BigInt64Array} attentionMask
   * @param {number} batchSize
   * @param {number} seqLen
   * @returns {Float32Array|Promise<Float32Array>}
   */
  embed(modelHandle, tokenIds, attentionMask, batchSize, seqLen) {
    if (this._worker) {
      return this._workerCall('embed', {
        modelHandle,
        tokenIds: Array.from(tokenIds instanceof BigInt64Array ? tokenIds.map(Number) : tokenIds),
        attentionMask: Array.from(attentionMask instanceof BigInt64Array ? attentionMask.map(Number) : attentionMask),
        batchSize,
        seqLen,
      }).then(resp => resp.output);
    }

    // Direct mode (synchronous)
    const abi = this._getAbi();
    const ids = tokenIds instanceof BigInt64Array
      ? tokenIds
      : new BigInt64Array(tokenIds.map(BigInt));
    const mask = attentionMask instanceof BigInt64Array
      ? attentionMask
      : new BigInt64Array(attentionMask.map(BigInt));

    const idsByteLen = ids.length * 8;
    const maskByteLen = mask.length * 8;

    const idsPtr = abi.alloc(idsByteLen);
    const maskPtr = abi.alloc(maskByteLen);
    if (!idsPtr || !maskPtr) throw new Error('WASM allocation failed for inputs');

    abi.writeArray(BigInt64Array, idsPtr, ids);
    abi.writeArray(BigInt64Array, maskPtr, mask);

    const maxOutputFloats = batchSize * 4096;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) throw new Error('WASM allocation failed for output');

    const resultLen = this.wasm.embed(
      modelHandle,
      idsPtr, abi.size(ids.length),
      maskPtr, abi.size(mask.length),
      batchSize, seqLen,
      outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    } else {
      output = null;
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(maskPtr, maskByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('Embedding inference failed');
    return output;
  }

  /**
   * Decode browser-supported audio and preprocess it into Whisper log-mel features.
   * @param {string|URL|ArrayBuffer|Uint8Array|Blob} audioSource
   * @returns {Promise<{data: Float32Array, nMels: number, nFrames: number, sampleRate: number, channels: number}>}
   */
  async preprocessWhisperAudio(audioSource) {
    const decoded = await this._decodeAudioSource(audioSource);

    if (this._worker) {
      const pcm = decoded.samples;
      const resp = await this._workerCall('audio-whisper-mel', {
        pcm,
        sampleRate: decoded.sampleRate,
        channels: decoded.channels,
      }, [pcm.buffer]);
      return {
        data: resp.data,
        nMels: resp.nMels,
        nFrames: resp.nFrames,
        sampleRate: decoded.sampleRate,
        channels: decoded.channels,
      };
    }

    return this._audioWhisperMelDirect(decoded);
  }

  /**
   * Decode browser-supported audio and preprocess it into CLAP input features.
   * @param {string|URL|ArrayBuffer|Uint8Array|Blob} audioSource
   * @param {object} [options]
   * @param {number} [options.channels=1] - Requested CLAP feature channels/fusion lanes.
   * @returns {Promise<{data: Float32Array, channelsUsed: number, timeFrames: number, melBins: number, isLonger: boolean, sampleRate: number, inputChannels: number}>}
   */
  async preprocessClapAudio(audioSource, options = {}) {
    const decoded = await this._decodeAudioSource(audioSource);
    const requestedChannels = options.channels ?? 1;

    if (this._worker) {
      const pcm = decoded.samples;
      const resp = await this._workerCall('audio-clap-features', {
        pcm,
        sampleRate: decoded.sampleRate,
        inputChannels: decoded.channels,
        outputChannels: requestedChannels,
      }, [pcm.buffer]);
      return {
        data: resp.data,
        channelsUsed: resp.channelsUsed,
        timeFrames: resp.timeFrames,
        melBins: resp.melBins,
        isLonger: resp.isLonger,
        sampleRate: decoded.sampleRate,
        inputChannels: decoded.channels,
      };
    }

    return this._audioClapFeaturesDirect(decoded, requestedChannels);
  }

  /**
   * Unload a model and free its resources.
   * @param {number} handle
   */
  unloadModel(handle) {
    if (this._worker) {
      return this._workerCall('unload-model', { handle });
    }
    this.wasm.unload_model(handle);
  }

  /**
   * Load a GGUF projector sidecar into the runtime and return its handle plus kind.
   * Direct mode only for now.
   * @param {string|URL|ArrayBuffer|Uint8Array|Blob} source
   * @returns {Promise<{handle:number, kind:string}>}
   */
  async loadProjectorGguf(source) {
    if (this._worker) {
      const projectorBytes = new Uint8Array(await this._readBinarySource(source));
      const resp = await this._workerCall('load-projector', { projectorBytes }, [projectorBytes.buffer]);
      return { handle: resp.handle, kind: resp.kind };
    }

    const projectorBytes = new Uint8Array(await this._readBinarySource(source));
    const abi = this._getAbi();
    const ptr = abi.alloc(projectorBytes.length);
    if (!ptr) throw new Error('WASM allocation failed for projector data');
    abi.copyBytesIn(ptr, projectorBytes);

    const handle = this.wasm.load_projector_gguf(ptr, abi.size(projectorBytes.length));
    abi.free(ptr, projectorBytes.length);

    if (!handle) throw new Error('Failed to load projector GGUF');
    const rawKind = this.wasm.projector_kind(handle);
    return {
      handle,
      kind: _projectorKindFromRaw(rawKind),
    };
  }

  /**
   * Unload a previously loaded projector GGUF.
   * Direct mode only for now.
   * @param {number} handle
   */
  unloadProjector(handle) {
    if (this._worker) {
      return this._workerCall('unload-projector', { handle });
    }
    this.wasm.unload_projector(handle);
  }

  async _decodeAudioSource(audioSource) {
    const bytes = await this._readBinarySource(audioSource);
    const AudioContextCtor = globalThis.AudioContext || globalThis.webkitAudioContext;
    if (!AudioContextCtor) throw new Error('AudioContext is not available in this environment');

    const context = new AudioContextCtor();
    try {
      const decoded = await context.decodeAudioData(bytes.slice(0));
      return {
        samples: _interleaveAudioBuffer(decoded),
        sampleRate: decoded.sampleRate,
        channels: decoded.numberOfChannels,
      };
    } finally {
      if (typeof context.close === 'function') {
        try {
          await context.close();
        } catch (_) {
          // Ignore close errors from already-closed contexts.
        }
      }
    }
  }

  async _readBinarySource(source) {
    if (source instanceof ArrayBuffer) return source;
    if (ArrayBuffer.isView(source)) {
      return source.buffer.slice(source.byteOffset, source.byteOffset + source.byteLength);
    }
    if (typeof Blob !== 'undefined' && source instanceof Blob) {
      return await source.arrayBuffer();
    }
    const resp = await fetch(source);
    return await resp.arrayBuffer();
  }

  _audioWhisperMelDirect(decoded) {
    const abi = this._getAbi();
    const pcm = decoded.samples;
    const pcmByteLen = pcm.length * 4;
    const pcmPtr = abi.alloc(pcmByteLen);
    if (!pcmPtr) throw new Error('WASM allocation failed for audio PCM');
    abi.writeArray(Float32Array, pcmPtr, pcm);

    const outLen = this.wasm.audio_whisper_mel_size();
    const outByteLen = outLen * 4;
    const outPtr = abi.alloc(outByteLen);
    const metaPtr = abi.alloc(8);
    if (!outPtr || !metaPtr) throw new Error('WASM allocation failed for Whisper audio output');

    const written = this.wasm.audio_whisper_mel_interleaved(
      pcmPtr,
      abi.size(pcm.length),
      decoded.sampleRate,
      decoded.channels,
      outPtr,
      metaPtr,
    );

    let data = null;
    if (written > 0) {
      data = abi.readCopy(Float32Array, outPtr, written);
    }
    const meta = abi.readCopy(Uint32Array, metaPtr, 2);
    const nMels = meta[0];
    const nFrames = meta[1];

    abi.free(pcmPtr, pcmByteLen);
    abi.free(outPtr, outByteLen);
    abi.free(metaPtr, 8);

    if (!data) throw new Error('Whisper audio preprocessing failed');
    return { data, nMels, nFrames, sampleRate: decoded.sampleRate, channels: decoded.channels };
  }

  _audioClapFeaturesDirect(decoded, requestedChannels) {
    const abi = this._getAbi();
    const pcm = decoded.samples;
    const pcmByteLen = pcm.length * 4;
    const pcmPtr = abi.alloc(pcmByteLen);
    if (!pcmPtr) throw new Error('WASM allocation failed for audio PCM');
    abi.writeArray(Float32Array, pcmPtr, pcm);

    const outLen = this.wasm.audio_clap_feature_size(requestedChannels);
    const outByteLen = outLen * 4;
    const outPtr = abi.alloc(outByteLen);
    const metaPtr = abi.alloc(16);
    if (!outPtr || !metaPtr) throw new Error('WASM allocation failed for CLAP audio output');

    const written = this.wasm.audio_clap_features_interleaved(
      pcmPtr,
      abi.size(pcm.length),
      decoded.sampleRate,
      decoded.channels,
      requestedChannels,
      outPtr,
      metaPtr,
    );

    let data = null;
    if (written > 0) {
      data = abi.readCopy(Float32Array, outPtr, written);
    }
    const meta = abi.readCopy(Uint32Array, metaPtr, 4);
    const channelsUsed = meta[0];
    const timeFrames = meta[1];
    const melBins = meta[2];
    const isLonger = meta[3] !== 0;

    abi.free(pcmPtr, pcmByteLen);
    abi.free(outPtr, outByteLen);
    abi.free(metaPtr, 16);

    if (!data) throw new Error('CLAP audio preprocessing failed');
    return {
      data,
      channelsUsed,
      timeFrames,
      melBins,
      isLonger,
      sampleRate: decoded.sampleRate,
      inputChannels: decoded.channels,
    };
  }

  // --- CLIP API ---

  /**
   * Load a CLIP model from SafeTensors.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource
   * @param {object|string} config - CLIP config object or JSON string
   * @returns {Promise<number>} model handle
   */
  async loadClipModel(modelSource, config) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }

    const configJson = typeof config === 'string' ? config : JSON.stringify(config);

    if (this._worker) {
      const resp = await this._workerCall('load-model-clip', {
        modelBytes, configJson,
      });
      return resp.handle;
    }

    // Direct mode
    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);

    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed for model data');
    abi.copyBytesIn(modelPtr, modelBytes);

    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) {
      abi.free(modelPtr, modelBytes.length);
      throw new Error('WASM allocation failed for config data');
    }
    abi.copyBytesIn(configPtr, configBytes);

    const handle = this.wasm.load_model_clip(
      modelPtr,
      abi.size(modelBytes.length),
      configPtr,
      abi.size(configBytes.length),
    );

    abi.free(modelPtr, modelBytes.length);
    abi.free(configPtr, configBytes.length);

    if (handle === 0) throw new Error('Failed to load CLIP model');
    return handle;
  }

  /**
   * Run CLIP text encoder. Returns [batch, projection_dim] text embeddings.
   * @param {number} modelHandle - CLIP model handle (from loadClipModel)
   * @param {number[]|BigInt64Array} tokenIds - flattened [batch * seqLen] token IDs
   * @param {number} batchSize
   * @param {number} seqLen
   * @returns {Float32Array|Promise<Float32Array>}
   */
  clipEmbedText(modelHandle, tokenIds, batchSize, seqLen) {
    if (this._worker) {
      return this._workerCall('clip-embed-text', {
        modelHandle,
        tokenIds: Array.from(tokenIds instanceof BigInt64Array ? tokenIds.map(Number) : tokenIds),
        batchSize,
        seqLen,
      }).then(resp => resp.output);
    }

    // Direct mode
    const abi = this._getAbi();
    const ids = tokenIds instanceof BigInt64Array
      ? tokenIds
      : new BigInt64Array(tokenIds.map(BigInt));

    const idsByteLen = ids.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    if (!idsPtr) throw new Error('WASM allocation failed for token IDs');
    abi.writeArray(BigInt64Array, idsPtr, ids);

    // Output: batch * projection_dim (max 2048 for safety)
    const maxOutputFloats = batchSize * 2048;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) throw new Error('WASM allocation failed for output');

    const resultLen = this.wasm.clip_embed_text(
      modelHandle,
      idsPtr, abi.size(ids.length),
      batchSize, seqLen,
      outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('CLIP text embedding failed');
    return output;
  }

  /**
   * Run CLIP vision encoder. Returns [batch, projection_dim] image embeddings.
   * @param {number} modelHandle - CLIP model handle (from loadClipModel)
   * @param {Float32Array} pixelValues - pre-normalized CHW f32 [batch, 3, imgSize, imgSize]
   * @param {number} batchSize
   * @returns {Float32Array|Promise<Float32Array>}
   */
  clipEmbedImage(modelHandle, pixelValues, batchSize) {
    if (this._worker) {
      return this._workerCall('clip-embed-image', {
        modelHandle,
        pixelValues,
        batchSize,
      }).then(resp => resp.output);
    }

    // Direct mode
    const abi = this._getAbi();
    const pixelByteLen = pixelValues.length * 4;
    const pixelPtr = abi.alloc(pixelByteLen);
    if (!pixelPtr) throw new Error('WASM allocation failed for pixel data');
    abi.writeArray(Float32Array, pixelPtr, pixelValues);

    const maxOutputFloats = batchSize * 2048;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) throw new Error('WASM allocation failed for output');

    const resultLen = this.wasm.clip_embed_image(
      modelHandle,
      pixelPtr, abi.size(pixelValues.length),
      batchSize,
      outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(pixelPtr, pixelByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('CLIP image embedding failed');
    return output;
  }

  /**
   * Preprocess a decoded RGBA image into normalized CHW f32 for vision models.
   * Use with Canvas API: ctx.getImageData() → RGBA bytes → this method → CHW f32.
   * @param {Uint8Array} rgbaData - raw RGBA pixel data from Canvas getImageData()
   * @param {number} width - source image width
   * @param {number} height - source image height
   * @param {number} targetSize - output square size (e.g. 224 for CLIP)
   * @param {number[]} mean - per-channel mean [R, G, B] (e.g. [0.48145466, 0.4578275, 0.40821073])
   * @param {number[]} std - per-channel std [R, G, B] (e.g. [0.26862954, 0.26130258, 0.27577711])
   * @returns {Float32Array|Promise<Float32Array>} CHW normalized f32 [3, targetSize, targetSize]
   */
  preprocessImage(rgbaData, width, height, targetSize, mean, std) {
    if (this._worker) {
      return this._workerCall('preprocess-image', {
        rgbaData,
        width,
        height,
        targetSize,
        mean,
        std,
      }).then(resp => resp.output);
    }

    // Direct mode
    const abi = this._getAbi();
    const rgbaPtr = abi.alloc(rgbaData.length);
    if (!rgbaPtr) throw new Error('WASM allocation failed for image data');
    abi.copyBytesIn(rgbaPtr, rgbaData);

    const meanArr = new Float32Array(mean);
    const stdArr = new Float32Array(std);
    const meanPtr = abi.alloc(12);
    const stdPtr = abi.alloc(12);
    if (!meanPtr || !stdPtr) throw new Error('WASM allocation failed for normalization params');
    abi.writeArray(Float32Array, meanPtr, meanArr);
    abi.writeArray(Float32Array, stdPtr, stdArr);

    const outLen = 3 * targetSize * targetSize;
    const outByteLen = outLen * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) throw new Error('WASM allocation failed for output');

    const resultLen = this.wasm.preprocess_image(
      rgbaPtr, abi.size(rgbaData.length),
      width, height, targetSize,
      meanPtr, stdPtr,
      outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(rgbaPtr, rgbaData.length);
    abi.free(meanPtr, 12);
    abi.free(stdPtr, 12);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('Image preprocessing failed');
    return output;
  }

  // --- Whisper API ---

  /**
   * Load a Whisper model from SafeTensors.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource
   * @param {object|string} config - Whisper config object or JSON string
   * @returns {Promise<number>} model handle
   */
  async loadWhisperModel(modelSource, config) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }
    const configJson = typeof config === 'string' ? config : JSON.stringify(config);
    if (this._worker) {
      const resp = await this._workerCall('load-model-whisper', { modelBytes, configJson });
      return resp.handle;
    }
    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);
    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed for model data');
    abi.copyBytesIn(modelPtr, modelBytes);
    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) { abi.free(modelPtr, modelBytes.length); throw new Error('WASM allocation failed'); }
    abi.copyBytesIn(configPtr, configBytes);
    const handle = this.wasm.load_model_whisper(modelPtr, abi.size(modelBytes.length), configPtr, abi.size(configBytes.length));
    abi.free(modelPtr, modelBytes.length);
    abi.free(configPtr, configBytes.length);
    if (handle === 0) throw new Error('Failed to load Whisper model');
    return handle;
  }

  /**
   * Run Whisper encoder on mel features. Returns encoder hidden states.
   * @param {number} modelHandle
   * @param {Float32Array} melFeatures - [batch, n_mels, time_steps]
   * @param {number} batchSize
   * @param {number} timeSteps
   * @returns {Float32Array|Promise<Float32Array>}
   */
  whisperEncode(modelHandle, melFeatures, batchSize, timeSteps) {
    if (this._worker) {
      return this._workerCall('whisper-encode', { modelHandle, melFeatures, batchSize, timeSteps }).then(r => r.output);
    }
    const abi = this._getAbi();
    const melByteLen = melFeatures.length * 4;
    const melPtr = abi.alloc(melByteLen);
    if (!melPtr) throw new Error('WASM allocation failed');
    abi.writeArray(Float32Array, melPtr, melFeatures);
    // Encoder output: batch * enc_seq * d_model. Conservative max.
    const maxOut = batchSize * 1500 * 1024;
    const outByteLen = maxOut * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) throw new Error('WASM allocation failed');
    const resultLen = this.wasm.whisper_encode(modelHandle, melPtr, abi.size(melFeatures.length), batchSize, timeSteps, outPtr);
    let output;
    if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
    abi.free(melPtr, melByteLen);
    abi.free(outPtr, outByteLen);
    if (!output) throw new Error('Whisper encode failed');
    return output;
  }

  /**
   * Run Whisper decoder. Returns logits [batch, vocab_size].
   * @param {number} modelHandle
   * @param {BigInt64Array|number[]} decoderIds - [batch * dec_seq]
   * @param {Float32Array} encoderHidden - from whisperEncode
   * @param {BigInt64Array|number[]} encoderMask - [batch * enc_seq]
   * @param {number} batchSize
   * @param {number} decSeq
   * @param {number} encSeq
   * @returns {Float32Array|Promise<Float32Array>}
   */
  whisperDecode(modelHandle, decoderIds, encoderHidden, encoderMask, batchSize, decSeq, encSeq) {
    if (this._worker) {
      return this._workerCall('whisper-decode', {
        modelHandle, decoderIds: Array.from(decoderIds instanceof BigInt64Array ? decoderIds.map(Number) : decoderIds),
        encoderHidden, encoderMask: Array.from(encoderMask instanceof BigInt64Array ? encoderMask.map(Number) : encoderMask),
        batchSize, decSeq, encSeq,
      }).then(r => r.output);
    }
    const abi = this._getAbi();
    const ids = decoderIds instanceof BigInt64Array ? decoderIds : new BigInt64Array(decoderIds.map(BigInt));
    const mask = encoderMask instanceof BigInt64Array ? encoderMask : new BigInt64Array(encoderMask.map(BigInt));
    const idsByteLen = ids.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);
    const encByteLen = encoderHidden.length * 4;
    const encPtr = abi.alloc(encByteLen);
    abi.writeArray(Float32Array, encPtr, encoderHidden);
    const maskByteLen = mask.length * 8;
    const maskPtr = abi.alloc(maskByteLen);
    abi.writeArray(BigInt64Array, maskPtr, mask);
    const maxOut = batchSize * 52000; // vocab_size
    const outByteLen = maxOut * 4;
    const outPtr = abi.alloc(outByteLen);
    const resultLen = this.wasm.whisper_decode(modelHandle, idsPtr, abi.size(ids.length), encPtr, abi.size(encoderHidden.length), maskPtr, abi.size(mask.length), batchSize, decSeq, encSeq, outPtr);
    let output;
    if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
    abi.free(idsPtr, idsByteLen);
    abi.free(encPtr, encByteLen);
    abi.free(maskPtr, maskByteLen);
    abi.free(outPtr, outByteLen);
    if (!output) throw new Error('Whisper decode failed');
    return output;
  }

  // --- CLAP API ---

  /**
   * Load a CLAP model from SafeTensors.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource
   * @param {object|string} config
   * @returns {Promise<number>} model handle
   */
  async loadClapModel(modelSource, config) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }
    const configJson = typeof config === 'string' ? config : JSON.stringify(config);
    if (this._worker) {
      const resp = await this._workerCall('load-model-clap', { modelBytes, configJson });
      return resp.handle;
    }
    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);
    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed');
    abi.copyBytesIn(modelPtr, modelBytes);
    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) { abi.free(modelPtr, modelBytes.length); throw new Error('WASM allocation failed'); }
    abi.copyBytesIn(configPtr, configBytes);
    const handle = this.wasm.load_model_clap(modelPtr, abi.size(modelBytes.length), configPtr, abi.size(configBytes.length));
    abi.free(modelPtr, modelBytes.length);
    abi.free(configPtr, configBytes.length);
    if (handle === 0) throw new Error('Failed to load CLAP model');
    return handle;
  }

  /**
   * Run CLAP text encoder. Returns [batch, projection_dim] text embeddings.
   */
  clapEmbedText(modelHandle, tokenIds, attentionMask, batchSize, seqLen) {
    if (this._worker) {
      return this._workerCall('clap-embed-text', {
        modelHandle,
        tokenIds: Array.from(tokenIds instanceof BigInt64Array ? tokenIds.map(Number) : tokenIds),
        attentionMask: Array.from(attentionMask instanceof BigInt64Array ? attentionMask.map(Number) : attentionMask),
        batchSize, seqLen,
      }).then(r => r.output);
    }
    const abi = this._getAbi();
    const ids = tokenIds instanceof BigInt64Array ? tokenIds : new BigInt64Array(tokenIds.map(BigInt));
    const mask = attentionMask instanceof BigInt64Array ? attentionMask : new BigInt64Array(attentionMask.map(BigInt));
    const idsByteLen = ids.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);
    const maskByteLen = mask.length * 8;
    const maskPtr = abi.alloc(maskByteLen);
    abi.writeArray(BigInt64Array, maskPtr, mask);
    const maxOut = batchSize * 2048;
    const outByteLen = maxOut * 4;
    const outPtr = abi.alloc(outByteLen);
    const resultLen = this.wasm.clap_embed_text(modelHandle, idsPtr, abi.size(ids.length), maskPtr, abi.size(mask.length), batchSize, seqLen, outPtr);
    let output;
    if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
    abi.free(idsPtr, idsByteLen);
    abi.free(maskPtr, maskByteLen);
    abi.free(outPtr, outByteLen);
    if (!output) throw new Error('CLAP text embedding failed');
    return output;
  }

  /**
   * Run CLAP audio encoder. Returns [batch, projection_dim] audio embeddings.
   * @param {Float32Array} inputFeatures - mel features from audio preprocessing
   * @param {Uint8Array} isLonger - per-batch flag (0 or 1) indicating if audio was longer than chunk
   */
  clapEmbedAudio(modelHandle, inputFeatures, batchSize, channels, timeFrames, melBins, isLonger) {
    if (this._worker) {
      return this._workerCall('clap-embed-audio', {
        modelHandle, inputFeatures, batchSize, channels, timeFrames, melBins, isLonger,
      }).then(r => r.output);
    }
    const abi = this._getAbi();
    const featByteLen = inputFeatures.length * 4;
    const featPtr = abi.alloc(featByteLen);
    abi.writeArray(Float32Array, featPtr, inputFeatures);
    const longerPtr = abi.alloc(isLonger.length);
    abi.copyBytesIn(longerPtr, isLonger);
    const maxOut = batchSize * 2048;
    const outByteLen = maxOut * 4;
    const outPtr = abi.alloc(outByteLen);
    const resultLen = this.wasm.clap_embed_audio(modelHandle, featPtr, abi.size(inputFeatures.length), batchSize, channels, timeFrames, melBins, longerPtr, abi.size(isLonger.length), outPtr);
    let output;
    if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
    abi.free(featPtr, featByteLen);
    abi.free(longerPtr, isLonger.length);
    abi.free(outPtr, outByteLen);
    if (!output) throw new Error('CLAP audio embedding failed');
    return output;
  }

  // --- Florence-2 API ---

  /**
   * Load a Florence-2 model from SafeTensors.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource
   * @param {object|string} config
   * @returns {Promise<number>} model handle
   */
  async loadFlorenceModel(modelSource, config) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }
    const configJson = typeof config === 'string' ? config : JSON.stringify(config);
    if (this._worker) {
      const resp = await this._workerCall('load-model-florence', { modelBytes, configJson });
      return resp.handle;
    }
    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);
    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed');
    abi.copyBytesIn(modelPtr, modelBytes);
    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) { abi.free(modelPtr, modelBytes.length); throw new Error('WASM allocation failed'); }
    abi.copyBytesIn(configPtr, configBytes);
    const handle = this.wasm.load_model_florence(modelPtr, abi.size(modelBytes.length), configPtr, abi.size(configBytes.length));
    abi.free(modelPtr, modelBytes.length);
    abi.free(configPtr, configBytes.length);
    if (handle === 0) throw new Error('Failed to load Florence model');
    return handle;
  }

  /**
   * Run Florence-2 encoder (vision + text prompt). Returns { hidden: Float32Array, encSeq: number }.
   * @param {Float32Array} pixelValues - pre-normalized CHW f32
   * @param {BigInt64Array|number[]} promptIds - prompt token IDs
   */
  florenceEncode(modelHandle, pixelValues, promptIds, batchSize) {
    if (this._worker) {
      return this._workerCall('florence-encode', {
        modelHandle, pixelValues,
        promptIds: Array.from(promptIds instanceof BigInt64Array ? promptIds.map(Number) : promptIds),
        batchSize,
      }).then(r => r);
    }
    const abi = this._getAbi();
    const ids = promptIds instanceof BigInt64Array ? promptIds : new BigInt64Array(promptIds.map(BigInt));
    const pixelByteLen = pixelValues.length * 4;
    const pixelPtr = abi.alloc(pixelByteLen);
    abi.writeArray(Float32Array, pixelPtr, pixelValues);
    const idsByteLen = ids.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);
    const maxOut = batchSize * 2000 * 1024; // enc_seq * d_model
    const outByteLen = maxOut * 4;
    const outPtr = abi.alloc(outByteLen);
    const encSeqPtr = abi.alloc(4);
    const resultLen = this.wasm.florence_encode(modelHandle, pixelPtr, abi.size(pixelValues.length), idsPtr, abi.size(ids.length), batchSize, outPtr, encSeqPtr);
    let hidden, encSeq;
    if (resultLen > 0) {
      hidden = abi.readCopy(Float32Array, outPtr, resultLen);
      encSeq = abi.readCopy(Uint32Array, encSeqPtr, 1)[0];
    }
    abi.free(pixelPtr, pixelByteLen);
    abi.free(idsPtr, idsByteLen);
    abi.free(outPtr, outByteLen);
    abi.free(encSeqPtr, 4);
    if (!hidden) throw new Error('Florence encode failed');
    return { hidden, encSeq };
  }

  /**
   * Run the Florence/BART text encoder. Returns encoder hidden states.
   */
  florenceEncodeText(modelHandle, inputIds, batchSize, seqLen) {
    if (this._worker) {
      return this._workerCall('florence-encode-text', {
        modelHandle,
        inputIds: Array.from(inputIds instanceof BigInt64Array ? inputIds.map(Number) : inputIds),
        batchSize,
        seqLen,
      }).then(r => r.output);
    }
    const abi = this._getAbi();
    const ids = inputIds instanceof BigInt64Array ? inputIds : new BigInt64Array(inputIds.map(BigInt));
    const idsByteLen = ids.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);
    const maxOut = batchSize * Math.max(seqLen, 1) * 2048;
    const outByteLen = maxOut * 4;
    const outPtr = abi.alloc(outByteLen);
    const resultLen = this.wasm.florence_encode_text(modelHandle, idsPtr, abi.size(ids.length), batchSize, seqLen, outPtr);
    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }
    abi.free(idsPtr, idsByteLen);
    abi.free(outPtr, outByteLen);
    if (!output) throw new Error('Florence text encode failed');
    return output;
  }

  /**
   * Run Florence-2 decoder. Returns logits [batch, vocab_size].
   */
  florenceDecode(modelHandle, decoderIds, encoderHidden, encoderMask, batchSize, decSeq, encSeq) {
    if (this._worker) {
      return this._workerCall('florence-decode', {
        modelHandle, decoderIds: Array.from(decoderIds instanceof BigInt64Array ? decoderIds.map(Number) : decoderIds),
        encoderHidden, encoderMask: Array.from(encoderMask instanceof BigInt64Array ? encoderMask.map(Number) : encoderMask),
        batchSize, decSeq, encSeq,
      }).then(r => r.output);
    }
    const abi = this._getAbi();
    const ids = decoderIds instanceof BigInt64Array ? decoderIds : new BigInt64Array(decoderIds.map(BigInt));
    const mask = encoderMask instanceof BigInt64Array ? encoderMask : new BigInt64Array(encoderMask.map(BigInt));
    const idsByteLen = ids.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);
    const encByteLen = encoderHidden.length * 4;
    const encPtr = abi.alloc(encByteLen);
    abi.writeArray(Float32Array, encPtr, encoderHidden);
    const maskByteLen = mask.length * 8;
    const maskPtr = abi.alloc(maskByteLen);
    abi.writeArray(BigInt64Array, maskPtr, mask);
    const maxOut = batchSize * 52000;
    const outByteLen = maxOut * 4;
    const outPtr = abi.alloc(outByteLen);
    const resultLen = this.wasm.florence_decode(modelHandle, idsPtr, abi.size(ids.length), encPtr, abi.size(encoderHidden.length), maskPtr, abi.size(mask.length), batchSize, decSeq, encSeq, outPtr);
    let output;
    if (resultLen > 0) { output = abi.readCopy(Float32Array, outPtr, resultLen); }
    abi.free(idsPtr, idsByteLen);
    abi.free(encPtr, encByteLen);
    abi.free(maskPtr, maskByteLen);
    abi.free(outPtr, outByteLen);
    if (!output) throw new Error('Florence decode failed');
    return output;
  }

  // --- Tokenizer API ---

  /**
   * Load a HuggingFace tokenizer.json or derive one from GGUF metadata.
   * String input is treated as tokenizer JSON unless `options.format === 'gguf'`.
   * @param {string|ArrayBuffer|Uint8Array|URL} source
   * @param {object} [options]
   * @param {'json'|'gguf'} [options.format]
   * @returns {Promise<number>} tokenizer handle
   */
  async loadTokenizer(source, options = {}) {
    let tokenizerBytes;
    let format = options.format;

    if (typeof source === 'string') {
      if (format === 'gguf') {
        const resp = await fetch(source);
        tokenizerBytes = new Uint8Array(await resp.arrayBuffer());
      } else {
        tokenizerBytes = TEXT_ENCODER.encode(source);
      }
    } else if (source instanceof ArrayBuffer) {
      tokenizerBytes = new Uint8Array(source);
    } else if (source instanceof Uint8Array) {
      tokenizerBytes = source;
    } else if (source instanceof URL) {
      const resp = await fetch(source);
      tokenizerBytes = new Uint8Array(await resp.arrayBuffer());
    } else {
      throw new Error('Unsupported tokenizer source');
    }

    if (!format) {
      format = tokenizerBytes.length >= 4 &&
        tokenizerBytes[0] === 0x47 &&
        tokenizerBytes[1] === 0x47 &&
        tokenizerBytes[2] === 0x55 &&
        tokenizerBytes[3] === 0x46
        ? 'gguf'
        : 'json';
    }

    if (this._worker) {
      const resp = await this._workerCall('load-tokenizer', { tokenizerBytes, format });
      return resp.handle;
    }

    const abi = this._getAbi();
    const ptr = abi.alloc(tokenizerBytes.length);
    if (!ptr) throw new Error('WASM allocation failed for tokenizer');
    abi.copyBytesIn(ptr, tokenizerBytes);

    const handle = format === 'gguf'
      ? this.wasm.load_tokenizer_gguf(ptr, abi.size(tokenizerBytes.length))
      : this.wasm.load_tokenizer(ptr, abi.size(tokenizerBytes.length));
    abi.free(ptr, tokenizerBytes.length);

    if (handle === 0) throw new Error('Failed to load tokenizer');
    return handle;
  }

  /**
   * Decode token IDs back to text using a loaded tokenizer.
   * @param {number} tokenizerHandle
   * @param {number[]|Int32Array} tokenIds
   * @returns {string|Promise<string>}
   */
  decodeTokens(tokenizerHandle, tokenIds) {
    const ids = tokenIds instanceof Int32Array ? tokenIds : Int32Array.from(tokenIds);
    if (this._worker) {
      return this._workerCall('decode-tokens', {
        tokenizerHandle,
        tokenIds: Array.from(ids),
      }).then((resp) => resp.text);
    }

    const abi = this._getAbi();
    const idsByteLen = ids.length * 4;
    const idsPtr = abi.alloc(idsByteLen);
    if (!idsPtr) throw new Error('WASM allocation failed for token IDs');
    abi.writeArray(Int32Array, idsPtr, ids);

    const outLenPtr = abi.alloc(4);
    if (!outLenPtr) {
      abi.free(idsPtr, idsByteLen);
      throw new Error('WASM allocation failed for decode metadata');
    }

    const textPtr = this.wasm.decode_tokens(
      tokenizerHandle,
      idsPtr,
      abi.size(ids.length),
      outLenPtr,
    );
    const textLen = abi.readCopy(Uint32Array, outLenPtr, 1)[0];
    abi.free(outLenPtr, 4);
    abi.free(idsPtr, idsByteLen);

    if (!textPtr) throw new Error('Token decode failed');
    const textBytes = abi.readCopy(Uint8Array, textPtr, textLen);
    abi.free(textPtr, textLen);
    return TEXT_DECODER.decode(textBytes);
  }

  /**
   * Render a single-turn chat prompt via a tokenizer-associated chat template.
   * @param {number} tokenizerHandle
   * @param {string} templateSource
   * @param {string} userPrompt
   * @param {object} [options]
   * @param {string} [options.systemPrompt]
   * @param {boolean} [options.addGenerationPrompt=true]
   * @returns {string|Promise<string>}
   */
  renderChatPrompt(tokenizerHandle, templateSource, userPrompt, options = {}) {
    const systemPrompt = options.systemPrompt ?? '';
    const addGenerationPrompt = options.addGenerationPrompt ?? true;

    if (this._worker) {
      return this._workerCall('render-chat-prompt', {
        tokenizerHandle,
        templateSource,
        userPrompt,
        systemPrompt,
        addGenerationPrompt,
      }).then((resp) => resp.text);
    }

    const abi = this._getAbi();
    const templateBytes = TEXT_ENCODER.encode(templateSource);
    const systemBytes = TEXT_ENCODER.encode(systemPrompt);
    const userBytes = TEXT_ENCODER.encode(userPrompt);

    const templateAllocLen = Math.max(1, templateBytes.length);
    const systemAllocLen = Math.max(1, systemBytes.length);
    const userAllocLen = Math.max(1, userBytes.length);

    const templatePtr = abi.alloc(templateAllocLen);
    const systemPtr = abi.alloc(systemAllocLen);
    const userPtr = abi.alloc(userAllocLen);
    if (!templatePtr || !systemPtr || !userPtr) {
      throw new Error('WASM allocation failed for chat template render input');
    }
    if (templateBytes.length > 0) abi.copyBytesIn(templatePtr, templateBytes);
    if (systemBytes.length > 0) abi.copyBytesIn(systemPtr, systemBytes);
    if (userBytes.length > 0) abi.copyBytesIn(userPtr, userBytes);

    const outLenPtr = abi.alloc(4);
    if (!outLenPtr) {
      abi.free(templatePtr, templateAllocLen);
      abi.free(systemPtr, systemAllocLen);
      abi.free(userPtr, userAllocLen);
      throw new Error('WASM allocation failed for chat template render metadata');
    }

    const textPtr = this.wasm.render_chat_prompt(
      tokenizerHandle,
      templatePtr,
      abi.size(templateBytes.length),
      systemPtr,
      abi.size(systemBytes.length),
      userPtr,
      abi.size(userBytes.length),
      addGenerationPrompt ? 1 : 0,
      outLenPtr,
    );
    const textLen = abi.readCopy(Uint32Array, outLenPtr, 1)[0];

    abi.free(outLenPtr, 4);
    abi.free(templatePtr, templateAllocLen);
    abi.free(systemPtr, systemAllocLen);
    abi.free(userPtr, userAllocLen);

    if (!textPtr || textLen === 0) {
      throw new Error('Chat prompt render failed');
    }

    const textBytes = abi.readCopy(Uint8Array, textPtr, textLen);
    abi.free(textPtr, textLen);
    return TEXT_DECODER.decode(textBytes);
  }

  /**
   * Tokenize single text as [CLS] text [SEP].
   * @param {number} tokenizerHandle
   * @param {string} text
   * @param {number} [maxLen=512]
   * @returns {{ ids: Int32Array, mask: Int32Array }|Promise<{ ids: Int32Array, mask: Int32Array }>}
   */
  tokenize(tokenizerHandle, text, maxLen = 512) {
    if (this._worker) {
      return this._workerCall('tokenize', { tokenizerHandle, text, maxLen })
        .then(resp => ({ ids: resp.ids, mask: resp.mask }));
    }

    const abi = this._getAbi();
    const textBytes = TEXT_ENCODER.encode(text);
    const textPtr = abi.alloc(textBytes.length);
    if (!textPtr) throw new Error('WASM allocation failed for text');
    abi.copyBytesIn(textPtr, textBytes);

    const idsByteLen = maxLen * 4;
    const idsPtr = abi.alloc(idsByteLen);
    const maskPtr = abi.alloc(idsByteLen);
    if (!idsPtr || !maskPtr) throw new Error('WASM allocation failed for output');

    const result = this.wasm.tokenize(
      tokenizerHandle,
      textPtr,
      abi.size(textBytes.length),
      maxLen,
      idsPtr,
      maskPtr,
    );

    let ids, mask;
    if (result > 0) {
      ids = abi.readCopy(Int32Array, idsPtr, maxLen);
      mask = abi.readCopy(Int32Array, maskPtr, maxLen);
    }

    abi.free(textPtr, textBytes.length);
    abi.free(idsPtr, idsByteLen);
    abi.free(maskPtr, idsByteLen);

    if (!ids) throw new Error('Tokenization failed');
    return { ids, mask };
  }

  /**
   * Tokenize a query-document pair as [CLS] textA [SEP] textB [SEP].
   * @param {number} tokenizerHandle
   * @param {string} textA
   * @param {string} textB
   * @param {number} [maxLen=512]
   * @returns {{ ids: Int32Array, mask: Int32Array }|Promise<{ ids: Int32Array, mask: Int32Array }>}
   */
  tokenizePair(tokenizerHandle, textA, textB, maxLen = 512) {
    if (this._worker) {
      return this._workerCall('tokenize-pair', { tokenizerHandle, textA, textB, maxLen })
        .then(resp => ({ ids: resp.ids, mask: resp.mask }));
    }

    const abi = this._getAbi();
    const aBytes = new TextEncoder().encode(textA);
    const bBytes = new TextEncoder().encode(textB);

    const aPtr = abi.alloc(aBytes.length);
    const bPtr = abi.alloc(bBytes.length);
    if (!aPtr || !bPtr) throw new Error('WASM allocation failed for text');
    abi.copyBytesIn(aPtr, aBytes);
    abi.copyBytesIn(bPtr, bBytes);

    const idsByteLen = maxLen * 4;
    const idsPtr = abi.alloc(idsByteLen);
    const maskPtr = abi.alloc(idsByteLen);
    if (!idsPtr || !maskPtr) throw new Error('WASM allocation failed for output');

    const result = this.wasm.tokenize_pair(
      tokenizerHandle,
      aPtr, abi.size(aBytes.length),
      bPtr, abi.size(bBytes.length),
      maxLen, idsPtr, maskPtr,
    );

    let ids, mask;
    if (result > 0) {
      ids = abi.readCopy(Int32Array, idsPtr, maxLen);
      mask = abi.readCopy(Int32Array, maskPtr, maxLen);
    }

    abi.free(aPtr, aBytes.length);
    abi.free(bPtr, bBytes.length);
    abi.free(idsPtr, idsByteLen);
    abi.free(maskPtr, idsByteLen);

    if (!ids) throw new Error('Pair tokenization failed');
    return { ids, mask };
  }

  /**
   * Rerank documents against a query using a cross-encoder model.
   * @param {number} modelHandle
   * @param {number} tokenizerHandle
   * @param {string} query
   * @param {string[]} documents
   * @param {object} [options]
   * @param {number} [options.maxLen=512]
   * @param {number} [options.numLabels=1]
   * @returns {Float32Array|Promise<Float32Array>}
   */
  rerank(modelHandle, tokenizerHandle, query, documents, options = {}) {
    const maxLen = options.maxLen || 512;
    const numLabels = options.numLabels || 1;

    if (this._worker) {
      return this._workerCall('rerank', {
        modelHandle, tokenizerHandle, query, documents, maxLen, numLabels,
      }).then(resp => resp.scores);
    }

    // Direct mode
    const abi = this._getAbi();
    const batch = documents.length;
    if (batch === 0) return new Float32Array(0);

    const allIds = new BigInt64Array(batch * maxLen);
    const allMask = new BigInt64Array(batch * maxLen);

    for (let i = 0; i < batch; i++) {
      const { ids, mask } = this.tokenizePair(tokenizerHandle, query, documents[i], maxLen);
      for (let j = 0; j < maxLen; j++) {
        allIds[i * maxLen + j] = BigInt(ids[j]);
        allMask[i * maxLen + j] = BigInt(mask[j]);
      }
    }

    const idsByteLen = allIds.length * 8;
    const maskByteLen = allMask.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    const maskPtr = abi.alloc(maskByteLen);
    if (!idsPtr || !maskPtr) throw new Error('WASM allocation failed for rerank inputs');

    abi.writeArray(BigInt64Array, idsPtr, allIds);
    abi.writeArray(BigInt64Array, maskPtr, allMask);

    const outByteLen = batch * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) throw new Error('WASM allocation failed for rerank output');

    const resultLen = this.wasm.rerank(
      modelHandle,
      idsPtr, abi.size(allIds.length),
      maskPtr, abi.size(allMask.length),
      batch, maxLen, numLabels,
      outPtr,
    );

    let scores;
    if (resultLen > 0) {
      scores = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(maskPtr, maskByteLen);
    abi.free(outPtr, outByteLen);

    if (!scores) throw new Error('Reranking failed');
    return scores;
  }

  // --- GLiNER API ---

  /**
   * Load a GLiNER model (DeBERTa + span classification head) from SafeTensors.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource
   * @param {object|string} config - DeBERTa config object or JSON string
   * @returns {Promise<number>} model handle
   */
  async loadGlinerModel(modelSource, config) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }

    const configJson = typeof config === 'string' ? config : JSON.stringify(config);

    if (this._worker) {
      const resp = await this._workerCall('load-model-gliner', {
        modelBytes, configJson,
      });
      return resp.handle;
    }

    // Direct mode
    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);

    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed for model data');
    abi.copyBytesIn(modelPtr, modelBytes);

    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) {
      abi.free(modelPtr, modelBytes.length);
      throw new Error('WASM allocation failed for config data');
    }
    abi.copyBytesIn(configPtr, configBytes);

    const handle = this.wasm.load_model_gliner(
      modelPtr,
      abi.size(modelBytes.length),
      configPtr,
      abi.size(configBytes.length),
    );

    abi.free(modelPtr, modelBytes.length);
    abi.free(configPtr, configBytes.length);

    if (handle === 0) throw new Error('Failed to load GLiNER model');
    return handle;
  }

  /**
   * Encode text to raw token IDs without [CLS]/[SEP] wrapping.
   * @param {number} tokenizerHandle
   * @param {string} text
   * @param {number} [maxIds=256]
   * @returns {Int32Array|Promise<Int32Array>}
   */
  tokenizeRaw(tokenizerHandle, text, maxIds = 256) {
    if (this._worker) {
      return this._workerCall('tokenize-raw', { tokenizerHandle, text, maxIds })
        .then(resp => resp.ids);
    }

    const abi = this._getAbi();
    const textBytes = new TextEncoder().encode(text);
    const textPtr = abi.alloc(textBytes.length);
    if (!textPtr) throw new Error('WASM allocation failed for text');
    abi.copyBytesIn(textPtr, textBytes);

    const outByteLen = maxIds * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) throw new Error('WASM allocation failed for output');

    const numIds = this.wasm.tokenize_raw(
      tokenizerHandle,
      textPtr,
      abi.size(textBytes.length),
      outPtr,
      maxIds,
    );

    let ids;
    if (numIds > 0) {
      ids = abi.readCopy(Int32Array, outPtr, numIds);
    }

    abi.free(textPtr, textBytes.length);
    abi.free(outPtr, outByteLen);

    if (!ids) throw new Error('Raw tokenization failed');
    return ids;
  }

  /**
   * Run GLiNER zero-shot NER.
   * @param {number} modelHandle - GLiNER model handle (from loadGlinerModel)
   * @param {number} tokenizerHandle - tokenizer handle
   * @param {string} text - input text
   * @param {string[]} labels - entity type labels
   * @param {object} [options]
   * @param {number} [options.maxWidth=8] - maximum entity span width in words
   * @param {number} [options.maxLen=512] - maximum sequence length
   * @param {number} [options.threshold=0.5] - sigmoid threshold for entity detection
   * @param {boolean} [options.flatNer=true] - remove overlapping entities
   * @param {number} [options.tokenP=128003] - [P] special token ID
   * @param {number} [options.tokenE=128005] - [E] special token ID
   * @param {number} [options.tokenSepText=128002] - [SEP_TEXT] special token ID
   * @param {(stage: string) => void} [options.onProgress] - optional stage callback
   * @returns {Array<{text: string, label: string, start: number, end: number, score: number}>}
   */
  async gliner(modelHandle, tokenizerHandle, text, labels, options = {}) {
    const maxWidth = options.maxWidth ?? 8;
    const maxLen = options.maxLen ?? 512;
    const threshold = options.threshold ?? 0.5;
    const flatNer = options.flatNer ?? true;
    const tokenP = options.tokenP ?? 128003;
    const tokenE = options.tokenE ?? 128005;
    const tokenSepText = options.tokenSepText ?? 128002;
    const onProgress = typeof options.onProgress === 'function' ? options.onProgress : null;

    const report = (stage) => {
      if (onProgress) onProgress(stage);
    };

    // --- 1. Split text into words (whitespace-based) ---
    report('Splitting prompt into words…');
    const words = [];
    const wordStarts = [];
    const wordEnds = [];
    let i = 0;
    while (i < text.length) {
      if (text[i] === ' ' || text[i] === '\t' || text[i] === '\n' || text[i] === '\r') {
        i++;
        continue;
      }
      const start = i;
      while (i < text.length && text[i] !== ' ' && text[i] !== '\t' && text[i] !== '\n' && text[i] !== '\r') i++;
      words.push(text.slice(start, i));
      wordStarts.push(start);
      wordEnds.push(i);
    }

    // --- 2. Build schema prefix ---
    // Pattern: ( [P] entities ( [E] label1 [E] label2 ... ) ) [SEP_TEXT]
    const tokenCache = new Map();
    const tokenizeRawAsync = async (t) => {
      if (tokenCache.has(t)) return tokenCache.get(t);
      const r = Array.from(await this.tokenizeRaw(tokenizerHandle, t));
      tokenCache.set(t, r);
      return r;
    };

    report('Tokenizing GLiNER schema…');
    const schemaIds = [];
    schemaIds.push(...await tokenizeRawAsync('('));
    schemaIds.push(tokenP);
    schemaIds.push(...await tokenizeRawAsync('entities'));
    schemaIds.push(...await tokenizeRawAsync('('));
    for (const label of labels) {
      schemaIds.push(tokenE);
      schemaIds.push(...await tokenizeRawAsync(label));
    }
    schemaIds.push(...await tokenizeRawAsync(')'));
    schemaIds.push(...await tokenizeRawAsync(')'));
    schemaIds.push(tokenSepText);

    // --- 3. Tokenize each word individually (lowercased) ---
    report(`Tokenizing ${words.length} word${words.length === 1 ? '' : 's'}…`);
    const wordTokenCounts = [];
    const textIds = [];
    for (const word of words) {
      const ids = await tokenizeRawAsync(word.toLowerCase());
      wordTokenCounts.push(ids.length);
      textIds.push(...ids);
    }

    // --- 4. Build input tensors ---
    const totalTokens = schemaIds.length + textIds.length;
    const seqLen = Math.min(totalTokens, maxLen);

    const inputIds = new Array(seqLen).fill(0);
    const attentionMask = new Array(seqLen).fill(0);
    const wordsMask = new Array(seqLen).fill(0);

    report('Building GLiNER span tensors…');
    // Fill schema tokens
    for (let j = 0; j < Math.min(schemaIds.length, seqLen); j++) {
      inputIds[j] = schemaIds[j];
      attentionMask[j] = 1;
      wordsMask[j] = 0; // schema tokens are non-word
    }

    // Fill text tokens with word IDs in words_mask
    let pos = schemaIds.length;
    let actualNumWords = 0;
    let textIdOffset = 0;
    for (let w = 0; w < words.length && pos < seqLen; w++) {
      const count = wordTokenCounts[w];
      let tokensAdded = 0;
      for (let t = 0; t < count && pos < seqLen; t++) {
        inputIds[pos] = textIds[textIdOffset + t];
        attentionMask[pos] = 1;
        wordsMask[pos] = w + 1; // 1-indexed word ID
        pos++;
        tokensAdded++;
      }
      if (tokensAdded > 0) actualNumWords = w + 1;
      textIdOffset += count;
    }

    // --- 5. Build span_idx ---
    const numSpans = actualNumWords * maxWidth;
    const spanIdx = new Array(numSpans * 2).fill(0);
    for (let w = 0; w < actualNumWords; w++) {
      for (let wi = 0; wi < maxWidth; wi++) {
        const spanPos = (w * maxWidth + wi) * 2;
        const endWord = w + wi;
        if (endWord < actualNumWords) {
          spanIdx[spanPos] = w;
          spanIdx[spanPos + 1] = endWord;
        } else {
          spanIdx[spanPos] = 0;
          spanIdx[spanPos + 1] = 0;
        }
      }
    }

    // --- 6. Call WASM gliner() ---
    const batch = 1;

    let logits = null;
    let meta = null;
    if (this._worker) {
      report('Running GLiNER inference in worker…');
      const resp = await this._workerCall('gliner', {
        modelHandle,
        inputIds,
        attentionMask,
        wordsMask,
        spanIdx,
        batchSize: batch,
        seqLen,
      });
      logits = resp.logits;
      meta = resp.meta;
    } else {
      report('Running GLiNER inference…');
      // Convert all arrays to BigInt64Array for WASM
      const idsI64 = new BigInt64Array(inputIds.map(BigInt));
      const maskI64 = new BigInt64Array(attentionMask.map(BigInt));
      const wmaskI64 = new BigInt64Array(wordsMask.map(BigInt));
      const spanI64 = new BigInt64Array(spanIdx.map(BigInt));

      const idsByteLen = idsI64.length * 8;
      const maskByteLen = maskI64.length * 8;
      const wmaskByteLen = wmaskI64.length * 8;
      const spanByteLen = spanI64.length * 8;

      const abi = this._getAbi();
      const idsPtr = abi.alloc(idsByteLen);
      const maskPtr = abi.alloc(maskByteLen);
      const wmaskPtr = abi.alloc(wmaskByteLen);
      const spanPtr = abi.alloc(spanByteLen);
      if (!idsPtr || !maskPtr || !wmaskPtr || !spanPtr) throw new Error('WASM allocation failed');

      abi.writeArray(BigInt64Array, idsPtr, idsI64);
      abi.writeArray(BigInt64Array, maskPtr, maskI64);
      abi.writeArray(BigInt64Array, wmaskPtr, wmaskI64);
      abi.writeArray(BigInt64Array, spanPtr, spanI64);

      // Output buffer: worst case num_words * max_width * num_labels
      const maxLogits = actualNumWords * maxWidth * labels.length;
      const logitsByteLen = maxLogits * 4;
      const logitsPtr = abi.alloc(logitsByteLen);
      const metaByteLen = 3 * 4; // 3 x u32
      const metaPtr = abi.alloc(metaByteLen);
      if (!logitsPtr || !metaPtr) throw new Error('WASM allocation failed for output');

      const numLogits = this.wasm.gliner(
        modelHandle,
        idsPtr, abi.size(idsI64.length),
        maskPtr, abi.size(maskI64.length),
        wmaskPtr, abi.size(wmaskI64.length),
        spanPtr, abi.size(spanI64.length),
        batch, seqLen,
        logitsPtr, metaPtr,
      );

      if (numLogits > 0) {
        logits = abi.readCopy(Float32Array, logitsPtr, numLogits);
        meta = abi.readCopy(Uint32Array, metaPtr, 3);
      }

      abi.free(idsPtr, idsByteLen);
      abi.free(maskPtr, maskByteLen);
      abi.free(wmaskPtr, wmaskByteLen);
      abi.free(spanPtr, spanByteLen);
      abi.free(logitsPtr, logitsByteLen);
      abi.free(metaPtr, metaByteLen);
    }

    if (!logits || !meta) throw new Error('GLiNER inference failed');

    // --- 7. Post-process: sigmoid + threshold + entity extraction ---
    report('Decoding GLiNER spans…');
    const numWords = meta[0];
    const mw = meta[1]; // max_width from head
    const numLabels = meta[2];

    const entities = [];
    for (let w = 0; w < numWords; w++) {
      for (let wi = 0; wi < mw; wi++) {
        const endWord = w + wi;
        if (endWord >= numWords) continue;
        for (let l = 0; l < numLabels; l++) {
          const idx = (w * mw + wi) * numLabels + l;
          const score = 1.0 / (1.0 + Math.exp(-logits[idx]));
          if (score >= threshold) {
            const entityText = words.slice(w, endWord + 1).join(' ');
            entities.push({
              text: entityText,
              label: labels[l],
              start: wordStarts[w],
              end: wordEnds[endWord],
              score,
            });
          }
        }
      }
    }

    // --- 8. Flat NER deduplication ---
    if (flatNer && entities.length > 1) {
      entities.sort((a, b) => b.score - a.score);
      const kept = [];
      for (const ent of entities) {
        const overlaps = kept.some(k => !(ent.end <= k.start || ent.start >= k.end));
        if (!overlaps) kept.push(ent);
      }
      kept.sort((a, b) => a.start - b.start);
      return kept;
    }

    entities.sort((a, b) => a.start - b.start);
    return entities;
  }

  /**
   * Unload a tokenizer and free its resources.
   * @param {number} handle
   */
  unloadTokenizer(handle) {
    if (this._worker) {
      return this._workerCall('unload-tokenizer', { handle });
    }
    this.wasm.unload_tokenizer(handle);
  }

  /**
   * Terminate the inference worker (worker mode only).
   */
  destroy() {
    this.abi = null;
    if (this._worker) {
      this._worker.terminate();
      this._worker = null;
      this._sab = null;
      this._pendingCalls = null;
    }
  }

  // --- GPT (decoder-only generative) API ---

  /**
   * Derive GPT config JSON from GGUF metadata.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource
   * @returns {Promise<object>}
   */
  async inferGptConfigFromGguf(modelSource) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }

    let configJson;
    if (this._worker) {
      const resp = await this._workerCall('infer-gpt-config-gguf', { modelBytes });
      configJson = resp.configJson;
    } else {
      configJson = this._inferGptConfigJsonFromGgufBytes(modelBytes);
    }
    return JSON.parse(configJson);
  }

  /**
   * Derive a chat template string from GGUF metadata when present.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource
   * @returns {Promise<string|null>}
   */
  async inferChatTemplateFromGguf(modelSource) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }

    if (this._worker) {
      const resp = await this._workerCall('infer-chat-template-gguf', { modelBytes });
      return resp.chatTemplate ?? null;
    }
    return this._inferGgufChatTemplateFromBytes(modelBytes);
  }

  /**
   * Load a GPT/decoder-only model (GPT-2, LLaMA, Mistral, Phi, Qwen2, Gemma, etc).
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource - SafeTensors or GGUF file
   * @param {object|string} config - HuggingFace config.json object or JSON string
   * @param {object} [options]
   * @param {'gguf'|'safetensors'} [options.format] - Auto-detected if omitted
   * @returns {Promise<number>} model handle
   */
  async loadGptModel(modelSource, config, options) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }

    let format = options?.format;
    if (!format) {
      const magic = new TextDecoder().decode(modelBytes.slice(0, 4));
      format = magic === 'GGUF' ? 'gguf' : 'safetensors';
    }

    let configJson = config == null ? null : (typeof config === 'string' ? config : JSON.stringify(config));

    if (!configJson && format === 'gguf') {
      if (this._worker) {
        const resp = await this._workerCall('infer-gpt-config-gguf', { modelBytes });
        configJson = resp.configJson;
      } else {
        configJson = this._inferGptConfigJsonFromGgufBytes(modelBytes);
      }
    }

    if (!configJson) {
      throw new Error('GPT model config is required for SafeTensors loads');
    }

    if (this._worker) {
      const resp = await this._workerCall('load-model-gpt', {
        modelBytes, configJson, format,
      });
      return resp.handle;
    }

    // Direct mode
    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);

    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed for model data');
    abi.copyBytesIn(modelPtr, modelBytes);

    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) {
      abi.free(modelPtr, modelBytes.length);
      throw new Error('WASM allocation failed for config data');
    }
    abi.copyBytesIn(configPtr, configBytes);

    const loadFn = format === 'gguf'
      ? this.wasm.load_model_gpt_gguf
      : this.wasm.load_model_gpt;
    const handle = loadFn(
      modelPtr,
      abi.size(modelBytes.length),
      configPtr,
      abi.size(configBytes.length),
    );

    abi.free(modelPtr, modelBytes.length);
    abi.free(configPtr, configBytes.length);

    if (handle === 0) throw new Error('Failed to load GPT model');
    return handle;
  }

  /**
   * Streaming model loader — progressively loads SafeTensors weights.
   * Parses the header first, then streams tensor data and registers
   * weights incrementally, yielding progress events without materializing
   * the whole file in JS memory when ReadableStream is available.
   *
   * @param {string|Response} urlOrResponse - URL or fetch Response
   * @param {string|object} config - HuggingFace config.json (string or object)
   * @param {object} [options]
   * @param {AbortSignal} [options.signal] - AbortController signal
   * @param {function} [options.onProgress] - callback({ loaded, total, currentWeight })
   * @param {string} [options.family] - model family for Conv1D detection (e.g. 'gpt2')
   * @returns {Promise<number>} model handle
   */
  async streamLoadGptModel(urlOrResponse, config, options = {}) {
    const { signal, onProgress, family } = options;
    const configJson = typeof config === 'string' ? config : JSON.stringify(config);
    const inferredFamily = family ?? (typeof config === 'object' ? config.model_type : undefined);

    if (this._worker) {
      if (signal) {
        throw new Error('streamLoadGptModel worker mode does not yet support AbortSignal');
      }
      const modelUrl = typeof urlOrResponse === 'string'
        ? new URL(urlOrResponse, location.href).href
        : (urlOrResponse instanceof URL ? urlOrResponse.href : null);
      if (!modelUrl) {
        throw new Error('streamLoadGptModel worker mode requires a URL or URL string source');
      }
      const resp = await this._workerCall(
        'stream-load-model-gpt',
        { modelUrl, configJson, family: inferredFamily },
        [],
        { onProgress },
      );
      return resp.handle;
    }

    const abi = this._getAbi();

    // Create empty model
    const configBytes = new TextEncoder().encode(configJson);
    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) throw new Error('WASM allocation failed for config');
    abi.copyBytesIn(configPtr, configBytes);
    const handle = this.wasm.create_model_gpt(configPtr, abi.size(configBytes.length));
    abi.free(configPtr, configBytes.length);
    if (!handle) throw new Error('Failed to create empty GPT model');

    try {
      const response = toStreamResponse(urlOrResponse) ?? await fetch(urlOrResponse, { signal });
      await streamRegisterSafetensors({
        response,
        abi,
        wasm: this.wasm,
        handle,
        signal,
        onProgress,
        family: inferredFamily,
      });
      return handle;
    } catch (err) {
      this.wasm.unload_model(handle);
      throw err;
    }
  }

  /**
   * Streaming GGUF loader for GPT/decoder-only models.
   * Requires explicit config JSON today; GGUF-derived config extraction is follow-on work.
   *
   * @param {string|URL|Response} urlOrResponse
   * @param {string|object} config
   * @param {object} [options]
   * @param {AbortSignal} [options.signal]
   * @param {function} [options.onProgress]
   * @param {function} [options.onMetadata] - callback({ tokenizerJson, chatTemplate, tokenizerModel, metadata })
   * @param {boolean} [options.autoLoadTokenizer=false] - automatically load derived tokenizer metadata when available
   * @param {function} [options.onTokenizerLoaded] - callback({ tokenizerHandle, tokenizerJson, chatTemplate, tokenizerModel, metadata })
   * @returns {Promise<number>} model handle
   */
  async streamLoadGgufModel(urlOrResponse, config, options = {}) {
    const {
      signal,
      onProgress,
      onMetadata,
      autoLoadTokenizer = false,
      onTokenizerLoaded,
    } = options;
    if (config == null) {
      throw new Error('streamLoadGgufModel currently requires explicit config JSON');
    }
    const configJson = typeof config === 'string' ? config : JSON.stringify(config);
    const metadataTasks = [];

    const handleMetadata = (metadata) => {
      const task = (async () => {
        if (onMetadata) {
          await onMetadata(metadata);
        }
        if (!autoLoadTokenizer || !metadata?.tokenizerJson) {
          return null;
        }
        const tokenizerHandle = await this.loadTokenizer(metadata.tokenizerJson);
        if (onTokenizerLoaded) {
          await onTokenizerLoaded({ ...metadata, tokenizerHandle });
        }
        return tokenizerHandle;
      })();
      metadataTasks.push(task.then(
        (value) => ({ ok: true, value }),
        (error) => ({ ok: false, error }),
      ));
    };

    const finalizeMetadataTasks = async () => {
      if (metadataTasks.length === 0) return;
      const results = await Promise.all(metadataTasks);
      for (const result of results) {
        if (!result.ok) throw result.error;
      }
    };

    if (this._worker) {
      if (signal) {
        throw new Error('streamLoadGgufModel worker mode does not yet support AbortSignal');
      }
      const modelUrl = typeof urlOrResponse === 'string'
        ? new URL(urlOrResponse, location.href).href
        : (urlOrResponse instanceof URL ? urlOrResponse.href : null);
      if (!modelUrl) {
        throw new Error('streamLoadGgufModel worker mode requires a URL or URL string source');
      }
      const resp = await this._workerCall(
        'stream-load-model-gpt-gguf',
        { modelUrl, configJson },
        [],
        { onProgress, onMetadata: handleMetadata },
      );
      await finalizeMetadataTasks();
      return resp.handle;
    }

    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);
    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) throw new Error('WASM allocation failed for config');
    abi.copyBytesIn(configPtr, configBytes);
    const handle = this.wasm.create_model_gpt(configPtr, abi.size(configBytes.length));
    abi.free(configPtr, configBytes.length);
    if (!handle) throw new Error('Failed to create empty GPT model');

    try {
      const response = toStreamResponse(urlOrResponse) ?? await fetch(urlOrResponse, { signal });
      const gpuResidency = this.gpu && typeof this.wasm.register_weight_gguf_gpu === 'function'
        ? {
            createBuffer: (sizeBytes) => this.gpu.createBuffer(sizeBytes),
            writeBufferAtOffset: (id, offsetBytes, srcBytes) => this.gpu.writeBufferAtOffset(id, offsetBytes, srcBytes),
            freeBuffer: (id) => this.gpu.freeBuffer(id),
          }
        : null;
      await streamRegisterGguf({
        response,
        abi,
        wasm: this.wasm,
        handle,
        gpuResidency,
        signal,
        onProgress,
        onMetadata: handleMetadata,
      });
      await finalizeMetadataTasks();
      return handle;
    } catch (err) {
      this.wasm.unload_model(handle);
      throw err;
    }
  }

  /**
   * Run GPT forward pass. Returns full logits [batch, seq_len, vocab_size].
   * @param {number} modelHandle
   * @param {number[]|BigInt64Array} tokenIds - flattened [batch * seqLen]
   * @param {number} batchSize
   * @param {number} seqLen
   * @returns {Float32Array|Promise<Float32Array>}
   */
  gptForward(modelHandle, tokenIds, batchSize, seqLen) {
    if (this._worker) {
      return this._workerCall('gpt-forward', {
        modelHandle,
        tokenIds: Array.from(tokenIds instanceof BigInt64Array ? tokenIds.map(Number) : tokenIds),
        batchSize,
        seqLen,
      }).then(resp => resp.output);
    }

    // Direct mode
    const abi = this._getAbi();
    const ids = tokenIds instanceof BigInt64Array
      ? tokenIds
      : new BigInt64Array(tokenIds.map(BigInt));

    const idsByteLen = ids.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    if (!idsPtr) throw new Error('WASM allocation failed for token IDs');
    abi.writeArray(BigInt64Array, idsPtr, ids);

    // Output: batch * seq_len * vocab_size (conservative: assume vocab_size <= 128256)
    const maxOutputFloats = batchSize * seqLen * 128256;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) {
      abi.free(idsPtr, idsByteLen);
      throw new Error('WASM allocation failed for output');
    }

    const resultLen = this.wasm.gpt_forward(
      modelHandle,
      idsPtr, abi.size(ids.length),
      batchSize, seqLen,
      outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('GPT forward pass failed');
    return output;
  }

  /**
   * Autoregressive text generation with JS-side sampling.
   * Full recompute each step (no KV cache — Phase 19 adds that).
   * @param {number} modelHandle
   * @param {number} vocabSize - from model config
   * @param {number[]} promptIds - initial token IDs
   * @param {object} [options]
   * @param {number} [options.maxTokens=128]
   * @param {number} [options.temperature=0] - 0 = greedy
   * @param {number} [options.topK=0] - 0 = disabled
   * @param {number} [options.topP=0] - 0 = disabled
   * @param {number} [options.eosTokenId] - stop token
   * @param {number} [options.repetitionPenalty=1.0]
   * @param {function} [options.onToken] - callback(tokenId, step)
   * @returns {Promise<number[]>} generated token IDs (excluding prompt)
   */
  async gptGenerate(modelHandle, vocabSize, promptIds, options = {}) {
    const {
      maxTokens = 128,
      temperature = 0,
      topK = 0,
      topP = 0,
      eosTokenId,
      repetitionPenalty = 1.0,
      onToken,
    } = options;

    const tokens = [...promptIds];

    for (let step = 0; step < maxTokens; step++) {
      const logits = await Promise.resolve(
        this.gptForward(modelHandle, tokens, 1, tokens.length)
      );

      // Extract last token's logits
      const lastLogits = new Float32Array(vocabSize);
      lastLogits.set(logits.slice(logits.length - vocabSize));

      // Apply repetition penalty
      if (repetitionPenalty !== 1.0) {
        const seen = new Set(tokens);
        for (const id of seen) {
          if (id < vocabSize) {
            if (lastLogits[id] > 0) lastLogits[id] /= repetitionPenalty;
            else lastLogits[id] *= repetitionPenalty;
          }
        }
      }

      const nextToken = _sample(lastLogits, temperature, topK, topP);

      if (eosTokenId !== undefined && nextToken === eosTokenId) break;
      tokens.push(nextToken);
      if (onToken) onToken(nextToken, step);
    }

    return tokens.slice(promptIds.length);
  }

  // --- KV Cache API ---

  /**
   * Create a KV cache for a GPT model.
   * @param {number} modelHandle
   * @param {number} maxLen - maximum sequence length
   * @param {object} [options]
   * @param {'f32'|'polar4'|'turbo3'} [options.cacheDtype='f32'] - GPU KV key format
   * @returns {number} cache handle
   */
  gptCreateKvCache(modelHandle, maxLen = 2048, options = {}) {
    const cacheDtype = options.cacheDtype ?? 'f32';
    const keyFormat = this._gpuKvKeyFormat(cacheDtype);
    const valueFormat = 0; // f32 values; compressed V formats are not wired yet.
    let handle;
    if (this.wasm.gpt_create_kv_cache_ex) {
      handle = this.wasm.gpt_create_kv_cache_ex(modelHandle, maxLen, keyFormat, valueFormat);
    } else {
      if (cacheDtype !== 'f32') {
        throw new Error('This WASM module does not support compressed GPU KV caches');
      }
      handle = this.wasm.gpt_create_kv_cache(modelHandle, maxLen);
    }
    if (!handle) throw new Error('Failed to create KV cache');
    return handle;
  }

  _gpuKvKeyFormat(cacheDtype) {
    switch (cacheDtype) {
      case 'f32':
      case undefined:
      case null:
        return 0;
      case 'polar4':
        return 1;
      case 'turbo3':
        return 2;
      default:
        throw new Error(`Unsupported GPU KV cache dtype: ${cacheDtype}`);
    }
  }

  /**
   * Run GPT forward with KV cache. First call does prefill, subsequent
   * calls with seqLen=1 do single-token decode.
   * @param {number} modelHandle
   * @param {number} cacheHandle
   * @param {number[]|BigInt64Array} tokenIds
   * @param {number} batchSize
   * @param {number} seqLen
   * @returns {Float32Array}
   */
  gptForwardCached(modelHandle, cacheHandle, tokenIds, batchSize, seqLen) {
    const abi = this._getAbi();
    const ids = tokenIds instanceof BigInt64Array
      ? tokenIds
      : new BigInt64Array(tokenIds.map(BigInt));

    const idsByteLen = ids.length * 8;
    const idsPtr = abi.alloc(idsByteLen);
    if (!idsPtr) throw new Error('WASM allocation failed for token IDs');
    abi.writeArray(BigInt64Array, idsPtr, ids);

    const maxOutputFloats = batchSize * seqLen * 128256;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) {
      abi.free(idsPtr, idsByteLen);
      throw new Error('WASM allocation failed for output');
    }

    const resultLen = this.wasm.gpt_forward_cached(
      modelHandle, cacheHandle,
      idsPtr, abi.size(ids.length),
      batchSize, seqLen,
      outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('GPT cached forward pass failed');
    return output;
  }

  /** Reset KV cache for a new sequence. */
  gptResetKvCache(cacheHandle) {
    this.wasm.gpt_reset_kv_cache(cacheHandle);
  }

  /** Free KV cache memory. */
  gptFreeKvCache(cacheHandle) {
    this.wasm.gpt_free_kv_cache(cacheHandle);
  }

  /**
   * Autoregressive generation with KV cache (O(1) per token instead of O(N)).
   * @param {number} modelHandle
   * @param {number} vocabSize
   * @param {number[]} promptIds
   * @param {object} [options]
   * @param {number} [options.maxTokens=128]
   * @param {number} [options.maxLen=2048] - KV cache max sequence length
   * @param {number} [options.temperature=0]
   * @param {number} [options.topK=0]
   * @param {number} [options.topP=0]
   * @param {number} [options.eosTokenId]
   * @param {number} [options.repetitionPenalty=1.0]
   * @param {'f32'|'polar4'|'turbo3'} [options.cacheDtype='f32']
   * @param {function} [options.onToken]
   * @returns {Promise<number[]>}
   */
  async gptGenerateCached(modelHandle, vocabSize, promptIds, options = {}) {
    const {
      maxTokens = 128,
      maxLen = 2048,
      temperature = 0,
      topK = 0,
      topP = 0,
      eosTokenId,
      repetitionPenalty = 1.0,
      cacheDtype = 'f32',
      onToken,
    } = options;

    const cache = this.gptCreateKvCache(modelHandle, maxLen, { cacheDtype });
    try {
      // Prefill: forward entire prompt
      const prefillLogits = this.gptForwardCached(
        modelHandle, cache, promptIds, 1, promptIds.length
      );

      const tokens = [...promptIds];
      // Extract last token's logits from prefill
      let lastLogits = new Float32Array(vocabSize);
      lastLogits.set(prefillLogits.slice(prefillLogits.length - vocabSize));

      for (let step = 0; step < maxTokens; step++) {
        if (repetitionPenalty !== 1.0) {
          const seen = new Set(tokens);
          for (const id of seen) {
            if (id < vocabSize) {
              if (lastLogits[id] > 0) lastLogits[id] /= repetitionPenalty;
              else lastLogits[id] *= repetitionPenalty;
            }
          }
        }

        const nextToken = _sample(lastLogits, temperature, topK, topP);
        if (eosTokenId !== undefined && nextToken === eosTokenId) break;
        tokens.push(nextToken);
        if (onToken) onToken(nextToken, step);

        // Decode: forward single new token
        const decLogits = this.gptForwardCached(
          modelHandle, cache, [nextToken], 1, 1
        );
        lastLogits = new Float32Array(vocabSize);
        lastLogits.set(decLogits.slice(decLogits.length - vocabSize));
      }

      return tokens.slice(promptIds.length);
    } finally {
      this.gptFreeKvCache(cache);
    }
  }

  /**
   * Generate from multiple prompts simultaneously using independent KV caches.
   * Each sequence gets its own cache. Sequences that hit EOS stop independently.
   * @param {number} modelHandle
   * @param {number} vocabSize
   * @param {number[][]} promptIdsBatch - array of prompt token ID arrays
   * @param {object} [options]
   * @param {number} [options.maxTokens=128]
   * @param {number} [options.maxLen=2048]
   * @param {number} [options.temperature=0]
   * @param {number} [options.topK=0]
   * @param {number} [options.topP=0]
   * @param {number} [options.eosTokenId]
   * @param {function} [options.onToken] - (batchIndex, token, step) => void
   * @returns {Promise<number[][]>} generated tokens per sequence (excluding prompts)
   */
  async gptGenerateBatch(modelHandle, vocabSize, promptIdsBatch, options = {}) {
    const {
      maxTokens = 128,
      maxLen = 2048,
      temperature = 0,
      topK = 0,
      topP = 0,
      eosTokenId,
      cacheDtype = 'f32',
      onToken,
    } = options;

    const batchSize = promptIdsBatch.length;
    const caches = [];
    const allTokens = [];
    const lastLogitsArr = [];
    const active = [];

    try {
      // Create caches and prefill each sequence independently
      for (let i = 0; i < batchSize; i++) {
        const cache = this.gptCreateKvCache(modelHandle, maxLen, { cacheDtype });
        caches.push(cache);

        const promptIds = promptIdsBatch[i];
        const prefillLogits = this.gptForwardCached(
          modelHandle, cache, promptIds, 1, promptIds.length,
        );

        allTokens.push([...promptIds]);
        const lastLogits = new Float32Array(vocabSize);
        lastLogits.set(prefillLogits.slice(prefillLogits.length - vocabSize));
        lastLogitsArr.push(lastLogits);
        active.push(true);
      }

      // Decode loop
      for (let step = 0; step < maxTokens; step++) {
        let anyActive = false;
        for (let i = 0; i < batchSize; i++) {
          if (!active[i]) continue;
          anyActive = true;

          const nextToken = _sample(lastLogitsArr[i], temperature, topK, topP);
          if (eosTokenId !== undefined && nextToken === eosTokenId) {
            active[i] = false;
            continue;
          }

          allTokens[i].push(nextToken);
          if (onToken) onToken(i, nextToken, step);

          const decLogits = this.gptForwardCached(
            modelHandle, caches[i], [nextToken], 1, 1,
          );
          const newLogits = new Float32Array(vocabSize);
          newLogits.set(decLogits.slice(decLogits.length - vocabSize));
          lastLogitsArr[i] = newLogits;
        }
        if (!anyActive) break;
      }

      return allTokens.map((tokens, i) => tokens.slice(promptIdsBatch[i].length));
    } finally {
      for (const cache of caches) {
        this.gptFreeKvCache(cache);
      }
    }
  }

  // --- T5 / Encoder-Decoder API ---

  /**
   * Load a T5 encoder-decoder model (SafeTensors only).
   * @param {ArrayBuffer|Uint8Array|string|URL} modelSource
   * @param {object|string} config - T5 config JSON
   * @returns {Promise<number>} model handle
   */
  async loadT5Model(modelSource, config) {
    let modelBytes;
    if (modelSource instanceof ArrayBuffer) {
      modelBytes = new Uint8Array(modelSource);
    } else if (modelSource instanceof Uint8Array) {
      modelBytes = modelSource;
    } else {
      const resp = await fetch(modelSource);
      modelBytes = new Uint8Array(await resp.arrayBuffer());
    }

    const configJson = typeof config === 'string' ? config : JSON.stringify(config);

    // Direct mode
    const abi = this._getAbi();
    const configBytes = new TextEncoder().encode(configJson);

    const modelPtr = abi.alloc(modelBytes.length);
    if (!modelPtr) throw new Error('WASM allocation failed for model data');
    abi.copyBytesIn(modelPtr, modelBytes);

    const configPtr = abi.alloc(configBytes.length);
    if (!configPtr) {
      abi.free(modelPtr, modelBytes.length);
      throw new Error('WASM allocation failed for config data');
    }
    abi.copyBytesIn(configPtr, configBytes);

    const handle = this.wasm.load_model_t5(
      modelPtr,
      abi.size(modelBytes.length),
      configPtr,
      abi.size(configBytes.length),
    );

    abi.free(modelPtr, modelBytes.length);
    abi.free(configPtr, configBytes.length);

    if (handle === 0) throw new Error('Failed to load T5 model');
    return handle;
  }

  /**
   * Run T5 encoder. Returns [batch * seqLen * dModel] f32 encoder hidden states.
   * @param {number} modelHandle
   * @param {number[]|BigInt64Array} tokenIds - flattened [batch * seqLen]
   * @param {number[]|BigInt64Array} attentionMask - flattened [batch * seqLen]
   * @param {number} batchSize
   * @param {number} seqLen
   * @returns {Float32Array}
   */
  t5Encode(modelHandle, tokenIds, attentionMask, batchSize, seqLen) {
    const abi = this._getAbi();
    const ids = tokenIds instanceof BigInt64Array
      ? tokenIds
      : new BigInt64Array(tokenIds.map(BigInt));
    const mask = attentionMask instanceof BigInt64Array
      ? attentionMask
      : new BigInt64Array(attentionMask.map(BigInt));

    const idsByteLen = ids.length * 8;
    const maskByteLen = mask.length * 8;

    const idsPtr = abi.alloc(idsByteLen);
    if (!idsPtr) throw new Error('WASM allocation failed for token IDs');
    abi.writeArray(BigInt64Array, idsPtr, ids);

    const maskPtr = abi.alloc(maskByteLen);
    if (!maskPtr) {
      abi.free(idsPtr, idsByteLen);
      throw new Error('WASM allocation failed for attention mask');
    }
    abi.writeArray(BigInt64Array, maskPtr, mask);

    // Allocate output: batch * seqLen * d_model (assume max d_model = 1024 for T5-large)
    const maxOutputFloats = batchSize * seqLen * 1024;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) {
      abi.free(idsPtr, idsByteLen);
      abi.free(maskPtr, maskByteLen);
      throw new Error('WASM allocation failed for output');
    }

    const resultLen = this.wasm.t5_encode(
      modelHandle,
      idsPtr, abi.size(ids.length),
      maskPtr, abi.size(mask.length),
      batchSize, seqLen,
      outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(maskPtr, maskByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('T5 encoder forward pass failed');
    return output;
  }

  /**
   * Run T5 decoder. Returns [batch * decSeq * vocabSize] f32 logits.
   * @param {number} modelHandle
   * @param {Float32Array} encoderOutput - [batch * encSeq * dModel]
   * @param {number[]|BigInt64Array} encoderMask - [batch * encSeq]
   * @param {number[]|BigInt64Array} decoderIds - [batch * decSeq]
   * @param {number} batchSize
   * @param {number} decSeq
   * @param {number} encSeq
   * @returns {Float32Array}
   */
  t5Decode(modelHandle, encoderOutput, encoderMask, decoderIds, batchSize, decSeq, encSeq) {
    const abi = this._getAbi();
    const encMask = encoderMask instanceof BigInt64Array
      ? encoderMask
      : new BigInt64Array(encoderMask.map(BigInt));
    const decIds = decoderIds instanceof BigInt64Array
      ? decoderIds
      : new BigInt64Array(decoderIds.map(BigInt));

    const encOutByteLen = encoderOutput.length * 4;
    const encMaskByteLen = encMask.length * 8;
    const decIdsByteLen = decIds.length * 8;

    const encOutPtr = abi.alloc(encOutByteLen);
    if (!encOutPtr) throw new Error('WASM allocation failed for encoder output');
    abi.writeArray(Float32Array, encOutPtr, encoderOutput);

    const encMaskPtr = abi.alloc(encMaskByteLen);
    if (!encMaskPtr) {
      abi.free(encOutPtr, encOutByteLen);
      throw new Error('WASM allocation failed for encoder mask');
    }
    abi.writeArray(BigInt64Array, encMaskPtr, encMask);

    const decIdsPtr = abi.alloc(decIdsByteLen);
    if (!decIdsPtr) {
      abi.free(encOutPtr, encOutByteLen);
      abi.free(encMaskPtr, encMaskByteLen);
      throw new Error('WASM allocation failed for decoder IDs');
    }
    abi.writeArray(BigInt64Array, decIdsPtr, decIds);

    // Allocate output: batch * decSeq * vocab_size (T5-small = 32128)
    const maxVocabSize = 32128;
    const maxOutputFloats = batchSize * decSeq * maxVocabSize;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) {
      abi.free(encOutPtr, encOutByteLen);
      abi.free(encMaskPtr, encMaskByteLen);
      abi.free(decIdsPtr, decIdsByteLen);
      throw new Error('WASM allocation failed for output');
    }

    const resultLen = this.wasm.t5_decode(
      modelHandle,
      encOutPtr, abi.size(encoderOutput.length),
      encMaskPtr, abi.size(encMask.length),
      decIdsPtr, abi.size(decIds.length),
      batchSize, decSeq, encSeq,
      outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(encOutPtr, encOutByteLen);
    abi.free(encMaskPtr, encMaskByteLen);
    abi.free(decIdsPtr, decIdsByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('T5 decoder forward pass failed');
    return output;
  }

  /**
   * Autoregressive T5 generation (encode once, decode greedily).
   * No decoder KV cache — full recompute per step.
   * @param {number} modelHandle
   * @param {number} vocabSize - e.g. 32128 for T5
   * @param {number[]|BigInt64Array} inputIds - encoder input [batch * encSeq]
   * @param {number[]|BigInt64Array} attentionMask - encoder mask [batch * encSeq]
   * @param {object} [options]
   * @param {number} [options.maxTokens=128]
   * @param {number} [options.decoderStartTokenId=0] - T5 uses pad_token_id=0 as decoder start
   * @param {number} [options.eosTokenId=1] - T5 eos_token_id
   * @param {number} [options.temperature=0]
   * @param {number} [options.topK=0]
   * @param {number} [options.topP=0]
   * @param {function} [options.onToken]
   * @returns {number[]} generated token IDs (excluding decoder start token)
   */
  t5Generate(modelHandle, vocabSize, inputIds, attentionMask, options = {}) {
    const {
      maxTokens = 128,
      decoderStartTokenId = 0,
      eosTokenId = 1,
      temperature = 0,
      topK = 0,
      topP = 0,
      onToken,
    } = options;

    const batchSize = 1;
    const encSeq = inputIds.length;

    // Encode once
    const encoderOutput = this.t5Encode(modelHandle, inputIds, attentionMask, batchSize, encSeq);

    let decoderIds = [decoderStartTokenId];

    for (let step = 0; step < maxTokens; step++) {
      const logits = this.t5Decode(
        modelHandle, encoderOutput, attentionMask,
        decoderIds, batchSize, decoderIds.length, encSeq,
      );

      // Extract last position's logits
      const lastLogits = new Float32Array(vocabSize);
      lastLogits.set(logits.slice(logits.length - vocabSize));

      const nextToken = _sample(lastLogits, temperature, topK, topP);
      if (nextToken === eosTokenId) break;

      decoderIds.push(nextToken);
      if (onToken) onToken(nextToken, step);
    }

    return decoderIds.slice(1);
  }

  // --- Vision-Language (Gemma3 Multimodal) API ---

  /**
   * Preprocess an image for Gemma3 vision tower (browser only).
   * Resizes to imageSize x imageSize, normalizes to [-1, 1] CHW format.
   * @param {ImageBitmap|HTMLImageElement|HTMLCanvasElement} source
   * @param {number} imageSize - e.g. 224 or 384
   * @returns {Float32Array} [3, imageSize, imageSize] CHW
   */
  preprocessImageBrowser(source, imageSize) {
    const canvas = new OffscreenCanvas(imageSize, imageSize);
    const ctx = canvas.getContext('2d');
    ctx.drawImage(source, 0, 0, imageSize, imageSize);
    const imageData = ctx.getImageData(0, 0, imageSize, imageSize);
    const rgba = imageData.data;

    const chw = new Float32Array(3 * imageSize * imageSize);
    const pixels = imageSize * imageSize;
    for (let i = 0; i < pixels; i++) {
      // Normalize to [-1, 1]: (pixel/255 - 0.5) / 0.5
      chw[i] = (rgba[i * 4] / 255 - 0.5) / 0.5;                    // R
      chw[pixels + i] = (rgba[i * 4 + 1] / 255 - 0.5) / 0.5;       // G
      chw[2 * pixels + i] = (rgba[i * 4 + 2] / 255 - 0.5) / 0.5;   // B
    }
    return chw;
  }

  /**
   * Encode images through the Gemma3 vision tower.
   * @param {number} modelHandle - GPT model with vision weights
   * @param {Float32Array} pixelValues - [batch * 3 * imageSize * imageSize]
   * @param {number} batch
   * @returns {Float32Array} [batch * mmTokensPerImage * hiddenSize]
   */
  gptVisionEncode(modelHandle, pixelValues, batch = 1) {
    const abi = this._getAbi();
    const inByteLen = pixelValues.length * 4;
    const inPtr = abi.alloc(inByteLen);
    if (!inPtr) throw new Error('WASM allocation failed for pixel values');
    abi.writeArray(Float32Array, inPtr, pixelValues);

    // Max output: batch * 256 tokens * 3072 hidden = ~3MB per image
    const maxOutputFloats = batch * 256 * 3072;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) {
      abi.free(inPtr, inByteLen);
      throw new Error('WASM allocation failed for output');
    }

    const resultLen = this.wasm.gpt_vision_encode(
      modelHandle, inPtr, abi.size(pixelValues.length), batch, outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(inPtr, inByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('Vision encode failed');
    return output;
  }

  gptProjectorVisionEncode(modelHandle, projectorHandle, pixelValues, batch = 1) {
    if (this._worker) {
      return this._workerCall('gpt-projector-vision-encode', {
        modelHandle,
        projectorHandle,
        pixelValues,
        batch,
      }).then(resp => resp.output);
    }
    const abi = this._getAbi();
    const inByteLen = pixelValues.length * 4;
    const inPtr = abi.alloc(inByteLen);
    if (!inPtr) throw new Error('WASM allocation failed for projector pixel values');
    abi.writeArray(Float32Array, inPtr, pixelValues);

    const maxOutputFloats = batch * 256 * 3072;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) {
      abi.free(inPtr, inByteLen);
      throw new Error('WASM allocation failed for projector output');
    }

    const resultLen = this.wasm.gpt_projector_vision_encode(
      modelHandle, projectorHandle, inPtr, abi.size(pixelValues.length), batch, outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(inPtr, inByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('Projector vision encode failed');
    return output;
  }

  gptProjectorImageEncode(projectorHandle, imageBytes) {
    if (this._worker) {
      const bytes = imageBytes instanceof Uint8Array ? imageBytes : new Uint8Array(imageBytes);
      return this._workerCall('gpt-projector-image-encode', {
        projectorHandle,
        imageBytes: bytes,
      }, [bytes.buffer]).then(resp => ({
        embeddings: resp.embeddings,
        tokensPerImage: resp.tokensPerImage,
      }));
    }
    const abi = this._getAbi();
    const bytes = imageBytes instanceof Uint8Array ? imageBytes : new Uint8Array(imageBytes);
    const inPtr = abi.alloc(bytes.length);
    if (!inPtr) throw new Error('WASM allocation failed for image bytes');
    abi.writeArray(Uint8Array, inPtr, bytes);

    const outTokensPtr = abi.alloc(4);
    if (!outTokensPtr) {
      abi.free(inPtr, bytes.length);
      throw new Error('WASM allocation failed for token count');
    }

    const maxOutputFloats = 4096 * 4096;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) {
      abi.free(inPtr, bytes.length);
      abi.free(outTokensPtr, 4);
      throw new Error('WASM allocation failed for projector embeddings');
    }

    const resultLen = this.wasm.gpt_projector_image_encode(
      projectorHandle, inPtr, abi.size(bytes.length), outTokensPtr, outPtr,
    );

    let embeddings;
    let tokensPerImage;
    if (resultLen > 0) {
      embeddings = abi.readCopy(Float32Array, outPtr, resultLen);
      tokensPerImage = [abi.readCopy(Uint32Array, outTokensPtr, 1)[0]];
    }

    abi.free(inPtr, bytes.length);
    abi.free(outTokensPtr, 4);
    abi.free(outPtr, outByteLen);

    if (!embeddings || !tokensPerImage) throw new Error('Projector image encode failed');
    return { embeddings, tokensPerImage };
  }

  gptProjectorAudioEncode(projectorHandle, audioBytes) {
    if (this._worker) {
      const bytes = audioBytes instanceof Uint8Array ? audioBytes : new Uint8Array(audioBytes);
      return this._workerCall('gpt-projector-audio-encode', {
        projectorHandle,
        audioBytes: bytes,
      }, [bytes.buffer]).then(resp => ({
        embeddings: resp.embeddings,
        tokensPerAudio: resp.tokensPerAudio,
      }));
    }
    const abi = this._getAbi();
    const bytes = audioBytes instanceof Uint8Array ? audioBytes : new Uint8Array(audioBytes);
    const inPtr = abi.alloc(bytes.length);
    if (!inPtr) throw new Error('WASM allocation failed for audio bytes');
    abi.writeArray(Uint8Array, inPtr, bytes);

    const outTokensPtr = abi.alloc(4);
    if (!outTokensPtr) {
      abi.free(inPtr, bytes.length);
      throw new Error('WASM allocation failed for audio token count');
    }

    const maxOutputFloats = 4096 * 4096;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);
    if (!outPtr) {
      abi.free(inPtr, bytes.length);
      abi.free(outTokensPtr, 4);
      throw new Error('WASM allocation failed for audio projector embeddings');
    }

    const resultLen = this.wasm.gpt_projector_audio_encode(
      projectorHandle, inPtr, abi.size(bytes.length), outTokensPtr, outPtr,
    );

    let embeddings;
    let tokensPerAudio;
    if (resultLen > 0) {
      embeddings = abi.readCopy(Float32Array, outPtr, resultLen);
      tokensPerAudio = [abi.readCopy(Uint32Array, outTokensPtr, 1)[0]];
    }

    abi.free(inPtr, bytes.length);
    abi.free(outTokensPtr, 4);
    abi.free(outPtr, outByteLen);

    if (!embeddings || !tokensPerAudio) throw new Error('Projector audio encode failed');
    return { embeddings, tokensPerAudio };
  }

  /**
   * Multimodal forward: embed tokens + inject image embeddings + run GPT.
   * @param {number} modelHandle
   * @param {number[]|BigInt64Array} expandedIds - token IDs with image placeholders
   * @param {Float32Array} imageEmbeddings - from gptVisionEncode
   * @param {number[]} imageOffsets - positions where image tokens start
   * @param {number} batchSize
   * @param {number} seqLen
   * @returns {Float32Array} logits
   */
  gptForwardMultimodal(modelHandle, expandedIds, imageEmbeddings, imageOffsets, batchSize, seqLen) {
    const abi = this._getAbi();
    const ids = expandedIds instanceof BigInt64Array
      ? expandedIds
      : new BigInt64Array(expandedIds.map(BigInt));
    const offsets = new Uint32Array(imageOffsets);

    const idsByteLen = ids.length * 8;
    const embByteLen = imageEmbeddings.length * 4;
    const offByteLen = offsets.length * 4;

    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);

    const embPtr = abi.alloc(embByteLen);
    abi.writeArray(Float32Array, embPtr, imageEmbeddings);

    const offPtr = abi.alloc(offByteLen);
    abi.writeArray(Uint32Array, offPtr, offsets);

    const maxOutputFloats = batchSize * seqLen * 262144;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);

    const resultLen = this.wasm.gpt_forward_multimodal(
      modelHandle, idsPtr, abi.size(ids.length),
      embPtr, abi.size(imageEmbeddings.length),
      offPtr, abi.size(offsets.length),
      batchSize, seqLen, outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(embPtr, embByteLen);
    abi.free(offPtr, offByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('Multimodal forward failed');
    return output;
  }

  /**
   * Multimodal forward with KV cache (prefill with images).
   * For decode steps, use gptForwardCached (no images needed).
   */
  gptForwardCachedMultimodal(modelHandle, cacheHandle, expandedIds, imageEmbeddings, imageOffsets, batchSize, seqLen) {
    const abi = this._getAbi();
    const ids = expandedIds instanceof BigInt64Array
      ? expandedIds
      : new BigInt64Array(expandedIds.map(BigInt));
    const offsets = new Uint32Array(imageOffsets);

    const idsByteLen = ids.length * 8;
    const embByteLen = imageEmbeddings.length * 4;
    const offByteLen = offsets.length * 4;

    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);

    const embPtr = abi.alloc(embByteLen);
    abi.writeArray(Float32Array, embPtr, imageEmbeddings);

    const offPtr = abi.alloc(offByteLen);
    abi.writeArray(Uint32Array, offPtr, offsets);

    const maxOutputFloats = batchSize * seqLen * 262144;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);

    const resultLen = this.wasm.gpt_forward_cached_multimodal(
      modelHandle, cacheHandle, idsPtr, abi.size(ids.length),
      embPtr, abi.size(imageEmbeddings.length),
      offPtr, abi.size(offsets.length),
      batchSize, seqLen, outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(embPtr, embByteLen);
    abi.free(offPtr, offByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('Cached multimodal forward failed');
    return output;
  }

  gptForwardMultimodalGemma4(modelHandle, tokenizerHandle, expandedIds, imageEmbeddings, tokensPerImage, audioEmbeddings = new Float32Array(0), tokensPerAudio = [], batchSize, seqLen) {
    if (this._worker) {
      return this._workerCall('gpt-forward-multimodal-gemma4', {
        modelHandle,
        tokenizerHandle,
        expandedIds: Array.from(expandedIds instanceof BigInt64Array ? expandedIds.map(Number) : expandedIds),
        imageEmbeddings,
        tokensPerImage,
        audioEmbeddings,
        tokensPerAudio,
        batchSize,
        seqLen,
      }).then(resp => resp.output);
    }
    const abi = this._getAbi();
    const ids = expandedIds instanceof BigInt64Array
      ? expandedIds
      : new BigInt64Array(expandedIds.map(BigInt));
    const counts = new Uint32Array(tokensPerImage);
    const audioCounts = new Uint32Array(tokensPerAudio);

    const idsByteLen = ids.length * 8;
    const embByteLen = imageEmbeddings.length * 4;
    const countsByteLen = counts.length * 4;
    const audioEmbByteLen = audioEmbeddings.length * 4;
    const audioCountsByteLen = audioCounts.length * 4;

    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);
    const embPtr = abi.alloc(embByteLen);
    abi.writeArray(Float32Array, embPtr, imageEmbeddings);
    const countsPtr = abi.alloc(countsByteLen);
    abi.writeArray(Uint32Array, countsPtr, counts);
    const audioEmbPtr = abi.alloc(audioEmbByteLen);
    abi.writeArray(Float32Array, audioEmbPtr, audioEmbeddings);
    const audioCountsPtr = abi.alloc(audioCountsByteLen);
    abi.writeArray(Uint32Array, audioCountsPtr, audioCounts);

    const maxOutputFloats = batchSize * seqLen * 262144;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);

    const resultLen = this.wasm.gpt_forward_multimodal_gemma4(
      modelHandle, tokenizerHandle, idsPtr, abi.size(ids.length),
      embPtr, abi.size(imageEmbeddings.length),
      countsPtr, abi.size(counts.length),
      audioEmbPtr, abi.size(audioEmbeddings.length),
      audioCountsPtr, abi.size(audioCounts.length),
      batchSize, seqLen, outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(embPtr, embByteLen);
    abi.free(countsPtr, countsByteLen);
    abi.free(audioEmbPtr, audioEmbByteLen);
    abi.free(audioCountsPtr, audioCountsByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('Gemma4 multimodal forward failed');
    return output;
  }

  gptForwardCachedMultimodalGemma4(modelHandle, tokenizerHandle, cacheHandle, expandedIds, imageEmbeddings, tokensPerImage, audioEmbeddings = new Float32Array(0), tokensPerAudio = [], batchSize, seqLen) {
    if (this._worker) {
      return this._workerCall('gpt-forward-cached-multimodal-gemma4', {
        modelHandle,
        tokenizerHandle,
        cacheHandle,
        expandedIds: Array.from(expandedIds instanceof BigInt64Array ? expandedIds.map(Number) : expandedIds),
        imageEmbeddings,
        tokensPerImage,
        audioEmbeddings,
        tokensPerAudio,
        batchSize,
        seqLen,
      }).then(resp => resp.output);
    }
    const abi = this._getAbi();
    const ids = expandedIds instanceof BigInt64Array
      ? expandedIds
      : new BigInt64Array(expandedIds.map(BigInt));
    const counts = new Uint32Array(tokensPerImage);
    const audioCounts = new Uint32Array(tokensPerAudio);

    const idsByteLen = ids.length * 8;
    const embByteLen = imageEmbeddings.length * 4;
    const countsByteLen = counts.length * 4;
    const audioEmbByteLen = audioEmbeddings.length * 4;
    const audioCountsByteLen = audioCounts.length * 4;

    const idsPtr = abi.alloc(idsByteLen);
    abi.writeArray(BigInt64Array, idsPtr, ids);
    const embPtr = abi.alloc(embByteLen);
    abi.writeArray(Float32Array, embPtr, imageEmbeddings);
    const countsPtr = abi.alloc(countsByteLen);
    abi.writeArray(Uint32Array, countsPtr, counts);
    const audioEmbPtr = abi.alloc(audioEmbByteLen);
    abi.writeArray(Float32Array, audioEmbPtr, audioEmbeddings);
    const audioCountsPtr = abi.alloc(audioCountsByteLen);
    abi.writeArray(Uint32Array, audioCountsPtr, audioCounts);

    const maxOutputFloats = batchSize * seqLen * 262144;
    const outByteLen = maxOutputFloats * 4;
    const outPtr = abi.alloc(outByteLen);

    const resultLen = this.wasm.gpt_forward_cached_multimodal_gemma4(
      modelHandle, tokenizerHandle, cacheHandle, idsPtr, abi.size(ids.length),
      embPtr, abi.size(imageEmbeddings.length),
      countsPtr, abi.size(counts.length),
      audioEmbPtr, abi.size(audioEmbeddings.length),
      audioCountsPtr, abi.size(audioCounts.length),
      batchSize, seqLen, outPtr,
    );

    let output;
    if (resultLen > 0) {
      output = abi.readCopy(Float32Array, outPtr, resultLen);
    }

    abi.free(idsPtr, idsByteLen);
    abi.free(embPtr, embByteLen);
    abi.free(countsPtr, countsByteLen);
    abi.free(audioEmbPtr, audioEmbByteLen);
    abi.free(audioCountsPtr, audioCountsByteLen);
    abi.free(outPtr, outByteLen);

    if (!output) throw new Error('Gemma4 cached multimodal forward failed');
    return output;
  }

  /**
   * Multimodal generation with KV cache: encode image, prefill with images, decode autoregressively.
   * @param {number} modelHandle
   * @param {number} vocabSize
   * @param {number[]} promptIds - expanded token IDs (with image placeholders)
   * @param {Float32Array} imageEmbeddings - from gptVisionEncode
   * @param {number[]} imageOffsets - positions where image tokens start
   * @param {object} [options]
   * @returns {Promise<number[]>} generated tokens
   */
  async gptGenerateMultimodal(modelHandle, vocabSize, promptIds, imageEmbeddings, imageOffsets, options = {}) {
    const {
      maxTokens = 128,
      maxLen = 2048,
      temperature = 0,
      topK = 0,
      topP = 0,
      eosTokenId,
      repetitionPenalty = 1.0,
      cacheDtype = 'f32',
      onToken,
    } = options;

    const cache = this.gptCreateKvCache(modelHandle, maxLen, { cacheDtype });
    try {
      // Prefill with images
      const prefillLogits = this.gptForwardCachedMultimodal(
        modelHandle, cache, promptIds, imageEmbeddings, imageOffsets, 1, promptIds.length,
      );

      const tokens = [...promptIds];
      let lastLogits = new Float32Array(vocabSize);
      lastLogits.set(prefillLogits.slice(prefillLogits.length - vocabSize));

      for (let step = 0; step < maxTokens; step++) {
        if (repetitionPenalty !== 1.0) {
          const seen = new Set(tokens);
          for (const id of seen) {
            if (id < vocabSize) {
              if (lastLogits[id] > 0) lastLogits[id] /= repetitionPenalty;
              else lastLogits[id] *= repetitionPenalty;
            }
          }
        }

        const nextToken = _sample(lastLogits, temperature, topK, topP);
        if (eosTokenId !== undefined && nextToken === eosTokenId) break;
        tokens.push(nextToken);
        if (onToken) onToken(nextToken, step);

        // Decode: standard forward (no images)
        const decLogits = this.gptForwardCached(modelHandle, cache, [nextToken], 1, 1);
        lastLogits = new Float32Array(vocabSize);
        lastLogits.set(decLogits.slice(decLogits.length - vocabSize));
      }

      return tokens.slice(promptIds.length);
    } finally {
      this.gptFreeKvCache(cache);
    }
  }

  async gptGenerateMultimodalGemma4(modelHandle, tokenizerHandle, vocabSize, promptIds, imageEmbeddings, tokensPerImage, audioEmbeddings = new Float32Array(0), tokensPerAudio = [], options = {}) {
    const {
      maxTokens = 128,
      maxLen = 2048,
      temperature = 0,
      topK = 0,
      topP = 0,
      eosTokenId,
      repetitionPenalty = 1.0,
      cacheDtype = 'f32',
      onToken,
    } = options;

    const cache = this.gptCreateKvCache(modelHandle, maxLen, { cacheDtype });
    try {
      const prefillLogits = this.gptForwardCachedMultimodalGemma4(
        modelHandle, tokenizerHandle, cache, promptIds, imageEmbeddings, tokensPerImage, audioEmbeddings, tokensPerAudio, 1, promptIds.length,
      );

      const tokens = [...promptIds];
      let lastLogits = new Float32Array(vocabSize);
      lastLogits.set(prefillLogits.slice(prefillLogits.length - vocabSize));

      for (let step = 0; step < maxTokens; step++) {
        if (repetitionPenalty !== 1.0) {
          const seen = new Set(tokens);
          for (const id of seen) {
            if (id < vocabSize) {
              if (lastLogits[id] > 0) lastLogits[id] /= repetitionPenalty;
              else lastLogits[id] *= repetitionPenalty;
            }
          }
        }

        const nextToken = _sample(lastLogits, temperature, topK, topP);
        if (eosTokenId !== undefined && nextToken === eosTokenId) break;
        tokens.push(nextToken);
        if (onToken) onToken(nextToken, step);

        const decLogits = this.gptForwardCached(modelHandle, cache, [nextToken], 1, 1);
        lastLogits = new Float32Array(vocabSize);
        lastLogits.set(decLogits.slice(decLogits.length - vocabSize));
      }

      return tokens.slice(promptIds.length);
    } finally {
      this.gptFreeKvCache(cache);
    }
  }

  // --- ONNX Runtime Web API (optional, lazy) ---

  /**
   * Initialize ONNX Runtime Web. Requires onnxruntime-web to be available.
   * Call this before using any ONNX methods.
   *
   * @param {object} ort - The onnxruntime-web module (import * as ort from 'onnxruntime-web')
   * @param {object} [options]
   * @param {string} [options.wasmPaths] - Path prefix for ORT WASM files (e.g. '/wasm/')
   * @param {string[]} [options.executionProviders] - Providers to use (default: ['wasm'])
   * @returns {Promise<void>}
   */
  async initOnnx(ort, options = {}) {
    if (!ort || !ort.InferenceSession) {
      throw new Error('onnxruntime-web module required: import * as ort from "onnxruntime-web"');
    }
    this._ort = ort;
    this._onnxSessions = new Map();
    this._onnxNextId = 1;
    this._onnxDefaultProviders = options.executionProviders ?? ['wasm'];

    if (options.wasmPaths) {
      ort.env.wasm.wasmPaths = options.wasmPaths;
    }

    if (this._worker) {
      await this._workerCall('onnx-init', {
        wasmPaths: options.wasmPaths,
        executionProviders: this._onnxDefaultProviders,
      });
    }
  }

  /**
   * Load an ONNX model.
   * @param {string|URL|ArrayBuffer|Uint8Array} modelSource - URL or bytes of .onnx file
   * @param {object} [options]
   * @param {string[]} [options.executionProviders] - Override default providers
   * @returns {Promise<number>} ONNX session handle
   */
  async loadOnnxModel(modelSource, options = {}) {
    if (!this._ort) throw new Error('Call initOnnx() first');

    const providers = options.executionProviders ?? this._onnxDefaultProviders;

    if (this._worker) {
      let modelBytes;
      if (modelSource instanceof ArrayBuffer) {
        modelBytes = new Uint8Array(modelSource);
      } else if (modelSource instanceof Uint8Array) {
        modelBytes = modelSource;
      } else {
        const resp = await fetch(modelSource);
        modelBytes = new Uint8Array(await resp.arrayBuffer());
      }
      const resp = await this._workerCall('load-onnx-model', {
        modelBytes,
        executionProviders: providers,
      });
      return resp.handle;
    }

    // Direct mode
    let session;
    if (modelSource instanceof ArrayBuffer || modelSource instanceof Uint8Array) {
      session = await this._ort.InferenceSession.create(modelSource, {
        executionProviders: providers,
      });
    } else {
      const resp = await fetch(modelSource);
      const bytes = await resp.arrayBuffer();
      session = await this._ort.InferenceSession.create(bytes, {
        executionProviders: providers,
      });
    }

    const handle = this._onnxNextId++;
    this._onnxSessions.set(handle, session);
    return handle;
  }

  /**
   * Run inference on an ONNX model.
   * @param {number} sessionHandle - from loadOnnxModel
   * @param {object} feeds - Map of input name → ORT Tensor. In direct mode, pass
   *   ort.Tensor instances directly. In worker mode, pass plain objects:
   *   { name: { data: Float32Array|Int32Array|BigInt64Array, dims: number[], type: string } }
   * @returns {Promise<object>} Map of output name → { data: TypedArray, dims: number[] }
   */
  async onnxInfer(sessionHandle, feeds) {
    if (!this._ort) throw new Error('Call initOnnx() first');

    if (this._worker) {
      // Serialize feeds for transfer
      const serialized = {};
      for (const [name, tensor] of Object.entries(feeds)) {
        if (tensor.data && tensor.dims && tensor.type) {
          serialized[name] = { data: tensor.data, dims: tensor.dims, type: tensor.type };
        } else {
          // Assume it's an ORT Tensor
          serialized[name] = { data: tensor.data, dims: tensor.dims, type: tensor.type };
        }
      }
      const resp = await this._workerCall('onnx-infer', {
        sessionHandle,
        feeds: serialized,
      });
      return resp.outputs;
    }

    // Direct mode
    const session = this._onnxSessions.get(sessionHandle);
    if (!session) throw new Error(`ONNX session ${sessionHandle} not found`);

    // Convert plain objects to ORT Tensors if needed
    const ortFeeds = {};
    for (const [name, tensor] of Object.entries(feeds)) {
      if (tensor instanceof this._ort.Tensor) {
        ortFeeds[name] = tensor;
      } else {
        ortFeeds[name] = new this._ort.Tensor(tensor.type, tensor.data, tensor.dims);
      }
    }

    const results = await session.run(ortFeeds);

    // Convert ORT Tensors to plain objects for consistent API
    const outputs = {};
    for (const [name, tensor] of Object.entries(results)) {
      outputs[name] = { data: tensor.data, dims: tensor.dims, type: tensor.type };
    }
    return outputs;
  }

  /**
   * Unload an ONNX model session.
   * @param {number} handle
   */
  async unloadOnnxModel(handle) {
    if (this._worker) {
      return this._workerCall('unload-onnx-model', { handle });
    }
    const session = this._onnxSessions.get(handle);
    if (session) {
      await session.release();
      this._onnxSessions.delete(handle);
    }
  }
}

function _interleaveAudioBuffer(audioBuffer) {
  const { numberOfChannels, length } = audioBuffer;
  const interleaved = new Float32Array(length * numberOfChannels);
  const channelData = [];
  for (let ch = 0; ch < numberOfChannels; ch++) {
    channelData.push(audioBuffer.getChannelData(ch));
  }
  for (let i = 0; i < length; i++) {
    for (let ch = 0; ch < numberOfChannels; ch++) {
      interleaved[i * numberOfChannels + ch] = channelData[ch][i];
    }
  }
  return interleaved;
}

/**
 * Sample a token from logits using temperature, top-k, top-p.
 * @param {Float32Array} logits - [vocab_size]
 * @param {number} temperature - 0 = greedy
 * @param {number} topK - 0 = disabled
 * @param {number} topP - 0 = disabled
 * @returns {number} sampled token ID
 */
function _sample(logits, temperature, topK, topP) {
  // Greedy (temperature = 0)
  if (temperature === 0) {
    let maxIdx = 0;
    for (let i = 1; i < logits.length; i++) {
      if (logits[i] > logits[maxIdx]) maxIdx = i;
    }
    return maxIdx;
  }

  // Apply temperature
  const scaled = new Float32Array(logits.length);
  for (let i = 0; i < logits.length; i++) {
    scaled[i] = logits[i] / temperature;
  }

  // Build (index, logit) pairs and sort descending
  let candidates = Array.from(scaled, (v, i) => ({ id: i, logit: v }));
  candidates.sort((a, b) => b.logit - a.logit);

  // Top-K filter
  if (topK > 0 && topK < candidates.length) {
    candidates = candidates.slice(0, topK);
  }

  // Softmax
  const maxLogit = candidates[0].logit;
  let sumExp = 0;
  for (const c of candidates) {
    c.prob = Math.exp(c.logit - maxLogit);
    sumExp += c.prob;
  }
  for (const c of candidates) {
    c.prob /= sumExp;
  }

  // Top-P (nucleus) filter
  if (topP > 0 && topP < 1) {
    let cumProb = 0;
    let cutoff = candidates.length;
    for (let i = 0; i < candidates.length; i++) {
      cumProb += candidates[i].prob;
      if (cumProb >= topP) {
        cutoff = i + 1;
        break;
      }
    }
    candidates = candidates.slice(0, cutoff);
    // Renormalize
    const sum2 = candidates.reduce((s, c) => s + c.prob, 0);
    for (const c of candidates) c.prob /= sum2;
  }

  // Weighted random sample
  const r = Math.random();
  let cum = 0;
  for (const c of candidates) {
    cum += c.prob;
    if (r < cum) return c.id;
  }
  return candidates[candidates.length - 1].id;
}

function _projectorKindFromRaw(rawKind) {
  switch (rawKind) {
    case 1: return 'antfly_gemma3';
    case 2: return 'clip_gemma4_image';
    case 3: return 'clip_gemma4_audio';
    case 4: return 'clip_gemma4_image_audio';
    default: return 'unknown';
  }
}

/** No-op stubs for when GPU is not available. */
function _gpuStubs() {
  return {
    gpu_is_available: () => 0,
    gpu_create_buffer: () => 0,
    gpu_free_buffer: () => {},
    gpu_upload: () => {},
    gpu_download: () => {},
    gpu_copy_buffer_to_buffer: () => {},
    gpu_matmul: () => {},
    gpu_matmul_transb: () => {},
    gpu_add: () => {},
    gpu_add_broadcast: () => {},
    gpu_mul: () => {},
    gpu_sub: () => {},
    gpu_div: () => {},
    gpu_less_than: () => {},
    gpu_where_select: () => {},
    gpu_neg: () => {},
    gpu_sqrt: () => {},
    gpu_rsqrt: () => {},
    gpu_exp: () => {},
    gpu_log: () => {},
    gpu_sin: () => {},
    gpu_cos: () => {},
    gpu_tanh: () => {},
    gpu_abs: () => {},
    gpu_erf: () => {},
    gpu_gelu: () => {},
    gpu_softmax: () => {},
    gpu_log_softmax: () => {},
    gpu_reduce_sum_last_dim: () => {},
    gpu_reduce_max_last_dim: () => {},
    gpu_reduce_mean_last_dim: () => {},
    gpu_reduce_sum: () => {},
    gpu_reduce_max: () => {},
    gpu_reduce_mean: () => {},
    gpu_broadcast_in_dim: () => {},
    gpu_attention: () => {},
    gpu_causal_attention: () => {},
    gpu_gqa_causal_attention: () => {},
    gpu_gqa_cached_attention: () => {},
    gpu_gqa_cached_attention_ex: () => {},
    gpu_write_buffer_at_offset: () => {},
    gpu_cross_attention: () => {},
    gpu_matmul_transb_q4_0: () => {},
    gpu_matmul_transb_q4_1: () => {},
    gpu_matmul_transb_q5_0: () => {},
    gpu_matmul_transb_q5_1: () => {},
    gpu_matmul_transb_q8_0: () => {},
    gpu_matmul_transb_q8_1: () => {},
    gpu_matmul_transb_iq4_nl: () => {},
    gpu_matmul_transb_iq4_xs: () => {},
    gpu_matmul_transb_q2_k: () => {},
    gpu_matmul_transb_q3_k: () => {},
    gpu_matmul_transb_q4_k: () => {},
    gpu_matmul_transb_q5_k: () => {},
    gpu_matmul_transb_q6_k: () => {},
    gpu_matmul_transb_q8_k: () => {},
    gpu_matmul_transb_i2_s: () => {},
    gpu_matmul_transb_i8_s: () => {},
    gpu_matmul_transb_q1_0: () => {},
    gpu_matmul_transb_tq1_0: () => {},
    gpu_matmul_transb_tq2_0: () => {},
    gpu_matmul_transb_mxfp4: () => {},
    gpu_matmul_transb_nvfp4: () => {},
    gpu_matmul_transb_iq1_s: () => {},
    gpu_matmul_transb_iq1_m: () => {},
    gpu_matmul_transb_iq2_xxs: () => {},
    gpu_matmul_transb_iq2_xs: () => {},
    gpu_matmul_transb_iq2_s: () => {},
    gpu_matmul_transb_iq3_xxs: () => {},
    gpu_matmul_transb_iq3_s: () => {},
    gpu_matmul_transb_q4_0_mmv: () => {},
    gpu_matmul_transb_q4_1_mmv: () => {},
    gpu_matmul_transb_q5_0_mmv: () => {},
    gpu_matmul_transb_q5_1_mmv: () => {},
    gpu_matmul_transb_q8_0_mmv: () => {},
    gpu_matmul_transb_q8_1_mmv: () => {},
    gpu_matmul_transb_iq4_nl_mmv: () => {},
    gpu_matmul_transb_iq4_xs_mmv: () => {},
    gpu_matmul_transb_q2_k_mmv: () => {},
    gpu_matmul_transb_q3_k_mmv: () => {},
    gpu_matmul_transb_q4_k_mmv: () => {},
    gpu_matmul_transb_q5_k_mmv: () => {},
    gpu_matmul_transb_q6_k_mmv: () => {},
    gpu_matmul_transb_q8_k_mmv: () => {},
    gpu_matmul_transb_i8_s_mmv: () => {},
    gpu_matmul_transb_q1_0_mmv: () => {},
    gpu_matmul_transb_tq1_0_mmv: () => {},
    gpu_matmul_transb_tq2_0_mmv: () => {},
    gpu_matmul_transb_mxfp4_mmv: () => {},
    gpu_matmul_transb_nvfp4_mmv: () => {},
    gpu_matmul_transb_iq1_s_mmv: () => {},
    gpu_matmul_transb_iq1_m_mmv: () => {},
    gpu_matmul_transb_iq2_xxs_mmv: () => {},
    gpu_matmul_transb_iq2_xs_mmv: () => {},
    gpu_matmul_transb_iq2_s_mmv: () => {},
    gpu_matmul_transb_iq3_xxs_mmv: () => {},
    gpu_matmul_transb_iq3_s_mmv: () => {},
    gpu_rms_norm: () => {},
    gpu_layer_norm: () => {},
  };
}
