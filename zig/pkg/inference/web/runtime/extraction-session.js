// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import { inspectBundle, tensorDirectory } from './extraction-bundle.js';
const encoder = new TextEncoder(), decoder = new TextDecoder();
export class ExtractionSession {
  constructor(wasm, abi) { this.wasm = wasm; this.abi = abi; this.handle = 0; }
  bytes(bytes, callback) {
    const ptr = this.abi.alloc(bytes.length);
    if (!ptr && bytes.length) throw new Error('WASM memory budget exceeded');
    try { this.abi.copyBytesIn(ptr, bytes); return callback(ptr, this.abi.size(bytes.length)); }
    finally { this.abi.free(ptr, bytes.length); }
  }
  text(text, callback) { return this.bytes(encoder.encode(text), callback); }
  check(value) {
    if (!value) throw new Error(decoder.decode(this.abi.view(Uint8Array, this.wasm.extraction_error_ptr(), Number(this.wasm.extraction_error_len()))));
    return value;
  }
  async hash(file, onProgress) {
    this.wasm.extraction_hash_begin();
    for (let at = 0; at < file.size; at += 8 * 1024 ** 2) {
      const bytes = new Uint8Array(await file.slice(at, at + 8 * 1024 ** 2).arrayBuffer());
      this.bytes(bytes, (ptr, len) => this.wasm.extraction_hash_update(ptr, len));
      onProgress?.(Math.min(file.size, at + bytes.length));
    }
    return decoder.decode(this.abi.view(Uint8Array, this.wasm.extraction_hash_end(), 64));
  }
  async load(files, precision, progress = () => {}) {
    if (this.handle) throw new Error('Unload the current model first');
    if (this.wasm.extraction_abi_version?.() !== 2) throw new Error('Extraction WASM ABI mismatch; rebuild browser assets');
    const bundle = await inspectBundle(files, precision);
    const read = path => bundle.files.get(path).text();
    const metadata = { config: bundle.architecture === 'laya' ? JSON.stringify(bundle.config) : await read('config.json'), encoder_config: ['laya', 'embedding_similarity'].includes(bundle.architecture) ? JSON.stringify(bundle.encoderConfig) : await read('encoder_config/config.json'), tokenizer_config: await read('tokenizer_config.json'), precision: bundle.precision, tasks: bundle.execution.tasks, capabilities: bundle.capabilities };
    if (bundle.architecture === 'embedding_similarity') {
      metadata.processor_config = await read('processor_config.json');
      metadata.sentence_config = await read('config_sentence_transformers.json');
      metadata.pooling_config = await read('1_Pooling/config.json');
      const calibrations = {};
      for (const path of bundle.files.keys()) if (/^calibrations\/[a-zA-Z0-9_-]+\.json$/.test(path)) {
        if (bundle.files.get(path).size > 256 * 1024) throw new Error('Calibration size limit exceeded');
        calibrations[path.slice(13, -5)] = JSON.parse(await read(path));
      }
      metadata.calibrations = JSON.stringify(calibrations);
      if (metadata.calibrations.length > 1024 * 1024) throw new Error('Calibration bundle size limit exceeded');
    }
    const directories = [];
    for (const path of bundle.weights) directories.push(await tensorDirectory(bundle.files.get(path), path.endsWith('.gguf') ? 'gguf' : 'safetensors'));
    if (bundle.architecture === 'laya' && directories[0].some(t => ![0, 1, 30].includes(t.kind) || bundle.precision === 'fp32' && t.kind !== 0 || bundle.precision === 'fp16' && t.kind === 30)) throw new Error('Laya tensor precision does not match the selected dense precision');
    // Verify all receipt pins before creating a model, including capability
    // manifests, embedding sidecars, and calibration policies.
    const pins = new Map();
    const verifiedHashes = new Map();
    for (const pin of bundle.receipt?.files ?? []) {
      if (pins.has(pin.path)) throw new Error(`Duplicate integrity pin: ${pin.path}`);
      pins.set(pin.path, pin);
      const file = bundle.files.get(pin.path);
      if (!file || pin.size_bytes !== file.size) throw new Error(`Integrity check failed: ${pin.path}`);
      const hash = await this.hash(file, loaded => progress({ stage: 'verify', file: pin.path, loaded, total: file.size }));
      if (hash !== pin.sha256) throw new Error(`Integrity check failed: ${pin.path}`);
      verifiedHashes.set(pin.path, hash);
    }
    try {
      this.handle = this.check(this.text(JSON.stringify(metadata), (ptr, len) => this.wasm.extraction_create(ptr, len)));
      const tokenizer = new Uint8Array(await bundle.files.get('tokenizer.json').arrayBuffer());
      this.check(this.bytes(tokenizer, (ptr, len) => this.wasm.extraction_tokenizer(this.handle, ptr, len)));
      let weightHash, weightBytes = 0;
      for (let index = 0; index < bundle.weights.length; index++) {
        const path = bundle.weights[index], file = bundle.files.get(path);
        weightHash = verifiedHashes.get(path) ?? await this.hash(file, loaded => progress({ stage: 'verify', file: path, loaded, total: file.size }));
        const pin = pins.get(path);
        if (pin && (pin.size_bytes !== file.size || pin.sha256 !== weightHash)) throw new Error(`Integrity check failed: ${path}`);
        weightBytes += file.size;
        for (const [i, t] of directories[index].entries()) {
          // Marker inference does not consume legacy count/span heads; text
          // decisions do not consume media towers. Integrity still covers all bytes.
          if (bundle.architecture === 'decide' && ['modernbert', 'modern_bert'].includes(bundle.encoderConfig.model_type) && /^(count_embed|count_pred|span_rep)\./.test(t.name)) continue;
          if (bundle.architecture === 'embedding_similarity' && /^(vision_tower|audio_tower|embed_vision|embed_audio)\./.test(t.name)) continue;
          const bytes = new Uint8Array(await file.slice(t.start, t.end).arrayBuffer());
          // Split encoder GGUF uses unprefixed names; boundary GGUF retains
          // the canonical encoder namespace. Never strip boundary names.
          const name = ['span', 'decide'].includes(bundle.architecture) && bundle.weights.length === 2 && index === 0 ? `encoder.${t.name}` : t.name;
          this.check(this.text(JSON.stringify({ name, shape: t.shape, kind: t.kind }), (meta, metaLen) => this.bytes(bytes, (ptr, len) => this.wasm.extraction_weight(this.handle, meta, metaLen, ptr, len))));
          progress({ stage: 'weights', file: path, loaded: i + 1, total: directories[index].length });
        }
      }
      this.check(this.text(weightHash, ptr => this.wasm.extraction_finalize(this.handle, ptr, BigInt(weightBytes))));
      return { family: bundle.family, runtimeArchitecture: bundle.runtimeArchitecture, tasks: bundle.tasks, capabilities: bundle.capabilities, execution: bundle.execution, architecture: bundle.architecture, precision: bundle.precision, bytes: bundle.bytes, backend: 'requested', qualified: false };
    } catch (error) { this.unload(); throw error; }
  }
  run(request, validateOnly = false, task) {
    const start = performance.now();
    const bytes = encoder.encode(JSON.stringify(request));
    if (bytes.length > 512 * 1024) throw new Error('Extraction request exceeds 512 KiB');
    try {
      this.check(this.bytes(bytes, (ptr, len) => task ? this.wasm.inference_run(this.handle, ptr, len, task === 'decide' ? 1 : 0, Number(validateOnly)) : this.wasm.extraction_run(this.handle, ptr, len, Number(validateOnly))));
    } catch (error) {
      if (error instanceof WebAssembly.RuntimeError) {
        const detail = decoder.decode(this.abi.view(Uint8Array, this.wasm.extraction_error_ptr(), Number(this.wasm.extraction_error_len())));
        throw new Error(`WASM trapped${detail ? `: ${detail}` : ''}; reload the model`, { cause: error });
      }
      throw error;
    }
    try {
      const value = JSON.parse(decoder.decode(this.abi.view(Uint8Array, this.wasm.extraction_result_ptr(), Number(this.wasm.extraction_result_len()))));
      return { value, elapsedMs: performance.now() - start, wasmBytes: this.wasm.memory.buffer.byteLength };
    } finally { this.wasm.extraction_result_free(); }
  }
  unload() { this.wasm.extraction_unload(); this.handle = 0; }
}
