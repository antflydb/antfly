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
    if (this.wasm.extraction_abi_version?.() !== 1) throw new Error('Extraction WASM ABI mismatch; rebuild browser assets');
    const bundle = await inspectBundle(files, precision);
    const read = path => bundle.files.get(path).text();
    const metadata = { config: bundle.architecture === 'laya' ? JSON.stringify(bundle.config) : await read('config.json'), encoder_config: bundle.architecture === 'laya' ? JSON.stringify(bundle.encoderConfig) : await read('encoder_config/config.json'), tokenizer_config: await read('tokenizer_config.json'), precision: bundle.precision };
    const directories = [];
    for (const path of bundle.weights) directories.push(await tensorDirectory(bundle.files.get(path), path.endsWith('.gguf') ? 'gguf' : 'safetensors'));
    if (bundle.architecture === 'laya' && directories[0].some(t => ![0, 1].includes(t.kind) || bundle.precision === 'fp32' && t.kind !== 0)) throw new Error('Laya tensor precision does not match the selected dense precision');
    try {
      this.handle = this.check(this.text(JSON.stringify(metadata), (ptr, len) => this.wasm.extraction_create(ptr, len)));
      const tokenizer = new Uint8Array(await bundle.files.get('tokenizer.json').arrayBuffer());
      this.check(this.bytes(tokenizer, (ptr, len) => this.wasm.extraction_tokenizer(this.handle, ptr, len)));
      // Integrity receipts are checked for every consumed file, not merely
      // trusted because the directory happens to have the expected name.
      const pins = new Map((bundle.receipt?.files ?? []).map(pin => [pin.path, pin]));
      for (const path of ['config.json', 'encoder_config/config.json', 'tokenizer.json', 'tokenizer_config.json']) {
        if (!pins.has(path)) continue;
        const file = bundle.files.get(path), pin = pins.get(path);
        if (pin.size_bytes !== file.size || await this.hash(file) !== pin.sha256) throw new Error(`Integrity check failed: ${path}`);
      }
      let weightHash, weightBytes = 0;
      for (let index = 0; index < bundle.weights.length; index++) {
        const path = bundle.weights[index], file = bundle.files.get(path);
        weightHash = await this.hash(file, loaded => progress({ stage: 'verify', file: path, loaded, total: file.size }));
        const pin = pins.get(path);
        if (pin && (pin.size_bytes !== file.size || pin.sha256 !== weightHash)) throw new Error(`Integrity check failed: ${path}`);
        weightBytes += file.size;
        for (const [i, t] of directories[index].entries()) {
          const bytes = new Uint8Array(await file.slice(t.start, t.end).arrayBuffer());
          // Split encoder GGUF uses unprefixed names; boundary GGUF retains
          // the canonical encoder namespace. Never strip boundary names.
          const name = bundle.architecture === 'span' && bundle.weights.length === 2 && index === 0 ? `encoder.${t.name}` : t.name;
          this.check(this.text(JSON.stringify({ name, shape: t.shape, kind: t.kind }), (meta, metaLen) => this.bytes(bytes, (ptr, len) => this.wasm.extraction_weight(this.handle, meta, metaLen, ptr, len))));
          progress({ stage: 'weights', file: path, loaded: i + 1, total: directories[index].length });
        }
      }
      this.check(this.text(weightHash, ptr => this.wasm.extraction_finalize(this.handle, ptr, BigInt(weightBytes))));
      return { architecture: bundle.architecture, precision: bundle.precision, bytes: bundle.bytes, backend: 'requested', qualified: false };
    } catch (error) { this.unload(); throw error; }
  }
  run(request, validateOnly = false) {
    const start = performance.now();
    const bytes = encoder.encode(JSON.stringify(request));
    if (bytes.length > 512 * 1024) throw new Error('Extraction request exceeds 512 KiB');
    try {
      this.check(this.bytes(bytes, (ptr, len) => this.wasm.extraction_run(this.handle, ptr, len, Number(validateOnly))));
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
