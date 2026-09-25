// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
// Browser bundle inspection is independent of the inference implementation.
const decoder = new TextDecoder('utf-8', { fatal: true });
export const LIMITS = Object.freeze({ file: 1024 ** 3, bundle: 1536 * 1024 ** 2, header: 16 * 1024 ** 2, tensor: 512 * 1024 ** 2, text: 256 * 1024, schema: 64 * 1024 });
const blockTypes = new Map([[0, [1, 4]], [1, [1, 2]], [2, [32, 18]], [8, [32, 34]], [12, [256, 144]]]);
function integer(n, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(n) || n < 0 || n > max) throw new Error('Invalid or oversized bundle integer');
  return n;
}
function byteLength(shape, kind) {
  const block = blockTypes.get(kind);
  if (!block || !shape.length || shape.length > 4 || shape.some(n => !Number.isSafeInteger(n) || n <= 0)) throw new Error('Unsupported tensor shape or precision');
  const n = shape.reduce((a, b) => integer(a * b), 1);
  if (shape.at(-1) % block[0]) throw new Error('Quantized tensor row is not block-aligned');
  return integer(n / block[0] * block[1], LIMITS.tensor);
}
export function normalizeFiles(files) {
  const pairs = files instanceof Map ? [...files] : Array.from(files, file => [file.webkitRelativePath || file.name, file]);
  // Folder selection includes the selected root directory. Strip it exactly
  // once, preserving encoder_config/config.json and variant subdirectories.
  const root = pairs.length && pairs.every(([p]) => p.includes('/') && p.split('/')[0] === pairs[0][0].split('/')[0]) ? pairs[0][0].split('/')[0] + '/' : '';
  const result = new Map();
  let total = 0;
  for (let [path, file] of pairs) {
    if (path.split('/').includes('..')) throw new Error('Unsafe bundle path');
    if (root) path = path.slice(root.length);
    if (path.startsWith('/') || path.includes('\\') || path.split('/').some(p => p === '..' || !p)) throw new Error('Unsafe bundle path');
    if (result.has(path)) throw new Error(`Duplicate bundle path: ${path}`);
    integer(file.size, LIMITS.file);
    total += file.size;
    // Irrelevant files in a directory are not loaded into WASM.
    result.set(path, file);
  }
  return result;
}
export async function readJson(files, path, max = 1024 * 1024) {
  const file = files.get(path);
  if (!file || file.size > max) throw new Error(`Missing or oversized ${path}`);
  return JSON.parse(await file.text());
}
export async function inspectBundle(input, precision) {
  const files = normalizeFiles(input);
  // Accept both the native importer layout and the pinned upstream folder.
  const upstreamLaya = !files.has('config.json') && files.has('rl_agent_config.json') && files.has('encoder/config.json');
  let config = await readJson(files, upstreamLaya ? 'encoder/config.json' : 'config.json');
  if (upstreamLaya) {
    const laya = await readJson(files, 'rl_agent_config.json');
    for (const name of ['tokenizer.json', 'tokenizer_config.json']) {
      const file = files.get(`tokenizer/${name}`);
      if (!file) throw new Error(`Missing tokenizer/${name}`);
      files.set(name, file);
    }
    const tokenConfig = await readJson(files, 'tokenizer_config.json');
    const mask = tokenConfig.mask_token ?? '[MASK]';
    laya.mask_token = typeof mask === 'string' ? mask : mask.content;
    config = { ...config, laya };
  }
  const isLaya = config.model_type === 'modernbert' && config.laya && typeof config.laya === 'object';
  if (isLaya) {
    for (const key of ['attention_bias', 'mlp_bias', 'norm_bias']) if (config[key]) throw new Error(`Unsupported Laya encoder: ${key}`);
    for (const [kind, key] of [['full_attention', 'global_rope_theta'], ['sliding_attention', 'local_rope_theta']]) {
      const params = config.rope_parameters?.[kind];
      if (params) {
        if (params.rope_type && params.rope_type !== 'default') throw new Error('Unsupported Laya RoPE scaling');
        config[key] = params.rope_theta;
      }
    }
  }
  const encoderConfig = isLaya ? config : await readJson(files, 'encoder_config/config.json');
  await readJson(files, 'tokenizer_config.json');
  if (!files.has('tokenizer.json') || files.get('tokenizer.json').size > 32 * 1024 ** 2) throw new Error('Missing or oversized tokenizer.json');
  const architecture = isLaya ? 'laya' : config.architecture === 'boundary' ? 'boundary' : config.model_type === 'extractor' && !config.boundary_head && (!config.config_version || config.config_version < 3) ? 'span' : null;
  if (!architecture) throw new Error('Unsupported extraction architecture');
  const variants = [...files.keys()].filter(p => /(^|\/)model\.gguf$/.test(p) || /(^|\/)encoder_model\.gguf$/.test(p));
  let weights;
  if (architecture === 'laya') {
    if (!files.has('model.safetensors')) throw new Error('Laya requires a complete model.safetensors');
    weights = ['model.safetensors'];
  } else if (architecture === 'boundary') {
    weights = files.has('model.gguf') ? ['model.gguf'] : files.has('model.safetensors') ? ['model.safetensors'] : variants.filter(p => !p.endsWith('encoder_model.gguf'));
  } else if (files.has('model.safetensors')) weights = ['model.safetensors'];
  else {
    const encoders = [...files.keys()].filter(p => /(^|\/)(encoder_model(?:-[^/]+)?|gliner2-encoder\.[^/]+)\.gguf$/.test(p));
    const candidates = precision ? encoders.filter(p => p.toLowerCase().replaceAll('-', '_').includes(precision.toLowerCase())) : encoders;
    if (candidates.length !== 1) throw new Error('Select a directory containing exactly one complete encoder/head precision pair');
    const encoder = candidates[0];
    const head = encoder.replace('encoder_model', 'head_model').replace('gliner2-encoder.', 'gliner2-head.');
    if (!files.has(head)) throw new Error('Missing head GGUF matching the encoder precision');
    weights = [encoder, head];
  }
  if (architecture === 'boundary' && weights.length !== 1) throw new Error('Select exactly one boundary model variant');
  let receipt = null;
  if (files.has('antfly_inference_bundle.json')) receipt = await readJson(files, 'antfly_inference_bundle.json', 65536);
  const layaPrecision = architecture === 'laya' && !precision ? ((await tensorDirectory(files.get(weights[0]), 'safetensors')).some(t => t.kind === 1) ? 'fp16' : 'fp32') : null;
  const selectedPrecision = precision || receipt?.precision || layaPrecision || (weights.every(p => p.endsWith('.safetensors')) ? 'fp32' : /q4_k/i.test(weights[0]) ? 'q4_k' : /q8_0/i.test(weights[0]) ? 'q8_0' : /q4_0/i.test(weights[0]) ? 'q4_0' : null);
  if (!['fp32', 'fp16', 'fp16_encoder', 'q8_0', 'q4_k', 'q4_0'].includes(selectedPrecision)) throw new Error('Unknown precision: provide a bundle receipt or select precision explicitly');
  if (architecture === 'laya' && !['fp16', 'fp32'].includes(selectedPrecision)) throw new Error('Laya supports dense FP16/FP32 SafeTensors, not GLiNER quantized bundles');
  const bytes = weights.reduce((sum, p) => sum + files.get(p).size, 0);
  integer(bytes, LIMITS.bundle);
  return { architecture, config, encoderConfig, precision: selectedPrecision, weights, bytes, files, receipt, qualified: false };
}

export async function tensorDirectory(file, format) {
  if (!file.size || file.size > LIMITS.file) throw new Error('Invalid weight file size');
  let tensors = [];
  let dataOffset;
  if (format === 'safetensors') {
    const prefix = new DataView(await file.slice(0, 8).arrayBuffer());
    const size = integer(Number(prefix.getBigUint64(0, true)), LIMITS.header);
    dataOffset = 8 + size;
    if (dataOffset > file.size) throw new Error('Truncated SafeTensors header');
    const header = JSON.parse(await file.slice(8, dataOffset).text());
    for (const [name, value] of Object.entries(header)) {
      if (name === '__metadata__') continue;
      const kind = { F32: 0, F16: 1 }[value.dtype];
      if (kind === undefined) throw new Error(`Unsupported SafeTensors dtype: ${value.dtype}`);
      const [start, end] = value.data_offsets;
      const length = byteLength(value.shape, kind);
      if (integer(end) - integer(start) !== length) throw new Error(`Invalid tensor byte length: ${name}`);
      tensors.push({ name, shape: value.shape, kind, start: dataOffset + start, end: dataOffset + end });
    }
  } else {
    const data = new Uint8Array(await file.slice(0, LIMITS.header).arrayBuffer());
    const view = new DataView(data.buffer);
    let at = 0;
    const take = n => { integer(n); const start = at; at += n; if (at > data.length) throw new Error('Truncated or oversized GGUF header'); return start; };
    const u32 = () => view.getUint32(take(4), true);
    const u64 = () => integer(Number(view.getBigUint64(take(8), true)));
    const string = () => { const n = integer(u64(), LIMITS.header); return decoder.decode(data.subarray(take(n), at)); };
    const value = (kind, depth = 0) => {
      if (depth > 4) throw new Error('GGUF metadata nesting limit');
      if (kind === 8) return string();
      if (kind === 9) { const type = u32(); const n = integer(u64(), 1000000); for (let i = 0; i < n; i++) value(type, depth + 1); return; }
      if (kind === 4) return u32();
      const width = { 0: 1, 1: 1, 2: 2, 3: 2, 5: 4, 6: 4, 7: 1, 10: 8, 11: 8, 12: 8 }[kind];
      if (!width) throw new Error('Unsupported GGUF metadata type');
      take(width);
    };
    if (u32() !== 0x46554747 || u32() !== 3) throw new Error('Expected GGUF v3');
    const count = integer(u64(), 2048), metadataCount = integer(u64(), 100000);
    let alignment = 32;
    for (let i = 0; i < metadataCount; i++) {
      const key = string(), kind = u32(), item = value(kind);
      if (key === 'general.alignment') alignment = item;
    }
    if (!Number.isInteger(alignment) || alignment < 1 || alignment > 4096 || alignment & (alignment - 1)) throw new Error('Invalid GGUF alignment');
    for (let i = 0; i < count; i++) {
      const name = string(), rank = integer(u32(), 4), dims = [];
      for (let j = 0; j < rank; j++) dims.push(u64());
      const kind = u32(), offset = u64(), shape = dims.reverse();
      const length = byteLength(shape, kind);
      if (offset % alignment) throw new Error('Unaligned GGUF tensor');
      tensors.push({ name, shape, kind, start: offset, end: offset + length });
    }
    dataOffset = Math.ceil(at / alignment) * alignment;
    tensors = tensors.map(t => ({ ...t, start: t.start + dataOffset, end: t.end + dataOffset }));
  }
  if (!tensors.length || tensors.length > 2048) throw new Error('Invalid tensor inventory');
  const names = new Set();
  let previousEnd = dataOffset;
  for (const t of tensors.sort((a, b) => a.start - b.start)) {
    if (!t.name || names.has(t.name) || t.start < previousEnd || t.end > file.size || !Number.isSafeInteger(t.end)) throw new Error(`Invalid, duplicate, overlapping or truncated tensor: ${t.name}`);
    previousEnd = t.end;
    names.add(t.name);
  }
  return tensors;
}
