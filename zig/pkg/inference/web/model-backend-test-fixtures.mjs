// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { layaFixture } from './laya-test-fixture.mjs';
const json = value => new Blob([JSON.stringify(value)]);
function safetensors(entries) {
  const header = {}; let offset = 0;
  const chunks = entries.map(([name, shape, data, dtype = 'F32']) => {
    header[name] = { dtype, shape, data_offsets: [offset, offset + data.byteLength] };
    offset += data.byteLength; return data;
  });
  const bytes = new TextEncoder().encode(JSON.stringify(header)), prefix = new ArrayBuffer(8);
  new DataView(prefix).setBigUint64(0, BigInt(bytes.length), true);
  return new Blob([prefix, bytes, ...chunks]);
}
export async function modernFixture(boundary = false, neck = false, precision = 'fp32') {
  const files = layaFixture({}, precision);
  const encoder = JSON.parse(await files.get('config.json').text()); delete encoder.laya;
  Object.assign(encoder, { hidden_activation: 'gelu', norm_eps: 1e-5, global_rope_theta: 160000, local_rope_theta: 10000, norm_bias: false, attention_bias: false, mlp_bias: false, attention_dropout: 0, embedding_dropout: 0, mlp_dropout: 0 });
  const blob = files.get('model.safetensors'), bytes = new Uint8Array(await blob.arrayBuffer());
  const length = Number(new DataView(bytes.buffer).getBigUint64(0, true));
  const directory = JSON.parse(new TextDecoder().decode(bytes.slice(8, 8 + length)));
  const entries = Object.entries(directory).filter(([name]) => name.startsWith('encoder.')).map(([name, d]) => [name, d.shape, bytes.slice(8 + length + d.data_offsets[0], 8 + length + d.data_offsets[1]), d.dtype]);
  const add = (name, shape, norm = false) => { const values = new Float32Array(shape.reduce((a, b) => a * b, 1)); if (norm) values.fill(1); entries.push([name, shape, values]); };
  let config;
  if (boundary) {
    config = JSON.parse(await readFile(new URL('../testdata/gliner25/models/base/config.json', import.meta.url)));
    config.model_name = 'arbitrary-modernbert'; config.max_len = 512;
    if (neck) { config.antenna_neck = 'linear'; add('gliner_neck.weight', [64, 64]); add('gliner_neck.bias', [64]); }
    const source = await readFile(new URL('../src/models/gliner_boundary_tensor_inventory.zig', import.meta.url), 'utf8');
    const parse = section => new Map([...section.matchAll(/\.name = "([^"]+)", \.shape = &\.\{([^}]*)\}/g)].map(([, name, dims]) => [name, dims.split(',').map(Number)]));
    const base = parse(source.split('pub const base')[1].split('pub const')[0]), small = parse(source.split('pub const small')[1].split('pub const')[0]);
    assert(base.size > 100);
    for (const [name, shape] of base) if (!name.startsWith('encoder.')) {
      const derived = shape.map((d, i) => { const scale = (d - small.get(name)[i]) / 384; return scale * 64 + d - scale * 768; });
      add(name, derived, name.endsWith('norm.weight') || name.includes('layer_norm.weight'));
    }
  } else {
    config = { model_type: 'extractor', architecture: 'span', config_version: 3, architecture_version: 1, counting_layer: 'count_lstm', span_head: { span_mode: 'markerV0' } };
    add('classifier.0.weight', [128, 64]); add('classifier.0.bias', [128]); add('classifier.2.weight', [1, 128]); add('classifier.2.bias', [1]); add('count_pred.0.weight', [1, 1]);
  }
  const tokenizer = JSON.parse(await files.get('tokenizer.json').text());
  tokenizer.normalizer.lowercase = false;
  const markers = ['[P]', '[C]', '[E]', '[R]', '[L]', '[SEP_STRUCT]', '[SEP_TEXT]', '[DESCRIPTION]', '[EXAMPLE]', '[OUTPUT]'];
  for (const [i, content] of markers.entries()) {
    const id = 22 + i; tokenizer.model.vocab[content] = id;
    tokenizer.added_tokens.push({ id, content, special: true, normalized: false, single_word: false, lstrip: false, rstrip: false });
  }
  files.set('config.json', json(config)); files.set('encoder_config/config.json', json(encoder));
  files.set('tokenizer.json', json(tokenizer)); files.set('model.safetensors', safetensors(entries));
  return files;
}
