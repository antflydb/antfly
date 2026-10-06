// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { openAsBlob } from 'node:fs';
import { readFile, readdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { inspectBundle } from './runtime/extraction-bundle.js';
import { ExtractionSession } from './runtime/extraction-session.js';
import { createWasmAbi } from './runtime/wasm-abi.js';
import { layaFixture } from './laya-test-fixture.mjs';

const config = { model_type: 'extractor', architecture: 'span', config_version: 3, architecture_version: 1, span_head: { span_mode: 'markerV0' } };
function bundle(extra = {}) {
  return new Map(Object.entries({ 'config.json': { ...config, ...extra }, 'encoder_config/config.json': {}, 'tokenizer_config.json': {}, 'tokenizer.json': {} }).map(([p, v]) => [p, new Blob([JSON.stringify(v)])]).concat([['gliner2-encoder.Q8_0.gguf', new Blob(['encoder'])], ['gliner_head.gguf', new Blob(['head'])]]));
}
test('Decide version-3 marker contract and matching split GGUF head', async () => {
  const files = bundle(), info = await inspectBundle(files);
  assert.equal(info.architecture, 'decide'); assert.equal(info.precision, 'q8_0');
  assert.deepEqual(info.weights, ['gliner2-encoder.Q8_0.gguf', 'gliner_head.gguf']);
  for (const extra of [{ config_version: 4 }, { architecture_version: 2 }, { span_head: { span_mode: 'other' } }]) await assert.rejects(inspectBundle(bundle(extra)), /architecture/);
  files.set('gliner2-head.Q8_0.gguf', new Blob(['other']));
  await assert.rejects(inspectBundle(files), /exactly one head/);
});

const root = process.env.DECIDE_MODEL, wasmPath = process.env.EXTRACTION_WASM;
test('Decide WASM rejects wrong geometry, tensor shapes, duplicates and incomplete inventories', { skip: !wasmPath }, async () => {
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const session = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  const wrapper = { ...config, model_name: 'microsoft/deberta-v3-large', counting_layer: 'count_lstm', token_pooling: 'first', use_moe: false };
  const encoder = { hidden_size: 1024, num_hidden_layers: 24, num_attention_heads: 16, intermediate_size: 4096, vocab_size: 128011, max_position_embeddings: 512, position_buckets: 256, hidden_act: 'gelu' };
  const create = (cfg = encoder) => session.check(session.text(JSON.stringify({ config: JSON.stringify(wrapper), encoder_config: JSON.stringify(cfg), tokenizer_config: '{}', precision: 'q8_0' }), (p, n) => instance.exports.extraction_create(p, n)));
  const weight = (name, shape) => session.check(session.text(JSON.stringify({ name, shape, kind: 0 }), (m, n) => session.bytes(new Uint8Array(4), (p, s) => instance.exports.extraction_weight(session.handle, m, n, p, s))));
  try {
    assert.throws(() => create({ ...encoder, hidden_size: 768 }), /UnsupportedGlinerDecideConfig/);
    session.handle = create();
    assert.throws(() => weight('classifier.0.weight', [2048, 768]), /InvalidGlinerTensorShape/);
    assert.throws(() => weight('unrecognized.weight', [1]), /UnexpectedGlinerTensor/);
    weight('classifier.2.bias', [1]);
    assert.throws(() => weight('classifier.2.bias', [1]), /DuplicateWeight/);
    const tokenizer = new Uint8Array(await layaFixture().get('tokenizer.json').arrayBuffer());
    session.check(session.bytes(tokenizer, (p, n) => instance.exports.extraction_tokenizer(session.handle, p, n)));
    assert.throws(() => session.check(session.text('0'.repeat(64), p => instance.exports.extraction_finalize(session.handle, p, 1n))), /IncompleteGlinerTensorInventory/);
  } finally { session.unload(); }
});
test('real Decide WASM Q8: reference classification, typed tasks, limits and repeatability', { skip: !root || !wasmPath, timeout: 600000 }, async () => {
  const files = new Map();
  for (const p of await readdir(root, { recursive: true })) if (/\.(json|gguf)$/.test(p)) files.set(p, await openAsBlob(resolve(root, p)));
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const session = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  const fixture = JSON.parse(await readFile(new URL('../testdata/gliner25/decide/cases.json', import.meta.url)));
  try {
    assert.equal((await session.load(files, 'q8_0')).architecture, 'decide');
    const before = instance.exports.extraction_live_bytes(session.handle);
    let first;
    for (const item of fixture.classification) {
      const request = { schema_version: 2, model: 'decide', schema: { entities: [], relations: [], ...item.v2_schema }, inputs: [{ id: item.name, content: item.text }], options: { include_confidence: true } };
      assert.equal(session.run(request, true).value.encoded_tokens, item.input_ids.length);
      const result = session.run(request).value;
      console.log(item.name, JSON.stringify(result.data[0].classifications));
      assert.equal(result.data[0].id, item.name);
      // Near ties can change with Q8 rounding; otherwise compare reference labels.
      for (const [taskIndex, task] of item.v2_schema.classifications.entries()) {
        const expected = item.result[task.name], logits = item.classifier_logits[taskIndex];
        const sorted = [...logits].sort((a, b) => b - a);
        if (sorted[0] - sorted[1] < .1) continue;
        const labels = (Array.isArray(expected) ? expected : [expected]).map(e => typeof e === 'string' ? e : e.label);
        assert.deepEqual(result.data[0].classifications.filter(c => c.name === task.name).map(c => c.label), labels, `${item.name}/${task.name}`);
      }
      first ??= { request, result };
    }
    assert.deepEqual(session.run(first.request).value, first.result);
    assert(instance.exports.extraction_live_bytes(session.handle) < before + 128 * 1024, 'request scratch must be released');
    const split = { ...first.request, schema: { classifications: Array.from({ length: 24 }, (_, i) => ({ ...first.request.schema.classifications[0], name: 'intent' + i })) } };
    assert(session.run(split, true).value.encoded_tokens > 512, 'oversized prompts must split into bounded sequences');
    assert.throws(() => session.run({ ...first.request, schema: { entities: ['person'], ...first.request.schema, structures: { person: { fields: { name: { type: 'str' } } } } } }, true), /UnsupportedGlinerSpanV2Task/);
    assert.throws(() => session.run({ ...first.request, options: { long_document: { mode: 'window' } } }, true), /UnsupportedGlinerSpanLongDocument/);
    assert.throws(() => session.run({ ...first.request, inputs: [{ content: 'word '.repeat(600) }] }, true), /Token|Sequence/);
  } finally { session.unload(); }
});
