// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { openAsBlob } from 'node:fs';
import { readFile, readdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { inspectBundle } from './runtime/extraction-bundle.js';
import { ExtractionSession } from './runtime/extraction-session.js';
import { createWasmAbi } from './runtime/wasm-abi.js';
import { layaFixture, layaRequest } from './laya-test-fixture.mjs';

function bundle() {
  return new Map(Object.entries({
    'encoder/config.json': { model_type: 'modernbert', rope_parameters: { full_attention: { rope_theta: 160000, rope_type: 'default' } } },
    'rl_agent_config.json': { temperature: [1, 2, 3] },
    'tokenizer/tokenizer_config.json': { mask_token: '[MASK]' },
    'tokenizer/tokenizer.json': {},
    'model.safetensors': {},
  }).map(([path, value]) => [path, new Blob([JSON.stringify(value)])]));
}
test('Laya upstream and native folders share a normalized config without mutating input', async () => {
  const files = bundle();
  const upstream = await inspectBundle(files, 'fp16');
  assert.equal(upstream.architecture, 'laya');
  assert.equal(upstream.config.global_rope_theta, 160000);
  assert.equal(upstream.config.laya.mask_token, '[MASK]');
  assert(!files.has('tokenizer.json'));
  const native = new Map([['config.json', new Blob([JSON.stringify(upstream.config)])], ['model.safetensors', files.get('model.safetensors')], ['tokenizer.json', files.get('tokenizer/tokenizer.json')], ['tokenizer_config.json', files.get('tokenizer/tokenizer_config.json')]]);
  assert.equal((await inspectBundle(native, 'fp16')).architecture, 'laya');
  native.set('config.json', new Blob([JSON.stringify({ ...upstream.config, model_type: 'modern_bert' })]));
  assert.equal((await inspectBundle(native, 'fp16')).architecture, 'laya');
  await assert.rejects(inspectBundle(files, 'q8_0'), /dense FP16/);
  files.delete('model.safetensors');
  await assert.rejects(inspectBundle(files, 'fp16'), /model.safetensors/);
});

const root = process.env.LAYA_MODEL, wasmPath = process.env.EXTRACTION_WASM;
test('tiny Laya WASM: packed layouts, pointer, two-stage, Q8 and OpenDecider', { skip: !wasmPath, timeout: 120000 }, async () => {
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const session = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  const variants = [ {},
    { packing: { mode: 'question', max_packed_len: 2048 } },
    { packing: { mode: 'question', max_packed_len: 2048, trunk_sees: 'questions' } },
    { packing: { mode: 'question', max_packed_len: 2048, fuse_layers: 2, question_first: true } },
    { packing: { mode: 'candidate', max_packed_len: 2048, two_stage: { top_k: 2, mass_cutoff: .9 } } },
    { decision_head: 'pointer', pointer_dim: 32 },
    { decision_head: 'pointer', pointer_dim: 32, packing: { mode: 'candidate', max_packed_len: 2048 } },
    { weight_quantization: 'q8_0' },
    { format: 'opendecider' },
  ];
  try {
    for (const precision of ['fp32', 'fp16', 'bf16']) for (const extra of variants) {
      await session.load(layaFixture(extra, precision), precision);
      const before = instance.exports.extraction_live_bytes(session.handle);
      const geometry = session.run(layaRequest, true).value;
      assert(geometry.encoded_tokens > 0);
      const output = session.run(layaRequest).value;
      assert.deepEqual(output.data[0].decisions.map(d => d.type), ['choice', 'score', 'boolean']);
      if (precision === 'fp32' && Object.keys(extra).length === 0) {
        // Stable numerical regression baseline for the deterministic tiny
        // checkpoint. Released-model oracle tests run separately when present.
        const expected = [
          { label: 'fetch', probabilities: [0.33324423, 0.33341044, 0.33334532], act: 0.53863657 },
          { label: 'high', probabilities: [0.33296826, 0.33311945, 0.33391225], act: 0.54081124 },
          { label: 'true', probabilities: [0.49963111, 0.50036889], act: 0.53691679 },
        ];
        for (const [i, decision] of output.data[0].decisions.entries()) {
          assert.equal(decision.label, expected[i].label);
          for (const [j, probability] of decision.probabilities.entries())
            assert(Math.abs(probability.probability - expected[i].probabilities[j]) < 1e-5);
          assert(Math.abs(decision.act_probability - expected[i].act) < 1e-5);
        }
      }
      assert.deepEqual(session.run(layaRequest).value, output, JSON.stringify(extra));
      for (const d of output.data[0].decisions) {
        assert(Math.abs(d.probabilities.reduce((sum, p) => sum + p.probability, 0) - 1) < 1e-5);
        if (extra.format === 'opendecider') assert.equal(d.act_probability, undefined);
      }
      if (extra.packing?.mode === 'candidate') {
        const many = structuredClone(layaRequest); many.schema.classifications = [ { ...many.schema.classifications[0], labels: Array.from({ length: 24 }, (_, i) => 'option' + i) } ];
        assert.equal(session.run(many).value.data[0].decisions[0].probabilities.length, 24);
      }
      assert(instance.exports.extraction_live_bytes(session.handle) < before + 65536, 'request scratch must be released');
      if (extra.packing?.mode === 'question' && !extra.packing.trunk_sees && !extra.packing.fuse_layers) {
        const cached = structuredClone(layaRequest); cached.inputs[0].content = 'state '.repeat(100);
        const cold = session.run(cached).value, warm = session.run(cached).value;
        for (let i = 0; i < cold.data[0].decisions.length; i++) for (let j = 0; j < cold.data[0].decisions[i].probabilities.length; j++)
          assert(Math.abs(cold.data[0].decisions[i].probabilities[j].probability - warm.data[0].decisions[i].probabilities[j].probability) < 1e-6);
        const retained = instance.exports.extraction_live_bytes(session.handle);
        assert(retained > before + 65536, 'long state should populate the trunk cache');
        assert.deepEqual(session.run(cached).value, warm);
        assert.equal(instance.exports.extraction_live_bytes(session.handle), retained, 'hot cache memory must plateau');
      }
      session.unload();
    }
    const invalid = layaFixture({ decision_head: 'pointer', pointer_dim: 32 });
    // Corrupt a required head tensor's geometry without changing its byte count.
    const blob = invalid.get('model.safetensors');
    const n = Number(new DataView(await blob.slice(0, 8).arrayBuffer()).getBigUint64(0, true));
    const header = JSON.parse(await blob.slice(8, 8 + n).text()); header['pointer.q.weight'].shape = [16, 128];
    const encoded = new TextEncoder().encode(JSON.stringify(header)), prefix = new ArrayBuffer(8); new DataView(prefix).setBigUint64(0, BigInt(encoded.length), true);
    invalid.set('model.safetensors', new Blob([prefix, encoded, blob.slice(8 + n)]));
    await assert.rejects(session.load(invalid, 'fp32'), /InvalidLayaWeights/);
  } finally { session.unload(); }
});
export const request = {
  schema_version: 2, model: 'laya', inputs: [{ id: 'local', content: 'Please search for the latest documentation about browser inference.' }],
  schema: { classifications: [
    { name: 'tool', mode: 'single', instruction: 'Which tool is needed to handle this request?', labels: ['search', 'fetch', 'none'] },
    { name: 'urgency', mode: 'ordinal', instruction: 'How urgent is this request?', labels: ['low', 'medium', 'high'] },
    { name: 'search_needed', mode: 'boolean', instruction: 'Does this request require searching for information?', labels: ['false', 'true'] },
  ] },
};
test('released Laya WASM: typed decisions, limits and repeatability', { skip: !root || !wasmPath, timeout: 600000 }, async () => {
  if (process.env.LAYA_ORACLE) {
    const oracle = JSON.parse(await readFile(process.env.LAYA_ORACLE, 'utf8'));
    if (oracle[0].text) request.inputs[0].content = oracle[0].text;
  }
  const files = new Map();
  for (const path of await readdir(root, { recursive: true })) if (/\.(json|safetensors)$/.test(path)) files.set(path, await openAsBlob(resolve(root, path)));
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const session = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  try {
    const info = await session.load(files, 'fp16');
    const loadedBytes = instance.exports.extraction_live_bytes(session.handle);
    console.log({ loadedBytes });
    assert.equal(info.architecture, 'laya');
    assert(session.run(request, true).value.encoded_tokens > 0);
    const result = session.run(request);
    console.log(JSON.stringify(result));
    console.log({ liveBytes: instance.exports.extraction_live_bytes(session.handle) });
    assert(instance.exports.extraction_live_bytes(session.handle) < loadedBytes + 65536, 'Only bounded tokenizer cache growth may remain after a request');
    const row = result.value.data[0];
    assert.equal(row.id, 'local');
    assert.deepEqual(row.decisions.map(d => d.type), ['choice', 'score', 'boolean']);
    for (const d of row.decisions) {
      assert(Math.abs(d.probabilities.reduce((sum, p) => sum + p.probability, 0) - 1) < 1e-5);
      assert(d.confidence >= 0 && d.confidence <= 1);
      assert(d.act_probability >= 0 && d.act_probability <= 1);
    }
    assert(row.decisions[1].expected_value >= 0 && row.decisions[1].expected_value <= 2);
    assert.equal(row.decisions[2].true_probability, row.decisions[2].probabilities[1].probability);
    try {
      assert.deepEqual(session.run(request).value, result.value);
    } catch (error) {
      console.log({ repeatLiveBytes: instance.exports.extraction_live_bytes(session.handle), wasmBytes: instance.exports.memory.buffer.byteLength });
      console.log(new TextDecoder().decode(new Uint8Array(instance.exports.memory.buffer, instance.exports.extraction_error_ptr(), Number(instance.exports.extraction_error_len()))));
      throw error;
    }
    if (process.env.LAYA_ORACLE) {
      const expected = JSON.parse(await readFile(process.env.LAYA_ORACLE, 'utf8'));
      let maxProbabilityError = 0;
      for (const [i, d] of row.decisions.entries()) {
        const ps = expected[i].probabilities;
        for (const [j, p] of d.probabilities.entries()) {
          const error = Math.abs(p.probability - ps[j]);
          maxProbabilityError = Math.max(maxProbabilityError, error);
          assert(error < 5e-5, `${d.name} probability ${j}`);
        }
        const confidence = d.type === 'boolean' ? Math.max(ps[1], 1 - ps[1]) : 1 + ps.reduce((sum, p) => sum + p * Math.log(Math.max(p, 1e-12)), 0) / Math.log(ps.length);
        assert(Math.abs(d.confidence - confidence) < 5e-5, `${d.name} confidence`);
        if (d.type === 'score') assert(Math.abs(d.expected_value - ps.reduce((sum, p, j) => sum + j * p, 0)) < 5e-5);
        assert(Math.abs(d.act_probability - expected[i].act_probability) < 5e-5, `${d.name} action probability`);
      }
      console.log({ oracleQuestions: expected.length, maxProbabilityError });
    }
    assert.throws(() => session.run({ ...request, schema: { entities: ['person'] } }, true), /Unsupported/);
    assert.throws(() => session.run({ ...request, inputs: [{ content: 'word '.repeat(600) }] }, true), /TextLimit/);
    assert.throws(() => session.run({ ...request, options: { long_document: { mode: 'window' } } }, true), /Unsupported/);
    assert.throws(() => session.run({ ...request, schema: { classifications: Array.from({length:17}, (_, i) => ({ ...request.schema.classifications[0], name: String(i) })) } }, true), /RequestLimit/);
  } finally { session.unload(); }
});
