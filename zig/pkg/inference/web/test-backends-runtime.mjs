// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import './configuration-test-runtime.mjs';
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFile, mkdtemp, open, rm } from 'node:fs/promises';
import { openAsBlob } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { layaFixture } from './laya-test-fixture.mjs';
import { modernFixture } from './model-backend-test-fixtures.mjs';
import { ExtractionSession } from './runtime/extraction-session.js';
import { createWasmAbi } from './runtime/wasm-abi.js';
import { inspectModel } from './runtime/model-adapters.js';
const json = value => new Blob([JSON.stringify(value)]);
const wasmPath = process.env.EXTRACTION_WASM;
async function session() {
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  return new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
}
const request = { model: 'local', input: 'state state', questions: [{ name: 'tool', type: 'choice', choices: [{ value: 'search' }, { value: 'fetch' }], instructions: 'question' }] };
test('inspection uses native configuration validation without model weights', { skip: !wasmPath }, async () => {
  const files = await modernFixture();
  files.delete('model.safetensors');
  assert.equal((await inspectModel(files)).availability.available, true);
  const config = JSON.parse(await files.get('config.json').text());
  Object.assign(config, { model_name: 'microsoft/deberta-v3-base', token_pooling: 'first', use_moe: false });
  files.set('config.json', json(config));
  files.set('encoder_config/config.json', json({ model_type: 'deberta-v2', hidden_size: 768, num_hidden_layers: 12, num_attention_heads: 12, intermediate_size: 3072, vocab_size: 128011 }));
  const unsupported = await inspectModel(files);
  assert.equal(unsupported.availability.available, false);
  assert.equal(unsupported.availability.reason, 'UnsupportedGlinerDecideConfig');
  assert.deepEqual(unsupported.execution.tasks, []);
  const large = { model_type: 'deberta-v2', hidden_size: 1024, num_hidden_layers: 24, num_attention_heads: 16, intermediate_size: 4096, vocab_size: 128011 };
  config.model_name = 'microsoft/deberta-v3-large';
  files.set('config.json', json(config)); files.set('encoder_config/config.json', json(large));
  assert.equal((await inspectModel(files)).availability.available, true);
  large.position_buckets = 128;
  files.set('encoder_config/config.json', json(large));
  assert.equal((await inspectModel(files)).availability.available, false);
  const runtime = await session();
  try {
    await runtime.load(await modernFixture(), 'fp32');
    const before = runtime.run(request, false, 'decide').value;
    await inspectModel(files);
    assert.deepEqual(runtime.run(request, false, 'decide').value, before);
  } finally { runtime.unload(); }
});
test('receipts verify sidecars, policies and manifests before model creation', { skip: !wasmPath }, async () => {
  const runtime = await session();
  const files = await modernFixture();
  for (const path of ['processor_config.json', 'config_sentence_transformers.json', '1_Pooling/config.json', 'calibrations/policy.json', 'model_manifest.json']) {
    files.set(path, json(path === 'model_manifest.json' ? { tasks: ['decide'], capabilities: ['typed_decisions'] } : {}));
    files.set('antfly_inference_bundle.json', json({ precision: 'fp32', files: [{ path, size_bytes: files.get(path).size, sha256: '0'.repeat(64) }] }));
    await assert.rejects(runtime.load(files, 'fp32'), new RegExp(`Integrity check failed: ${path.replaceAll('.', '\\.')}`));
    assert.equal(runtime.handle, 0);
  }
  const file = files.get('model_manifest.json');
  files.set('antfly_inference_bundle.json', json({ precision: 'fp32', files: [{ path: 'model_manifest.json', size_bytes: file.size, sha256: createHash('sha256').update(Buffer.from(await file.arrayBuffer())).digest('hex') }] }));
  try { await runtime.load(files, 'fp32'); assert(runtime.handle); } finally { runtime.unload(); }
});
test('ModernBERT marker and boundary execute extraction and typed decisions', { skip: !wasmPath, timeout: 120000 }, async () => {
  const runtime = await session();
  try {
    for (const [boundary, neck, precision] of [[false, false, 'fp32'], [false, false, 'bf16'], [true, false, 'fp32'], [true, true, 'fp32']]) {
      await runtime.load(await modernFixture(boundary, neck, precision), precision);
      const before = runtime.wasm.extraction_live_bytes(runtime.handle);
      const valid = runtime.run(request, true, 'decide').value;
      assert.equal(valid.valid, true);
      const result = runtime.run(request, false, 'decide').value;
      assert.equal(result.answers[0].name, 'tool');
      assert.equal(result.answers[0].choice, 'search');
      assert.deepEqual(runtime.run(request, false, 'decide').value, result);
      const typed = runtime.run({ ...request, questions: [...request.questions, { name: 'priority', type: 'score', instructions: 'question', levels: [{ label: 'low' }, { label: 'high' }] }, { name: 'act', type: 'predicate', instructions: 'question' }] }, false, 'decide').value;
      assert.deepEqual(typed.answers.map(answer => answer.type), ['choice', 'score', 'predicate']);
      assert.equal(typeof typed.answers[1].score, 'number');
      assert.equal(typed.answers[2].probability, 0.5);
      const extract = runtime.run({ schema_version: 2, model: 'local', inputs: [{ content: 'state state' }], schema: { classifications: [{ name: 'tool', mode: 'single', labels: ['search', 'fetch'] }] } }).value;
      assert.equal(extract.data[0].classifications[0].label, 'search');
      assert(runtime.wasm.extraction_live_bytes(runtime.handle) < before + 128 * 1024);
      runtime.unload();
    }
  } finally { runtime.unload(); }
});

test('compatibility entrypoints enforce task and head restrictions', { skip: !wasmPath }, async () => {
  const runtime = await session();
  const extraction = { schema_version: 2, model: 'local', inputs: [{ content: 'state state' }], schema: { classifications: [{ name: 'tool', mode: 'single', labels: ['search', 'fetch'] }] } };
  try {
    for (const declaration of [
      { tasks: ['decide'], capabilities: ['typed_decisions'] },
      { tasks: ['extract'], capabilities: ['extraction'] },
    ]) {
      const files = await modernFixture(true);
      files.set('model_manifest.json', json(declaration));
      await runtime.load(files, 'fp32');
      for (const task of ['extract', undefined]) for (const validate of [false, true]) {
        assert.throws(() => runtime.run(extraction, validate, task), /UnsupportedInferenceTask|UnsupportedExtractionFeature/);
      }
      if (declaration.tasks.includes('decide')) assert.equal(runtime.run(request, false, 'decide').value.answers[0].choice, 'search');
      runtime.unload();
    }
    for (const boundary of [false, true]) {
      const files = await modernFixture(boundary);
      files.set('model_manifest.json', json({ tasks: ['extract', 'decide'], capabilities: ['typed_decisions'] }));
      const loaded = await runtime.load(files, 'fp32');
      assert.deepEqual(loaded.execution.tasks, ['decide']);
      for (const task of ['extract', undefined]) assert.throws(() => runtime.run(extraction, true, task), /UnsupportedInferenceTask/);
      runtime.unload();
    }
    const files = await modernFixture(true);
    files.set('antfly_catalog.json', json({ tasks: ['extract'], capabilities: ['classification'] }));
    const loaded = await runtime.load(files, 'fp32');
    assert.deepEqual(loaded.execution.capabilities, ['classification']);
    for (const task of ['extract', undefined]) assert.equal(runtime.run(extraction, false, task).value.data[0].classifications[0].label, 'search');
    assert.throws(() => runtime.run(request, true, 'decide'), /UnsupportedInferenceTask/);
    runtime.unload();
    // Older Laya envelopes still work under the decider role correction.
    await runtime.load(layaFixture(), 'fp32');
    const legacy = { schema_version: 2, model: 'local', inputs: [{ content: 'state state' }], schema: { classifications: [{ name: 'tool', instruction: 'question', mode: 'single', labels: ['search', 'fetch'] }] } };
    assert.equal(runtime.run(legacy, true).value.valid, true);
    assert.equal(runtime.run(legacy, false).value.data.length, 1);
  } finally { runtime.unload(); }
});

test('legacy classification rejects omitted and empty labels during validation and execution', { skip: !wasmPath, timeout: 120000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'antfly-web-legacy-'));
  const runtime = await session();
  try {
    // Sparse FP16 data exercises the full legacy inventory without retaining
    // hundreds of megabytes on the JS heap. Invalid requests run no encoder.
    const inventory = await readFile(new URL('../src/web/extraction_span_inventory.zig', import.meta.url), 'utf8');
    const header = {}; let offset = 0;
    for (const [, name, dims] of inventory.matchAll(/\.name = "([^"]+)", \.shape = &\.\{([^}]*)\}/g)) {
      const shape = dims.split(',').map(Number), size = shape.reduce((a, b) => a * b, 1) * 2;
      const canonical = /^(encoder\.|embeddings\.)/.test(name) ? `encoder.${name}` : name;
      header[canonical] = { dtype: 'F16', shape, data_offsets: [offset, offset + size] };
      offset += size;
    }
    const bytes = Buffer.from(JSON.stringify(header)), prefix = Buffer.alloc(8);
    prefix.writeBigUInt64LE(BigInt(bytes.length));
    const path = join(directory, 'model.safetensors'), file = await open(path, 'w');
    try { await file.write(prefix); await file.write(bytes); await file.truncate(8 + bytes.length + offset); }
    finally { await file.close(); }
    const files = new Map([
      ['config.json', json({ model_type: 'extractor', model_name: 'microsoft/deberta-v3-base', max_width: 8 })],
      ['encoder_config/config.json', json({ model_type: 'deberta-v2', hidden_size: 768, num_hidden_layers: 12, num_attention_heads: 12, intermediate_size: 3072, vocab_size: 128011, max_position_embeddings: 512 })],
      ['tokenizer_config.json', json({})], ['tokenizer.json', layaFixture().get('tokenizer.json')],
      ['model.safetensors', await openAsBlob(path)],
    ]);
    await runtime.load(files, 'fp16');
    const request = { schema_version: 1, model: 'local', text: 'state', task: 'classification' };
    for (const labels of [undefined, []]) for (const task of ['extract', undefined]) for (const validate of [true, false]) {
      const invalid = labels === undefined ? request : { ...request, labels };
      assert.throws(() => runtime.run(invalid, validate, task), /NoLabelsProvided/);
      assert(runtime.handle, 'invalid requests preserve the loaded model');
    }
    assert.equal(runtime.run({ ...request, labels: ['search', 'fetch'] }, true, 'extract').value.valid, true);
  } finally { runtime.unload(); await rm(directory, { recursive: true, force: true }); }
});

// Sparse files keep the fixed 24-layer/262144-token checkpoint fixture off
// the JS heap. Nonzero embeddings/projection exercise every real encoder op.
async function embeddingFixture(directory) {
  const config = JSON.parse(await readFile(new URL('../src/architectures/embedding_gemma2_config.json', import.meta.url)));
  const files = new Map([['config.json', json(config)], ['tokenizer_config.json', json({})], ['config_sentence_transformers.json', json({})], ['1_Pooling/config.json', json({ pooling_mode_mean_tokens: true })], ['processor_config.json', new Blob([await readFile(new URL('../src/models/embedding_gemma2_processor.json', import.meta.url))])]]);
  const tokens = ['<pad>', '<eos>', '<bos>', '<unk>'];
  const added_tokens = tokens.slice(0, 3).map((content, id) => ({ id, content, special: true, normalized: false }));
  for (const [id, content] of [[255999, '<|image>'], [256000, '<|audio>'], [258880, '<|image|>'], [258881, '<|audio|>'], [258882, '<image|>'], [258883, '<audio|>']]) added_tokens.push({ id, content, special: true, normalized: false });
  files.set('tokenizer.json', json({ model: { type: 'WordPiece', unk_token: '<unk>', continuing_subword_prefix: '##', max_input_chars_per_word: 100, vocab: Object.fromEntries(tokens.map((t, i) => [t, i])) }, added_tokens, pre_tokenizer: { type: 'WhitespaceSplit' }, post_processor: { type: 'TemplateProcessing', single: [{ SpecialToken: { id: '<bos>', type_id: 0 } }, { Sequence: { id: 'A', type_id: 0 } }, { SpecialToken: { id: '<eos>', type_id: 0 } }], special_tokens: { '<bos>': { id: '<bos>', ids: [2], tokens: ['<bos>'] }, '<eos>': { id: '<eos>', ids: [1], tokens: ['<eos>'] } } } }));
  const header = {}; let offset = 0; const writes = [];
  const add = (name, shape, nonzero = []) => {
    const size = shape.reduce((a, b) => a * b, 1) * 2;
    header[name] = { dtype: 'BF16', shape, data_offsets: [offset, offset + size] };
    for (const index of nonzero) writes.push(offset + index * 2);
    offset += size;
  };
  add('language_model.embed_tokens.weight', [262144, 512], [0, 512, 1024, 1536]);
  add('language_model.embedding_projection.weight', [768, 512], [0]);
  add('language_model.ple.per_layer_model_projection.weight', [24 * 512, 512]);
  add('language_model.norm.weight', [512], Array.from({ length: 512 }, (_, i) => i)); add('language_model.ple.per_layer_projection_norm.weight', [512]);
  for (let layer = 0; layer < 24; layer++) {
    const d = (layer + 1) % 6 ? 256 : 512, kv = (layer + 1) % 6 ? 2 : 1, p = `language_model.layers.${layer}.`;
    for (const [name, shape] of [['self_attn.q_proj.weight', [4 * d, 512]], ['self_attn.k_proj.weight', [kv * d, 512]], ['self_attn.v_proj.weight', [kv * d, 512]], ['self_attn.o_proj.weight', [512, 4 * d]], ['mlp.gate_proj.weight', [2048, 512]], ['mlp.up_proj.weight', [2048, 512]], ['mlp.down_proj.weight', [512, 2048]], ['ple_block.per_layer_input_gate.weight', [512, 512]], ['ple_block.per_layer_projection.weight', [512, 512]]]) add(p + name, shape);
    for (const name of ['input_layernorm.weight', 'post_attention_layernorm.weight', 'pre_feedforward_layernorm.weight', 'post_feedforward_layernorm.weight', 'ple_block.post_per_layer_input_norm.weight', 'self_attn.q_norm.weight', 'self_attn.k_norm.weight', 'layer_scalar']) add(p + name, [name === 'layer_scalar' ? 1 : name.startsWith('self_attn') ? d : 512], name === 'layer_scalar' ? [0] : []);
  }
  const bytes = Buffer.from(JSON.stringify(header)), prefix = Buffer.alloc(8); prefix.writeBigUInt64LE(BigInt(bytes.length));
  const path = join(directory, 'model.safetensors'), file = await open(path, 'w');
  try { await file.write(prefix); await file.write(bytes); await file.truncate(8 + bytes.length + offset); for (const position of writes) await file.write(Buffer.from([0x80, 0x3f]), 0, 2, 8 + bytes.length + position); } finally { await file.close(); }
  files.set('model.safetensors', await openAsBlob(path)); return files;
}
test('EmbeddingGemma2 full geometry: native identity, cosine prototypes and multi-choice thresholds', { skip: !wasmPath, timeout: 600000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'antfly-web-embedding-'));
  const runtime = await session();
  try {
    const files = await embeddingFixture(directory);
    await runtime.load(files, 'bf16');
    const question = { name: 'tool', type: 'choice', choices: [{ value: 'search' }, { value: 'fetch' }], instructions: 'question', embedding_options: { min_margin: 0.1 } };
    const input = { model: 'local', input: 'state', questions: [question] };
    assert.equal(runtime.run(input, true, 'decide').value.valid, true);
    const before = runtime.wasm.extraction_live_bytes(runtime.handle);
    const result = runtime.run(input, false, 'decide').value;
    assert.equal(result.answers[0].decision_method, 'embedding_similarity');
    assert.equal(result.answers[0].status, 'abstained');
    assert(result.answers[0].similarities.every(s => Math.abs(s.similarity - 1) < 1e-6));
    const aggregate = createHash('sha256').update('embeddinggemma2-f32-mean-v1');
    for (const name of ['config.json', 'tokenizer.json', 'tokenizer_config.json', 'processor_config.json', 'config_sentence_transformers.json', '1_Pooling/config.json', 'model.safetensors']) {
      aggregate.update(name === 'model.safetensors' ? 'weights' : name).update(Buffer.from([0]));
      const hash = createHash('sha256'); for await (const chunk of files.get(name).stream()) hash.update(chunk); aggregate.update(hash.digest());
    }
    assert.equal(result.model_identity, aggregate.digest('hex'));
    assert.throws(() => runtime.run({ ...input, model_identity: '0'.repeat(64) }, true, 'decide'), /IdentityMismatch/);
    assert.throws(() => runtime.run({ ...input, questions: [{ ...question, type: 'multi_choice' }] }, true, 'decide'), /ThresholdRequired/);
    const multi = runtime.run({ ...input, questions: [{ ...question, type: 'multi_choice', embedding_options: {}, similarity_thresholds: { search: 0.5, fetch: 0.5 } }] }, false, 'decide').value;
    assert.deepEqual(multi.answers[0].choices, ['search', 'fetch']);
    assert.throws(() => runtime.run({ ...input, questions: [{ ...question, embedding_options: { calibration_id: 'missing' } }] }, true, 'decide'), /Calibration/);
    assert.throws(() => runtime.run({ ...input, questions: [{ name: 'score', type: 'score', levels: [{ label: 'low' }, { label: 'high' }], instructions: 'score' }] }, true, 'decide'), /UnsupportedEmbeddingDecisionKind/);
    assert.throws(() => runtime.run({ ...input, input: 'word '.repeat(3000) }, true, 'decide'), /Limit/);
    assert(runtime.wasm.extraction_live_bytes(runtime.handle) < before + 128 * 1024);
    const calibration = {
      version: 1, method: 'heldout-abstention-v1', qualified: true,
      binding: { model_identity: result.model_identity, renderer_version: result.renderer_version, task_type: 'CLUSTERING', dimensions: 768, prototype_set_hash: result.answers[0].prototype_set_hash, labels: ['search', 'fetch'], mode: 'single' },
      metrics: { fit: { count: 30, dataset_sha256: 'b'.repeat(64) }, validation: { count: 30, dataset_sha256: 'c'.repeat(64) }, holdout: { count: 100, selected: 100, correct: 100, dataset_sha256: 'd'.repeat(64) } },
      targets: { precision_lower_95: 0.9, minimum_coverage_or_f1: 0.8 },
      thresholds: { min_similarity: 0.5, min_margin: 0.1 },
    };
    files.set('calibrations/fixture.json', json(calibration));
    files.set('calibrations/unqualified.json', json({ ...calibration, qualified: false }));
    runtime.unload(); await runtime.load(files, 'bf16');
    const calibrated = { ...input, questions: [{ ...question, embedding_options: { calibration_id: 'fixture' } }] };
    assert.equal(runtime.run(calibrated, true, 'decide').value.valid, true);
    assert.equal(runtime.run(calibrated, false, 'decide').value.answers[0].calibration_id, 'fixture');
    assert.throws(() => runtime.run({ ...input, questions: [{ ...question, embedding_options: { calibration_id: 'unqualified' } }] }, true, 'decide'), /Unqualified/);
    assert.throws(() => runtime.run({ ...calibrated, questions: [{ ...calibrated.questions[0], instructions: 'changed' }] }, true, 'decide'), /CalibrationMismatch/);
    const examples = { ...input, questions: [{ ...question, choices: [{ value: 'search', examples: ['one', 'two'] }, { value: 'fetch', examples: ['three'] }] }] };
    assert.equal(runtime.run(examples, true, 'decide').value.valid, true);
    assert.equal(runtime.run(examples, false, 'decide').value.answers[0].status, 'abstained');
  } finally { runtime.unload(); await rm(directory, { recursive: true, force: true }); }
});

test('multilingual Laya vocabulary executes beyond the former 65536 limit', { skip: !wasmPath, timeout: 120000 }, async () => {
  const runtime = await session();
  try {
    await runtime.load(layaFixture({}, 'bf16', 262144), 'bf16');
    assert.equal(runtime.run(request, true, 'decide').value.valid, true);
    assert.equal(runtime.run(request, false, 'decide').value.answers[0].name, 'tool');
  } finally { runtime.unload(); }
});
