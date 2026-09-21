// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFile, readdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createWasmAbi } from './runtime/wasm-abi.js';
import { ExtractionSession } from './runtime/extraction-session.js';
import { tensorDirectory, normalizeFiles } from './runtime/extraction-bundle.js';

function safeTensor(shape = [1, 1], data = new Uint8Array(4)) {
  const header = new TextEncoder().encode(JSON.stringify({ 'test.weight': { dtype: 'F32', shape, data_offsets: [0, data.length] } }));
  const size = new ArrayBuffer(8); new DataView(size).setBigUint64(0, BigInt(header.length), true);
  return new Blob([size, header, data]);
}
test('SafeTensors directory validates exact bytes and shapes', async () => {
  const tensors = await tensorDirectory(safeTensor(), 'safetensors');
  assert.deepEqual(tensors[0].shape, [1, 1]);
  await assert.rejects(tensorDirectory(safeTensor([2, 2]), 'safetensors'), /byte length/);
  await assert.rejects(tensorDirectory(safeTensor([0]), 'safetensors'), /shape/);
  await assert.rejects(tensorDirectory(new Blob([new Uint8Array(4)]), 'gguf'));
});
test('folder roots preserve nested encoder sidecars and reject traversal', () => {
  const blob = new Blob(['{}']);
  assert(normalizeFiles(new Map([['bundle/config.json', blob], ['bundle/encoder_config/config.json', blob]])).has('encoder_config/config.json'));
  assert.throws(() => normalizeFiles(new Map([['../bad', blob]])), /Unsafe/);
});

const wasmPath = process.env.EXTRACTION_WASM;
test('real WASM ABI: staged loading, bounded errors and SHA256', { skip: !wasmPath }, async () => {
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const wasm = instance.exports, session = new ExtractionSession(wasm, createWasmAbi(wasm));
  assert.equal(wasm.extraction_abi_version(), 1);
  assert.equal(await session.hash(new Blob(['abc'])), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
  assert.throws(() => session.run({ schema_version: 2 }), /ModelNotLoaded/);
  session.unload(); session.unload();
});
test('local model extraction using the real WASM engine', { skip: !wasmPath || !process.env.EXTRACTION_MODEL, timeout: 240000 }, async () => {
  const directory = resolve(process.env.EXTRACTION_MODEL);
  const files = new Map();
  for (const path of await readdir(directory, { recursive: true })) {
    if (!/\.(json|gguf|safetensors)$/.test(path)) continue;
    files.set(path, new Blob([await readFile(resolve(directory, path))]));
  }
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const session = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  const model = await session.load(files, process.env.EXTRACTION_PRECISION);
  const request = model.architecture === 'span'
    ? { schema_version: 1, model: 'local', text: process.env.EXTRACTION_TEXT || 'John works at Apple.', labels: ['person', 'organization', 'location'] }
    : { schema_version: 2, model: 'local', inputs: [{ content: 'John works at Apple.' }], schema: { entities: ['person', 'organization'] }, options: { offset_unit: 'utf16_codeunits' } };
  const result = session.run(request);
  console.log(JSON.stringify({ model, ...result }));
  assert(result.value);
  if (model.architecture === 'boundary') {
    const unicode = session.run({ ...request, inputs: [{ content: '😀 John works at Apple.' }], options: { offset_unit: 'utf16_codeunits', include_spans: true } }).value.data[0];
    assert.equal(unicode.entities.find(e => e.text === 'John').start, 3, 'UTF16 presentation offsets count supplementary codepoints twice');
    const long = session.run({ ...request, inputs: [{ content: 'John works at Apple. '.repeat(20) }], options: { long_document: { mode: 'window', window_words: 32, overlap_words: 4, max_windows: 32 }, include_spans: true } }).value;
    assert(long.data[0].entities.length > 1, 'Long-document windows produce merged document spans');
    assert.throws(() => session.run({ ...request, inputs: [{ content: 'word '.repeat(65536) }] }), /Limit|TooLarge|Exceeded/);
  } else {
    assert.throws(() => session.run({ ...request, text: 'word '.repeat(600) }), /Tokens|TokenLimit/);
  }
  if (process.env.EXTRACTION_ORACLE && model.architecture === 'boundary') {
    const fixture = JSON.parse(await readFile(process.env.EXTRACTION_ORACLE, 'utf8'));
    for (const item of fixture.cases) {
      const output = session.run({ schema_version: 2, model: 'oracle', inputs: [{ content: item.text }], schema: item.schema, options: { include_confidence: true, include_spans: true, offset_unit: 'unicode_codepoints' } }).value.data[0];
      const actualEntities = (output.entities ?? []).map(e => `${e.label}:${e.text}:${e.start}:${e.end}`).sort();
      const expectedEntities = item.expected.entities.flatMap(group => group.values.map(e => `${group.name}:${e.text}:${e.source.start}:${e.source.end}`)).sort();
      assert.deepEqual(actualEntities, expectedEntities, `${item.id}: entity decisions and offsets`);
      const actualClasses = (output.classifications ?? []).map(c => `${c.name}:${c.label}`).sort();
      const expectedClasses = item.expected.classifications.flatMap(group => group.labels.map(c => `${group.name}:${c.label}`)).sort();
      assert.deepEqual(actualClasses, expectedClasses, `${item.id}: classification decisions`);
      for (const group of item.expected.structures) assert.equal(output.structures?.[group.name]?.length ?? 0, group.instances.length, `${item.id}: record count`);
      assert.equal(output.relations?.length ?? 0, item.expected.relations.length, `${item.id}: relation count`);
      console.log(`Oracle decisions matched: ${item.id}`);
    }
  }
  const cycles = Number(process.env.EXTRACTION_CYCLES || 1);
  const memories = [];
  for (let i = 1; i < cycles; i++) {
    session.unload();
    await session.load(files, process.env.EXTRACTION_PRECISION);
    memories.push(session.run(request).wasmBytes);
  }
  if (memories.length > 2) assert(memories.at(-1) <= memories[1] + 16 * 1024 ** 2, 'Repeated model lifecycle memory should plateau');
  if (cycles > 1) console.log({ cycles, memories });
  session.unload();
});
