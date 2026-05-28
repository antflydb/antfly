#!/usr/bin/env node
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

// Test script: loads bge-small-en-v1.5 SafeTensors model through WASM and runs embedding.
//
// Usage:
//   zig build -Dwasm=true wasm
//   node web/test-wasm.mjs

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateTermiteWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, abi, alloc, free, bytesIn, write, read, size } = await instantiateTermiteWasm(root);

// Load model
const modelPath = path.join(root, 'models', 'BAAI', 'bge-small-en-v1.5', 'model.safetensors');
const configPath = path.join(root, 'models', 'BAAI', 'bge-small-en-v1.5', 'config.json');
const tokenizerPath = path.join(root, 'models', 'BAAI', 'bge-small-en-v1.5', 'tokenizer.json');

if (!fs.existsSync(modelPath)) {
  console.error('Model not found. Run: termite pull BAAI/bge-small-en-v1.5:native');
  process.exit(1);
}

console.log('Loading model...');
const modelBytes = fs.readFileSync(modelPath);
const configBytes = new TextEncoder().encode(fs.readFileSync(configPath, 'utf-8'));

// Allocate and copy model data
const modelPtr = alloc(modelBytes.length);
bytesIn(modelPtr, modelBytes);

const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

const t0 = performance.now();
const handle = wasm.load_model_safetensors(
  modelPtr, size(modelBytes.length),
  configPtr, size(configBytes.length),
);
const loadMs = (performance.now() - t0).toFixed(0);

free(modelPtr, modelBytes.length);
free(configPtr, configBytes.length);

if (handle === 0) {
  console.error('Failed to load model');
  process.exit(1);
}
console.log(`Model loaded (handle=${handle}) in ${loadMs}ms`);

// Run embedding: "[CLS] this is a test sentence . [SEP]"
const tokenIds = [101, 2023, 2003, 1037, 3231, 6251, 1012, 102];
const mask = [1, 1, 1, 1, 1, 1, 1, 1];
const batchSize = 1;
const seqLen = tokenIds.length;

const ids = new BigInt64Array(tokenIds.map(BigInt));
const maskArr = new BigInt64Array(mask.map(BigInt));

const idsByteLen = ids.length * 8;
const maskByteLen = maskArr.length * 8;

const idsPtr = alloc(idsByteLen);
const maskPtr = alloc(maskByteLen);
write(BigInt64Array, idsPtr, ids);
write(BigInt64Array, maskPtr, maskArr);

// Output buffer: batch * hidden_dim (384 for bge-small)
const maxOutputFloats = batchSize * 4096;
const outByteLen = maxOutputFloats * 4;
const outPtr = alloc(outByteLen);

console.log('Running embedding...');
const t1 = performance.now();
const resultLen = wasm.embed(handle, idsPtr, size(ids.length), maskPtr, size(maskArr.length), batchSize, seqLen, outPtr);
const embedMs = (performance.now() - t1).toFixed(1);

if (resultLen === 0) {
  console.error('Embedding failed');
  process.exit(1);
}

const result = read(Float32Array, outPtr, resultLen);

free(idsPtr, idsByteLen);
free(maskPtr, maskByteLen);
free(outPtr, outByteLen);

// Report results
const dim = resultLen / (batchSize * seqLen);
const clsEmbedding = result.slice(0, dim);
const norm = Math.sqrt(clsEmbedding.reduce((s, v) => s + v * v, 0));

console.log(`\nResults:`);
console.log(`  Output shape: [${batchSize}, ${seqLen}, ${dim}] (${resultLen} floats)`);
console.log(`  Embed time: ${embedMs}ms`);
console.log(`  CLS embedding L2 norm: ${norm.toFixed(4)}`);
console.log(`  CLS first 8 values: [${Array.from(clsEmbedding.slice(0, 8)).map(v => v.toFixed(6)).join(', ')}]`);

// Sanity checks
const allFinite = result.every(v => isFinite(v));
const maxAbs = result.reduce((m, v) => Math.max(m, Math.abs(v)), 0);
console.log(`  All finite: ${allFinite}`);
console.log(`  Max |value|: ${maxAbs.toFixed(4)}`);

wasm.unload_model(handle);
console.log('\nModel unloaded.');

if (fs.existsSync(tokenizerPath)) {
  console.log('Loading tokenizer...');
  const tokenizerBytes = new TextEncoder().encode(fs.readFileSync(tokenizerPath, 'utf-8'));
  const tokPtr = alloc(tokenizerBytes.length);
  bytesIn(tokPtr, tokenizerBytes);
  const tokHandle = wasm.load_tokenizer(tokPtr, size(tokenizerBytes.length));
  free(tokPtr, tokenizerBytes.length);

  if (tokHandle === 0) {
    console.error('Failed to load tokenizer');
    process.exit(1);
  }

  const rawText = 'machine learning';
  const textBytes = new TextEncoder().encode(rawText);
  const textPtr = alloc(textBytes.length);
  bytesIn(textPtr, textBytes);
  const idsPtr2 = alloc(64 * 4);
  const rawCount = wasm.tokenize_raw(tokHandle, textPtr, size(textBytes.length), idsPtr2, 64);
  free(textPtr, textBytes.length);
  if (rawCount === 0) {
    console.error('Raw tokenization failed');
    process.exit(1);
  }
  const rawIds = read(Int32Array, idsPtr2, rawCount);
  free(idsPtr2, 64 * 4);

  const decodeIdsPtr = alloc(rawIds.length * 4);
  write(Int32Array, decodeIdsPtr, rawIds);
  const outLenPtr = alloc(4);
  const textOutPtr = wasm.decode_tokens(tokHandle, decodeIdsPtr, size(rawIds.length), outLenPtr);
  const textOutLen = read(Uint32Array, outLenPtr, 1)[0];
  free(outLenPtr, 4);
  free(decodeIdsPtr, rawIds.length * 4);

  if (!textOutPtr || textOutLen === 0) {
    console.error('Token decode failed');
    process.exit(1);
  }

  const decodedText = new TextDecoder().decode(read(Uint8Array, textOutPtr, textOutLen));
  free(textOutPtr, textOutLen);
  wasm.unload_tokenizer(tokHandle);

  console.log(`Decoded text: ${JSON.stringify(decodedText)}`);
  if (decodedText.trim().length === 0) {
    console.error('Decoded text was empty');
    process.exit(1);
  }
}

console.log('Test passed!');
