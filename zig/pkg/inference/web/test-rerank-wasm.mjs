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

// Test script: loads tokenizer + model through WASM, tests tokenization and reranking.
//
// Usage:
//   zig build -Dwasm=true wasm
//   node web/test-rerank-wasm.mjs

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateAntflyInferenceWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, size } = await instantiateAntflyInferenceWasm(root);

// --- Test 1: Load tokenizer ---

const modelDir = path.join(root, 'models', 'BAAI', 'bge-small-en-v1.5');
const tokenizerPath = path.join(modelDir, 'tokenizer.json');

if (!fs.existsSync(tokenizerPath)) {
  console.error('Tokenizer not found. Run: antfly inference pull BAAI/bge-small-en-v1.5:native');
  process.exit(1);
}

console.log('Loading tokenizer...');
const tokenizerJson = fs.readFileSync(tokenizerPath);
const tokPtr = alloc(tokenizerJson.length);
bytesIn(tokPtr, tokenizerJson);
const tokHandle = wasm.load_tokenizer(tokPtr, size(tokenizerJson.length));
free(tokPtr, tokenizerJson.length);

if (tokHandle === 0) {
  console.error('Failed to load tokenizer');
  process.exit(1);
}
console.log(`Tokenizer loaded (handle=${tokHandle})`);

// --- Test 2: Single text tokenization ---

console.log('\nTest: single text tokenization');
const text = 'this is a test sentence';
const textBytes = new TextEncoder().encode(text);
const maxLen = 16;

const textPtr = alloc(textBytes.length);
bytesIn(textPtr, textBytes);

const idsOutLen = maxLen * 4;
const idsPtr = alloc(idsOutLen);
const maskPtr = alloc(idsOutLen);

const tokResult = wasm.tokenize(tokHandle, textPtr, size(textBytes.length), maxLen, idsPtr, maskPtr);
const ids = read(Int32Array, idsPtr, maxLen);
const mask = read(Int32Array, maskPtr, maxLen);

free(textPtr, textBytes.length);
free(idsPtr, idsOutLen);
free(maskPtr, idsOutLen);

console.log(`  Result length: ${tokResult}`);
console.log(`  IDs: [${Array.from(ids).join(', ')}]`);
console.log(`  Mask: [${Array.from(mask).join(', ')}]`);

// Verify structure: starts with CLS (101), ends active part with SEP (102)
const activeLen = mask.reduce((s, v) => s + v, 0);
console.log(`  Active tokens: ${activeLen}`);
console.log(`  First token (CLS): ${ids[0]} (expected 101)`);
console.log(`  Last active token (SEP): ${ids[activeLen - 1]} (expected 102)`);

if (ids[0] !== 101) { console.error('FAIL: missing CLS token'); process.exit(1); }
if (ids[activeLen - 1] !== 102) { console.error('FAIL: missing SEP token'); process.exit(1); }
console.log('  PASS');

// --- Test 3: Pair tokenization ---

console.log('\nTest: pair tokenization');
const queryText = 'search query';
const docText = 'this is a relevant document';
const queryBytes = new TextEncoder().encode(queryText);
const docBytes = new TextEncoder().encode(docText);

const qPtr = alloc(queryBytes.length);
const dPtr = alloc(docBytes.length);
bytesIn(qPtr, queryBytes);
bytesIn(dPtr, docBytes);

const pairIdsPtr = alloc(idsOutLen);
const pairMaskPtr = alloc(idsOutLen);

const pairResult = wasm.tokenize_pair(
  tokHandle,
  qPtr, size(queryBytes.length),
  dPtr, size(docBytes.length),
  maxLen, pairIdsPtr, pairMaskPtr,
);

const pairIds = read(Int32Array, pairIdsPtr, maxLen);
const pairMask = read(Int32Array, pairMaskPtr, maxLen);

free(qPtr, queryBytes.length);
free(dPtr, docBytes.length);
free(pairIdsPtr, idsOutLen);
free(pairMaskPtr, idsOutLen);

console.log(`  Result length: ${pairResult}`);
console.log(`  IDs: [${Array.from(pairIds).join(', ')}]`);
console.log(`  Mask: [${Array.from(pairMask).join(', ')}]`);

// Verify: [CLS] query_tokens [SEP] doc_tokens [SEP] [PAD...]
const pairActiveLen = pairMask.reduce((s, v) => s + v, 0);
console.log(`  Active tokens: ${pairActiveLen}`);
console.log(`  First token (CLS): ${pairIds[0]} (expected 101)`);

// Count SEP tokens — should be exactly 2
let sepCount = 0;
for (let i = 0; i < pairActiveLen; i++) {
  if (pairIds[i] === 102) sepCount++;
}
console.log(`  SEP tokens found: ${sepCount} (expected 2)`);

if (pairIds[0] !== 101) { console.error('FAIL: missing CLS token'); process.exit(1); }
if (sepCount !== 2) { console.error('FAIL: expected 2 SEP tokens'); process.exit(1); }
console.log('  PASS');

// --- Test 4: Reranking (smoke test with embedding model) ---

const modelPath = path.join(modelDir, 'model.safetensors');
const configPath = path.join(modelDir, 'config.json');

if (!fs.existsSync(modelPath)) {
  console.log('\nSkipping rerank test (model.safetensors not found)');
  wasm.unload_tokenizer(tokHandle);
  console.log('\nTokenizer tests passed!');
  process.exit(0);
}

console.log('\nTest: reranking smoke test');
console.log('Loading model...');
const modelBytes = fs.readFileSync(modelPath);
const configBytes = new TextEncoder().encode(fs.readFileSync(configPath, 'utf-8'));

const modelPtr = alloc(modelBytes.length);
bytesIn(modelPtr, modelBytes);
const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

const modelHandle = wasm.load_model_safetensors(
  modelPtr, size(modelBytes.length),
  configPtr, size(configBytes.length),
);
free(modelPtr, modelBytes.length);
free(configPtr, configBytes.length);

if (modelHandle === 0) {
  console.error('Failed to load model');
  process.exit(1);
}
console.log(`  Model loaded (handle=${modelHandle})`);

// Tokenize 2 query-doc pairs
const query = 'machine learning';
const docs = [
  'deep learning is a subset of machine learning',
  'the weather is nice today',
];
const rerankMaxLen = 32;
const batchSize = docs.length;

const allIds = new BigInt64Array(batchSize * rerankMaxLen);
const allMask = new BigInt64Array(batchSize * rerankMaxLen);

for (let i = 0; i < docs.length; i++) {
  const qB = new TextEncoder().encode(query);
  const dB = new TextEncoder().encode(docs[i]);
  const q2 = alloc(qB.length);
  const d2 = alloc(dB.length);
  bytesIn(q2, qB);
  bytesIn(d2, dB);

  const pIds = alloc(rerankMaxLen * 4);
  const pMask = alloc(rerankMaxLen * 4);

  wasm.tokenize_pair(tokHandle, q2, size(qB.length), d2, size(dB.length), rerankMaxLen, pIds, pMask);

  const tmpIds = read(Int32Array, pIds, rerankMaxLen);
  const tmpMask = read(Int32Array, pMask, rerankMaxLen);

  for (let j = 0; j < rerankMaxLen; j++) {
    allIds[i * rerankMaxLen + j] = BigInt(tmpIds[j]);
    allMask[i * rerankMaxLen + j] = BigInt(tmpMask[j]);
  }

  free(q2, qB.length);
  free(d2, dB.length);
  free(pIds, rerankMaxLen * 4);
  free(pMask, rerankMaxLen * 4);
}

// Run reranking
const idsRankByteLen = allIds.length * 8;
const maskRankByteLen = allMask.length * 8;
const idsRankPtr = alloc(idsRankByteLen);
const maskRankPtr = alloc(maskRankByteLen);
write(BigInt64Array, idsRankPtr, allIds);
write(BigInt64Array, maskRankPtr, allMask);

const scoresOutLen = batchSize * 4;
const scoresPtr = alloc(scoresOutLen);

console.log('  Running rerank...');
const t0 = performance.now();
const numScores = wasm.rerank(
  modelHandle,
  idsRankPtr, size(allIds.length),
  maskRankPtr, size(allMask.length),
  batchSize, rerankMaxLen, 1,
  scoresPtr,
);
const elapsed = (performance.now() - t0).toFixed(1);

const scores = read(Float32Array, scoresPtr, numScores);

free(idsRankPtr, idsRankByteLen);
free(maskRankPtr, maskRankByteLen);
free(scoresPtr, scoresOutLen);

console.log(`  Rerank time: ${elapsed}ms`);
console.log(`  Scores: [${Array.from(scores).map(v => v.toFixed(6)).join(', ')}]`);

// Verify scores are finite
const allFinite = scores.every(v => isFinite(v));
console.log(`  All finite: ${allFinite}`);
if (!allFinite) { console.error('FAIL: non-finite scores'); process.exit(1); }
if (numScores !== batchSize) { console.error(`FAIL: expected ${batchSize} scores, got ${numScores}`); process.exit(1); }

// Verify scores are in (0, 1) range (sigmoid output)
const allInRange = scores.every(v => v > 0 && v < 1);
console.log(`  All in (0,1): ${allInRange}`);
if (!allInRange) { console.error('FAIL: scores out of sigmoid range'); process.exit(1); }
console.log('  PASS');

// --- Test 5: Relative ordering ---

console.log('\nTest: relative ordering');
const orderQuery = 'artificial intelligence research';
const orderDocs = [
  'deep learning and neural networks are key areas of AI research',
  'the recipe calls for two cups of flour and one egg',
  'machine learning models can classify images with high accuracy',
];
const orderMaxLen = 64;
const orderBatch = orderDocs.length;

const orderAllIds = new BigInt64Array(orderBatch * orderMaxLen);
const orderAllMask = new BigInt64Array(orderBatch * orderMaxLen);

for (let i = 0; i < orderDocs.length; i++) {
  const qB = new TextEncoder().encode(orderQuery);
  const dB = new TextEncoder().encode(orderDocs[i]);
  const q2 = alloc(qB.length);
  const d2 = alloc(dB.length);
  bytesIn(q2, qB);
  bytesIn(d2, dB);
  const pIds2 = alloc(orderMaxLen * 4);
  const pMask2 = alloc(orderMaxLen * 4);
  wasm.tokenize_pair(tokHandle, q2, size(qB.length), d2, size(dB.length), orderMaxLen, pIds2, pMask2);
  const tmpIds2 = read(Int32Array, pIds2, orderMaxLen);
  const tmpMask2 = read(Int32Array, pMask2, orderMaxLen);
  for (let j = 0; j < orderMaxLen; j++) {
    orderAllIds[i * orderMaxLen + j] = BigInt(tmpIds2[j]);
    orderAllMask[i * orderMaxLen + j] = BigInt(tmpMask2[j]);
  }
  free(q2, qB.length);
  free(d2, dB.length);
  free(pIds2, orderMaxLen * 4);
  free(pMask2, orderMaxLen * 4);
}

const orderIdsByteLen = orderAllIds.length * 8;
const orderMaskByteLen = orderAllMask.length * 8;
const orderIdsPtr = alloc(orderIdsByteLen);
const orderMaskPtr = alloc(orderMaskByteLen);
write(BigInt64Array, orderIdsPtr, orderAllIds);
write(BigInt64Array, orderMaskPtr, orderAllMask);
const orderScoresPtr = alloc(orderBatch * 4);

const orderT0 = performance.now();
const orderNumScores = wasm.rerank(
  modelHandle, orderIdsPtr, size(orderAllIds.length), orderMaskPtr, size(orderAllMask.length),
  orderBatch, orderMaxLen, 1, orderScoresPtr,
);
const orderElapsed = (performance.now() - orderT0).toFixed(1);

const orderScores = read(Float32Array, orderScoresPtr, orderNumScores);

free(orderIdsPtr, orderIdsByteLen);
free(orderMaskPtr, orderMaskByteLen);
free(orderScoresPtr, orderBatch * 4);

console.log(`  Rerank time: ${orderElapsed}ms`);
for (let i = 0; i < orderDocs.length; i++) {
  console.log(`  [${i}] ${orderScores[i].toFixed(6)} — "${orderDocs[i].slice(0, 50)}..."`);
}

// The irrelevant doc (recipe) should have a different score than the AI docs
// (We can't guarantee strict ordering with an embedding model, but verify basic sanity)
if (orderNumScores !== orderBatch) {
  console.error(`FAIL: expected ${orderBatch} scores, got ${orderNumScores}`);
  process.exit(1);
}
const orderAllFinite = orderScores.every(v => isFinite(v) && v > 0 && v < 1);
if (!orderAllFinite) {
  console.error('FAIL: scores not in valid range');
  process.exit(1);
}
console.log('  PASS');

// --- Test 6: Single document ---

console.log('\nTest: single document reranking');
const singleQuery = 'test';
const singleDoc = 'this is a test document';
const singleMaxLen = 32;

const sqB = new TextEncoder().encode(singleQuery);
const sdB = new TextEncoder().encode(singleDoc);
const sq = alloc(sqB.length);
const sd = alloc(sdB.length);
bytesIn(sq, sqB);
bytesIn(sd, sdB);
const spIds = alloc(singleMaxLen * 4);
const spMask = alloc(singleMaxLen * 4);
wasm.tokenize_pair(tokHandle, sq, size(sqB.length), sd, size(sdB.length), singleMaxLen, spIds, spMask);
const sIds = new BigInt64Array(singleMaxLen);
const sMask = new BigInt64Array(singleMaxLen);
const sTmpIds = read(Int32Array, spIds, singleMaxLen);
const sTmpMask = read(Int32Array, spMask, singleMaxLen);
for (let j = 0; j < singleMaxLen; j++) {
  sIds[j] = BigInt(sTmpIds[j]);
  sMask[j] = BigInt(sTmpMask[j]);
}
free(sq, sqB.length);
free(sd, sdB.length);
free(spIds, singleMaxLen * 4);
free(spMask, singleMaxLen * 4);

const sIdsByteLen = sIds.length * 8;
const sMaskByteLen = sMask.length * 8;
const sIdsPtr = alloc(sIdsByteLen);
const sMaskPtr = alloc(sMaskByteLen);
write(BigInt64Array, sIdsPtr, sIds);
write(BigInt64Array, sMaskPtr, sMask);
const sScoresPtr = alloc(4);

const sNum = wasm.rerank(modelHandle, sIdsPtr, size(sIds.length), sMaskPtr, size(sMask.length), 1, singleMaxLen, 1, sScoresPtr);
const sScore = read(Float32Array, sScoresPtr, 1);

free(sIdsPtr, sIdsByteLen);
free(sMaskPtr, sMaskByteLen);
free(sScoresPtr, 4);

console.log(`  Score: ${sScore[0].toFixed(6)}`);
if (sNum !== 1 || !isFinite(sScore[0]) || sScore[0] <= 0 || sScore[0] >= 1) {
  console.error('FAIL: single document reranking failed');
  process.exit(1);
}
console.log('  PASS');

// Cleanup
wasm.unload_model(modelHandle);
wasm.unload_tokenizer(tokHandle);
console.log('\nAll tests passed!');
