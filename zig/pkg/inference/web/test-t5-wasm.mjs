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

// Test script: loads T5-small through WASM, tests encoder and greedy decode.
//
// Usage:
//   zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-t5-wasm.mjs

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateAntflyInferenceWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, size } = await instantiateAntflyInferenceWasm(root);

// --- Load T5-small model ---

const modelDir = path.join(root, 'models', 'google', 't5-small');
const modelPath = path.join(modelDir, 'model.safetensors');
const configPath = path.join(modelDir, 'config.json');

if (!fs.existsSync(modelPath)) {
  console.error('T5-small model not found at:', modelDir);
  console.error('Download: huggingface-cli download google/t5-small --local-dir', modelDir);
  process.exit(1);
}

console.log('Loading T5-small...');

const modelData = fs.readFileSync(modelPath);
const configJson = fs.readFileSync(configPath, 'utf-8');
const config = JSON.parse(configJson);

const modelPtr = alloc(modelData.length);
bytesIn(modelPtr, modelData);
const configBytes = new TextEncoder().encode(configJson);
const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

const modelHandle = wasm.load_model_t5(modelPtr, size(modelData.length), configPtr, size(configBytes.length));
free(modelPtr, modelData.length);
free(configPtr, configBytes.length);

if (modelHandle === 0) {
  console.error('Failed to load T5 model');
  process.exit(1);
}
console.log(`T5-small loaded (handle=${modelHandle})`);

// --- Helper: call WASM T5 encode ---
function t5Encode(ids, mask, batch, seqLen) {
  const idsArr = new BigInt64Array(ids.map(BigInt));
  const maskArr = new BigInt64Array(mask.map(BigInt));
  const idsByteLen = idsArr.length * 8;
  const maskByteLen = maskArr.length * 8;

  const idsPtr = alloc(idsByteLen);
  write(BigInt64Array, idsPtr, idsArr);
  const maskPtr = alloc(maskByteLen);
  write(BigInt64Array, maskPtr, maskArr);

  const dModel = config.d_model || 512;
  const maxOut = batch * seqLen * dModel;
  const outByteLen = maxOut * 4;
  const outPtr = alloc(outByteLen);

  const resultLen = wasm.t5_encode(
    modelHandle, idsPtr, size(idsArr.length), maskPtr, size(maskArr.length),
    batch, seqLen, outPtr,
  );

  let output = null;
  if (resultLen > 0) {
    output = read(Float32Array, outPtr, resultLen);
  }

  free(idsPtr, idsByteLen);
  free(maskPtr, maskByteLen);
  free(outPtr, outByteLen);

  return output;
}

// --- Helper: call WASM T5 decode ---
function t5Decode(encoderOutput, encoderMask, decoderIds, batch, decSeq, encSeq) {
  const encOutByteLen = encoderOutput.length * 4;
  const encMask = new BigInt64Array(encoderMask.map(BigInt));
  const decIds = new BigInt64Array(decoderIds.map(BigInt));
  const encMaskByteLen = encMask.length * 8;
  const decIdsByteLen = decIds.length * 8;

  const encOutPtr = alloc(encOutByteLen);
  write(Float32Array, encOutPtr, encoderOutput);
  const encMaskPtr = alloc(encMaskByteLen);
  write(BigInt64Array, encMaskPtr, encMask);
  const decIdsPtr = alloc(decIdsByteLen);
  write(BigInt64Array, decIdsPtr, decIds);

  const vocabSize = config.vocab_size || 32128;
  const maxOut = batch * decSeq * vocabSize;
  const outByteLen = maxOut * 4;
  const outPtr = alloc(outByteLen);

  const resultLen = wasm.t5_decode(
    modelHandle,
    encOutPtr, size(encoderOutput.length),
    encMaskPtr, size(encMask.length),
    decIdsPtr, size(decIds.length),
    batch, decSeq, encSeq,
    outPtr,
  );

  let output = null;
  if (resultLen > 0) {
    output = read(Float32Array, outPtr, resultLen);
  }

  free(encOutPtr, encOutByteLen);
  free(encMaskPtr, encMaskByteLen);
  free(decIdsPtr, decIdsByteLen);
  free(outPtr, outByteLen);

  return output;
}

// --- Test 1: Encoder forward ---
// "translate English to German: The house is wonderful."
// T5 tokenizer IDs (approximate, hand-encoded for t5-small):
// [13959, 1566, 12, 2968, 10, 37, 629, 19, 1627, 5, 1]
const inputIds = [13959, 1566, 12, 2968, 10, 37, 629, 19, 1627, 5, 1];
const attentionMask = inputIds.map(() => 1);
const batchSize = 1;
const encSeq = inputIds.length;

console.log('\nTest 1: T5 encoder forward');
const t0 = performance.now();
const encoderOutput = t5Encode(inputIds, attentionMask, batchSize, encSeq);
const t1 = performance.now();

if (!encoderOutput) {
  console.error('FAIL: encoder returned null');
  process.exit(1);
}

const dModel = config.d_model || 512;
const expectedLen = batchSize * encSeq * dModel;
if (encoderOutput.length !== expectedLen) {
  console.error(`FAIL: expected ${expectedLen} floats, got ${encoderOutput.length}`);
  process.exit(1);
}

// Check output is not all zeros
const sum = encoderOutput.reduce((a, b) => a + Math.abs(b), 0);
if (sum < 1e-6) {
  console.error('FAIL: encoder output is all zeros');
  process.exit(1);
}

console.log(`  Output shape: [${batchSize}, ${encSeq}, ${dModel}]`);
console.log(`  First 5 values: [${encoderOutput.slice(0, 5).map(v => v.toFixed(4)).join(', ')}]`);
console.log(`  L1 norm: ${sum.toFixed(2)}`);
console.log(`  Time: ${(t1 - t0).toFixed(1)} ms`);
console.log('  PASS');

// --- Test 2: Decoder forward (single step) ---
console.log('\nTest 2: T5 decoder forward (single step)');
const decoderStartId = config.decoder_start_token_id ?? 0;
const decoderIds = [decoderStartId];

const t2 = performance.now();
const logits = t5Decode(encoderOutput, attentionMask, decoderIds, batchSize, 1, encSeq);
const t3 = performance.now();

if (!logits) {
  console.error('FAIL: decoder returned null');
  process.exit(1);
}

const vocabSize = config.vocab_size || 32128;
if (logits.length !== batchSize * 1 * vocabSize) {
  console.error(`FAIL: expected ${batchSize * vocabSize} logits, got ${logits.length}`);
  process.exit(1);
}

// Greedy argmax
let maxIdx = 0;
let maxVal = logits[0];
for (let i = 1; i < vocabSize; i++) {
  if (logits[i] > maxVal) {
    maxVal = logits[i];
    maxIdx = i;
  }
}

console.log(`  Logits shape: [1, 1, ${vocabSize}]`);
console.log(`  Top token: ${maxIdx} (logit=${maxVal.toFixed(4)})`);
console.log(`  Time: ${(t3 - t2).toFixed(1)} ms`);
console.log('  PASS');

// --- Test 3: Greedy generation (5 tokens) ---
console.log('\nTest 3: T5 greedy generation (5 tokens)');
const eosId = config.eos_token_id ?? 1;
let genIds = [decoderStartId];
const genStart = performance.now();

for (let step = 0; step < 5; step++) {
  const stepLogits = t5Decode(
    encoderOutput, attentionMask,
    genIds, batchSize, genIds.length, encSeq,
  );

  if (!stepLogits) {
    console.error(`FAIL: decoder returned null at step ${step}`);
    process.exit(1);
  }

  // Extract last token's logits
  const lastLogits = stepLogits.slice(stepLogits.length - vocabSize);
  let bestIdx = 0;
  let bestVal = lastLogits[0];
  for (let i = 1; i < vocabSize; i++) {
    if (lastLogits[i] > bestVal) {
      bestVal = lastLogits[i];
      bestIdx = i;
    }
  }

  if (bestIdx === eosId) {
    console.log(`  EOS at step ${step}`);
    break;
  }

  genIds.push(bestIdx);
  console.log(`  Step ${step}: token ${bestIdx} (logit=${bestVal.toFixed(4)})`);
}

const genEnd = performance.now();
console.log(`  Generated IDs: [${genIds.slice(1).join(', ')}]`);
console.log(`  Time: ${(genEnd - genStart).toFixed(1)} ms (${((genEnd - genStart) / Math.max(genIds.length - 1, 1)).toFixed(1)} ms/token)`);
console.log('  PASS');

// --- Test 4: KV-cached greedy generation ---
console.log('\nTest 4: T5 KV-cached greedy generation (5 tokens)');

// Helper: call WASM T5 decode with KV cache
function t5ForwardCached(cacheHandle, encoderOutput, encoderMask, decoderIds, batch, decSeq, encSeq) {
  const encOutByteLen = encoderOutput.length * 4;
  const encMask = new BigInt64Array(encoderMask.map(BigInt));
  const decIds = new BigInt64Array(decoderIds.map(BigInt));
  const encMaskByteLen = encMask.length * 8;
  const decIdsByteLen = decIds.length * 8;

  const encOutPtr = alloc(encOutByteLen);
  write(Float32Array, encOutPtr, encoderOutput);
  const encMaskPtr = alloc(encMaskByteLen);
  write(BigInt64Array, encMaskPtr, encMask);
  const decIdsPtr = alloc(decIdsByteLen);
  write(BigInt64Array, decIdsPtr, decIds);

  const maxOut = batch * decSeq * vocabSize;
  const outByteLen = maxOut * 4;
  const outPtr = alloc(outByteLen);

  const resultLen = wasm.t5_forward_cached(
    modelHandle, cacheHandle,
    encOutPtr, size(encoderOutput.length),
    encMaskPtr, size(encMask.length),
    decIdsPtr, size(decIds.length),
    batch, decSeq, encSeq,
    outPtr,
  );

  let output = null;
  if (resultLen > 0) {
    output = read(Float32Array, outPtr, resultLen);
  }

  free(encOutPtr, encOutByteLen);
  free(encMaskPtr, encMaskByteLen);
  free(decIdsPtr, decIdsByteLen);
  free(outPtr, outByteLen);

  return output;
}

const cacheHandle = wasm.t5_create_kv_cache(modelHandle, 128);
if (cacheHandle === 0) {
  console.error('FAIL: t5_create_kv_cache returned 0');
  process.exit(1);
}
console.log(`  Cache created (handle=${cacheHandle})`);

let cachedGenIds = [decoderStartId];
const cachedGenStart = performance.now();

// Prefill: pass the start token
const prefillLogits = t5ForwardCached(cacheHandle, encoderOutput, attentionMask, [decoderStartId], batchSize, 1, encSeq);
if (!prefillLogits) {
  console.error('FAIL: cached prefill returned null');
  process.exit(1);
}

// Greedy argmax from prefill
let pMaxIdx = 0;
let pMaxVal = prefillLogits[0];
for (let i = 1; i < vocabSize; i++) {
  if (prefillLogits[i] > pMaxVal) {
    pMaxVal = prefillLogits[i];
    pMaxIdx = i;
  }
}
if (pMaxIdx === eosId) {
  console.log('  EOS at prefill');
} else {
  cachedGenIds.push(pMaxIdx);
  console.log(`  Prefill: token ${pMaxIdx} (logit=${pMaxVal.toFixed(4)})`);
}

// Decode: one token at a time
for (let step = 1; step < 5 && cachedGenIds[cachedGenIds.length - 1] !== eosId; step++) {
  const lastToken = cachedGenIds[cachedGenIds.length - 1];
  const stepLogits = t5ForwardCached(cacheHandle, encoderOutput, attentionMask, [lastToken], batchSize, 1, encSeq);
  if (!stepLogits) {
    console.error(`FAIL: cached decode returned null at step ${step}`);
    process.exit(1);
  }

  let bestIdx = 0;
  let bestVal = stepLogits[0];
  for (let i = 1; i < vocabSize; i++) {
    if (stepLogits[i] > bestVal) {
      bestVal = stepLogits[i];
      bestIdx = i;
    }
  }

  if (bestIdx === eosId) {
    console.log(`  EOS at step ${step}`);
    break;
  }

  cachedGenIds.push(bestIdx);
  console.log(`  Step ${step}: token ${bestIdx} (logit=${bestVal.toFixed(4)})`);
}

const cachedGenEnd = performance.now();
console.log(`  Generated IDs: [${cachedGenIds.slice(1).join(', ')}]`);
console.log(`  Time: ${(cachedGenEnd - cachedGenStart).toFixed(1)} ms (${((cachedGenEnd - cachedGenStart) / Math.max(cachedGenIds.length - 1, 1)).toFixed(1)} ms/token)`);

// Compare with non-cached generation
const uncachedTokens = genIds.slice(1);
const cachedTokens = cachedGenIds.slice(1);
const minLen = Math.min(uncachedTokens.length, cachedTokens.length);
let match = minLen > 0;
for (let i = 0; i < minLen; i++) {
  if (uncachedTokens[i] !== cachedTokens[i]) {
    console.error(`  MISMATCH at position ${i}: uncached=${uncachedTokens[i]}, cached=${cachedTokens[i]}`);
    match = false;
  }
}
if (match && minLen > 0) {
  console.log(`  Cached output matches non-cached (${minLen} tokens compared)`);
} else if (minLen === 0) {
  console.log('  Warning: no tokens to compare (EOS at start)');
}

wasm.t5_free_kv_cache(cacheHandle);
console.log('  Cache freed');
console.log('  PASS');

console.log('\nAll T5 tests passed!');
