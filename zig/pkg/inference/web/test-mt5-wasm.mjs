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

// Test script: loads mT5-small through WASM, tests encoder and greedy decode.
//
// Usage:
//   ~/bin/zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-mt5-wasm.mjs
//
// Requires:
//   models/google/mt5-small/ with model.safetensors, config.json

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateAntflyInferenceWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, size } = await instantiateAntflyInferenceWasm(root);

// --- Load mT5-small model ---

const modelDir = path.join(root, 'models', 'google', 'mt5-small');
const modelPath = path.join(modelDir, 'model.safetensors');
const configPath = path.join(modelDir, 'config.json');

if (!fs.existsSync(modelPath)) {
  console.error('mT5-small model not found at:', modelDir);
  console.error('Download: huggingface-cli download google/mt5-small --local-dir', modelDir);
  process.exit(1);
}

console.log('Loading mT5-small...');

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
  console.error('Failed to load mT5 model');
  process.exit(1);
}
console.log(`mT5-small loaded (handle=${modelHandle})`);
console.log(`  d_model=${config.d_model}, num_heads=${config.num_heads}, num_layers=${config.num_layers}`);
console.log(`  vocab_size=${config.vocab_size}, d_kv=${config.d_kv}, d_ff=${config.d_ff}`);

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

  const vocabSize = config.vocab_size || 250112;
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
// mT5 uses SentencePiece tokenizer. These are approximate IDs for
// "translate English to German: The house is wonderful." in mT5 vocab.
// mT5 has a 250112-token vocabulary covering 101 languages.
// For testing, we use simple token IDs that exist in any mT5 vocab.
const inputIds = [1, 259, 1513, 339, 3, 1];  // Simple tokens
const attentionMask = inputIds.map(() => 1);
const batchSize = 1;
const encSeq = inputIds.length;

console.log('\nTest 1: mT5 encoder forward');
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
console.log('\nTest 2: mT5 decoder forward (single step)');
const decoderStartId = config.decoder_start_token_id ?? 0;
const decoderIds = [decoderStartId];

const t2 = performance.now();
const logits = t5Decode(encoderOutput, attentionMask, decoderIds, batchSize, 1, encSeq);
const t3 = performance.now();

if (!logits) {
  console.error('FAIL: decoder returned null');
  process.exit(1);
}

const vocabSize = config.vocab_size || 250112;
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
console.log('\nTest 3: mT5 greedy generation (5 tokens)');
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
console.log(`  Time: ${(genEnd - genStart).toFixed(1)} ms`);
console.log('  PASS');

console.log('\nAll mT5 tests passed!');
