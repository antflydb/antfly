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

// Test script: loads Florence-2 model through WASM, tests encoder and decoder.
//
// Usage:
//   ~/bin/zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-florence-wasm.mjs
//
// Requires:
//   models/microsoft/Florence-2-base/model.safetensors
//   models/microsoft/Florence-2-base/config.json

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateTermiteWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, view, offset, size } = await instantiateTermiteWasm(root);

const modelDir = path.join(root, 'models', 'microsoft', 'Florence-2-base');
const modelPath = path.join(modelDir, 'model.safetensors');
const configPath = path.join(modelDir, 'config.json');

if (!fs.existsSync(modelPath)) {
  console.log('Florence-2 model not found at', modelDir);
  console.log('Download from HuggingFace: microsoft/Florence-2-base');
  process.exit(0);
}

// --- Test 1: Load Florence-2 model ---

console.log('Test 1: Load Florence-2 model');
const modelBytes = fs.readFileSync(modelPath);
const configJson = fs.readFileSync(configPath, 'utf-8');
const configBytes = new TextEncoder().encode(configJson);

const modelPtr = alloc(modelBytes.length);
bytesIn(modelPtr, modelBytes);
const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

const modelHandle = wasm.load_model_florence(
  modelPtr, size(modelBytes.length),
  configPtr, size(configBytes.length),
);
free(modelPtr, modelBytes.length);
free(configPtr, configBytes.length);

if (modelHandle === 0) {
  console.error('FAIL: Failed to load Florence-2 model');
  process.exit(1);
}
console.log(`  Model loaded (handle=${modelHandle})`);
console.log('  PASS');

// Parse config
const config = JSON.parse(configJson);
const d_model = config.text_config?.d_model || 768;
const vocab_size = config.text_config?.vocab_size || config.vocab_size || 51289;
const image_size = config.vision_config?.image_size || 768;

// --- Test 2: Encoder forward ---

console.log('\nTest 2: Encoder forward (vision + prompt)');
const batch = 1;

// Synthetic pixel values: [batch, 3, image_size, image_size] as CHW
const pixelLen = batch * 3 * image_size * image_size;
const pixels = new Float32Array(pixelLen);
for (let i = 0; i < pixelLen; i++) pixels[i] = (Math.random() - 0.5) * 2.0;

const pixelByteLen = pixels.length * 4;
const pixelPtr = alloc(pixelByteLen);
write(Float32Array, pixelPtr, pixels);

// Prompt: "<OCR>" — use simple token IDs
const promptSeqLen = 4;
const promptIds = new BigInt64Array([0n, 10n, 20n, 2n]); // BOS, some tokens, EOS
const promptIdsByteLen = promptIds.length * 8;
const promptIdsPtr = alloc(promptIdsByteLen);
write(BigInt64Array, promptIdsPtr, promptIds);

// Output buffer — generous size
const maxEncOut = batch * 2048 * d_model; // upper bound
const encOutByteLen = maxEncOut * 4;
const encOutPtr = alloc(encOutByteLen);

// enc_seq output
const encSeqPtr = alloc(4);

console.log(`  Running encoder (image: [${batch}, 3, ${image_size}, ${image_size}], prompt: ${promptSeqLen} tokens)...`);
const et0 = performance.now();
const encResultLen = wasm.florence_encode(
  modelHandle,
  pixelPtr, size(pixels.length),
  promptIdsPtr, size(promptIds.length),
  batch,
  encOutPtr,
  encSeqPtr,
);
const encElapsed = (performance.now() - et0).toFixed(1);

let encHidden = null;
let encSeq = 0;
if (encResultLen > 0) {
  encHidden = read(Float32Array, encOutPtr, encResultLen);
  encSeq = read(Uint32Array, encSeqPtr, 1)[0];
}

free(pixelPtr, pixelByteLen);
free(promptIdsPtr, promptIdsByteLen);
free(encOutPtr, encOutByteLen);
free(encSeqPtr, 4);

if (!encHidden) {
  console.error('FAIL: Encoder returned 0');
  process.exit(1);
}

console.log(`  Time: ${encElapsed}ms`);
console.log(`  Encoder seq_len: ${encSeq}`);
console.log(`  Output length: ${encHidden.length} (expected ${batch * encSeq * d_model})`);
console.log(`  First 5 values: [${Array.from(encHidden.slice(0, 5)).map(v => v.toFixed(6)).join(', ')}]`);

const encAllFinite = encHidden.every(v => isFinite(v));
console.log(`  All finite: ${encAllFinite}`);
if (!encAllFinite) { console.error('FAIL: non-finite encoder outputs'); process.exit(1); }
if (encHidden.length !== batch * encSeq * d_model) {
  console.error(`FAIL: expected ${batch * encSeq * d_model} floats, got ${encHidden.length}`);
  process.exit(1);
}
console.log('  PASS');

// --- Test 3: Decoder forward ---

console.log('\nTest 3: Decoder forward');
const decSeq = 1;
const decoderStartTokenId = config.text_config?.decoder_start_token_id || config.bos_token_id || 2;

const decIds = new BigInt64Array([BigInt(decoderStartTokenId)]);
const decIdsByteLen = decIds.length * 8;
const decIdsPtr = alloc(decIdsByteLen);
write(BigInt64Array, decIdsPtr, decIds);

// Re-upload encoder hidden
const encByteLen = encHidden.length * 4;
const encPtr = alloc(encByteLen);
write(Float32Array, encPtr, encHidden);

// Encoder mask: all ones
const encMask = new BigInt64Array(batch * encSeq);
for (let i = 0; i < encMask.length; i++) encMask[i] = 1n;
const encMaskByteLen = encMask.length * 8;
const encMaskPtr = alloc(encMaskByteLen);
write(BigInt64Array, encMaskPtr, encMask);

// Output: [batch, dec_seq, vocab_size] logits
const maxDecOut = batch * decSeq * vocab_size;
const decOutByteLen = maxDecOut * 4;
const decOutPtr = alloc(decOutByteLen);

console.log(`  Running decoder (dec_seq=${decSeq}, enc_seq=${encSeq})...`);
const dt0 = performance.now();
const decResultLen = wasm.florence_decode(
  modelHandle,
  decIdsPtr, size(decIds.length),
  encPtr, size(encHidden.length),
  encMaskPtr, size(encMask.length),
  batch, decSeq, encSeq,
  decOutPtr,
);
const decElapsed = (performance.now() - dt0).toFixed(1);

let logits = null;
if (decResultLen > 0) {
  logits = read(Float32Array, decOutPtr, decResultLen);
}

free(decIdsPtr, decIdsByteLen);
free(encPtr, encByteLen);
free(encMaskPtr, encMaskByteLen);
free(decOutPtr, decOutByteLen);

if (!logits) {
  console.error('FAIL: Decoder returned 0');
  process.exit(1);
}

console.log(`  Time: ${decElapsed}ms`);
console.log(`  Output length: ${logits.length} (expected ${batch * decSeq * vocab_size})`);
console.log(`  First 5 logit values: [${Array.from(logits.slice(0, 5)).map(v => v.toFixed(4)).join(', ')}]`);

const decAllFinite = logits.every(v => isFinite(v));
console.log(`  All finite: ${decAllFinite}`);
if (!decAllFinite) { console.error('FAIL: non-finite logits'); process.exit(1); }

// Find argmax token
let maxVal = logits[0];
let maxIdx = 0;
for (let i = 1; i < logits.length; i++) {
  if (logits[i] > maxVal) { maxVal = logits[i]; maxIdx = i; }
}
console.log(`  Argmax token: ${maxIdx} (logit=${maxVal.toFixed(4)})`);
console.log('  PASS');

// --- Test 4: Greedy decode loop (5 steps) ---

console.log('\nTest 4: Greedy decode loop (5 steps)');
const MAX_STEPS = 5;
const tokens = [decoderStartTokenId];
const eosTokenId = config.eos_token_id || 2;

// Re-upload encoder hidden for the loop
const encPtr2 = alloc(encByteLen);
write(Float32Array, encPtr2, encHidden);

for (let step = 0; step < MAX_STEPS; step++) {
  const stepDecSeq = tokens.length;
  const stepIds = new BigInt64Array(tokens.map(BigInt));
  const stepIdsByteLen = stepIds.length * 8;
  const stepIdsPtr = alloc(stepIdsByteLen);
  write(BigInt64Array, stepIdsPtr, stepIds);

  const stepEncMaskPtr = alloc(encMaskByteLen);
  write(BigInt64Array, stepEncMaskPtr, encMask);

  const stepOutLen = batch * stepDecSeq * vocab_size;
  const stepOutByteLen = stepOutLen * 4;
  const stepOutPtr = alloc(stepOutByteLen);

  const stepResultLen = wasm.florence_decode(
    modelHandle,
    stepIdsPtr, size(stepIds.length),
    encPtr2, size(encHidden.length),
    stepEncMaskPtr, size(encMask.length),
    batch, stepDecSeq, encSeq,
    stepOutPtr,
  );

  let stepLogits = null;
  if (stepResultLen > 0) {
    const lastTokenOffset = (stepDecSeq - 1) * vocab_size;
    stepLogits = new Float32Array(vocab_size);
    stepLogits.set(view(Float32Array, offset(stepOutPtr) + lastTokenOffset * 4, vocab_size));
  }

  free(stepIdsPtr, stepIdsByteLen);
  free(stepEncMaskPtr, encMaskByteLen);
  free(stepOutPtr, stepOutByteLen);

  if (!stepLogits) {
    console.error(`  FAIL: Decode step ${step} returned 0`);
    process.exit(1);
  }

  // Argmax
  let sMaxVal = stepLogits[0];
  let sMaxIdx = 0;
  for (let i = 1; i < vocab_size; i++) {
    if (stepLogits[i] > sMaxVal) { sMaxVal = stepLogits[i]; sMaxIdx = i; }
  }
  tokens.push(sMaxIdx);
  console.log(`  Step ${step}: token=${sMaxIdx} (logit=${sMaxVal.toFixed(4)})`);

  if (sMaxIdx === eosTokenId) {
    console.log('  Reached EOS');
    break;
  }
}

free(encPtr2, encByteLen);

console.log(`  Generated tokens: [${tokens.join(', ')}]`);
const allValid = tokens.every(t => t >= 0 && t < vocab_size);
console.log(`  All valid token IDs: ${allValid}`);
if (!allValid) { console.error('FAIL: invalid token IDs'); process.exit(1); }
console.log('  PASS');

// Cleanup
wasm.unload_model(modelHandle);
console.log('\nAll Florence-2 tests passed!');
