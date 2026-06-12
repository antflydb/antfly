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

// Test script: loads Whisper model through WASM, tests encoder and decoder.
//
// Usage:
//   ~/bin/zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-whisper-wasm.mjs
//
// Requires:
//   models/openai/whisper-tiny/model.safetensors
//   models/openai/whisper-tiny/config.json
//   models/openai/whisper-tiny/tokenizer.json

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateAntflyInferenceWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, view, offset, size } = await instantiateAntflyInferenceWasm(root);

const modelDir = path.join(root, 'models', 'openai', 'whisper-tiny');
const modelPath = path.join(modelDir, 'model.safetensors');
const configPath = path.join(modelDir, 'config.json');
const tokenizerPath = path.join(modelDir, 'tokenizer.json');

if (!fs.existsSync(modelPath)) {
  console.log('Whisper model not found at', modelDir);
  console.log('Download with: python3 -c "from transformers import WhisperModel; ..."');
  process.exit(0);
}

// --- Test 1: Load Whisper model ---

console.log('Test 1: Load Whisper model');
const modelBytes = fs.readFileSync(modelPath);
const configJson = fs.readFileSync(configPath, 'utf-8');
const configBytes = new TextEncoder().encode(configJson);

const modelPtr = alloc(modelBytes.length);
bytesIn(modelPtr, modelBytes);
const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

const modelHandle = wasm.load_model_whisper(
  modelPtr, size(modelBytes.length),
  configPtr, size(configBytes.length),
);
free(modelPtr, modelBytes.length);
free(configPtr, configBytes.length);

if (modelHandle === 0) {
  console.error('FAIL: Failed to load Whisper model');
  process.exit(1);
}
console.log(`  Model loaded (handle=${modelHandle})`);
console.log('  PASS');

// Parse config
const config = JSON.parse(configJson);
const d_model = config.d_model || 384;
const num_mel_bins = config.num_mel_bins || 80;
const max_source_positions = config.max_source_positions || 1500;
const vocab_size = config.vocab_size || 51865;

// --- Test 2: Encoder forward with synthetic mel ---

console.log('\nTest 2: Encoder forward (synthetic mel)');
const batch = 1;
const time_steps = 3000; // 30 seconds of audio at 100fps = 3000 frames

// Create synthetic mel spectrogram [batch, num_mel_bins, time_steps]
const melLen = batch * num_mel_bins * time_steps;
const melData = new Float32Array(melLen);
// Fill with small random values (simulating log-mel features)
for (let i = 0; i < melLen; i++) {
  melData[i] = (Math.random() - 0.5) * 2.0;
}

const melByteLen = melData.length * 4;
const melPtr = alloc(melByteLen);
write(Float32Array, melPtr, melData);

// Encoder output: [batch, enc_seq, d_model] where enc_seq = (time_steps + 2 - 3) / 2 + 1 = 1500
const enc_seq = Math.floor((time_steps + 2 * 1 - 3) / 2 + 1);
const maxEncOut = batch * enc_seq * d_model;
const encOutByteLen = maxEncOut * 4;
const encOutPtr = alloc(encOutByteLen);

console.log(`  Running encoder (mel: [${batch}, ${num_mel_bins}, ${time_steps}])...`);
const et0 = performance.now();
const encResultLen = wasm.whisper_encode(
  modelHandle,
  melPtr, size(melData.length),
  batch, time_steps,
  encOutPtr,
);
const encElapsed = (performance.now() - et0).toFixed(1);

let encHidden = null;
if (encResultLen > 0) {
  encHidden = read(Float32Array, encOutPtr, encResultLen);
}

free(melPtr, melByteLen);
free(encOutPtr, encOutByteLen);

if (!encHidden) {
  console.error('FAIL: Encoder returned 0');
  process.exit(1);
}

console.log(`  Time: ${encElapsed}ms`);
console.log(`  Output length: ${encHidden.length} (expected ${batch * enc_seq * d_model})`);
console.log(`  First 5 values: [${Array.from(encHidden.slice(0, 5)).map(v => v.toFixed(6)).join(', ')}]`);

const encAllFinite = encHidden.every(v => isFinite(v));
console.log(`  All finite: ${encAllFinite}`);
if (!encAllFinite) { console.error('FAIL: non-finite encoder outputs'); process.exit(1); }
if (encHidden.length !== batch * enc_seq * d_model) {
  console.error(`FAIL: expected ${batch * enc_seq * d_model} floats, got ${encHidden.length}`);
  process.exit(1);
}
console.log('  PASS');

// --- Test 3: Decoder forward ---

console.log('\nTest 3: Decoder forward');
const dec_seq = 1; // Single-step decode
const decoder_start_token_id = config.decoder_start_token_id || 50258;

// Decoder input: just the start token
const decIds = new BigInt64Array([BigInt(decoder_start_token_id)]);
const decIdsByteLen = decIds.length * 8;
const decIdsPtr = alloc(decIdsByteLen);
write(BigInt64Array, decIdsPtr, decIds);

// Encoder hidden as input
const encByteLen = encHidden.length * 4;
const encPtr = alloc(encByteLen);
write(Float32Array, encPtr, encHidden);

// Encoder mask: all ones
const encMask = new BigInt64Array(batch * enc_seq);
for (let i = 0; i < encMask.length; i++) encMask[i] = 1n;
const encMaskByteLen = encMask.length * 8;
const encMaskPtr = alloc(encMaskByteLen);
write(BigInt64Array, encMaskPtr, encMask);

// Output: [batch, dec_seq, vocab_size] logits
const maxDecOut = batch * dec_seq * vocab_size;
const decOutByteLen = maxDecOut * 4;
const decOutPtr = alloc(decOutByteLen);

console.log(`  Running decoder (dec_seq=${dec_seq}, enc_seq=${enc_seq})...`);
const dt0 = performance.now();
const decResultLen = wasm.whisper_decode(
  modelHandle,
  decIdsPtr, size(decIds.length),
  encPtr, size(encHidden.length),
  encMaskPtr, size(encMask.length),
  batch, dec_seq, enc_seq,
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
console.log(`  Output length: ${logits.length} (expected ${batch * dec_seq * vocab_size})`);
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
const tokens = [decoder_start_token_id];
const eos_token_id = config.eos_token_id || 50257;

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

  const stepResultLen = wasm.whisper_decode(
    modelHandle,
    stepIdsPtr, size(stepIds.length),
    encPtr2, size(encHidden.length),
    stepEncMaskPtr, size(encMask.length),
    batch, stepDecSeq, enc_seq,
    stepOutPtr,
  );

  let stepLogits = null;
  if (stepResultLen > 0) {
    // Take last token's logits
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

  if (sMaxIdx === eos_token_id) {
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
console.log('\nAll Whisper tests passed!');
