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

// Test script: loads Gemma3-2B Q4_0 GGUF through WASM, tests forward pass and generation.
//
// Usage:
//   ~/bin/zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-gemma3-q4-wasm.mjs
//
// Requires:
//   models/google/gemma-3-2b-it/gemma-3-2b-it-q4_0.gguf
//   models/google/gemma-3-2b-it/config.json
//
// Model conversion (from HuggingFace):
//   python llama.cpp/convert_hf_to_gguf.py google/gemma-3-2b-it --outtype f16
//   ./llama.cpp/build/bin/llama-quantize gemma-3-2b-it-f16.gguf gemma-3-2b-it-q4_0.gguf q4_0

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateTermiteWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, view, offset, size } = await instantiateTermiteWasm(root);

function readF32Slice(ptr, floatOffset, length) {
  const out = new Float32Array(length);
  out.set(view(Float32Array, offset(ptr) + floatOffset * 4, length));
  return out;
}

// --- Check for model files ---

const modelDir = path.join(root, 'models', 'google', 'gemma-3-2b-it');
const ggufPath = path.join(modelDir, 'gemma-3-2b-it-q4_0.gguf');
const configPath = path.join(modelDir, 'config.json');

if (!fs.existsSync(ggufPath)) {
  console.log('Gemma3-2B Q4_0 GGUF not found at', ggufPath);
  console.log('See header comments for conversion instructions.');
  process.exit(0);
}

if (!fs.existsSync(configPath)) {
  console.log('config.json not found at', configPath);
  process.exit(0);
}

const configJson = fs.readFileSync(configPath, 'utf-8');
const config = JSON.parse(configJson);
const vocabSize = config.vocab_size || 262144;

// --- Test 1: Load Gemma3 Q4_0 GGUF model ---

console.log('Test 1: Load Gemma3-2B Q4_0 GGUF');
const ggufData = fs.readFileSync(ggufPath);
const configBytes = new TextEncoder().encode(configJson);

const ggufPtr = alloc(ggufData.length);
bytesIn(ggufPtr, ggufData);
const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

const t0 = performance.now();
const modelHandle = wasm.load_model_gpt_gguf(
  ggufPtr, size(ggufData.length),
  configPtr, size(configBytes.length),
);
const loadTime = (performance.now() - t0).toFixed(1);

free(ggufPtr, ggufData.length);
free(configPtr, configBytes.length);

if (modelHandle === 0) {
  console.error('FAIL: Failed to load Gemma3 Q4_0 GGUF model');
  process.exit(1);
}
console.log(`  Model loaded (handle=${modelHandle}) in ${loadTime}ms`);
console.log(`  GGUF size: ${(ggufData.length / 1024 / 1024).toFixed(1)}MB`);
console.log('  PASS');

// --- Test 2: Forward pass with KV cache ---

console.log('\nTest 2: Cached forward pass');

const cache = wasm.gpt_create_kv_cache(modelHandle, 512);
if (!cache) {
  console.error('FAIL: Failed to create KV cache');
  process.exit(1);
}

// Simple prompt: BOS + a few token IDs valid for Gemma3
// Gemma3 BOS = 2, common tokens: 651="The", 6239="capital"
const promptIds = [2, 651, 6239, 576, 6771, 603]; // "The capital of France is"
const ids = new BigInt64Array(promptIds.map(BigInt));
const idsByteLen = ids.length * 8;
const idsPtr = alloc(idsByteLen);
write(BigInt64Array, idsPtr, ids);

const maxOut = promptIds.length * vocabSize;
const outByteLen = maxOut * 4;
const outPtr = alloc(outByteLen);

const t1 = performance.now();
const resultLen = wasm.gpt_forward_cached(
  modelHandle, cache,
  idsPtr, size(ids.length),
  1, promptIds.length,
  outPtr,
);
const prefillTime = (performance.now() - t1).toFixed(1);

if (resultLen === 0) {
  free(idsPtr, idsByteLen);
  free(outPtr, outByteLen);
  console.error('FAIL: Cached forward returned 0');
  process.exit(1);
}

// Extract last token's logits
const logits = readF32Slice(outPtr, resultLen - vocabSize, vocabSize);

free(idsPtr, idsByteLen);
free(outPtr, outByteLen);

// Check logits are finite
const allFinite = logits.every(v => isFinite(v));
if (!allFinite) {
  console.error('FAIL: Non-finite logits detected');
  process.exit(1);
}

let maxIdx = 0;
for (let i = 1; i < vocabSize; i++) {
  if (logits[i] > logits[maxIdx]) maxIdx = i;
}

console.log(`  Prefill time: ${prefillTime}ms (${promptIds.length} tokens)`);
console.log(`  Output length: ${resultLen} floats`);
console.log(`  Greedy next token: ${maxIdx} (logit=${logits[maxIdx].toFixed(4)})`);
console.log(`  First 5 logits: [${Array.from(logits.slice(0, 5)).map(v => v.toFixed(4)).join(', ')}]`);
console.log('  PASS');

// --- Test 3: Greedy generation (32 tokens) ---

console.log('\nTest 3: Greedy generation (32 tokens)');
const MAX_STEPS = 32;
const eosTokenId = config.eos_token_id || 1;
const generated = [];
let lastToken = maxIdx;

const genStart = performance.now();
for (let step = 0; step < MAX_STEPS; step++) {
  generated.push(lastToken);

  if (lastToken === eosTokenId) {
    console.log(`  EOS at step ${step}`);
    break;
  }

  const decId = new BigInt64Array([BigInt(lastToken)]);
  const decIdsPtr = alloc(8);
  write(BigInt64Array, decIdsPtr, decId);
  const decOutPtr = alloc(vocabSize * 4);

  const decLen = wasm.gpt_forward_cached(
    modelHandle, cache,
    decIdsPtr, size(1), 1, 1,
    decOutPtr,
  );

  if (decLen === 0) {
    free(decIdsPtr, 8);
    free(decOutPtr, vocabSize * 4);
    console.error(`FAIL: Decode step ${step} returned 0`);
    process.exit(1);
  }

  const decLogits = read(Float32Array, decOutPtr, vocabSize);
  free(decIdsPtr, 8);
  free(decOutPtr, vocabSize * 4);

  let next = 0;
  for (let i = 1; i < vocabSize; i++) {
    if (decLogits[i] > decLogits[next]) next = i;
  }
  lastToken = next;
}
const genTime = (performance.now() - genStart).toFixed(1);

console.log(`  Generated ${generated.length} tokens in ${genTime}ms`);
console.log(`  Tokens: [${generated.join(', ')}]`);
console.log(`  Avg time/token: ${(parseFloat(genTime) / generated.length).toFixed(1)}ms`);

// Validate all tokens are in valid range
const allValid = generated.every(t => t >= 0 && t < vocabSize);
if (!allValid) {
  console.error('FAIL: Invalid token IDs in output');
  process.exit(1);
}

// Check that output isn't all the same token (degenerate)
const uniqueTokens = new Set(generated).size;
console.log(`  Unique tokens: ${uniqueTokens}`);
if (uniqueTokens === 1 && generated.length > 5) {
  console.warn('  WARNING: All generated tokens are identical (may indicate a model issue)');
}
console.log('  PASS');

// --- Cleanup ---

wasm.gpt_free_kv_cache(cache);
wasm.unload_model(modelHandle);

console.log('\nAll Gemma3 Q4_0 tests passed!');
