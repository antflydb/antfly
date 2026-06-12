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

// Test script: loads CLAP model through WASM, tests text and audio encoders.
//
// Usage:
//   ~/bin/zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-clap-wasm.mjs
//
// Requires:
//   models/laion/clap-htsat-unfused/model.safetensors
//   models/laion/clap-htsat-unfused/config.json

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateAntflyInferenceWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, size } = await instantiateAntflyInferenceWasm(root);

const modelDir = path.join(root, 'models', 'laion', 'clap-htsat-unfused');
const modelPath = path.join(modelDir, 'model.safetensors');
const configPath = path.join(modelDir, 'config.json');

if (!fs.existsSync(modelPath)) {
  console.log('CLAP model not found at', modelDir);
  console.log('Download from HuggingFace: laion/clap-htsat-unfused');
  process.exit(0);
}

// --- Test 1: Load CLAP model ---

console.log('Test 1: Load CLAP model');
const modelBytes = fs.readFileSync(modelPath);
const configJson = fs.readFileSync(configPath, 'utf-8');
const configBytes = new TextEncoder().encode(configJson);

const modelPtr = alloc(modelBytes.length);
bytesIn(modelPtr, modelBytes);
const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

const modelHandle = wasm.load_model_clap(
  modelPtr, size(modelBytes.length),
  configPtr, size(configBytes.length),
);
free(modelPtr, modelBytes.length);
free(configPtr, configBytes.length);

if (modelHandle === 0) {
  console.error('FAIL: Failed to load CLAP model');
  process.exit(1);
}
console.log(`  Model loaded (handle=${modelHandle})`);
console.log('  PASS');

// Parse config for expected dimensions
const config = JSON.parse(configJson);
const projection_dim = config.projection_dim || 512;

// --- Test 2: Text embedding ---

console.log('\nTest 2: Text embedding');
const batch = 1;
const seq_len = 16;

// Create synthetic token IDs and attention mask (all ones)
const tokenIds = new BigInt64Array(batch * seq_len);
for (let i = 0; i < tokenIds.length; i++) tokenIds[i] = BigInt(Math.floor(Math.random() * 1000) + 1);
const attentionMask = new BigInt64Array(batch * seq_len);
for (let i = 0; i < attentionMask.length; i++) attentionMask[i] = 1n;

const idsByteLen = tokenIds.length * 8;
const idsPtr = alloc(idsByteLen);
write(BigInt64Array, idsPtr, tokenIds);

const maskByteLen = attentionMask.length * 8;
const maskPtr = alloc(maskByteLen);
write(BigInt64Array, maskPtr, attentionMask);

const maxOut = batch * projection_dim;
const outByteLen = maxOut * 4;
const outPtr = alloc(outByteLen);

console.log(`  Running text encoder (batch=${batch}, seq_len=${seq_len})...`);
const tt0 = performance.now();
const textResultLen = wasm.clap_embed_text(
  modelHandle,
  idsPtr, size(tokenIds.length),
  maskPtr, size(attentionMask.length),
  batch, seq_len,
  outPtr,
);
const textElapsed = (performance.now() - tt0).toFixed(1);

let textEmb = null;
if (textResultLen > 0) {
  textEmb = read(Float32Array, outPtr, textResultLen);
}

free(idsPtr, idsByteLen);
free(maskPtr, maskByteLen);
free(outPtr, outByteLen);

if (!textEmb) {
  console.error('FAIL: Text encoder returned 0');
  process.exit(1);
}

console.log(`  Time: ${textElapsed}ms`);
console.log(`  Output length: ${textEmb.length} (expected ${batch * projection_dim})`);
console.log(`  First 5 values: [${Array.from(textEmb.slice(0, 5)).map(v => v.toFixed(6)).join(', ')}]`);

const textAllFinite = textEmb.every(v => isFinite(v));
console.log(`  All finite: ${textAllFinite}`);
if (!textAllFinite) { console.error('FAIL: non-finite text embeddings'); process.exit(1); }
if (textEmb.length !== batch * projection_dim) {
  console.error(`FAIL: expected ${batch * projection_dim} floats, got ${textEmb.length}`);
  process.exit(1);
}
console.log('  PASS');

// --- Test 3: Audio embedding ---

console.log('\nTest 3: Audio embedding');
const audioBatch = 1;
const channels = 1; // mono, unfused
const time_frames = 1001; // ~10 seconds of audio at 48kHz (480000 samples / 480 hop = 1001)
const mel_bins = 64;

// Create synthetic mel spectrogram [batch, channels, time_frames, mel_bins]
const featLen = audioBatch * channels * time_frames * mel_bins;
const features = new Float32Array(featLen);
for (let i = 0; i < featLen; i++) features[i] = (Math.random() - 0.5) * 2.0;

const featByteLen = features.length * 4;
const featPtr = alloc(featByteLen);
write(Float32Array, featPtr, features);

// is_longer: [0] (not longer than 10 seconds)
const isLonger = new Uint8Array([0]);
const isLongerPtr = alloc(isLonger.length);
bytesIn(isLongerPtr, isLonger);

const audioMaxOut = audioBatch * projection_dim;
const audioOutByteLen = audioMaxOut * 4;
const audioOutPtr = alloc(audioOutByteLen);

console.log(`  Running audio encoder (batch=${audioBatch}, channels=${channels}, time=${time_frames}, mel=${mel_bins})...`);
const at0 = performance.now();
const audioResultLen = wasm.clap_embed_audio(
  modelHandle,
  featPtr, size(features.length),
  audioBatch, channels, time_frames, mel_bins,
  isLongerPtr, size(isLonger.length),
  audioOutPtr,
);
const audioElapsed = (performance.now() - at0).toFixed(1);

let audioEmb = null;
if (audioResultLen > 0) {
  audioEmb = read(Float32Array, audioOutPtr, audioResultLen);
}

free(featPtr, featByteLen);
free(isLongerPtr, isLonger.length);
free(audioOutPtr, audioOutByteLen);

if (!audioEmb) {
  console.error('FAIL: Audio encoder returned 0');
  process.exit(1);
}

console.log(`  Time: ${audioElapsed}ms`);
console.log(`  Output length: ${audioEmb.length} (expected ${audioBatch * projection_dim})`);
console.log(`  First 5 values: [${Array.from(audioEmb.slice(0, 5)).map(v => v.toFixed(6)).join(', ')}]`);

const audioAllFinite = audioEmb.every(v => isFinite(v));
console.log(`  All finite: ${audioAllFinite}`);
if (!audioAllFinite) { console.error('FAIL: non-finite audio embeddings'); process.exit(1); }
if (audioEmb.length !== audioBatch * projection_dim) {
  console.error(`FAIL: expected ${audioBatch * projection_dim} floats, got ${audioEmb.length}`);
  process.exit(1);
}
console.log('  PASS');

// --- Test 4: Cosine similarity ---

console.log('\nTest 4: Cosine similarity (text vs audio)');
// Just verify both embeddings are in the same space by computing cosine sim
function cosineSim(a, b) {
  let dot = 0, normA = 0, normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

const sim = cosineSim(textEmb, audioEmb);
console.log(`  Cosine similarity: ${sim.toFixed(6)}`);
console.log(`  Similarity is finite: ${isFinite(sim)}`);
if (!isFinite(sim)) { console.error('FAIL: non-finite cosine similarity'); process.exit(1); }
console.log('  PASS');

// Cleanup
wasm.unload_model(modelHandle);
console.log('\nAll CLAP tests passed!');
