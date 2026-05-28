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

// Test script: loads CLIP model through WASM, tests text and image embedding.
//
// Usage:
//   ~/bin/zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-clip-wasm.mjs
//
// Requires:
//   models/openai/clip-vit-base-patch32/model.safetensors
//   models/openai/clip-vit-base-patch32/config.json
//   models/openai/clip-vit-base-patch32/tokenizer.json

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateTermiteWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, size } = await instantiateTermiteWasm(root);

const modelDir = path.join(root, 'models', 'openai', 'clip-vit-base-patch32');

// --- Test 1: Load CLIP model ---

const modelPath = path.join(modelDir, 'model.safetensors');
const configPath = path.join(modelDir, 'config.json');
const tokenizerPath = path.join(modelDir, 'tokenizer.json');

if (!fs.existsSync(modelPath)) {
  console.log('CLIP model not found at', modelDir);
  console.log('Download with: termite pull openai/clip-vit-base-patch32:native');
  process.exit(0);
}

console.log('Test 1: Load CLIP model');
const modelBytes = fs.readFileSync(modelPath);
const configJson = fs.readFileSync(configPath, 'utf-8');
const configBytes = new TextEncoder().encode(configJson);

const modelPtr = alloc(modelBytes.length);
bytesIn(modelPtr, modelBytes);
const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

const modelHandle = wasm.load_model_clip(
  modelPtr, size(modelBytes.length),
  configPtr, size(configBytes.length),
);
free(modelPtr, modelBytes.length);
free(configPtr, configBytes.length);

if (modelHandle === 0) {
  console.error('FAIL: Failed to load CLIP model');
  process.exit(1);
}
console.log(`  Model loaded (handle=${modelHandle})`);
console.log('  PASS');

// Parse config for dimensions
const config = JSON.parse(configJson);
const textConfig = config.text_config || config;
const visionConfig = config.vision_config || config;
const projectionDim = config.projection_dim || 512;
const imageSize = visionConfig.image_size || 224;

// --- Test 2: Load tokenizer ---

console.log('\nTest 2: Load tokenizer');
if (!fs.existsSync(tokenizerPath)) {
  console.log('  Tokenizer not found, skipping text embedding tests');
} else {
  const tokenizerJson = fs.readFileSync(tokenizerPath);
  const tokPtr = alloc(tokenizerJson.length);
  bytesIn(tokPtr, tokenizerJson);
  const tokHandle = wasm.load_tokenizer(tokPtr, size(tokenizerJson.length));
  free(tokPtr, tokenizerJson.length);

  if (tokHandle === 0) {
    console.error('FAIL: Failed to load tokenizer');
    process.exit(1);
  }
  console.log(`  Tokenizer loaded (handle=${tokHandle})`);
  console.log('  PASS');

  // --- Test 3: Text embedding ---

  console.log('\nTest 3: Text embedding');
  const seqLen = 77; // CLIP default max_position_embeddings
  const text = 'a photo of a cat';
  const textBytes = new TextEncoder().encode(text);
  const textPtr = alloc(textBytes.length);
  bytesIn(textPtr, textBytes);

  // Tokenize: [CLS] text [SEP] with padding to seqLen
  const idsOutLen = seqLen * 4;
  const idsPtr = alloc(idsOutLen);
  const maskPtr = alloc(idsOutLen);
  wasm.tokenize(tokHandle, textPtr, size(textBytes.length), seqLen, idsPtr, maskPtr);

  const ids32 = read(Int32Array, idsPtr, seqLen);

  free(textPtr, textBytes.length);
  free(idsPtr, idsOutLen);
  free(maskPtr, idsOutLen);

  // CLIP uses SOT (49406) and EOT (49407) tokens, but our tokenizer may use
  // CLS(101)/SEP(102). Convert to BigInt64Array for the WASM export.
  const idsI64 = new BigInt64Array(seqLen);
  for (let i = 0; i < seqLen; i++) idsI64[i] = BigInt(ids32[i]);

  const idsByteLen = idsI64.length * 8;
  const idsWasmPtr = alloc(idsByteLen);
  write(BigInt64Array, idsWasmPtr, idsI64);

  const maxTextOut = 1 * projectionDim;
  const textOutByteLen = maxTextOut * 4;
  const textOutPtr = alloc(textOutByteLen);

  console.log('  Running text encoder...');
  const t0 = performance.now();
  const textResultLen = wasm.clip_embed_text(
    modelHandle,
    idsWasmPtr, size(idsI64.length),
    1, seqLen,
    textOutPtr,
  );
  const textElapsed = (performance.now() - t0).toFixed(1);

  let textEmb = null;
  if (textResultLen > 0) {
    textEmb = read(Float32Array, textOutPtr, textResultLen);
  }

  free(idsWasmPtr, idsByteLen);
  free(textOutPtr, textOutByteLen);

  if (!textEmb) {
    console.error('FAIL: Text embedding returned 0');
    process.exit(1);
  }

  console.log(`  Time: ${textElapsed}ms`);
  console.log(`  Output length: ${textEmb.length} (expected ${projectionDim})`);
  console.log(`  First 5 values: [${Array.from(textEmb.slice(0, 5)).map(v => v.toFixed(6)).join(', ')}]`);

  const textAllFinite = textEmb.every(v => isFinite(v));
  console.log(`  All finite: ${textAllFinite}`);
  if (!textAllFinite) { console.error('FAIL: non-finite text embeddings'); process.exit(1); }
  if (textEmb.length !== projectionDim) {
    console.error(`FAIL: expected ${projectionDim} dims, got ${textEmb.length}`);
    process.exit(1);
  }
  console.log('  PASS');

  wasm.unload_tokenizer(tokHandle);
}

// --- Test 4: Image preprocessing ---

console.log('\nTest 4: Image preprocessing');
// Create a synthetic RGBA image (red square)
const testWidth = 64;
const testHeight = 64;
const rgbaLen = testWidth * testHeight * 4;
const rgbaData = new Uint8Array(rgbaLen);
for (let i = 0; i < testWidth * testHeight; i++) {
  rgbaData[i * 4 + 0] = 255; // R
  rgbaData[i * 4 + 1] = 0;   // G
  rgbaData[i * 4 + 2] = 0;   // B
  rgbaData[i * 4 + 3] = 255; // A
}

const rgbaPtr = alloc(rgbaLen);
bytesIn(rgbaPtr, rgbaData);

// ImageNet normalization (standard for CLIP)
const mean = new Float32Array([0.48145466, 0.4578275, 0.40821073]);
const std = new Float32Array([0.26862954, 0.26130258, 0.27577711]);
const meanPtr = alloc(12);
const stdPtr = alloc(12);
write(Float32Array, meanPtr, mean);
write(Float32Array, stdPtr, std);

const expectedPixels = 3 * imageSize * imageSize;
const pixelOutByteLen = expectedPixels * 4;
const pixelOutPtr = alloc(pixelOutByteLen);

const preprocT0 = performance.now();
const pixelResultLen = wasm.preprocess_image(
  rgbaPtr, rgbaLen,
  testWidth, testHeight, imageSize,
  meanPtr, stdPtr,
  pixelOutPtr,
);
const preprocElapsed = (performance.now() - preprocT0).toFixed(1);

let pixelValues = null;
if (pixelResultLen > 0) {
  pixelValues = read(Float32Array, pixelOutPtr, pixelResultLen);
}

free(rgbaPtr, rgbaLen);
free(meanPtr, 12);
free(stdPtr, 12);
free(pixelOutPtr, pixelOutByteLen);

if (!pixelValues) {
  console.error('FAIL: Image preprocessing returned 0');
  process.exit(1);
}

console.log(`  Time: ${preprocElapsed}ms`);
console.log(`  Output length: ${pixelValues.length} (expected ${expectedPixels})`);
console.log(`  First 5 values (R channel): [${Array.from(pixelValues.slice(0, 5)).map(v => v.toFixed(4)).join(', ')}]`);

const pixelAllFinite = pixelValues.every(v => isFinite(v));
console.log(`  All finite: ${pixelAllFinite}`);
if (!pixelAllFinite) { console.error('FAIL: non-finite pixel values'); process.exit(1); }
if (pixelValues.length !== expectedPixels) {
  console.error(`FAIL: expected ${expectedPixels} floats, got ${pixelValues.length}`);
  process.exit(1);
}

// Verify R channel is high (red image normalized), G and B channels are low
const rPlane = pixelValues.slice(0, imageSize * imageSize);
const gPlane = pixelValues.slice(imageSize * imageSize, 2 * imageSize * imageSize);
const rMean = rPlane.reduce((a, b) => a + b, 0) / rPlane.length;
const gMean = gPlane.reduce((a, b) => a + b, 0) / gPlane.length;
console.log(`  R channel mean: ${rMean.toFixed(4)}, G channel mean: ${gMean.toFixed(4)}`);
if (rMean <= gMean) {
  console.error('FAIL: red image should have higher R than G channel');
  process.exit(1);
}
console.log('  PASS');

// --- Test 5: Image embedding ---

console.log('\nTest 5: Image embedding');
const pixelByteLen = pixelValues.length * 4;
const pixelPtr = alloc(pixelByteLen);
write(Float32Array, pixelPtr, pixelValues);

const maxImageOut = 1 * projectionDim;
const imageOutByteLen = maxImageOut * 4;
const imageOutPtr = alloc(imageOutByteLen);

console.log('  Running vision encoder...');
const vt0 = performance.now();
const imageResultLen = wasm.clip_embed_image(
  modelHandle,
  pixelPtr, size(pixelValues.length),
  1,
  imageOutPtr,
);
const vElapsed = (performance.now() - vt0).toFixed(1);

let imageEmb = null;
if (imageResultLen > 0) {
  imageEmb = read(Float32Array, imageOutPtr, imageResultLen);
}

free(pixelPtr, pixelByteLen);
free(imageOutPtr, imageOutByteLen);

if (!imageEmb) {
  console.error('FAIL: Image embedding returned 0');
  process.exit(1);
}

console.log(`  Time: ${vElapsed}ms`);
console.log(`  Output length: ${imageEmb.length} (expected ${projectionDim})`);
console.log(`  First 5 values: [${Array.from(imageEmb.slice(0, 5)).map(v => v.toFixed(6)).join(', ')}]`);

const imageAllFinite = imageEmb.every(v => isFinite(v));
console.log(`  All finite: ${imageAllFinite}`);
if (!imageAllFinite) { console.error('FAIL: non-finite image embeddings'); process.exit(1); }
if (imageEmb.length !== projectionDim) {
  console.error(`FAIL: expected ${projectionDim} dims, got ${imageEmb.length}`);
  process.exit(1);
}

// Verify embedding has non-trivial norm (not all zeros)
const norm = Math.sqrt(imageEmb.reduce((a, v) => a + v * v, 0));
console.log(`  L2 norm: ${norm.toFixed(4)}`);
if (norm < 0.01) {
  console.error('FAIL: image embedding has near-zero norm');
  process.exit(1);
}
console.log('  PASS');

// Cleanup
wasm.unload_model(modelHandle);
console.log('\nAll CLIP tests passed!');
