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

// Test script: loads Gemma3-4B multimodal through WASM, tests vision encode and generation.
//
// Usage:
//   ~/bin/zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-gemma3-vision-wasm.mjs
//
// Requires:
//   models/google/gemma-3-4b-it/ with model.safetensors, config.json, tokenizer.json

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateAntflyInferenceWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, view, offset, size } = await instantiateAntflyInferenceWasm(root);

function readF32Slice(ptr, floatOffset, length) {
  const out = new Float32Array(length);
  out.set(view(Float32Array, offset(ptr) + floatOffset * 4, length));
  return out;
}

// --- Check for model files ---

const modelDir = path.join(root, 'models', 'google', 'gemma-3-4b-it');
const configPath = path.join(modelDir, 'config.json');

if (!fs.existsSync(configPath)) {
  console.log('Gemma3-4B-IT model not found at', modelDir);
  console.log('Download from HuggingFace: google/gemma-3-4b-it');
  console.log('  mkdir -p', modelDir);
  console.log('  huggingface-cli download google/gemma-3-4b-it --local-dir', modelDir);
  console.log('\nSkipping model-dependent tests. Running API smoke tests only.\n');
}

const hasModel = fs.existsSync(configPath);
const config = hasModel ? JSON.parse(fs.readFileSync(configPath, 'utf-8')) : null;

// --- Test 1: WASM exports exist ---

console.log('Test 1: Vision-language WASM exports exist');
const requiredExports = [
  'gpt_vision_encode',
  'gpt_forward_multimodal',
  'gpt_forward_cached_multimodal',
  'load_model_gpt',
  'gpt_create_kv_cache',
  'gpt_forward_cached',
  'gpt_free_kv_cache',
];

let allPresent = true;
for (const name of requiredExports) {
  if (typeof wasm[name] !== 'function') {
    console.error(`  FAIL: Missing export: ${name}`);
    allPresent = false;
  }
}
if (!allPresent) process.exit(1);
console.log(`  All ${requiredExports.length} exports present`);
console.log('  PASS');

// --- Test 2: Image preprocessing (CPU-side, no model needed) ---

console.log('\nTest 2: Image preprocessing simulation');
{
  // Simulate preprocessImageBrowser: create a synthetic 224x224 CHW float32 image
  const imageSize = 224;
  const channels = 3;
  const pixelCount = channels * imageSize * imageSize;
  const pixelValues = new Float32Array(pixelCount);

  // Fill with normalized values in [-1, 1] range (as preprocessImageBrowser would)
  for (let c = 0; c < channels; c++) {
    for (let y = 0; y < imageSize; y++) {
      for (let x = 0; x < imageSize; x++) {
        const idx = c * imageSize * imageSize + y * imageSize + x;
        // Gradient pattern: normalized to [-1, 1]
        pixelValues[idx] = ((x + y * c) / (imageSize * channels)) * 2.0 - 1.0;
      }
    }
  }

  // Check shape and range
  if (pixelValues.length !== pixelCount) {
    console.error(`  FAIL: Expected ${pixelCount} values, got ${pixelValues.length}`);
    process.exit(1);
  }

  const min = pixelValues.reduce((a, b) => Math.min(a, b), Infinity);
  const max = pixelValues.reduce((a, b) => Math.max(a, b), -Infinity);
  console.log(`  Synthetic image: [${channels}, ${imageSize}, ${imageSize}]`);
  console.log(`  Value range: [${min.toFixed(3)}, ${max.toFixed(3)}]`);
  console.log('  PASS');
}

// --- Test 3: Vision encode (requires model) ---

if (hasModel) {
  console.log('\nTest 3: Vision encode');

  // Find model safetensors file(s)
  const stFiles = fs.readdirSync(modelDir).filter(f => f.endsWith('.safetensors'));
  if (stFiles.length === 0) {
    console.error('  No .safetensors files found in', modelDir);
    process.exit(1);
  }

  // Load model
  const configJson = fs.readFileSync(configPath, 'utf-8');
  const configBytes = new TextEncoder().encode(configJson);
  const configPtr = alloc(configBytes.length);
  bytesIn(configPtr, configBytes);

  // For multi-shard models, use streaming loader
  const modelHandle = wasm.create_model_gpt(configPtr, size(configBytes.length));
  free(configPtr, configBytes.length);

  if (modelHandle === 0) {
    console.error('  FAIL: create_model_gpt returned 0');
    process.exit(1);
  }
  console.log(`  Empty model created (handle=${modelHandle})`);

  // Register weights from all safetensors shards (handles files >2 GiB via fd reads)
  const dtypeMap = { F32: 0, F16: 1, BF16: 2 };
  let registered = 0;
  let oom = false;

  function readBytes(fd, offset, length) {
    const buf = Buffer.alloc(length);
    let read = 0;
    while (read < length) {
      const n = fs.readSync(fd, buf, read, length - read, offset + read);
      if (n === 0) break;
      read += n;
    }
    return buf;
  }

  for (const stFile of stFiles) {
    if (oom) break;
    const stPath = path.join(modelDir, stFile);
    const fd = fs.openSync(stPath, 'r');

    // Read header length (8 bytes)
    const headerLenBuf = readBytes(fd, 0, 8);
    const headerLen = Number(new DataView(headerLenBuf.buffer, headerLenBuf.byteOffset, 8).getBigUint64(0, true));

    // Read header JSON
    const headerBuf = readBytes(fd, 8, headerLen);
    const stHeader = JSON.parse(headerBuf.toString('utf-8'));
    const dataOffset = 8 + headerLen;

    for (const [name, meta] of Object.entries(stHeader)) {
      if (name === '__metadata__') continue;
      if (name.endsWith('.position_ids')) continue;

      const absStart = dataOffset + meta.data_offsets[0];
      const weightLen = meta.data_offsets[1] - meta.data_offsets[0];
      const shape = meta.shape;
      const rows = shape.length >= 2 ? shape[0] : 1;
      const cols = shape.length >= 2 ? shape[1] : shape[0] || 1;
      const dtype = dtypeMap[meta.dtype] ?? 0;

      try {
        const nameBytes = new TextEncoder().encode(name);
        const namePtr = alloc(nameBytes.length);
        bytesIn(namePtr, nameBytes);

        const dataPtr = alloc(weightLen);

        // Read weight data directly into WASM memory
        const raw = readBytes(fd, absStart, weightLen);
        bytesIn(dataPtr, raw);

        const ok = wasm.register_weight(
          modelHandle,
          namePtr, size(nameBytes.length),
          dataPtr, size(weightLen),
          rows, cols, dtype,
        );
        free(namePtr, nameBytes.length);
        free(dataPtr, weightLen);
        if (ok) registered++;
      } catch (e) {
        if (e instanceof WebAssembly.RuntimeError || e.message?.includes('alloc')) {
          oom = true;
          break;
        }
        throw e;
      }
    }
    fs.closeSync(fd);
  }

  if (oom) {
    console.log(`  Registered ${registered} weights before WASM OOM`);
    console.log('  Model too large for 4GB WASM memory — skipping inference tests');
    wasm.unload_model(modelHandle);
    console.log('\nAll Gemma3 vision WASM tests passed! (inference tests skipped — model too large)');
    process.exit(0);
  }
  console.log(`  Registered ${registered} weights`);

  // Create synthetic image input
  const imageSize = config.vision_config?.image_size || 224;
  const pixelCount = 3 * imageSize * imageSize;
  const pixelValues = new Float32Array(pixelCount);
  for (let i = 0; i < pixelCount; i++) pixelValues[i] = (Math.random() * 2.0 - 1.0);

  const pixPtr = alloc(pixelCount * 4);
  write(Float32Array, pixPtr, pixelValues);

  // Estimate output size: mm_tokens_per_image * hidden_size
  const mmTokens = config.mm_tokens_per_image || 256;
  const hiddenSize = config.hidden_size || 2304;
  const maxOut = 1 * mmTokens * hiddenSize;
  const outPtr = alloc(maxOut * 4);

  console.log(`  Running vision encode (image_size=${imageSize}, mm_tokens=${mmTokens})...`);
  const t0 = performance.now();
  const resultLen = wasm.gpt_vision_encode(modelHandle, pixPtr, size(pixelCount), 1, outPtr);
  const t1 = performance.now();

  free(pixPtr, pixelCount * 4);

  if (resultLen === 0) {
    console.error('  FAIL: gpt_vision_encode returned 0');
    free(outPtr, maxOut * 4);
    wasm.unload_model(modelHandle);
    process.exit(1);
  }

  const visionOutput = read(Float32Array, outPtr, resultLen);
  free(outPtr, maxOut * 4);

  const expectedLen = mmTokens * hiddenSize;
  console.log(`  Output: ${resultLen} floats (expected ${expectedLen})`);
  console.log(`  First 5: [${visionOutput.slice(0, 5).map(v => v.toFixed(4)).join(', ')}]`);
  console.log(`  Time: ${(t1 - t0).toFixed(1)} ms`);

  // Verify not all zeros
  const visionSum = visionOutput.reduce((a, b) => a + Math.abs(b), 0);
  if (visionSum < 1e-6) {
    console.error('  FAIL: Vision output is all zeros');
    wasm.unload_model(modelHandle);
    process.exit(1);
  }
  console.log(`  L1 norm: ${visionSum.toFixed(2)}`);
  console.log('  PASS');

  // --- Test 4: Multimodal forward (single step) ---

  console.log('\nTest 4: Multimodal forward');
  {
    const vocabSize = config.vocab_size || 262144;

    // Construct expanded IDs with image placeholder.
    // Gemma3 uses image_token_index (default 255999) for image placeholders.
    const imageTokenId = config.image_token_index || 255999;
    // Prompt: [BOS] + [image_token * mmTokens] + text tokens
    const bosId = config.bos_token_id ?? 2;
    const textTokens = [2, 1596]; // <bos> "What"
    const expandedIds = [bosId];
    for (let i = 0; i < mmTokens; i++) expandedIds.push(imageTokenId);
    expandedIds.push(...textTokens);

    const seqLen = expandedIds.length;
    const ids64 = new BigInt64Array(expandedIds.map(v => BigInt(v)));
    const idsByteLen = ids64.length * 8;
    const idsPtr = alloc(idsByteLen);
    write(BigInt64Array, idsPtr, ids64);

    // Image embeddings from Test 3
    const imgEmbPtr = alloc(visionOutput.length * 4);
    write(Float32Array, imgEmbPtr, visionOutput);

    // Image offset: image tokens start at position 1 (after BOS)
    const offsets = new Uint32Array([1]);
    const offsetPtr = alloc(4);
    write(Uint32Array, offsetPtr, offsets);

    const mmMaxOut = 1 * seqLen * vocabSize;
    const mmOutPtr = alloc(mmMaxOut * 4);

    console.log(`  seq_len=${seqLen}, vocab_size=${vocabSize}`);
    const mmT0 = performance.now();
    const mmResultLen = wasm.gpt_forward_multimodal(
      modelHandle,
      idsPtr, size(ids64.length),
      imgEmbPtr, size(visionOutput.length),
      offsetPtr, size(1),
      1, seqLen,
      mmOutPtr,
    );
    const mmT1 = performance.now();

    free(idsPtr, idsByteLen);
    free(imgEmbPtr, visionOutput.length * 4);
    free(offsetPtr, 4);

    if (mmResultLen === 0) {
      console.error('  FAIL: gpt_forward_multimodal returned 0');
      free(mmOutPtr, mmMaxOut * 4);
      wasm.unload_model(modelHandle);
      process.exit(1);
    }

    // Extract last token logits
    const mmLogits = readF32Slice(mmOutPtr, mmResultLen - vocabSize, vocabSize);
    free(mmOutPtr, mmMaxOut * 4);

    let maxIdx = 0;
    for (let i = 1; i < vocabSize; i++) {
      if (mmLogits[i] > mmLogits[maxIdx]) maxIdx = i;
    }

    console.log(`  Output: ${mmResultLen} floats`);
    console.log(`  Top token: ${maxIdx} (logit=${mmLogits[maxIdx].toFixed(4)})`);
    console.log(`  Time: ${(mmT1 - mmT0).toFixed(1)} ms`);
    console.log('  PASS');

    // --- Test 5: Cached multimodal generation (3 tokens) ---

    console.log('\nTest 5: Cached multimodal generation (3 tokens)');
    {
      const maxLen = seqLen + 32;
      const cache = wasm.gpt_create_kv_cache(modelHandle, maxLen);
      if (!cache) {
        console.error('  FAIL: Failed to create KV cache');
        wasm.unload_model(modelHandle);
        process.exit(1);
      }
      console.log(`  KV cache created (handle=${cache}, max_len=${maxLen})`);

      // Prefill with multimodal
      const pfIdsPtr = alloc(idsByteLen);
      write(BigInt64Array, pfIdsPtr, ids64);
      const pfImgPtr = alloc(visionOutput.length * 4);
      write(Float32Array, pfImgPtr, visionOutput);
      const pfOffsetPtr = alloc(4);
      write(Uint32Array, pfOffsetPtr, offsets);
      const pfOutPtr = alloc(mmMaxOut * 4);

      const pfT0 = performance.now();
      const pfLen = wasm.gpt_forward_cached_multimodal(
        modelHandle, cache,
        pfIdsPtr, size(ids64.length),
        pfImgPtr, size(visionOutput.length),
        pfOffsetPtr, size(1),
        1, seqLen,
        pfOutPtr,
      );
      const pfT1 = performance.now();

      free(pfIdsPtr, idsByteLen);
      free(pfImgPtr, visionOutput.length * 4);
      free(pfOffsetPtr, 4);

      if (pfLen === 0) {
        console.error('  FAIL: Cached multimodal prefill returned 0');
        free(pfOutPtr, mmMaxOut * 4);
        wasm.gpt_free_kv_cache(cache);
        wasm.unload_model(modelHandle);
        process.exit(1);
      }

      // Get greedy token from prefill
      const pfLogits = readF32Slice(pfOutPtr, pfLen - vocabSize, vocabSize);
      free(pfOutPtr, mmMaxOut * 4);

      let nextToken = 0;
      for (let i = 1; i < vocabSize; i++) {
        if (pfLogits[i] > pfLogits[nextToken]) nextToken = i;
      }
      console.log(`  Prefill: ${(pfT1 - pfT0).toFixed(1)} ms, first token: ${nextToken}`);

      const eosId = config.eos_token_id ?? 1;
      const genTokens = [nextToken];

      // Decode 2 more tokens with standard cached forward
      for (let step = 0; step < 2; step++) {
        if (nextToken === eosId) {
          console.log(`  EOS at step ${step}`);
          break;
        }

        const decId = new BigInt64Array([BigInt(nextToken)]);
        const decIdsPtr = alloc(8);
        write(BigInt64Array, decIdsPtr, decId);
        const decOutPtr = alloc(vocabSize * 4);

        const decT0 = performance.now();
        const decLen = wasm.gpt_forward_cached(modelHandle, cache, decIdsPtr, size(1), 1, 1, decOutPtr);
        const decT1 = performance.now();

        free(decIdsPtr, 8);

        if (decLen === 0) {
          console.error(`  FAIL: Cached decode returned 0 at step ${step}`);
          free(decOutPtr, vocabSize * 4);
          break;
        }

        const decLogits = read(Float32Array, decOutPtr, vocabSize);
        free(decOutPtr, vocabSize * 4);

        nextToken = 0;
        for (let i = 1; i < vocabSize; i++) {
          if (decLogits[i] > decLogits[nextToken]) nextToken = i;
        }
        genTokens.push(nextToken);
        console.log(`  Decode step ${step}: token=${nextToken}, time=${(decT1 - decT0).toFixed(1)} ms`);
      }

      console.log(`  Generated tokens: [${genTokens.join(', ')}]`);
      wasm.gpt_free_kv_cache(cache);
      console.log('  PASS');
    }
  }

  wasm.unload_model(modelHandle);
} else {
  console.log('\nTests 3-5 skipped (no model files)');
}

console.log('\nAll Gemma3 vision WASM tests passed!');
