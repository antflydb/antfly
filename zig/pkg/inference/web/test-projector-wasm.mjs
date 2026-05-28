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

// Projector export smoke test for embedded Termite WASM.
//
// Usage:
//   zig build -Dwasm=true wasm
//   node web/test-projector-wasm.mjs

import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateTermiteWasm } from './test-wasm-runtime.mjs';
import { InferenceWeb } from './inference-web.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

console.log('Loading Termite WASM...');
const { wasm, memoryModel } = await instantiateTermiteWasm(root);
console.log(`Loaded ${memoryModel} module`);

console.log('\nTest 1: projector WASM exports exist');
const requiredExports = [
  'load_projector_gguf',
  'projector_kind',
  'unload_projector',
  'gpt_projector_vision_encode',
  'gpt_projector_image_encode',
  'gpt_projector_audio_encode',
  'gpt_forward_multimodal_gemma4',
  'gpt_forward_cached_multimodal_gemma4',
];

let allPresent = true;
for (const name of requiredExports) {
  if (typeof wasm[name] !== 'function') {
    console.error(`  FAIL: missing export ${name}`);
    allPresent = false;
  }
}
if (!allPresent) process.exit(1);
console.log(`  All ${requiredExports.length} projector-related exports are present`);

console.log('\nTest 2: InferenceWeb multimodal projector helpers exist');
const helperMethods = [
  'loadProjectorGguf',
  'unloadProjector',
  'gptProjectorVisionEncode',
  'gptProjectorImageEncode',
  'gptProjectorAudioEncode',
  'gptForwardMultimodalGemma4',
  'gptForwardCachedMultimodalGemma4',
  'gptGenerateMultimodalGemma4',
];

for (const name of helperMethods) {
  if (typeof InferenceWeb.prototype[name] !== 'function') {
    console.error(`  FAIL: missing InferenceWeb helper ${name}`);
    process.exit(1);
  }
}
console.log(`  All ${helperMethods.length} InferenceWeb helpers are present`);

console.log('\nTest 3: worker-mode projector helpers dispatch correct RPCs');
const termite = new InferenceWeb();
termite._worker = { postMessage() {} };
termite._readBinarySource = async (source) => {
  if (source instanceof Uint8Array) return source;
  if (source instanceof ArrayBuffer) return source;
  throw new Error(`unexpected source type: ${typeof source}`);
};

const workerCalls = [];
termite._workerCall = async (type, extra = {}, transfer = []) => {
  workerCalls.push({ type, extra, transfer });
  switch (type) {
    case 'load-projector':
      return { handle: 17, kind: 'clip_gemma4_image_audio' };
    case 'unload-projector':
      return { ok: true };
    case 'gpt-projector-vision-encode':
      return { output: new Float32Array([1, 2, 3, 4]) };
    case 'gpt-projector-image-encode':
      return { embeddings: new Float32Array([0.5, 1.5]), tokensPerImage: [77] };
    case 'gpt-projector-audio-encode':
      return { embeddings: new Float32Array([2.5, 3.5]), tokensPerAudio: [33] };
    case 'gpt-forward-multimodal-gemma4':
      return { output: new Float32Array([9, 8, 7]) };
    case 'gpt-forward-cached-multimodal-gemma4':
      return { output: new Float32Array([6, 5, 4]) };
    default:
      throw new Error(`unexpected worker RPC ${type}`);
  }
};

const projectorBytes = new Uint8Array([1, 2, 3, 4]);
const loadedProjector = await termite.loadProjectorGguf(projectorBytes);
if (loadedProjector.handle !== 17 || loadedProjector.kind !== 'clip_gemma4_image_audio') {
  console.error('  FAIL: loadProjectorGguf did not return worker response');
  process.exit(1);
}

await termite.unloadProjector(17);

const visionOutput = await termite.gptProjectorVisionEncode(10, 17, new Float32Array([0.1, 0.2, 0.3, 0.4]), 2);
if (!(visionOutput instanceof Float32Array) || visionOutput.length !== 4) {
  console.error('  FAIL: gptProjectorVisionEncode did not return worker output');
  process.exit(1);
}

const imageEncoded = await termite.gptProjectorImageEncode(17, new Uint8Array([9, 8, 7]));
if (!(imageEncoded.embeddings instanceof Float32Array) || imageEncoded.tokensPerImage[0] !== 77) {
  console.error('  FAIL: gptProjectorImageEncode did not preserve worker payload');
  process.exit(1);
}

const audioEncoded = await termite.gptProjectorAudioEncode(17, new Uint8Array([6, 5, 4]));
if (!(audioEncoded.embeddings instanceof Float32Array) || audioEncoded.tokensPerAudio[0] !== 33) {
  console.error('  FAIL: gptProjectorAudioEncode did not preserve worker payload');
  process.exit(1);
}

const multimodalOutput = await termite.gptForwardMultimodalGemma4(
  10,
  11,
  [101, 102, 103],
  new Float32Array([0.25, 0.5]),
  [64],
  new Float32Array([0.75, 1.0]),
  [32],
  1,
  3,
);
if (!(multimodalOutput instanceof Float32Array) || multimodalOutput.length !== 3) {
  console.error('  FAIL: gptForwardMultimodalGemma4 did not return worker output');
  process.exit(1);
}

const cachedMultimodalOutput = await termite.gptForwardCachedMultimodalGemma4(
  10,
  11,
  12,
  [101, 102, 103],
  new Float32Array([0.25, 0.5]),
  [64],
  new Float32Array([0.75, 1.0]),
  [32],
  1,
  3,
);
if (!(cachedMultimodalOutput instanceof Float32Array) || cachedMultimodalOutput.length !== 3) {
  console.error('  FAIL: gptForwardCachedMultimodalGemma4 did not return worker output');
  process.exit(1);
}

const expectedRpcSequence = [
  'load-projector',
  'unload-projector',
  'gpt-projector-vision-encode',
  'gpt-projector-image-encode',
  'gpt-projector-audio-encode',
  'gpt-forward-multimodal-gemma4',
  'gpt-forward-cached-multimodal-gemma4',
];
for (const [index, expectedType] of expectedRpcSequence.entries()) {
  if (workerCalls[index]?.type !== expectedType) {
    console.error(`  FAIL: expected worker RPC ${expectedType} at index ${index}, got ${workerCalls[index]?.type}`);
    process.exit(1);
  }
}

if (!(workerCalls[0].extra.projectorBytes instanceof Uint8Array) || workerCalls[0].transfer.length !== 1) {
  console.error('  FAIL: load-projector did not transfer projector bytes');
  process.exit(1);
}
if (!(workerCalls[3].extra.imageBytes instanceof Uint8Array) || workerCalls[3].transfer.length !== 1) {
  console.error('  FAIL: gpt-projector-image-encode did not transfer image bytes');
  process.exit(1);
}
if (!(workerCalls[4].extra.audioBytes instanceof Uint8Array) || workerCalls[4].transfer.length !== 1) {
  console.error('  FAIL: gpt-projector-audio-encode did not transfer audio bytes');
  process.exit(1);
}
if (!Array.isArray(workerCalls[5].extra.expandedIds) || workerCalls[5].extra.expandedIds[0] !== 101) {
  console.error('  FAIL: gpt-forward-multimodal-gemma4 did not normalize expandedIds for worker transport');
  process.exit(1);
}
if (!Array.isArray(workerCalls[6].extra.expandedIds) || workerCalls[6].extra.expandedIds[2] !== 103) {
  console.error('  FAIL: gpt-forward-cached-multimodal-gemma4 did not normalize expandedIds for worker transport');
  process.exit(1);
}
console.log(`  Executed ${workerCalls.length} projector worker RPCs with expected payloads`);

console.log('\nTest 4: Gemma4 cached multimodal generation composes the new projector path correctly');
const generateTermite = new InferenceWeb();
const generationCalls = [];
let freedCache = null;
generateTermite.gptCreateKvCache = (modelHandle, maxLen, options) => {
  generationCalls.push({ type: 'create-cache', modelHandle, maxLen, options });
  return 44;
};
generateTermite.gptForwardCachedMultimodalGemma4 = (
  modelHandle,
  tokenizerHandle,
  cacheHandle,
  promptIds,
  imageEmbeddings,
  tokensPerImage,
  audioEmbeddings,
  tokensPerAudio,
  batchSize,
  seqLen,
) => {
  generationCalls.push({
    type: 'prefill',
    modelHandle,
    tokenizerHandle,
    cacheHandle,
    promptIds: [...promptIds],
    imageEmbeddings: Array.from(imageEmbeddings),
    tokensPerImage: [...tokensPerImage],
    audioEmbeddings: Array.from(audioEmbeddings),
    tokensPerAudio: [...tokensPerAudio],
    batchSize,
    seqLen,
  });
  return new Float32Array([0, 0, 10, 0]);
};
generateTermite.gptForwardCached = (modelHandle, cacheHandle, ids, batchSize, seqLen) => {
  generationCalls.push({ type: 'decode', modelHandle, cacheHandle, ids: [...ids], batchSize, seqLen });
  return new Float32Array([0, 9, 0, 0]);
};
generateTermite.gptFreeKvCache = (cacheHandle) => {
  freedCache = cacheHandle;
};

const emittedTokens = [];
const generated = await generateTermite.gptGenerateMultimodalGemma4(
  3,
  4,
  4,
  [12, 13],
  new Float32Array([0.1, 0.2]),
  [16],
  new Float32Array([0.3, 0.4]),
  [8],
  {
    maxTokens: 2,
    maxLen: 99,
    eosTokenId: 1,
    onToken: (token, step) => emittedTokens.push({ token, step }),
  },
);

if (generated.length !== 1 || generated[0] !== 2) {
  console.error(`  FAIL: unexpected generated Gemma4 multimodal tokens ${generated}`);
  process.exit(1);
}
if (freedCache !== 44) {
  console.error('  FAIL: gptGenerateMultimodalGemma4 did not free its KV cache');
  process.exit(1);
}
if (generationCalls[0]?.type !== 'create-cache' || generationCalls[1]?.type !== 'prefill' || generationCalls[2]?.type !== 'decode') {
  console.error('  FAIL: gptGenerateMultimodalGemma4 did not call prefill/decode in the expected order');
  process.exit(1);
}
if (emittedTokens.length !== 1 || emittedTokens[0].token !== 2 || emittedTokens[0].step !== 0) {
  console.error('  FAIL: gptGenerateMultimodalGemma4 onToken callbacks are incorrect');
  process.exit(1);
}
console.log('  Gemma4 multimodal generation used cached prefill/decode and freed the KV cache');

console.log('\nProjector WASM smoke test passed.');
