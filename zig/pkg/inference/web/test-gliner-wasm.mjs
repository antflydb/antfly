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

// GLiNER E2E test: loads tokenizer + GLiNER model through WASM, tests NER.
//
// Usage:
//   zig build -Dwasm=true wasm
//   node --max-old-space-size=4096 web/test-gliner-wasm.mjs

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { instantiateTermiteWasm } from './test-wasm-runtime.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');

// Load WASM module
const { wasm, alloc, free, bytesIn, write, read, size } = await instantiateTermiteWasm(root);

// --- Model paths ---

const modelDir = path.join(root, 'models', 'recognizers', 'fastino', 'gliner2-base-v1');
const tokenizerPath = path.join(modelDir, 'tokenizer.json');
const modelPath = path.join(modelDir, 'model.safetensors');
const configPath = path.join(modelDir, 'config.json');
const addedTokensPath = path.join(modelDir, 'added_tokens.json');
const glinerConfigPath = path.join(modelDir, 'gliner_config.json');

for (const [name, p] of [['tokenizer', tokenizerPath], ['model', modelPath], ['config', configPath]]) {
  if (!fs.existsSync(p)) {
    console.error(`${name} not found at ${p}`);
    process.exit(1);
  }
}

// Read GLiNER-specific config
const addedTokens = JSON.parse(fs.readFileSync(addedTokensPath, 'utf-8'));
const glinerConfig = JSON.parse(fs.readFileSync(glinerConfigPath, 'utf-8'));
const TOKEN_P = addedTokens['[P]'];
const TOKEN_E = addedTokens['[E]'];
const TOKEN_SEP_TEXT = addedTokens['[SEP_TEXT]'];
const MAX_WIDTH = glinerConfig.max_width || 8;
const THRESHOLD = glinerConfig.threshold || 0.5;

console.log(`Special tokens: [P]=${TOKEN_P}, [E]=${TOKEN_E}, [SEP_TEXT]=${TOKEN_SEP_TEXT}`);
console.log(`GLiNER config: max_width=${MAX_WIDTH}, threshold=${THRESHOLD}`);

// --- Test 1: Load tokenizer ---

console.log('\nTest 1: Load tokenizer');
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

// --- Test 2: Raw tokenization ---

console.log('\nTest 2: tokenize_raw');
function tokenizeRaw(text, maxIds = 256) {
  const textBytes = new TextEncoder().encode(text);
  const textPtr = alloc(textBytes.length);
  bytesIn(textPtr, textBytes);

  const outByteLen = maxIds * 4;
  const outPtr = alloc(outByteLen);

  const numIds = wasm.tokenize_raw(tokHandle, textPtr, size(textBytes.length), outPtr, maxIds);

  let ids = null;
  if (numIds > 0) {
    ids = read(Int32Array, outPtr, numIds);
  }

  free(textPtr, textBytes.length);
  free(outPtr, outByteLen);
  return ids;
}

const helloIds = tokenizeRaw('hello');
console.log(`  tokenize_raw("hello") = [${helloIds ? Array.from(helloIds).join(', ') : 'null'}]`);
if (!helloIds || helloIds.length === 0) {
  console.error('FAIL: tokenize_raw returned empty');
  process.exit(1);
}
// Should NOT contain CLS (101) or SEP (102)
if (Array.from(helloIds).includes(101) || Array.from(helloIds).includes(102)) {
  console.error('FAIL: raw tokenization includes [CLS] or [SEP]');
  process.exit(1);
}

const entitiesIds = tokenizeRaw('entities');
console.log(`  tokenize_raw("entities") = [${Array.from(entitiesIds).join(', ')}]`);
console.log('  PASS');

// --- Test 3: Load GLiNER model ---

console.log('\nTest 3: Load GLiNER model');
console.log('  Reading model file...');
const modelBytes = fs.readFileSync(modelPath);
console.log(`  Model size: ${(modelBytes.length / 1024 / 1024).toFixed(1)} MB`);

const configJson = fs.readFileSync(configPath, 'utf-8');

const modelPtr = alloc(modelBytes.length);
bytesIn(modelPtr, modelBytes);

const configBytes = new TextEncoder().encode(configJson);
const configPtr = alloc(configBytes.length);
bytesIn(configPtr, configBytes);

console.log('  Loading model...');
const t0 = performance.now();
const modelHandle = wasm.load_model_gliner(
  modelPtr, size(modelBytes.length),
  configPtr, size(configBytes.length),
);
const loadTime = (performance.now() - t0).toFixed(0);

free(modelPtr, modelBytes.length);
free(configPtr, configBytes.length);

if (modelHandle === 0) {
  console.error('FAIL: Failed to load GLiNER model');
  process.exit(1);
}
console.log(`  Model loaded (handle=${modelHandle}) in ${loadTime}ms`);
console.log('  PASS');

// --- Test 4: GLiNER NER inference ---

console.log('\nTest 4: GLiNER NER inference');

const text = 'John Smith works at Google in New York';
const labels = ['person', 'organization', 'location'];
console.log(`  Text: "${text}"`);
console.log(`  Labels: [${labels.join(', ')}]`);

// 4a. Split text into words
const words = text.split(/\s+/);
const wordStarts = [];
const wordEnds = [];
let charPos = 0;
for (const word of words) {
  const idx = text.indexOf(word, charPos);
  wordStarts.push(idx);
  wordEnds.push(idx + word.length);
  charPos = idx + word.length;
}
console.log(`  Words: [${words.join(', ')}] (${words.length} words)`);

// 4b. Build schema prefix
const schemaIds = [];
schemaIds.push(...Array.from(tokenizeRaw('(')));
schemaIds.push(TOKEN_P);
schemaIds.push(...Array.from(tokenizeRaw('entities')));
schemaIds.push(...Array.from(tokenizeRaw('(')));
for (const label of labels) {
  schemaIds.push(TOKEN_E);
  schemaIds.push(...Array.from(tokenizeRaw(label)));
}
schemaIds.push(...Array.from(tokenizeRaw(')')));
schemaIds.push(...Array.from(tokenizeRaw(')')));
schemaIds.push(TOKEN_SEP_TEXT);
console.log(`  Schema prefix: ${schemaIds.length} tokens`);

// 4c. Tokenize each word
const wordTokenCounts = [];
const textTokenIds = [];
for (const word of words) {
  const ids = tokenizeRaw(word.toLowerCase());
  wordTokenCounts.push(ids.length);
  textTokenIds.push(...Array.from(ids));
}
console.log(`  Text tokens: ${textTokenIds.length} sub-tokens for ${words.length} words`);

// 4d. Build input tensors
const maxLen = 512;
const seqLen = Math.min(schemaIds.length + textTokenIds.length, maxLen);

const inputIds = new Array(maxLen).fill(0);
const attentionMask = new Array(maxLen).fill(0);
const wordsMask = new Array(maxLen).fill(0);

for (let j = 0; j < Math.min(schemaIds.length, seqLen); j++) {
  inputIds[j] = schemaIds[j];
  attentionMask[j] = 1;
}

let pos = schemaIds.length;
let actualNumWords = 0;
for (let w = 0; w < words.length && pos < seqLen; w++) {
  const count = wordTokenCounts[w];
  const offset = wordTokenCounts.slice(0, w).reduce((a, b) => a + b, 0);
  let tokensAdded = 0;
  for (let t = 0; t < count && pos < seqLen; t++) {
    inputIds[pos] = textTokenIds[offset + t];
    attentionMask[pos] = 1;
    wordsMask[pos] = w + 1;
    pos++;
    tokensAdded++;
  }
  if (tokensAdded > 0) actualNumWords = w + 1;
}
console.log(`  Actual words in sequence: ${actualNumWords}`);
console.log(`  Total active tokens: ${pos}`);

// 4e. Build span_idx
const numSpans = actualNumWords * MAX_WIDTH;
const spanIdx = new Array(numSpans * 2).fill(0);
for (let w = 0; w < actualNumWords; w++) {
  for (let wi = 0; wi < MAX_WIDTH; wi++) {
    const spanPos = (w * MAX_WIDTH + wi) * 2;
    const endWord = w + wi;
    if (endWord < actualNumWords) {
      spanIdx[spanPos] = w;
      spanIdx[spanPos + 1] = endWord;
    }
  }
}
console.log(`  Span indices: ${numSpans} spans (${actualNumWords} words x ${MAX_WIDTH} max_width)`);

// 4f. Marshal to WASM
const idsI64 = new BigInt64Array(inputIds.map(BigInt));
const maskI64 = new BigInt64Array(attentionMask.map(BigInt));
const wmaskI64 = new BigInt64Array(wordsMask.map(BigInt));
const spanI64 = new BigInt64Array(spanIdx.map(BigInt));

const idsByteLen = idsI64.length * 8;
const maskByteLen = maskI64.length * 8;
const wmaskByteLen = wmaskI64.length * 8;
const spanByteLen = spanI64.length * 8;

const idsWasmPtr = alloc(idsByteLen);
const maskWasmPtr = alloc(maskByteLen);
const wmaskWasmPtr = alloc(wmaskByteLen);
const spanWasmPtr = alloc(spanByteLen);

write(BigInt64Array, idsWasmPtr, idsI64);
write(BigInt64Array, maskWasmPtr, maskI64);
write(BigInt64Array, wmaskWasmPtr, wmaskI64);
write(BigInt64Array, spanWasmPtr, spanI64);

const maxLogits = actualNumWords * MAX_WIDTH * labels.length;
const logitsByteLen = maxLogits * 4;
const logitsWasmPtr = alloc(logitsByteLen);
const metaByteLen = 3 * 4;
const metaWasmPtr = alloc(metaByteLen);

console.log('  Running GLiNER inference...');
const t1 = performance.now();
const numLogits = wasm.gliner(
  modelHandle,
  idsWasmPtr, size(idsI64.length),
  maskWasmPtr, size(maskI64.length),
  wmaskWasmPtr, size(wmaskI64.length),
  spanWasmPtr, size(spanI64.length),
  1, maxLen,
  logitsWasmPtr, metaWasmPtr,
);
const inferenceTime = (performance.now() - t1).toFixed(0);
console.log(`  Inference time: ${inferenceTime}ms`);

if (numLogits === 0) {
  console.error('FAIL: GLiNER inference returned 0 logits');
  // Cleanup
  free(idsWasmPtr, idsByteLen);
  free(maskWasmPtr, maskByteLen);
  free(wmaskWasmPtr, wmaskByteLen);
  free(spanWasmPtr, spanByteLen);
  free(logitsWasmPtr, logitsByteLen);
  free(metaWasmPtr, metaByteLen);
  process.exit(1);
}

const logits = read(Float32Array, logitsWasmPtr, numLogits);
const meta = read(Uint32Array, metaWasmPtr, 3);

free(idsWasmPtr, idsByteLen);
free(maskWasmPtr, maskByteLen);
free(wmaskWasmPtr, wmaskByteLen);
free(spanWasmPtr, spanByteLen);
free(logitsWasmPtr, logitsByteLen);
free(metaWasmPtr, metaByteLen);

const [numWordsOut, maxWidthOut, numLabelsOut] = meta;
console.log(`  Output: ${numLogits} logits, num_words=${numWordsOut}, max_width=${maxWidthOut}, num_labels=${numLabelsOut}`);

// Verify logits are finite
const allFinite = logits.every(v => isFinite(v));
if (!allFinite) {
  console.error('FAIL: non-finite logits detected');
  process.exit(1);
}

// 4g. Post-process: sigmoid + threshold + entity extraction
function sigmoidFn(x) { return 1.0 / (1.0 + Math.exp(-x)); }

const entities = [];
for (let w = 0; w < numWordsOut; w++) {
  for (let wi = 0; wi < maxWidthOut; wi++) {
    const endWord = w + wi;
    if (endWord >= numWordsOut) continue;
    for (let l = 0; l < numLabelsOut; l++) {
      const idx = (w * maxWidthOut + wi) * numLabelsOut + l;
      const score = sigmoidFn(logits[idx]);
      if (score >= THRESHOLD) {
        entities.push({
          text: words.slice(w, endWord + 1).join(' '),
          label: labels[l],
          start: wordStarts[w],
          end: wordEnds[endWord],
          score,
        });
      }
    }
  }
}

// Flat NER deduplication
entities.sort((a, b) => b.score - a.score);
const kept = [];
for (const ent of entities) {
  const overlaps = kept.some(k => !(ent.end <= k.start || ent.start >= k.end));
  if (!overlaps) kept.push(ent);
}
kept.sort((a, b) => a.start - b.start);

console.log(`  Entities found (${kept.length}):`);
for (const e of kept) {
  console.log(`    "${e.text}" [${e.label}] score=${e.score.toFixed(4)} (${e.start}:${e.end})`);
}

// Verify we found at least one entity
if (kept.length === 0) {
  console.error('FAIL: no entities detected');
  process.exit(1);
}

// Verify all scores are in valid range
for (const e of kept) {
  if (e.score < 0 || e.score > 1) {
    console.error(`FAIL: score ${e.score} out of range [0, 1]`);
    process.exit(1);
  }
}

console.log('  PASS');

// Cleanup
wasm.unload_model(modelHandle);
wasm.unload_tokenizer(tokHandle);
console.log('\nAll GLiNER tests passed!');
