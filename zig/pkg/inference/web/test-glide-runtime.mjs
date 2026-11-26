// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

import './configuration-test-runtime.mjs';
import assert from 'node:assert/strict';
import { openAsBlob } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { test } from 'node:test';
import { ExtractionSession } from './runtime/extraction-session.js';
import { createWasmAbi } from './runtime/wasm-abi.js';

const { GLIDE_MODEL, EXTRACTION_WASM } = process.env;
const nativeCases = process.env.GLIDE_NATIVE_CASES ?? new URL('./glide-native-cases.json', import.meta.url);
test('released GLiDE checkpoint matches native typed decisions in CPU WASM', {
  skip: !GLIDE_MODEL || !EXTRACTION_WASM,
  timeout: 240_000,
}, async () => {
  const names = [
    'config.json', 'decision_config.json', 'tokenizer.json', 'tokenizer_config.json',
    'model.safetensors.index.json', 'model-00001-of-00001.safetensors', 'readout.safetensors',
  ];
  const files = new Map(await Promise.all(names.map(async name => [name, await openAsBlob(join(GLIDE_MODEL, name))])));
  const { instance } = await WebAssembly.instantiate(await readFile(EXTRACTION_WASM), { env: {} });
  const session = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  try {
    await session.load(files, 'bf16');
    const cases = JSON.parse(await readFile(nativeCases, 'utf8'));
    assert(cases.length > 0, 'Native oracle must include at least one decision request');
    const image = [{ role: 'user', content: [{ type: 'input_image', image_url: 'https://example.invalid/mail.png' }] }];
    assert.throws(() => session.run({ ...cases[0].request, input: image }, false, 'decide'), /InvalidDecideRequest/);
    for (const { request, response } of cases) {
      const { value: actual } = session.run(request, false, 'decide');
      assert.equal(actual.usage.input_tokens, response.usage.input_tokens);
      assert.deepEqual(actual.answers?.map(a => a.name), response.answers?.map(a => a.name));
      for (let i = 0; i < response.answers.length; i++) {
        const got = actual.answers[i], expected = response.answers[i];
        assert.equal(got.choice, expected.choice);
        if (expected.score !== undefined) assert(Math.abs(got.score - expected.score) <= 0.02);
        if (expected.probability !== undefined) assert(Math.abs(got.probability - expected.probability) <= 0.01);
        if (expected.probabilities) {
          assert.deepEqual(got.probabilities.map(p => p.value), expected.probabilities.map(p => p.value));
          for (const [j, entry] of got.probabilities.entries()) {
            assert(Math.abs(entry.probability - expected.probabilities[j].probability) <= 0.01, entry.value);
          }
        }
        assert(Math.abs(got.confidence - expected.confidence) <= 0.01, got.name);
      }
    }
  } finally {
    session.unload();
  }
});
