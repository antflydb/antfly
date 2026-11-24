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

// Real-checkpoint retrieval check for the browser `embed` task.
//
//   EXTRACTION_WASM=zig-out/antfly-extraction-cpu.wasm \
//   EMBEDDING_GEMMA2_MODEL=/path/to/embeddinggemma-2 \
//   [EMBEDDING_GEMMA2_TEXT_MODEL=/path/to/text-only-copy] \
//   node --test pkg/inference/web/test-embedding-gemma2-retrieval.mjs
//
// The text-only copy is what examples/cloudflare-serverless/publish-local-model.mjs
// publishes; it must embed text exactly like the full multimodal bundle.
import './configuration-test-runtime.mjs';
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFile, readdir } from 'node:fs/promises';
import { openAsBlob } from 'node:fs';
import { join, relative } from 'node:path';
import { ExtractionSession } from './runtime/extraction-session.js';
import { createWasmAbi } from './runtime/wasm-abi.js';

const wasmPath = process.env.EXTRACTION_WASM;
const modelDir = process.env.EMBEDDING_GEMMA2_MODEL;
const textDir = process.env.EMBEDDING_GEMMA2_TEXT_MODEL;
const skip = !wasmPath || !modelDir;

async function bundle(dir) {
  const files = new Map();
  async function walk(at) {
    for (const entry of await readdir(at, { withFileTypes: true })) {
      const path = join(at, entry.name);
      if (entry.isDirectory()) await walk(path);
      else if (/\.(json|safetensors|model|jinja)$/.test(entry.name) && entry.name !== 'catalog.json') files.set(relative(dir, path), await openAsBlob(path));
    }
  }
  await walk(dir);
  return files;
}
async function load(dir) {
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const session = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  await session.load(await bundle(dir), 'bf16');
  return session;
}
const embed = (session, inputs, task_type, dimensions = 256) =>
  session.run({ inputs, task_type, dimensions, max_tokens: 256 }, false, 'embed').value;
const cosine = (a, b) => {
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i]; }
  return dot / Math.sqrt(na * nb);
};

const mail = [
  'Invoice #4471 from Rapid Plumbing\n\nThanks for having us out to fix the burst pipe under the kitchen sink. The total is $340, due in 30 days.',
  'Team offsite agenda\n\nWe will hike in the morning, then strategy sessions after lunch and dinner at the lodge. Bring boots.',
  'Your flight to Lisbon is confirmed\n\nTAP Air Portugal flight TP 202 departs Friday at 18:40. Check in opens 24 hours before departure.',
  'Quarterly security training reminder\n\nPlease complete the phishing awareness module before the end of the month.',
  'Dinner on Saturday?\n\nWe are trying the new ramen place downtown at 7. Let me know if you can make it!',
];
const queries = [
  ['how much do I owe the plumber', 0],
  ['what are we doing at the company retreat', 1],
  ['when does my plane leave', 2],
  ['do I have mandatory compliance training', 3],
  ['weekend noodle plans with friends', 4],
];

test('EmbeddingGemma2 retrieval embeddings rank the right message first', { skip, timeout: 1800000 }, async () => {
  const session = await load(modelDir);
  try {
    const docs = embed(session, mail, 'RETRIEVAL_DOCUMENT');
    assert.equal(docs.embeddings.length, mail.length);
    assert.equal(docs.dimensions, 256);
    for (const vector of docs.embeddings) assert.ok(vector.length === 256 && vector.every(Number.isFinite));
    let correct = 0;
    for (const [query, expected] of queries) {
      const [q] = embed(session, [query], 'RETRIEVAL_QUERY').embeddings;
      const scores = docs.embeddings.map((d) => cosine(q, d));
      const best = scores.indexOf(Math.max(...scores));
      if (best === expected) correct++;
      console.log(`${best === expected ? 'ok  ' : 'MISS'} ${query} -> ${best} (${scores.map((s) => s.toFixed(3)).join(' ')})`);
    }
    assert.equal(correct, queries.length, 'every query retrieves its message');
    // Matryoshka: the 128-d prefix of a 256-d request equals a 128-d request (after renormalization).
    const small = embed(session, [mail[0]], 'RETRIEVAL_DOCUMENT', 128).embeddings[0];
    assert.ok(cosine(small, docs.embeddings[0].slice(0, 128)) > 0.999);
    // Long input truncates to max_tokens instead of failing.
    assert.equal(embed(session, [mail[1].repeat(200)], 'RETRIEVAL_DOCUMENT').embeddings.length, 1);
    if (textDir) {
      const stripped = await load(textDir);
      try {
        const textDocs = embed(stripped, mail, 'RETRIEVAL_DOCUMENT');
        for (let i = 0; i < mail.length; i++)
          assert.ok(cosine(textDocs.embeddings[i], docs.embeddings[i]) > 0.99999, `text-only copy embeds message ${i} identically`);
      } finally {
        stripped.unload();
      }
    }
  } finally {
    session.unload();
  }
});
