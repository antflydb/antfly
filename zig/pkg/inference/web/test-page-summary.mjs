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

import { computeLoadWarnings, computeMetricEntries } from './runtime/page-summary.js';

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

console.log('Test 1: metric summary reflects task, model metadata, and source info');
const metrics = computeMetricEntries({
  task: 'generate',
  modelConfig: {
    model_type: 'gemma4',
    vocab_size: 262144,
    num_hidden_layers: 34,
    hidden_size: 2560,
    max_position_embeddings: 32768,
  },
  sourceInfo: {
    label: 'ggml-org/gemma-4-e2b-it-gguf / gemma-4-E2B-it-Q8_0.gguf [GGUF] + mmproj',
    sizeBytes: 4.63 * 1024 ** 3,
  },
});
if (metrics.length !== 8) fail(`expected 8 metric rows, got ${metrics.length}`);
if (metrics[0][0] !== 'Task' || metrics[0][1] !== 'generate') fail(`unexpected task metric: ${JSON.stringify(metrics[0])}`);
if (metrics[6][0] !== 'Source' || !String(metrics[6][1]).includes('GGUF')) fail(`unexpected source metric: ${JSON.stringify(metrics[6])}`);
if (metrics[7][0] !== 'Size' || !String(metrics[7][1]).endsWith('GiB')) fail(`unexpected size metric: ${JSON.stringify(metrics[7])}`);
console.log('  metric summary is correct');

console.log('\nTest 2: load warnings cover large models, projector readiness, and chat-template/raw-prompt modes');
const generateWarnings = computeLoadWarnings({
  task: 'generate',
  sourceInfo: { sizeBytes: 4.63 * 1024 ** 3 },
  projectorKind: 'clip_gemma4_image_audio',
  projectorFilePath: 'mmproj-gemma-4-E2B-it-bf16.gguf',
  chatTemplate: '',
});
const titles = generateWarnings.map((warning) => warning.title);
if (!titles.includes('Large model warning')) fail(`missing large model warning: ${titles}`);
if (!titles.includes('External projector ready')) fail(`missing projector warning: ${titles}`);
if (!titles.includes('Raw prompt mode')) fail(`missing raw prompt warning: ${titles}`);

const projectorWarning = generateWarnings.find((warning) => warning.title === 'External projector ready');
if (!projectorWarning?.text.includes('mmproj-gemma-4-E2B-it-bf16.gguf')) {
  fail(`unexpected projector warning text: ${projectorWarning?.text}`);
}

const chatTemplateWarnings = computeLoadWarnings({
  task: 'generate',
  sourceInfo: { sizeBytes: null },
  projectorKind: null,
  projectorFilePath: null,
  chatTemplate: '{{ user }}',
});
if (!chatTemplateWarnings.some((warning) => warning.title === 'Chat template ready')) {
  fail('chat template warning missing for templated model');
}
if (chatTemplateWarnings.some((warning) => warning.title === 'Raw prompt mode')) {
  fail('raw prompt warning should not appear when a chat template is available');
}
console.log('  generate-mode warnings are correct');

console.log('\nTest 3: task-specific mode warnings cover encoder, CLIP, and seq2seq flows');
const embedWarnings = computeLoadWarnings({
  task: 'embed',
  sourceInfo: { sizeBytes: 128 * 1024 ** 2 },
  projectorKind: null,
  projectorFilePath: null,
  chatTemplate: null,
});
if (!embedWarnings.some((warning) => warning.title === 'Embedding mode')) fail('embedding mode warning missing');

const extractWarnings = computeLoadWarnings({
  task: 'extract',
  sourceInfo: { sizeBytes: 192 * 1024 ** 2 },
  projectorKind: null,
  projectorFilePath: null,
  chatTemplate: null,
});
if (!extractWarnings.some((warning) => warning.title === 'Extraction mode')) fail('extraction mode warning missing');

const relationWarnings = computeLoadWarnings({
  task: 'relations',
  sourceInfo: { sizeBytes: 320 * 1024 ** 2 },
  projectorKind: null,
  projectorFilePath: null,
  chatTemplate: null,
});
if (!relationWarnings.some((warning) => warning.title === 'Relation extraction mode')) fail('relation extraction mode warning missing');

const transcribeWarnings = computeLoadWarnings({
  task: 'transcribe',
  sourceInfo: { sizeBytes: 256 * 1024 ** 2 },
  projectorKind: null,
  projectorFilePath: null,
  chatTemplate: null,
});
if (!transcribeWarnings.some((warning) => warning.title === 'Transcription mode')) fail('transcription mode warning missing');

const clipWarnings = computeLoadWarnings({
  task: 'clip-embed',
  sourceInfo: { sizeBytes: 256 * 1024 ** 2 },
  projectorKind: null,
  projectorFilePath: null,
  chatTemplate: null,
});
if (!clipWarnings.some((warning) => warning.title === 'CLIP mode')) fail('CLIP mode warning missing');

const seq2seqWarnings = computeLoadWarnings({
  task: 'seq2seq',
  sourceInfo: { sizeBytes: 512 * 1024 ** 2 },
  projectorKind: null,
  projectorFilePath: null,
  chatTemplate: null,
});
if (!seq2seqWarnings.some((warning) => warning.title === 'Seq2Seq mode')) fail('Seq2Seq mode warning missing');
console.log('  task-specific warnings are correct');

console.log('\nPage summary test passed.');
