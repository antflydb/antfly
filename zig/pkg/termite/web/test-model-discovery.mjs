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

import {
  basename,
  buildDiscoveredModelEntries,
  isInterestingModelFile,
} from './runtime/model-discovery.js';

function makeFile(path, json = undefined) {
  return {
    path,
    handle: { path },
    json,
  };
}

const files = [
  makeFile('ggml-org/gemma-4-e2b-it-gguf/gemma-4-E2B-it-Q8_0.gguf'),
  makeFile('ggml-org/gemma-4-e2b-it-gguf/mmproj-gemma-4-E2B-it-bf16.gguf'),

  makeFile('openai-community/gpt2/model.safetensors'),
  makeFile('openai-community/gpt2/config.json', { model_type: 'gpt2' }),
  makeFile('openai-community/gpt2/tokenizer.json'),
  makeFile('openai-community/gpt2/tokenizer_config.json', { chat_template: '{{ user }}' }),

  makeFile('google/mt5-small/model.safetensors'),
  makeFile('google/mt5-small/config.json', { model_type: 'mt5' }),
  makeFile('google/mt5-small/tokenizer.json'),

  makeFile('sentence-transformers/all-MiniLM-L6-v2/model.safetensors'),
  makeFile('sentence-transformers/all-MiniLM-L6-v2/config.json', { model_type: 'bert' }),
  makeFile('sentence-transformers/all-MiniLM-L6-v2/tokenizer.json'),

  makeFile('openai/clip-vit-base-patch32/model.safetensors'),
  makeFile('openai/clip-vit-base-patch32/config.json', { model_type: 'clip' }),
  makeFile('openai/clip-vit-base-patch32/tokenizer.json'),

  makeFile('openai/whisper-small/model.safetensors'),
  makeFile('openai/whisper-small/config.json', { model_type: 'whisper' }),
  makeFile('openai/whisper-small/tokenizer.json'),

  makeFile('urchade/gliner_small-v2.1/model.safetensors'),
  makeFile('urchade/gliner_small-v2.1/config.json', { model_type: 'extractor' }),
  makeFile('urchade/gliner_small-v2.1/gliner_config.json', { model_type: 'gliner2', max_width: 12, default_labels: ['person', 'organization'] }),
  makeFile('urchade/gliner_small-v2.1/tokenizer.json'),

  makeFile('Babelscape/rebel-large/model.safetensors'),
  makeFile('Babelscape/rebel-large/config.json', { model_type: 'bart' }),
  makeFile('Babelscape/rebel-large/rebel_config.json', { max_length: 256, triplet_token: '<triplet>', subject_token: '<subj>', object_token: '<obj>' }),
  makeFile('Babelscape/rebel-large/tokenizer.json'),

  makeFile('microsoft/phi-vision/model.safetensors'),
  makeFile('microsoft/phi-vision/config.json', { model_type: 'phi3_v' }),
  makeFile('microsoft/phi-vision/tokenizer.json'),
];

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

console.log('Test 1: bundle discovery classifies supported and unsupported local model bundles');
const entries = await buildDiscoveredModelEntries(files, {
  readJsonFile: async (file) => {
    if (file.json === undefined) throw new Error(`missing json fixture for ${file.path}`);
    return file.json;
  },
});

if (entries.length !== 9) fail(`expected 9 discovered entries, got ${entries.length}`);

const ggufEntry = entries.find((entry) => entry.kind === 'gguf');
if (!ggufEntry) fail('missing GGUF entry');
if (!ggufEntry.projectorFile || basename(ggufEntry.projectorFile.path) !== 'mmproj-gemma-4-E2B-it-bf16.gguf') {
  fail('GGUF entry did not preserve its mmproj sidecar');
}
if (!ggufEntry.label.includes('[GGUF] + mmproj')) {
  fail(`GGUF label did not include mmproj suffix: ${ggufEntry.label}`);
}

const gptEntry = entries.find((entry) => entry.bundlePath === 'openai-community/gpt2');
if (!gptEntry || gptEntry.taskHint !== 'generate' || !gptEntry.supported) {
  fail('GPT SafeTensors bundle was not classified as supported generate task');
}
if (!gptEntry.label.includes('[HF SafeTensors GPT]')) {
  fail(`unexpected GPT label: ${gptEntry?.label}`);
}

const seq2seqEntry = entries.find((entry) => entry.bundlePath === 'google/mt5-small');
if (!seq2seqEntry || seq2seqEntry.taskHint !== 'seq2seq') {
  fail('mT5 bundle was not classified as seq2seq');
}

const embedEntry = entries.find((entry) => entry.bundlePath === 'sentence-transformers/all-MiniLM-L6-v2');
if (!embedEntry || embedEntry.taskHint !== 'embed') {
  fail('BERT bundle was not classified as embed');
}

const clipEntry = entries.find((entry) => entry.bundlePath === 'openai/clip-vit-base-patch32');
if (!clipEntry || clipEntry.taskHint !== 'clip-embed') {
  fail('CLIP bundle was not classified as clip-embed');
}

const unsupportedEntry = entries.find((entry) => entry.bundlePath === 'microsoft/phi-vision');
if (!unsupportedEntry || unsupportedEntry.supported !== false) {
  fail('unsupported HF bundle was not preserved');
}
if (unsupportedEntry.unsupportedReason !== 'model_type=phi3_v not wired into this page') {
  fail(`unexpected unsupported reason: ${unsupportedEntry.unsupportedReason}`);
}
if (!unsupportedEntry.label.includes('[Unsupported: model_type=phi3_v not wired into this page]')) {
  fail(`unexpected unsupported label: ${unsupportedEntry.label}`);
}

const whisperEntry = entries.find((entry) => entry.bundlePath === 'openai/whisper-small');
if (!whisperEntry || whisperEntry.supported !== true || whisperEntry.taskHint !== 'transcribe') {
  fail('whisper bundle should now be classified as a supported transcription task');
}
if (!whisperEntry.label.includes('[HF SafeTensors Whisper]')) {
  fail(`unexpected whisper label: ${whisperEntry?.label}`);
}

const glinerEntry = entries.find((entry) => entry.bundlePath === 'urchade/gliner_small-v2.1');
if (!glinerEntry || glinerEntry.supported !== true || glinerEntry.taskHint !== 'extract') {
  fail('GLiNER extractor bundle should now be classified as a supported extraction task');
}
if (!glinerEntry.glinerConfigFile || glinerEntry.glinerConfigFile.path !== 'urchade/gliner_small-v2.1/gliner_config.json') {
  fail('GLiNER bundle did not preserve its gliner_config.json sidecar');
}
if (!glinerEntry.label.includes('[HF SafeTensors GLiNER]')) {
  fail(`unexpected GLiNER label: ${glinerEntry?.label}`);
}

const bartEntry = entries.find((entry) => entry.bundlePath === 'Babelscape/rebel-large');
if (!bartEntry || bartEntry.supported !== true || bartEntry.taskHint !== 'relations') {
  fail('REBEL bundle should now be classified as a supported relation-extraction task');
}
if (!bartEntry.rebelConfigFile || bartEntry.rebelConfigFile.path !== 'Babelscape/rebel-large/rebel_config.json') {
  fail('REBEL bundle did not preserve its rebel_config.json sidecar');
}
if (!bartEntry.label.includes('[HF SafeTensors REBEL]')) {
  fail(`unexpected REBEL label: ${bartEntry?.label}`);
}
console.log('  classified GGUF, HF GPT, Whisper, GLiNER, REBEL, seq2seq, encoder, CLIP, and family-specific unsupported bundles as expected');

console.log('\nTest 2: interesting-file filter matches the local models-directory picker rules');
const interestingNames = [
  'model.gguf',
  'model.safetensors',
  'config.json',
  'gliner_config.json',
  'rebel_config.json',
  'tokenizer.json',
  'tokenizer_config.json',
];
for (const name of interestingNames) {
  if (!isInterestingModelFile(name)) fail(`expected ${name} to be collected by the directory picker`);
}
const ignoredNames = [
  'merges.txt',
  'vocab.json',
  'readme.md',
  'weights.bin',
];
for (const name of ignoredNames) {
  if (isInterestingModelFile(name)) fail(`expected ${name} to be ignored by the directory picker`);
}
console.log('  interesting-file filter matches current local bundle discovery rules');

console.log('\nModel discovery test passed.');
