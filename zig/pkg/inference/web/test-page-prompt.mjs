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

import { computePromptModeState } from './runtime/page-prompt.js';

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

console.log('Test 1: unloaded state prompts the user to load a model');
const unloaded = computePromptModeState({
  modelLoaded: false,
  tokenizerLoaded: false,
  modelConfig: null,
  task: null,
  chatTemplate: null,
  useChatTemplate: false,
});
if (unloaded.promptMeta !== 'Load a model to start generating.') fail(`unexpected unloaded prompt meta: ${unloaded.promptMeta}`);
if (!unloaded.useChatTemplateDisabled || !unloaded.systemPromptDisabled) fail('unloaded state should disable prompt-template controls');
console.log('  unloaded prompt state is correct');

console.log('\nTest 2: task-specific prompt meta covers encoder, CLIP, and seq2seq modes');
const sharedConfig = { model_type: 'test' };
const embedState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'embed',
  chatTemplate: null,
  useChatTemplate: false,
});
if (!embedState.promptMeta.includes('single embedding vector preview')) fail('embed prompt meta mismatch');

const clipState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'clip-embed',
  chatTemplate: null,
  useChatTemplate: false,
});
if (!clipState.promptMeta.includes('CLIP text embedding preview')) fail('CLIP prompt meta mismatch');

const seq2seqState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'seq2seq',
  chatTemplate: null,
  useChatTemplate: false,
});
if (!seq2seqState.promptMeta.includes('encoder-decoder text generation')) fail('seq2seq prompt meta mismatch');

const extractState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'extract',
  chatTemplate: null,
  useChatTemplate: false,
});
if (!extractState.promptMeta.includes('candidate entity labels')) fail('extract prompt meta mismatch');
if (!extractState.useChatTemplateDisabled || !extractState.systemPromptDisabled) fail('extract state should disable chat-template controls');

const relationState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'relations',
  chatTemplate: null,
  useChatTemplate: false,
});
if (!relationState.promptMeta.includes('relation extraction')) fail('relations prompt meta mismatch');
if (!relationState.useChatTemplateDisabled || !relationState.systemPromptDisabled) fail('relations state should disable chat-template controls');

const transcribeState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'transcribe',
  chatTemplate: null,
  useChatTemplate: false,
});
if (!transcribeState.promptMeta.includes('Select an audio clip')) fail('transcribe prompt meta mismatch');
if (!transcribeState.useChatTemplateDisabled || !transcribeState.systemPromptDisabled) fail('transcribe state should disable chat-template controls');
console.log('  task-specific prompt meta is correct');

console.log('\nTest 3: generate mode prompt meta distinguishes no-template, active-template, and raw-template states');
const noTemplateState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'generate',
  chatTemplate: '',
  useChatTemplate: false,
});
if (!noTemplateState.promptMeta.includes('No chat template was found')) fail('no-template prompt meta mismatch');
if (!noTemplateState.useChatTemplateDisabled || !noTemplateState.systemPromptDisabled) fail('no-template state should disable chat-template controls');

const activeTemplateState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'generate',
  chatTemplate: '{{ user }}',
  useChatTemplate: true,
});
if (!activeTemplateState.promptMeta.includes('chat template is active')) fail('active-template prompt meta mismatch');
if (activeTemplateState.useChatTemplateDisabled || activeTemplateState.systemPromptDisabled) fail('active-template state should enable both controls');

const rawTemplateState = computePromptModeState({
  modelLoaded: true,
  tokenizerLoaded: true,
  modelConfig: sharedConfig,
  task: 'generate',
  chatTemplate: '{{ user }}',
  useChatTemplate: false,
});
if (!rawTemplateState.promptMeta.includes('raw prompt mode is selected')) fail('raw-template prompt meta mismatch');
if (rawTemplateState.useChatTemplateDisabled || !rawTemplateState.systemPromptDisabled) fail('raw-template state should keep the toggle enabled but disable the system prompt');
console.log('  generate-mode prompt meta is correct');

console.log('\nPage prompt test passed.');
