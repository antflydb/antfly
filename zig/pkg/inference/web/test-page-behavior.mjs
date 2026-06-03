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
  actionLabelForTask,
  buildExpandedGemma4MultimodalPrompt,
  buildExpandedMultimodalPrompt,
  computeMediaUiState,
  hasIntegratedVisionConfig,
  imageOffsetsFromExpandedTokenIds,
  supportsBrowserMultimodal,
} from './runtime/page-behavior.js';

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

console.log('Test 1: action labels follow the loaded model task');
if (actionLabelForTask('embed') !== 'Embed Text') fail('embed task label mismatch');
if (actionLabelForTask('clip-embed') !== 'Embed Text') fail('clip-embed task label mismatch');
if (actionLabelForTask('extract') !== 'Extract Entities') fail('extract task label mismatch');
if (actionLabelForTask('relations') !== 'Extract Relations') fail('relations task label mismatch');
if (actionLabelForTask('transcribe') !== 'Transcribe Audio') fail('transcribe task label mismatch');
if (actionLabelForTask('seq2seq') !== 'Generate Text') fail('seq2seq task label mismatch');
if (actionLabelForTask('generate') !== 'Generate Response') fail('generate task label mismatch');
if (actionLabelForTask('unknown') !== 'Run Model') fail('fallback task label mismatch');
console.log('  action labels are correct');

console.log('\nTest 2: multimodal availability covers integrated and external-projector cases');
const integratedConfig = {
  image_token_index: 32000,
  mm_tokens_per_image: 4,
  vision_config: { image_size: 896 },
};
if (!hasIntegratedVisionConfig(integratedConfig)) fail('integrated vision config should be detected');
if (!supportsBrowserMultimodal('generate', integratedConfig, null)) fail('integrated multimodal generate path should be supported');
if (supportsBrowserMultimodal('embed', integratedConfig, null)) fail('embed task should not report multimodal support');

const integratedState = computeMediaUiState({ task: 'generate', config: integratedConfig, projectorKind: null });
if (!integratedState.imageEnabled || integratedState.audioEnabled) {
  fail(`unexpected integrated media state: ${JSON.stringify(integratedState)}`);
}
if (!integratedState.imageHint.includes('896x896')) fail('integrated image hint should mention resize dimensions');

const gemma3State = computeMediaUiState({ task: 'generate', config: integratedConfig, projectorKind: 'antfly_gemma3' });
if (!gemma3State.imageEnabled || gemma3State.audioEnabled) fail('Gemma3 projector state should enable image only');

const gemma4ImageAudioState = computeMediaUiState({ task: 'generate', config: integratedConfig, projectorKind: 'clip_gemma4_image_audio' });
if (!gemma4ImageAudioState.imageEnabled || !gemma4ImageAudioState.audioEnabled) fail('Gemma4 image+audio projector state should enable both inputs');
if (!gemma4ImageAudioState.audioHint.includes('audio projector')) fail('Gemma4 audio hint should mention audio projector');

const gemma4AudioOnlyState = computeMediaUiState({ task: 'generate', config: integratedConfig, projectorKind: 'clip_gemma4_audio' });
if (gemma4AudioOnlyState.imageEnabled || !gemma4AudioOnlyState.audioEnabled) fail('Gemma4 audio-only projector state should disable image and enable audio');

const extractState = computeMediaUiState({ task: 'extract', config: integratedConfig, projectorKind: null });
if (extractState.imageEnabled || extractState.audioEnabled) fail('extract task should disable image and audio');

const relationState = computeMediaUiState({ task: 'relations', config: integratedConfig, projectorKind: null });
if (relationState.imageEnabled || relationState.audioEnabled) fail('relations task should disable image and audio');

const transcribeState = computeMediaUiState({ task: 'transcribe', config: integratedConfig, projectorKind: null });
if (transcribeState.imageEnabled || !transcribeState.audioEnabled) fail('transcribe task should enable audio only');
if (!transcribeState.audioHint.includes('transcribe')) fail('transcribe audio hint should mention transcription');
console.log('  media availability and hints are correct');

console.log('\nTest 3: multimodal prompt expansion handles integrated and Gemma4 placeholders');
const integratedPrompt = buildExpandedMultimodalPrompt('Describe this <start_of_image> briefly.', integratedConfig, 1);
if (!integratedPrompt.includes('<image_soft_token><image_soft_token><image_soft_token><image_soft_token>')) {
  fail('integrated multimodal prompt did not expand image soft tokens');
}

const implicitGemma4Prompt = buildExpandedGemma4MultimodalPrompt('Summarize the scene.', [2], [1]);
if (!implicitGemma4Prompt.startsWith('<|audio><|audio|><audio|>\n<|image><|image|><|image|><image|>\n')) {
  fail(`Gemma4 implicit prompt expansion mismatch: ${implicitGemma4Prompt}`);
}

const explicitGemma4Prompt = buildExpandedGemma4MultimodalPrompt(
  'Compare <|image|> and <|audio|> evidence.',
  [3],
  [2],
);
if (!explicitGemma4Prompt.includes('<|image><|image|><|image|><|image|><image|>')) {
  fail('Gemma4 explicit image placeholder was not expanded');
}
if (!explicitGemma4Prompt.includes('<|audio><|audio|><|audio|><audio|>')) {
  fail('Gemma4 explicit audio placeholder was not expanded');
}
console.log('  multimodal prompt expansion is correct');

console.log('\nTest 4: integrated image offsets are recovered from expanded token ids');
const tokenIds = [111, 32001, 32000, 32000, 32000, 32000, 32002, 222];
const offset = imageOffsetsFromExpandedTokenIds(tokenIds, {
  image_token_index: 32000,
  boi_token_index: 32001,
  eoi_token_index: 32002,
  mm_tokens_per_image: 4,
}, 1);
if (offset.length !== 1 || offset[0] !== 2) {
  fail(`unexpected image offsets: ${offset}`);
}
console.log('  image offsets are derived correctly from expanded multimodal token ids');

console.log('\nPage behavior test passed.');
