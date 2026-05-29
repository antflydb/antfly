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

import { computeLoadAvailability } from './runtime/page-controller.js';

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

console.log('Test 1: local model mode stays gated until runtime init and a selection exist');
const waitingState = computeLoadAvailability({
  sourceMode: 'file',
  inferenceReady: false,
  hasDirectoryPicker: true,
  hasSingleFile: false,
  hasDiscoveredModel: false,
  discoveredModelCount: 0,
  remoteUrl: '',
});
if (!waitingState.loadDisabled) fail('load button should stay disabled before init');
if (waitingState.loadHint !== 'Wait for the top status to say Initialized (...).') fail(`unexpected pre-init hint: ${waitingState.loadHint}`);
if (waitingState.modelFileDisabled) fail('single file input should stay enabled in local mode');
if (!waitingState.discoveredModelDisabled) fail('discovered model select should be disabled when there are no discovered bundles');
if (waitingState.pickDirDisabled) fail('directory picker should stay enabled when supported');
if (!waitingState.modelUrlDisabled || !waitingState.streamLoadDisabled) fail('remote controls should be disabled in local mode');
console.log('  pre-init local mode gating is correct');

console.log('\nTest 2: local model mode enables load for either a single GGUF or a discovered bundle');
const localBundleState = computeLoadAvailability({
  sourceMode: 'file',
  inferenceReady: true,
  hasDirectoryPicker: true,
  hasSingleFile: false,
  hasDiscoveredModel: true,
  discoveredModelCount: 3,
  remoteUrl: '',
});
if (localBundleState.loadDisabled) fail('discovered local bundle should enable load');
if (localBundleState.loadHint !== 'Ready to load the selected model.') fail(`unexpected local ready hint: ${localBundleState.loadHint}`);
if (localBundleState.discoveredModelDisabled) fail('discovered model select should remain enabled when entries exist');

const localFileState = computeLoadAvailability({
  sourceMode: 'file',
  inferenceReady: true,
  hasDirectoryPicker: false,
  hasSingleFile: true,
  hasDiscoveredModel: false,
  discoveredModelCount: 0,
  remoteUrl: '',
});
if (localFileState.loadDisabled) fail('single local GGUF file should enable load');
if (!localFileState.pickDirDisabled) fail('directory picker should be disabled when unsupported');
console.log('  local single-file and discovered-bundle states are correct');

console.log('\nTest 3: remote URL mode requires a URL and flips the enabled controls');
const remoteEmptyState = computeLoadAvailability({
  sourceMode: 'url',
  inferenceReady: true,
  hasDirectoryPicker: true,
  hasSingleFile: true,
  hasDiscoveredModel: true,
  discoveredModelCount: 4,
  remoteUrl: '   ',
});
if (!remoteEmptyState.loadDisabled) fail('empty remote URL should keep load disabled');
if (remoteEmptyState.loadHint !== 'Enter a remote GGUF URL first.') fail(`unexpected remote-empty hint: ${remoteEmptyState.loadHint}`);
if (!remoteEmptyState.modelFileDisabled || !remoteEmptyState.discoveredModelDisabled || !remoteEmptyState.pickDirDisabled) {
  fail('local controls should be disabled in remote URL mode');
}
if (remoteEmptyState.modelUrlDisabled || remoteEmptyState.streamLoadDisabled) {
  fail('remote URL controls should remain enabled in remote mode');
}

const remoteReadyState = computeLoadAvailability({
  sourceMode: 'url',
  inferenceReady: true,
  hasDirectoryPicker: true,
  hasSingleFile: false,
  hasDiscoveredModel: false,
  discoveredModelCount: 0,
  remoteUrl: 'https://example.com/model.gguf',
});
if (remoteReadyState.loadDisabled) fail('non-empty remote URL should enable load');
if (remoteReadyState.loadTitle !== 'Load the selected model') fail(`unexpected remote-ready title: ${remoteReadyState.loadTitle}`);
console.log('  remote URL mode gating is correct');

console.log('\nPage controller test passed.');
