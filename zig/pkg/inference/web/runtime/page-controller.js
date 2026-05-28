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

export function computeLoadAvailability({
  sourceMode,
  termiteReady,
  hasDirectoryPicker,
  hasSingleFile,
  hasDiscoveredModel,
  discoveredModelCount,
  remoteUrl,
}) {
  const normalizedSourceMode = sourceMode === 'url' ? 'url' : 'file';

  if (normalizedSourceMode === 'file') {
    const hasSelectedModel = Boolean(hasSingleFile || hasDiscoveredModel);
    const loadDisabled = !termiteReady || !hasSelectedModel;
    const loadDisabledReason = !termiteReady
      ? 'Wait for the top status to say Initialized (...).'
      : !hasSelectedModel
        ? 'Choose a supported local model bundle or single GGUF file first.'
        : '';
    return {
      loadDisabled,
      loadDisabledReason,
      loadHint: loadDisabled ? loadDisabledReason : 'Ready to load the selected model.',
      loadTitle: loadDisabled ? loadDisabledReason : 'Load the selected model',
      modelFileDisabled: false,
      discoveredModelDisabled: discoveredModelCount === 0,
      pickDirDisabled: !hasDirectoryPicker,
      modelUrlDisabled: true,
      streamLoadDisabled: true,
    };
  }

  const hasUrl = String(remoteUrl ?? '').trim().length > 0;
  const loadDisabled = !termiteReady || !hasUrl;
  const loadDisabledReason = !termiteReady
    ? 'Wait for the top status to say Initialized (...).'
    : !hasUrl
      ? 'Enter a remote GGUF URL first.'
      : '';
  return {
    loadDisabled,
    loadDisabledReason,
    loadHint: loadDisabled ? loadDisabledReason : 'Ready to load the selected model.',
    loadTitle: loadDisabled ? loadDisabledReason : 'Load the selected model',
    modelFileDisabled: true,
    discoveredModelDisabled: true,
    pickDirDisabled: true,
    modelUrlDisabled: false,
    streamLoadDisabled: false,
  };
}
