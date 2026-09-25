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

function fmtNumber(value) {
  return typeof value === 'number' ? value.toLocaleString() : 'n/a';
}

function formatGiB(byteLen) {
  return `${(byteLen / (1024 ** 3)).toFixed(2)} GiB`;
}

export function computeMetricEntries({ task, modelConfig, sourceInfo }) {
  if (!modelConfig) return [];
  return [
    ['Task', task ?? 'unknown'],
    ['Model type', modelConfig.model_type ?? 'unknown'],
    ['Vocab size', fmtNumber(modelConfig.vocab_size)],
    ['Layers', fmtNumber(modelConfig.num_hidden_layers)],
    ['Hidden size', fmtNumber(modelConfig.hidden_size)],
    ['Max positions', fmtNumber(modelConfig.max_position_embeddings)],
    ['Source', sourceInfo?.label ?? 'n/a'],
    ['Size', typeof sourceInfo?.sizeBytes === 'number' ? formatGiB(sourceInfo.sizeBytes) : 'n/a'],
  ];
}

export function computeLoadWarnings({
  task,
  sourceInfo,
  projectorKind,
  projectorFilePath,
  chatTemplate,
}) {
  const warnings = [];

  if (typeof sourceInfo?.sizeBytes === 'number' && sourceInfo.sizeBytes > 3.5 * 1024 ** 3) {
    warnings.push({
      title: 'Large model warning',
      text: 'This model bundle is close to or beyond the practical browser memory range. For very large models, the wasm64/Electron path is still the safer target.',
    });
  }

  if (projectorKind) {
    warnings.push({
      title: 'External projector ready',
      text: projectorFilePath
        ? `Loaded ${projectorFilePath} as ${projectorKind}. The page will use the external projector for multimodal generation when images are selected.`
        : `Loaded external projector ${projectorKind}.`,
    });
  }

  if (chatTemplate) {
    warnings.push({
      title: 'Chat template ready',
      text: 'The selected model bundle includes a chat template. This page will render a system + user turn before tokenization unless you disable that toggle.',
    });
  } else if (task === 'generate') {
    warnings.push({
      title: 'Raw prompt mode',
      text: 'No chat template was found in the selected model bundle. Prompt text will be tokenized exactly as written.',
    });
  }

  if (task === 'embed') {
    warnings.push({
      title: 'Embedding mode',
      text: 'This bundle loaded as a text encoder. The action button now tokenizes the prompt and returns a single embedding vector preview instead of autoregressive generation.',
    });
  } else if (task === 'extract') {
    warnings.push({
      title: 'Extraction mode',
      text: 'This bundle loaded as a GLiNER extraction model. Provide candidate entity labels and the action button will return extracted spans instead of text generation.',
    });
  } else if (task === 'relations') {
    warnings.push({
      title: 'Relation extraction mode',
      text: 'This bundle loaded as a REBEL/BART relation extraction model. The action button now runs encoder-decoder generation and parses entity/relation triples instead of returning plain generated text.',
    });
  } else if (task === 'transcribe') {
    warnings.push({
      title: 'Transcription mode',
      text: 'This bundle loaded as a Whisper speech-to-text model. Select an audio clip and the action button will run browser transcription instead of text generation.',
    });
  } else if (task === 'clip-embed') {
    warnings.push({
      title: 'CLIP mode',
      text: 'This bundle loaded as a CLIP text encoder. The action button now returns a CLIP text embedding preview for the prompt.',
    });
  } else if (task === 'seq2seq') {
    warnings.push({
      title: 'Seq2Seq mode',
      text: 'This bundle loaded as a T5-style encoder-decoder model. The action button now runs text-to-text generation instead of GPT KV-cache decoding.',
    });
  }

  return warnings;
}
