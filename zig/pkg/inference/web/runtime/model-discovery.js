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

export function basename(path) {
  const slash = path.lastIndexOf('/');
  return slash >= 0 ? path.slice(slash + 1) : path;
}

export function parentPath(path) {
  const slash = path.lastIndexOf('/');
  return slash >= 0 ? path.slice(0, slash) : '';
}

export function isProjectorGgufName(name) {
  const lowered = name.toLowerCase();
  return lowered.includes('mmproj') || lowered.includes('projector');
}

export function hasBasename(file, expected) {
  return basename(file.path).toLowerCase() === expected.toLowerCase();
}

export function safetensorsFilesForDir(dirFiles) {
  return dirFiles.filter((file) => basename(file.path).toLowerCase().endsWith('.safetensors'));
}

export function isInterestingModelFile(entryName) {
  const lowered = entryName.toLowerCase();
  return lowered.endsWith('.gguf') ||
    lowered.endsWith('.safetensors') ||
    lowered === 'config.json' ||
    lowered === 'gliner_config.json' ||
    lowered === 'rebel_config.json' ||
    lowered === 'tokenizer.json' ||
    lowered === 'tokenizer_config.json';
}

export function isGenerativeModelType(modelType) {
  const lowered = String(modelType ?? '').toLowerCase();
  return [
    'gpt2',
    'gpt_oss',
    'llama',
    'mistral',
    'mixtral',
    'phi',
    'phi3',
    'phi3small',
    'qwen2',
    'qwen2_moe',
    'qwen2_vl',
    'qwen3_5',
    'qwen3_5_text',
    'gemma',
    'gemma2',
    'gemma3',
    'gemma3_text',
    'gemma4',
    'gemma4_text',
  ].includes(lowered);
}

export function isEmbeddingModelType(modelType) {
  const lowered = String(modelType ?? '').toLowerCase();
  return [
    'bert',
    'roberta',
    'xlm-roberta',
    'distilbert',
    'deberta',
    'deberta-v2',
  ].includes(lowered);
}

export function isSeq2SeqModelType(modelType) {
  const lowered = String(modelType ?? '').toLowerCase();
  return [
    't5',
    'mt5',
    'longt5',
  ].includes(lowered);
}

export function isClipModelType(modelType) {
  const lowered = String(modelType ?? '').toLowerCase();
  return [
    'clip',
    'clip_text_model',
  ].includes(lowered);
}

export function isExtractionModelType(modelType) {
  const lowered = String(modelType ?? '').toLowerCase();
  return [
    'extractor',
    'gliner2',
  ].includes(lowered);
}

export function isRelationExtractionModelType(modelType) {
  return String(modelType ?? '').toLowerCase() === 'bart';
}

export function isLikelyRebelBundle(bundlePath, rebelConfigFile) {
  if (rebelConfigFile) return true;
  return String(bundlePath ?? '').toLowerCase().includes('rebel');
}

export function inferBundleTaskHintFromConfig(config, options = {}) {
  const modelType = config?.model_type ?? null;
  if (String(modelType ?? '').toLowerCase() === 'whisper') return 'transcribe';
  if (isGenerativeModelType(modelType)) return 'generate';
  if (isSeq2SeqModelType(modelType)) return 'seq2seq';
  if (isEmbeddingModelType(modelType)) return 'embed';
  if (isClipModelType(modelType)) return 'clip-embed';
  if (isExtractionModelType(modelType)) return 'extract';
  if (isRelationExtractionModelType(modelType)) {
    return isLikelyRebelBundle(options.bundlePath, options.rebelConfigFile) ? 'relations' : 'seq2seq';
  }
  return null;
}

export function unsupportedReasonForModelType(modelType) {
  const lowered = String(modelType ?? '').toLowerCase();
  if (lowered === 'bart') {
    return 'model_type=bart is not wired into this page for generic seq2seq bundles yet';
  }
  return `model_type=${modelType ?? 'unknown'} not wired into this page`;
}

export function formatDiscoveredModelLabel(entry) {
  if (!entry.supported) {
    return entry.bundlePath
      ? `${entry.bundlePath} [Unsupported: ${entry.unsupportedReason}]`
      : `[Unsupported: ${entry.unsupportedReason}]`;
  }

  if (entry.kind === 'hf-gpt-safetensors') {
    const variant = basename(entry.modelFile.path);
    const suffix = entry.taskHint === 'embed'
      ? 'HF SafeTensors Encoder'
      : entry.taskHint === 'clip-embed'
        ? 'HF SafeTensors CLIP'
        : entry.taskHint === 'transcribe'
          ? 'HF SafeTensors Whisper'
          : entry.taskHint === 'extract'
            ? 'HF SafeTensors GLiNER'
            : entry.taskHint === 'relations'
              ? 'HF SafeTensors REBEL'
          : entry.taskHint === 'generate'
            ? 'HF SafeTensors GPT'
            : entry.taskHint === 'seq2seq'
              ? 'HF SafeTensors T5'
              : 'HF SafeTensors Bundle';
    return entry.bundlePath
      ? `${entry.bundlePath} / ${variant} [${suffix}]`
      : `${variant} [${suffix}]`;
  }

  const variant = basename(entry.modelFile.path);
  const projectorSuffix = entry.projectorFile ? ' + mmproj' : '';
  return entry.bundlePath
    ? `${entry.bundlePath} / ${variant} [GGUF]${projectorSuffix}`
    : `${variant} [GGUF]${projectorSuffix}`;
}

export async function buildDiscoveredModelEntries(files, { readJsonFile }) {
  const byDir = new Map();
  for (const file of files) {
    const bundlePath = parentPath(file.path);
    const current = byDir.get(bundlePath) ?? [];
    current.push(file);
    byDir.set(bundlePath, current);
  }

  const entries = [];
  for (const [bundlePath, dirFiles] of byDir.entries()) {
    const sorted = [...dirFiles].sort((a, b) => a.path.localeCompare(b.path));
    const decoderFiles = sorted.filter((file) => !isProjectorGgufName(file.path));
    const projectorFiles = sorted.filter((file) => isProjectorGgufName(file.path));
    const ggufDecoderFiles = decoderFiles.filter((file) => basename(file.path).toLowerCase().endsWith('.gguf'));

    if (ggufDecoderFiles.length > 0) {
      const projectorFile = projectorFiles[0] ?? null;
      for (const modelFile of ggufDecoderFiles) {
        entries.push({
          kind: 'gguf',
          supported: true,
          bundlePath,
          modelFile,
          projectorFile,
          configFile: null,
          tokenizerFile: null,
          tokenizerConfigFile: null,
          label: '',
        });
      }
    }

    const configFile = sorted.find((file) => hasBasename(file, 'config.json')) ?? null;
    const glinerConfigFile = sorted.find((file) => hasBasename(file, 'gliner_config.json')) ?? null;
    const rebelConfigFile = sorted.find((file) => hasBasename(file, 'rebel_config.json')) ?? null;
    const tokenizerFile = sorted.find((file) => hasBasename(file, 'tokenizer.json')) ?? null;
    const tokenizerConfigFile = sorted.find((file) => hasBasename(file, 'tokenizer_config.json')) ?? null;
    const safetensorsFiles = safetensorsFilesForDir(sorted);
    if (configFile && tokenizerFile && safetensorsFiles.length === 1) {
      const config = await readJsonFile(configFile);
      const taskHint = inferBundleTaskHintFromConfig(config, { bundlePath, rebelConfigFile });
      if (taskHint) {
        entries.push({
          kind: 'hf-gpt-safetensors',
          supported: true,
          bundlePath,
          modelFile: safetensorsFiles[0],
          projectorFile: null,
          configFile,
          glinerConfigFile,
          rebelConfigFile,
          tokenizerFile,
          tokenizerConfigFile,
          taskHint,
          label: '',
        });
      } else {
        let unsupportedReason = 'unknown model_type';
        try {
          unsupportedReason = unsupportedReasonForModelType(config?.model_type);
        } catch (_) {
          unsupportedReason = 'could not parse config.json';
        }
        entries.push({
          kind: 'unsupported',
          supported: false,
          bundlePath,
          modelFile: safetensorsFiles[0],
          projectorFile: null,
          configFile,
          glinerConfigFile,
          rebelConfigFile,
          tokenizerFile,
          tokenizerConfigFile,
          taskHint: null,
          unsupportedReason,
          label: '',
        });
      }
    }
  }

  entries.sort((a, b) => {
    if (a.bundlePath !== b.bundlePath) return a.bundlePath.localeCompare(b.bundlePath);
    return a.modelFile.path.localeCompare(b.modelFile.path);
  });
  for (const entry of entries) {
    entry.label = formatDiscoveredModelLabel(entry);
  }
  return entries;
}
