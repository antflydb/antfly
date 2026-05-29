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

function firstTokenId(value) {
  if (Array.isArray(value)) return value.length > 0 ? value[0] : undefined;
  return typeof value === 'number' ? value : undefined;
}

export function actionLabelForTask(task) {
  if (task === 'embed' || task === 'clip-embed') return 'Embed Text';
  if (task === 'extract') return 'Extract Entities';
  if (task === 'relations') return 'Extract Relations';
  if (task === 'transcribe') return 'Transcribe Audio';
  if (task === 'seq2seq') return 'Generate Text';
  if (task === 'generate') return 'Generate Response';
  return 'Run Model';
}

export function hasIntegratedVisionConfig(config) {
  const imageTokenId = firstTokenId(config?.image_token_index);
  const mmTokensPerImage = Number(config?.mm_tokens_per_image ?? 0);
  const visionCfg = config?.vision_config ?? null;
  const imageSize = Number(visionCfg?.image_size ?? config?.vision_image_size ?? 0);
  return typeof imageTokenId === 'number' &&
    mmTokensPerImage > 0 &&
    visionCfg != null &&
    imageSize > 0;
}

export function isGemma3ProjectorKind(projectorKind) {
  return projectorKind === 'antfly_gemma3';
}

export function isGemma4ImageProjectorKind(projectorKind) {
  return projectorKind === 'clip_gemma4_image' || projectorKind === 'clip_gemma4_image_audio';
}

export function isGemma4AudioProjectorKind(projectorKind) {
  return projectorKind === 'clip_gemma4_audio' || projectorKind === 'clip_gemma4_image_audio';
}

export function supportsBrowserMultimodal(task, config, projectorKind) {
  if (task !== 'generate' || !config) return false;
  if (typeof projectorKind === 'string' && projectorKind.length > 0) return true;
  return hasIntegratedVisionConfig(config);
}

export function computeMediaUiState({ task, config, projectorKind }) {
  if (task === 'extract' || task === 'relations') {
    return {
      imageEnabled: false,
      audioEnabled: false,
      imageHint: 'This task does not use image input.',
      audioHint: 'This task does not use audio input.',
    };
  }

  if (task === 'transcribe') {
    return {
      imageEnabled: false,
      audioEnabled: true,
      imageHint: 'This task does not use image input.',
      audioHint: 'Required. Select an audio clip and the page will transcribe it with Whisper.',
    };
  }

  const multimodal = supportsBrowserMultimodal(task, config, projectorKind);
  const hasExternalProjector = typeof projectorKind === 'string' && projectorKind.length > 0;
  const externalImage = hasExternalProjector && (isGemma3ProjectorKind(projectorKind) || isGemma4ImageProjectorKind(projectorKind));
  const externalAudio = hasExternalProjector && isGemma4AudioProjectorKind(projectorKind);
  const imageSize = Number(config?.vision_config?.image_size ?? config?.vision_image_size ?? 0);

  if (hasExternalProjector) {
    if (isGemma3ProjectorKind(projectorKind)) {
      return {
        imageEnabled: multimodal && externalImage,
        audioEnabled: false,
        imageHint: imageSize > 0
          ? `Optional. External Gemma 3 projector loaded. Selected images will be resized to ${imageSize}x${imageSize} before projector encoding.`
          : 'Optional. External Gemma 3 projector loaded for multimodal generation.',
        audioHint: 'This external projector does not expose audio support in the browser path.',
      };
    }
    if (isGemma4ImageProjectorKind(projectorKind)) {
      return {
        imageEnabled: multimodal && externalImage,
        audioEnabled: externalAudio,
        imageHint: 'Optional. External Gemma 4 projector loaded. Selected image bytes will be encoded through the projector before multimodal prefill.',
        audioHint: externalAudio
          ? 'Optional. External Gemma 4 audio projector loaded. Selected audio clips will be encoded before multimodal prefill.'
          : 'This external projector does not expose audio support in the browser path.',
      };
    }
    if (isGemma4AudioProjectorKind(projectorKind)) {
      return {
        imageEnabled: false,
        audioEnabled: externalAudio,
        imageHint: 'This external projector does not expose image support in the browser path.',
        audioHint: 'Optional. External Gemma 4 audio projector loaded. Selected audio clips will be encoded before multimodal prefill.',
      };
    }
    return {
      imageEnabled: multimodal && externalImage,
      audioEnabled: externalAudio,
      imageHint: `Optional. External projector ${projectorKind} is loaded.`,
      audioHint: `Optional. External projector ${projectorKind} is loaded.`,
    };
  }

  if (multimodal) {
    return {
      imageEnabled: hasIntegratedVisionConfig(config),
      audioEnabled: false,
      imageHint: imageSize > 0
        ? `Optional. This model supports the integrated browser multimodal path. Selected images will be resized to ${imageSize}x${imageSize} before vision encoding.`
        : 'Optional. This model supports the integrated browser multimodal path.',
      audioHint: 'This integrated browser multimodal path does not currently expose audio input.',
    };
  }

  return {
    imageEnabled: false,
    audioEnabled: false,
    imageHint: 'Optional. Enabled when the loaded model supports the integrated browser multimodal path.',
    audioHint: 'Optional. Enabled when the loaded model supports an audio-capable multimodal path.',
  };
}

export function expandedImageSequenceText(config) {
  const mmTokensPerImage = Number(config?.mm_tokens_per_image ?? 0);
  if (mmTokensPerImage <= 0) {
    throw new Error('Multimodal config does not expose mm_tokens_per_image');
  }
  return `\n\n<start_of_image>${'<image_soft_token>'.repeat(mmTokensPerImage)}<end_of_image>\n\n`;
}

export function buildExpandedMultimodalPrompt(renderedPrompt, config, imageCount) {
  if (imageCount <= 0) return renderedPrompt;
  const marker = '<start_of_image>';
  const replacement = expandedImageSequenceText(config);
  const count = renderedPrompt.split(marker).length - 1;
  if (count === 0) {
    if (imageCount !== 1) {
      throw new Error('Prompt must contain one <start_of_image> marker per image when using multiple images.');
    }
    return `${replacement}${renderedPrompt}`;
  }
  if (count !== imageCount) {
    throw new Error(`Prompt contains ${count} <start_of_image> marker(s), but ${imageCount} image(s) were selected.`);
  }
  return renderedPrompt.split(marker).join(replacement);
}

export function expandedGemma4ImageSequenceText(tokenCount) {
  if (!Number.isInteger(tokenCount) || tokenCount <= 0) {
    throw new Error('Gemma4 projector did not return a valid image token count');
  }
  return `<|image>${'<|image|>'.repeat(tokenCount)}<image|>`;
}

export function expandedGemma4AudioSequenceText(tokenCount) {
  if (!Number.isInteger(tokenCount) || tokenCount <= 0) {
    throw new Error('Gemma4 projector did not return a valid audio token count');
  }
  return `<|audio>${'<|audio|>'.repeat(tokenCount)}<audio|>`;
}

export function replaceMultimodalMarker(promptText, marker, replacementText, kind) {
  const count = promptText.split(marker).length - 1;
  if (count === 0) {
    if (replacementText.length !== 1) {
      throw new Error(`Prompt must contain one ${marker} marker per ${kind} input when using multiple ${kind} inputs.`);
    }
    return `${replacementText[0]}\n${promptText}`;
  }
  if (count !== replacementText.length) {
    throw new Error(`Prompt contains ${count} ${marker} marker(s), but ${replacementText.length} ${kind} input(s) were selected.`);
  }

  let idx = 0;
  return promptText.replaceAll(marker, () => replacementText[idx++]);
}

export function buildExpandedGemma4MultimodalPrompt(renderedPrompt, imageTokenCounts, audioTokenCounts) {
  let promptText = renderedPrompt;
  if (Array.isArray(imageTokenCounts) && imageTokenCounts.length > 0) {
    const replacementText = imageTokenCounts.map((count) => expandedGemma4ImageSequenceText(count));
    promptText = replaceMultimodalMarker(promptText, '<|image|>', replacementText, 'image');
  }
  if (Array.isArray(audioTokenCounts) && audioTokenCounts.length > 0) {
    const replacementText = audioTokenCounts.map((count) => expandedGemma4AudioSequenceText(count));
    promptText = replaceMultimodalMarker(promptText, '<|audio|>', replacementText, 'audio');
  }
  return promptText;
}

export function imageOffsetsFromExpandedTokenIds(tokenIds, config, imageCount) {
  const imageTokenId = firstTokenId(config?.image_token_index);
  const boiTokenId = firstTokenId(config?.boi_token_index);
  const eoiTokenId = firstTokenId(config?.eoi_token_index);
  const mmTokensPerImage = Number(config?.mm_tokens_per_image ?? 0);
  if (typeof imageTokenId !== 'number' || typeof boiTokenId !== 'number' || typeof eoiTokenId !== 'number' || mmTokensPerImage <= 0) {
    throw new Error('Multimodal token ids are missing from the loaded config');
  }

  const offsets = [];
  let runStart = null;
  for (let idx = 0; idx < tokenIds.length; idx++) {
    const tokenId = tokenIds[idx];
    if (tokenId === imageTokenId) {
      if (runStart == null) runStart = idx;
      continue;
    }
    if (runStart != null) {
      if (idx - runStart !== mmTokensPerImage) {
        throw new Error('Image placeholder token run does not match mm_tokens_per_image');
      }
      if (runStart === 0 || tokenIds[runStart - 1] !== boiTokenId || tokenId !== eoiTokenId) {
        throw new Error('Expanded multimodal prompt tokens are missing the expected image boundary tokens');
      }
      offsets.push(runStart);
      runStart = null;
    }
  }
  if (runStart != null) {
    throw new Error('Expanded multimodal prompt ended inside an image placeholder run');
  }
  if (offsets.length !== imageCount) {
    throw new Error(`Expanded multimodal prompt resolved ${offsets.length} image placeholder run(s), expected ${imageCount}.`);
  }
  return offsets;
}
