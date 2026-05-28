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

export function computePromptModeState({
  modelLoaded,
  tokenizerLoaded,
  modelConfig,
  task,
  chatTemplate,
  useChatTemplate,
}) {
  const hasTemplate = typeof chatTemplate === 'string' && chatTemplate.trim().length > 0;

  if (!modelLoaded || !tokenizerLoaded || !modelConfig) {
    return {
      useChatTemplateDisabled: !hasTemplate,
      systemPromptDisabled: !hasTemplate || !useChatTemplate,
      promptMeta: 'Load a model to start generating.',
    };
  }

  if (task === 'embed') {
    return {
      useChatTemplateDisabled: !hasTemplate,
      systemPromptDisabled: !hasTemplate || !useChatTemplate,
      promptMeta: 'Tokenizer and encoder model are loaded. The action button will tokenize the prompt and return a single embedding vector preview.',
    };
  }

  if (task === 'clip-embed') {
    return {
      useChatTemplateDisabled: !hasTemplate,
      systemPromptDisabled: !hasTemplate || !useChatTemplate,
      promptMeta: 'Tokenizer and CLIP text encoder are loaded. The action button will tokenize the prompt and return a CLIP text embedding preview.',
    };
  }

  if (task === 'seq2seq') {
    return {
      useChatTemplateDisabled: !hasTemplate,
      systemPromptDisabled: !hasTemplate || !useChatTemplate,
      promptMeta: 'Tokenizer and seq2seq model are loaded. The action button will tokenize the prompt and run encoder-decoder text generation.',
    };
  }

  if (task === 'extract') {
    return {
      useChatTemplateDisabled: true,
      systemPromptDisabled: true,
      promptMeta: 'Tokenizer and GLiNER model are loaded. Enter candidate entity labels separated by commas or new lines, then the action button will return extracted spans.',
    };
  }

  if (task === 'relations') {
    return {
      useChatTemplateDisabled: true,
      systemPromptDisabled: true,
      promptMeta: 'Tokenizer and REBEL model are loaded. The action button will run encoder-decoder relation extraction and return entities plus relation triples.',
    };
  }

  if (task === 'transcribe') {
    return {
      useChatTemplateDisabled: true,
      systemPromptDisabled: true,
      promptMeta: 'Tokenizer and Whisper model are loaded. Select an audio clip and the action button will run browser transcription.',
    };
  }

  if (!hasTemplate) {
    return {
      useChatTemplateDisabled: true,
      systemPromptDisabled: true,
      promptMeta: 'Tokenizer and model are loaded. No chat template was found in the selected bundle, so prompt text will be tokenized exactly as written.',
    };
  }

  if (useChatTemplate) {
    return {
      useChatTemplateDisabled: false,
      systemPromptDisabled: false,
      promptMeta: 'Tokenizer and model are loaded. The discovered chat template is active, so this page will render a system + user conversation before tokenization.',
    };
  }

  return {
    useChatTemplateDisabled: false,
    systemPromptDisabled: true,
    promptMeta: 'Tokenizer and model are loaded. A chat template is available, but raw prompt mode is selected.',
  };
}
