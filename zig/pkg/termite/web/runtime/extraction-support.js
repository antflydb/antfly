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

function asObject(value) {
  return value && typeof value === 'object' ? value : null;
}

function readTokenIdFromAddedTokensArray(addedTokens, tokenName) {
  if (!Array.isArray(addedTokens)) return null;
  for (const entry of addedTokens) {
    const obj = asObject(entry);
    if (!obj) continue;
    if (obj.content === tokenName && Number.isInteger(obj.id)) return obj.id;
  }
  return null;
}

function readTokenIdFromAddedTokensDecoder(decoder, tokenName) {
  const obj = asObject(decoder);
  if (!obj) return null;
  for (const [id, entry] of Object.entries(obj)) {
    if (asObject(entry)?.content === tokenName) {
      const parsed = Number(id);
      if (Number.isInteger(parsed)) return parsed;
    }
  }
  return null;
}

function firstDefinedNumber(...values) {
  for (const value of values) {
    if (Number.isInteger(value)) return value;
  }
  return null;
}

export function parseJsonText(text, label = 'JSON') {
  try {
    return JSON.parse(text);
  } catch (err) {
    throw new Error(`Failed to parse ${label}: ${err.message}`);
  }
}

export function extractGlinerSpecialTokenIds(tokenizerJsonOrObject) {
  const tokenizer = typeof tokenizerJsonOrObject === 'string'
    ? parseJsonText(tokenizerJsonOrObject, 'tokenizer.json')
    : (tokenizerJsonOrObject ?? {});

  const addedTokens = tokenizer.added_tokens;
  const addedTokensDecoder = tokenizer.added_tokens_decoder;

  return {
    tokenP: firstDefinedNumber(
      readTokenIdFromAddedTokensArray(addedTokens, '[P]'),
      readTokenIdFromAddedTokensDecoder(addedTokensDecoder, '[P]'),
    ),
    tokenE: firstDefinedNumber(
      readTokenIdFromAddedTokensArray(addedTokens, '[E]'),
      readTokenIdFromAddedTokensDecoder(addedTokensDecoder, '[E]'),
    ),
    tokenSepText: firstDefinedNumber(
      readTokenIdFromAddedTokensArray(addedTokens, '[SEP_TEXT]'),
      readTokenIdFromAddedTokensDecoder(addedTokensDecoder, '[SEP_TEXT]'),
    ),
  };
}

export function normalizeExtractionLabels(text) {
  const raw = typeof text === 'string' ? text : '';
  const seen = new Set();
  const labels = [];
  for (const piece of raw.split(/[\n,]/)) {
    const label = piece.trim();
    if (!label) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    labels.push(label);
  }
  return labels;
}

export function defaultExtractionLabels(glinerConfig) {
  return Array.isArray(glinerConfig?.default_labels)
    ? glinerConfig.default_labels.filter((label) => typeof label === 'string' && label.trim().length > 0)
    : [];
}

export function buildGlinerRunOptions({ glinerConfig, modelConfig, tokenIds }) {
  const tokenP = Number(tokenIds?.tokenP);
  const tokenE = Number(tokenIds?.tokenE);
  const tokenSepText = Number(tokenIds?.tokenSepText);
  if (!Number.isInteger(tokenP) || tokenP <= 0 || !Number.isInteger(tokenE) || tokenE <= 0 || !Number.isInteger(tokenSepText) || tokenSepText <= 0) {
    throw new Error('GLiNER tokenizer metadata is missing [P], [E], or [SEP_TEXT] token IDs');
  }

  const maxWidth = Math.max(1, Number(glinerConfig?.max_width) || 8);
  const maxLen = Math.max(8, Number(glinerConfig?.max_len ?? modelConfig?.max_position_embeddings) || 512);
  const threshold = Number(glinerConfig?.threshold ?? 0.5);
  const flatNer = glinerConfig?.flat_ner !== false;

  return {
    maxWidth,
    maxLen,
    threshold: Number.isFinite(threshold) ? threshold : 0.5,
    flatNer,
    tokenP,
    tokenE,
    tokenSepText,
  };
}
