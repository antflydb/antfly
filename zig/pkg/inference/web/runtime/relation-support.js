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

export function defaultRebelConfig(config = {}) {
  return {
    maxLength: Math.max(1, Number(config?.max_length) || 256),
    numBeams: Math.max(1, Number(config?.num_beams) || 3),
    tripletToken: typeof config?.triplet_token === 'string' && config.triplet_token.length > 0 ? config.triplet_token : '<triplet>',
    subjectToken: typeof config?.subject_token === 'string' && config.subject_token.length > 0 ? config.subject_token : '<subj>',
    objectToken: typeof config?.object_token === 'string' && config.object_token.length > 0 ? config.object_token : '<obj>',
  };
}

function sanitizeGeneratedText(input) {
  return String(input ?? '')
    .replaceAll('<s>', '')
    .replaceAll('</s>', '')
    .replaceAll('<pad>', '')
    .trim();
}

function parseSpecialTokenTriplet(part, config) {
  const subjIdx = part.indexOf(config.subjectToken);
  if (subjIdx < 0) return null;
  const objStart = subjIdx + config.subjectToken.length;
  const objIdx = part.indexOf(config.objectToken, objStart);
  if (objIdx < 0) return null;

  const subject = part.slice(0, subjIdx).trim();
  const object = part.slice(objStart, objIdx).trim();
  const relation = part.slice(objIdx + config.objectToken.length).trim();
  if (!subject || !object || !relation) return null;
  return { subject, object, relation };
}

function parseTripletsWithoutTokens(text) {
  const parts = text.split('  ').map((part) => part.trim()).filter(Boolean);
  const triplets = [];
  for (let i = 0; i + 2 < parts.length; i += 3) {
    triplets.push({
      subject: parts[i],
      object: parts[i + 1],
      relation: parts[i + 2],
    });
  }
  return triplets;
}

function findSpan(text, needle) {
  const lowerText = String(text ?? '').toLowerCase();
  const lowerNeedle = String(needle ?? '').toLowerCase();
  const idx = lowerText.indexOf(lowerNeedle);
  return idx >= 0 ? { start: idx, end: idx + lowerNeedle.length } : { start: 0, end: 0 };
}

export function parseRebelTriplets(generatedText, rebelConfig = {}) {
  const config = defaultRebelConfig(rebelConfig);
  const cleaned = sanitizeGeneratedText(generatedText);
  if (!cleaned) return [];

  const hasSpecialTokens =
    cleaned.includes(config.tripletToken) ||
    cleaned.includes(config.subjectToken) ||
    cleaned.includes(config.objectToken);

  if (!hasSpecialTokens) return parseTripletsWithoutTokens(cleaned);

  const triplets = [];
  for (const rawPart of cleaned.split(config.tripletToken)) {
    const part = rawPart.trim();
    if (!part) continue;
    const parsed = parseSpecialTokenTriplet(part, config);
    if (parsed) triplets.push(parsed);
  }
  return triplets;
}

export function tripletsToRelationOutput(sourceText, triplets) {
  const entities = [];
  const entityIndex = new Map();

  function getOrCreateEntity(text) {
    const key = text.toLowerCase();
    const existing = entityIndex.get(key);
    if (existing != null) return existing;
    const span = findSpan(sourceText, text);
    const entity = {
      text,
      label: 'ENTITY',
      start: span.start,
      end: span.end,
      score: 1,
    };
    const idx = entities.length;
    entities.push(entity);
    entityIndex.set(key, idx);
    return idx;
  }

  const relations = [];
  for (const triplet of triplets) {
    const headIdx = getOrCreateEntity(triplet.subject);
    const tailIdx = getOrCreateEntity(triplet.object);
    relations.push({
      head: entities[headIdx],
      tail: entities[tailIdx],
      label: triplet.relation,
      score: 1,
    });
  }

  return { entities, relations };
}
