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
  buildGlinerRunOptions,
  defaultExtractionLabels,
  extractGlinerSpecialTokenIds,
  normalizeExtractionLabels,
} from './runtime/extraction-support.js';

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

console.log('Test 1: GLiNER special token ids are parsed from tokenizer.json');
const tokenIds = extractGlinerSpecialTokenIds({
  added_tokens: [
    { id: 128001, content: '[P]' },
    { id: 128005, content: '[E]' },
    { id: 128002, content: '[SEP_TEXT]' },
  ],
});
if (tokenIds.tokenP !== 128001 || tokenIds.tokenE !== 128005 || tokenIds.tokenSepText !== 128002) {
  fail(`unexpected GLiNER token ids: ${JSON.stringify(tokenIds)}`);
}
console.log('  tokenizer special tokens are parsed correctly');

console.log('\nTest 2: extraction labels are normalized and deduplicated');
const labels = normalizeExtractionLabels('person, organization\nlocation\nPerson\n');
if (labels.length !== 3 || labels[0] !== 'person' || labels[1] !== 'organization' || labels[2] !== 'location') {
  fail(`unexpected normalized labels: ${JSON.stringify(labels)}`);
}
console.log('  extraction labels are normalized correctly');

console.log('\nTest 3: GLiNER runtime options combine config defaults and tokenizer metadata');
const options = buildGlinerRunOptions({
  glinerConfig: {
    max_width: 12,
    max_len: 384,
    threshold: 0.35,
    flat_ner: false,
    default_labels: ['person'],
  },
  modelConfig: { max_position_embeddings: 512 },
  tokenIds,
});
if (options.maxWidth !== 12 || options.maxLen !== 384 || options.threshold !== 0.35 || options.flatNer !== false) {
  fail(`unexpected GLiNER runtime options: ${JSON.stringify(options)}`);
}
if (defaultExtractionLabels({ default_labels: ['person', '', 'organization'] }).length !== 2) {
  fail('default extraction labels should preserve non-empty labels only');
}
console.log('  GLiNER runtime options are derived correctly');

console.log('\nExtraction support test passed.');
