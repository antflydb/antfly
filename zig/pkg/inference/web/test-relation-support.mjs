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
  defaultRebelConfig,
  parseRebelTriplets,
  tripletsToRelationOutput,
} from './runtime/relation-support.js';

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exit(1);
}

console.log('Test 1: REBEL config defaults are stable');
const config = defaultRebelConfig({});
if (config.tripletToken !== '<triplet>' || config.subjectToken !== '<subj>' || config.objectToken !== '<obj>') {
  fail(`unexpected default REBEL config: ${JSON.stringify(config)}`);
}
console.log('  REBEL defaults are correct');

console.log('\nTest 2: special-token REBEL output parses into triplets');
const specialTriplets = parseRebelTriplets(
  '<s><triplet> John Smith <subj> Google <obj> works for <triplet> Google <subj> Mountain View <obj> located in </s>',
  config,
);
if (specialTriplets.length !== 2 || specialTriplets[0].subject !== 'John Smith' || specialTriplets[1].relation !== 'located in') {
  fail(`unexpected special-token triplets: ${JSON.stringify(specialTriplets)}`);
}
console.log('  special-token REBEL output parses correctly');

console.log('\nTest 3: plain-text fallback output parses into triplets and relation objects');
const plainTriplets = parseRebelTriplets('John Smith  Google  works for  Google  Mountain View  located in', config);
if (plainTriplets.length !== 2) {
  fail(`unexpected plain-text triplets: ${JSON.stringify(plainTriplets)}`);
}
const relationOutput = tripletsToRelationOutput('John Smith works at Google in Mountain View.', plainTriplets);
if (relationOutput.entities.length !== 3 || relationOutput.relations.length !== 2) {
  fail(`unexpected relation output: ${JSON.stringify(relationOutput)}`);
}
console.log('  plain-text REBEL output converts into relation objects correctly');

console.log('\nRelation support test passed.');
