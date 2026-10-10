// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

import { readFileSync } from "node:fs";
import assert from "node:assert/strict";

const module = await WebAssembly.compile(readFileSync(process.argv[2]));
assert.deepEqual(WebAssembly.Module.imports(module), [], "the backend must not depend on host libc/I/O");
const instance = await WebAssembly.instantiate(module, {});
assert.equal(instance.exports.antfly_sql_regex_smoke(), 47);
assert.equal(instance.exports.antfly_sql_regex_smoke(), 47, "reopening must not retain native context state");
assert.equal(instance.exports.antfly_sql_regex_replacement_smoke(), 16);
assert.equal(instance.exports.antfly_sql_regex_replacement_smoke(), 16);
const captures = JSON.parse(readFileSync(new URL("src/testdata/capture-postgres.json", import.meta.url))).entries.length;
assert.equal(instance.exports.antfly_sql_regex_capture_smoke(), captures);
assert.equal(instance.exports.antfly_sql_regex_capture_smoke(), captures, "reopening must not retain native matcher state");
const selection = JSON.parse(readFileSync(new URL("src/testdata/selection-postgres.json", import.meta.url))).entries.length;
assert.equal(instance.exports.antfly_sql_regex_selection_smoke(), selection);
assert.equal(instance.exports.antfly_sql_regex_selection_smoke(), selection, "reopening must not retain selection state");
console.log(`${47 + 16 + captures + selection} PostgreSQL regex contracts passed twice in import-free freestanding WASM`);
