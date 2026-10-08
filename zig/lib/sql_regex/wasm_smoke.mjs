// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import { readFileSync } from "node:fs";
import assert from "node:assert/strict";

const module = await WebAssembly.compile(readFileSync(process.argv[2]));
assert.deepEqual(WebAssembly.Module.imports(module), [], "the backend must not depend on host libc/I/O");
const instance = await WebAssembly.instantiate(module, {});
assert.equal(instance.exports.antfly_sql_regex_smoke(), 47);
assert.equal(instance.exports.antfly_sql_regex_smoke(), 47, "reopening must not retain native context state");
assert.equal(instance.exports.antfly_sql_regex_replacement_smoke(), 16);
assert.equal(instance.exports.antfly_sql_regex_replacement_smoke(), 16);
console.log("63 PostgreSQL ARE contracts passed twice in import-free freestanding WASM");
