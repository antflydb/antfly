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

import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { instantiateAntflyInferenceWasm } from "./test-wasm-runtime.mjs";

const memoryModel = process.argv[2] ?? "wasm32";
const runtime = await instantiateAntflyInferenceWasm(fileURLToPath(new URL("../", import.meta.url)), { memoryModel });
const bytes = new Uint8Array([0, 1, 127, 128, 255]);
const ptr = runtime.alloc(bytes.byteLength);
runtime.bytesIn(ptr, bytes);
assert.deepEqual(runtime.read(Uint8Array, ptr, bytes.byteLength), bytes);
runtime.free(ptr, bytes.byteLength);
console.log(`inference ${memoryModel} initialization and ABI roundtrip passed`);
