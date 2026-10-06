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

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

let entropyDenied = false;
const { instance } = await WebAssembly.instantiate(await readFile(process.argv[2]), {
    env: {
        antfly_platform_random_secure(ptr, len) {
            if (entropyDenied) return 1;
            new Uint8Array(instance.exports.memory.buffer, ptr, len).fill(0x5a);
            return 0;
        },
    },
});
assert.equal(instance.exports.checkAwakeClock(), 0, "WASM clocks do not invoke a native I/O vtable");
assert.equal(instance.exports.checkWideValues(), 0, "single-threaded WASM wide counters preserve all 64 bits");
assert.equal(instance.exports.checkF16Distances(), 0, "WASM float16 distances match decoded float32 distances");
assert.equal(instance.exports.checkEntropy(), 0, "WASM uses host-provided entropy");
entropyDenied = true;
assert.equal(instance.exports.checkEntropy(), 2, "WASM fails closed when secure entropy is unavailable");
console.log("platform WASM counters, float16 SIMD, and host entropy checks passed");
