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
import {
    createGoParityRemoteTemplateRenderer,
    formatDotpromptMediaUrl,
    instantiateAntflyEmbeddedApiFromBytes,
} from "./wasm_client.mjs";

const renderer = createGoParityRemoteTemplateRenderer({
    remoteText({ url, credentials }) {
        return `text:${url}:${credentials}`;
    },
    remoteMedia({ url, mode, credentials }) {
        if (mode === "extract") return `pdf-text:${url}:${credentials}`;
        return formatDotpromptMediaUrl(`media:${mode}:${url}:${credentials}`);
    },
    transcribeAudio({ url, language }) {
        return `transcribed:${url}:${language}`;
    },
});

assert.equal(
    renderer({
        templateSource: "{{title}} {{body}}",
        jsonDoc: { title: "Hello", body: "world" },
    }),
    "Hello world",
);

assert.equal(
    renderer({
        templateSource: "{{author.name}}",
        jsonDoc: { author: { name: "Ada" } },
    }),
    "Ada",
);

assert.equal(
    renderer({
        templateSource: "{{scrubHtml body}}",
        jsonDoc: { body: "<p>Hello</p><script>evil()</script><p>World</p>" },
    }),
    "HelloWorld",
);

assert.equal(
    renderer({
        templateSource: "{{remoteText url=this credentials=\"primary\"}}",
        jsonDoc: "\"https://example.com/doc.txt\"",
    }),
    "text:https://example.com/doc.txt:primary",
);

assert.equal(
    renderer({
        templateSource: "{{remoteMedia url=this}}",
        jsonDoc: "\"https://example.com/image.png\"",
    }),
    "<<<dotprompt:media:url media:raw:https://example.com/image.png:>>>",
);

assert.equal(
    renderer({
        templateSource: "{{remoteMedia url=this mode=\"render\" credentials=\"thumbs\"}}",
        jsonDoc: "\"https://example.com/doc.pdf\"",
    }),
    "<<<dotprompt:media:url media:render:https://example.com/doc.pdf:thumbs>>>",
);

assert.equal(
    renderer({
        templateSource: "{{remotePDF url=this credentials=\"primary\"}}",
        jsonDoc: "\"https://example.com/doc.pdf\"",
    }),
    "pdf-text:https://example.com/doc.pdf:primary",
);

assert.equal(
    renderer({
        templateSource: "{{transcribeAudio url=this language=\"en\"}}",
        jsonDoc: "\"https://example.com/audio.mp3\"",
    }),
    "transcribed:https://example.com/audio.mp3:en",
);


// A future optional GPU import must not prevent CPU-only instantiation.
const wasmString = (value) => [value.length, ...new TextEncoder().encode(value)];
const section = (id, data) => [id, data.length, ...data];
const optionalGpuModule = new Uint8Array([
    0, 97, 115, 109, 1, 0, 0, 0,
    ...section(1, [2, 96, 0, 1, 127, 96, 0, 0]),
    ...section(2, [2,
        ...wasmString("webgpu"), ...wasmString("gpu_is_available"), 0, 0,
        ...wasmString("webgpu"), ...wasmString("gpu_future_operation"), 0, 1,
    ]),
    ...section(7, [2,
        ...wasmString("availability"), 0, 0,
        ...wasmString("dispatch"), 0, 1,
    ]),
]);
const cpuApi = await instantiateAntflyEmbeddedApiFromBytes(optionalGpuModule);
assert.equal(cpuApi.exports.availability(), 0);
assert.throws(() => cpuApi.exports.dispatch(), /WebGPU operation unavailable: gpu_future_operation/);
let dispatched = false;
const gpuApi = await instantiateAntflyEmbeddedApiFromBytes(optionalGpuModule, {
    webgpuOps: { getImports() { return {
        gpu_is_available: () => 1,
        gpu_future_operation: () => { dispatched = true; },
    }; } },
});
assert.equal(gpuApi.exports.availability(), 1);
gpuApi.exports.dispatch();
assert.equal(dispatched, true);
const partialGpuApi = await instantiateAntflyEmbeddedApiFromBytes(optionalGpuModule, {
    webgpuOps: { getImports() { return Object.freeze({ gpu_is_available: () => 1 }); } },
});
assert.equal(partialGpuApi.exports.availability(), 0, "incomplete GPU bindings select the CPU backend");


const entropyModule = new Uint8Array([
    0, 97, 115, 109, 1, 0, 0, 0,
    ...section(1, [1, 96, 2, 127, 127, 1, 127]),
    ...section(2, [1, ...wasmString("env"), ...wasmString("antfly_platform_random_secure"), 0, 0]),
    ...section(5, [1, 0, 2]),
    ...section(7, [2, ...wasmString("fill"), 0, 0, ...wasmString("memory"), 2, 0]),
]);
const entropyApi = await instantiateAntflyEmbeddedApiFromBytes(entropyModule);
const originalCrypto = Object.getOwnPropertyDescriptor(globalThis, "crypto");
let entropyCalls = 0;
try {
    Object.defineProperty(globalThis, "crypto", { configurable: true, value: {
        getRandomValues(bytes) {
            assert.ok(bytes.length <= 65536);
            entropyCalls += 1;
            bytes.fill(0x5a);
            return bytes;
        },
    } });
    assert.equal(entropyApi.exports.fill(0, 70000), 0);
    assert.equal(entropyCalls, 2);
    assert.ok(new Uint8Array(entropyApi.exports.memory.buffer, 0, 70000).every((byte) => byte === 0x5a));
    assert.equal(entropyApi.exports.fill(-1, 32), 1);
    Object.defineProperty(globalThis, "crypto", { configurable: true, value: undefined });
    assert.equal(entropyApi.exports.fill(0, 32), 1);
} finally {
    if (originalCrypto) Object.defineProperty(globalThis, "crypto", originalCrypto);
    else delete globalThis.crypto;
}
console.log("antfly embedded WASM client, optional GPU imports, and secure entropy passed: ok");
