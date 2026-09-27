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
    createGoParityRemoteTemplateRenderer,
    formatDotpromptMediaUrl,
    instantiateAntflyEmbeddedApiFromUrl,
} from "./antfly_embedded_wasm_client.mjs";
import { WebGPUOps } from "./webgpu_ops.mjs";

async function render() {
    const status = document.getElementById("status");
    const output = document.getElementById("output");

    try {
        // Try to initialize real WebGPU — falls back to SIMD stubs if unavailable
        const gpu = new WebGPUOps();
        const hasGpu = await gpu.init();

        const wasmUrl = new URL("./antfly.wasm", import.meta.url);
        const api = await instantiateAntflyEmbeddedApiFromUrl(wasmUrl, {
            webgpuOps: hasGpu ? gpu : undefined,
            remoteTemplateRenderer: createGoParityRemoteTemplateRenderer({
                remoteText({ url }) {
                    return `hosted:${url}`;
                },
                remoteMedia({ url, mode }) {
                    if (mode === "extract") return `pdf-text:${url}`;
                    return formatDotpromptMediaUrl(`hosted:${mode}:${url}`);
                },
            }),
        });
        api.open();
        const putMany = api.putMany({
            "doc:a": { title: "alpha hosted", kind: "note" },
            "doc:b": { title: "beta hosted", kind: "note" },
        });
        const idle = api.runUntilIdle();
        const pendingWorkStats = api.pendingWorkStats();
        const capabilities = api.capabilities();
        const indexes = api.listIndexes();
        const lookup = api.lookup("doc:a", { fields: ["title"] });
        const remoteRendered = api.renderRemoteTemplate("{{remoteText url=this}}", "\"https://example.com/doc.txt\"");
        const remoteMediaRendered = api.renderRemoteTemplate("{{remoteMedia url=this}}", "\"https://example.com/img.png\"");
        const remotePdfRendered = api.renderRemoteTemplate("{{remotePDF url=this}}", "\"https://example.com/doc.pdf\"");
        const search = api.search({
            full_text_search: {
                match: { field: "title", text: "alpha hosted" },
            },
            limit: 1,
        });
        const deleted = api.delete("doc:b");
        const stats = api.stats();
        api.close();

        status.textContent = "shared embedded api ready";
        status.dataset.status = "ok";
        output.textContent = JSON.stringify({
            putMany,
            idle,
            pendingWorkStats,
            capabilities,
            indexes,
            lookup,
            remoteRendered,
            remoteMediaRendered,
            remotePdfRendered,
            search,
            deleted,
            stats,
        }, null, 2);
    } catch (err) {
        status.textContent = `shared embedded api failed: ${err}`;
        status.dataset.status = "error";
        output.textContent = String(err);
    }
}

window.instantiateAntflyEmbeddedApi = async () => {
    const wasmUrl = new URL("./antfly.wasm", import.meta.url);
    return await instantiateAntflyEmbeddedApiFromUrl(wasmUrl);
};
render();
