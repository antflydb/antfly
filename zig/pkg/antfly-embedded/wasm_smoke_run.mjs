// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
    createGoParityRemoteTemplateRenderer,
    formatDotpromptMediaUrl,
    instantiateAntflyEmbeddedApiFromBytes,
} from "./antfly_embedded_wasm_client.mjs";

const dir = path.dirname(fileURLToPath(import.meta.url));
const wasmPath = path.join(dir, "antfly.wasm");
const wasmBytes = await readFile(wasmPath);
const api = await instantiateAntflyEmbeddedApiFromBytes(wasmBytes, {
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

const putManyResult = api.putMany({
    "doc:a": { title: "alpha hosted", kind: "note" },
    "doc:b": { title: "beta hosted", kind: "note" },
});
if (putManyResult.inserted !== 2) throw new Error("putMany failed");

const idleResult = api.runUntilIdle();
if (typeof idleResult.derived_target_sequence !== "number") throw new Error("runUntilIdle failed");

const pendingWorkStats = api.pendingWorkStats();
if (typeof pendingWorkStats.derived_target_sequence !== "number") throw new Error("pendingWorkStats failed");

const capabilities = api.capabilities();
if (capabilities.hosted_profile !== true) throw new Error("capabilities hosted profile failed");
if (capabilities.manual_maintenance !== true) throw new Error("capabilities manual maintenance failed");
if (capabilities.local_template_rendering !== true) throw new Error("capabilities local template rendering failed");
if (capabilities.remote_template_rendering !== false) throw new Error("capabilities remote template rendering failed");
if (capabilities.remote_template_host_callbacks !== true) throw new Error("capabilities remote template host callbacks failed");

const indexConfigs = api.listIndexes();
if (!indexConfigs.some((cfg) => cfg.name === "full_text_index_v0")) throw new Error("listIndexes failed");

const lookupResult = api.lookup("doc:a", { fields: ["title"] });
if (lookupResult._source.title !== "alpha hosted") throw new Error("lookup failed");
if ("kind" in lookupResult._source) throw new Error("lookup projection failed");

const remoteRendered = api.renderRemoteTemplate("{{remoteText url=this}}", "\"https://example.com/doc.txt\"");
if (remoteRendered !== "hosted:https://example.com/doc.txt") throw new Error("remote template host render failed");

const remoteMediaRendered = api.renderRemoteTemplate("{{remoteMedia url=this}}", "\"https://example.com/img.png\"");
if (remoteMediaRendered !== "<<<dotprompt:media:url hosted:raw:https://example.com/img.png>>>") throw new Error("remote media parity render failed");

const remotePdfRendered = api.renderRemoteTemplate("{{remotePDF url=this}}", "\"https://example.com/doc.pdf\"");
if (remotePdfRendered !== "pdf-text:https://example.com/doc.pdf") throw new Error("remote pdf parity render failed");

const searchResult = api.search({
    full_text_search: {
        match: { field: "title", text: "alpha hosted" },
    },
    limit: 1,
});
if (!api.searchHits(searchResult).some((hit) => hit._id === "doc:a")) throw new Error("search failed");
if (api.searchTotalHits(searchResult) < 1) throw new Error("search total hits failed");

const deleted = api.delete("doc:b");
if (deleted.deleted !== 1) throw new Error("delete failed");

const stats = api.stats();
if (typeof stats.doc_count !== "number") throw new Error("stats failed");

api.close();
console.log("antfly embedded shared smoke passed: ok");
