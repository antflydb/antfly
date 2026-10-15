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
if (api.exports.antfly_embedded_smoke_run() !== 0) {
    throw new Error("hosted text index did not survive close/reopen");
}
api.open();
api.putMany({ a: { id: "a" }, b: { id: "b" } });
api.addIndex({ name: "links", kind: "graph", config_json: "{}" });
api.graphCatalog({ action: "put", name: "knowledge", definition: {
    labels: [{ name: "Document", table: "docs" }],
    relationships: [{ name: "KNOWS", index: "links", source_label: "Document", target_label: "Document", stored_type: "knows" }],
} });
const graphWrite = { query: "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE (a)-[r:KNOWS {since:2020}]->(b) RETURN r.since", write_request_id: "00112233445566778899aabbccddeeff" };
const committed = api.cypher("knowledge", graphWrite);
if (committed.commit.status !== "committed") throw new Error("WASM graph write did not commit");
const graphRows = api.cypher("knowledge", { query: "MATCH (a)-[r:KNOWS]->(b) RETURN a.id,r.since,b.id" }).rows;
if (JSON.stringify(graphRows) !== JSON.stringify([[{type:"string",value:"a"},{type:"integer",value:"2020"},{type:"string",value:"b"}]])) throw new Error("WASM graph values lost identity or integer precision");
try { api.cypher("knowledge", graphWrite); throw new Error("WASM duplicate graph write was accepted"); }
catch (error) { if (error.diagnostic?.code !== "CypherWriteRequestAlreadyUsed") throw error; }
if (api.graphWriteStatus(graphWrite.write_request_id).disposition !== "committed") throw new Error("WASM graph receipt unavailable");
const exact = api.cypher("knowledge", { query:"RETURN $minimum,$maximum", typed_parameters:{type:"map",value:{minimum:{type:"integer",value:"-9223372036854775808"},maximum:{type:"integer",value:"9223372036854775807"}}} });
if (exact.rows[0][0].value !== "-9223372036854775808" || exact.rows[0][1].value !== "9223372036854775807") throw new Error("WASM graph parameter precision lost");
try {
    api.cypher("knowledge", { query: "CREATE (n:Document {id:'aborted'}) RETURN 1/0", write_request_id: "ffeeddccbbaa99887766554433221100" });
    throw new Error("WASM failed graph write was accepted");
} catch (error) {
    if (error.diagnostic?.code !== "CypherDivisionByZero" || error.diagnostic?.write_outcome !== "not_started") throw error;
}
if (api.cypher("knowledge", { query: "MATCH (n:Document {id:'aborted'}) RETURN n" }).rows.length !== 0) throw new Error("WASM failed graph write published a row");
for (const query of [
    "RETURN reduce(a=[],x IN range(1,10000)|[a]) AS nested",
    "CREATE (n:Document {id:'depth_rollback'}) WITH n,reduce(a=[],x IN range(1,10000)|[a]) AS a RETURN a",
]) {
    try { api.cypher("knowledge", { query }); throw new Error("WASM accepted an excessive value depth"); }
    catch (error) { if (error.diagnostic?.code !== "CypherValueDepthExceeded") throw error; }
}
if (api.cypher("knowledge", { query: "MATCH (n:Document {id:'depth_rollback'}) RETURN n" }).rows.length !== 0) throw new Error("WASM depth failure published a row");
const shared = api.cypher("knowledge", { query: "WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a RETURN a=a" });
if (shared.rows[0][0].value !== true || shared.stats.intermediate_states >= 1000) throw new Error("WASM shared value comparison expanded its contents");
for (const [query, count] of [
    ["UNWIND range(1,1000) AS x RETURN x,count(*)", 1000],
    ["UNWIND range(9223372036854774800,9223372036854775799) AS x RETURN x,count(*)", 1000],
    ["UNWIND range(1,1000) AS x RETURN x UNION RETURN 0 AS x", 1001],
]) {
    const result = api.cypher("knowledge", { query });
    if (result.rows.length !== count || result.stats.intermediate_states >= 30000) throw new Error("WASM deduplication exceeded linear work");
}
api.close();
console.log("antfly embedded shared smoke passed: ok");
