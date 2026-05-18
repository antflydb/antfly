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

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function parseJsonDocument(jsonDoc) {
    return typeof jsonDoc === "string" ? JSON.parse(jsonDoc) : jsonDoc;
}

function resolvePath(root, path) {
    if (!path || path === "this") return root;
    let value = root;
    for (const part of path.split(".")) {
        if (part.length === 0) return undefined;
        if (value == null || typeof value !== "object") return undefined;
        value = value[part];
    }
    return value;
}

function coerceTemplateValue(value) {
    if (value == null) return "";
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    return JSON.stringify(value);
}

function scrubHtml(text) {
    return String(text)
        .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, "")
        .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, "")
        .replace(/<[^>]+>/g, "")
        .trim();
}

function parseHelperExpression(expr) {
    const trimmed = expr.trim();
    if (!trimmed) return null;

    const firstSpace = trimmed.indexOf(" ");
    if (firstSpace === -1) {
        return { name: null, args: trimmed };
    }

    const name = trimmed.slice(0, firstSpace);
    const rest = trimmed.slice(firstSpace + 1);
    const hash = {};
    const hashRe = /([A-Za-z_][A-Za-z0-9_]*)=("([^"\\]|\\.)*"|'([^'\\]|\\.)*'|[^\s]+)/g;
    let match;
    while ((match = hashRe.exec(rest)) !== null) {
        const raw = match[2];
        if ((raw.startsWith('"') && raw.endsWith('"')) || (raw.startsWith("'") && raw.endsWith("'"))) {
            hash[match[1]] = raw.slice(1, -1);
        } else {
            hash[match[1]] = { path: raw };
        }
    }

    return { name, rest, hash };
}

function resolveHelperArg(doc, value) {
    if (value && typeof value === "object" && "path" in value) {
        return resolvePath(doc, value.path);
    }
    return value;
}

export function formatDotpromptMediaUrl(url) {
    return `<<<dotprompt:media:url ${url}>>>`;
}

export function createGoParityRemoteTemplateRenderer(handlers = {}) {
    return ({ templateSource, jsonDoc }) => {
        const doc = parseJsonDocument(jsonDoc);
        return String(templateSource).replace(/{{([^}]+)}}/g, (_, rawExpr) => {
            const parsed = parseHelperExpression(rawExpr);
            if (!parsed) return "";

            if (!parsed.name) {
                return coerceTemplateValue(resolvePath(doc, parsed.args));
            }

            if (parsed.name === "scrubHtml") {
                return scrubHtml(coerceTemplateValue(resolvePath(doc, parsed.rest.trim())));
            }

            if (parsed.name === "remoteMedia") {
                if (typeof handlers.remoteMedia !== "function") {
                    throw new Error("remoteMedia handler missing");
                }
                return handlers.remoteMedia({
                    url: coerceTemplateValue(resolveHelperArg(doc, parsed.hash.url)),
                    mode: parsed.hash.mode ? String(resolveHelperArg(doc, parsed.hash.mode)) : "raw",
                    credentials: parsed.hash.credentials ? coerceTemplateValue(resolveHelperArg(doc, parsed.hash.credentials)) : "",
                    document: doc,
                });
            }

            if (parsed.name === "remotePDF") {
                if (typeof handlers.remotePDF === "function") {
                    return handlers.remotePDF({
                        url: coerceTemplateValue(resolveHelperArg(doc, parsed.hash.url)),
                        credentials: parsed.hash.credentials ? coerceTemplateValue(resolveHelperArg(doc, parsed.hash.credentials)) : "",
                        document: doc,
                    });
                }
                if (typeof handlers.remoteMedia === "function") {
                    return handlers.remoteMedia({
                        url: coerceTemplateValue(resolveHelperArg(doc, parsed.hash.url)),
                        mode: "extract",
                        credentials: parsed.hash.credentials ? coerceTemplateValue(resolveHelperArg(doc, parsed.hash.credentials)) : "",
                        document: doc,
                    });
                }
                throw new Error("remotePDF handler missing");
            }

            if (parsed.name === "remoteText") {
                if (typeof handlers.remoteText !== "function") {
                    throw new Error("remoteText handler missing");
                }
                return handlers.remoteText({
                    url: coerceTemplateValue(resolveHelperArg(doc, parsed.hash.url)),
                    credentials: parsed.hash.credentials ? coerceTemplateValue(resolveHelperArg(doc, parsed.hash.credentials)) : "",
                    document: doc,
                });
            }

            if (parsed.name === "transcribeAudio") {
                if (typeof handlers.transcribeAudio !== "function") {
                    throw new Error("transcribeAudio handler missing");
                }
                return handlers.transcribeAudio({
                    url: coerceTemplateValue(resolveHelperArg(doc, parsed.hash.url)),
                    credentials: parsed.hash.credentials ? coerceTemplateValue(resolveHelperArg(doc, parsed.hash.credentials)) : "",
                    language: parsed.hash.language ? coerceTemplateValue(resolveHelperArg(doc, parsed.hash.language)) : "",
                    document: doc,
                });
            }

            return "";
        });
    };
}

function createHostImports(options = {}) {
    let exportsRef = null;

    function requireExports() {
        if (!exportsRef) {
            throw new Error("wasm exports not initialized");
        }
        return exportsRef;
    }

    function readUtf8(ptr, len) {
        const exports = requireExports();
        return textDecoder.decode(new Uint8Array(exports.memory.buffer, Number(ptr), Number(len)));
    }

    function allocUtf8(text) {
        const exports = requireExports();
        const bytes = textEncoder.encode(text);
        const ptr = Number(exports.antfly_embedded_alloc(bytes.length));
        if (ptr === 0 && bytes.length > 0) {
            throw new Error("allocation failed");
        }
        new Uint8Array(exports.memory.buffer, ptr, bytes.length).set(bytes);
        return { ptr, len: bytes.length };
    }

    function writeU32(ptr, value) {
        const exports = requireExports();
        new DataView(exports.memory.buffer).setUint32(Number(ptr), Number(value), true);
    }

    const noop = () => {};
    const webgpuStubs = {
        gpu_create_buffer: () => 0,
        gpu_upload: noop,
        gpu_matmul_transb_q4_0: noop,
        gpu_matmul_transb_q4_1: noop,
        gpu_matmul_transb_q5_0: noop,
        gpu_matmul_transb_q5_1: noop,
        gpu_matmul_transb_q8_0: noop,
        gpu_matmul_transb_q8_1: noop,
        gpu_matmul_transb_iq4_nl: noop,
        gpu_matmul_transb_iq4_xs: noop,
        gpu_matmul_transb_q2_k: noop,
        gpu_matmul_transb_q3_k: noop,
        gpu_matmul_transb_q4_k: noop,
        gpu_matmul_transb_q5_k: noop,
        gpu_matmul_transb_q6_k: noop,
        gpu_matmul_transb_q8_k: noop,
        gpu_download: noop,
        gpu_free_buffer: noop,
        gpu_layer_norm: noop,
        gpu_rms_norm: noop,
        gpu_attention: noop,
        gpu_causal_attention: noop,
        gpu_cross_attention: noop,
        gpu_gqa_causal_attention: noop,
        gpu_write_buffer_at_offset: noop,
        gpu_gqa_cached_attention: noop,
        gpu_matmul_transb: noop,
        gpu_is_available: () => 0,
    };

    // WebGPUOps.getImports(memory) needs WASM memory, which isn't available
    // until after instantiation. Use a lazy proxy that defers to exports.memory.
    let webgpuImports;
    if (options.webgpuOps && typeof options.webgpuOps.getImports === "function") {
        const lazyMemory = { get buffer() { return requireExports().memory.buffer; } };
        webgpuImports = options.webgpuOps.getImports(lazyMemory);
    } else {
        webgpuImports = webgpuStubs;
    }

    return {
        imports: {
            env: {
                antfly_embedded_host_render_json_to_text(
                    templatePtr,
                    templateLen,
                    jsonPtr,
                    jsonLen,
                    outPtrPtr,
                    outLenPtr,
                ) {
                    const renderer = options.remoteTemplateRenderer;
                    if (typeof renderer !== "function") {
                        return 1;
                    }

                    try {
                        const rendered = renderer({
                            templateSource: readUtf8(templatePtr, templateLen),
                            jsonDoc: readUtf8(jsonPtr, jsonLen),
                        });
                        const { ptr, len } = allocUtf8(String(rendered ?? ""));
                        writeU32(outPtrPtr, ptr);
                        writeU32(outLenPtr, len);
                        return 0;
                    } catch {
                        return 1;
                    }
                },
            },
            webgpu: webgpuImports,
        },
        setExports(exports) {
            exportsRef = exports;
        },
    };
}

export async function instantiateAntflyEmbeddedApiFromUrl(wasmUrl, options = {}) {
    const host = createHostImports(options);
    const response = await fetch(wasmUrl);
    if (!response.ok) {
        throw new Error(`failed to fetch wasm: ${response.status} ${response.statusText}`);
    }
    const { instance } = await WebAssembly.instantiateStreaming(response, host.imports);
    host.setExports(instance.exports);
    return bindAntflyEmbeddedApi(instance.exports);
}

export async function instantiateAntflyEmbeddedApiFromBytes(wasmBytes, options = {}) {
    const host = createHostImports(options);
    const { instance } = await WebAssembly.instantiate(wasmBytes, host.imports);
    host.setExports(instance.exports);
    return bindAntflyEmbeddedApi(instance.exports);
}

export function bindAntflyEmbeddedApi(exports) {
    function searchHits(result) {
        if (Array.isArray(result?.responses) && result.responses.length > 0) {
            if (result.responses[0]?.hits?.hits) {
                return result.responses.flatMap((response) => response.hits?.hits ?? []);
            }
            return result.responses;
        }
        return [];
    }

    function searchTotalHits(result) {
        if (Array.isArray(result?.responses) && result.responses.length > 0) {
            if (result.responses[0]?.hits?.total != null) {
                return result.responses.reduce((sum, response) => sum + (response.hits?.total ?? 0), 0);
            }
            return result.responses.length;
        }
        return 0;
    }

    function toJsonBody(value) {
        return typeof value === "string" ? value : JSON.stringify(value);
    }

    function readUtf8(ptr, len) {
        return textDecoder.decode(new Uint8Array(exports.memory.buffer, Number(ptr), Number(len)));
    }

    function lastMessage() {
        return readUtf8(
            exports.antfly_embedded_last_message_ptr(),
            exports.antfly_embedded_last_message_len(),
        );
    }

    function lastResult() {
        return readUtf8(
            exports.antfly_embedded_last_result_ptr(),
            exports.antfly_embedded_last_result_len(),
        );
    }

    function writeInput(text) {
        const bytes = textEncoder.encode(text);
        const ptr = Number(exports.antfly_embedded_alloc(bytes.length));
        if (ptr === 0 && bytes.length > 0) {
            throw new Error("allocation failed");
        }
        new Uint8Array(exports.memory.buffer, ptr, bytes.length).set(bytes);
        return { ptr, len: bytes.length };
    }

    function freeInput(input) {
        exports.antfly_embedded_free(input.ptr, input.len);
    }

    function expectOk(status) {
        if (Number(status) !== 0) {
            throw new Error(lastMessage());
        }
    }

    function callJson(fn, body) {
        const input = writeInput(body);
        try {
            expectOk(fn(input.ptr, input.len));
            return lastResult();
        } finally {
            freeInput(input);
        }
    }

    return {
        exports,
        open() {
            expectOk(exports.antfly_embedded_open_default());
            return lastMessage();
        },
        close() {
            exports.antfly_embedded_close();
            return lastMessage();
        },
        batchJson(body) {
            return callJson(exports.antfly_embedded_batch_json, toJsonBody(body));
        },
        batch(body) {
            return JSON.parse(this.batchJson(body));
        },
        put(key, document) {
            return this.batch({
                inserts: {
                    [key]: document,
                },
            });
        },
        putMany(records) {
            return this.batch({
                inserts: records,
            });
        },
        delete(key) {
            return this.batch({
                deletes: [key],
            });
        },
        deleteMany(keys) {
            return this.batch({
                deletes: keys,
            });
        },
        lookupJson(key, body = "{}") {
            const keyInput = writeInput(key);
            const bodyInput = writeInput(toJsonBody(body));
            try {
                expectOk(exports.antfly_embedded_lookup_json(
                    keyInput.ptr,
                    keyInput.len,
                    bodyInput.ptr,
                    bodyInput.len,
                ));
                return lastResult();
            } finally {
                freeInput(keyInput);
                freeInput(bodyInput);
            }
        },
        lookup(key, body = "{}") {
            return JSON.parse(this.lookupJson(key, body));
        },
        scanJson(body) {
            return callJson(exports.antfly_embedded_scan_json, toJsonBody(body));
        },
        scan(body) {
            return JSON.parse(this.scanJson(body));
        },
        searchJson(body) {
            return callJson(exports.antfly_embedded_search_json, toJsonBody(body));
        },
        search(body) {
            return JSON.parse(this.searchJson(body));
        },
        renderRemoteTemplateJson(templateSource, jsonDoc) {
            const templateInput = writeInput(templateSource);
            const jsonInput = writeInput(typeof jsonDoc === "string" ? jsonDoc : JSON.stringify(jsonDoc));
            try {
                expectOk(exports.antfly_embedded_render_remote_template_json(
                    templateInput.ptr,
                    templateInput.len,
                    jsonInput.ptr,
                    jsonInput.len,
                ));
                return lastResult();
            } finally {
                freeInput(templateInput);
                freeInput(jsonInput);
            }
        },
        renderRemoteTemplate(templateSource, jsonDoc) {
            return this.renderRemoteTemplateJson(templateSource, jsonDoc);
        },
        searchHits(result) {
            return searchHits(result);
        },
        searchTotalHits(result) {
            return searchTotalHits(result);
        },
        searchFirstHit(result) {
            return searchHits(result)[0] ?? null;
        },
        runUntilIdleJson() {
            expectOk(exports.antfly_embedded_run_until_idle_json());
            return lastResult();
        },
        runUntilIdle() {
            return JSON.parse(this.runUntilIdleJson());
        },
        statsJson() {
            expectOk(exports.antfly_embedded_stats_json());
            return lastResult();
        },
        stats() {
            return JSON.parse(this.statsJson());
        },
        pendingWorkStatsJson() {
            expectOk(exports.antfly_embedded_pending_work_stats_json());
            return lastResult();
        },
        pendingWorkStats() {
            return JSON.parse(this.pendingWorkStatsJson());
        },
        capabilitiesJson() {
            expectOk(exports.antfly_embedded_capabilities_json());
            return lastResult();
        },
        capabilities() {
            return JSON.parse(this.capabilitiesJson());
        },
        listIndexesJson() {
            expectOk(exports.antfly_embedded_list_indexes_json());
            return lastResult();
        },
        listIndexes() {
            return JSON.parse(this.listIndexesJson());
        },
        listEnrichmentsJson() {
            expectOk(exports.antfly_embedded_list_enrichments_json());
            return lastResult();
        },
        listEnrichments() {
            return JSON.parse(this.listEnrichmentsJson());
        },

        // --- Termite model lifecycle (uses termite's exports directly) ---

        loadEmbeddingModel(bytes) {
            const ptr = Number(exports.wasm_alloc(bytes.length));
            if (ptr === 0 && bytes.length > 0) throw new Error("allocation failed");
            new Uint8Array(exports.memory.buffer, ptr, bytes.length).set(bytes);
            const handle = Number(exports.load_model_gguf(ptr, bytes.length));
            if (handle === 0) throw new Error("load_model_gguf failed");
            return handle;
        },
        loadTokenizer(bytes) {
            const ptr = Number(exports.wasm_alloc(bytes.length));
            if (ptr === 0 && bytes.length > 0) throw new Error("allocation failed");
            new Uint8Array(exports.memory.buffer, ptr, bytes.length).set(
                bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes),
            );
            const handle = Number(exports.load_tokenizer(ptr, bytes.length));
            if (handle === 0) throw new Error("load_tokenizer failed");
            return handle;
        },

        // --- Bridge configuration ---

        configureLocalEmbedder(modelHandle, tokenizerHandle, maxSeqLen, hiddenSize) {
            expectOk(exports.antfly_configure_local_embedder(modelHandle, tokenizerHandle, maxSeqLen, hiddenSize));
        },
        openWithEmbedder() {
            expectOk(exports.antfly_embedded_open_with_embedder());
            return lastMessage();
        },

        // --- Convenience: all-in-one setup ---

        async setupVectorSearch(modelSource, tokenizerSource, config = {}) {
            const [modelBytes, tokenizerBytes] = await Promise.all([
                modelSource instanceof Uint8Array ? modelSource : fetch(modelSource).then(r => r.arrayBuffer()).then(b => new Uint8Array(b)),
                tokenizerSource instanceof Uint8Array ? tokenizerSource : fetch(tokenizerSource).then(r => r.arrayBuffer()).then(b => new Uint8Array(b)),
            ]);
            const modelHandle = this.loadEmbeddingModel(modelBytes);
            const tokenizerHandle = this.loadTokenizer(tokenizerBytes);
            this.configureLocalEmbedder(modelHandle, tokenizerHandle, config.maxSeqLen || 512, config.hiddenSize || 384);
            this.openWithEmbedder();
            return { modelHandle, tokenizerHandle };
        },
    };
}
