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

import module from "./dist/antfly-serverless.wasm";
import { instantiateHost } from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import { createR2Transport } from "../../zig/lib/objectstore/wasm/r2.mjs";
import { withCommitCoordinator } from "../../zig/lib/objectstore/wasm/coordinated.mjs";
import { createImmutableReadCache } from "../../zig/lib/objectstore/wasm/cache.mjs";
import { collect } from "./collect.mjs";
import { pruneEmbeddingCheckpoints } from "./embedding-checkpoints.mjs";
export { CommitCoordinator } from "./coordinator.mjs";
const lanes = {interactive: null, maintenance: null};
const busy = {interactive: false, maintenance: false};
const cache = createImmutableReadCache();
class Busy extends Error {}
async function run(env, input, signal, collection) {
  const role = ["reindex", "publish", "compact", "enrich", "gc_prepare", "gc_trace", "gc_wal"].includes(input.op) ? "maintenance" : "interactive";
  if (busy[role]) throw new Busy("database is busy");
  busy[role] = true;
  let lane = lanes[role];
  let recycleAtBytes = 24 * 1024 * 1024;
  try {
    const configuredRecycle = Number(env.WASM_RECYCLE_BYTES ?? recycleAtBytes);
    if (!Number.isSafeInteger(configuredRecycle) || configuredRecycle < 4 * 1024 * 1024 || configuredRecycle > recycleAtBytes) throw new Error("InvalidHostedRecycleBudget");
    recycleAtBytes = configuredRecycle;
    if (!env.ANTFLY_INCARNATION) throw new Error("database incarnation is not configured");
    if (env.EMBEDDING_MODEL && (["reindex", "publish", "compact", "enrich"].includes(input.op) || (input.op === "search" && input.mode && input.mode !== "text"))) {
      const accepted = await env.COMMITS.getByName(env.ANTFLY_INCARNATION).ensureEmbeddingProfile({model: env.EMBEDDING_MODEL, dimensions: Number(env.EMBEDDING_DIMENSIONS ?? 384), revision: env.EMBEDDING_REVISION ?? "1"});
      if (!accepted) throw new Error("HostedEmbeddingProfileChanged");
    }
    if (!lane) lanes[role] = lane = await instantiateHost(module, null, {
      onTransportError: (error, op) => console.error("Antfly object operation failed", op, String(error)),
      onLog: message => console.error("Antfly runtime", message),
    });
    const maxPutBytes = Number(env.OBJECT_MAX_PUT_BYTES ?? 4 * 1024 * 1024);
    if (!Number.isSafeInteger(maxPutBytes) || maxPutBytes < 1 || maxPutBytes > 8 * 1024 * 1024) throw new Error("InvalidHostedObjectBudget");
    const transport = cache.wrap(withCommitCoordinator(createR2Transport({ database: env.DATABASE }, {maxPutBytes}),
      env.COMMITS.getByName(env.ANTFLY_INCARNATION), collection), env.ANTFLY_INCARNATION);
    const publicationBudget = ["publish", "reindex"].includes(input.op) ? await env.COMMITS.getByName(env.ANTFLY_INCARNATION).publicationBudget(Number(env.PUBLICATION_RECORDS ?? 16)) : 16;
    const settings = input.op === "latest" ? {} : { enrichment: Boolean(env.EMBEDDING_MODEL), embedding_dimensions: Number(env.EMBEDDING_DIMENSIONS ?? 384), publication_records: publicationBudget,
      reindex_records: Math.min(64, publicationBudget),
      enrichment_documents: Number(env.ENRICHMENT_DOCUMENTS ?? 1),
      paged_text: env.PAGED_TEXT === "true" || env.PAGED_PROJECTIONS === "true",
      paged_projections: env.PAGED_PROJECTIONS === "true",
      embedding_checkpoints: env.EMBEDDING_CHECKPOINTS !== "false",
      embedding_windows: Number(env.EMBEDDING_WINDOWS ?? 1),
      embedding_identity: JSON.stringify([env.EMBEDDING_MODEL ?? "", env.EMBEDDING_REVISION ?? "1", env.AI_GATEWAY ?? ""]),
      working_set_bytes: Number(env.WORKING_SET_BYTES ?? 20 * 1024 * 1024),
      compaction_max_documents: Number(env.COMPACTION_MAX_DOCUMENTS ?? 1000),
      compaction_max_artifact_bytes: Number(env.COMPACTION_MAX_ARTIFACT_BYTES ?? 4 * 1024 * 1024) };
    const inference = async request => {
      if (request.task !== "embedding" || !env.EMBEDDING_MODEL?.startsWith("@cf/") || !env.AI_GATEWAY ||
        request.dimensions !== Number(env.EMBEDDING_DIMENSIONS ?? 384) || !Array.isArray(request.texts) || request.texts.length < 1 || request.texts.length > 64 || request.texts.some(text => typeof text !== "string" || text.length > 16384)) throw new Error("HostedEmbeddingCapabilityUnsupported");
      return {json: await env.AI.run(env.EMBEDDING_MODEL, {text: request.texts}, {gateway: {id: env.AI_GATEWAY}})};
    };
    return await lane.request({ ...input, ...settings, bucket: "database", prefix: "antfly", namespace: env.ANTFLY_INCARNATION }, { transport, signal, inference,
      onMetrics: metrics => {
        if (env.LOG_METRICS === "true") console.log(JSON.stringify({event: "antfly.operation", op: input.op, role, ...metrics,
          cache_retained_bytes: cache.retainedBytes(), wasm_linear_total_bytes: Object.values(lanes).reduce((sum, instance) => sum + (instance?.memoryBytes() ?? 0), 0)}));
      } });
  } catch (error) {
    if (/IntegrityMismatch/.test(String(error))) cache.clear();
    throw error;
  } finally {
    // Linear memory cannot shrink. Recycle between requests before allocator
    // size-class churn consumes the cap despite a small live working set.
    // Durable state and the immutable JS cache survive lane replacement.
    if (lane && (!lane.usable() || lane.memoryBytes() >= recycleAtBytes)) lanes[role] = null;
    busy[role] = false;
  }
}
async function readJson(request) {
  if (Number(request.headers.get("Content-Length") ?? 0) > 1024 * 1024) throw new Error("request too large");
  const reader = request.body?.getReader();
  if (!reader) throw new Error("body required");
  const chunks = []; let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > 1024 * 1024) { await reader.cancel(); throw new Error("request too large"); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const input = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { input.set(chunk, offset); offset += chunk.byteLength; }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(input));
}
async function enqueue(env, kind = "publish", job_id, delaySeconds = 0) {
  if (!env.MAINTENANCE) return false;
  await env.MAINTENANCE.send({ incarnation: env.ANTFLY_INCARNATION, ...(["collect", "reindex"].includes(kind) ? { kind, job_id } : kind === "compact" ? {kind} : {}) }, { delaySeconds });
  return true;
}
export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path === "/" && env.ASSETS) return env.ASSETS.fetch(request);
    if (!env.API_TOKEN || request.headers.get("Authorization") !== `Bearer ${env.API_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    if (request.method !== "POST" || !["/api", "/ai"].includes(path)) return new Response("not found", { status: 404 });
    try {
      const args = await readJson(request);
      if (path === "/ai") {
        // Model and gateway selection are deployment policy. Native typed task
        // adapters remain the responsibility of #1271; this passes their model
        // input to Workers AI without assuming chat-completions semantics.
        if (!env.AI_MODEL?.startsWith("@cf/") || !env.AI_GATEWAY) return new Response("AI model and gateway are not configured", { status: 503 });
        return Response.json(await env.AI.run(env.AI_MODEL, args, { gateway: { id: env.AI_GATEWAY } }));
      }
      if (args.op === "prune_checkpoints") return Response.json(await pruneEmbeddingCheckpoints(env));
      if (args.op === "collect") {
        const result = await collect(env, (input, token) => run(env, input, request.signal, token), args.job_id);
        if (result.has_more) await enqueue(env, "collect", result.job_id, result.delay_seconds);
        return Response.json(result, env.WORKER_VERSION ? {headers: {"X-Antfly-Version": env.WORKER_VERSION.id}} : undefined);
      }
      if (!["mutate", "lookup", "list", "search", "count", "status", "publish", "compact", "enrich", "latest", "reindex"].includes(args.op)) return new Response("unsupported operation", { status: 400 });
      const result = await run(env, args, request.signal);
      if (args.op === "reindex" && result.has_more && args.reindex_action !== "status") {
        try { result.maintenance_pending = !await enqueue(env, "reindex", result.job_id); }
        catch { result.maintenance_pending = true; }
      }
      if ((args.op === "mutate" && result.committed) || (args.op === "publish" && result.has_more) || (args.op === "enrich" && (result.wal_appends || result.yielded))) {
        try { result.maintenance_pending = !await enqueue(env); }
        catch (error) {
          // Preserve the accepted write's durable outcome if scheduling fails.
          result.maintenance_pending = true;
          console.error("Antfly maintenance enqueue failed", String(error));
        }
      }
      return Response.json(result, env.WORKER_VERSION ? {headers: {"X-Antfly-Version": env.WORKER_VERSION.id}} : undefined);
    } catch (error) {
      const retryable = error instanceof Busy || /\bSQLITE_NOMEM\b/.test(String(error));
      return Response.json({ error: String(error.message ?? error) }, { status: retryable ? 503 : 409,
        headers: retryable ? { "Retry-After": "1" } : undefined });
    }
  },
  async queue(batch, env) {
    for (const message of batch.messages) {
      if (message.body?.incarnation !== env.ANTFLY_INCARNATION) { message.ack(); continue; }
      let publishing = !message.body.kind || message.body.kind === "reindex";
      try {
        if (message.body.kind === "reindex") {
          const result = await run(env, {op: "reindex", reindex_action: "step", reindex_job_id: message.body.job_id});
          if (result.has_more && !await enqueue(env, "reindex", result.job_id)) throw new Error("maintenance queue is not configured");
          message.ack(); continue;
        }
        if (message.body.kind === "compact") {
          await run(env, {op: "compact"}); message.ack(); continue;
        }
        if (message.body.kind === "collect") {
          const result = await collect(env, (input, token) => run(env, input, undefined, token), message.body.job_id);
          if (result.has_more && !await enqueue(env, "collect", result.job_id, result.delay_seconds)) throw new Error("maintenance queue is not configured");
          message.ack(); continue;
        }
        const result = await run(env, { op: "publish" });
        publishing = false;
        if (result.has_more) {
          if (!await enqueue(env)) throw new Error("maintenance queue is not configured");
        } else if (env.EMBEDDING_MODEL) {
          const enriched = await run(env, {op: "enrich"});
          // A bounded scan may checkpoint before reaching a pending document.
          // Continue from that checkpoint even when this scan appended nothing.
          const pending = enriched.wal_appends || BigInt((await run(env, {op: "status"})).pending_embeddings ?? "0") > 0n;
          if (pending && !await enqueue(env)) throw new Error("maintenance queue is not configured");
        }
        message.ack();
      } catch (error) {
        console.error("Antfly publication deferred", String(error));
        if (publishing && /Canceled|DeadlineExceeded|ObjectIoBudgetExceeded|OutOfMemory|HostedWorkingSetBudgetExceeded|ResponseTooLarge/.test(String(error))) {
          await env.COMMITS.getByName(env.ANTFLY_INCARNATION).lowerPublicationBudget(Number(env.PUBLICATION_RECORDS ?? 16));
        }
        message.retry({ delaySeconds: 10 });
      }
    }
  },
  // Recovery also covers a crash between WAL acceptance and queue admission.
  async scheduled(controller, env) {
    await enqueue(env);
    const reindex = await run(env, {op: "reindex", reindex_action: "status"});
    if (reindex.has_more) await enqueue(env, "reindex", reindex.job_id);
    if (env.AUTO_GC === "true" && Math.floor(controller.scheduledTime / 60000) % 15 === 0) await enqueue(env, "collect");
    if (env.EMBEDDING_MODEL && env.EMBEDDING_CHECKPOINTS !== "false") await pruneEmbeddingCheckpoints(env);
    if (env.AUTO_COMPACT === "true" && Math.floor(controller.scheduledTime / 60000) % 60 === 0) await enqueue(env, "compact");
  },
};
