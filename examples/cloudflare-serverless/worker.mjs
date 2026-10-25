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

import {tenantEnv,tenantEnvs} from "./tenants.mjs";
import {publicationCoordinator} from "./publication-view.mjs";
import {withReadView} from "./read-view.mjs";
import {countMetadataRpc,instrumentTransport,unsupportedRpc} from "./performance.mjs";
import module from "./dist/antfly-serverless.wasm";
import { instantiateHost } from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import { createR2Transport } from "../../zig/lib/objectstore/wasm/r2.mjs";
import { withCommitCoordinator } from "../../zig/lib/objectstore/wasm/coordinated.mjs";
import { createImmutableReadCache } from "../../zig/lib/objectstore/wasm/cache.mjs";
import { collect } from "./collect.mjs";
import { pruneEmbeddingCheckpoints } from "./embedding-checkpoints.mjs";
export { R2CommitCoordinator } from "./r2-coordinator.mjs";
export { CommitCoordinator } from "./coordinator.mjs";
const lanes = {interactive: null, maintenance: null};
const busy = {interactive: false, maintenance: false};
const cache = createImmutableReadCache();
const profiles=new Set();
class Busy extends Error {}
let admitted=0;
async function run(env, input, signal, collection) {
  const role = ["reindex", "publish", "compact", "enrich", "gc_prepare", "gc_trace", "gc_wal"].includes(input.op) ? "maintenance" : "interactive";
  const actorMutation=env.COORDINATOR_BACKEND==="r2"&&input.op==="mutate"&&env.COORDINATOR_RPC_FALLBACK!=="true";
  if (!actorMutation&&busy[role]) throw new Busy("database is busy");
  if(!actorMutation)busy[role] = true;
  let lane = actorMutation?null:lanes[role];
  let recycleAtBytes = 24 * 1024 * 1024;
  try {
    const configuredRecycle = Number(env.WASM_RECYCLE_BYTES ?? recycleAtBytes);
    if (!Number.isSafeInteger(configuredRecycle) || configuredRecycle < 4 * 1024 * 1024 || configuredRecycle > recycleAtBytes) throw new Error("InvalidHostedRecycleBudget");
    recycleAtBytes = configuredRecycle;
    if (!env.ANTFLY_INCARNATION) throw new Error("database incarnation is not configured");
    if (env.EMBEDDING_MODEL && (["reindex", "publish", "compact", "enrich"].includes(input.op) || (input.op === "search" && input.mode && input.mode !== "text"))) {
      const profile={model:env.EMBEDDING_MODEL,dimensions:Number(env.EMBEDDING_DIMENSIONS??384),revision:env.EMBEDDING_REVISION??"1"};
      const key=JSON.stringify([env.ANTFLY_INCARNATION,env.COORDINATOR_STORAGE_ID??env.ANTFLY_INCARNATION,profile]);
      if(!profiles.has(key)){
        if(!await env.COMMITS.getByName(env.ANTFLY_INCARNATION).ensureEmbeddingProfile(profile))throw new Error("HostedEmbeddingProfileChanged");
        if(profiles.size>=64)profiles.delete(profiles.values().next().value);profiles.add(key);
      }
    }
    if (!actorMutation&&!lane) lanes[role] = lane = await instantiateHost(module, null, {
      onTransportError: (error, op) => console.error("Antfly object operation failed", op, String(error)),
      onLog: message => console.error("Antfly runtime", message),
    });
    const maxPutBytes = Number(env.OBJECT_MAX_PUT_BYTES ?? 4 * 1024 * 1024);
    if (!Number.isSafeInteger(maxPutBytes) || maxPutBytes < 1 || maxPutBytes > 8 * 1024 * 1024) throw new Error("InvalidHostedObjectBudget");
    const physical={};
    const stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
    const rpc=async(method,...args)=>{physical.rpc_calls=(physical.rpc_calls??0)+1;if(method==="publicationCall"||method==="objectCall")countMetadataRpc(physical,args[0]);const start=performance.now();try{return await stub[method](...args);}finally{physical.rpc_wait_ms=(physical.rpc_wait_ms??0)+performance.now()-start;}};
    const coordinator={objectCall:(...args)=>rpc("objectCall",...args),openReadView:(...args)=>rpc("openReadView",...args)};
    let publicationView;
    if(env.COORDINATOR_BACKEND==="r2"&&input.op==="publish"){
      try{publicationView=await rpc("openPublication",Number(env.PUBLICATION_RECORDS??16));}
      catch(error){if(!unsupportedRpc(error,"openPublication"))throw error;}
    }
    let transport = cache.wrap(withCommitCoordinator(instrumentTransport(createR2Transport({ database: env.DATABASE }, {maxPutBytes}),physical),
      env.COORDINATOR_BACKEND==="r2"&&input.op==="publish"?publicationCoordinator({objectCall:coordinator.objectCall,publicationCall:(...args)=>rpc("publicationCall",...args)},publicationView):coordinator, collection), env.ANTFLY_INCARNATION);
    if(env.COORDINATOR_BACKEND==="r2"&&["lookup","list","count","search","status","latest"].includes(input.op))transport=withReadView(transport,coordinator,env.ANTFLY_INCARNATION,{onMetrics:()=>{physical.snapshot_reads=(physical.snapshot_reads??0)+1;}});
    const publicationBudget = publicationView?.budget??(["publish", "reindex"].includes(input.op) ? await env.COMMITS.getByName(env.ANTFLY_INCARNATION).publicationBudget(Number(env.PUBLICATION_RECORDS ?? 16)) : 16);
    const settings = input.op === "latest" ? {} : { enrichment: Boolean(env.EMBEDDING_MODEL), embedding_dimensions: Number(env.EMBEDDING_DIMENSIONS ?? 384), publication_records: publicationBudget,
      reindex_records: Math.min(64, publicationBudget),
      enrichment_documents: Number(env.ENRICHMENT_DOCUMENTS ?? 1),
      pack_small_bodies: env.COORDINATOR_BACKEND === "r2" && env.PACK_SMALL_BODIES !== "false",
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
    if(actorMutation){try{return await stub.mutateBatch({...input,...settings});}catch(error){if(!unsupportedRpc(error,"mutateBatch"))throw error;return await run({...env,COORDINATOR_RPC_FALLBACK:"true"},input,signal,collection);}}
    return await lane.request({ ...input, ...settings, bucket: "database", prefix: "antfly", namespace: env.ANTFLY_INCARNATION }, { transport, signal, inference,
      onMetrics: metrics => {
        if (env.LOG_METRICS === "true") console.log(JSON.stringify({event: "antfly.operation", op: input.op, role, ...metrics, physical,
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
    if(!actorMutation)busy[role] = false;
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
async function enqueue(env, kind = "publish", job_id, delaySeconds = 0, admission, force=false) {
  if (!env.MAINTENANCE) return false;
  const stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
  if(kind==="publish"&&env.COORDINATOR_BACKEND==="r2"){
    try{admission??=job_id?{id:job_id,enqueue:true}:await stub.requestPublication(force);}catch(error){if(!unsupportedRpc(error,"requestPublication"))throw error;}
    if(admission&&!admission.enqueue)return true;
  }
  try{await env.MAINTENANCE.send({incarnation:env.ANTFLY_INCARNATION,...(env.ANTFLY_TENANT?{tenant:env.ANTFLY_TENANT}:{}),...(kind==="publish"?(admission?{kind,publication_id:admission.id}:{}):{kind,job_id})},{delaySeconds});}
  catch(error){if(admission)await stub.abandonPublication(admission.id).catch(()=>{});throw error;}
  return true;
}
export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path === "/" && env.ASSETS) return env.ASSETS.fetch(request);
    if (!env.API_TOKEN || request.headers.get("Authorization") !== `Bearer ${env.API_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    if (request.method !== "POST" || !["/api", "/ai"].includes(path)) return new Response("not found", { status: 404 });
    if(admitted>=8)return Response.json({error:"database is busy"},{status:503,headers:{"Retry-After":"1"}});
    admitted++;
    try {
      if(request.headers.has("X-Antfly-Tenant")){if(env.COORDINATOR_BACKEND!=="r2")throw new Error("TenantRoutingRequiresR2");env=tenantEnv(env,request.headers.get("X-Antfly-Tenant"));}
      const args = await readJson(request);
      if (path === "/ai") {
        // Model and gateway selection are deployment policy. Native typed task
        // adapters remain the responsibility of #1271; this passes their model
        // input to Workers AI without assuming chat-completions semantics.
        if (!env.AI_MODEL?.startsWith("@cf/") || !env.AI_GATEWAY) return new Response("AI model and gateway are not configured", { status: 503 });
        return Response.json(await env.AI.run(env.AI_MODEL, args, { gateway: { id: env.AI_GATEWAY } }));
      }
      if (args.op === "prune_checkpoints") return Response.json(await pruneEmbeddingCheckpoints(env));
      if (args.op === "collect_metadata" || args.op === "migrate_coordinator") {
        const stub = env.COMMITS.getByName(env.ANTFLY_INCARNATION);
        const result = args.op === "collect_metadata" ? await stub.collectMetadata(args.job_id) : await stub.migrateFromSqlite();
        if (result.has_more) await enqueue(env, args.op, result.job_id, result.delay_seconds ?? 0);
        return Response.json(result);
      }
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
      const publication=result.__publication;delete result.__publication;
      if ((args.op === "mutate" && result.committed) || (args.op === "publish" && result.has_more) || (args.op === "enrich" && (result.wal_appends || result.yielded))) {
        try { result.maintenance_pending = !await enqueue(env,"publish",undefined,0,publication,args.op==="enrich"); }
        catch (error) {
          // Preserve the accepted write's durable outcome if scheduling fails.
          result.maintenance_pending = true;
          console.error("Antfly maintenance enqueue failed", String(error));
        }
      }
      return Response.json(result, env.WORKER_VERSION ? {headers: {"X-Antfly-Version": env.WORKER_VERSION.id}} : undefined);
    } catch (error) {
      const retryable = error instanceof Busy || /\b(SQLITE_NOMEM|ObjectStoreTransportFailed|ObjectStoreRateLimited|CoordinatorRecovering|ImmutableLogFenced|CoordinatorMigrationRequired|CoordinatorMigrationFrozen|CoordinatorCommitBusy|CoordinatorIoBudgetExceeded|DeadlineExceeded|WorkLeaseLost|10058)\b/.test(String(error));
      return Response.json({ error: String(error.message ?? error) }, { status: retryable ? 503 : 409,
        headers: retryable ? { "Retry-After": "1" } : undefined });
    } finally {admitted--;}
  },
  async queue(batch, rootEnv) {
    for (const message of batch.messages) {
      let env;try{env=tenantEnv(rootEnv,message.body?.tenant);}catch{message.ack();continue;}
      if (message.body?.incarnation !== env.ANTFLY_INCARNATION) { message.ack(); continue; }
      let publishing = !message.body.kind || ["publish","reindex"].includes(message.body.kind);
      try {
        if (["collect_metadata", "migrate_coordinator"].includes(message.body.kind)) {
          if (message.body.kind === "collect_metadata" && !message.body.job_id) {message.ack();continue;}
          const stub = env.COMMITS.getByName(env.ANTFLY_INCARNATION);
          const result = message.body.kind === "collect_metadata" ? await stub.collectMetadata(message.body.job_id) : await stub.migrateFromSqlite();
          if (result.has_more && !await enqueue(env, message.body.kind, result.job_id, result.delay_seconds ?? 0)) throw new Error("maintenance queue is not configured");
          message.ack(); continue;
        }
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
        let publication=message.body.publication_id;const stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
        if(publication){try{if(!await stub.validPublication(publication)){message.ack();continue;}}catch(error){if(!unsupportedRpc(error,"validPublication"))throw error;publication=undefined;}}
        const result = await run(env, { op: "publish" });
        publishing = false;
        if (result.has_more) {
          if (!await enqueue(env,"publish",publication)) throw new Error("maintenance queue is not configured");
        } else if (publication && (await stub.finishPublication(publication,result.wal_end_lsn)).has_more) {
          if(!await enqueue(env,"publish",publication))throw new Error("maintenance queue is not configured");
        } else if (env.EMBEDDING_MODEL) {
          const enriched = await run(env, {op: "enrich"});
          // A bounded scan may checkpoint before reaching a pending document.
          // Continue from that checkpoint even when this scan appended nothing.
          const pending = enriched.wal_appends || BigInt((await run(env, {op: "status"})).pending_embeddings ?? "0") > 0n;
          if (pending && !await enqueue(env,"publish",undefined,0,undefined,true)) throw new Error("maintenance queue is not configured");
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
  async scheduled(controller, rootEnv) {
    for(const env of tenantEnvs(rootEnv)){
    if (env.COORDINATOR_MIGRATION_REQUIRED === "true") {
      const status = await env.COMMITS.getByName(env.ANTFLY_INCARNATION).migrationStatus();
      if (!["none", "active"].includes(status.phase)) await enqueue(env, "migrate_coordinator");
      if (status.phase !== "active") continue;
    }
    const pendingEmbedding=env.EMBEDDING_MODEL&&BigInt((await run(env,{op:"status"})).pending_embeddings??"0")>0n;
    await enqueue(env,"publish",undefined,0,undefined,Boolean(pendingEmbedding));
    if (env.COORDINATOR_BACKEND === "r2") {
      const metadata = await env.COMMITS.getByName(env.ANTFLY_INCARNATION).collectMetadata(undefined,{scheduled:true});
      if (metadata.has_more) await enqueue(env, "collect_metadata", metadata.job_id, metadata.delay_seconds ?? 0);
    }
    const reindex = await run(env, {op: "reindex", reindex_action: "status"});
    if (reindex.has_more) await enqueue(env, "reindex", reindex.job_id);
    if (env.AUTO_GC === "true" && Math.floor(controller.scheduledTime / 60000) % 15 === 0) await enqueue(env, "collect");
    if (env.EMBEDDING_MODEL && env.EMBEDDING_CHECKPOINTS !== "false") await pruneEmbeddingCheckpoints(env);
    if (env.AUTO_COMPACT === "true" && Math.floor(controller.scheduledTime / 60000) % 60 === 0) await enqueue(env, "compact");
    }
  },
};
