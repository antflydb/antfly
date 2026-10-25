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

import {exactSearch,isExactSearch,rejectPrivateScanInput} from "./exact-search.mjs";
import {publicationFailureReducesBudget} from "./publication-budget.mjs";
import {IngressBudget} from "./ingress-budget.mjs";
import {CriticalPath} from "./trace.mjs";
import {tenantEnv,tenantEnvs} from "./tenants.mjs";
import {managedMaintenance} from "./maintenance-tickets.mjs";
import {createRuntime,Busy} from "./runtime.mjs";
import {runtimeName,readerSlots} from "./runtime-routing.mjs";
import {unsupportedRpc} from "./performance.mjs";
import {collect} from "./collect.mjs";
import {pruneEmbeddingCheckpoints} from "./embedding-checkpoints.mjs";
export {configureKernels} from "./kernels.mjs";
export {R2ReadRuntime,R2BuildRuntime} from "./runtime-actors.mjs";
export { R2CommitCoordinator } from "./r2-coordinator.mjs";
export { CommitCoordinator } from "./coordinator.mjs";
const localRuntime=createRuntime();
let admitted=0,readerCursor=0;const ingressBudget=new IngressBudget();
async function run(env,input,signal,collection,trace){
 if(isExactSearch(env,input))return exactSearch(env,input,next=>runStep(env,next,signal,collection,trace),{signal});
 return runStep(env,input,signal,collection,trace);
}
async function runStep(env,input,signal,collection,trace){
 const read=["lookup","list","search","count","status","latest"].includes(input.op);
 const binding=read?env.READERS:env.BUILDERS;
 if(env.COORDINATOR_BACKEND==="r2"&&input.op!=="mutate"&&binding){
  const slot=read?readerCursor++%readerSlots(env):0;
  try{return await binding.getByName(runtimeName(env.ANTFLY_INCARNATION,slot)).execute(input,collection,{trace_id:trace?.id});}
  catch(error){if(env.SPLIT_RUNTIME==="true"||!unsupportedRpc(error,"execute"))throw error;}
 }
 return localRuntime.run(env,input,signal,collection,undefined,trace);
}
async function background(env,input){
 if(env.COORDINATOR_BACKEND==="r2"&&env.BUILDERS)return env.BUILDERS.getByName(runtimeName(env.ANTFLY_INCARNATION,0)).execute(input);
 if(input.op==="collect")return collect(env,(request,token)=>run(env,request,undefined,token),input.job_id);
 const stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
 return input.op==="collect_metadata"?stub.collectMetadata(input.job_id,{scheduled:input.scheduled}):stub.migrateFromSqlite();
}

async function readJson(request,reservation) {
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
      reservation.add(value.byteLength);
      chunks.push(value);
    }
  } catch(error){try{await reader.cancel();}catch{}throw error;}finally { reader.releaseLock(); }
  const input = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { input.set(chunk, offset); offset += chunk.byteLength; }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(input));
}
async function enqueue(env, kind = "publish", job_id, delaySeconds = 0, admission, force=false, continuation) {
  if (!env.MAINTENANCE) return false;
  const stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
  if(kind==="publish"&&env.COORDINATOR_BACKEND==="r2"){
    try{admission??=job_id?{id:job_id,enqueue:true}:await stub.requestPublication(force);}catch(error){if(!unsupportedRpc(error,"requestPublication"))throw error;}
    if(admission&&!admission.enqueue)return true;
  }
  let ticket;
  if(env.COORDINATOR_BACKEND==="r2"&&managedMaintenance.has(kind)){
    try{ticket=continuation?await stub.advanceMaintenance(kind,continuation,job_id):await stub.requestMaintenance(kind,job_id);}catch(error){if(!unsupportedRpc(error,continuation?"advanceMaintenance":"requestMaintenance"))throw error;}
    if(ticket&&!ticket.enqueue)return true;
  }
  try{await env.MAINTENANCE.send({incarnation:env.ANTFLY_INCARNATION,...(env.ANTFLY_TENANT?{tenant:env.ANTFLY_TENANT}:{}),...(ticket?{maintenance:{id:ticket.id,step:ticket.step}}:{}),...(kind==="publish"?(admission?{kind,publication_id:admission.id}:{}):{kind,job_id})},{delaySeconds});}
  catch(error){if(admission)await stub.abandonPublication(admission.id).catch(()=>{});if(ticket)await stub.abandonMaintenance(kind,ticket).catch(()=>{});throw error;}
  return true;
}
async function continueMaintenance(env,message,kind,result){
 if(result.has_more){if(!await enqueue(env,kind,result.job_id,result.delay_seconds??0,undefined,false,message.body.maintenance))throw new Error("maintenance queue is not configured");}
 else if(message.body.maintenance)await env.COMMITS.getByName(env.ANTFLY_INCARNATION).finishMaintenance(kind,message.body.maintenance);
}
export default {
  async fetch(request, env, ctx) {
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path === "/" && env.ASSETS) return env.ASSETS.fetch(request);
    if (!env.API_TOKEN || request.headers.get("Authorization") !== `Bearer ${env.API_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    if (request.method !== "POST" || !["/api", "/ai"].includes(path)) return new Response("not found", { status: 404 });
    if(admitted>=(env.SPLIT_RUNTIME==="true"?32:env.READERS&&env.BUILDERS?16:8))return Response.json({error:"database is busy"},{status:503,headers:{"Retry-After":"1"}});
    admitted++;
    const trace=new CriticalPath({scope:"http"}),ingress=ingressBudget.open();
    try {
      if(request.headers.has("X-Antfly-Tenant")){if(env.COORDINATOR_BACKEND!=="r2")throw new Error("TenantRoutingRequiresR2");env=tenantEnv(env,request.headers.get("X-Antfly-Tenant"));}
      const args = await trace.span("parse",()=>readJson(request,ingress));
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
        const result = await background(env,{op:args.op,job_id:args.job_id});
        if (result.has_more) await enqueue(env, args.op, result.job_id, result.delay_seconds ?? 0);
        return Response.json(result);
      }
      if (args.op === "collect") {
        const result = await background(env,{op:"collect",job_id:args.job_id});
        if (result.has_more) await enqueue(env, "collect", result.job_id, result.delay_seconds);
        return Response.json(result, env.WORKER_VERSION ? {headers: {"X-Antfly-Version": env.WORKER_VERSION.id}} : undefined);
      }
      if (!["mutate", "lookup", "list", "search", "count", "status", "publish", "compact", "enrich", "latest", "reindex"].includes(args.op)) return new Response("unsupported operation", { status: 400 });
      rejectPrivateScanInput(args);
      const result = await trace.span("dispatch",()=>run(env, args, request.signal,undefined,trace));
      if (args.op === "reindex" && result.has_more && args.reindex_action !== "status") {
        try { result.maintenance_pending = !await enqueue(env, "reindex", result.job_id); }
        catch { result.maintenance_pending = true; }
      }
      const publication=result.__publication;delete result.__publication;
      if ((args.op === "mutate" && result.committed) || (args.op === "publish" && result.has_more) || (args.op === "enrich" && (result.wal_appends || result.yielded))) {
        if(args.op==="mutate"&&env.COORDINATOR_BACKEND==="r2"&&publication&&ctx?.waitUntil){
          // The native commit already reserved the durable outbox. Queue
          // delivery is repairable and does not belong on the mutation ACK.
          result.maintenance_pending=!env.MAINTENANCE;
          ctx.waitUntil((async()=>{const queued=new CriticalPath({id:trace.id,scope:"outbox"});queued.publicationId=publication.id;try{await queued.span("queue_delivery",()=>enqueue(env,"publish",undefined,0,publication));}catch(error){console.error("Antfly outbox delivery deferred",String(error));}finally{queued.emit(env.LOG_METRICS==="true");}})());
        }else try { result.maintenance_pending = !await trace.span("queue_admission",()=>enqueue(env,"publish",undefined,0,publication,args.op==="enrich")); }
        catch (error) {
          // Preserve the accepted write's durable outcome if scheduling fails.
          result.maintenance_pending = true;
          console.error("Antfly maintenance enqueue failed", String(error));
        }
      }
      return Response.json(result, env.WORKER_VERSION ? {headers: {"X-Antfly-Version": env.WORKER_VERSION.id}} : undefined);
    } catch (error) {
      const retryable = error instanceof Busy || /\b(SQLITE_NOMEM|ObjectStoreTransportFailed|ObjectStoreRateLimited|CoordinatorRecovering|ImmutableLogFenced|CoordinatorMigrationRequired|CoordinatorMigrationFrozen|CoordinatorCommitBusy|CoordinatorIoBudgetExceeded|PublicationExecutionBudgetExceeded|DeadlineExceeded|Canceled|WalCommitContended|WorkLeaseLost|10058)\b/.test(String(error));
      return Response.json({ error: String(error.message ?? error) }, { status: retryable ? 503 : 409,
        headers: retryable ? { "Retry-After": "1" } : undefined });
    } finally {ingress.release();admitted--;trace.emit(env.LOG_METRICS==="true");}
  },
  async queue(batch, rootEnv) {
    for (const message of batch.messages) {
      let env;try{env=tenantEnv(rootEnv,message.body?.tenant);}catch{message.ack();continue;}
      if (message.body?.incarnation !== env.ANTFLY_INCARNATION) { message.ack(); continue; }
      let publishing = !message.body.kind || ["publish","reindex"].includes(message.body.kind);
      try {
        if(message.body.maintenance&&!await env.COMMITS.getByName(env.ANTFLY_INCARNATION).validMaintenance(message.body.kind,message.body.maintenance,message.body.job_id)){message.ack();continue;}
        if (["collect_metadata", "migrate_coordinator"].includes(message.body.kind)) {
          if (message.body.kind === "collect_metadata" && !message.body.job_id) {message.ack();continue;}
          const result = await background(env,{op:message.body.kind,job_id:message.body.job_id});
          await continueMaintenance(env,message,message.body.kind,result);
          message.ack(); continue;
        }
        if (message.body.kind === "reindex") {
          const result = await run(env, {op: "reindex", reindex_action: "step", reindex_job_id: message.body.job_id});
          await continueMaintenance(env,message,"reindex",result);
          message.ack(); continue;
        }
        if (message.body.kind === "compact") {
          await run(env, {op: "compact"});await continueMaintenance(env,message,"compact",{done:true});message.ack();continue;
        }
        if (message.body.kind === "collect") {
          const result = await background(env,{op:"collect",job_id:message.body.job_id});
          await continueMaintenance(env,message,"collect",result);
          message.ack(); continue;
        }
        let publication=message.body.publication_id;const stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
        if(publication){try{if(!await stub.validPublication(publication)){message.ack();continue;}}catch(error){if(!unsupportedRpc(error,"validPublication"))throw error;publication=undefined;}}
        const trace=new CriticalPath({id:publication,scope:"publication"});trace.publicationId=publication;
        let result;try{result = await run(env, { op: "publish" },undefined,undefined,trace);}finally{trace.emit(env.LOG_METRICS==="true");}
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
        if (publishing && publicationFailureReducesBudget(error)) {
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
      const metadata = await background(env,{op:"collect_metadata",scheduled:true});
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
