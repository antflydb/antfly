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

import {PreparedPublication} from "./prepared-publication.mjs";
import {CriticalPath} from "./trace.mjs";
import {publicationCoordinator} from "./publication-view.mjs";
import {publicationExecutionFailure} from "./publication-budget.mjs";
import {withReadView} from "./read-view.mjs";
import {countMetadataRpc,instrumentTransport,unsupportedRpc} from "./performance.mjs";
import module from "./dist/antfly-serverless.wasm";
import { instantiateHost } from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import { createR2Transport } from "../../zig/lib/objectstore/wasm/r2.mjs";
import { withCommitCoordinator } from "../../zig/lib/objectstore/wasm/coordinated.mjs";
import { createImmutableReadCache } from "../../zig/lib/objectstore/wasm/cache.mjs";
export class Busy extends Error {}
export function createRuntime({kernel,sharedCache,resources=()=>({})}={}){
const lanes = {interactive: null, maintenance: null};
const busy = {interactive: false, maintenance: false};
const cache = sharedCache??createImmutableReadCache();
const profiles=new Set();
async function run(env, input, signal, collection, host, trace,readAdmission) {
  const ownedTrace=!trace;
  trace??=new CriticalPath({scope:"runtime:"+input.op});
  const runtimeStarted=trace.started;
  // Acquire a physical slot before opening short publication work leases.
  // Time waiting behind another tenant must not consume a new work lease.
  if(kernel&&!host){
    // Read pins last at least five minutes; the entire queued request still
    // has its original 30-second deadline and metadata/WAL grace bound. Prepare
    // independent views before scarce WASM admission. Publication leases remain
    // inside admission because their lifetime is only thirty seconds.
    if(!readAdmission&&env.COORDINATOR_BACKEND==="r2"&&!input.cursor&&["lookup","list","count","search","status","latest"].includes(input.op)){
      const physical={rpc_calls:1},started=performance.now();
      try{const view=await trace.span("rpc:openReadView",()=>env.COMMITS.getByName(env.ANTFLY_INCARNATION).openReadView({trace_id:trace.id,...(input.exact_state?{version:input.exact_state.generation}:{})}));readAdmission={view,started,physical};}
      catch(error){if(!unsupportedRpc(error,"openReadView"))throw error;readAdmission={legacy:true,started,physical};}
      physical.rpc_wait_ms=performance.now()-started;
    }
    try{return await kernel.withHost(host=>run(env,input,signal,collection,host,trace,readAdmission),{signal,fairKey:env.ANTFLY_INCARNATION,priority:["publish","enrich"].includes(input.op)?"publication":["compact","collect","collect_metadata","reindex"].includes(input.op)?"maintenance":"interactive",onStage:sample=>trace.stage(sample)});}finally{if(ownedTrace)trace.emit(env.LOG_METRICS==="true");}
  }
  const role = ["reindex", "publish", "compact", "enrich", "gc_prepare", "gc_trace", "gc_wal"].includes(input.op) ? "maintenance" : "interactive";
  const actorMutation=env.COORDINATOR_BACKEND==="r2"&&input.op==="mutate"&&(env.SPLIT_RUNTIME==="true"||env.COORDINATOR_RPC_FALLBACK!=="true");
  if (!actorMutation&&busy[role]) throw new Busy("database is busy");
  if(!actorMutation)busy[role] = true;
  let lane = actorMutation?null:lanes[role];
  let recycleAtBytes = 24 * 1024 * 1024;
  let preparedPublication,abortPublication,publicationStarted,emitOperationMetrics,nativeMetrics,operationSucceeded=false,publicationFinalized=false;
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
    if(kernel&&!actorMutation)lane=host;
    if (!actorMutation&&!lane) lanes[role] = lane = await instantiateHost(module, null, {
      onTransportError: (error, op) => console.error("Antfly object operation failed", op, String(error)),
      onLog: message => console.error("Antfly runtime", message),
    });
    const maxPutBytes = Number(env.OBJECT_MAX_PUT_BYTES ?? 4 * 1024 * 1024);
    if (!Number.isSafeInteger(maxPutBytes) || maxPutBytes < 1 || maxPutBytes > 8 * 1024 * 1024) throw new Error("InvalidHostedObjectBudget");
    const physical=readAdmission?.physical??{};
    const stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION);
    const rpc=async(method,...args)=>{physical.rpc_calls=(physical.rpc_calls??0)+1;if(method==="publicationCall"||method==="objectCall")countMetadataRpc(physical,args[0]);const start=performance.now();try{return await trace.span("rpc:"+method,()=>stub[method](...args));}finally{physical.rpc_wait_ms=(physical.rpc_wait_ms??0)+performance.now()-start;}};
    const coordinator={objectCall:(...args)=>rpc("objectCall",...args),openReadView:options=>rpc("openReadView",{...options,trace_id:trace.id})};
    let publicationView;
    if(env.COORDINATOR_BACKEND==="r2"&&["publish","compact"].includes(input.op)){
      if(input.op==="publish"&&env.BATCHED_PUBLICATION!=="false"){
        try{publicationView=await rpc("beginPublication",Number(env.PUBLICATION_RECORDS??16));preparedPublication=new PreparedPublication(publicationView);publicationStarted=performance.now();abortPublication=()=>rpc("abortPublication",publicationView.lease);}
        catch(error){if(!unsupportedRpc(error,"beginPublication"))throw error;}
      }
      try{publicationView??=await rpc("openPublication",Number(env.PUBLICATION_RECORDS??16));}
      catch(error){if(!unsupportedRpc(error,"openPublication"))throw error;}
    }
    let transport = cache.wrap(withCommitCoordinator(instrumentTransport(createR2Transport({ database: env.DATABASE }, {maxPutBytes}),physical),
      env.COORDINATOR_BACKEND==="r2"&&["publish","compact"].includes(input.op)?preparedPublication??publicationCoordinator({objectCall:coordinator.objectCall,publicationCall:(...args)=>rpc("publicationCall",...args)},publicationView):coordinator, collection), env.ANTFLY_INCARNATION);
    if(env.COORDINATOR_BACKEND==="r2"&&!readAdmission?.legacy&&["lookup","list","count","search","status","latest"].includes(input.op))transport=withReadView(transport,coordinator,env.ANTFLY_INCARNATION,{initial:readAdmission?.view,admittedAt:readAdmission?.started,onMetrics:()=>{physical.snapshot_reads=(physical.snapshot_reads??0)+1;}});
    const publicationBudget = publicationView?.budget??(["publish", "reindex"].includes(input.op) ? await env.COMMITS.getByName(env.ANTFLY_INCARNATION).publicationBudget(Number(env.PUBLICATION_RECORDS ?? 16)) : 16);
    const settings = input.op === "latest" ? {} : { coarse_probes:Number(input.coarse_probes??env.ANN_COARSE_PROBES??0), enrichment: Boolean(env.EMBEDDING_MODEL), embedding_dimensions: Number(env.EMBEDDING_DIMENSIONS ?? 384), publication_records: publicationBudget,
      publication_lease:preparedPublication?.lease??null,
      reindex_records: Math.min(64, publicationBudget),
      enrichment_documents: Number(env.ENRICHMENT_DOCUMENTS ?? 1),
      pack_small_bodies: env.COORDINATOR_BACKEND === "r2" && env.PACK_SMALL_BODIES !== "false",
      paged_text: env.PAGED_TEXT === "true" || env.PAGED_PROJECTIONS === "true",
      paged_projections: env.PAGED_PROJECTIONS === "true",
      embedding_checkpoints: env.EMBEDDING_CHECKPOINTS !== "false",
      embedding_windows: Number(env.EMBEDDING_WINDOWS ?? 1),
      embedding_identity: JSON.stringify([env.EMBEDDING_MODEL ?? "", env.EMBEDDING_REVISION ?? "1", env.AI_GATEWAY ?? ""]),
      working_set_bytes: kernel&&role==="interactive"?Math.min(8*1024*1024,Number(env.READER_WORKING_SET_BYTES??8*1024*1024)):Number(env.WORKING_SET_BYTES ?? 20 * 1024 * 1024),
      compaction_max_documents: Number(env.COMPACTION_MAX_DOCUMENTS ?? 1000),
      compaction_max_artifact_bytes: Number(env.COMPACTION_MAX_ARTIFACT_BYTES ?? 4 * 1024 * 1024) };
    const inference = async request => {
      if (request.task !== "embedding" || !env.EMBEDDING_MODEL?.startsWith("@cf/") || !env.AI_GATEWAY ||
        request.dimensions !== Number(env.EMBEDDING_DIMENSIONS ?? 384) || !Array.isArray(request.texts) || request.texts.length < 1 || request.texts.length > 64 || request.texts.some(text => typeof text !== "string" || text.length > 16384)) throw new Error("HostedEmbeddingCapabilityUnsupported");
      return {json: await env.AI.run(env.EMBEDDING_MODEL, {text: request.texts}, {gateway: {id: env.AI_GATEWAY}})};
    };
    if(actorMutation){try{return await trace.span("writer_rpc",()=>stub.mutateBatch({...input,...settings,__trace_id:trace.id}));}catch(error){if(env.SPLIT_RUNTIME==="true"||!unsupportedRpc(error,"mutateBatch"))throw error;return await run({...env,COORDINATOR_RPC_FALLBACK:"true"},input,signal,collection);}}
    const result=await lane.request({ ...input, ...settings, bucket: "database", prefix: "antfly", namespace: env.ANTFLY_INCARNATION }, { transport, signal, inference,...(input.exact_scan?{deadlineMs:Date.now()+5000}:{}),
      onMetrics: metrics => {
        nativeMetrics=metrics;
        emitOperationMetrics=()=>{if (env.LOG_METRICS === "true") console.log(JSON.stringify({event: "antfly.operation", trace_id:trace.id, op: input.op, exact_scan:input.exact_scan===true, role, ...metrics, operation_outcome:operationSucceeded?"ok":"error", operation_elapsed_ms:performance.now()-runtimeStarted, physical,
          ...resources(),cache_retained_bytes: cache.retainedBytes(), wasm_linear_total_bytes: kernel?kernel.memoryBytes(): Object.values(lanes).reduce((sum, instance) => sum + (instance?.memoryBytes() ?? 0), 0)}));};
      } });
    if(preparedPublication){await rpc("finalizePublication",preparedPublication.batch(),{records:result.consumed_records??0,elapsed_ms:performance.now()-publicationStarted,native_elapsed_ms:nativeMetrics?.elapsed_ms,native_io_wait_ms:nativeMetrics?.io_wait_ms,operations:nativeMetrics?.operations,transferred_bytes:nativeMetrics?.transferred_bytes,working_set_peak_bytes:nativeMetrics?.working_set_peak_bytes});publicationFinalized=true;}
    operationSucceeded=true;return result;
  } catch (error) {
    if (/IntegrityMismatch/.test(String(error))) cache.clear();
    throw preparedPublication?publicationExecutionFailure(error,nativeMetrics):error;
  } finally {
    if(abortPublication&&!publicationFinalized)try{await abortPublication();}catch(error){console.error("Antfly publication lease cleanup deferred",String(error));}
    try{emitOperationMetrics?.();}catch{}
    // Linear memory cannot shrink. Recycle between requests before allocator
    // size-class churn consumes the cap despite a small live working set.
    // Durable state and the immutable JS cache survive lane replacement.
    if (!kernel&&lane && (!lane.usable() || lane.memoryBytes() >= recycleAtBytes)) lanes[role] = null;
    if(!actorMutation)busy[role] = false;
    if(!kernel&&ownedTrace)trace.emit(env.LOG_METRICS==="true");
  }
}
return {run};
}
