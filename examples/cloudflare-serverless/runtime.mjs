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

import {publicationCoordinator} from "./publication-view.mjs";
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
async function run(env, input, signal, collection, host) {
  // Acquire a physical slot before opening publication leases or snapshots.
  // Time waiting behind another tenant must not consume a new work lease.
  if(kernel&&!host)return kernel.withHost(host=>run(env,input,signal,collection,host),{signal});
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
    if(kernel&&!actorMutation)lane=host;
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
      working_set_bytes: kernel&&role==="interactive"?Math.min(8*1024*1024,Number(env.READER_WORKING_SET_BYTES??8*1024*1024)):Number(env.WORKING_SET_BYTES ?? 20 * 1024 * 1024),
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
          ...resources(),cache_retained_bytes: cache.retainedBytes(), wasm_linear_total_bytes: kernel?kernel.memoryBytes(): Object.values(lanes).reduce((sum, instance) => sum + (instance?.memoryBytes() ?? 0), 0)}));
      } });
  } catch (error) {
    if (/IntegrityMismatch/.test(String(error))) cache.clear();
    throw error;
  } finally {
    // Linear memory cannot shrink. Recycle between requests before allocator
    // size-class churn consumes the cap despite a small live working set.
    // Durable state and the immutable JS cache survive lane replacement.
    if (!kernel&&lane && (!lane.usable() || lane.memoryBytes() >= recycleAtBytes)) lanes[role] = null;
    if(!actorMutation)busy[role] = false;
  }
}
return {run};
}
