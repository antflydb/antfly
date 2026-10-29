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

import {NativeReadPlans,withNativeReadPlan,withNativeReadPlanPermit,withNativeReadPreparation} from "./native-read-plan.mjs";
import {PublicationReadHints} from "./publication-read-hints.mjs";
import {PreparedPublication} from "./prepared-publication.mjs";
import {CriticalPath} from "./trace.mjs";
import {publicationCoordinator} from "./publication-view.mjs";
import {publicationExecutionFailure} from "./publication-budget.mjs";
import {prepareReadAdmission} from "./read-admission.mjs";
import {withReadView} from "./read-view.mjs";
import {withPointReadLane} from "./point-read-lane.mjs";
import {edgeCacheForEnv} from "./edge-immutable-cache.mjs";
import {countMetadataRpc,instrumentTransport,unsupportedRpc} from "./performance.mjs";
import module from "./dist/antfly-serverless.wasm";
import { instantiateHost } from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import { createR2Transport } from "../../zig/lib/objectstore/wasm/r2.mjs";
import { withCommitCoordinator } from "../../zig/lib/objectstore/wasm/coordinated.mjs";
import { createImmutableReadCache } from "../../zig/lib/objectstore/wasm/cache.mjs";
export class Busy extends Error {}
export function createRuntime({kernel,sharedCache,resources=()=>({}),background,pointReadLanes=false,readViews}={}){
const lanes = {interactive: null, maintenance: null};
const busy = {interactive: false, maintenance: false};
const cache = sharedCache??createImmutableReadCache();
const profiles=new Set(),nativeReadPlans=new NativeReadPlans();
async function run(env, input, signal, collection, host, trace,readAdmission,requestOptions={}) {
  const requestBackground=requestOptions.background??background;
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
    let pending;
    const launch=admission=>{
      if(pending)return pending;
      const execute=host=>run(env,input,signal,collection,host,trace,admission,requestOptions),options={signal,...(admission?.nativeResume?{preferredHostIdentity:admission.nativeResume.hostIdentity}:{}),...(admission?.nativePlan?{deadlineMs:admission.nativePlan.deadlineMs,beforePromote:()=>admission.nativePlan.fresh}:admission?.deadlineMs?{deadlineMs:admission.deadlineMs}:{}),fairKey:env.ANTFLY_INCARNATION,...(env.NATIVE_READ_AFFINITY==="true"?{affinityKey:env.ANTFLY_INCARNATION}:{}),priority:["publish","enrich"].includes(input.op)?"publication":["compact","collect","collect_metadata","reindex"].includes(input.op)?"maintenance":"interactive",onStage:sample=>trace.stage(sample)};
      const small=pointReadLanes&&env.DIRECT_READS==="true"&&input.op==="lookup"&&!input.cursor&&(admission?.view||admission?.nativePlan)&&JSON.stringify(input).length<=4096;
      pending=small?withPointReadLane(kernel,execute,{...options,onPromote:()=>trace.stage({name:"point_read_capacity_promotion",duration_ms:0})}):kernel.withHost(execute,options);
      pending.catch(()=>{});return pending;
    };
    const deferred=[];if(env.FUSED_READS==="true"&&input.op==="lookup")requestOptions={...requestOptions,deferMetrics:emit=>deferred.push(emit)};
    try{
      if(!readAdmission&&env.NATIVE_READ_PLAN==="true"&&env.COORDINATOR_BACKEND==="r2"&&input.op==="lookup"&&!input.cursor){
        const deadlineMs=Date.now()+30000,hint=nativeReadPlans.get(env);
        if(env.NATIVE_READ_DETACHED==="true"){
          // Authority starts in the submitting caller before any preparation
          // queue. Preparation returns its physical slot before awaiting it.
          let admitted,freshReady=false;const begun=new Promise(resolve=>admitted=resolve);
          const fresh=prepareReadAdmission(env,input,trace,readViews?.get(env),{background:requestBackground,deadlineMs,onAdmitted:admitted});
          fresh.then(()=>{freshReady=true;admitted();},()=>{freshReady=true;admitted();});fresh.catch(()=>{});
          let prepared,hostIdentity,usage,preparationPhysical;
          try{
            if(hint){
              await begun;
              const ticket=Array.from(crypto.getRandomValues(new Uint8Array(16)));
              const prepare=async allowed=>{
                if(!allowed)return;
                const execute=async host=>{
                  if(env.OPPORTUNISTIC_READ_PREPARE==="true"&&freshReady)return undefined;
                  hostIdentity=host.poolIdentity;
                  return run(env,input,signal,collection,host,trace,{nativePreparation:{hint,ticket,spentBytes:usage?.transferred_bytes??0},deadlineMs},{...requestOptions,
                    onNativeMetrics:sample=>{usage={
                      operations:(usage?.operations??0)+sample.operations,
                      transferred_bytes:(usage?.transferred_bytes??0)+sample.transferred_bytes,
                      elapsed_ms:(usage?.elapsed_ms??0)+sample.elapsed_ms,
                      io_wait_ms:(usage?.io_wait_ms??0)+sample.io_wait_ms,
                      working_set_peak_bytes:Math.max(usage?.working_set_peak_bytes??0,sample.working_set_peak_bytes??0),
                    };},
                    onPhysicalMetrics:sample=>{preparationPhysical??={};for(const[key,value]of Object.entries(sample??{}))if(typeof value==="number")preparationPhysical[key]=(preparationPhysical[key]??0)+value;},
                  });
                };
                const opportunistic=env.OPPORTUNISTIC_READ_PREPARE==="true";
                const options={signal,deadlineMs,fairKey:env.ANTFLY_INCARNATION,...(env.NATIVE_READ_AFFINITY==="true"?{affinityKey:env.ANTFLY_INCARNATION}:{}),onStage:sample=>trace.stage(sample),...(opportunistic?{tryOnly:true,preparationOnly:true}:{})};
                const runPreparation=async host=>{
                  if(!opportunistic)return execute(host);
                  kernel.waitingReadPlans=(kernel.waitingReadPlans??0)+1;
                  try{return await execute(host);}finally{kernel.waitingReadPlans--;kernel.drain?.();}
                };
                if(pointReadLanes&&env.DIRECT_READS==="true")prepared=await withPointReadLane(kernel,runPreparation,options);
                else if(opportunistic){const result=await kernel.tryWithHost(runPreparation,options);prepared=result.admitted?result.value:undefined;}
                else prepared=await kernel.withHost(execute,options);
              };
              if(env.OPPORTUNISTIC_READ_PREPARE==="true"){if(!freshReady)await prepare(true);}
              else await withNativeReadPlanPermit(kernel,pointReadLanes&&env.DIRECT_READS==="true"?8*1024*1024:kernel.reservationBytes,prepare,{deadlineMs,signal});
              if(prepared?.__read_prepared)prepared.ticket=ticket;
            }
            const admission=await fresh;
            return await launch({...admission,deadlineMs:Math.min(deadlineMs,admission.deadlineMs??Infinity),nativeResume:{ticket:prepared?.ticket??Array.from(crypto.getRandomValues(new Uint8Array(16))),spentBytes:Math.max(prepared?.__read_spent_bytes??0,usage?.transferred_bytes??0),hostIdentity,usage,physical:preparationPhysical}});
          }finally{await Promise.allSettled([fresh]);}
        }
        if(!hint){const admission=await prepareReadAdmission(env,input,trace,readViews?.get(env),{background:requestBackground,deadlineMs});return launch({nativePlan:{hint:null,fresh:Promise.resolve(admission),deadlineMs:Math.min(deadlineMs,admission.deadlineMs??Infinity)}});}
        const reservation=pointReadLanes&&env.DIRECT_READS==="true"&&JSON.stringify(input).length<=4096?8*1024*1024:kernel.reservationBytes;
        return await withNativeReadPlanPermit(kernel,reservation,async(speculative,releasePermit)=>{
          if(!speculative){const admission=await prepareReadAdmission(env,input,trace,readViews?.get(env),{background:requestBackground,deadlineMs});return launch({...admission,deadlineMs:Math.min(deadlineMs,admission.deadlineMs??Infinity)});}
          let admitted;const begun=new Promise(resolve=>admitted=resolve);
          const fresh=prepareReadAdmission(env,input,trace,readViews?.get(env),{background:requestBackground,deadlineMs,onAdmitted:admitted});
          fresh.then(()=>{admitted();releasePermit();},()=>{admitted();releasePermit();});await begun;
          try{return await launch({nativePlan:{hint,fresh,deadlineMs}});}
          catch(error){await fresh;throw error;}
          finally{await Promise.allSettled([fresh]);}
        },{deadlineMs,signal});
      }
      readAdmission??=await prepareReadAdmission(env,input,trace,readViews?.get(env),{background:requestBackground,...(env.FUSED_READS==="true"&&input.op==="lookup"?{onPrepared:admission=>launch(admission)}:{})});
      return await launch(readAdmission);
    }catch(error){if(pending)await Promise.allSettled([pending]);throw error;}
    finally{for(const emit of deferred)try{emit();}catch{}if(ownedTrace)trace.emit(env.LOG_METRICS==="true");}
  }
  const role = ["reindex", "publish", "compact", "enrich", "gc_prepare", "gc_trace", "gc_wal"].includes(input.op) ? "maintenance" : "interactive";
  const actorMutation=env.COORDINATOR_BACKEND==="r2"&&input.op==="mutate"&&(env.SPLIT_RUNTIME==="true"||env.COORDINATOR_RPC_FALLBACK!=="true");
  if (!actorMutation&&!kernel&&busy[role]) throw new Busy("database is busy");
  if(!actorMutation&&!kernel)busy[role] = true;
  let lane = actorMutation?null:lanes[role];
  let recycleAtBytes = 24 * 1024 * 1024;
  let physical;
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
    physical=readAdmission?.physical??{};
    for(const[key,value]of Object.entries(readAdmission?.nativeResume?.physical??{}))if(typeof value==="number")physical[key]=(physical[key]??0)+value;
    const stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION),readHints=env.COORDINATOR_BACKEND==="r2"&&["publish","compact","reindex"].includes(input.op)?new PublicationReadHints(env.ANTFLY_INCARNATION):null;
    const rpc=async(method,...args)=>{physical.rpc_calls=(physical.rpc_calls??0)+1;if(method==="publicationCall"||method==="objectCall")countMetadataRpc(physical,args[0]);const start=performance.now();try{return await trace.span("rpc:"+method,()=>stub[method](...args));}finally{physical.rpc_wait_ms=(physical.rpc_wait_ms??0)+performance.now()-start;}};
    const coordinator={objectCall:(request,body)=>rpc("objectCall",request,body,readHints?.forHead(request,body)),openReadView:options=>rpc("openReadView",{...options,trace_id:trace.id})};
    let publicationView;
    if(env.COORDINATOR_BACKEND==="r2"&&["publish","compact"].includes(input.op)){
      if(input.op==="compact"&&input.operation_id!==undefined){
        publicationView=await rpc("beginCompaction",input.operation_id);
        if(publicationView.complete){operationSucceeded=true;return publicationView.result;}
        preparedPublication=new PreparedPublication(publicationView);publicationStarted=performance.now();abortPublication=()=>rpc("abortPublication",publicationView.lease);
      }
      if(input.op==="publish"&&env.BATCHED_PUBLICATION!=="false"&&env.PUBLICATION_CONTINUATIONS!=="true"){
        try{publicationView=await rpc("beginPublication",Number(env.PUBLICATION_RECORDS??16));preparedPublication=new PreparedPublication(publicationView,{read:(request,body)=>rpc("objectCall",request,body)});publicationStarted=performance.now();abortPublication=()=>rpc("abortPublication",publicationView.lease);}
        catch(error){if(!unsupportedRpc(error,"beginPublication"))throw error;}
      }
      try{const method=input.op==="publish"&&env.PUBLICATION_CONTINUATIONS==="true"?"openPublicationPlan":"openPublication";publicationView??=await rpc(method,Number(env.PUBLICATION_RECORDS??16));}
      catch(error){if(!unsupportedRpc(error,"openPublication")&&!unsupportedRpc(error,"openPublicationPlan"))throw error;}
    }
    let transport = cache.wrap(withCommitCoordinator(edgeCacheForEnv(env,{background:requestBackground,budgetKey:()=>physical,onMetrics:sample=>{for(const[key,value]of Object.entries(sample))physical[key]=(physical[key]??0)+value;}})(instrumentTransport(createR2Transport({ database: env.DATABASE }, {maxPutBytes}),physical)),
      env.COORDINATOR_BACKEND==="r2"&&["publish","compact"].includes(input.op)?preparedPublication??publicationCoordinator({objectCall:coordinator.objectCall,publicationCall:(request,body,expected)=>rpc("publicationCall",request,body,expected,readHints?.forHead(request,body))},publicationView):coordinator, collection), env.ANTFLY_INCARNATION, env.DATABASE,env.FOREGROUND_CACHE_POLICY==="true"?{populate:input.op!=="list"&&!(input.op==="search"&&input.exact_scan),protect:input.op==="lookup"||input.op==="search"&&!input.exact_scan,preserveProtected:!(input.op==="lookup"||input.op==="search"&&!input.exact_scan)}:undefined);
    if(readHints)transport=readHints.wrap(transport);
    if(env.COORDINATOR_BACKEND==="r2"&&!readAdmission?.legacy&&!readAdmission?.nativePlan&&!readAdmission?.nativePreparation&&["lookup","list","count","search","status","latest"].includes(input.op))transport=withReadView(transport,coordinator,env.ANTFLY_INCARNATION,{initial:readAdmission?.view,admittedAt:readAdmission?.started,onMetrics:sample=>{for(const[key,value]of Object.entries(sample))physical[key]=(physical[key]??0)+value;}});
    if(readAdmission?.nativePlan)transport=withNativeReadPlan(transport,coordinator,env.ANTFLY_INCARNATION,{fresh:readAdmission.nativePlan.fresh,onMetrics:sample=>{for(const[key,value]of Object.entries(sample))physical[key]=(physical[key]??0)+value;}});
    if(readAdmission?.nativePreparation)transport=withNativeReadPreparation(transport);
    const activeContinuation=env.PUBLICATION_PREPARED_SLICES==="true"&&input.op==="publish"&&env.PUBLICATION_CONTINUATIONS==="true"&&input.publication_action!=="status"&&publicationView?.entries?.some(([key,record])=>key.endsWith("/PUBLICATION_PLAN")&&record?.body?.byteLength>0);
    if(activeContinuation){publicationView=await rpc("beginPublication",Number(env.PUBLICATION_RECORDS??16),true);preparedPublication=new PreparedPublication(publicationView,{read:(request,body)=>rpc("objectCall",request,body)});publicationStarted=performance.now();abortPublication=()=>rpc("abortPublication",publicationView.lease);transport=withCommitCoordinator(transport,preparedPublication,collection);}
    const publicationBudget = publicationView?.budget??(["publish", "reindex"].includes(input.op) ? await env.COMMITS.getByName(env.ANTFLY_INCARNATION).publicationBudget(Number(env.PUBLICATION_RECORDS ?? 16)) : 16);
    const settings = input.op === "latest" ? {} : { ...(env.NATIVE_READ_STATE==="true"?{native_read_state:true}:{}),...(env.WAL_DOCUMENT_INDEX==="true"?{wal_document_index:true}:{}), wal_inline_tail:env.WAL_INLINE_TAIL==="true", coarse_probes:Number(input.coarse_probes??env.ANN_COARSE_PROBES??0), enrichment: Boolean(env.EMBEDDING_MODEL), embedding_dimensions: Number(env.EMBEDDING_DIMENSIONS ?? 384), publication_records: publicationBudget,
      publication_lease:preparedPublication?.lease??null,
      reindex_records: Math.min(64, publicationBudget),
      enrichment_documents: Number(env.ENRICHMENT_DOCUMENTS ?? 1),
      ...(readAdmission?.nativePlan?{native_read_plan_mode:true,native_read_plan:readAdmission.nativePlan.hint}:{}),
      ...(readAdmission?.nativePreparation?{native_read_plan_mode:true,native_read_plan:readAdmission.nativePreparation.hint,native_read_prepare:readAdmission.nativePreparation.ticket,native_read_spent_bytes:readAdmission.nativePreparation.spentBytes??0}:{}),
      ...(readAdmission?.nativeResume?{native_read_plan_mode:true,native_read_resume:readAdmission.nativeResume.ticket,native_read_spent_bytes:readAdmission.nativeResume.spentBytes}:{}),
      ...(input.op==="publish"&&env.PUBLICATION_CONTINUATIONS==="true"?{publication_continuations:true,publication_batch_probe:true,publication_continuation_force:env.PUBLICATION_CONTINUATION_FORCE==="true",publication_units:Number(env.PUBLICATION_UNITS??128),...(env.PUBLICATION_ADAPTIVE_SLICES==="true"?{publication_adaptive_slices:true,publication_max_units:publicationView?.posting_limit??4096}:{}),...(env.PUBLICATION_PREPARED_SLICES==="true"?{publication_prepared_slices:true}:{})}:{}),
      ...(env.INLINE_POINT_BODIES === "true"?{inline_point_bodies:true}:{}),
      ...(env.CANONICAL_POINT_BODIES === "true"?{canonical_point_bodies:true}:{}),
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
    const {inline_point_bodies:ignoredInline,canonical_point_bodies:ignoredCanonical,native_read_plan_mode:ignoredReadMode,native_read_state:ignoredReadState,wal_document_index:ignoredDocumentIndex,native_read_plan:ignoredReadPlan,native_read_prepare:ignoredReadPrepare,native_read_resume:ignoredReadResume,native_read_spent_bytes:ignoredReadSpent,native_write_plan:ignoredWritePlan,publication_continuations:ignoredContinuation,publication_batch_probe:ignoredPublicationProbe,publication_batch_version:ignoredBatchVersion,publication_continuation_force:ignoredForce,publication_units:ignoredUnits,publication_adaptive_slices:ignoredAdaptive,publication_max_units:ignoredMaximum,publication_prepared_slices:ignoredPrepared,compaction_probe_only:ignoredProbe,...nativeInput}=input;
    const plannedCompact=env.NATIVE_COMPACTION_PLAN==="true"&&env.COORDINATOR_BACKEND==="r2"&&input.op==="compact"&&input.operation_id===undefined&&env.BATCHED_PUBLICATION!=="false";
    const plannedPublication=input.op==="publish"&&env.PUBLICATION_CONTINUATIONS==="true";
    const compactDeadline=Date.now()+30000;let probeOperations=0,probeBytes=0,probeMetrics,batchRecords,batchVersion,preparedSlice=false;
    const priorReadUsage=readAdmission?.nativeResume?.usage;
    if(priorReadUsage&&(!Number.isSafeInteger(priorReadUsage.operations)||!Number.isSafeInteger(priorReadUsage.transferred_bytes)||priorReadUsage.operations>=1024||priorReadUsage.transferred_bytes>=32*1024*1024))throw Error("ObjectIoBudgetExceeded");
    const executeNative=probe=>lane.request({ ...nativeInput, ...settings,...(plannedPublication&&preparedPublication?{publication_continuations:preparedPublication.format===3,publication_batch_probe:false,publication_lease:preparedPublication.lease,...(batchRecords!==undefined?{publication_records:Math.min(batchRecords,publicationView.budget),publication_batch_version:batchVersion}:{})}:{}),...(plannedCompact?{compaction_probe_only:probe,publication_lease:preparedPublication?.lease??null}:{}), bucket: "database", prefix: "antfly", namespace: env.ANTFLY_INCARNATION }, { transport, signal, inference,background:requestBackground,onIoSettled:sample=>{if(env.LOG_METRICS==="true")console.log(JSON.stringify(sample));},...(priorReadUsage?{maxOperations:1024-priorReadUsage.operations,maxTransferBytes:32*1024*1024-priorReadUsage.transferred_bytes}:{}),...(readAdmission?.nativePlan?{deadlineMs:readAdmission.nativePlan.deadlineMs}:readAdmission?.deadlineMs?{deadlineMs:readAdmission.deadlineMs}:{}),...(plannedCompact||plannedPublication?{deadlineMs:compactDeadline,maxOperations:1024-probeOperations,maxTransferBytes:32*1024*1024-probeBytes}:{}),...(input.exact_scan?{deadlineMs:Date.now()+5000}:{}),
      onMetrics: metrics => {
        if(probeMetrics)metrics={...metrics,operations:metrics.operations+probeMetrics.operations,transferred_bytes:metrics.transferred_bytes+probeMetrics.transferred_bytes,elapsed_ms:metrics.elapsed_ms+probeMetrics.elapsed_ms,io_wait_ms:metrics.io_wait_ms+probeMetrics.io_wait_ms,working_set_peak_bytes:Math.max(metrics.working_set_peak_bytes,probeMetrics.working_set_peak_bytes)};
        requestOptions.onNativeMetrics?.(metrics);
        if(priorReadUsage)metrics={...metrics,operations:metrics.operations+priorReadUsage.operations,transferred_bytes:metrics.transferred_bytes+priorReadUsage.transferred_bytes,elapsed_ms:metrics.elapsed_ms+priorReadUsage.elapsed_ms,io_wait_ms:metrics.io_wait_ms+priorReadUsage.io_wait_ms,working_set_peak_bytes:Math.max(metrics.working_set_peak_bytes,priorReadUsage.working_set_peak_bytes),native_read_preparation_operations:priorReadUsage.operations};
        nativeMetrics=metrics;
        emitOperationMetrics=()=>{if (env.LOG_METRICS === "true") console.log(JSON.stringify({event: readAdmission?.nativePreparation?"antfly.read_preparation":"antfly.operation", trace_id:trace.id, op: input.op, exact_scan:input.exact_scan===true, role, ...metrics, operation_outcome:operationSucceeded?"ok":"error", operation_elapsed_ms:performance.now()-runtimeStarted, physical,
          ...resources(),cache_retained_bytes: cache.retainedBytes(), wasm_linear_total_bytes: kernel?kernel.memoryBytes(): Object.values(lanes).reduce((sum, instance) => sum + (instance?.memoryBytes() ?? 0), 0)}));};
      } });
    let result;
    try{
      result=await executeNative(plannedCompact);
      if(plannedPublication&&result.__publication_slice===true){preparedSlice=true;throw Error("PublicationNeedsPreparedSlice");}
      if(plannedPublication&&result.__publication_batch_records!==undefined){
        batchRecords=result.__publication_batch_records;batchVersion=result.__publication_batch_version;
        if(!Number.isInteger(batchRecords)||batchRecords<1||batchRecords>64||!/^[0-9]+$/.test(batchVersion))throw Error("InvalidPublicationBatchPlan");
        throw Error("PublicationNeedsPreparedBatch");
      }
    }catch(error){
      if(!(plannedCompact&&error?.message==="CompactionNeedsPublicationLease")&&!(plannedPublication&&["PublicationNeedsPreparedBatch","PublicationNeedsPreparedSlice"].includes(error?.message)))throw error;
      if(!Number.isSafeInteger(nativeMetrics?.operations)||!Number.isSafeInteger(nativeMetrics?.transferred_bytes))throw Error("NativePlanAccountingMissing");
      probeOperations=nativeMetrics.operations;probeBytes=nativeMetrics.transferred_bytes;probeMetrics=nativeMetrics;
      if(probeOperations>=1024||probeBytes>=32*1024*1024)throw Error("ObjectIoBudgetExceeded");
      if(env.LOG_METRICS==="true")console.log(JSON.stringify({event:plannedCompact?"antfly.compaction_plan":"antfly.publication_batch_plan",trace_id:trace.id,operations:probeOperations,transferred_bytes:probeBytes}));
      publicationView=await rpc("beginPublication",Number(env.PUBLICATION_RECORDS??16),preparedSlice);preparedPublication=new PreparedPublication(publicationView,{read:(request,body)=>rpc("objectCall",request,body)});publicationStarted=performance.now();abortPublication=()=>rpc("abortPublication",publicationView.lease);
      transport=withCommitCoordinator(transport,preparedPublication,collection);
      result=await executeNative(false);
    }
    if(preparedPublication){const batch=preparedPublication.batch(),head=batch.writes.find(([key])=>key===`antfly/progress/${env.ANTFLY_INCARNATION}/HEAD`),hints=head?readHints?.forHead({op:"put",key:head[0]},head[1].body):undefined;
      if(input.op==="compact"&&input.operation_id!==undefined)await rpc("finalizeCompaction",input.operation_id,batch,result,hints);
      else await rpc("finalizePublication",batch,input.op==="publish"?{posting_quantum:result.posting_quantum,records:result.consumed_records??0,elapsed_ms:performance.now()-publicationStarted,native_elapsed_ms:nativeMetrics?.elapsed_ms,native_io_wait_ms:nativeMetrics?.io_wait_ms,operations:nativeMetrics?.operations,transferred_bytes:nativeMetrics?.transferred_bytes,working_set_peak_bytes:nativeMetrics?.working_set_peak_bytes}:undefined,hints);publicationFinalized=true;}
    if(readAdmission?.nativePlan||readAdmission?.nativeResume){nativeReadPlans.remember(env,result.__read_plan);const{__read_plan:ignoredPlan,...publicResult}=result;result=publicResult;}
    if(input.op==="publish"&&env.PUBLICATION_CONTINUATIONS==="true"&&result.job_id&&env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.publication_progress",trace_id:trace.id,job_id:result.job_id,checkpoint_sha256:result.checkpoint_sha256,receipt_sha256:result.receipt_sha256,posting_quantum:result.posting_quantum,phase:result.phase,offset:result.offset??0,published:result.published,wal_end_lsn:result.wal_end_lsn}));
    const accepted=readAdmission?.nativePlan?await readAdmission.nativePlan.fresh:readAdmission;accepted?.confirm?.(result.visibility);
    operationSucceeded=true;return result;
  } catch (error) {
    if(readAdmission?.nativePlan){try{(await readAdmission.nativePlan.fresh)?.invalidate?.();}catch{}}else readAdmission?.invalidate?.();
    if (/IntegrityMismatch/.test(String(error))) cache.clear();
    throw preparedPublication?publicationExecutionFailure(error,nativeMetrics):error;
  } finally {
    if(abortPublication&&!publicationFinalized)try{await abortPublication();}catch(error){console.error("Antfly publication lease cleanup deferred",String(error));}
    requestOptions.onPhysicalMetrics?.(physical);
    try{if(readAdmission?.nativePreparation)emitOperationMetrics?.();else if(requestOptions.deferMetrics&&emitOperationMetrics)requestOptions.deferMetrics(emitOperationMetrics);else emitOperationMetrics?.();}catch{}
    // Linear memory cannot shrink. Recycle between requests before allocator
    // size-class churn consumes the cap despite a small live working set.
    // Durable state and the immutable JS cache survive lane replacement.
    if (!kernel&&lane && (!lane.usable() || lane.memoryBytes() >= recycleAtBytes)) lanes[role] = null;
    if(!actorMutation&&!kernel)busy[role] = false;
    if(!kernel&&ownedTrace)trace.emit(env.LOG_METRICS==="true");
  }
}
return {run};
}
