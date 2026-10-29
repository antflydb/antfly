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

import {edgeCacheForEnv} from "./edge-immutable-cache.mjs";
import {actorOwner} from "./actor-owner.mjs";
import {writeReadHints} from "./publication-read-hints.mjs";
import {FreshReadAdmissions} from "./fresh-read-admissions.mjs";
import {metadataObjectCall} from "./metadata-object-call.mjs";
import {applyPublication} from "./prepared-publication.mjs";
import {CriticalPath} from "./trace.mjs";
import {nextPublicationBudget} from "./publication-budget.mjs";
import {encode} from "./r2-coordinator-store.mjs";
import {publicationKeys,publicationCall,publicationSnapshot} from "./publication-view.mjs";
import {metadataKernel,readObservationKernel,buildKernel,sharedCache,boundRows,kernelMetrics} from "./kernels.mjs";
import {MaintenanceTickets} from "./maintenance-tickets.mjs";
import {compactionIdentity,readCompactionReceipt,beginCompactionReceipt,finishCompactionReceipt} from "./compaction-receipt.mjs";
import {PreparedWal} from "./prepared-wal.mjs";
import {OptimisticWal} from "./optimistic-wal.mjs";
import {metadataKeys} from "./read-view.mjs";
import {MutationGroups} from "./mutation-groups.mjs";
import {metadataCollectionDue} from "./maintenance-policy.mjs";
import {instrumentTransport} from "./performance.mjs";
import {PublicationScheduler} from "./publication-scheduler.mjs";
import {tenantEnvs} from "./tenants.mjs";
import {captureReadView,inlineReadSet} from "./read-view.mjs";
import {createR2Transport} from "../../zig/lib/objectstore/wasm/r2.mjs";
import {withCommitCoordinator} from "../../zig/lib/objectstore/wasm/coordinated.mjs";
import {openAuthority, activateR2} from "./coordinator-authority.mjs";
import {DurableObject} from "cloudflare:workers";
import module from "./dist/antfly-serverless.wasm";
import {AuthorityAdmission} from "./authority-admission.mjs";
import {placementEnv} from "./runtime-placement.mjs";
import {R2CoordinatorStore} from "./r2-coordinator-store.mjs";
import {migrateCoordinator, MIGRATION} from "./r2-coordinator-migration.mjs";
import {collectCoordinator} from "./r2-coordinator-gc.mjs";
import {R2CollectionJournal} from "./r2-collection-journal.mjs";
export class R2CommitCoordinator extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    env=placementEnv(env);
    if (!env.ANTFLY_INCARNATION) throw new Error("CoordinatorNamespaceRequired");
    if(env.ANTFLY_TENANTS){const binding=env.COMMITS;env=tenantEnvs(env).find(candidate=>binding.idFromName(candidate.ANTFLY_INCARNATION).equals(ctx.id));if(!env)throw new Error("UnknownTenant");}
    this.env = env;
    const instanceId=crypto.randomUUID(),owner=actorOwner(ctx.id.toString(),env.WORKER_VERSION?.id,env.COORDINATOR_STORAGE_ID??env.ANTFLY_INCARNATION);
    this.store = new R2CoordinatorStore(module, env.DATABASE, env.COORDINATOR_STORAGE_ID ?? env.ANTFLY_INCARNATION,{owner,inlineReadSet:env.INLINE_METADATA_ROOTS==="true"?inlineReadSet(env.ANTFLY_INCARNATION):undefined,remoteCache:edgeCacheForEnv(env,{background:promise=>ctx.waitUntil(promise)}),kernel:metadataKernel,readKernel:readObservationKernel(),cache:sharedCache,boundRows,background:promise=>ctx.waitUntil(promise),onMetrics:sample=>{if(env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.coordinator",...sample,...kernelMetrics(),actor_instance_id:instanceId}));}});
    this.serviceStore = Object.create(this.store);
    this.serviceStore.transaction = run => this.transaction(run);
    this.maintenanceStore=Object.create(this.store);this.maintenanceStore.transaction=run=>this.transaction(run,{priority:"maintenance"});
    this.publication=new PublicationScheduler(this.maintenanceStore,env.ANTFLY_INCARNATION);
    this.maintenance=new MaintenanceTickets(this.maintenanceStore);
    this.journal = new R2CollectionJournal(this.maintenanceStore);
    this.authorityAdmission=new AuthorityAdmission(async()=>{if(this.store.ownerReady)await this.store.ownerReady;return openAuthority(env,"r2");});
    if(env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.actor_started",role:"writer",actor_id:ctx.id.toString(),actor_instance_id:instanceId,version:env.WORKER_VERSION?.id,owner_identity:owner?"actor-version":"ephemeral",...kernelMetrics()}));
  }
  async ensureAuthority(){if(!this.authority){const authority=await this.authorityAdmission.get();this.authority??=authority;}return this.authority;}
  requestMaintenance(kind,jobId){return this.maintenance.request(kind,jobId);}
  validMaintenance(kind,ticket,jobId){return this.maintenance.valid(kind,ticket,jobId);}
  advanceMaintenance(kind,ticket,jobId){return this.maintenance.advance(kind,ticket,jobId);}
  finishMaintenance(kind,ticket){return this.maintenance.finish(kind,ticket);}
  abandonMaintenance(kind,ticket){return this.maintenance.abandon(kind,ticket);}
  requestPublication(force=false){return this.publication.request({force});}
  validPublication(id){return this.publication.valid(id);}
  abandonPublication(id){return this.publication.abandon(id);}
  finishPublication(id,watermark){return this.publication.finish(id,watermark);}
  openReadView(options={}) {
    const observedAt=Date.now();
    this.readAdmissions??=new FreshReadAdmissions({delayMs:Number(this.env.READ_VIEW_BATCH_MS??2)});
    return this.readAdmissions.run(options.immutable===false?JSON.stringify(["metadata",options.version??"latest"]):options.version??"latest",async(traceIds,close)=>{
      await this.ensureAuthority();
      const capture=async tx=>{
        const keys=await metadataKeys(this.env.ANTFLY_INCARNATION);
        const[migration]=await tx.getMany([MIGRATION,keys.head,keys.wal,keys.floor,...(options.immutable===false?[]:[keys.hints])]);
        if(migration&&migration.phase!=="active"||this.env.COORDINATOR_MIGRATION_REQUIRED==="true"&&!migration||this.authority.descriptor.mode!=="r2")throw Error("CoordinatorMigrationRequired");
        return captureReadView(tx,keys,{...options,lease_observed_at_ms:observedAt,read_lease_policy:this.env.FENCED_READ_LEASES!=="false"?this.authority.descriptor.read_lease_policy:undefined});
      };
      if(this.env.READ_ONLY_SNAPSHOTS==="false")close();
      return(this.env.READ_ONLY_SNAPSHOTS==="false"?Promise.reject(Error("ReadSnapshotNeedsAdmission")):this.store.readOnly(capture,{traceId:options.trace_id,traceIds,onAdmission:close,deadlineMs:options.deadlineMs,priority:options.immutable===false?"commit":"interactive"})).catch(error=>{
        close();if(!/ReadSnapshotNeeds(?:Admission|Pin)/.test(String(error)))throw error;
        return this.transaction(capture,{traceId:options.trace_id,deadlineMs:options.deadlineMs});
      }).then(view=>options.immutable!==false&&!view.immutable&&this.store.readHints?.version===view.version?{...view,immutable:structuredClone(this.store.readHints.records)}:view);
    },options.trace_id,{untilAdmission:true,admissionClass:options.immutable===false?"commit":"read"});
  }
  async nativePublication(tx,op,lease,owner=lease?.owner){
    const keys=await metadataKeys(this.env.ANTFLY_INCARNATION),names=[keys.head,keys.progress+"HEAD_FENCE"],values=await tx.getMany(names),records=new Map(names.map((key,i)=>[key,values[i]]));
    // No recursive metadata-kernel acquisition: the tiny native lease action
    // operates only on these transaction-owned copies and writes the overlay.
    const local={get:async key=>{if(!records.has(key))throw Error("PublicationLeaseKeyUnsupported");return structuredClone(records.get(key));},put:(key,value)=>{if(!records.has(key))throw Error("PublicationLeaseKeyUnsupported");records.set(key,structuredClone(value));tx.put(key,value);}};
    const transport=withCommitCoordinator(async request=>{if(request.op!=="bucket_exists")throw Error("PublicationLeaseIoUnsupported");return{json:{exists:true}};},{objectCall:(request,body)=>this.objectCallInTransaction(local,request,body)});
    return metadataKernel.request({op,publication_owner:owner,publication_lease:lease??null,bucket:"database",prefix:"antfly",namespace:this.env.ANTFLY_INCARNATION},{transport,fairKey:this.store.namespace,priority:"maintenance",deadlineMs:Date.now()+5000});
  }
  async beginPublication(maximum,continuations=false){
    if(!Number.isSafeInteger(maximum)||maximum<1||maximum>256)throw Error("InvalidPublicationBudget");
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION,continuations);
    return this.transaction(async tx=>{
      const lease=await this.nativePublication(tx,"publication_acquire",null,crypto.randomUUID());
      await captureReadView(tx,await metadataKeys(this.env.ANTFLY_INCARNATION),{immutable:false});
      return{format:continuations?3:2,lease,posting_limit:await tx.get("__antfly_publication_quantum_limit")??4096,budget:Math.min(maximum,await tx.get("__antfly_publication_budget")??maximum),entries:await publicationSnapshot(tx,keys)};
    },{priority:"maintenance"});
  }
  compactionIdentity(id){return compactionIdentity(id,{max_documents:Number(this.env.COMPACTION_MAX_DOCUMENTS??1000),max_artifact_bytes:Number(this.env.COMPACTION_MAX_ARTIFACT_BYTES??4*1024*1024)});}
  async compactionStatus(id){
    const identity=await this.compactionIdentity(id);
    return this.transaction(async tx=>{const prior=await readCompactionReceipt(tx,identity);return prior?.state==="complete"?{state:"complete",result:prior.result}:{state:prior?.state??"unknown"};});
  }
  async beginCompaction(id){
    const identity=await this.compactionIdentity(id),keys=await publicationKeys(this.env.ANTFLY_INCARNATION);
    return this.transaction(tx=>beginCompactionReceipt(tx,identity,async()=>{
      const lease=await this.nativePublication(tx,"publication_acquire",null,crypto.randomUUID());
      await captureReadView(tx,await metadataKeys(this.env.ANTFLY_INCARNATION),{immutable:false});
      return{format:2,lease,budget:16,entries:await publicationSnapshot(tx,keys)};
    }),{priority:"maintenance"});
  }
  async finalizeCompaction(id,batch,result,readHints){
    const identity=await this.compactionIdentity(id),keys=await publicationKeys(this.env.ANTFLY_INCARNATION);
    return this.transaction(tx=>finishCompactionReceipt(tx,identity,batch,result,async receipt=>{
      await this.nativePublication(tx,"publication_validate",batch?.lease);
      const entries=await publicationSnapshot(tx,keys);
      await applyPublication(tx,entries.map(([key])=>key),batch);
      const head=await tx.get(keys[0]),version=head?new TextDecoder().decode(head.body).trim():"0";
      if(version!==receipt.version)throw Error("InvalidCompactionResult");
      const written=batch.writes.find(([key])=>key===keys[0]);
      if(receipt.published!==Boolean(written))throw Error("InvalidCompactionResult");
      if(written)writeReadHints(tx,this.env.ANTFLY_INCARNATION,version,readHints);
      if(!(await this.nativePublication(tx,"publication_release",batch.lease)).released)throw Error("WorkLeaseLost");
    }),{priority:"maintenance"});
  }
  async finalizePublication(batch,observation,readHints){
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION,batch?.format===3);
    const result=await this.transaction(async tx=>{
      await this.nativePublication(tx,"publication_validate",batch?.lease);
      const entries=await publicationSnapshot(tx,keys);
      await applyPublication(tx,entries.map(([key])=>key),batch);
      const head=batch.writes.find(([key])=>key===`antfly/progress/${this.env.ANTFLY_INCARNATION}/HEAD`);
      if(head)writeReadHints(tx,this.env.ANTFLY_INCARNATION,new TextDecoder().decode(head[1].body).trim(),readHints);
      if(!(await this.nativePublication(tx,"publication_release",batch.lease)).released)throw Error("WorkLeaseLost");
      if(this.env.PUBLICATION_ADAPTIVE_SLICES==="true"&&Number.isSafeInteger(observation?.posting_quantum)&&observation.posting_quantum>=1&&observation.posting_quantum<=4096)tx.put("__antfly_publication_quantum_limit",observation.posting_quantum);
      if(observation&&Number.isFinite(observation.elapsed_ms)&&observation.elapsed_ms>=0&&observation.elapsed_ms<=30000&&Number.isInteger(observation.records)&&observation.records>=1&&observation.records<=256){
        const maximum=Number(this.env.PUBLICATION_RECORDS??16),target=Number(this.env.PUBLICATION_SLICE_MS??2000);
        if(!Number.isSafeInteger(target)||target<250||target>10000)throw Error("InvalidPublicationSlice");
        const current=Math.min(maximum,await tx.get("__antfly_publication_budget")??maximum);
        const next=nextPublicationBudget(current,maximum,observation,{targetComputeMs:target,workingSetBytes:Number(this.env.WORKING_SET_BYTES??20*1024*1024)});
        if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.publication_feedback",current,next,maximum,...observation}));
        if(next!==current)tx.put("__antfly_publication_budget",next);
      }
      return{committed:true};
    },{priority:"maintenance"});
    const head=batch.writes.find(([key])=>key===`antfly/progress/${this.env.ANTFLY_INCARNATION}/HEAD`);
    if(head)this.store.setReadHints(this.env.ANTFLY_INCARNATION,new TextDecoder().decode(head[1].body).trim(),readHints);
    return result;
  }
  abortPublication(lease){return this.transaction(tx=>this.nativePublication(tx,"publication_release",lease),{priority:"maintenance"});}
  async transaction(run,options) {
    await this.ensureAuthority();
    return this.store.transaction(async tx => {
      const migration = await tx.get(MIGRATION);
      if ((migration && migration.phase !== "active") || (this.env.COORDINATOR_MIGRATION_REQUIRED === "true" && !migration) || this.authority.descriptor.mode !== "r2") throw new Error("CoordinatorMigrationRequired");
      return run(tx);
    },options);
  }
  async migrationStatus() {
    await this.ensureAuthority();
    const state = await this.store.transaction(async tx => await tx.get(MIGRATION) ?? {phase:"none"});
    if (state.phase === "none" && this.env.LEGACY_COMMITS && await this.env.LEGACY_COMMITS.getByName(this.env.ANTFLY_INCARNATION).frozenR2Status()) return {phase:"freeze"};
    return state.phase === "active" && this.authority.descriptor.mode !== "r2" ? {...state,phase:"cutover"} : state;
  }
  async migrateFromSqlite() {
    await this.ensureAuthority();
    if (!this.env.LEGACY_COMMITS || this.env.COORDINATOR_MIGRATION_REQUIRED !== "true") throw new Error("CoordinatorMigrationNotConfigured");
    const result = await migrateCoordinator(this.store, this.env.LEGACY_COMMITS.getByName(this.env.ANTFLY_INCARNATION), this.env.ANTFLY_INCARNATION);
    if (result.done) {
      const state = await this.store.transaction(tx => tx.get(MIGRATION));
      this.authority = await activateR2(this.env,this.authority,state.token);
      if (this.authority.descriptor.mode !== "r2") return {has_more:true,phase:"cutover"};
    }
    return result;
  }
  async collectMetadata(jobId,{scheduled=false}={}) {
    const key="__antfly_coord_gc/policy",now=Date.now();
    if(scheduled&&!jobId){const due=await this.maintenanceStore.transaction(async tx=>{
      const active=await tx.get("__antfly_coord_gc/state");if(active)return {has_more:true,job_id:active.id};
      const prior=await tx.get(key),revision=tx.snapshot().revision;
      if(!prior){tx.put(key,{at:now,revision:revision+1});return false;}
      return metadataCollectionDue(prior,revision,now);
    });if(!due)return{done:true,skipped:true};if(typeof due==="object")return due;}
    const result=await collectCoordinator(this.maintenanceStore,this.store.bucket,Date.now,jobId);
    if(result.done&&result.stats)await this.maintenanceStore.transaction(async tx=>{
      const prior=await tx.get(key);if(prior?.job_id!==result.job_id)tx.put(key,{at:Date.now(),revision:tx.snapshot().revision+1,job_id:result.job_id,stats:result.stats});
    });return result;
  }
  publicationBudget(maximum) {
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 256) throw new Error("InvalidPublicationBudget");
    return this.transaction(async tx => Math.min(maximum, await tx.get("__antfly_publication_budget") ?? maximum));
  }
  lowerPublicationBudget(maximum) {
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 256) throw new Error("InvalidPublicationBudget");
    return this.transaction(async tx => {const next = Math.max(1, Math.floor(Math.min(maximum, await tx.get("__antfly_publication_budget") ?? maximum)/2)); tx.put("__antfly_publication_budget", next); if(this.env.PUBLICATION_ADAPTIVE_SLICES==="true")tx.put("__antfly_publication_quantum_limit",Math.max(1,Math.floor((await tx.get("__antfly_publication_quantum_limit")??128)/2))); return next;});
  }
  ensureEmbeddingProfile(profile) {
    if (!profile.model?.startsWith("@cf/") || !Number.isSafeInteger(profile.dimensions) || profile.dimensions < 1 || profile.dimensions > 1536 || typeof (profile.revision ?? "1") !== "string" || !(profile.revision ?? "1").length || (profile.revision ?? "1").length > 256) return false;
    return this.transaction(async tx => {
      const key = "__antfly_embedding_profile", encoded = JSON.stringify(profile), previous = await tx.get(key);
      if (previous != null) {const old = JSON.parse(previous); return old.model === profile.model && old.dimensions === profile.dimensions && (old.revision ?? "1") === (profile.revision ?? "1");}
      tx.put(key, encoded); return true;
    });
  }
  collection(op, args) {return this.journal.rpc(op, args);}
  async objectCall(request, body,readHints) {
    if (!(/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(request.key ?? "") || /\/progress\//.test(request.key ?? ""))) return {status: 3};
    if (body.byteLength > 4096) return {status: 4};
    const result=await this.transaction(tx=>this.objectCallInTransaction(tx,request,body,readHints));
    if(result.status===0&&request.op==="put"&&request.key===`antfly/progress/${this.env.ANTFLY_INCARNATION}/HEAD`)this.store.setReadHints(this.env.ANTFLY_INCARNATION,new TextDecoder().decode(body).trim(),readHints);
    return result;
  }
  async openPublicationPlan(maximum) {
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION,true);
    // Advisory immutable preparation does not acquire a lease or extend a pin.
    // Native publication binds the eventual batch to this generation and all
    // mutations still validate the current native HEAD guard atomically.
    return this.transaction(async tx=>({posting_limit:await tx.get("__antfly_publication_quantum_limit")??4096,budget:Math.min(maximum,await tx.get("__antfly_publication_budget")??maximum),entries:await publicationSnapshot(tx,keys)}),{priority:"maintenance"});
  }
  async openPublication(maximum) {
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION);
    return this.transaction(async tx=>{await captureReadView(tx,await metadataKeys(this.env.ANTFLY_INCARNATION),{immutable:false});return{budget:Math.min(maximum,await tx.get("__antfly_publication_budget")??maximum),entries:await publicationSnapshot(tx,keys)};},{priority:"maintenance"});
  }
  async publicationCall(request,body,expected,readHints) {
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION,this.env.PUBLICATION_CONTINUATIONS==="true");
    const response=await this.transaction(tx=>publicationCall(tx,keys,(...args)=>this.objectCallInTransaction(...args,readHints),request,body,expected),{priority:"maintenance"});
    if(response.result.status===0&&request.op==="put"&&request.key===`antfly/progress/${this.env.ANTFLY_INCARNATION}/HEAD`)this.store.setReadHints(this.env.ANTFLY_INCARNATION,new TextDecoder().decode(body).trim(),readHints);
    return response;
  }
  async objectCallInTransaction(tx,request,body,readHints){
    const result=await metadataObjectCall(tx,request,body,(tx,collection)=>this.journal.valid(tx,collection));
    // Compaction, reindex and legacy publication use individual HEAD CAS calls.
    // Give them the same atomic read protection as prepared publication.
    if(result.status===0&&request.op==="put"&&request.key===`antfly/progress/${this.env.ANTFLY_INCARNATION}/HEAD`){writeReadHints(tx,this.env.ANTFLY_INCARNATION,new TextDecoder().decode(body).trim(),readHints);await captureReadView(tx,await metadataKeys(this.env.ANTFLY_INCARNATION),{immutable:false});}
    return result;
  }
  mutateBatch(input) {
    this.mutationGroups??=new MutationGroups((inputs,deadline,admission)=>this.commitMutations(inputs,deadline,admission));
    return this.mutationGroups.submit(input);
  }
  async commitMutations(inputs,deadline=Date.now()+30000,admission) {
    const trace=new CriticalPath({id:inputs[0]?.__trace_id,scope:"wal"}),started=performance.now(),keys=await metadataKeys(this.env.ANTFLY_INCARNATION),physical={};
    if(admission)trace.stage({name:"write_admission",duration_ms:admission.queue_wait_ms});
    let usedOperations=0,usedBytes=0;
    // Point mutations touch bounded WAL/operation-index paths. Larger groups
    // retain the full native variant and its exclusive 32 MiB reservation.
    const compactWal=inputs.length===1&&inputs[0].mutations?.length===1&&new TextEncoder().encode(JSON.stringify(inputs[0])).byteLength<=16384;
    const walWorkingBytes=compactWal?Math.min(8*1024*1024,Number(this.env.WORKING_SET_BYTES??20*1024*1024)):Number(this.env.WORKING_SET_BYTES??20*1024*1024);
    if(this.env.NATIVE_WRITE_PLAN==="true")return this.commitNativeWritePlan(inputs,deadline,{trace,compactWal,walWorkingBytes});
    this.optimisticWal??=new OptimisticWal();
    for(let attempt=0;attempt<3;attempt++){
      // The shared durable generation pin protects the manifest read during
      // preparation. New publication can only make admission more conservative.
      if(attempt===0&&this.env.OPTIMISTIC_WAL==="true"){
        const local=this.store.speculativeEntries([keys.head,keys.wal,keys.floor]);
        if(local)try{const view=await captureReadView({getMany:async names=>names.map(key=>structuredClone(local.get(key))),get:async key=>structuredClone(local.get(key)),put:()=>{throw Error("PreparationNeedsPin");}},keys,{immutable:false});this.optimisticWal.remember(view,keys.wal);}catch(error){if(error?.message!=="PreparationNeedsPin")throw error;this.optimisticWal.clear();}
      }
      const retained=attempt===0&&this.env.OPTIMISTIC_WAL==="true"&&this.store.acquired&&!this.store.ioDrain?this.optimisticWal.prepare():null;
      const view=retained??await trace.span("prepare_snapshot",()=>this.openReadView({trace_id:trace.id,immutable:false})),prepared=new PreparedWal(keys.wal,view.entries);
      const dependencies=retained?this.optimisticWal.dependencies(view,keys.wal,encode):[];
      const transport=this.store.dataCache.wrap(withCommitCoordinator(edgeCacheForEnv(this.env,{background:promise=>this.ctx.waitUntil(promise),budgetKey:()=>physical,onMetrics:sample=>{for(const[key,value]of Object.entries(sample))physical[key]=(physical[key]??0)+value;}})(instrumentTransport(createR2Transport({database:this.env.DATABASE},{maxPutBytes:4*1024*1024}),physical)),prepared),this.env.ANTFLY_INCARNATION,{preserveProtected:this.env.FOREGROUND_CACHE_POLICY==="true"});

      let result;try{result=await trace.span("wal_prepare",()=>buildKernel.request({op:"mutate",...(this.env.NATIVE_READ_STATE==="true"?{native_read_state:true}:{}),...(this.env.WAL_DOCUMENT_INDEX==="true"?{wal_document_index:true}:{}),wal_inline_tail:this.env.WAL_INLINE_TAIL==="true",namespace:this.env.ANTFLY_INCARNATION,bucket:"database",prefix:"antfly",working_set_bytes:walWorkingBytes,mutation_groups:inputs.map(input=>({mutations:input.mutations,operation_id:input.operation_id,timestamp_ns:input.timestamp_ns??"0"}))},{transport,kernelVariant:compactWal?"compact":"full",memoryReservationBytes:(compactWal?16:32)*1024*1024,fairKey:this.env.ANTFLY_INCARNATION,priority:"interactive",deadlineMs:deadline,maxOperations:1024-usedOperations,maxTransferBytes:32*1024*1024-usedBytes,onStage:sample=>trace.stage(sample),onMetrics:sample=>{usedOperations+=sample.operations;usedBytes+=sample.transferred_bytes;if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.wal_prepare",trace_id:trace.id,...sample,physical}));}}));}catch(error){if(retained&&/^(FileNotFound|ObjectNotFound)$/.test(error?.message)){this.optimisticWal.clear();continue;}throw error;}
      // Replays still take fresh authority admission; a cached receipt never
      // substitutes for current namespace/fencing validation.
      if(retained&&!prepared.head){this.optimisticWal.clear();continue;}
      let maximum=0n;for(const item of result.results)maximum=BigInt("0x"+item.visibility.slice(-16))>maximum?BigInt("0x"+item.visibility.slice(-16)):maximum;
      let committed;
      if(prepared.head&&this.env.NATIVE_WAL_COMMIT!=="false"){
        if(this.authority.descriptor.mode!=="r2")throw new Error("CoordinatorMigrationRequired");
        if(Date.now()>=deadline)throw new Error("DeadlineExceeded");
        const native=await trace.span("durable_commit",()=>this.store.commitWal({...(retained?{dependencies}:{}),wal_key:keys.wal,expected_etag:prepared.original?.etag??null,value:encode(prepared.head),publication_key:"__antfly_maintenance/publication",watermark:String(maximum),now_ms:Date.now(),id:crypto.randomUUID(),migration_required:this.env.COORDINATOR_MIGRATION_REQUIRED==="true"},{deadlineMs:deadline,traceId:trace.id}));
        committed=native.applied?native.publication:null;
      }else committed=await trace.span("durable_commit",()=>this.transaction(async tx=>{
        if(Date.now()>=deadline)throw new Error("DeadlineExceeded");
        for(const dependency of dependencies)if(((await tx.get(dependency.key))?encode(await tx.get(dependency.key)):null)!==dependency.value)return null;
        if(!await prepared.commit(tx,(...args)=>this.objectCallInTransaction(...args)))return null;
        return await this.publication.reserve(tx,String(maximum));
      }));
      if(committed){
        if(this.env.OPTIMISTIC_WAL==="true")this.optimisticWal.remember(view,keys.wal,prepared.head);
        trace.publicationId=committed.id;
        if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.wal_group",trace_ids:inputs.map(input=>input.__trace_id).filter(Boolean),callers:inputs.length,records:inputs.reduce((n,input)=>n+input.mutations.length,0),native_ms:performance.now()-started,prepare_attempts:attempt+1,physical_r2:physical}));
        trace.emit(this.env.LOG_METRICS==="true");
        return result.results.map((item,i)=>({...item,__publication:i===0?committed:{...committed,enqueue:false}}));
      }
      this.optimisticWal.clear();
    }
    throw new Error("WalCommitContended");
  }
  async commitNativeWritePlan(inputs,deadline,{trace,compactWal,walWorkingBytes}) {
    const started=performance.now();
    try{
    await trace.span("writer_authority",()=>this.ensureAuthority());
    if(this.authority.descriptor.mode!=="r2")throw Error("CoordinatorMigrationRequired");
    const ioBudget={operations:0,bytes:0},physical={};
    const refresh=()=>trace.span("write_refresh",()=>this.transaction(async()=>{},{deadlineMs:deadline,traceId:trace.id,ioBudget}));
    if(!this.store.state||this.store.recovering)await refresh();
    const transport=this.store.dataCache.wrap(edgeCacheForEnv(this.env,{background:promise=>this.ctx.waitUntil(promise),budgetKey:()=>physical,onMetrics:sample=>{for(const[key,value]of Object.entries(sample))physical[key]=(physical[key]??0)+value;}})(instrumentTransport(createR2Transport({database:this.env.DATABASE},{maxPutBytes:4*1024*1024}),physical)),this.env.ANTFLY_INCARNATION,{preserveProtected:this.env.FOREGROUND_CACHE_POLICY==="true"});
    for(let attempt=0;attempt<3;attempt++){
      if(Date.now()>=deadline)throw Error("DeadlineExceeded");
      if(ioBudget.operations>=1024||ioBudget.bytes>=32*1024*1024)throw Error("CoordinatorIoBudgetExceeded");
      let prepared;
      try{prepared=await trace.span("native_wal_plan",()=>buildKernel.request({op:"mutate",...(this.env.NATIVE_READ_STATE==="true"?{native_read_state:true}:{}),...(this.env.WAL_DOCUMENT_INDEX==="true"?{wal_document_index:true}:{}),namespace:this.env.ANTFLY_INCARNATION,bucket:"database",prefix:"antfly",wal_inline_tail:this.env.WAL_INLINE_TAIL==="true",working_set_bytes:walWorkingBytes,native_write_plan:{state:structuredClone(this.store.state),metadata_namespace:this.store.namespace,reservation_id:crypto.randomUUID()},mutation_groups:inputs.map(input=>({mutations:input.mutations,operation_id:input.operation_id,timestamp_ns:input.timestamp_ns??"0"}))},{transport,kernelVariant:compactWal?"compact":"full",memoryReservationBytes:(compactWal?16:32)*1024*1024,fairKey:this.env.ANTFLY_INCARNATION,priority:"interactive",deadlineMs:deadline,maxOperations:1024-ioBudget.operations,maxTransferBytes:32*1024*1024-ioBudget.bytes,onStage:sample=>trace.stage(sample),onMetrics:sample=>{ioBudget.operations+=sample.operations;ioBudget.bytes+=sample.transferred_bytes;if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.wal_prepare",trace_id:trace.id,native_write_plan:true,...sample,physical}));}}));}
      catch(error){if(error?.message!=="FileNotFound")throw error;await refresh();continue;}
      if(!prepared.__write_plan||!Array.isArray(prepared.results)||prepared.results.length!==inputs.length)throw Error("NativeWritePlanRequired");
      let committed;
      try{committed=await trace.span("native_wal_finalize",()=>this.store.commitWal({...prepared.__write_plan,migration_required:this.env.COORDINATOR_MIGRATION_REQUIRED==="true"},{deadlineMs:deadline,traceId:trace.id,nativePlan:true,ioBudget}));}
      catch(error){if(!["PreconditionFailed","ImmutableLogRotationRequired"].includes(error?.message))throw error;await refresh();continue;}
      if(!committed.applied){await refresh();continue;}
      trace.publicationId=committed.publication.id;
      if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.wal_group",trace_id:trace.id,trace_ids:inputs.map(input=>input.__trace_id).filter(Boolean),callers:inputs.length,records:inputs.reduce((n,input)=>n+input.mutations.length,0),native_ms:performance.now()-started,native_write_plan:true,prepare_attempts:attempt+1,operations:ioBudget.operations,transferred_bytes:ioBudget.bytes,physical_r2:physical}));
      return prepared.results.map((item,i)=>({...item,__publication:i===0?committed.publication:{...committed.publication,enqueue:false}}));
    }
    throw Error("WalCommitContended");
    }finally{trace.emit(this.env.LOG_METRICS==="true");}
  }
  pruneEmbeddingCheckpointMetadata(namespace, today) {
    if (namespace !== this.env.ANTFLY_INCARNATION || !Number.isSafeInteger(today) || today < 0) throw new Error("InvalidEmbeddingCheckpointSweep");
    return this.transaction(async tx => {
      const stateKey = `__antfly_embedding_gc/${namespace}`, state = await tx.get(stateKey) ?? {};
      const prefix = `antfly/progress/${namespace}/EMBEDDING_CACHE/`;
      const entries = [...await tx.list({prefix, startAfter: state.metadata_cursor, limit: 128})];
      for (const [key] of entries) {const day = key.slice(prefix.length).split("/")[0]; if (/^[a-f0-9]{8}$/.test(day) && parseInt(day,16)+1 < today) tx.delete(key);}
      state.metadata_cursor = entries.length === 128 ? entries.at(-1)[0] : undefined; tx.put(stateKey, state); return state.payload_cursor ?? null;
    });
  }
  saveEmbeddingCheckpointCursor(namespace, expected, next) {
    return this.transaction(async tx => {const key = `__antfly_embedding_gc/${namespace}`, state = await tx.get(key) ?? {}; if ((state.payload_cursor ?? null) !== expected) return false; state.payload_cursor = next; tx.put(key,state); return true;});
  }
}
