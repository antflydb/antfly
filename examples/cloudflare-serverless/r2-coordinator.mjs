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

import {FreshReadAdmissions} from "./fresh-read-admissions.mjs";
import {metadataObjectCall} from "./metadata-object-call.mjs";
import {applyPublication} from "./prepared-publication.mjs";
import {CriticalPath} from "./trace.mjs";
import {encode} from "./r2-coordinator-store.mjs";
import {publicationKeys,publicationCall,publicationSnapshot} from "./publication-view.mjs";
import {metadataKernel,buildKernel,sharedCache,boundRows,kernelMetrics} from "./kernels.mjs";
import {MaintenanceTickets} from "./maintenance-tickets.mjs";
import {PreparedWal} from "./prepared-wal.mjs";
import {metadataKeys} from "./read-view.mjs";
import {MutationGroups} from "./mutation-groups.mjs";
import {metadataCollectionDue} from "./maintenance-policy.mjs";
import {instrumentTransport} from "./performance.mjs";
import {PublicationScheduler} from "./publication-scheduler.mjs";
import {tenantEnvs} from "./tenants.mjs";
import {captureReadView} from "./read-view.mjs";
import {createR2Transport} from "../../zig/lib/objectstore/wasm/r2.mjs";
import {withCommitCoordinator} from "../../zig/lib/objectstore/wasm/coordinated.mjs";
import {openAuthority, activateR2} from "./coordinator-authority.mjs";
import {DurableObject} from "cloudflare:workers";
import module from "./dist/antfly-serverless.wasm";
import {R2CoordinatorStore} from "./r2-coordinator-store.mjs";
import {migrateCoordinator, MIGRATION} from "./r2-coordinator-migration.mjs";
import {collectCoordinator} from "./r2-coordinator-gc.mjs";
import {R2CollectionJournal} from "./r2-collection-journal.mjs";
export class R2CommitCoordinator extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    if (!env.ANTFLY_INCARNATION) throw new Error("CoordinatorNamespaceRequired");
    if(env.ANTFLY_TENANTS){const binding=env.COMMITS;env=tenantEnvs(env).find(candidate=>binding.idFromName(candidate.ANTFLY_INCARNATION).equals(ctx.id));if(!env)throw new Error("UnknownTenant");}
    this.env = env;
    this.store = new R2CoordinatorStore(module, env.DATABASE, env.COORDINATOR_STORAGE_ID ?? env.ANTFLY_INCARNATION,{kernel:metadataKernel,cache:sharedCache,boundRows,background:promise=>ctx.waitUntil(promise),onMetrics:sample=>{if(env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.coordinator",...sample,...kernelMetrics()}));}});
    this.serviceStore = Object.create(this.store);
    this.serviceStore.transaction = run => this.transaction(run);
    this.maintenanceStore=Object.create(this.store);this.maintenanceStore.transaction=run=>this.transaction(run,{priority:"maintenance"});
    this.publication=new PublicationScheduler(this.maintenanceStore,env.ANTFLY_INCARNATION);
    this.maintenance=new MaintenanceTickets(this.maintenanceStore);
    this.journal = new R2CollectionJournal(this.maintenanceStore);
    ctx.blockConcurrencyWhile(async () => {this.authority = await openAuthority(env, "r2");});
  }
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
    this.readAdmissions??=new FreshReadAdmissions({delayMs:Number(this.env.READ_VIEW_BATCH_MS??2)});
    return this.readAdmissions.run(options.version??"latest",traceIds=>{
      const capture=async tx=>{
        const migration=await tx.get(MIGRATION);
        if(migration&&migration.phase!=="active"||this.env.COORDINATOR_MIGRATION_REQUIRED==="true"&&!migration||this.authority.descriptor.mode!=="r2")throw Error("CoordinatorMigrationRequired");
        return captureReadView(tx,await metadataKeys(this.env.ANTFLY_INCARNATION),options);
      };
      return(this.env.READ_ONLY_SNAPSHOTS==="false"?Promise.reject(Error("ReadSnapshotNeedsAdmission")):this.store.readOnly(capture,{traceId:options.trace_id,traceIds})).catch(error=>{
        if(!/ReadSnapshotNeeds(?:Admission|Pin)/.test(String(error)))throw error;
        return this.transaction(capture,{traceId:options.trace_id});
      });
    },options.trace_id);
  }
  async nativePublication(tx,op,lease,owner=lease?.owner){
    const keys=await metadataKeys(this.env.ANTFLY_INCARNATION),names=[keys.head,keys.progress+"HEAD_FENCE"],values=await tx.getMany(names),records=new Map(names.map((key,i)=>[key,values[i]]));
    // No recursive metadata-kernel acquisition: the tiny native lease action
    // operates only on these transaction-owned copies and writes the overlay.
    const local={get:async key=>{if(!records.has(key))throw Error("PublicationLeaseKeyUnsupported");return structuredClone(records.get(key));},put:(key,value)=>{if(!records.has(key))throw Error("PublicationLeaseKeyUnsupported");records.set(key,structuredClone(value));tx.put(key,value);}};
    const transport=withCommitCoordinator(async request=>{if(request.op!=="bucket_exists")throw Error("PublicationLeaseIoUnsupported");return{json:{exists:true}};},{objectCall:(request,body)=>this.objectCallInTransaction(local,request,body)});
    return metadataKernel.request({op,publication_owner:owner,publication_lease:lease??null,bucket:"database",prefix:"antfly",namespace:this.env.ANTFLY_INCARNATION},{transport,fairKey:this.store.namespace,priority:"maintenance",deadlineMs:Date.now()+5000});
  }
  async beginPublication(maximum){
    if(!Number.isSafeInteger(maximum)||maximum<1||maximum>256)throw Error("InvalidPublicationBudget");
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION);
    return this.transaction(async tx=>{
      const lease=await this.nativePublication(tx,"publication_acquire",null,crypto.randomUUID());
      await captureReadView(tx,await metadataKeys(this.env.ANTFLY_INCARNATION));
      return{format:2,lease,budget:Math.min(maximum,await tx.get("__antfly_publication_budget")??maximum),entries:await publicationSnapshot(tx,keys)};
    },{priority:"maintenance"});
  }
  async finalizePublication(batch,observation){
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION);
    return this.transaction(async tx=>{
      await this.nativePublication(tx,"publication_validate",batch?.lease);
      const entries=await publicationSnapshot(tx,keys);
      await applyPublication(tx,entries.map(([key])=>key),batch);
      if(!(await this.nativePublication(tx,"publication_release",batch.lease)).released)throw Error("WorkLeaseLost");
      if(observation&&Number.isFinite(observation.elapsed_ms)&&observation.elapsed_ms>=0&&observation.elapsed_ms<=30000&&Number.isInteger(observation.records)&&observation.records>=1&&observation.records<=256){
        const maximum=Number(this.env.PUBLICATION_RECORDS??16),target=Number(this.env.PUBLICATION_SLICE_MS??2000);
        if(!Number.isSafeInteger(target)||target<250||target>10000)throw Error("InvalidPublicationSlice");
        const current=Math.min(maximum,await tx.get("__antfly_publication_budget")??maximum),elapsed=observation.elapsed_ms;
        let next=current;
        if(elapsed>target)next=Math.max(1,Math.min(current,Math.floor(observation.records*target/elapsed*.8)));
        else if(elapsed<target*.5&&observation.records>=current)next=Math.min(maximum,current+Math.max(1,Math.floor(current/4)));
        if(next!==current)tx.put("__antfly_publication_budget",next);
      }
      return{committed:true};
    },{priority:"maintenance"});
  }
  abortPublication(lease){return this.transaction(tx=>this.nativePublication(tx,"publication_release",lease),{priority:"maintenance"});}
  transaction(run,options) {
    return this.store.transaction(async tx => {
      const migration = await tx.get(MIGRATION);
      if ((migration && migration.phase !== "active") || (this.env.COORDINATOR_MIGRATION_REQUIRED === "true" && !migration) || this.authority.descriptor.mode !== "r2") throw new Error("CoordinatorMigrationRequired");
      return run(tx);
    },options);
  }
  async migrationStatus() {
    const state = await this.store.transaction(async tx => await tx.get(MIGRATION) ?? {phase:"none"});
    if (state.phase === "none" && this.env.LEGACY_COMMITS && await this.env.LEGACY_COMMITS.getByName(this.env.ANTFLY_INCARNATION).frozenR2Status()) return {phase:"freeze"};
    return state.phase === "active" && this.authority.descriptor.mode !== "r2" ? {...state,phase:"cutover"} : state;
  }
  async migrateFromSqlite() {
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
    return this.transaction(async tx => {const next = Math.max(1, Math.floor(Math.min(maximum, await tx.get("__antfly_publication_budget") ?? maximum)/2)); tx.put("__antfly_publication_budget", next); return next;});
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
  objectCall(request, body) {
    if (!(/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(request.key ?? "") || /\/progress\//.test(request.key ?? ""))) return {status: 3};
    if (body.byteLength > 4096) return {status: 4};
    return this.transaction(tx=>this.objectCallInTransaction(tx,request,body));
  }
  async openPublication(maximum) {
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION);
    return this.transaction(async tx=>{await captureReadView(tx,await metadataKeys(this.env.ANTFLY_INCARNATION));return{budget:Math.min(maximum,await tx.get("__antfly_publication_budget")??maximum),entries:await publicationSnapshot(tx,keys)};},{priority:"maintenance"});
  }
  async publicationCall(request,body,expected) {
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION);
    return this.transaction(tx=>publicationCall(tx,keys,(...args)=>this.objectCallInTransaction(...args),request,body,expected),{priority:"maintenance"});
  }
  objectCallInTransaction(tx,request,body){return metadataObjectCall(tx,request,body,(tx,collection)=>this.journal.valid(tx,collection));}
  mutateBatch(input) {
    this.mutationGroups??=new MutationGroups((inputs,deadline)=>this.commitMutations(inputs,deadline));
    return this.mutationGroups.submit(input);
  }
  async commitMutations(inputs,deadline=Date.now()+30000) {
    const trace=new CriticalPath({id:inputs[0]?.__trace_id,scope:"wal"}),started=performance.now(),keys=await metadataKeys(this.env.ANTFLY_INCARNATION),physical={};
    let usedOperations=0,usedBytes=0;
    for(let attempt=0;attempt<3;attempt++){
      // The shared durable generation pin protects the manifest read during
      // preparation. New publication can only make admission more conservative.
      const view=await trace.span("prepare_snapshot",()=>this.openReadView({trace_id:trace.id})),prepared=new PreparedWal(keys.wal,view.entries);
      const transport=this.store.cache.wrap(withCommitCoordinator(instrumentTransport(createR2Transport({database:this.env.DATABASE},{maxPutBytes:4*1024*1024}),physical),prepared),this.env.ANTFLY_INCARNATION);

      const result=await trace.span("wal_prepare",()=>buildKernel.request({op:"mutate",namespace:this.env.ANTFLY_INCARNATION,bucket:"database",prefix:"antfly",working_set_bytes:Number(this.env.WORKING_SET_BYTES??20*1024*1024),mutation_groups:inputs.map(input=>({mutations:input.mutations,operation_id:input.operation_id,timestamp_ns:input.timestamp_ns??"0"}))},{transport,fairKey:this.env.ANTFLY_INCARNATION,priority:"interactive",deadlineMs:deadline,maxOperations:1024-usedOperations,maxTransferBytes:32*1024*1024-usedBytes,onStage:sample=>trace.stage(sample),onMetrics:sample=>{usedOperations+=sample.operations;usedBytes+=sample.transferred_bytes;if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.wal_prepare",trace_id:trace.id,...sample,physical}));}}));
      let maximum=0n;for(const item of result.results)maximum=BigInt("0x"+item.visibility.slice(-16))>maximum?BigInt("0x"+item.visibility.slice(-16)):maximum;
      let committed;
      if(prepared.head&&this.env.NATIVE_WAL_COMMIT!=="false"){
        if(this.authority.descriptor.mode!=="r2")throw new Error("CoordinatorMigrationRequired");
        if(Date.now()>=deadline)throw new Error("DeadlineExceeded");
        const native=await trace.span("durable_commit",()=>this.store.commitWal({wal_key:keys.wal,expected_etag:prepared.original?.etag??null,value:encode(prepared.head),publication_key:"__antfly_maintenance/publication",watermark:String(maximum),now_ms:Date.now(),id:crypto.randomUUID(),migration_required:this.env.COORDINATOR_MIGRATION_REQUIRED==="true"},{deadlineMs:deadline,traceId:trace.id}));
        committed=native.applied?native.publication:null;
      }else committed=await trace.span("durable_commit",()=>this.transaction(async tx=>{
        if(Date.now()>=deadline)throw new Error("DeadlineExceeded");
        if(!await prepared.commit(tx,(...args)=>this.objectCallInTransaction(...args)))return null;
        return await this.publication.reserve(tx,String(maximum));
      }));
      if(committed){
        trace.publicationId=committed.id;
        if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.wal_group",trace_ids:inputs.map(input=>input.__trace_id).filter(Boolean),callers:inputs.length,records:inputs.reduce((n,input)=>n+input.mutations.length,0),native_ms:performance.now()-started,prepare_attempts:attempt+1,physical_r2:physical}));
        trace.emit(this.env.LOG_METRICS==="true");
        return result.results.map((item,i)=>({...item,__publication:i===0?committed:{...committed,enqueue:false}}));
      }
    }
    throw new Error("WalCommitContended");
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
