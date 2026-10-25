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
import {openReadView} from "./read-view.mjs";
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
  openReadView(options) {return openReadView(this,this.env.ANTFLY_INCARNATION,options);}
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
    return this.transaction(async tx=>({budget:Math.min(maximum,await tx.get("__antfly_publication_budget")??maximum),entries:await publicationSnapshot(tx,keys)}),{priority:"maintenance"});
  }
  async publicationCall(request,body,expected) {
    const keys=await publicationKeys(this.env.ANTFLY_INCARNATION);
    return this.transaction(tx=>publicationCall(tx,keys,(...args)=>this.objectCallInTransaction(...args),request,body,expected),{priority:"maintenance"});
  }
  async objectCallInTransaction(tx,request,body) {
      if(!(/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(request.key??"")||/\/progress\//.test(request.key??"")))return{status:3};
      if(body.byteLength>4096)return{status:4};
      if (request.collection != null) {try {await this.journal.valid(tx, request.collection);} catch {return {status: 5};}}
      const previous = await tx.get(request.key);
      if (request.op === "put") {
        if (request.guard_key != null || request.guard_etag != null) {
          const parent = request.key.slice(0, request.key.lastIndexOf("/"));
          if (!request.key.endsWith("/REINDEX") || request.guard_key !== `${parent}/HEAD` || typeof request.guard_etag !== "string") return {status: 3};
          if ((await tx.get(request.guard_key))?.etag !== request.guard_etag) return {status: 2};
        }
        if ((request.if_none_match && previous) || (request.if_match_etag != null && previous?.etag !== request.if_match_etag)) return {status: 2};
        const etag = crypto.randomUUID(); tx.put(request.key, {etag, body, contentType: request.content_type ?? null});
        return {status: 0, json: {etag}};
      }
      if (request.op === "get" || request.op === "head") {
        if (!previous) return {status: 1};
        if (request.if_match_etag != null && request.if_match_etag !== previous.etag) return {status: 2};
        return {status: 0, json: {bucket: request.bucket, key: request.key, etag: previous.etag, content_length: previous.body.byteLength, content_type: previous.contentType}, body: request.op === "get" ? previous.body : undefined};
      }
      return {status: 3};
  }
  mutateBatch(input) {
    this.mutationGroups??=new MutationGroups((inputs,deadline)=>this.commitMutations(inputs,deadline));
    return this.mutationGroups.submit(input);
  }
  async commitMutations(inputs,deadline=Date.now()+30000) {
    const started=performance.now(),keys=await metadataKeys(this.env.ANTFLY_INCARNATION),physical={};
    let usedOperations=0,usedBytes=0;
    for(let attempt=0;attempt<3;attempt++){
      // The shared durable generation pin protects the manifest read during
      // preparation. New publication can only make admission more conservative.
      const view=await this.openReadView({}),prepared=new PreparedWal(keys.wal,view.entries);
      const transport=this.store.cache.wrap(withCommitCoordinator(instrumentTransport(createR2Transport({database:this.env.DATABASE},{maxPutBytes:4*1024*1024}),physical),prepared),this.env.ANTFLY_INCARNATION);

      const result=await buildKernel.request({op:"mutate",namespace:this.env.ANTFLY_INCARNATION,bucket:"database",prefix:"antfly",working_set_bytes:Number(this.env.WORKING_SET_BYTES??20*1024*1024),mutation_groups:inputs.map(input=>({mutations:input.mutations,operation_id:input.operation_id,timestamp_ns:input.timestamp_ns??"0"}))},{transport,deadlineMs:deadline,maxOperations:1024-usedOperations,maxTransferBytes:32*1024*1024-usedBytes,onMetrics:sample=>{usedOperations+=sample.operations;usedBytes+=sample.transferred_bytes;if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.wal_prepare",...sample,physical}));}});
      let maximum=0n;for(const item of result.results)maximum=BigInt("0x"+item.visibility.slice(-16))>maximum?BigInt("0x"+item.visibility.slice(-16)):maximum;
      const committed=await this.transaction(async tx=>{
        if(Date.now()>=deadline)throw new Error("DeadlineExceeded");
        if(!await prepared.commit(tx,(...args)=>this.objectCallInTransaction(...args)))return null;
        return await this.publication.reserve(tx,String(maximum));
      });
      if(committed){
        if(this.env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.wal_group",callers:inputs.length,records:inputs.reduce((n,input)=>n+input.mutations.length,0),native_ms:performance.now()-started,prepare_attempts:attempt+1,physical_r2:physical}));
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
