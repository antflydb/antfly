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

// Cloudflare-independent adapter for the shared native ordered metadata store.
import {readObservationResponse} from "../../zig/lib/objectstore/wasm/observation-plan.mjs";
import {normalizeReadHints} from "./publication-read-hints.mjs";
import {RequestPool} from "./request-pool.mjs";
import {recoveryReadAhead,warmRecoveryReadAhead,proofPrefetchBytes} from "./recovery-read-ahead.mjs";
import {CriticalPath} from "./trace.mjs";
import {IdleCheckpoint} from "./idle-checkpoint.mjs";
import {instrumentTransport} from "./performance.mjs";
import {maintenanceMutationBudget,maintenanceSliceMs} from "./maintenance-policy.mjs";
import {instantiateHost} from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import {createR2Transport} from "../../zig/lib/objectstore/wasm/r2.mjs";
import {createImmutableReadCache} from "../../zig/lib/objectstore/wasm/cache.mjs";
const utf8 = new TextEncoder();
const proofDeadline=Symbol("proof deadline");
function beforeProofDeadline(promise,deadlineMs,monotonicDeadline,clock){
 const expired=()=>{const error=Error("DeadlineExceeded");error[proofDeadline]=true;return error;};
 const remaining=()=>Math.min(deadlineMs-clock(),monotonicDeadline-performance.now());
 if(remaining()<=0){Promise.resolve(promise).catch(()=>{});return Promise.reject(expired());}
 return new Promise((resolve,reject)=>{
  const timer=setTimeout(()=>reject(expired()),Math.max(1,Math.ceil(remaining())));
  Promise.resolve(promise).then(value=>{clearTimeout(timer);if(remaining()<=0)reject(expired());else resolve(value);},error=>{clearTimeout(timer);reject(error);});
 });
}
const hex=bytes=>Array.from(typeof bytes==="string"?utf8.encode(bytes):bytes,n=>n.toString(16).padStart(2,"0")).join("");
const compare = (l, r) => {const a = utf8.encode(l), b = utf8.encode(r); for (let i=0; i<Math.min(a.length,b.length); i++) if (a[i]!==b[i]) return a[i]-b[i]; return a.length-b.length;};
function rootMetrics(state){try{const root=JSON.parse(state?.root??'null');return{log_slot:state?.next??0,log_revision:state?.revision??0,inline_root_format:root?.format??0,inline_root_rows:root?.format===3?root.read_rows.exact.length:0,inline_root_has_pin:root?.format===3&&root.read_rows.selected!=null,inline_root_bytes:utf8.encode(state?.root??'').byteLength};}catch{return{inline_root_format:0};}}
export const encode = value => JSON.stringify(value, (_, v) => v instanceof Uint8Array || v instanceof ArrayBuffer ? {__antfly_bytes: Array.from(new Uint8Array(v.buffer ?? v, v.byteOffset ?? 0, v.byteLength))} : v);
const decode = value => JSON.parse(value, (_, v) => v && Object.keys(v).length === 1 && Array.isArray(v.__antfly_bytes) ? Uint8Array.from(v.__antfly_bytes) : v);
export class R2CoordinatorStore {
  constructor(module, bucket, namespace, {prefix = "antfly", clock = Date.now, onMetrics, owner, inlineReadSet,transport,kernel,readKernel,cache,boundRows,background,checkpointCommits=64,recoveryWindow=8,remoteCache=transport=>transport,proofBudget,rowBudgetBytes=512*1024,exclusiveOwnerId,readBatch=true,readContinuations=false} = {}) {
    if(!Number.isSafeInteger(rowBudgetBytes)||rowBudgetBytes<32768||rowBudgetBytes>512*1024)throw Error("InvalidCoordinatorRowBudget");this.rowBudgetBytes=rowBudgetBytes;
    this.exclusiveOwnerId=exclusiveOwnerId;this.ownerServing=false;
    this.rootBackend="journal";this.rootCursor=null;this.journalProof=null;this.readBatch=readBatch;this.readContinuations=readContinuations;this.observationPlanSupported=false;
    this.module = module; this.bucket = bucket; this.namespace = namespace; this.prefix = prefix; this.clock = clock; this.onMetrics = onMetrics; this.background = background;
    this.kernel=kernel;this.boundRows=boundRows;this.checkpointCommits=checkpointCommits;this.recoveryWindow=recoveryWindow;
    const copyOwner=value=>{if(!Array.isArray(value)&&!(value instanceof Uint8Array)||value.length!==16||!Array.from(value).every(v=>Number.isInteger(v)&&v>=0&&v<=255))throw Error("InvalidCoordinatorOwner");return Array.from(value);};
    const randomOwner=()=>Array.from(crypto.getRandomValues(new Uint8Array(16)));
    this.owner=typeof owner?.then==="function"||owner==null?randomOwner():copyOwner(owner);
    if(typeof owner?.then==="function"){this.ownerReady=Promise.resolve(owner).then(value=>{if(value!=null)this.owner=copyOwner(value);});this.ownerReady.catch(()=>{});}
    if(inlineReadSet!=null){this.inlineReadSetReady=Promise.resolve(inlineReadSet).then(value=>{this.inlineReadSet=value;});this.inlineReadSetReady.catch(()=>{});}
    const scopedCache=cache?.forBucket?.(bucket)??cache;
    this.cache = scopedCache?.metadata??scopedCache??createImmutableReadCache({includeCommitLog:true,maxEntries:1024,writeThrough:true});
    this.dataCache=scopedCache??this.cache;
    const raw=transport??createR2Transport({database:bucket},{maxPutBytes:4*1024*1024});
    this.rawTransport=raw;this.remoteCache=remoteCache;this.snapshotKernel=readKernel??kernel??new RequestPool(()=>instantiateHost(module,null,{clock}),1,{clock});this.snapshotReaders=0;this.proofBudget=proofBudget;
    this.transport = this.cache.wrap(this.remoteCache(async(request,body)=>instrumentTransport(raw,this.physical??={})(request,body),{budgetKey:()=>this.physical??={},onMetrics:sample=>{for(const[key,value]of Object.entries(sample)){this.physical??={};this.physical[key]=(this.physical[key]??0)+value;}}}), `coordinator/${namespace}`);
    this.pending = []; this.running = false; this.state = null;
    this.rows=new Map();this.rowBytes=0;this.hintBytes=0;this.cursorHintBytes=0;this.rootHint=null;this.readHints=null;this.rowRevision=null;this.acquired=false;
    if(background)this.idleCheckpoint=new IdleCheckpoint({clock:this.clock,revision:()=>this.state?.revision??0,isIdle:()=>this.acquired&&!this.recovering,background,onError:error=>{try{this.onMetrics?.({event:"antfly.checkpoint_deferred",error:String(error)});}catch{}},checkpoint:options=>this.checkpointHint(options)});
  }
  configureAuthority(descriptor){
    if(!["r2","r2-conditional","r2-bounded","sqlite"].includes(descriptor?.mode))throw Error("CoordinatorAuthorityMismatch");const backend=descriptor.mode==="r2-conditional"?"conditional":descriptor.mode==="r2-bounded"?"bounded_journal":"journal";
    if(this.rootBackend!==backend){if(this.state||this.observedState||this.acquired||this.running||this.pending.length)throw Error("CoordinatorMigrationRequired");this.rootBackend=backend;this.rootCursor=null;this.journalProof=null;this.rootProofScope=Array.from(crypto.getRandomValues(new Uint8Array(16)));this.rootProofKey=Array.from(crypto.getRandomValues(new Uint8Array(32)));if(backend==="conditional")this.idleCheckpoint=null;}
    if(descriptor.format===2){
      this.ownerMvcc=["serialized-owner-v2","serialized-owner-v3"].includes(descriptor.serving?.protocol);
      if(descriptor.serving?.protocol==="serialized-owner-v3"&&this.exclusiveOwnerId!=null&&!this.durableBootstrap)throw Error("DurableBootstrapStorageRequired");
      if(descriptor.mode!=="r2-bounded"||!["serialized-owner-v1","serialized-owner-v2","serialized-owner-v3"].includes(descriptor.serving?.protocol)||typeof descriptor.serving.owner_id!=="string"||this.exclusiveOwnerId!=null&&descriptor.serving.owner_id!==this.exclusiveOwnerId)throw Error("ResidentServingOwnerMismatch");
      this.requiresExclusiveOwner=true;this.ownerServing=this.exclusiveOwnerId===descriptor.serving.owner_id;
    }
    this.readLeasePolicy=descriptor.read_lease_policy;
  }
  configureDurableCheckpoint(){
    if(!this.durableBootstrap||!this.background||this.durableCheckpoint)return;
    this.durableCheckpoint=new IdleCheckpoint({clock:this.clock,revision:()=>this.durableBootstrap.generation??0,delay:1000,retryDelay:1000,maxAttempts:4,shouldRetry:()=>false,
      isIdle:()=>this.acquired&&!this.running&&!this.pending.length&&!this.recovering&&!this.ioDrain&&!this.freezeRequested&&performance.now()-(this.durableBootstrap.lastActivity??0)>=1000,
      checkpoint:options=>this.flushDurableCheckpoint(options),background:this.background,onError:error=>{try{this.onMetrics?.({event:"antfly.durable_checkpoint_error",error_message:String(error.message??error).slice(0,2048)});}catch{}}});
  }
  flushDurableCheckpoint({deadlineMs=this.clock()+5000}={}){
    if(!this.durableBootstrap)return Promise.reject(Error("DurableBootstrapStorageRequired"));
    return this.enqueue({kind:"durable_checkpoint",priority:"maintenance",deadlineMs,run:()=>{}});
  }
  async servingCodec(action,extra={},{tryOnly=false}={}){
    const run=host=>{
      if(host.servingCapsuleABI!==1)throw Error("ServingCapsuleModuleRequired");
      if(action==="probe")return;
      return host.request({op:"coordinate",working_set_bytes:4*1024*1024,bucket:"database",prefix:this.prefix,namespace:this.namespace,coordinate:{root_backend:"bounded_journal",action,owner:this.owner,serving_scope:this.exclusiveOwnerId,...extra}},{transport:()=>{throw Error("ServingCapsuleIoForbidden");},deadlineMs:this.deadline??this.clock()+5000});
    };
    const options={deadlineMs:this.deadline??this.clock()+5000,fairKey:this.namespace,priority:action==="serving_export"?"maintenance":action==="serving_import"?"interactive":"commit"};
    return tryOnly?this.snapshotKernel.tryWithHost(run,options):this.snapshotKernel.withHost(run,options);
  }
  async restoreDurableRead(){
    if(!this.durableBootstrap)return;
    this.configureDurableCheckpoint();
    if(!WebAssembly.Module.exports(this.module).some(entry=>entry.name==="antfly_host_serving_capsule_v1"))throw Error("ServingCapsuleModuleRequired");
    if(this.durableRestore){while(!this.durableRestore.done)await new Promise(r=>setTimeout(r,2));if(this.durableRestore.error)throw Error(this.durableRestore.error);return;}
    const ticket=this.durableRestore={done:false,error:null};
    try{if(this.durableBootstrap.entry?.dirty)await this.servingCodec("probe");const restored=await this.durableBootstrap.restore((action,extra)=>this.servingCodec(action,extra));
      if(restored){this.durableReadState=restored;this.durableWriteCursor=restored.state;this.observedState=restored.state;this.base=restored.base;this.pagePrefix=restored.page_prefix;}
    }catch(error){ticket.error=String(error.message??error);throw error;}finally{ticket.done=true;}
  }
  // Scheduling hint only: native MAC, clocks, floor and slot checks remain
  // mandatory. Refresh before preparation when an idle certificate cannot
  // cover the likely storage round trips; never renew authority in JavaScript.
  needsWriteRefresh(){
    if(this.rootBackend!=="bounded_journal")return false;
    const anchor=this.journalProof?.anchor,wall=this.clock(),mono=performance.now();
    return !anchor||![anchor.wall_ms,anchor.mono_ms,wall,mono].every(Number.isFinite)||wall<anchor.wall_ms||mono<anchor.mono_ms||Math.max(wall-anchor.wall_ms,mono-anchor.mono_ms)>=25000;
  }
  rootOptions(){return this.rootBackend!=="journal"?{root_backend:this.rootBackend,...(this.rootBackend==="conditional"?{root_cursor:this.rootCursor}:{journal_proof:this.journalProof,journal_read_batch:this.readBatch}),root_proof_scope:this.rootProofScope,root_proof_key:this.rootProofKey}:{};}
  // Publish a captured, already committed recovery floor on the existing
  // observation pool. Foreground transactions never await this hint or share
  // its mutable accounting. Native authentication/CAS rejects stale owners and
  // preserves a newer checkpoint; the native one-second write limit applies.
  async checkpointHint({deadlineMs=Infinity}={}) {
    if(this.freezeRequested||this.rootBackend==="conditional"||!this.acquired||this.recovering||this.ioDrain||!this.state)return{busy:true};
    const state=structuredClone(this.state),physical={};
    // Capture the proof and its state together before yielding. Expired proofs
    // retain the existing strict recovery path; hints never renew authority.
    const certified=this.checkpointHintSupported===true&&this.rootBackend==="bounded_journal"&&!this.needsWriteRefresh();
    const authority=certified?structuredClone(this.rootOptions()):{};
    const transport=this.cache.wrap(instrumentTransport(this.rawTransport,physical),`coordinator/${this.namespace}`);
    const result=await this.snapshotKernel.request({op:"coordinate",working_set_bytes:4*1024*1024,bucket:"database",prefix:this.prefix,namespace:this.namespace,coordinate:{...authority,action:certified?"checkpoint_hint":"checkpoint",owner:this.owner,state,checkpoint_commits:this.checkpointCommits}},{transport,fairKey:this.namespace,priority:"maintenance",deadlineMs:Math.min(this.clock()+10000,deadlineMs),maxOperations:32,maxTransferBytes:1024*1024,onMetrics:sample=>{try{this.onMetrics?.({event:"antfly.checkpoint_hint",...sample,physical_r2:physical});}catch{}}});
    if(result.checkpointed&&this.state?.revision===state.revision)this.replayBytes=0;
    return{done:result.checkpointed,revision:state.revision};
  }
  async native(coordinate) {
    if(this.ownerReady)await this.ownerReady;
    if(this.inlineReadSetReady)await this.inlineReadSetReady;
    if(!this.kernel)this.host ??= await instantiateHost(this.module, this.transport, {clock:this.clock,onMetrics:sample=>this.chargeMetrics(sample)});
    const cursor=Object.hasOwn(coordinate,"state")?coordinate.state:this.state??(coordinate.action==="begin"&&this.observedState?.next>0?this.observedState:null);
    let transport=this.transport,cleanup=async()=>{},abandoned=false;
    if(this.rootBackend==="journal"&&coordinate.action==="retire_logs"&&this.base&&Array.isArray(coordinate.keys)&&coordinate.keys.length<=32){
      const prefix=this.base+"/epochs/",keys=[...new Set(coordinate.keys.filter(key=>typeof key==="string"&&key.startsWith(prefix)&&/^[a-f0-9]{32}\/(?:START|SEALED|[0-9]{20}\.json)$/.test(key.slice(prefix.length))))];
      if((this.usedOperations??0)+keys.length>=1024||(this.usedBytes??0)+keys.length*16384>=32*1024*1024)throw Error("CoordinatorIoBudgetExceeded");
      // Reserve speculation conservatively in addition to native accounting.
      // All results, including missing objects and errors, live for this one
      // proof request. Native checkpoint/retirement validation remains final.
      this.usedOperations=(this.usedOperations??0)+keys.length;this.usedBytes=(this.usedBytes??0)+keys.length*16384;this.prefetchReservedBytes=(this.prefetchReservedBytes??0)+keys.length*16384;
      const results=new Map(),resolvers=new Map(),tasks=[];let next=0,started=false,stopped=false;
      for(const key of keys)results.set(key,new Promise(resolve=>resolvers.set(key,resolve)));
      cleanup=async()=>{stopped=true;await Promise.all(tasks);};
      const start=request=>{
        if(stopped||next>=keys.length)return;
        const key=keys[next++];
        tasks.push(this.transport({...request,key,max_response_bytes:16384},new Uint8Array()).then(value=>({value}),error=>({error})).then(result=>{resolvers.get(key)(result);start(request);}));
      };
      transport=async(request,body)=>{
        if(request.op!=="get")return this.transport(request,body);
        if(!started){started=true;for(let i=0;i<Math.min(8,keys.length);i++)start(request);}
        const pending=results.get(request.key);
        if(!pending)return this.transport(request,body);
        results.delete(request.key);const result=await pending;if(result.error)throw result.error;return result.value;
      };
    }else if(this.rootBackend==="journal"&&coordinate.action==="begin"&&this.recoveryWindow>0&&(!cursor||this.recovering)){
      transport=recoveryReadAhead(this.transport,{window:this.recoveryWindow,reserve:(operations,bytes)=>{if((this.usedOperations??0)+operations>=1024||(this.usedBytes??0)+bytes>=32*1024*1024)throw new Error("CoordinatorIoBudgetExceeded");this.usedOperations+=operations;this.usedBytes+=bytes;this.prefetchReservedBytes+=bytes;}});
    }else if(this.rootBackend==="journal"&&coordinate.action==="begin"&&cursor&&this.base&&(this.usedOperations??0)+2<1024&&(this.usedBytes??0)+34816<32*1024*1024){
      // Prefetch only within this recovery request. Negative results are never
      // retained, and append's before/after floor checks use the base transport.
      this.usedOperations=(this.usedOperations??0)+2;this.usedBytes=(this.usedBytes??0)+34816;this.prefetchReservedBytes=(this.prefetchReservedBytes??0)+34816;
      const epoch=hex(cursor.epoch);
      const checkpoint=this.base+"/CHECKPOINT",keys=[checkpoint,`${this.base}/epochs/${epoch}/SEALED`,`${this.base}/epochs/${epoch}/${String(cursor.next).padStart(20,"0")}.json`];
      let results;
      transport=async(request,body)=>{
        if(request.op==="get"&&request.key===checkpoint&&!results){
          const settled=await Promise.all(keys.map(key=>this.transport({...request,key,max_response_bytes:16384},new Uint8Array()).then(value=>({value}),error=>({error}))));
          results=new Map(keys.map((key,i)=>[key,settled[i]]));
        }
        if(request.op==="get"&&results?.has(request.key)){const result=results.get(request.key);results.delete(request.key);if(result.error)throw result.error;return result.value;}
        return this.transport(request,body);
      };
    }
    try {
      if ((this.usedOperations ?? 0) >= 1024 || (this.usedBytes ?? 0) >= 32*1024*1024) throw new Error("CoordinatorIoBudgetExceeded");
      const result=await (this.readingOwner&&coordinate.action==="read"?this.snapshotKernel:this.kernel??this.host).request({op: "coordinate", bucket: "database", prefix: this.prefix, namespace: this.namespace, coordinate: {...this.rootOptions(),checkpoint_commits:this.checkpointCommits,owner: this.owner, state: cursor, inline_read_set:this.inlineReadSet, ...coordinate}}, {transport,drainTransport:()=>cleanup(),background:this.background,onIoSettled:sample=>this.onMetrics?.(sample),fairKey:this.namespace,priority:this.groupPriority==="maintenance"?"maintenance":"commit",onStage:sample=>this.trace?.stage(sample),onMetrics:sample=>this.chargeMetrics({...sample,action:coordinate.action}),deadlineMs: this.deadline ?? this.clock() + 30000,maxOperations:1024-(this.usedOperations??0),maxTransferBytes:32*1024*1024-(this.usedBytes??0)});
      if(result.checkpoint_hint_supported===true)this.checkpointHintSupported=true;
      if(this.rootBackend==="conditional"&&Object.hasOwn(result,"root_cursor"))this.rootCursor=result.root_cursor;
      if(this.rootBackend==="bounded_journal"&&Object.hasOwn(result,"journal_proof"))this.journalProof=result.journal_proof;
      return result;
    } catch(error) {
      if(error?.ioSettled){abandoned=true;const draining=error.ioSettled;this.ioDrain=draining;draining.then(()=>{if(this.ioDrain===draining)this.ioDrain=null;});}
      throw error;
    } finally {if(!abandoned)await cleanup();if (this.host&&(!this.host.usable() || this.host.memoryBytes() >= 24 * 1024 * 1024)) this.host = null;}
  }
  chargeMetrics(sample){this.nativeCpuMs=(this.nativeCpuMs??0)+Math.max(0,(sample.elapsed_ms??0)-(sample.io_wait_ms??0));this.usedOperations=(this.usedOperations??0)+sample.operations;this.usedBytes=(this.usedBytes??0)+sample.transferred_bytes;try{this.onMetrics?.(sample);}catch{}}
  exportRootHint(){
    const state=this.state;
    if(this.rootBackend!=="bounded_journal"||!this.acquired||!state||!Number.isSafeInteger(state.revision)||state.next<1||state.next>64)return;
    return{namespace:this.namespace,epoch:[...state.epoch],next:state.next,revision:state.revision};
  }
  offerRootHint(hint){
    if(!hint||typeof hint.namespace!=="string"||hint.namespace!==this.namespace||!Array.isArray(hint.epoch)||hint.epoch.length!==16||!Array.from(hint.epoch).every(n=>Number.isInteger(n)&&n>=0&&n<=255)||!Number.isSafeInteger(hint.next)||hint.next<1||hint.next>64||!Number.isSafeInteger(hint.revision)||hint.revision<1||this.rootHint&&hint.revision<=this.rootHint.revision)return false;
    const value={namespace:hint.namespace,epoch:[...hint.epoch],next:hint.next,revision:hint.revision},charge=utf8.encode(JSON.stringify(value)).length*2+256;
    if(charge>4096||charge+this.hintBytes+(this.servingTailBytes??0)>this.rowBudgetBytes)return false;
    this.boundRows?.(this);this.rootHint=value;this.cursorHintBytes=charge;
    while(this.rows.size&&this.rowBytes+this.hintBytes+(this.servingTailBytes??0)+charge>this.rowBudgetBytes){const oldest=this.rows.keys().next().value;this.rowBytes-=this.rows.get(oldest).bytes;this.rows.delete(oldest);}
    return true;
  }
  setReadHints(namespace,generation,hints){
    const accepted=normalizeReadHints(namespace,generation,hints);
    if(!accepted||accepted.charge+this.cursorHintBytes+(this.servingTailBytes??0)>this.rowBudgetBytes||this.readHints&&BigInt(accepted.version)<BigInt(this.readHints.version))return false;
    this.boundRows?.(this);this.readHints=structuredClone(accepted);this.hintBytes=accepted.charge;
    while(this.rows.size&&this.rowBytes+this.hintBytes+this.cursorHintBytes+(this.servingTailBytes??0)>this.rowBudgetBytes){const oldest=this.rows.keys().next().value;this.rowBytes-=this.rows.get(oldest).bytes;this.rows.delete(oldest);}
    return true;
  }
  remember(key,value){
    this.boundRows?.(this);
    const previous=this.rows.get(key);if(previous)this.rowBytes-=previous.bytes;this.rows.delete(key);
    const bytes=utf8.encode(key).length+(value===undefined?0:utf8.encode(encode(value)).length)+128;
    if(bytes>this.rowBudgetBytes-this.hintBytes-this.cursorHintBytes-(this.servingTailBytes??0))return;
    while(this.rows.size>=256||this.rowBytes+this.hintBytes+this.cursorHintBytes+(this.servingTailBytes??0)+bytes>this.rowBudgetBytes){const oldest=this.rows.keys().next().value;this.rowBytes-=this.rows.get(oldest).bytes;this.rows.delete(oldest);}
    this.rows.set(key,{value:structuredClone(value),bytes});this.rowBytes+=bytes;
  }
  // A preparation hint only. It carries no freshness claim; callers must
  // validate its dependencies inside their final authoritative native commit.
  speculativeEntries(names){
    if(!this.acquired||this.recovering||this.ioDrain||this.running||!this.state?.root)return null;
    const root=JSON.parse(this.state.root);if(root.format!==3)return null;
    const map=new Map((root.read_rows?.exact??[]).map(row=>[row.key,row.value]));
    if(root.wal)map.set(root.wal.key,root.wal.value);
    if(root.read_rows?.selected)map.set(root.read_rows.selected.key,root.read_rows.selected.value);
    if(names.some(name=>!map.has(name)))return null;
    return new Map([...map].map(([key,value])=>[key,value==null?undefined:JSON.parse(value,(_,v)=>v&&Object.keys(v).length===1&&typeof v.__antfly_utf8==="string"?utf8.encode(v.__antfly_utf8):v&&Object.keys(v).length===1&&Array.isArray(v.__antfly_bytes)?Uint8Array.from(v.__antfly_bytes):v)]));
  }
  async readOnly(run,{traceId,traceIds=[],onAdmission,priority="interactive",background,deadlineMs:outerDeadlineMs=Infinity,readKeys,readPrefix,ownerCurrent=false,ownerCold=false,ownerDurable=false}={}){
    if(this.freezeRequested)throw Error("CoordinatorMigrationRequired");
    if(this.ownerReady)await this.ownerReady;
    if(ownerDurable&&(!this.durableReadState||this.recovering||this.ioDrain))throw Error("ReadSnapshotNeedsAdmission");
    if(ownerCurrent&&(!this.ownerMvcc||!this.acquired||this.recovering||this.ioDrain||!this.state))throw Error("ReadSnapshotNeedsAdmission");
    if(ownerCurrent&&(readKeys||readPrefix))throw Error("InvalidOwnerSnapshotProjection");
    // Seal admission membership before capturing the immutable committed root.
    if(ownerCurrent)onAdmission?.();
    if(this.snapshotReaders>=8)throw Error("ReadSnapshotNeedsAdmission");
    const capturedHint=this.rootHint?{epoch:[...this.rootHint.epoch],next:this.rootHint.next,revision:this.rootHint.revision}:null;
    const owned=this.acquired,captured=structuredClone(ownerDurable?this.durableReadState.state:owned?this.state:(this.rootBackend==="bounded_journal"||this.observedState?.next>0?this.observedState:null)),physical={},rows=new Map(),deadlineMs=Math.min(this.clock()+5000,outerDeadlineMs),started=performance.now();let outcome="error",usedOperations=0,usedBytes=0,admissionBudgetExceeded=false,capsuleMetrics={};
    const monotonicDeadline=started+Math.max(0,Math.min(5000,deadlineMs-this.clock()));
    let proofPermit,failure,finalization;
    const reserve=(operations,bytes)=>{if(usedOperations+operations>=254||usedBytes+bytes>=8*1024*1024){admissionBudgetExceeded=true;throw Error("ReadSnapshotNeedsAdmission");}usedOperations+=operations;usedBytes+=bytes;};
    const cached=this.cache.wrap(this.remoteCache(instrumentTransport(this.rawTransport,physical),{background,budgetKey:()=>physical,onMetrics:sample=>{for(const[key,value]of Object.entries(sample))physical[key]=(physical[key]??0)+value;}}),`coordinator/${this.namespace}`);
    const raw=(request,body)=>{onAdmission?.();return cached(request,body);};
    const guarded=async(request,body)=>{if(request.op!=="get")throw Error("ReadSnapshotWriteForbidden");return raw(request,body);};
    let transport,proofTransport=guarded;
    if(this.rootBackend==="journal"&&captured&&this.base){
      proofTransport=warmRecoveryReadAhead(guarded,{base:this.base,cursor:captured,reserve});
    }
    // An authenticated warm cursor identifies the three initial proof keys.
    // Fetch them in one wave; native recovery still selects and validates the
    // checkpoint/epoch, and unused speculation remains charged to this call.
    transport=this.rootBackend!=="journal"?proofTransport:owned&&captured&&this.base?proofTransport:recoveryReadAhead(proofTransport,{window:this.recoveryWindow||1,initialWindow:1,reserve,cursor:captured,followSeals:!owned});
    let snapshotProof=structuredClone(owned?this.journalProof:this.observedProof??null);
    const native=async(state,coordinate)=>{
      if(usedOperations>=254||usedBytes>=8*1024*1024)throw Error("ReadSnapshotNeedsAdmission");
      const result=await this.snapshotKernel.request({op:"coordinate",working_set_bytes:4*1024*1024,bucket:"database",prefix:this.prefix,namespace:this.namespace,coordinate:{...this.rootOptions(),...(this.rootBackend==="bounded_journal"?{journal_proof:snapshotProof,...(capturedHint?{journal_hint:capturedHint}:{})}:{}),owner:coordinate.action==="read"?state.owner:this.owner,state,...coordinate}},{transport,background:background??this.background,onIoSettled:sample=>this.onMetrics?.(sample),fairKey:this.namespace,priority,onStage:sample=>{if(sample.name==="kernel_queue"){onAdmission?.();capsuleMetrics.observation_queue_ms=(capsuleMetrics.observation_queue_ms??0)+sample.duration_ms;}if(sample.name==="wasm_instantiate")capsuleMetrics.observation_instantiate_ms=(capsuleMetrics.observation_instantiate_ms??0)+sample.duration_ms;},deadlineMs,maxOperations:254-usedOperations,maxTransferBytes:8*1024*1024-usedBytes,onMetrics:sample=>{usedOperations+=sample.operations;usedBytes+=sample.transferred_bytes;try{this.onMetrics?.({event:"antfly.coordinator_snapshot_native",trace_id:traceId,trace_ids:traceIds,action:coordinate.action,...sample});}catch{}}});
      if(result.observation_plan_supported===true)this.observationPlanSupported=true;
      if(Object.hasOwn(result,"journal_proof"))snapshotProof=result.journal_proof;for(const[key,value]of Object.entries(result.journal_observation??{}))if(Number.isSafeInteger(value)&&value>=0)capsuleMetrics['journal_'+key]=(capsuleMetrics['journal_'+key]??0)+value;return result;
    };
    this.snapshotReaders++;
    const finish=()=>{
      if(finalization)return finalization;
      // Snapshot the native lifetime before attaching our combined lifetime to
      // the returned error; otherwise finalization could await itself.
      const nativeSettlement=proofPermit?failure?.ioSettled:undefined;
      return finalization=(async()=>{
      try{
        await proofTransport.drain?.();
        if(nativeSettlement)await nativeSettlement;
      }finally{
        proofPermit?.();this.snapshotReaders--;
        if(outcome==="ok"&&(this.clock()>=deadlineMs||proofPermit&&performance.now()>=monotonicDeadline))outcome="error";
        try{this.onMetrics?.({event:"antfly.coordinator_snapshot",trace_id:traceId,trace_ids:traceIds,outcome,owned,owner_current:ownerCurrent,owner_cold:ownerCold,owner_durable:ownerDurable,proof_prefetch:!!proofPermit,proof_prefetch_reserved_bytes:proofPermit?proofPrefetchBytes:0,...capsuleMetrics,elapsed_ms:performance.now()-started,operations:usedOperations,transferred_bytes:usedBytes,physical_r2:physical});}catch{}
      }
      })();
    };
    try{
      // The submitting request starts and owns proof I/O. Reserve a bounded
      // auxiliary permit instead of holding an idle WASM stack on storage.
      // Without a lifetime port, use the original in-kernel path.
      if(proofTransport.prime&&typeof background==="function"&&this.proofBudget){
        const acquisition=this.proofBudget.acquire(this.namespace);
        try{proofPermit=await beforeProofDeadline(acquisition.ready,deadlineMs,monotonicDeadline,this.clock);}
        catch(error){acquisition.cancel();throw error;}
        reserve(1,proofPrefetchBytes-4*16384);proofPermit.start();
        await beforeProofDeadline(proofTransport.prime({op:"get",bucket:"database",key:this.base+"/CHECKPOINT"}),deadlineMs,monotonicDeadline,this.clock);
      }
      const plannedObservation=async(state,coordinate)=>{
        if(!this.readContinuations||!this.observationPlanSupported||this.rootBackend!=="bounded_journal"||!captured||!snapshotProof||typeof background!=="function"||!this.proofBudget)return null;
        const acquisition=this.proofBudget.acquire(this.namespace,{concurrent:true});
        try{proofPermit=await beforeProofDeadline(acquisition.ready,deadlineMs,monotonicDeadline,this.clock);}catch(error){acquisition.cancel();throw error;}
        let result;
        try{
          result=await native(state,{...coordinate,action:"observe_step"});
          for(let step=0;result.observation_plan;step++){
            if(step>=8)throw Error("InvalidImmutableLogObservationPlan");
            proofPermit.start();
            const response=await readObservationResponse(transport,result.observation_key,{base:this.base,reserve,
              wait:pending=>beforeProofDeadline(pending,deadlineMs,monotonicDeadline,this.clock),
              check:()=>{if(this.clock()>=deadlineMs||performance.now()>=monotonicDeadline)throw Error("DeadlineExceeded");},
              onDraining:ioSettled=>{const id=crypto.randomUUID();try{this.onMetrics?.({event:"antfly.coordinator_snapshot_native",trace_id:traceId,io_draining:true,io_lifecycle_id:id});}catch{}ioSettled.then(()=>{try{this.onMetrics?.({event:"antfly.io_settled",io_lifecycle_id:id});}catch{}});},
            });
            capsuleMetrics.observation_detached_gets=(capsuleMetrics.observation_detached_gets??0)+1;
            result=await native(state,{...coordinate,action:"observe_step",observation_plan:result.observation_plan,observation_response:response});
          }
        }catch(error){if(error.message!=="ImmutableLogProofExpired")throw error;capsuleMetrics.observation_continuation_expired=(capsuleMetrics.observation_continuation_expired??0)+1;result=null;}
        if(!result||result.observation_fallback){proofPermit();proofPermit=null;return null;}
        return result;
      };
      let observed=ownerCurrent||ownerDurable?{state:captured,base:this.base,page_prefix:this.pagePrefix,has_more:false}:null,state=captured;
      for(let attempt=0;!ownerCurrent&&!ownerDurable&&attempt<4;attempt++){
        const coordinate=readKeys?{action:"observe_read",observe_owned:owned,commands:readKeys.map(key=>({kind:"get",key})),read_prefix:readPrefix??null} :{action:owned?"observe":"observe_committed"};
        observed=readKeys?await plannedObservation(state,coordinate):null;
        observed??=await native(state,coordinate);state=observed.state;
        if(!observed.has_more)break;
        // A sealed epoch may lack START after a partial hint upload. A
        // legacy unowned observer cannot authenticate that continuation independently.
        // A bounded observer carries the native MAC and original deadline.
        if(!owned&&this.rootBackend==="journal"&&state.next===0)throw Error("ReadSnapshotNeedsAdmission");
      }
      if(observed.has_more)throw Error("ReadSnapshotNeedsAdmission");
      if(readKeys){
        if(!Array.isArray(observed.results)||observed.results.length!==readKeys.length)throw Error("InvalidCoordinatorReadProjection");
        readKeys.forEach((key,i)=>rows.set(key,observed.results[i][0]?decode(observed.results[i][0].value):undefined));
        if(observed.selected){if(!readPrefix||!observed.selected.key.startsWith(readPrefix))throw Error("InvalidCoordinatorReadProjection");rows.set(observed.selected.key,decode(observed.selected.value));}
      }
      Object.assign(capsuleMetrics,rootMetrics(observed.state));
      if(!ownerCurrent&&!ownerDurable&&(!this.observedState||observed.state.revision>=this.observedState.revision)){this.observedState=structuredClone(observed.state);this.observedProof=structuredClone(snapshotProof);}
      this.base??=observed.base;this.pagePrefix??=observed.page_prefix;
      const tx={getMany:async names=>{
        // A decoded row is reusable only after a fresh matching observation.
        if(this.rowRevision===observed.state.revision&&this.state?.revision===observed.state.revision&&hex(this.state.epoch)===hex(observed.state.epoch)&&hex(this.state.owner)===hex(observed.state.owner)){
          for(const name of names)if(!rows.has(name)&&this.rows.has(name))rows.set(name,structuredClone(this.rows.get(name).value));
        }
        const missing=[...new Set(names.filter(name=>!rows.has(name)))];
        if(missing.length){const result=await native(observed.state,{action:"read",commands:missing.map(key=>({kind:"get",key}))});missing.forEach((key,i)=>rows.set(key,result.results[i][0]?decode(result.results[i][0].value):undefined));
          // Publish decoded rows only while the captured committed root is still
          // current. A read that outlives a commit must never seed its cache.
          if(ownerCurrent&&this.acquired&&this.rowRevision===observed.state.revision&&this.state?.revision===observed.state.revision&&hex(this.state.epoch)===hex(observed.state.epoch)&&hex(this.state.owner)===hex(observed.state.owner)){
            for(const key of missing)this.remember(key,rows.get(key));
          }}
        return names.map(key=>structuredClone(rows.get(key)));
      },get:async key=>(await tx.getMany([key]))[0],snapshot:()=>structuredClone(observed.state),put:()=>{throw Error("ReadSnapshotNeedsPin");}};
      const result=await run(tx);outcome="ok";return result;
    }catch(error){
      failure=error;
      if(!ownerCurrent&&(admissionBudgetExceeded||["ConditionalRootMissing","ObjectIoBudgetExceeded","HostedWorkingSetBudgetExceeded","ReadAheadCapacityExceeded"].includes(error?.message))){outcome="needs_admission";failure=Error("ReadSnapshotNeedsAdmission");if(error.ioSettled)Object.defineProperty(failure,"ioSettled",{value:error.ioSettled});throw failure;}
      throw error;
    }finally{
      const settlement=finish();
      if(proofPermit){
        try{await beforeProofDeadline(settlement,deadlineMs,monotonicDeadline,this.clock);}
        catch(error){
          if(!error[proofDeadline])throw error;
          // Timed-out binding I/O still owns its permit and origin lifetime.
          // Late resource failures remain visible through instrumentTransport.
          try{background(settlement);}catch{await settlement;}
          if(failure){if(!failure.ioSettled)Object.defineProperty(failure,"ioSettled",{value:settlement});}
          else{Object.defineProperty(error,"ioSettled",{value:settlement});throw error;}
        }
      }else await settlement;
      if(!failure&&outcome!=="ok")throw Error("DeadlineExceeded");
    }
  }
  async ownerRead(run,{deadlineMs=this.clock()+30000,traceId,prefetchKeys}={}){
    if(this.freezeRequested)throw Error("CoordinatorMigrationRequired");
    if(!this.ownerServing)throw Error("ResidentServingOwnerRequired");
    if(prefetchKeys){if(!Array.isArray(prefetchKeys)||prefetchKeys.length>8)throw Error("InvalidOwnerSnapshotProjection");const callback=run;run=async tx=>{await tx.getMany(prefetchKeys);return callback(tx);};}
    if(this.ownerMvcc&&!this.acquired&&!this.recovering&&!this.ioDrain&&(this.durableReadState||!this.running&&!this.pending.length)){
      const admission=this.writeAdmission??0n,durable=!!this.durableReadState;
      try{
        const result=await this.readOnly(run,{ownerCold:true,ownerDurable:durable,traceId,deadlineMs});
        // Fresh observation is sufficient for reads before write takeover. A
        // concurrent write admission invalidates this result even if it fails.
        if(!durable&&((this.writeAdmission??0n)!==admission||this.acquired||this.running||this.recovering||this.ioDrain||this.pending.length))throw Error("ReadSnapshotNeedsAdmission");
        return result;
      }catch(error){if(!/^ReadSnapshotNeeds(?:Admission|Pin)$/.test(error.message))throw error;}
    }
    if(this.ownerMvcc)try{return await this.readOnly(run,{ownerCurrent:true,traceId,deadlineMs});}catch(error){if(!/^ReadSnapshotNeeds(?:Admission|Pin)$/.test(error.message))throw error;}
    const reservedOperations=1024-254,reservedBytes=24*1024*1024,ioBudget={operations:reservedOperations,bytes:reservedBytes};
    for(let attempt=0;attempt<4;attempt++)try{
      return await this.enqueue({kind:"owner_read",priority:"interactive",deadlineMs,traceId,ioBudget,run:tx=>run({get:tx.get,getMany:tx.getMany,snapshot:tx.snapshot,put:()=>{throw Error("OwnerReadWriteForbidden");},delete:()=>{throw Error("OwnerReadWriteForbidden");}})});
    }catch(error){
      // Only native recovery that stopped BEFORE the callback can continue.
      // A draining/resource failure or a callback's error is never replayed.
      if(error?.message!=="CoordinatorRecovering"||!this.recovering||this.ioDrain||this.clock()>=deadlineMs)throw error;
      try{this.onMetrics?.({event:"antfly.owner_read_recovery",trace_id:traceId,attempt:attempt+1,operations:ioBudget.operations-reservedOperations,transferred_bytes:ioBudget.bytes-reservedBytes});}catch{}
    }
    throw Error("CoordinatorRecovering");
  }
  commitWal(commit,{deadlineMs=Infinity,traceId,nativePlan=false,ioBudget}={}){
    return this.enqueue({kind:"commit_wal",commit,deadlineMs,traceId,nativePlan,ioBudget,priority:"interactive"});
  }
  transaction(run,{priority="interactive",traceId,deadlineMs=Infinity,ioBudget}={}) {
    return this.enqueue({run,priority,traceId,deadlineMs,ioBudget});
  }
  enqueue(input) {
    if(this.freezeRequested)return Promise.reject(Error("CoordinatorMigrationRequired"));
    if(input.kind!=="owner_read"&&input.kind!=="durable_checkpoint")this.writeAdmission=(this.writeAdmission??0n)+1n;
    if(this.requiresExclusiveOwner&&!this.ownerServing)return Promise.reject(Error("ResidentServingOwnerRequired"));
    if(this.ioDrain)return Promise.reject(new Error("CoordinatorRecovering"));
    if(this.pending.length>=64)return Promise.reject(new Error("CoordinatorCommitBusy"));
    return new Promise((resolve,reject)=>{
      let admit;
      const ready=new Promise(start=>admit=start);
      // Register native batch execution in this submitter's context. A long
      // drain loop must not keep later groups in an already answered RPC.
      ready.then(async owner=>{
        if(!owner)return;
        let completed;
        try{completed=await this.batch();}
        catch(error){completed=this.pending.splice(0).map(item=>({...item,batchError:error}));}
        // No native I/O or queue ownership remains in a caller's continuation
        // after its result can end the RPC. Admit the next owner first.
        this.running=false;this.drain();
        for(const item of completed)item.callbackFailed?item.reject(item.callbackError):item.batchError?item.reject(item.batchError):item.resolve(item.result);
      });
      this.pending.push({...input,queuedAt:performance.now(),startBatch:()=>admit(true),resolve:value=>{admit(false);resolve(value);},reject:error=>{admit(false);reject(error);}});
      this.drain();
    });
  }
  drain() {
    if(this.running||!this.pending.length)return;
    const now=performance.now();
    this.pending.sort((a,b)=>Number(a.priority!=="interactive"&&now-a.queuedAt<5000)-Number(b.priority!=="interactive"&&now-b.queuedAt<5000));
    this.running=true;this.pending[0].startBatch();
  }
  async batch() {
    const started=performance.now(),wallStarted=this.clock();this.trace=new CriticalPath({id:this.pending[0]?.traceId,scope:"coordinator"});this.deadline = this.clock()+30000;
    const ioBudget=this.pending[0]?.ioBudget,creditedOperations=ioBudget?.operations??0,creditedBytes=ioBudget?.bytes??0;
    this.physical={};this.usedOperations=creditedOperations; this.usedBytes=creditedBytes;this.nativeCpuMs=0;this.prefetchReservedBytes=0;let beginMs=0,commitMs=0,maxQueueMs=0;
    // The selected owner remains first until consumed. Arrivals during begin
    // cannot defer its callback and strand a consumed batch permit.
    this.groupPriority=this.pending[0]?.priority;this.readingOwner=this.pending[0]?.kind==="owner_read";
    const checkDurableDeadline=()=>{const now=this.clock();if(now<wallStarted||now>=this.deadline||performance.now()-started>=this.deadline-wallStarted)throw Error("DeadlineExceeded");};
    const completed=[];
    try {
      if(this.ioDrain)throw new Error("CoordinatorRecovering");
      if(Number.isFinite(this.pending[0]?.deadlineMs)){
        this.deadline=Math.min(this.deadline,this.pending[0].deadlineMs);
        if(this.clock()>=this.deadline){completed.push(this.pending.shift());throw new Error("DeadlineExceeded");}
      }
      if(this.pending[0]?.kind==="durable_checkpoint"){
        const item=this.pending.shift();completed.push(item);
        if(this.freezeRequested||!this.acquired||this.recovering||this.ioDrain||!this.state){item.result={done:false,revision:this.state?.revision??0};return completed;}
        if(this.durableBootstrap.entry?.dirty===false){item.result={done:true,revision:this.durableBootstrap.generation??0,native_revision:this.state.revision};return completed;}
        // Optional acceleration must not hold the metadata queue while waiting
        // for native capacity needed by foreground preparation/finalization.
        const exported=await this.servingCodec("serving_export",{state:this.state,owner:this.state.owner},{tryOnly:true});checkDurableDeadline();
        if(!exported.admitted){item.result={done:false,busy:true,revision:this.durableBootstrap.generation??0};return completed;}
        await this.durableBootstrap.finish(this.state,()=>exported.value);checkDurableDeadline();
        item.result={done:true,revision:this.durableBootstrap.generation??0,native_revision:this.state.revision};return completed;
      }
      // Commit-only native plans expose no read result before append. A warm
      // retained root is authenticated by append's before/after floor and seal
      // checks, then linearized by its immutable conditional journal slot.
      // Stale slots fail closed and force ordinary recovery before a retry.
      const warmCommit=this.pending[0]?.kind==="commit_wal"&&this.pending[0].nativePlan&&this.acquired&&!this.recovering&&this.state?.next<64&&(this.rootBackend!=="conditional"||this.rootCursor?.revision!=null);
      // In descriptor-v2 mode the live provider instance owns the confirmed
      // state. Expired append certificates are renewed by writers, not readers.
      const warmOwnerRead=this.pending[0]?.kind==="owner_read"&&this.ownerServing&&this.acquired&&!this.recovering&&this.state;
      if(this.durableBootstrap&&!warmOwnerRead){
        await this.durableBootstrap.begin();checkDurableDeadline();
        // A clean, actor-local checkpoint proves no accepted-prefix PUT was
        // outstanding. Reuse only its identity/cursor after a new durable
        // intent; begin must still obtain fresh native admission and proofs.
        if(!this.acquired&&this.durableWriteCursor){this.state=this.durableWriteCursor;this.owner=[...this.state.owner];this.durableWriteCursor=null;}
      }
      const recovered = warmCommit||warmOwnerRead?{state:this.state,base:this.base,page_prefix:this.pagePrefix,has_more:false}:await this.trace.span("recovery",()=>this.native({action: "begin"}));
      beginMs=performance.now()-started;
      if(!recovered.has_more&&hex(recovered.state.owner)!==hex(this.owner))throw new Error("ImmutableLogFenced");
      this.state = recovered.state; this.base = recovered.base; this.pagePrefix = recovered.page_prefix;
      this.recovering=Boolean(recovered.has_more);
      if (recovered.has_more) throw new Error("CoordinatorRecovering");
      this.acquired=true;this.durableReadState=null;
      if(this.rowRevision!==this.state.revision){this.rows.clear();this.rowBytes=0;this.rowRevision=this.state.revision;}
      if(this.pending[0]?.kind==="commit_wal"){
        const item=this.pending.shift();completed.push(item);
        maxQueueMs=performance.now()-item.queuedAt;
        if(this.clock()>=item.deadlineMs)throw new Error("DeadlineExceeded");this.deadline=Math.min(this.deadline,item.deadlineMs);
        const commitStarted=performance.now();
        const committed=await this.trace.span("wal_commit",()=>this.native({action:"commit_wal",wal_commit:item.commit}));
        commitMs=performance.now()-commitStarted;
        if(this.idleCheckpoint?.replayCost)this.replayBytes=Math.min(64*1024,(this.replayBytes??0)+utf8.encode(JSON.stringify(item.commit)).byteLength);
        this.state=committed.state;this.rows.clear();this.rowBytes=0;this.rowRevision=this.state.revision;
        item.result=committed;return completed;
      }
      const overlay = new Map();
      const tx = {
        getMany: async keys => {
          const missing = [...new Set(keys.filter(key => !overlay.has(key)&&!this.rows.has(key)))];
          const rows = missing.length ? (await this.native({action: "read", commands: missing.map(key => ({kind: "get", key}))})).results : [];
          const found = new Map(missing.map((key, i) => [key, rows[i][0] ? decode(rows[i][0].value) : undefined]));
          for(const key of keys)if(!found.has(key)&&this.rows.has(key))found.set(key,this.rows.get(key).value);
          for(const [key,value]of found)this.remember(key,value);
          return keys.map(key => structuredClone(overlay.has(key) ? overlay.get(key) : found.get(key)));
        },
        get: async key => (await tx.getMany([key]))[0],
        put: (key, value) => {if (value === undefined) throw new Error("CoordinatorValueRequired"); overlay.set(key, structuredClone(value));},
        delete: key => overlay.set(key, undefined),
        list: async ({prefix = "", startAfter = "", limit = 128} = {}) => {
          if (!Number.isSafeInteger(limit) || limit < 1 || limit > 128) throw new Error("CoordinatorBatchExceeded");
          const values = new Map(); let after = startAfter;
          for (let scan = 0; scan < 8; scan++) {
            const page = (await this.native({action: "read", commands: [{kind: "list", key: prefix, after, limit: 128}]})).results[0];
            for (const row of page) values.set(row.key, decode(row.value));
            for (const [key, value] of overlay) if (key.startsWith(prefix) && compare(key, startAfter) > 0) {
              if (value === undefined) values.delete(key); else values.set(key, value);
            }
            const sorted = [...values].sort(([a],[b]) => compare(a,b));
            if (page.length < 128 || (sorted.length >= limit && compare(sorted[limit-1][0], page.at(-1).key) <= 0)) return structuredClone(new Map(sorted.slice(0, limit)));
            after = page.at(-1).key;
          }
          throw new Error("CoordinatorBatchExceeded");
        },
        mutationCount: () => overlay.size,
        shouldYieldMaintenance: () => this.groupPriority==="maintenance"&&(overlay.size>=maintenanceMutationBudget||performance.now()-started>=maintenanceSliceMs||this.pending.some(item=>item.priority==="interactive")),
        snapshot: () => structuredClone(this.state),
        checkpoint: async () => (await this.native({action: "checkpoint"})).checkpointed,
        retirableLogs: async (keys, before) => (await this.native({action: "retire_logs", keys, before})).keys,
        trace: async page => this.native({action: "trace", page}),
      };
      // Independent RPCs serialize through one overlay and share a durable
      // commit. Every result waits for that commit. A failing callback rolls
      // back only its own writes; reads return copies to isolate mutations.
      const groupPriority=this.pending[0]?.priority,explicitDeadline=Number.isFinite(this.pending[0]?.deadlineMs)||!!ioBudget;
      for (let count=0;count<(groupPriority==="maintenance"||explicitDeadline?1:16)&&this.pending.length&&this.pending[0].kind!=="commit_wal"&&overlay.size<=80&&(count===0||performance.now()-started<5000);count++) {
        if(count>0){const queuedNow=performance.now();this.pending.sort((a,b)=>Number(a.priority!=="interactive"&&queuedNow-a.queuedAt<5000)-Number(b.priority!=="interactive"&&queuedNow-b.queuedAt<5000));}
        if(this.pending[0]?.kind==="commit_wal"||this.pending[0]?.priority!==groupPriority||count>0&&(Number.isFinite(this.pending[0]?.deadlineMs)||this.pending[0]?.ioBudget))break;
        const item=this.pending.shift(), previous=new Map(overlay);
        maxQueueMs=Math.max(maxQueueMs,performance.now()-item.queuedAt);
        try {
          const result=await item.run(tx);
          if (overlay.size>512) throw new Error("CoordinatorCommitBusy");
          completed.push({...item,result:structuredClone(result)});
        } catch(error) {
          if(/IntegrityMismatch/.test(String(error))){this.cache.clear();this.rows.clear();this.rowBytes=0;}
          overlay.clear(); for(const [key,value] of previous) overlay.set(key,value);
          completed.push({...item,callbackFailed:true,callbackError:error});
        }
      }
      if (overlay.size) {
        if (overlay.size > 512) throw new Error("CoordinatorBatchExceeded");
        const commitStarted=performance.now();
        const committed = await this.native({action: "commit", commands: [...overlay].map(([key,value]) => ({kind: value === undefined ? "delete" : "put", key, value: value === undefined ? null : encode(value)}))});
        commitMs=performance.now()-commitStarted;
        this.state = committed.state;
        if(this.idleCheckpoint?.replayCost)this.replayBytes=Math.min(64*1024,(this.replayBytes??0)+[...overlay].reduce((sum,[key,value])=>sum+utf8.encode(key).byteLength+(value?.byteLength??0),0));
        for(const [key,value]of overlay)this.remember(key,value);
        this.rowRevision=this.state.revision;
      }
    } catch(error) {
      if(completed[0]?.kind!=="durable_checkpoint"){this.acquired=false;this.durableReadState=null;this.durableWriteCursor=null;this.clearServingTail?.();this.rows.clear();this.rowBytes=0;this.rowRevision=null;}
      for(const item of completed)item.batchError=error;
      // Recovery advances a bounded cursor before returning backpressure.
      // Reject queued callers rather than retaining an unbounded request set.
      if (!completed.length)for(const item of this.pending.splice(0,ioBudget?1:16))completed.push({...item,batchError:error});
    } finally {
      if(ioBudget){ioBudget.operations=this.usedOperations;ioBudget.bytes=this.usedBytes;}
      try{this.onMetrics?.({event:"antfly.coordinator_group",owner_read:this.readingOwner===true,operations:this.usedOperations-creditedOperations,transferred_bytes:this.usedBytes-creditedBytes,elapsed_ms:performance.now()-started,physical_r2:{...this.physical},trace:this.trace.report(),prefetch_reserved_bytes:this.prefetchReservedBytes,begin_ms:beginMs,commit_ms:commitMs,native_cpu_ms:this.nativeCpuMs,queue_wait_max_ms:maxQueueMs,row_cache_bytes:this.rowBytes,callbacks:completed.length,trace_ids:completed.map(item=>item.traceId).filter(Boolean)});}catch{}
      this.deadline = null;this.readingOwner=false;
      this.idleCheckpoint?.schedule();this.durableCheckpoint?.schedule();
    }
    return completed;
  }
}
