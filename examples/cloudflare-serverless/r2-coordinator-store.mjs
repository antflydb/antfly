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
import {normalizeReadHints} from "./publication-read-hints.mjs";
import {RequestPool} from "./request-pool.mjs";
import {recoveryReadAhead,warmRecoveryReadAhead} from "./recovery-read-ahead.mjs";
import {CriticalPath} from "./trace.mjs";
import {IdleCheckpoint} from "./idle-checkpoint.mjs";
import {instrumentTransport} from "./performance.mjs";
import {maintenanceMutationBudget,maintenanceSliceMs} from "./maintenance-policy.mjs";
import {instantiateHost} from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import {createR2Transport} from "../../zig/lib/objectstore/wasm/r2.mjs";
import {createImmutableReadCache} from "../../zig/lib/objectstore/wasm/cache.mjs";
const utf8 = new TextEncoder();
const hex=bytes=>Array.from(typeof bytes==="string"?utf8.encode(bytes):bytes,n=>n.toString(16).padStart(2,"0")).join("");
const compare = (l, r) => {const a = utf8.encode(l), b = utf8.encode(r); for (let i=0; i<Math.min(a.length,b.length); i++) if (a[i]!==b[i]) return a[i]-b[i]; return a.length-b.length;};
function rootMetrics(state){try{const root=JSON.parse(state?.root??'null');return{log_slot:state?.next??0,log_revision:state?.revision??0,inline_root_format:root?.format??0,inline_root_rows:root?.format===3?root.read_rows.exact.length:0,inline_root_has_pin:root?.format===3&&root.read_rows.selected!=null,inline_root_bytes:utf8.encode(state?.root??'').byteLength};}catch{return{inline_root_format:0};}}
export const encode = value => JSON.stringify(value, (_, v) => v instanceof Uint8Array || v instanceof ArrayBuffer ? {__antfly_bytes: Array.from(new Uint8Array(v.buffer ?? v, v.byteOffset ?? 0, v.byteLength))} : v);
const decode = value => JSON.parse(value, (_, v) => v && Object.keys(v).length === 1 && Array.isArray(v.__antfly_bytes) ? Uint8Array.from(v.__antfly_bytes) : v);
export class R2CoordinatorStore {
  constructor(module, bucket, namespace, {prefix = "antfly", clock = Date.now, onMetrics, owner, inlineReadSet,transport,kernel,readKernel,cache,boundRows,background,checkpointCommits=64,recoveryWindow=8,remoteCache=transport=>transport} = {}) {
    this.module = module; this.bucket = bucket; this.namespace = namespace; this.prefix = prefix; this.clock = clock; this.onMetrics = onMetrics;
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
    this.rawTransport=raw;this.remoteCache=remoteCache;this.snapshotKernel=readKernel??kernel??new RequestPool(()=>instantiateHost(module,null,{clock}),1,{clock});this.snapshotReaders=0;
    this.transport = this.cache.wrap(this.remoteCache(async(request,body)=>instrumentTransport(raw,this.physical??={})(request,body),{budgetKey:()=>this.physical??={},onMetrics:sample=>{for(const[key,value]of Object.entries(sample)){this.physical??={};this.physical[key]=(this.physical[key]??0)+value;}}}), `coordinator/${namespace}`);
    this.pending = []; this.running = false; this.state = null;
    this.rows=new Map();this.rowBytes=0;this.hintBytes=0;this.readHints=null;this.rowRevision=null;this.acquired=false;
    if(background)this.idleCheckpoint=new IdleCheckpoint({revision:()=>this.state?.revision??0,isIdle:()=>this.acquired&&!this.recovering,background,onError:error=>{try{this.onMetrics?.({event:"antfly.checkpoint_deferred",error:String(error)});}catch{}},checkpoint:()=>this.checkpointHint()});
  }
  // Publish a captured, already committed recovery floor on the existing
  // observation pool. Foreground transactions never await this hint or share
  // its mutable accounting. Native authentication/CAS rejects stale owners and
  // preserves a newer checkpoint; the native one-second write limit applies.
  async checkpointHint() {
    if(!this.acquired||this.recovering||this.ioDrain||!this.state)return{busy:true};
    const state=structuredClone(this.state),physical={};
    const transport=this.cache.wrap(instrumentTransport(this.rawTransport,physical),`coordinator/${this.namespace}`);
    const result=await this.snapshotKernel.request({op:"coordinate",working_set_bytes:4*1024*1024,bucket:"database",prefix:this.prefix,namespace:this.namespace,coordinate:{action:"checkpoint",owner:this.owner,state,checkpoint_commits:this.checkpointCommits}},{transport,fairKey:this.namespace,priority:"maintenance",deadlineMs:this.clock()+10000,maxOperations:32,maxTransferBytes:1024*1024,onMetrics:sample=>{try{this.onMetrics?.({event:"antfly.checkpoint_hint",...sample,physical_r2:physical});}catch{}}});
    return{done:result.checkpointed,revision:state.revision};
  }
  async native(coordinate) {
    if(this.ownerReady)await this.ownerReady;
    if(this.inlineReadSetReady)await this.inlineReadSetReady;
    if(!this.kernel)this.host ??= await instantiateHost(this.module, this.transport, {clock:this.clock,onMetrics:sample=>this.chargeMetrics(sample)});
    const cursor=Object.hasOwn(coordinate,"state")?coordinate.state:this.state??(coordinate.action==="begin"&&this.observedState?.next>0?this.observedState:null);
    let transport=this.transport,cleanup=async()=>{},abandoned=false;
    if(coordinate.action==="retire_logs"&&this.base&&Array.isArray(coordinate.keys)&&coordinate.keys.length<=32){
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
    }else if(coordinate.action==="begin"&&this.recoveryWindow>0&&(!cursor||this.recovering)){
      transport=recoveryReadAhead(this.transport,{window:this.recoveryWindow,reserve:(operations,bytes)=>{if((this.usedOperations??0)+operations>=1024||(this.usedBytes??0)+bytes>=32*1024*1024)throw new Error("CoordinatorIoBudgetExceeded");this.usedOperations+=operations;this.usedBytes+=bytes;this.prefetchReservedBytes+=bytes;}});
    }else if(coordinate.action==="begin"&&cursor&&this.base&&(this.usedOperations??0)+2<1024&&(this.usedBytes??0)+34816<32*1024*1024){
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
      return await (this.kernel??this.host).request({op: "coordinate", bucket: "database", prefix: this.prefix, namespace: this.namespace, coordinate: {checkpoint_commits:this.checkpointCommits,owner: this.owner, state: cursor, inline_read_set:this.inlineReadSet, ...coordinate}}, {transport,drainTransport:()=>cleanup(),fairKey:this.namespace,priority:this.groupPriority==="maintenance"?"maintenance":"commit",onStage:sample=>this.trace?.stage(sample),onMetrics:sample=>this.chargeMetrics({...sample,action:coordinate.action}),deadlineMs: this.deadline ?? this.clock() + 30000,maxOperations:1024-(this.usedOperations??0),maxTransferBytes:32*1024*1024-(this.usedBytes??0)});
    } catch(error) {
      if(error?.ioSettled){abandoned=true;const draining=error.ioSettled;this.ioDrain=draining;draining.then(()=>{if(this.ioDrain===draining)this.ioDrain=null;});}
      throw error;
    } finally {if(!abandoned)await cleanup();if (this.host&&(!this.host.usable() || this.host.memoryBytes() >= 24 * 1024 * 1024)) this.host = null;}
  }
  chargeMetrics(sample){this.nativeCpuMs=(this.nativeCpuMs??0)+Math.max(0,(sample.elapsed_ms??0)-(sample.io_wait_ms??0));this.usedOperations=(this.usedOperations??0)+sample.operations;this.usedBytes=(this.usedBytes??0)+sample.transferred_bytes;try{this.onMetrics?.(sample);}catch{}}
  setReadHints(namespace,generation,hints){
    const accepted=normalizeReadHints(namespace,generation,hints);
    if(!accepted||this.readHints&&BigInt(accepted.version)<BigInt(this.readHints.version))return false;
    this.boundRows?.(this);this.readHints=structuredClone(accepted);this.hintBytes=accepted.charge;
    while(this.rows.size&&this.rowBytes+this.hintBytes>512*1024){const oldest=this.rows.keys().next().value;this.rowBytes-=this.rows.get(oldest).bytes;this.rows.delete(oldest);}
    return true;
  }
  remember(key,value){
    this.boundRows?.(this);
    const previous=this.rows.get(key);if(previous)this.rowBytes-=previous.bytes;this.rows.delete(key);
    const bytes=utf8.encode(key).length+(value===undefined?0:utf8.encode(encode(value)).length)+128;
    if(bytes>512*1024-this.hintBytes)return;
    while(this.rows.size>=256||this.rowBytes+this.hintBytes+bytes>512*1024){const oldest=this.rows.keys().next().value;this.rowBytes-=this.rows.get(oldest).bytes;this.rows.delete(oldest);}
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
  async readOnly(run,{traceId,traceIds=[],onAdmission,priority="interactive",background,deadlineMs:outerDeadlineMs=Infinity,readKeys,readPrefix}={}){
    if(this.ownerReady)await this.ownerReady;
    if(this.snapshotReaders>=8)throw Error("ReadSnapshotNeedsAdmission");
    const owned=this.acquired,captured=structuredClone(owned?this.state:(this.observedState?.next>0?this.observedState:null)),physical={},rows=new Map(),deadlineMs=Math.min(this.clock()+5000,outerDeadlineMs),started=performance.now();let outcome="error",usedOperations=0,usedBytes=0,admissionBudgetExceeded=false,capsuleMetrics={};
    const reserve=(operations,bytes)=>{if(usedOperations+operations>=254||usedBytes+bytes>=8*1024*1024){admissionBudgetExceeded=true;throw Error("ReadSnapshotNeedsAdmission");}usedOperations+=operations;usedBytes+=bytes;};
    const cached=this.cache.wrap(this.remoteCache(instrumentTransport(this.rawTransport,physical),{background,budgetKey:()=>physical,onMetrics:sample=>{for(const[key,value]of Object.entries(sample))physical[key]=(physical[key]??0)+value;}}),`coordinator/${this.namespace}`);
    const raw=(request,body)=>{onAdmission?.();return cached(request,body);};
    const guarded=async(request,body)=>{if(request.op!=="get")throw Error("ReadSnapshotWriteForbidden");return raw(request,body);};
    let transport,proofTransport=guarded;
    if(captured&&this.base){
      proofTransport=warmRecoveryReadAhead(guarded,{base:this.base,cursor:captured,reserve});
    }
    // An authenticated warm cursor identifies the three initial proof keys.
    // Fetch them in one wave; native recovery still selects and validates the
    // checkpoint/epoch, and unused speculation remains charged to this call.
    transport=owned&&captured&&this.base?proofTransport:recoveryReadAhead(proofTransport,{window:this.recoveryWindow||1,initialWindow:1,reserve,cursor:captured,followSeals:!owned});
    const native=(state,coordinate)=>{
      if(usedOperations>=254||usedBytes>=8*1024*1024)throw Error("ReadSnapshotNeedsAdmission");
      return this.snapshotKernel.request({op:"coordinate",working_set_bytes:4*1024*1024,bucket:"database",prefix:this.prefix,namespace:this.namespace,coordinate:{owner:coordinate.action==="read"?state.owner:this.owner,state,...coordinate}},{transport,fairKey:this.namespace,priority,onStage:sample=>{if(sample.name==="kernel_queue")onAdmission?.();},deadlineMs,maxOperations:254-usedOperations,maxTransferBytes:8*1024*1024-usedBytes,onMetrics:sample=>{usedOperations+=sample.operations;usedBytes+=sample.transferred_bytes;try{this.onMetrics?.({event:"antfly.coordinator_snapshot_native",trace_id:traceId,trace_ids:traceIds,action:coordinate.action,...sample});}catch{}}});
    };
    this.snapshotReaders++;
    try{
      let observed,state=captured;
      for(let attempt=0;attempt<4;attempt++){
        observed=await native(state,readKeys?{action:"observe_read",observe_owned:owned,commands:readKeys.map(key=>({kind:"get",key})),read_prefix:readPrefix??null}:{action:owned?"observe":"observe_committed"});state=observed.state;
        if(!observed.has_more)break;
        // A sealed epoch may lack START after a partial hint upload. An
        // unowned observer cannot authenticate that continuation independently.
        if(!owned&&state.next===0)throw Error("ReadSnapshotNeedsAdmission");
      }
      if(observed.has_more)throw Error("ReadSnapshotNeedsAdmission");
      if(readKeys){
        if(!Array.isArray(observed.results)||observed.results.length!==readKeys.length)throw Error("InvalidCoordinatorReadProjection");
        readKeys.forEach((key,i)=>rows.set(key,observed.results[i][0]?decode(observed.results[i][0].value):undefined));
        if(observed.selected){if(!readPrefix||!observed.selected.key.startsWith(readPrefix))throw Error("InvalidCoordinatorReadProjection");rows.set(observed.selected.key,decode(observed.selected.value));}
      }
      capsuleMetrics=rootMetrics(observed.state);
      if(!this.observedState||observed.state.revision>=this.observedState.revision)this.observedState=structuredClone(observed.state);
      this.base??=observed.base;this.pagePrefix??=observed.page_prefix;
      const tx={getMany:async names=>{
        // A decoded row is reusable only after a fresh matching observation.
        if(this.rowRevision===observed.state.revision&&this.state?.revision===observed.state.revision&&hex(this.state.epoch)===hex(observed.state.epoch)&&hex(this.state.owner)===hex(observed.state.owner)){
          for(const name of names)if(!rows.has(name)&&this.rows.has(name))rows.set(name,structuredClone(this.rows.get(name).value));
        }
        const missing=[...new Set(names.filter(name=>!rows.has(name)))];
        if(missing.length){const result=await native(observed.state,{action:"read",commands:missing.map(key=>({kind:"get",key}))});missing.forEach((key,i)=>rows.set(key,result.results[i][0]?decode(result.results[i][0].value):undefined));}
        return names.map(key=>structuredClone(rows.get(key)));
      },get:async key=>(await tx.getMany([key]))[0],put:()=>{throw Error("ReadSnapshotNeedsPin");}};
      const result=await run(tx);outcome="ok";return result;
    }catch(error){
      if(admissionBudgetExceeded||["ObjectIoBudgetExceeded","HostedWorkingSetBudgetExceeded"].includes(error?.message)){outcome="needs_admission";throw Error("ReadSnapshotNeedsAdmission");}
      throw error;
    }finally{await proofTransport.drain?.();this.snapshotReaders--;const expired=outcome==="ok"&&this.clock()>=deadlineMs;if(expired)outcome="error";try{this.onMetrics?.({event:"antfly.coordinator_snapshot",trace_id:traceId,trace_ids:traceIds,outcome,owned,...capsuleMetrics,elapsed_ms:performance.now()-started,operations:usedOperations,transferred_bytes:usedBytes,physical_r2:physical});}catch{}if(expired)throw Error("DeadlineExceeded");}
  }
  commitWal(commit,{deadlineMs=Infinity,traceId,nativePlan=false,ioBudget}={}){
    return this.enqueue({kind:"commit_wal",commit,deadlineMs,traceId,nativePlan,ioBudget,priority:"interactive"});
  }
  transaction(run,{priority="interactive",traceId,deadlineMs=Infinity,ioBudget}={}) {
    return this.enqueue({run,priority,traceId,deadlineMs,ioBudget});
  }
  enqueue(input) {
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
    const started=performance.now();this.trace=new CriticalPath({id:this.pending[0]?.traceId,scope:"coordinator"});this.deadline = this.clock()+30000;
    const ioBudget=this.pending[0]?.ioBudget,creditedOperations=ioBudget?.operations??0,creditedBytes=ioBudget?.bytes??0;
    this.physical={};this.usedOperations=creditedOperations; this.usedBytes=creditedBytes;this.nativeCpuMs=0;this.prefetchReservedBytes=0;let beginMs=0,commitMs=0,maxQueueMs=0;
    // The selected owner remains first until consumed. Arrivals during begin
    // cannot defer its callback and strand a consumed batch permit.
    this.groupPriority=this.pending[0]?.priority;
    const completed=[];
    try {
      if(this.ioDrain)throw new Error("CoordinatorRecovering");
      if(Number.isFinite(this.pending[0]?.deadlineMs)){
        this.deadline=Math.min(this.deadline,this.pending[0].deadlineMs);
        if(this.clock()>=this.deadline){completed.push(this.pending.shift());throw new Error("DeadlineExceeded");}
      }
      // Commit-only native plans expose no read result before append. A warm
      // retained root is authenticated by append's before/after floor and seal
      // checks, then linearized by its immutable conditional journal slot.
      // Stale slots fail closed and force ordinary recovery before a retry.
      const warmCommit=this.pending[0]?.kind==="commit_wal"&&this.pending[0].nativePlan&&this.acquired&&!this.recovering&&this.state?.next<64;
      const recovered = warmCommit?{state:this.state,base:this.base,page_prefix:this.pagePrefix,has_more:false}:await this.trace.span("recovery",()=>this.native({action: "begin"}));
      beginMs=performance.now()-started;
      if(this.acquired&&hex(recovered.state.owner)!==hex(this.owner))throw new Error("ImmutableLogFenced");
      this.state = recovered.state; this.base = recovered.base; this.pagePrefix = recovered.page_prefix;
      this.recovering=Boolean(recovered.has_more);
      if (recovered.has_more) throw new Error("CoordinatorRecovering");
      this.acquired=true;
      if(this.rowRevision!==this.state.revision){this.rows.clear();this.rowBytes=0;this.rowRevision=this.state.revision;}
      if(this.pending[0]?.kind==="commit_wal"){
        const item=this.pending.shift();completed.push(item);
        maxQueueMs=performance.now()-item.queuedAt;
        if(this.clock()>=item.deadlineMs)throw new Error("DeadlineExceeded");this.deadline=Math.min(this.deadline,item.deadlineMs);
        const commitStarted=performance.now();
        const committed=await this.trace.span("wal_commit",()=>this.native({action:"commit_wal",wal_commit:item.commit}));
        commitMs=performance.now()-commitStarted;
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
        for(const [key,value]of overlay)this.remember(key,value);
        this.rowRevision=this.state.revision;
      }
    } catch(error) {
      if(completed.some(item=>item.nativePlan)){this.acquired=false;this.rows.clear();this.rowBytes=0;this.rowRevision=null;}
      for(const item of completed)item.batchError=error;
      // Recovery advances a bounded cursor before returning backpressure.
      // Reject queued callers rather than retaining an unbounded request set.
      if (!completed.length)for(const item of this.pending.splice(0,ioBudget?1:16))completed.push({...item,batchError:error});
    } finally {
      if(ioBudget){ioBudget.operations=this.usedOperations;ioBudget.bytes=this.usedBytes;}
      try{this.onMetrics?.({event:"antfly.coordinator_group",operations:this.usedOperations-creditedOperations,transferred_bytes:this.usedBytes-creditedBytes,elapsed_ms:performance.now()-started,physical_r2:{...this.physical},trace:this.trace.report(),prefetch_reserved_bytes:this.prefetchReservedBytes,begin_ms:beginMs,commit_ms:commitMs,native_cpu_ms:this.nativeCpuMs,queue_wait_max_ms:maxQueueMs,row_cache_bytes:this.rowBytes,callbacks:completed.length,trace_ids:completed.map(item=>item.traceId).filter(Boolean)});}catch{}
      this.deadline = null;
      this.idleCheckpoint?.schedule();
    }
    return completed;
  }
}
