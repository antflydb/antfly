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
import {IdleCheckpoint} from "./idle-checkpoint.mjs";
import {instrumentTransport} from "./performance.mjs";
import {instantiateHost} from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import {createR2Transport} from "../../zig/lib/objectstore/wasm/r2.mjs";
import {createImmutableReadCache} from "../../zig/lib/objectstore/wasm/cache.mjs";
const utf8 = new TextEncoder();
const hex=bytes=>Array.from(typeof bytes==="string"?utf8.encode(bytes):bytes,n=>n.toString(16).padStart(2,"0")).join("");
const compare = (l, r) => {const a = utf8.encode(l), b = utf8.encode(r); for (let i=0; i<Math.min(a.length,b.length); i++) if (a[i]!==b[i]) return a[i]-b[i]; return a.length-b.length;};
export const encode = value => JSON.stringify(value, (_, v) => v instanceof Uint8Array || v instanceof ArrayBuffer ? {__antfly_bytes: Array.from(new Uint8Array(v.buffer ?? v, v.byteOffset ?? 0, v.byteLength))} : v);
const decode = value => JSON.parse(value, (_, v) => v && Object.keys(v).length === 1 && Array.isArray(v.__antfly_bytes) ? Uint8Array.from(v.__antfly_bytes) : v);
export class R2CoordinatorStore {
  constructor(module, bucket, namespace, {prefix = "antfly", clock = Date.now, onMetrics, owner, transport,kernel,cache,boundRows,background,checkpointCommits=64} = {}) {
    this.module = module; this.bucket = bucket; this.namespace = namespace; this.prefix = prefix; this.clock = clock; this.onMetrics = onMetrics;
    this.kernel=kernel;this.boundRows=boundRows;this.checkpointCommits=checkpointCommits;
    this.owner = owner ?? Array.from(crypto.getRandomValues(new Uint8Array(16)));
    this.cache = cache??createImmutableReadCache({includeCommitLog:true});
    const raw=transport??createR2Transport({database:bucket},{maxPutBytes:4*1024*1024});
    this.transport = this.cache.wrap(async(request,body)=>instrumentTransport(raw,this.physical??={})(request,body), `coordinator/${namespace}`);
    this.pending = []; this.running = false; this.state = null;
    this.rows=new Map();this.rowBytes=0;this.rowRevision=null;this.acquired=false;
    if(background)this.idleCheckpoint=new IdleCheckpoint({revision:()=>this.state?.revision??0,isIdle:()=>!this.running&&!this.pending.length,background,onError:error=>{try{this.onMetrics?.({event:"antfly.checkpoint_deferred",error:String(error)});}catch{}},checkpoint:()=>this.transaction(async tx=>{if(this.pending.length)return{busy:true};const revision=tx.snapshot().revision;return{done:await tx.checkpoint(),revision};},{priority:"maintenance"})});
  }
  async native(coordinate) {
    if(!this.kernel)this.host ??= await instantiateHost(this.module, this.transport, {clock:this.clock,onMetrics:sample=>this.chargeMetrics(sample)});
    let transport=this.transport;
    if(coordinate.action==="begin"&&this.state&&this.base){
      // Prefetch only within this recovery request. Negative results are never
      // retained, and append's before/after floor checks use the base transport.
      this.usedOperations=(this.usedOperations??0)+2;this.usedBytes=(this.usedBytes??0)+34816;this.prefetchReservedBytes=(this.prefetchReservedBytes??0)+34816;
      const epoch=hex(this.state.epoch);
      const checkpoint=this.base+"/CHECKPOINT",keys=[checkpoint,`${this.base}/epochs/${epoch}/SEALED`,`${this.base}/epochs/${epoch}/${String(this.state.next).padStart(20,"0")}.json`];
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
      return await (this.kernel??this.host).request({op: "coordinate", bucket: "database", prefix: this.prefix, namespace: this.namespace, coordinate: {checkpoint_commits:this.checkpointCommits,owner: this.owner, state: this.state, ...coordinate}}, {transport,onMetrics:sample=>this.chargeMetrics({...sample,action:coordinate.action}),deadlineMs: this.deadline ?? this.clock() + 30000,maxOperations:1024-(this.usedOperations??0),maxTransferBytes:32*1024*1024-(this.usedBytes??0)});
    } finally {if (this.host&&(!this.host.usable() || this.host.memoryBytes() >= 24 * 1024 * 1024)) this.host = null;}
  }
  chargeMetrics(sample){this.nativeCpuMs=(this.nativeCpuMs??0)+Math.max(0,(sample.elapsed_ms??0)-(sample.io_wait_ms??0));this.usedOperations=(this.usedOperations??0)+sample.operations;this.usedBytes=(this.usedBytes??0)+sample.transferred_bytes;try{this.onMetrics?.(sample);}catch{}}
  remember(key,value){
    this.boundRows?.(this);
    const previous=this.rows.get(key);if(previous)this.rowBytes-=previous.bytes;this.rows.delete(key);
    const bytes=utf8.encode(key).length+(value===undefined?0:utf8.encode(encode(value)).length)+128;
    if(bytes>512*1024)return;
    while(this.rows.size>=256||this.rowBytes+bytes>512*1024){const oldest=this.rows.keys().next().value;this.rowBytes-=this.rows.get(oldest).bytes;this.rows.delete(oldest);}
    this.rows.set(key,{value:structuredClone(value),bytes});this.rowBytes+=bytes;
  }
  transaction(run,{priority="interactive"}={}) {
    if (this.pending.length >= 64) return Promise.reject(new Error("CoordinatorCommitBusy"));
    return new Promise((resolve,reject) => {
      this.pending.push({run,resolve,reject,priority,queuedAt:performance.now()});
      if (!this.running) {this.running=true; queueMicrotask(()=>this.drain());}
    });
  }
  async drain() {
    while (this.pending.length) await this.batch();
    this.running=false;
    this.idleCheckpoint?.schedule();
  }
  async batch() {
    const started=performance.now();this.deadline = this.clock()+30000;
    this.physical={};this.usedOperations=0; this.usedBytes=0;this.nativeCpuMs=0;this.prefetchReservedBytes=0;let beginMs=0,commitMs=0,maxQueueMs=0;
    // Maintenance becomes equally eligible after five seconds of waiting;
    // other work prefers interactive callers at group boundaries.
    this.pending.sort((a,b)=>Number(a.priority!=="interactive"&&started-a.queuedAt<5000)-Number(b.priority!=="interactive"&&started-b.queuedAt<5000));
    const completed=[];
    try {
      const recovered = await this.native({action: "begin"});
      beginMs=performance.now()-started;
      if(this.acquired&&hex(recovered.state.owner)!==hex(this.owner))throw new Error("ImmutableLogFenced");
      this.state = recovered.state; this.base = recovered.base; this.pagePrefix = recovered.page_prefix;
      if (recovered.has_more) throw new Error("CoordinatorRecovering");
      this.acquired=true;
      if(this.rowRevision!==this.state.revision){this.rows.clear();this.rowBytes=0;this.rowRevision=this.state.revision;}
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
        snapshot: () => structuredClone(this.state),
        checkpoint: async () => (await this.native({action: "checkpoint"})).checkpointed,
        retirableLogs: async (keys, before) => (await this.native({action: "retire_logs", keys, before})).keys,
        trace: async page => this.native({action: "trace", page}),
      };
      // Independent RPCs serialize through one overlay and share a durable
      // commit. Every result waits for that commit. A failing callback rolls
      // back only its own writes; reads return copies to isolate mutations.
      for (let count=0;count<16&&this.pending.length&&overlay.size<=80&&(count===0||performance.now()-started<5000);count++) {
        const queuedNow=performance.now();this.pending.sort((a,b)=>Number(a.priority!=="interactive"&&queuedNow-a.queuedAt<5000)-Number(b.priority!=="interactive"&&queuedNow-b.queuedAt<5000));
        const item=this.pending.shift(), previous=new Map(overlay);
        maxQueueMs=Math.max(maxQueueMs,performance.now()-item.queuedAt);
        try {
          const result=await item.run(tx);
          if (overlay.size>512) throw new Error("CoordinatorCommitBusy");
          completed.push({...item,result:structuredClone(result)});
        } catch(error) {
          if(/IntegrityMismatch/.test(String(error))){this.cache.clear();this.rows.clear();this.rowBytes=0;}
          overlay.clear(); for(const [key,value] of previous) overlay.set(key,value);
          item.reject(error);
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
      for(const item of completed) item.resolve(item.result);
    } catch(error) {
      for(const item of completed) item.reject(error);
      // Recovery advances a bounded cursor before returning backpressure.
      // Reject queued callers rather than retaining an unbounded request set.
      if (!completed.length) for(const item of this.pending.splice(0,16)) item.reject(error);
    } finally {
      try{this.onMetrics?.({event:"antfly.coordinator_group",operations:this.usedOperations,transferred_bytes:this.usedBytes,elapsed_ms:performance.now()-started,physical_r2:{...this.physical},prefetch_reserved_bytes:this.prefetchReservedBytes,begin_ms:beginMs,commit_ms:commitMs,native_cpu_ms:this.nativeCpuMs,queue_wait_max_ms:maxQueueMs,row_cache_bytes:this.rowBytes,callbacks:completed.length});}catch{}
      this.deadline = null;
    }
  }
}
