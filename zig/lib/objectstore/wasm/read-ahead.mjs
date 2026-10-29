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

// Speculation is scoped to one native recovery call. Only a contiguous
// immutable commit prefix can cover a negative seal probe. At the first hole,
// error or window boundary the native engine observes a fresh SEALED record.
import {ObjectStoreError} from "./r2.mjs";
const decoder=new TextDecoder(),encoder=new TextEncoder();
const hex=value=>{const bytes=typeof value==="string"?encoder.encode(value):value;if(!bytes||bytes.length!==16||!Array.from(bytes).every(n=>Number.isInteger(n)&&n>=0&&n<=255))return null;return Array.from(bytes,n=>n.toString(16).padStart(2,"0")).join("");};
const state=value=>value&&hex(value.epoch)&&hex(value.owner)&&Number.isSafeInteger(value.next)&&value.next>=0&&value.next<=64&&Number.isSafeInteger(value.revision)&&value.revision>=0?value:null;
const parse=result=>{try{return JSON.parse(decoder.decode(result.body));}catch{return null;}};

// A warm reader knows the previous proof keys. If CHECKPOINT advances while
// their probes are in flight, start its new proof keys immediately. Native
// recovery remains the authority; all obsolete I/O settles before returning.
// Five bounded responses: checkpoint, prior proof pair and an optional new
// checkpoint's proof pair. This is request-owned space outside WASM memory.
export const proofPrefetchBytes = 5 * (16384 + 4096);
export class ReadAheadBudget {
 constructor(maxInFlight=4,{reserveQueued}={}){
  if(!Number.isInteger(maxInFlight)||maxInFlight<1||maxInFlight>4)throw Error('InvalidReadAheadBudget');
  this.limit=maxInFlight;this.active=0;this.bytes=0;this.peakBytes=0;this.started=0;
  this.activeKeys=new Set();this.pending=[];this.lastKey=null;this.reserveQueued=reserveQueued;
 }
 tryAcquire(key=Symbol('unkeyed proof')){
  if(this.active>=this.limit||this.activeKeys.has(key))return null;
  this.active++;this.bytes+=proofPrefetchBytes;this.peakBytes=Math.max(this.peakBytes,this.bytes);this.activeKeys.add(key);this.lastKey=key;
  let held=true,started=false;
  const release=()=>{if(held){held=false;this.active--;this.bytes-=proofPrefetchBytes;this.activeKeys.delete(key);this.drain();}};
  release.start=()=>{if(held&&!started){started=true;this.started++;}};
  return release;
 }
 acquire(key){
  if(typeof key!=='string'||key.length===0||key.length>4096)throw Error('InvalidReadAheadKey');
  this.drain();
  const immediate=this.tryAcquire(key);
  if(immediate)return{ready:Promise.resolve(immediate),cancel:immediate};
  if(this.pending.length>=32)return{ready:Promise.reject(Error('ReadAheadCapacityExceeded')),cancel(){}};
  const queueRelease=this.reserveQueued?.();
  if(this.reserveQueued&&!queueRelease)return{ready:Promise.reject(Error('ReadAheadCapacityExceeded')),cancel(){}};
  let waiter,release,stopped=false;
  const ready=new Promise((resolve,reject)=>{
   waiter={key,reject,queueRelease,admit:value=>{release=value;if(stopped)value();else resolve(value);}};
   this.pending.push(waiter);this.drain();
  });
  return{ready,cancel:()=>{
   if(stopped)return;stopped=true;
   const index=this.pending.indexOf(waiter);
   if(index>=0){this.pending.splice(index,1);waiter.queueRelease?.();waiter.reject(Error('ReadAheadCanceled'));this.drain();}
   release?.();
  }};
 }
 drain(){
  while(this.active<this.limit){
   const eligible=waiter=>!this.activeKeys.has(waiter.key);
   let index=this.pending.findIndex(waiter=>eligible(waiter)&&waiter.key!==this.lastKey);
   if(index<0)index=this.pending.findIndex(eligible);
   if(index<0)return;
   const waiter=this.pending.splice(index,1)[0];waiter.queueRelease?.();const release=this.tryAcquire(waiter.key);
   // Resolving admission never starts another caller's storage work here.
   waiter.admit(release);
  }
 }
}

const budgets=new WeakMap();
export function readAheadBudgetFor(owner,maxInFlight=4){
 let budget=budgets.get(owner);
 if(!budget){budget=new ReadAheadBudget(maxInFlight,{reserveQueued:typeof owner.reserveQueuedAdmission==='function'?()=>owner.reserveQueuedAdmission():undefined});budgets.set(owner,budget);}
 return budget;
}
export function readAheadMetrics(owner){
 const budget=budgets.get(owner);
 return{proof_prefetch_active:budget?.active??0,proof_prefetch_queued:budget?.pending.length??0,proof_prefetch_bytes:budget?.bytes??0,proof_prefetch_peak_bytes:budget?.peakBytes??0,proof_prefetch_started:budget?.started??0,proof_prefetch_limit_bytes:budget?budget.limit*proofPrefetchBytes:0};
}

export function warmRecoveryReadAhead(transport,{base,cursor,reserve=()=>{}}={}){
 const prior=state(cursor);if(typeof base!=='string'||!base.length||!prior)throw Error('InvalidRecoveryCursor');
 const keys=value=>[`${base}/epochs/${hex(value.epoch)}/SEALED`,`${base}/epochs/${hex(value.epoch)}/${String(value.next).padStart(20,'0')}.json`],checkpoint=base+'/CHECKPOINT';
 let primed=false,closed=false,ready;
 const pending=new Map(),outstanding=new Set();
 // Four speculative responses are charged in addition to the native proof.
 reserve(4,4*16384);
 const start=(request,key)=>{
  if(!pending.has(key)){
   const task=Promise.resolve().then(()=>transport({...request,key,max_response_bytes:16384},new Uint8Array())).then(value=>{
    const bodyBytes=value.body?.byteLength??0,headerBytes=encoder.encode(JSON.stringify(value.json??{})).byteLength;
    if(bodyBytes>16384||headerBytes>4096)throw new ObjectStoreError(4,'proof response exceeds its retained-input budget');
    return{value};
   }).catch(error=>({error}));
   pending.set(key,task);outstanding.add(task);task.then(()=>outstanding.delete(task));
  }
  return pending.get(key);
 };
 const begin=request=>{
  if(primed)return;
  if(closed)throw Error('ReadAheadClosed');
  primed=true;
  const floor=start(request,checkpoint);for(const key of keys(prior))start(request,key);
  ready=floor.then(result=>{
   if(closed)return;
   const fresh=state(result.value&&parse(result.value)?.state),selected=fresh&&fresh.revision>prior.revision?fresh:prior;
   return Promise.all(keys(selected).map(key=>start(request,key)));
  });
 };
 const read=async(request,body)=>{
  if(request.op!=='get')return transport(request,body);
  if(request.key===checkpoint&&!primed)begin(request);
  if(pending.has(request.key)){const result=await pending.get(request.key);pending.delete(request.key);if(result.error)throw result.error;return result.value;}
  return transport(request,body);
 };
 // Priming never interprets an observation as authority. The native engine
 // consumes and authenticates exactly these request-owned responses later.
 read.prime=request=>{begin(request);return ready;};
 read.drain=async()=>{
  closed=true;
  await ready;
  while(outstanding.size)await Promise.all([...outstanding]);
  pending.clear();
 };
 return read;
}
export function recoveryReadAhead(transport,{window=8,initialWindow=window,reserve=()=>{},cursor,followSeals=false}={}){
 if(!Number.isInteger(window)||window<1||window>16||!Number.isInteger(initialWindow)||initialWindow<1||initialWindow>window)throw new Error("InvalidRecoveryWindow");
 let current,base,slots=new Map(),covered=false,width=initialWindow;
 const followSeal=result=>{
  const frame=result?.value&&parse(result.value);
  if(!followSeals||!current||frame?.kind!=="seal"||!state(frame.state)||hex(frame.epoch)!==hex(current?.epoch)||hex(frame.state.epoch)===hex(current.epoch)||frame.state.next!==0||frame.parent_revision<current.revision||frame.state.revision!==frame.parent_revision+1)return;
  current=frame.state;slots.clear();covered=false;width=initialWindow;
 };
 const commit=result=>{const frame=result?.value&&parse(result.value);return frame?.kind==="commit"&&state(frame.state)&&hex(frame.epoch)===hex(current?.epoch)&&frame.slot===current.next&&frame.parent_revision===current.revision&&frame.state.revision===current.revision+1&&frame.state.next===current.next+1&&hex(frame.state.owner)===hex(current.owner)?frame:null;};
 return async(request,body)=>{
  if(request.op!=="get")return transport(request,body);
  if(request.key.endsWith("/CHECKPOINT")){
   const result=await transport(request,body),checkpoint=parse(result),floor=state(checkpoint?.state),hint=state(cursor);current=hint&&(!floor||hint.revision>floor.revision)?hint:floor;base=request.key.slice(0,-11);slots.clear();covered=false;width=initialWindow;return result;
  }
  const epoch=hex(current?.epoch),prefix=epoch&&`${base}/epochs/${epoch}/`,slotKey=prefix&&prefix+String(current.next).padStart(20,"0")+".json";
  if(prefix&&request.key===prefix+"SEALED"){
   if(covered&&commit(slots.get(slotKey)))throw new ObjectStoreError(1,"immutable prefix covers seal probe");
   if(!slots.has(slotKey)&&current.next<64){
    slots.clear();const keys=Array.from({length:Math.min(width,64-current.next)},(_,i)=>prefix+String(current.next+i).padStart(20,"0")+".json");
    reserve(keys.length,keys.length*16384);
    const settled=await Promise.all([request.key,...keys].map(key=>transport({...request,key,max_response_bytes:16384},new Uint8Array()).then(value=>({value}),error=>({error}))));
    for(let i=0;i<keys.length;i++)slots.set(keys[i],settled[i+1]);
    const seal=settled[0];covered=seal.error?.status===1;
    if(seal.error)throw seal.error;followSeal(seal);return seal.value;
   }
   // A hole or invalid frame cannot use an earlier negative seal observation.
   covered=false;const result=await transport(request,body);followSeal({value:result});return result;
  }
  if(request.key===slotKey&&slots.has(slotKey)){
   const result=slots.get(slotKey);slots.delete(slotKey);const next=commit(result);if(next){current=next.state;if(slots.size===0)width=Math.min(window,width*2);}else{covered=false;followSeal(result);}
   if(result.error)throw result.error;return result.value;
  }
  return transport(request,body);
 };
}
