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

import {ObjectStoreError} from './r2.mjs';
import {proofPrefetchBytes,ReadAheadBudget} from './read-ahead.mjs';
// These are the existing point-read lanes, independently bounded from
// metadata proof I/O. Preparation leaves one lane available for admission.
const pointBudgets=new WeakMap();
export function pointDependencyBudgetFor(kernel){
 let budget=pointBudgets.get(kernel);
 if(!budget){budget=new ReadAheadBudget(Math.max(1,Math.min(4,kernel.pointDependencySlots??kernel.slots.length)),{reserveQueued:typeof kernel.reserveQueuedAdmission==='function'?()=>kernel.reserveQueuedAdmission():undefined});pointBudgets.set(kernel,budget);}
 return budget;
}
export function pointDependencyMetrics(kernel){
 const b=pointBudgets.get(kernel);
 return{point_dependency_active:b?.active??0,point_dependency_queued:b?.pending.length??0,point_dependency_bytes:b?.bytes??0,point_dependency_peak_bytes:b?.peakBytes??0,point_dependency_limit_bytes:b?b.limit*proofPrefetchBytes:0};
}
const encoder=new TextEncoder();
const replayFields=new Set(["op","bucket","key","range","if_match_etag","max_response_bytes","version_id"]);
// A bounded, request-owned immutable dependency transcript. Native execution
// unwinds before external I/O; resumption revalidates the original read view.
// No authority, absence inference, live session or native pointer is retained.
export class ReadDependencies {
 constructor({maxBytes=proofPrefetchBytes,maxSteps=64,deadlineMs,signal,onMetrics=()=>{}}={}){
  if(!Number.isSafeInteger(maxBytes)||maxBytes<4096||maxBytes>proofPrefetchBytes||!Number.isSafeInteger(maxSteps)||maxSteps<1||maxSteps>64||!Number.isFinite(deadlineMs))throw Error('InvalidReadDependencies');
  Object.assign(this,{maxBytes,maxSteps,deadlineMs,signal,onMetrics});this.values=new Map();this.bytes=0;this.steps=0;this.operations=0;this.transferred=0;
 }
 check(){if(this.signal?.aborted||Date.now()>=this.deadlineMs)throw Error('DeadlineExceeded');}
 wrap(transport,{enabled=true}={}){return async(request,body)=>{
  this.check();
  if(!['get','head','bucket_exists','read_view_barrier'].includes(request.op))throw Error('PointReadWriteForbidden');
  if(request.op==='read_view_barrier')return{}; // admission already completed
  const immutable=Object.keys(request).every(key=>replayFields.has(key))&&request.bucket==='database'&&request.op==='get'&&request.version_id==null&&request.if_match_etag==null&&(/\/artifacts\/(?:sha256\/[a-f0-9]{2}\/[a-f0-9]{62}|graph\/[a-f0-9]{64}\/[a-f0-9]{32}\/[a-f0-9]{64})$/.test(request.key??'')||/\/manifests\/[^/]+\/manifests\/[0-9]+\.bin$/.test(request.key??'')||/\/wal-v1\/[a-f0-9]{64}\/pages\/(?:[a-f0-9]{32}\/)?[a-f0-9]{64}$/.test(request.key??''));
  if(!enabled||!immutable||this.disabled)return transport(request,body);
  const key=JSON.stringify(request),prior=this.values.get(key);
  if(prior){this.onMetrics({point_dependency_replays:1});if(prior.error)throw prior.error;return{json:structuredClone(prior.value.json),body:prior.value.body.slice()};}
  const maximum=request.max_response_bytes;
  const descriptorBytes=encoder.encode(key).byteLength;
  const charge=Number.isSafeInteger(maximum)&&maximum>0&&descriptorBytes<=4096?maximum*2+8192+descriptorBytes*2+512:Infinity;
  if(this.steps>=this.maxSteps||charge>this.maxBytes-this.bytes){this.clear();this.disabled=true;this.onMetrics({point_dependency_fallbacks:1});return transport(request,body);}
  if(this.pending&&this.pending.key!==key)throw Error('InvalidReadDependency');
  this.pending={key,request:structuredClone(request),transport,charge};
  throw new ObjectStoreError(10,'immutable read dependency');
 };}
 async load(){
  this.check();const pending=this.pending;if(!pending)throw Error('InvalidReadDependency');this.pending=null;
  const {key,request,transport,charge}=pending;this.steps++;this.bytes+=charge;
  if(++this.operations>=1024||this.transferred+charge>=32*1024*1024)throw Error('ObjectIoBudgetExceeded');this.transferred+=charge;
  this.onMetrics({point_dependency_gets:1,point_dependency_reserved_bytes:charge});
  // This task belongs to the caller, including after logical cancellation.
  const task=Promise.resolve().then(()=>transport(request,new Uint8Array())).then(value=>{
   const json=structuredClone(value.json??{}),body=value.body??new Uint8Array();
   if(!(body instanceof Uint8Array))throw Error('InvalidReadDependency');
   if(body.byteLength>request.max_response_bytes||encoder.encode(JSON.stringify(json)).byteLength>4096)throw Error('ObjectIoBudgetExceeded');
   this.values.set(key,{value:{json,body:body.slice()}});
  },error=>{if(error instanceof ObjectStoreError&&error.status===1){this.values.set(key,{error});return;}throw error;});
  let timer,cancel,settled=false;const finished=task.finally(()=>{settled=true;});finished.catch(()=>{});
  const deadline=new Promise((_,reject)=>{cancel=()=>reject(Error('DeadlineExceeded'));timer=setTimeout(cancel,Math.max(0,this.deadlineMs-Date.now()));this.signal?.addEventListener('abort',cancel,{once:true});if(this.signal?.aborted)cancel();});
  try{await Promise.race([finished,deadline]);this.check();}
  catch(error){if(!settled)Object.defineProperty(error,'ioSettled',{value:finished});throw error;}
  finally{clearTimeout(timer);this.signal?.removeEventListener('abort',cancel);}
 }
 clear(){this.values.clear();this.pending=null;this.bytes=0;}
}
