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

import{ObjectStoreError}from'./r2.mjs';
const encoder=new TextEncoder();
const allowed=new Set(['op','bucket','key','range','if_match_etag','max_response_bytes','version_id']);
// Native discovery produces immutable descriptors, never authority or JS guesses.
export class ReadGraph {
 constructor({deadlineMs,signal,onMetrics=()=>{},reserve,budget,fairKey}){if(!Number.isFinite(deadlineMs)||typeof reserve!=="function"||typeof fairKey!=="string"||!fairKey.length||fairKey.length>4096||typeof budget?.acquire!=="function")throw Error("InvalidReadGraph");Object.assign(this,{deadlineMs,signal,onMetrics,reserve,budget,fairKey});this.values=new Map();this.pending=new Map();this.bytes=0;this.steps=0;this.operations=0;this.transferred=0;this.releases=[];}
 check(){if(this.signal?.aborted||Date.now()>=this.deadlineMs)throw Error('DeadlineExceeded');}
 wrap(transport,{enabled=true}={}){return async(request,body)=>{
  this.check();if(!['get','head','bucket_exists','read_view_barrier'].includes(request.op))throw Error('PointReadWriteForbidden');if(request.op==='read_view_barrier')return{};
  if(!enabled||this.disabled||request.op!=='get'||request.bucket!=='database'||request.version_id!=null||request.if_match_etag!=null||Object.keys(request).some(k=>!allowed.has(k))||!(/\/artifacts\/(?:sha256\/[a-f0-9]{2}\/[a-f0-9]{62}|graph\/[a-f0-9]{64}\/[a-f0-9]{32}\/[a-f0-9]{64})$|\/manifests\/[^/]+\/manifests\/[0-9]+\.bin$|\/wal-v1\/[a-f0-9]{64}\/pages\/(?:[a-f0-9]{32}\/)?[a-f0-9]{64}$/.test(request.key??'')))return transport(request,body);
  const key=JSON.stringify(request),prior=this.values.get(key);if(prior){this.onMetrics({point_dependency_replays:1});if(prior.error)throw prior.error;return{json:structuredClone(prior.json),body:prior.body.slice()};}
  if(this.pending.has(key))throw new ObjectStoreError(10,'immutable read graph');
  const max=request.max_response_bytes,descriptor=encoder.encode(key).byteLength,charge=max*2+8192+descriptor*2+512;
  if(!Number.isSafeInteger(max)||max<1||max+4096>this.budget.maxBytes||descriptor>4096||this.bytes+charge>1024*1024||this.steps+this.pending.size>=64){
   if(this.pending.size)throw new ObjectStoreError(10,'immutable read graph wave full');
   this.clear();this.disabled=true;this.onMetrics({point_dependency_fallbacks:1});return transport(request,body);
  }
  const release=this.reserve(charge);this.releases.push(release);this.bytes+=charge;this.pending.set(key,{key,request:structuredClone(request),transport,charge});throw new ObjectStoreError(10,'immutable read graph');
 };}
 async load(){
  this.check();const wave=[...this.pending.values()];this.pending.clear();if(!wave.length)throw Error('InvalidReadDependency');this.onMetrics({point_graph_waves:1,point_graph_nodes:wave.length});
  let next=0,failure,lateFailure;const tickets=new Set();
  const worker=async()=>{while(!failure&&next<wave.length){const node=wave[next++];let ticket,release;
   try{this.check();ticket=this.budget.acquire(this.fairKey,node.request.max_response_bytes+4096);tickets.add(ticket);release=await ticket.ready;if(failure)throw failure;ticket.started=true;this.check();
    if(++this.operations>=1024||this.transferred+node.charge>=32*1024*1024)throw Error('ObjectIoBudgetExceeded');this.steps++;this.transferred+=node.charge;
    this.onMetrics({point_dependency_gets:1,point_dependency_reserved_bytes:node.charge});
    try{const value=await node.transport(node.request,new Uint8Array()),bytes=value.body??new Uint8Array();if(!(bytes instanceof Uint8Array)||bytes.length>node.request.max_response_bytes||encoder.encode(JSON.stringify(value.json??{})).byteLength>4096)throw Error('ObjectIoBudgetExceeded');this.values.set(node.key,{json:structuredClone(value.json??{}),body:bytes.slice()});}
    catch(error){if(error instanceof ObjectStoreError&&error.status===1)this.values.set(node.key,{error});else throw error;}
   }catch(error){if(failure&&error!==failure&&!["ReadAheadCanceled","DeadlineExceeded"].includes(error.message)){lateFailure??=error;this.onMetrics({point_graph_secondary_error:String(error.message??error)});}failure??=error;}finally{if(release)release();else ticket?.cancel();if(ticket)tickets.delete(ticket);}
  }};
  const finished=Promise.all(Array.from({length:Math.min(4,wave.length)},worker)).then(()=>{if(failure)throw failure.message==="DeadlineExceeded"?(lateFailure??failure):failure;});finished.catch(()=>{});let timer,cancel,settled=false;const settlement=finished.finally(()=>settled=true);settlement.catch(()=>{});
  const deadline=new Promise((_,reject)=>{cancel=()=>{failure??=Error('DeadlineExceeded');for(const t of tickets)if(!t.started)t.cancel();reject(failure);};timer=setTimeout(cancel,Math.max(0,this.deadlineMs-Date.now()));this.signal?.addEventListener('abort',cancel,{once:true});if(this.signal?.aborted)cancel();});
  try{await Promise.race([settlement,deadline]);this.check();}catch(error){if(!settled)Object.defineProperty(error,'ioSettled',{value:settlement});throw error;}finally{clearTimeout(timer);this.signal?.removeEventListener('abort',cancel);}
 }
 clear(){this.values.clear();this.pending.clear();for(const release of this.releases)release();this.releases=[];this.bytes=0;}
}
