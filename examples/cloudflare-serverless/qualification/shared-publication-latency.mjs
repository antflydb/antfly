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

// Paired native-WASM and native-R2-metadata experiment. Uniform local delays
// expose durable round trips; this is not a cloud SLO or a gate acceptance path.
import{readFile,writeFile,mkdir}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';import assert from'node:assert/strict';
import{Bucket}from'../test/object-fixture.mjs';import{instantiateHost}from'../../../zig/lib/objectstore/wasm/asyncify.mjs';import{createR2Transport}from'../../../zig/lib/objectstore/wasm/r2.mjs';import{withCommitCoordinator}from'../../../zig/lib/objectstore/wasm/coordinated.mjs';
import{R2CoordinatorStore}from'../r2-coordinator-store.mjs';import{metadataObjectCall}from'../metadata-object-call.mjs';import{publicationKeys,publicationSnapshot,publicationCall,publicationCoordinator}from'../publication-view.mjs';import{metadataKeys,captureReadView}from'../read-view.mjs';import{PreparedPublication,applyPublication}from'../prepared-publication.mjs';
const out=resolve('dist/qualification/shared-pipeline-lab'),delay=Number(process.argv.find(a=>a.startsWith('--delay='))?.slice(8)??0);if(!Number.isInteger(delay)||delay<0||delay>100)throw Error('InvalidPublicationDelay');await mkdir(out,{recursive:true});
const file=resolve(out,'publication-'+Date.now()+'.json'),report={schema:1,qualification:false,delay_ms:delay,terms:12000,cases:[],limitations:['Actual native modules and metadata protocol with an in-memory R2-shaped binding; not actual workerd or Cloudflare.','Same native request capacities and deadlines on both sides; delayed binding I/O includes preparation.']};
const previous=await readFile(resolve(out,'before/antfly-serverless.wasm')),current=await readFile(resolve('dist/antfly-serverless.wasm'));report.modules={previous:createHash('sha256').update(previous).digest('hex'),current:createHash('sha256').update(current).digest('hex')};
for(const preparedSlices of[false,true]){
 const bytes=preparedSlices?current:previous,module=await WebAssembly.compile(bytes),bucket=new Bucket(),raw=createR2Transport({database:bucket}),namespace='publication-comparison',keys=await publicationKeys(namespace,true),mk=await metadataKeys(namespace);let operations=0,gets=0,puts=0,rpcs=0,active=0,peak=0;
 const transport=async(request,body)=>{operations++;gets+=request.op==='get'||request.op==='head';puts+=request.op==='put'||request.op==='put_many'?(request.items?.length??1):0;peak=Math.max(peak,++active);try{if(delay)await new Promise(r=>setTimeout(r,delay));return await raw(request,body);}finally{active--;}};
 let store=new R2CoordinatorStore(module,bucket,namespace,{transport});const request=(op,extra={})=>({op,bucket:'database',prefix:'antfly',namespace,paged_text:true,paged_projections:true,...extra});
 const callMetadata=(req,body)=>{rpcs++;return store.transaction(tx=>metadataObjectCall(tx,req,body));};
 const leaseHost=await instantiateHost(module,null),native=(tx,op,lease,owner=lease?.owner)=>leaseHost.request(request(op,{publication_owner:owner,publication_lease:lease??null}),{transport:withCommitCoordinator(transport,{objectCall:(req,body)=>metadataObjectCall(tx,req,body)})});
 const body=JSON.stringify({text:Array.from({length:12000},(_,i)=>'term'+i).join(' '),sparse_embedding:{needle:1}}),mutation=request('mutate',{operation_id:'large',mutations:[{kind:'upsert',doc_id:'large',body}]});
 let host=await instantiateHost(module,withCommitCoordinator(transport,{objectCall:callMetadata}));const receipt=await host.request(mutation),started=performance.now(),samples=[];let result;
 const initialCounts={operations,gets,puts,rpcs};
 for(let step=0;step<256;step++){
  const begin=performance.now(),before={operations,puts,rpcs};
  if(preparedSlices){
   rpcs++;const initial=await store.transaction(async tx=>{const lease=await native(tx,'publication_acquire',null,crypto.randomUUID());await captureReadView(tx,mk);return{format:3,lease,entries:await publicationSnapshot(tx,keys)};});
   const staged=new PreparedPublication(initial,{read:callMetadata});host=await instantiateHost(module,withCommitCoordinator(transport,staged));
   result=await host.request(request('publish',{publication_continuations:true,publication_adaptive_slices:true,publication_lease:initial.lease}));
   rpcs++;await store.transaction(async tx=>{await native(tx,'publication_validate',initial.lease);await applyPublication(tx,(await publicationSnapshot(tx,keys)).map(([key])=>key),staged.batch());assert.equal((await native(tx,'publication_release',initial.lease)).released,true);});
  }else{
   rpcs++;const initial=await store.transaction(async tx=>({entries:await publicationSnapshot(tx,keys)}));
   const coordinator=publicationCoordinator({objectCall:callMetadata,publicationCall:(req,body,expected)=>{rpcs++;return store.transaction(tx=>publicationCall(tx,keys,(tx,req,body)=>metadataObjectCall(tx,req,body),req,body,expected));}},initial);
   host=await instantiateHost(module,withCommitCoordinator(transport,coordinator));result=await host.request(request('publish',{publication_continuations:true,publication_units:128}));
  }
  samples.push({ms:performance.now()-begin,operations:operations-before.operations,puts:puts-before.puts,metadata_calls:rpcs-before.rpcs,phase:result.phase,quantum:result.posting_quantum??null});
  if(result.published)break;assert.equal(result.wal_end_lsn,'0');
 }
 assert.equal(result.published,true);const elapsed=performance.now()-started,publicationCounts={operations,gets,puts,rpcs,peak},stats=values=>{const a=values.toSorted((a,b)=>a-b);return{n:a.length,p50:a[Math.ceil(a.length*.5)-1],p95:a[Math.ceil(a.length*.95)-1],max:a.at(-1)};};
 host=await instantiateHost(module,withCommitCoordinator(transport,{objectCall:callMetadata}));
 assert.equal((await host.request(request('lookup',{doc_id:'large',required_visibility:receipt.visibility}))).body,body);
 assert.equal((await host.request(request('search',{text:'term11999'}))).hits[0].doc_id,'large');
 assert.equal((await host.request(request('search',{mode:'sparse',sparse:[{term:'needle',weight:1}]}))).hits[0].doc_id,'large');
 assert.equal((await host.request(mutation)).visibility,receipt.visibility);
 const item={variant:preparedSlices?'prepared-adaptive-packed':'previous-native',native_sha256:report.modules[preparedSlices?'current':'previous'],publication_ms:elapsed,slices:samples.length,slice_ms:stats(samples.map(s=>s.ms)),physical:{operations:publicationCounts.operations-initialCounts.operations,gets:publicationCounts.gets-initialCounts.gets,puts:publicationCounts.puts-initialCounts.puts,metadata_calls:publicationCounts.rpcs-initialCounts.rpcs,peak:publicationCounts.peak},samples,verified:true};report.cases.push(item);await writeFile(file,JSON.stringify(report,null,2));console.log(JSON.stringify({...item,samples:undefined}));
}
const[before,after]=report.cases;report.comparison={slice_ratio:after.slices/before.slices,latency_ratio:after.publication_ms/before.publication_ms,metadata_call_ratio:after.physical.metadata_calls/before.physical.metadata_calls};await writeFile(file,JSON.stringify(report,null,2));console.log(JSON.stringify({file,comparison:report.comparison}));
