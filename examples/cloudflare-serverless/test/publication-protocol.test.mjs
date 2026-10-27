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

import{test}from"node:test";
import assert from"node:assert/strict";
import{readFile}from"node:fs/promises";
import{Bucket}from"./object-fixture.mjs";
import{R2CoordinatorStore}from"../r2-coordinator-store.mjs";
import{metadataObjectCall}from"../metadata-object-call.mjs";
import{PreparedPublication,applyPublication}from"../prepared-publication.mjs";
import{compactionIdentity,readCompactionReceipt,beginCompactionReceipt,finishCompactionReceipt}from"../compaction-receipt.mjs";
import{publicationKeys,publicationSnapshot}from"../publication-view.mjs";
import{captureReadView,metadataKeys}from"../read-view.mjs";
import{instantiateHost}from"../../../zig/lib/objectstore/wasm/asyncify.mjs";
import{createR2Transport}from"../../../zig/lib/objectstore/wasm/r2.mjs";
import{withCommitCoordinator}from"../../../zig/lib/objectstore/wasm/coordinated.mjs";
import{nextPublicationBudget,publicationExecutionFailure,publicationFailureReducesBudget}from"../publication-budget.mjs";

test("publication feedback separates fixed network wait from admitted capacity",()=>{
 const healthy={records:64,operations:36,transferred_bytes:130000,working_set_peak_bytes:100000,native_elapsed_ms:14000,native_io_wait_ms:13900};
 assert.equal(nextPublicationBudget(64,64,healthy),64);
 assert.equal(nextPublicationBudget(64,64,{...healthy,records:1}),64,"short WAL tails must not collapse the budget");
 let budget=1;
 for(let i=0;i<6;i++)budget=nextPublicationBudget(budget,64,{...healthy,records:budget});
 assert.equal(budget,64,"healthy full prefixes recover from a previously clamped budget");
 assert.equal(nextPublicationBudget(64,64,{...healthy,operations:960}),40);
 assert.equal(nextPublicationBudget(64,64,{...healthy,transferred_bytes:30*1024*1024}),40);
 assert.equal(nextPublicationBudget(64,64,{...healthy,working_set_peak_bytes:20*1024*1024}),38);
 assert.equal(nextPublicationBudget(64,64,{...healthy,native_io_wait_ms:10000}),25);
 assert.equal(nextPublicationBudget(64,64,{records:64,elapsed_ms:14000}),64,"older callers have no usable capacity observation");
 assert.equal(nextPublicationBudget(64,16,{}),16);
 assert.equal(nextPublicationBudget(64,64,{...healthy,native_io_wait_ms:15000}),64);
});
test("publication feedback retains wall deadline headroom during slow storage",()=>{
 const ioBound={records:16,operations:64,transferred_bytes:600000,working_set_peak_bytes:600000,native_elapsed_ms:24000,native_io_wait_ms:23900};
 assert.equal(nextPublicationBudget(16,64,ioBound),12,"successful slow prefixes shrink before the unchanged 30 second deadline");
 assert.equal(nextPublicationBudget(8,64,{...ioBound,records:8,native_elapsed_ms:21000,native_io_wait_ms:20900}),8,"recovered prefixes must not immediately double into timeout");
 assert.equal(nextPublicationBudget(8,64,{...ioBound,records:8,native_elapsed_ms:14000,native_io_wait_ms:13900}),16,"ample wall headroom still allows recovery");
});
test("queue delay and early cancellation do not lower publication capacity",()=>{
 const canceled=Error("Canceled");
 assert.equal(publicationExecutionFailure(canceled,{elapsed_ms:989}),canceled);
 assert.equal(publicationFailureReducesBudget(canceled),false);
 assert.equal(publicationFailureReducesBudget(Error("ObjectStoreRateLimited")),false);
 const exhausted=publicationExecutionFailure(canceled,{elapsed_ms:20000});
 assert.equal(exhausted.cause,canceled);
 assert.equal(publicationFailureReducesBudget(exhausted),true);
 for(const code of["ObjectIoBudgetExceeded","OutOfMemory","HostedWorkingSetBudgetExceeded","ResponseTooLarge"])assert.equal(publicationFailureReducesBudget(Error(code)),true);
});
const module=await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless.wasm",import.meta.url))),utf8=new TextEncoder(),text=new TextDecoder();
async function fixture(){
 const bucket=new Bucket(),namespace="protocol",keys=await metadataKeys(namespace),pk=await publicationKeys(namespace);let now=1000000,lose=false;
 const raw=createR2Transport({database:bucket}),transport=async(request,body)=>{const result=await raw(request,body);if(lose&&request.op==="put"&&/epochs\/.*\/\d+\.json$/.test(request.key)){lose=false;throw Error("lost ACK");}return result;};
 let store=new R2CoordinatorStore(module,bucket,namespace,{clock:()=>now,transport});
 const req=(op,extra={})=>({op,bucket:"database",prefix:"antfly",namespace,paged_text:true,...extra});
 const plain=withCommitCoordinator(raw,{objectCall:(request,body)=>store.transaction(tx=>metadataObjectCall(tx,request,body))});
 const host=await instantiateHost(module,plain,{clock:()=>now}),leaseHost=await instantiateHost(module,null,{clock:()=>now});
 const native=(tx,op,lease,owner=lease?.owner)=>leaseHost.request(req(op,{publication_owner:owner,publication_lease:lease??null}),{transport:withCommitCoordinator(raw,{objectCall:(request,body)=>metadataObjectCall(tx,request,body)})});
 const begin=()=>store.transaction(async tx=>{const lease=await native(tx,"publication_acquire",null,crypto.randomUUID());await captureReadView(tx,keys,{},()=>now);return{format:2,lease,entries:await publicationSnapshot(tx,pk)};});
 const finish=batch=>store.transaction(async tx=>{await native(tx,"publication_validate",batch.lease);await applyPublication(tx,(await publicationSnapshot(tx,pk)).map(([key])=>key),batch,{clock:()=>now});assert.equal((await native(tx,"publication_release",batch.lease)).released,true);});
 const prepare=async(initial,options={})=>{const prepared=new PreparedPublication(initial),build=await instantiateHost(module,withCommitCoordinator(raw,prepared),{clock:()=>now});const result=await build.request(req("publish",{...options,publication_lease:initial.lease}));return{prepared,result};};
 const mutate=id=>host.request(req("mutate",{operation_id:id,mutations:[{kind:"upsert",doc_id:id,body:JSON.stringify({text:"durable "+id})}]}));
 return{bucket,keys,pk,req,host,begin,finish,prepare,mutate,native,clock:()=>now,store:()=>store,tick:ms=>now+=ms,lose:()=>lose=true,reopen:()=>store=new R2CoordinatorStore(module,bucket,namespace,{clock:()=>now,transport})};
}
test("native publication commits tentative head/offsets/release atomically and permits concurrent WAL appends",async()=>{
 const f=await fixture();await f.mutate("first");const initial=await f.begin(),{prepared,result}=await f.prepare(initial);
 assert.equal(result.published,true);assert.equal(text.decode((await f.store().transaction(tx=>tx.get(f.keys.head))).body).trim(),"0");
 await f.mutate("later");await f.finish(prepared.batch());
 assert.equal(text.decode((await f.store().transaction(tx=>tx.get(f.keys.head))).body).trim(),"1");
 assert.equal((await f.host.request(f.req("status"))).published_lsn,"1");assert.equal((await f.host.request(f.req("count"))).count,"2");
 const next=await f.begin(),second=await f.prepare(next);await f.finish(second.prepared.batch());assert.equal((await f.host.request(f.req("status"))).published_lsn,"2");
});
test("publication makes its newly visible generation readable without a pin-write admission",async()=>{
 const f=await fixture();await f.mutate("first");const initial=await f.begin(),{prepared}=await f.prepare(initial);await f.finish(prepared.batch());
 const writes=f.bucket.writes;
 const view=await f.store().readOnly(tx=>captureReadView(tx,f.keys,{},f.clock));
 assert.equal(view.version,"1");
 assert.equal(f.bucket.writes,writes,"first reader must not write metadata or queue behind maintenance");
 assert.ok(view.entries.some(([key,pin])=>key===f.keys.progress+"READ_PINS/1"&&pin));
});
test("finalize rejects expired and replaced native ownership without publishing tentative state",async()=>{
 const f=await fixture();await f.mutate("first");const initial=await f.begin(),build=await f.prepare(initial);f.tick(31000);
 await assert.rejects(f.finish(build.prepared.batch()),/WorkLeaseLost/);
 const successor=await f.begin();assert.ok(BigInt(successor.lease.token)>BigInt(initial.lease.token));await assert.rejects(f.finish(build.prepared.batch()),/WorkLeaseLost/);
 assert.equal(text.decode((await f.store().transaction(tx=>tx.get(f.keys.head))).body).trim(),"0");
 await f.store().transaction(tx=>f.native(tx,"publication_release",successor.lease));
});
test("ambiguous finalize acknowledgement reopens with complete head and released lease",async()=>{
 const f=await fixture();await f.mutate("first");const initial=await f.begin(),build=await f.prepare(initial);await build.prepared.objectCall({op:"put",key:f.keys.progress+"READ_PINS/1"},utf8.encode("1600000000000"));f.lose();
 await assert.rejects(f.finish(build.prepared.batch()),/ObjectStoreTransportFailed/);f.reopen();
 assert.equal((await f.host.request(f.req("status"))).published_lsn,"1");
 await assert.rejects(f.store().transaction(tx=>f.native(tx,"publication_validate",initial.lease)),/WorkLeaseLost/);
 assert.equal(text.decode((await f.store().transaction(tx=>tx.get(f.keys.progress+"READ_PINS/1"))).body),"1600000000000");
});
test("tentative batches reject unauthorized keys and fresh read-set conflicts",async()=>{
 const f=await fixture();await f.mutate("first");const initial=await f.begin(),build=await f.prepare(initial);
 await f.store().transaction(tx=>tx.put(f.keys.floor,{etag:crypto.randomUUID(),body:utf8.encode("0"),contentType:null}));
 await assert.rejects(f.finish(build.prepared.batch()),/PublicationConflict/);
 const batch=build.prepared.batch();batch.writes.push([f.keys.wal,{etag:"bad",body:utf8.encode("bad"),contentType:null}]);
 await assert.rejects(f.store().transaction(tx=>applyPublication(tx,f.pk,batch)),/PublicationConflict|InvalidPublicationBatch/);
 await f.store().transaction(tx=>f.native(tx,"publication_release",initial.lease));
});
test("read-only committed snapshots bypass suspended writers and retain fresh fencing",async()=>{
 const bucket=new Bucket(),store=new R2CoordinatorStore(module,bucket,"observer");await store.transaction(tx=>tx.put("value",1));
 let entered,release;const started=new Promise(r=>entered=r),blocked=new Promise(r=>release=r);
 const writer=store.transaction(async tx=>{tx.put("value",2);entered();await blocked;});await started;
 const read=await Promise.race([store.readOnly(tx=>tx.get("value")),new Promise((_,reject)=>setTimeout(()=>reject(Error("reader blocked behind writer")),1000))]);assert.equal(read,1);
 release();await writer;assert.equal(await store.readOnly(tx=>tx.get("value")),2);
 const reopened=new R2CoordinatorStore(module,bucket,"observer");await reopened.transaction(tx=>tx.get("value"));await assert.rejects(store.readOnly(tx=>tx.get("value")),/ImmutableLogFenced/);
});
test("read-only native observation never repairs or rotates an epoch",async()=>{
 const bucket=new Bucket(),store=new R2CoordinatorStore(module,bucket,"observer-boundary");for(let i=0;i<64;i++)await store.transaction(tx=>tx.put("value",i));const writes=bucket.writes;
 assert.equal(await store.readOnly(tx=>tx.get("value")),63);assert.equal(bucket.writes,writes);
 await assert.rejects(store.readOnly(tx=>tx.put("value",99)),/ReadSnapshotNeedsPin/);assert.equal(bucket.writes,writes);
});
test("fully paged compaction validates protected source without changing writer fencing",async()=>{
 const f=await fixture();await f.mutate("first");const initial=await f.begin(),build=await f.prepare(initial);await f.finish(build.prepared.batch());
 const before=await f.store().transaction(tx=>tx.getMany([f.keys.head,f.keys.progress+"HEAD_FENCE"]));
 const result=await f.host.request(f.req("compact"));assert.equal(result.published,false);
 assert.deepEqual(await f.store().transaction(tx=>tx.getMany([f.keys.head,f.keys.progress+"HEAD_FENCE"])),before);
});

async function keyedCompaction(f,id){
 const identity=await compactionIdentity(id,{max_documents:1000,max_artifact_bytes:4*1024*1024});
 const begin=()=>f.store().transaction(tx=>beginCompactionReceipt(tx,identity,async()=>{
  const lease=await f.native(tx,"publication_acquire",null,crypto.randomUUID());
  await captureReadView(tx,f.keys,{},f.clock);return{format:2,lease,entries:await publicationSnapshot(tx,f.pk)};
 }));
 const finish=(batch,result)=>f.store().transaction(tx=>finishCompactionReceipt(tx,identity,batch,result,async()=>{
  await f.native(tx,"publication_validate",batch.lease);
  await applyPublication(tx,(await publicationSnapshot(tx,f.pk)).map(([key])=>key),batch,{clock:f.clock});
  assert.equal((await f.native(tx,"publication_release",batch.lease)).released,true);
 }));
 const prepare=async initial=>{
  const prepared=new PreparedPublication(initial),raw=createR2Transport({database:f.bucket}),host=await instantiateHost(module,withCommitCoordinator(raw,prepared),{clock:f.clock});
  const result=await host.request(f.req("compact",{publication_lease:initial.lease}));return{batch:prepared.batch(),result};
 };
 return{identity,begin,finish,prepare};
}
test("keyed compaction recovers an atomic receipt after a lost acknowledgement and eviction",async()=>{
 const f=await fixture();await f.mutate("first");const publication=await f.prepare(await f.begin(),{paged_text:false});await f.finish(publication.prepared.batch());
 const operation=await keyedCompaction(f,"maintenance/1"),initial=await operation.begin(),prepared=await operation.prepare(initial);
 assert.equal(prepared.result.published,true);assert.ok(prepared.batch.writes.some(([key])=>key===f.keys.head));
 f.lose();await assert.rejects(operation.finish(prepared.batch,prepared.result),/ObjectStoreTransportFailed/);f.reopen();
 const replay=await operation.begin();assert.equal(replay.complete,true);assert.deepEqual(replay.result,prepared.result);
 const before=await f.store().transaction(tx=>tx.get(f.keys.progress+"HEAD_FENCE"));
 assert.deepEqual(await operation.finish(prepared.batch,{published:true,version:"999",artifact_count:999}),prepared.result);
 assert.deepEqual(await f.store().transaction(tx=>tx.get(f.keys.progress+"HEAD_FENCE")),before,"receipt replay cannot reacquire or release ownership");
 const changed=await compactionIdentity("maintenance/1",{max_documents:2000,max_artifact_bytes:4*1024*1024});
 await assert.rejects(f.store().transaction(tx=>readCompactionReceipt(tx,changed)),/CompactionOperationConflict/);
});
test("keyed compaction fences an expired attempt and never records an uncommitted result",async()=>{
 const f=await fixture();await f.mutate("first");const publication=await f.prepare(await f.begin());await f.finish(publication.prepared.batch());
 const operation=await keyedCompaction(f,"maintenance/takeover"),old=await operation.begin(),stale=await operation.prepare(old);
 await assert.rejects(operation.begin(),/WorkLeaseLost/);f.tick(31000);
 const next=await operation.begin();assert.notEqual(next.lease.token,old.lease.token);
 await assert.rejects(operation.finish(stale.batch,stale.result),/WorkLeaseLost/);
 assert.equal((await f.store().transaction(tx=>readCompactionReceipt(tx,operation.identity))).state,"pending");
 const current=await operation.prepare(next);await operation.finish(current.batch,current.result);
 assert.deepEqual((await operation.begin()).result,current.result);
 await assert.rejects(compactionIdentity("😀".repeat(65),{max_documents:1000,max_artifact_bytes:1}),/OperationId/);
 await assert.rejects(compactionIdentity("\ud800",{max_documents:1000,max_artifact_bytes:1}),/OperationId/);
});


test("fresh observation reuses only cache rows bound to its exact committed revision",async()=>{
 const bucket=new Bucket(),store=new R2CoordinatorStore(module,bucket,"snapshot-row-binding");await store.transaction(tx=>tx.put("value",{number:1}));
 const request=store.snapshotKernel.request.bind(store.snapshotKernel),actions=[];
 store.snapshotKernel.request=(input,options)=>{actions.push(input.coordinate.action);return request(input,options);};
 const first=await store.readOnly(tx=>tx.get("value"));first.number=99;assert.deepEqual(actions,['observe']);assert.deepEqual(await store.readOnly(tx=>tx.get("value")),{number:1});
 let entered,release;const started=new Promise(r=>entered=r),gate=new Promise(r=>release=r);actions.length=0;
 const historical=store.readOnly(async tx=>{entered();await gate;return tx.get("value");});await started;
 await store.transaction(tx=>tx.put("value",{number:2}));release();assert.deepEqual(await historical,{number:1});assert.deepEqual(actions,['observe','read']);
 actions.length=0;assert.deepEqual(await store.readOnly(tx=>tx.get("value")),{number:2});assert.deepEqual(actions,['observe']);
 const replacement=new R2CoordinatorStore(module,bucket,"snapshot-row-binding");await replacement.transaction(tx=>tx.get("value"));await assert.rejects(store.readOnly(tx=>tx.get("value")),/ImmutableLogFenced/);
});


test("fresh observations retain execution capacity while the writer metadata pool is suspended",async()=>{
 const{RequestPool}=await import('../request-pool.mjs');const writer=new RequestPool(()=>instantiateHost(module,null),1),observer=new RequestPool(()=>instantiateHost(module,null),1);
 const store=new R2CoordinatorStore(module,new Bucket(),"snapshot-capacity",{kernel:writer,readKernel:observer});await store.transaction(tx=>tx.put("value",1));
 let entered,release;const started=new Promise(r=>entered=r),gate=new Promise(r=>release=r);const held=writer.withHost(async()=>{entered();await gate;});await started;
 let timeout;try{assert.equal(await Promise.race([store.readOnly(tx=>tx.get("value")),new Promise((_,reject)=>timeout=setTimeout(()=>reject(Error('snapshot queued behind writer kernel')),1000))]),1);}finally{clearTimeout(timeout);release();await held;}
});

test("cold committed observations preserve authority without taking writer ownership",async()=>{
 const bucket=new Bucket(),writer=new R2CoordinatorStore(module,bucket,"cold-committed");await writer.transaction(tx=>tx.put("value",1));
 const cold=new R2CoordinatorStore(module,bucket,"cold-committed"),writes=bucket.writes;assert.equal(await cold.readOnly(tx=>tx.get("value")),1);assert.equal(bucket.writes,writes);assert.equal(cold.acquired,false);assert.equal(cold.state,null);
 await writer.transaction(tx=>tx.put("value",2));assert.equal(await cold.readOnly(tx=>tx.get("value")),2);assert.equal(cold.acquired,false);
 const replacement=new R2CoordinatorStore(module,bucket,"cold-committed");await replacement.transaction(tx=>tx.put("value",3));assert.equal(await cold.readOnly(tx=>tx.get("value")),3);await assert.rejects(writer.readOnly(tx=>tx.get("value")),/ImmutableLogFenced/);
 await cold.transaction(tx=>tx.put("value",4));assert.equal(cold.acquired,true);await assert.rejects(replacement.readOnly(tx=>tx.get("value")),/ImmutableLogFenced/);assert.equal(await cold.readOnly(tx=>tx.get("value")),4);
});

test("unowned recovery continuations require a committed cursor before independent authentication",async()=>{
 let calls=0;const store=new R2CoordinatorStore(module,new Bucket(),'missing-start-continuation',{readKernel:{async request(input){calls++;assert.equal(input.coordinate.action,'observe_committed');return{state:{format:1,owner:Array(16).fill(2),epoch:Array(16).fill(3),next:0,revision:64,root:'null'},has_more:true};}}});
 await assert.rejects(store.readOnly(()=>{throw Error('must not serve an incomplete prefix');}),/ReadSnapshotNeedsAdmission/);assert.equal(calls,1);assert.equal(store.acquired,false);assert.equal(store.observedState,undefined);
});
