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
import{publicationKeys,publicationSnapshot}from"../publication-view.mjs";
import{captureReadView,metadataKeys}from"../read-view.mjs";
import{instantiateHost}from"../../../zig/lib/objectstore/wasm/asyncify.mjs";
import{createR2Transport}from"../../../zig/lib/objectstore/wasm/r2.mjs";
import{withCommitCoordinator}from"../../../zig/lib/objectstore/wasm/coordinated.mjs";
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
 const finish=batch=>store.transaction(async tx=>{await native(tx,"publication_validate",batch.lease);await applyPublication(tx,(await publicationSnapshot(tx,pk)).map(([key])=>key),batch);assert.equal((await native(tx,"publication_release",batch.lease)).released,true);});
 const prepare=async initial=>{const prepared=new PreparedPublication(initial),build=await instantiateHost(module,withCommitCoordinator(raw,prepared),{clock:()=>now});const result=await build.request(req("publish",{publication_lease:initial.lease}));return{prepared,result};};
 const mutate=id=>host.request(req("mutate",{operation_id:id,mutations:[{kind:"upsert",doc_id:id,body:JSON.stringify({text:"durable "+id})}]}));
 return{bucket,keys,pk,req,host,begin,finish,prepare,mutate,native,store:()=>store,tick:ms=>now+=ms,lose:()=>lose=true,reopen:()=>store=new R2CoordinatorStore(module,bucket,namespace,{clock:()=>now,transport})};
}
test("native publication commits tentative head/offsets/release atomically and permits concurrent WAL appends",async()=>{
 const f=await fixture();await f.mutate("first");const initial=await f.begin(),{prepared,result}=await f.prepare(initial);
 assert.equal(result.published,true);assert.equal(text.decode((await f.store().transaction(tx=>tx.get(f.keys.head))).body).trim(),"0");
 await f.mutate("later");await f.finish(prepared.batch());
 assert.equal(text.decode((await f.store().transaction(tx=>tx.get(f.keys.head))).body).trim(),"1");
 assert.equal((await f.host.request(f.req("status"))).published_lsn,"1");assert.equal((await f.host.request(f.req("count"))).count,"2");
 const next=await f.begin(),second=await f.prepare(next);await f.finish(second.prepared.batch());assert.equal((await f.host.request(f.req("status"))).published_lsn,"2");
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
