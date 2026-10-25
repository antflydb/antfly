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

import {test} from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {Bucket} from "./object-fixture.mjs";
import {R2CoordinatorStore} from "../r2-coordinator-store.mjs";
import {openReadView,withReadView,metadataKeys} from "../read-view.mjs";
import {PublicationScheduler} from "../publication-scheduler.mjs";
import {metadataCollectionDue} from "../maintenance-policy.mjs";
import {tenantEnv,tenantEnvs} from "../tenants.mjs";
import {createR2Transport} from "../../../zig/lib/objectstore/wasm/r2.mjs";
import {instrumentTransport} from "../performance.mjs";
const module=await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless.wasm",import.meta.url)));
const utf8=new TextEncoder();
const record=body=>({etag:crypto.randomUUID(),body:utf8.encode(body),contentType:null});
const fixture=()=>{const bucket=new Bucket();return{bucket,store:new R2CoordinatorStore(module,bucket,"performance")};};
test("one view snapshots heads and atomically shares a durable generation pin",async()=>{
 const {bucket,store}=fixture(),keys=await metadataKeys("view");
 await store.transaction(tx=>{tx.put(keys.head,record("7"));tx.put(keys.wal,record('{"latest_lsn":9}'));tx.put(keys.floor,record("6"));});
 const first=await openReadView(store,"view"),writes=bucket.writes;
 for(let i=0;i<8;i++)assert.equal((await openReadView(store,"view")).version,"7");
 assert.equal(bucket.writes,writes,"shared pins must not be committed again for every query");
 let calls=0,fallback=0;const transport=withReadView(async()=>{fallback++;throw new Error("unexpected fallback");},{openReadView:async()=>{calls++;return first;}},"view");
 const head=await transport({op:"get",bucket:"database",key:keys.head});assert.equal(new TextDecoder().decode(head.body),"7");
 await transport({op:"head",bucket:"database",key:keys.wal});await transport({op:"get",bucket:"database",key:keys.floor});await transport({op:"get",bucket:"database",key:keys.progress+"READ_PINS/7"});
 assert.equal(calls,1);assert.equal(fallback,0);
 await store.transaction(tx=>{tx.put(keys.head,record("8"));tx.put(keys.floor,record("8"));});
 assert.equal((await openReadView(store,"view")).version,"8");
 const retired=new Map((await openReadView(store,"view",{version:"7"})).entries);assert.ok(!retired.has(keys.progress+"READ_PINS/7"));
 await assert.rejects(openReadView(store,"view",{version:"18446744073709551616"}),/InvalidReadViewVersion/);
});
test("whole reads preserve all values while the bounded decoded cache evicts entries",async()=>{
 const {store}=fixture();await store.transaction(tx=>{for(let i=0;i<512;i++)tx.put(`key/${i}`,i);});
 await store.transaction(tx=>tx.getMany(Array.from({length:128},(_,i)=>`key/${i}`)));
 const values=await store.transaction(tx=>tx.getMany(Array.from({length:512},(_,i)=>`key/${i}`)));
 assert.deepEqual(values,Array.from({length:512},(_,i)=>i));assert.ok(store.rowBytes<=512*1024&&store.rows.size<=256);
});
test("publication tickets coalesce callers, retain concurrent writes and recover failed admission",async()=>{
 const {store}=fixture();let now=Date.now();const jobs=new PublicationScheduler(store,"jobs",()=>now),keys=await metadataKeys("jobs");
 await store.transaction(tx=>tx.put(keys.wal,record('{"latest_lsn":1}')));
 const calls=await Promise.all(Array.from({length:32},()=>jobs.request()));assert.equal(calls.filter(c=>c.enqueue).length,1);
 const id=calls[0].id;assert.ok(calls.every(c=>c.id===id));
 await store.transaction(tx=>tx.put(keys.wal,record('{"latest_lsn":9007199254740993}')));
 assert.deepEqual(await jobs.finish(id,"9007199254740992"),{has_more:true});assert.equal((await jobs.request()).enqueue,false);
 assert.deepEqual(await jobs.finish(id,"9007199254740993"),{done:true});
 assert.equal((await jobs.request()).enqueue,false,"idle cron must not republish an already visible watermark");
 const enrichment=await jobs.request({force:true});assert.equal(enrichment.enqueue,true,"pending enrichment must continue even without a new WAL watermark");await jobs.finish(enrichment.id,"9007199254740993");
 await store.transaction(tx=>tx.put(keys.wal,record('{"latest_lsn":9007199254740994}')));
 const next=await jobs.request();await jobs.abandon(id);assert.equal(await jobs.valid(next.id),true);
 await jobs.abandon(next.id);const lost=await jobs.request();now+=120001;const recovered=await jobs.request();assert.notEqual(lost.id,recovered.id);assert.equal(await jobs.valid(lost.id),false);
});
test("interactive callbacks take the next group slot before queued maintenance",async()=>{
 const {store}=fixture();const order=[];let release,entered;const gate=new Promise(r=>release=r),started=new Promise(r=>entered=r);
 const first=store.transaction(async()=>{entered();await gate;});await started;
 const maintenance=store.transaction(()=>order.push("maintenance"),{priority:"maintenance"}),interactive=store.transaction(()=>order.push("interactive"));release();
 await Promise.all([first,maintenance,interactive]);assert.deepEqual(order,["interactive","maintenance"]);
});
test("tenant routes reject physical collisions and unknown Queue destinations",()=>{
 const env={ANTFLY_INCARNATION:"root",ANTFLY_TENANTS:JSON.stringify({a:{incarnation:"tenant-a",storage_id:"storage-a"},b:{incarnation:"tenant-b",storage_id:"storage-b"}})};
 assert.equal(tenantEnvs(env).length,3);assert.equal(tenantEnv(env,"a").ANTFLY_INCARNATION,"tenant-a");assert.equal(tenantEnv(env,"b").COORDINATOR_STORAGE_ID,"storage-b");
 assert.throws(()=>tenantEnv(env,"unknown"),/UnknownTenant/);assert.throws(()=>tenantEnv({...env,ANTFLY_TENANTS:JSON.stringify({a:{incarnation:"root",storage_id:"other"}})},"a"),/Collision/);
});
test("physical counters include failed transports without changing results",async()=>{
 const counts={},transport=instrumentTransport(async()=>{throw new Error("failed");},counts);await assert.rejects(transport({op:"get"}),/failed/);assert.equal(counts.calls,1);assert.equal(counts.by_op.get,1);assert.ok(counts.wait_ms>=0);
});


test("recovery prefetch overlaps independent reads without retaining negative tail entries",async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket},{maxPutBytes:4*1024*1024});let active=0,peak=0;
 const store=new R2CoordinatorStore(module,bucket,"prefetch",{transport:async(request,body)=>{active++;peak=Math.max(peak,active);await new Promise(r=>setTimeout(r,1));try{return await raw(request,body);}finally{active--;}}});
 await store.transaction(tx=>tx.put("value",1));peak=0;
 assert.equal(await store.transaction(tx=>tx.get("value")),1);assert.ok(peak>=3,"checkpoint, seal and next-slot reads should overlap");
 const other=new R2CoordinatorStore(module,bucket,"prefetch");await other.transaction(tx=>tx.put("value",2));
 await assert.rejects(store.transaction(tx=>tx.get("value")),/ImmutableLogFenced/);
});


test("metadata collection backs off on low yield and retains a maximum orphan sweep interval",()=>{
 const baseline={at:1000,revision:10};assert.equal(metadataCollectionDue(baseline,1000,1001),false);assert.equal(metadataCollectionDue(baseline,266,301000),true);assert.equal(metadataCollectionDue(baseline,10,3601000),true);assert.equal(metadataCollectionDue(baseline,1000,999),false);
 const low={...baseline,stats:{reclaimed_bytes:1024,deleted:1,scanned:100}};assert.equal(metadataCollectionDue(low,300,301000),false);assert.equal(metadataCollectionDue(low,1034,901000),true);
});


test("read snapshots fall back only for an older missing RPC",async()=>{
 let calls=0;const transport=withReadView(async()=>{calls++;return{json:{},body:Uint8Array.of(1)};},{openReadView:async()=>{throw new Error("RPC property openReadView is not a function");}},"old");
 await transport({op:"get",key:"antfly/progress/old/HEAD"});await transport({op:"get",key:"antfly/progress/old/HEAD"});assert.equal(calls,2);
 const denied=withReadView(async()=>{throw new Error("must not fall back");},{openReadView:async()=>{throw new Error("ImmutableLogCheckpointMissing");}},"old");await assert.rejects(denied({op:"get",key:"antfly/progress/old/HEAD"}),/CheckpointMissing/);
});


test("a stale warm owner cannot turn bounded recovery into a fresh takeover",async()=>{
 const bucket=new Bucket(),now=Date.now(),options={clock:()=>now};const old=new R2CoordinatorStore(module,bucket,"long-fence",options);
 await old.transaction(tx=>tx.put("value",0));const newer=new R2CoordinatorStore(module,bucket,"long-fence",options);
 await newer.transaction(tx=>tx.put("value",1));for(let i=0;i<5000;i++)await newer.transaction(tx=>tx.put("value",i+2));
 await assert.rejects(old.transaction(tx=>tx.get("value")),/ImmutableLogFenced/);
 await assert.rejects(old.transaction(tx=>tx.put("stale",true)),/ImmutableLogFenced/);
 await assert.rejects(old.native({action:"begin"}),/ImmutableLogFenced/,"the native recovery protocol must also fence before returning another owner's continuation");
 assert.equal(await newer.transaction(tx=>tx.get("value")),5001);
});

test("native mutation admission shares a commit and isolates conflicting callers",async()=>{
 const {MutationGroups}=await import("../mutation-groups.mjs");let commits=[];
 const queue=new MutationGroups(async inputs=>{commits.push(inputs.map(i=>i.operation_id));if(inputs.some(i=>i.operation_id==="bad"))throw new Error("WalIdempotencyConflict");return inputs.map(i=>({visibility:i.operation_id}));});
 const settled=await Promise.allSettled(["a","bad","b"].map(operation_id=>queue.submit({operation_id,mutations:[{}]})));
 assert.equal(commits[0].length,3);assert.equal(settled[0].value.visibility,"a");assert.equal(settled[2].value.visibility,"b");assert.equal(settled[1].status,"rejected");assert.equal(queue.pending,0);
 commits=[];await Promise.all(Array.from({length:8},(_,i)=>queue.submit({operation_id:String(i),mutations:[{}]})));assert.equal(commits.length,1);assert.equal(commits[0].length,8);
});
test("publication snapshots reduce reads and fence cached metadata at every write",async()=>{
 const {publicationKeys,publicationCall,publicationCoordinator}=await import("../publication-view.mjs");
 const {store}=fixture(),keys=await publicationKeys("publication-view");let calls=0;
 await store.transaction(tx=>tx.put(keys[0],record("1")));
 const view=publicationCoordinator({publicationCall:async(request,body,expected)=>{calls++;return store.transaction(tx=>publicationCall(tx,keys,async(tx,request,body)=>{
   const previous=await tx.get(request.key);
   if(request.op==="put"){if(request.if_match_etag!==previous?.etag)return{status:2};const value=record("");value.body=body;tx.put(request.key,value);return{status:0,json:{etag:value.etag}};}
   return previous?{status:0,json:{etag:previous.etag},body:previous.body}:{status:1};
 },request,body,expected));}});
 const request={op:"get",key:keys[0],bucket:"database"},first=await view.objectCall(request);
 for(let i=0;i<10;i++)assert.equal((await view.objectCall(request)).json.etag,first.json.etag);
 assert.equal(calls,1);
 await store.transaction(tx=>tx.put(keys[0],record("2")));
 assert.equal((await view.objectCall({op:"put",key:keys[0],if_match_etag:first.json.etag},utf8.encode("stale"))).status,2);
 assert.equal(new TextDecoder().decode((await store.transaction(tx=>tx.get(keys[0]))).body),"2");
 // Missing cached records also participate in the read set.
 const missing=await view.objectCall({op:"get",key:keys[2]});assert.equal(missing.status,1);
 await store.transaction(tx=>tx.put(keys[2],record("3")));
 assert.equal((await view.objectCall({op:"put",key:keys[0],if_match_etag:(await store.transaction(tx=>tx.get(keys[0]))).etag},utf8.encode("wrong floor"))).status,2);
});

test("publication views fall back for old coordinators and propagate storage failures",async()=>{
 const {publicationCoordinator}=await import("../publication-view.mjs");let calls=0;
 const old=publicationCoordinator({publicationCall:async()=>{throw new Error("publicationCall is not a function");},objectCall:async()=>{calls++;return {status:1};}});
 await old.objectCall({op:"get",key:"head"});await old.objectCall({op:"get",key:"head"});assert.equal(calls,2);
 const broken=publicationCoordinator({publicationCall:async()=>{throw new Error("ImmutableLogFenced");},objectCall:async()=>{throw new Error("unexpected fallback");}});
 await assert.rejects(broken.objectCall({op:"get",key:"head"}),/ImmutableLogFenced/);
});

test("publication prefix snapshots permit concurrent WAL appends but retain HEAD CAS",async()=>{
 const {publicationCoordinator}=await import("../publication-view.mjs");const keys=await metadataKeys("live-wal");const head=record("7"),wal=record('{"latest_lsn":10}');let expected;
 const view=publicationCoordinator({publicationCall:async(request,body,readSet)=>{expected=readSet;return {result:{status:0,json:{etag:"published"}},entries:[]};}},{entries:[[keys.head,head],[keys.wal,wal]]});
 await view.objectCall({op:"get",key:keys.wal});await view.objectCall({op:"get",key:keys.head});
 await view.objectCall({op:"put",key:keys.head,if_match_etag:head.etag},utf8.encode("8"));
 assert.deepEqual(expected,[[keys.head,head.etag]],"an append changes the WAL HEAD without changing the built prefix");
});

test("mutation groups do not multiply transient or fencing failures into individual writes",async()=>{
 const {MutationGroups}=await import("../mutation-groups.mjs");for(const reason of ["ImmutableLogFenced","ObjectStoreTransportFailed","DeadlineExceeded"]){let calls=0;const queue=new MutationGroups(async()=>{calls++;throw new Error(reason);});const results=await Promise.allSettled(Array.from({length:8},(_,i)=>queue.submit({operation_id:String(i),mutations:[{}]})));assert.equal(calls,1);assert.ok(results.every(result=>result.status==="rejected"));assert.equal(queue.pending,0);}
});

test("metadata RPC counters expose protocol types without tenant or document identifiers",async()=>{
 const {countMetadataRpc}=await import("../performance.mjs");const counts={};countMetadataRpc(counts,{op:"get",key:"antfly/progress/private-tenant/READ_PINS/91"});countMetadataRpc(counts,{op:"put",key:"antfly/wal-v1/secret-hash/HEAD"});assert.deepEqual(counts.metadata_by_op,{"get:READ_PINS":1,"put:WAL_HEAD":1});
});

test("prepared WAL uploads expose no HEAD until the original ETag commits, including GC rotation",async()=>{
 const {PreparedWal}=await import("../prepared-wal.mjs");const {store}=fixture(),keys=await metadataKeys("prepared");const original=record('{"write_epoch":1,"latest_lsn":0}');
 await store.transaction(tx=>tx.put(keys.wal,original));const prepared=new PreparedWal(keys.wal,[[keys.wal,original],[keys.head,null]]),body=utf8.encode('{"write_epoch":1,"latest_lsn":8}');
 assert.equal(prepared.objectCall({op:"put",key:keys.wal,if_match_etag:original.etag},body).status,0);body.fill(0);
 assert.equal((await store.transaction(tx=>tx.get(keys.wal))).etag,original.etag,"preparation must not publish metadata");
 assert.equal(prepared.objectCall({op:"put",key:keys.head,if_none_match:true},utf8.encode("1")).status,3,"preparation has no progress-write authority");
 const cas=async(tx,request,body)=>{const previous=await tx.get(request.key);if(request.if_match_etag!==previous?.etag)return{status:2};tx.put(request.key,{etag:"committed",body});return{status:0};};
 await store.transaction(tx=>tx.put(keys.wal,record('{"write_epoch":2,"latest_lsn":0}')));
 assert.equal(await store.transaction(tx=>prepared.commit(tx,cas)),false,"GC epoch rotation must invalidate prepared pages");
 const current=await store.transaction(tx=>tx.get(keys.wal)),fresh=new PreparedWal(keys.wal,[[keys.wal,current]]);
 fresh.objectCall({op:"put",key:keys.wal,if_match_etag:current.etag},utf8.encode("durable"));
 assert.equal(await store.transaction(tx=>fresh.commit(tx,cas)),true);assert.equal(new TextDecoder().decode((await store.transaction(tx=>tx.get(keys.wal))).body),"durable");
});
test("runtime routes isolate tenant identities and enforce bounded reader pools",async()=>{
 const {runtimeName,readerSlots}=await import("../runtime-routing.mjs");assert.notEqual(runtimeName("a",0),runtimeName("a",1));assert.notEqual(runtimeName("a",0),runtimeName("b",0));assert.equal(readerSlots({}),2);assert.throws(()=>readerSlots({READERS_PER_TENANT:"5"}),/InvalidReaderPoolSize/);
});

test("maintenance tickets coalesce cron admission and reject stale continuation steps",async()=>{
 const {MaintenanceTickets}=await import("../maintenance-tickets.mjs");const {store}=fixture();let now=1000;const tickets=new MaintenanceTickets(store,()=>now);
 const requests=await Promise.all(Array.from({length:16},()=>tickets.request("collect_metadata","job")));assert.equal(requests.filter(r=>r.enqueue).length,1);const first=requests.find(r=>r.enqueue);
 assert.equal(await tickets.valid("collect_metadata",first,"job"),true);
 const next=await tickets.advance("collect_metadata",first,"job");assert.equal(next.step,1);assert.equal((await tickets.advance("collect_metadata",first,"job")).enqueue,false);
 assert.equal(await tickets.valid("collect_metadata",first,"job"),false);await tickets.finish("collect_metadata",first);assert.equal(await tickets.valid("collect_metadata",next,"job"),true);
 now+=120001;const replacement=await tickets.request("collect_metadata","job");assert.notEqual(replacement.id,next.id);assert.equal(await tickets.valid("collect_metadata",next,"job"),false);await tickets.abandon("collect_metadata",replacement);assert.equal((await tickets.request("collect_metadata","job")).enqueue,true);
});

test("shared kernel pools bound concurrency and leave metadata an independent slot",async()=>{
 const {RequestPool}=await import("../request-pool.mjs");let active=0,peak=0;const metadata=new RequestPool(async()=>({request:async n=>n,usable:()=>true,memoryBytes:()=>65536}),1);
 const readers=new RequestPool(async()=>({request:async n=>{active++;peak=Math.max(peak,active);await new Promise(r=>setTimeout(r,2));const result=await metadata.request(n);active--;return result;},usable:()=>true,memoryBytes:()=>65536}),2);
 assert.deepEqual(await Promise.all(Array.from({length:12},(_,i)=>readers.request(i))),Array.from({length:12},(_,i)=>i));assert.equal(peak,2);assert.equal(readers.memoryBytes(),131072);
});
test("compact kernels enforce their declared sixteen MiB memory maximum",async()=>{
 const {limitWasmMemory}=await import("../wasm-memory.mjs");const bytes=limitWasmMemory(await readFile(new URL("../dist/antfly-serverless.wasm",import.meta.url)),256),compiled=await WebAssembly.compile(bytes),imports={};for(const item of WebAssembly.Module.imports(compiled)){assert.equal(item.kind,"function");imports[item.module]??={};imports[item.module][item.name]=()=>0;}
 const instance=await WebAssembly.instantiate(compiled,imports);assert.throws(()=>instance.exports.memory.grow(256),RangeError);
});


test("mutation admission deadline includes time waiting for an earlier WAL group",async()=>{
 const {MutationGroups}=await import("../mutation-groups.mjs");let now=0,release,started,commits=0;
 const began=new Promise(resolve=>started=resolve),blocked=new Promise(resolve=>release=resolve);
 const queue=new MutationGroups(async(inputs,deadline)=>{commits++;assert.equal(deadline,30000);started();await blocked;return inputs.map(()=>({visibility:"ok"}));},{clock:()=>now});
 const first=queue.submit({operation_id:"first",mutations:[]});await began;
 const second=queue.submit({operation_id:"second",mutations:[]});const expired=assert.rejects(second,/DeadlineExceeded/);
 now=30001;release();await first;await expired;assert.equal(commits,1);assert.equal(queue.pending,0);
});


test("expired kernel admission cannot open a publication lease behind another tenant",async()=>{
 const {RequestPool}=await import("../request-pool.mjs");let entered,release,leases=0;
 const began=new Promise(resolve=>entered=resolve),blocked=new Promise(resolve=>release=resolve);
 const pool=new RequestPool(async()=>({usable:()=>true,memoryBytes:()=>65536}),1);
 const first=pool.withHost(async()=>{entered();await blocked;return "first";});await began;
 await assert.rejects(pool.withHost(async()=>{leases++;},{signal:AbortSignal.timeout(5)}),/DeadlineExceeded/);
 assert.equal(leases,0);assert.equal(pool.pending.length,0);release();assert.equal(await first,"first");
});


test("idle checkpoints coalesce and defer behind foreground work without delaying its result",async()=>{
 const {IdleCheckpoint}=await import("../idle-checkpoint.mjs");let revision=8,idle=true,work,calls=0;
 const checkpoints=new IdleCheckpoint({revision:()=>revision,isIdle:()=>idle,checkpoint:async()=>{calls++;return{done:true,revision};},background:promise=>work=promise,delay:2});
 checkpoints.schedule();checkpoints.schedule();idle=false;await work;assert.equal(calls,0);
 idle=true;checkpoints.schedule();await work;assert.equal(calls,1);checkpoints.schedule();assert.equal(checkpoints.pending,false);
 revision=16;checkpoints.schedule();await work;assert.equal(calls,2);assert.equal(checkpoints.last,16);
 let retries=0;const throttled=new IdleCheckpoint({revision:()=>8,isIdle:()=>true,checkpoint:async()=>({done:++retries===2,revision:8}),background:promise=>work=promise,delay:1,retryDelay:1});throttled.schedule();await work;assert.equal(retries,2);assert.equal(throttled.last,8);
});


test("kernel instantiation cannot open a lease after its admission signal expires",async()=>{
 const {RequestPool}=await import("../request-pool.mjs");let entered,release,leases=0;
 const began=new Promise(resolve=>entered=resolve),blocked=new Promise(resolve=>release=resolve),controller=new AbortController();
 const pool=new RequestPool(async()=>{entered();await blocked;return{usable:()=>true,memoryBytes:()=>65536};},1);
 const expired=assert.rejects(pool.withHost(()=>{leases++;},{signal:controller.signal}),/DeadlineExceeded/);
 await began;controller.abort();release();await expired;assert.equal(leases,0);
});
