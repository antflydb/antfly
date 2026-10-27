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
test('app observers bound active namespaces and never retain completed views or request contexts',async()=>{
 const{AppReadViews}=await import('../app-read-views.mjs');let release,calls=0;const held=new Promise(resolve=>release=resolve),created=[],cache=new AppReadViews(env=>{created.push(env);return{open:async options=>{calls++;if(options.hold)await held;return{version:calls};}};},{maxStores:2}),bucket={},env={DATABASE:bucket,LOCAL_METADATA_READS:'true',ANTFLY_INCARNATION:'a',API_TOKEN:'must-not-retain',ctx:{waitUntil(){}}};
 const a=cache.get(env),first=a.open({hold:true}),b=cache.get({...env,ANTFLY_INCARNATION:'b'}),second=b.open({hold:true});assert.equal(cache.get({...env,ANTFLY_INCARNATION:'c'}),undefined);assert.equal(cache.metrics().app_read_view_stores,2);assert.equal(cache.metrics().app_read_view_active,2);assert.ok(created.every(e=>!Object.hasOwn(e,'API_TOKEN')&&!Object.hasOwn(e,'ctx')));release();await Promise.all([first,second]);assert.equal(cache.metrics().app_read_view_active,0);const c=cache.get({...env,ANTFLY_INCARNATION:'c'});assert.notDeepEqual(await c.open({}),await c.open({}));assert.equal(cache.metrics().app_read_view_evictions,1);
});
test('app observers isolate bucket bindings and leave physical recovery to coordinator actors',async()=>{
 const{AppReadViews}=await import('../app-read-views.mjs');let created=0;const cache=new AppReadViews(()=>{created++;return{open:async()=>({})};}),env={DATABASE:{},LOCAL_METADATA_READS:'true',ANTFLY_INCARNATION:'a'};
 const a=cache.get(env);assert.equal(cache.get(env),a);assert.notEqual(cache.get({...env,DATABASE:{}}),a);assert.equal(created,2);
 for(const change of[{LOCAL_METADATA_READS:'false'},{READ_ONLY_SNAPSHOTS:'false'},{__ANTFLY_PHYSICAL_EPOCH:1},{__ANTFLY_PHYSICAL_EPOCH:100}])assert.equal(cache.get({...env,...change}),undefined);assert.equal(created,2);
});
import {openReadView,withReadView,metadataKeys} from "../read-view.mjs";
import {PublicationScheduler} from "../publication-scheduler.mjs";
import {metadataCollectionDue} from "../maintenance-policy.mjs";
import {tenantEnv,tenantEnvs} from "../tenants.mjs";
import {createR2Transport} from "../../../zig/lib/objectstore/wasm/r2.mjs";
import {instrumentTransport} from "../performance.mjs";
import {createEdgeImmutableCache} from "../edge-immutable-cache.mjs";
const module=await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless.wasm",import.meta.url)));
const utf8=new TextEncoder();
const record=body=>({etag:crypto.randomUUID(),body:utf8.encode(body),contentType:null});
const fixture=()=>{const bucket=new Bucket();return{bucket,store:new R2CoordinatorStore(module,bucket,"performance")};};
test('queued read cohorts close at native admission and an old completion cannot erase a newer cohort',async()=>{
 const{FreshReadAdmissions}=await import('../fresh-read-admissions.mjs'),admissions=new FreshReadAdmissions({delayMs:0});let entered,admit,observed,finish,nextEntered,nextAdmit,reads=0;
 const queued=new Promise(resolve=>entered=resolve),admitted=new Promise(resolve=>admit=resolve),started=new Promise(resolve=>observed=resolve),done=new Promise(resolve=>finish=resolve),nextQueued=new Promise(resolve=>nextEntered=resolve),nextReady=new Promise(resolve=>nextAdmit=resolve);
 const read=async(ids,close)=>{const ordinal=++reads;if(ordinal===1){entered();await admitted;close();observed();await done;}else{nextEntered();await nextReady;close();}return{ordinal,ids:[...ids]};};
 const a=admissions.run('latest',read,'a',{untilAdmission:true});await queued;const b=admissions.run('latest',read,'b',{untilAdmission:true});admit();await started;
 const c=admissions.run('latest',read,'c',{untilAdmission:true});await nextQueued;finish();assert.deepEqual((await a).ids,['a','b']);assert.equal((await b).ordinal,1);
 const d=admissions.run('latest',read,'d',{untilAdmission:true});nextAdmit();assert.deepEqual((await c).ids,['c','d']);assert.equal((await d).ordinal,2);assert.equal(reads,2);
});
test('read cohort capacity preserves separate mutation metadata admission and releases every waiter',async()=>{
 const{FreshReadAdmissions}=await import('../fresh-read-admissions.mjs'),admissions=new FreshReadAdmissions({delayMs:0});let release;const held=new Promise(resolve=>release=resolve),read=async(_,close)=>{await held;close();return{ok:true};};
 const readers=Array.from({length:64},()=>admissions.run('read',read,undefined,{untilAdmission:true}));await assert.rejects(admissions.run('overflow-read',read),/CoordinatorCommitBusy/);
 const commits=Array.from({length:16},()=>admissions.run('metadata',read,undefined,{untilAdmission:true,admissionClass:'commit'}));await assert.rejects(admissions.run('overflow-commit',read,undefined,{admissionClass:'commit'}),/CoordinatorCommitBusy/);
 release();assert.equal((await Promise.all([...readers,...commits])).length,80);assert.deepEqual(await admissions.run('recovered',async()=>({ok:true})),{ok:true});
 await assert.rejects(admissions.run('failed',async()=>{throw Error('failed observation');},undefined,{untilAdmission:true}),/failed observation/);assert.deepEqual(await admissions.run('failed',async()=>({recovered:true})),{recovered:true});
});
test('native observations close admission before transport I/O even without pool stage callbacks',async()=>{
 const{instantiateHost}=await import('../../../zig/lib/objectstore/wasm/asyncify.mjs'),bucket=new Bucket(),namespace='admission-hook',seed=new R2CoordinatorStore(module,bucket,namespace);await seed.transaction(tx=>tx.put('one',1));
 const host=await instantiateHost(module,null),raw=createR2Transport({database:bucket});let closed=false,priority;
 const store=new R2CoordinatorStore(module,bucket,namespace,{transport:async(...args)=>{assert.equal(closed,true);return raw(...args);},readKernel:{request(input,{onStage,...options}){priority=options.priority;return host.request(input,options);}}});
 assert.equal(await store.readOnly(tx=>tx.get('one'),{onAdmission:()=>{closed=true;},priority:'commit'}),1);assert.equal(priority,'commit');
});
function edgeFixture(){
 const entries=new Map(),storage={async open(){return{async match(key){return entries.get(key.url)?.clone();},async put(key,response){entries.set(key.url,new Response(await response.arrayBuffer(),{headers:response.headers}));}};}};
 const key='antfly/artifacts/sha256/aa/'+'b'.repeat(62),request={op:'get',bucket:'database',key},physical={},transport=instrumentTransport(async req=>({json:{bucket:req.bucket,key:req.key,etag:'confirmed',content_length:3},body:utf8.encode('abc')}),physical);
 return{entries,storage,key,request,physical,transport,wrap:options=>createEdgeImmutableCache({scope:'one',storage,...options})};
}
test('edge cache scopes require a distinct bucket incarnation independently of logical storage names',async()=>{
 const{edgeCacheForEnv}=await import('../edge-immutable-cache.mjs'),f=edgeFixture(),env={EDGE_IMMUTABLE_CACHE:'true',ANTFLY_INCARNATION:'same-name'};
 assert.throws(()=>edgeCacheForEnv(env),/EdgeImmutableCacheScopeRequired/);
 for(const suffix of ['aa','ab']){const wrap=edgeCacheForEnv({...env,EDGE_IMMUTABLE_CACHE_SCOPE:'01234567-0123-4567-89ab-0123456789'+suffix},{storage:f.storage});await wrap(f.transport)(f.request);}assert.equal(f.physical.calls,2);
 await edgeCacheForEnv({...env,EDGE_IMMUTABLE_CACHE_SCOPE:'01234567-0123-4567-89ab-0123456789aa'},{storage:f.storage})(f.transport)(f.request);assert.equal(f.physical.calls,2);
});
test('edge immutable cache survives reconstruction while mutable metadata and misses stay on R2',async()=>{
 const f=edgeFixture(),events=[],first=f.wrap({onMetrics:value=>events.push(value)})(f.transport);await first(f.request);assert.equal(f.physical.calls,1);
 const reopened=f.wrap({onMetrics:value=>events.push(value)})(f.transport);assert.equal(new TextDecoder().decode((await reopened(f.request)).body),'abc');assert.equal(f.physical.calls,1);assert.ok(events.some(e=>e.edge_cache_hits===1));
 for(const key of ['antfly/progress/ns/HEAD','antfly/coord-v1/'+'a'.repeat(64)+'/CHECKPOINT','antfly/coord-v1/'+'a'.repeat(64)+'/CHECKPOINT_REQUIRED']){await reopened({...f.request,key});await reopened({...f.request,key});}assert.equal(f.physical.calls,7);assert.equal(f.entries.size,1);
});
test('edge immutable cache isolates namespace conditions ranges and response budgets',async()=>{
 const f=edgeFixture(),run=f.wrap()(f.transport);await run(f.request);await assert.rejects(run({...f.request,max_response_bytes:2}),e=>e.status===4);assert.equal(f.physical.calls,1);
 const ranged=await run({...f.request,range:{offset:1,length:1},if_match_etag:'confirmed',max_response_bytes:1});assert.equal(new TextDecoder().decode(ranged.body),'b');assert.equal(ranged.json.content_length,3);assert.equal(f.physical.calls,1);
 const conditional=f.wrap()(async()=>{throw Object.assign(Error('condition failed'),{status:2});});await assert.rejects(conditional({...f.request,if_match_etag:'other'}),error=>error.status===2);
 await f.wrap({scope:'other'})(f.transport)(f.request);assert.equal(f.physical.calls,2);
 const value=await run(f.request);value.body[0]=0;assert.equal(new TextDecoder().decode((await run(f.request)).body),'abc');
});
test('edge immutable cache admits only confirmed creates and never caches absence or ambiguous writes',async()=>{
 const f=edgeFixture(),fail=new Error('ambiguous PUT'),write={...f.request,op:'put',if_none_match:true};
 await assert.rejects(f.wrap()(async()=>{throw fail;})(write,utf8.encode('abc')),error=>error===fail);assert.equal(f.entries.size,0);
 let reads=0;const missing=f.wrap()(async()=>{reads++;throw Object.assign(Error('missing'),{status:1});});for(let i=0;i<2;i++)await assert.rejects(missing(f.request),e=>e.status===1);assert.equal(reads,2);assert.equal(f.entries.size,0);
 await f.wrap()(async()=>({json:{etag:'confirmed'}}))(write,utf8.encode('abc'));await f.wrap()(async()=>{throw Error('unexpected R2 read');})(f.request);
 const fromUpload=await f.wrap()(async()=>{throw Error('unexpected ranged R2 read');})({...f.request,range:{offset:1,length:1},if_match_etag:'confirmed',max_response_bytes:1});assert.equal(new TextDecoder().decode(fromUpload.body),'b');
 const size=f.entries.size;await f.wrap()(async()=>({json:{etag:'unconditional'}}))({...write,if_none_match:false},utf8.encode('bad'));assert.equal(f.entries.size,size);assert.equal(new TextDecoder().decode((await f.wrap()(f.transport)(f.request)).body),'abc');
});
test('edge immutable cache bounds stored bodies and rejects corrupt entries without weakening R2 errors',async()=>{
 const f=edgeFixture(),run=f.wrap({maxObjectBytes:2})(f.transport);await run(f.request);await run(f.request);assert.equal(f.entries.size,0);assert.equal(f.physical.calls,2);
 await f.wrap()(f.transport)(f.request);const key=f.entries.keys().next().value;f.entries.set(key,new Response('corrupt',{headers:{'Content-Length':'7','x-antfly-object':'!!!'}}));assert.equal(new TextDecoder().decode((await f.wrap()(f.transport)(f.request)).body),'abc');assert.equal(f.physical.calls,4);
 const entry=f.entries.get(key);f.entries.set(key,new Response('xyz',{headers:entry.headers}));assert.equal(new TextDecoder().decode((await f.wrap()(f.transport)(f.request)).body),'abc');assert.equal(f.physical.calls,5);
 const original=Error('R2 transport failed');await assert.rejects(f.wrap()(async()=>{throw original;})({...f.request,key:f.key.replace('aa/','ab/')}),e=>e===original);
});
test('edge immutable cache outages fall back while resource failures remain visible',async()=>{
 const f=edgeFixture(),events=[],failed=message=>({async open(){throw Error(message);}});await f.wrap({storage:failed('unavailable'),onMetrics:event=>events.push(event)})(f.transport)(f.request);assert.equal(f.physical.calls,1);assert.ok(events.some(e=>e.edge_cache_errors===1));
 await assert.rejects(f.wrap({storage:failed('MemoryLimitExceeded'),onMetrics:event=>events.push(event)})(f.transport)(f.request),/MemoryLimitExceeded/);assert.equal(f.physical.calls,1);assert.ok(events.some(e=>e.edge_cache_resource_errors===1));
});
test('edge immutable cache background writes copy bytes and never replace confirmed R2 receipts',async()=>{
 const f=edgeFixture(),background=[],body=utf8.encode('abc'),wrap=f.wrap({background:promise=>background.push(promise)}),write={...f.request,op:'put',if_none_match:true};await wrap(async()=>({json:{etag:'confirmed'}}))(write,body);body.fill(0);await Promise.all(background);assert.equal(new TextDecoder().decode((await f.wrap()(f.transport)(f.request)).body),'abc');
 const broken={async open(){return{async put(){throw Error('cache unavailable');},async match(){return undefined;}};}};const result=await f.wrap({storage:broken})(async()=>({json:{etag:'durable'}}))(write,utf8.encode('ok'));assert.equal(result.json.etag,'durable');
});
test('edge immutable cache shares bounded call admission across operation wrappers and preserves platform headroom',async()=>{
 const f=edgeFixture(),wrap=()=>f.wrap({budgetKey:()=>f.physical})(f.transport),a=wrap(),b=wrap();for(let i=0;i<50;i++)await(i%2?a:b)(f.request);assert.equal(f.physical.edge_cache_calls,32);assert.equal(f.physical.calls,20);
 f.physical.calls=960-f.physical.edge_cache_calls;const before=f.physical.calls;await assert.rejects(a({...f.request,key:'antfly/progress/ns/HEAD'}),error=>error.status===8);assert.equal(f.physical.calls,before);
});
test('edge immutable cache bounds background fills without queuing request-owned work',async()=>{
 const f=edgeFixture(),fills={active:0,bytes:0},background=[],events=[];let release,active=0,peak=0;const held=new Promise(resolve=>release=resolve),storage={async open(){return{async put(){peak=Math.max(peak,++active);await held;active--;}};}};
 const wrap=f.wrap({storage,fills,background:promise=>background.push(promise),onMetrics:event=>events.push(event)}),write={...f.request,op:'put',if_none_match:true};
 await Promise.all(Array.from({length:12},()=>wrap(async()=>({json:{etag:'confirmed'}}))(write,utf8.encode('abc'))));assert.equal(fills.active,2);assert.ok(fills.bytes<=512*1024);assert.equal(background.length,2);assert.equal(peak,2);assert.equal(events.filter(event=>event.edge_cache_skipped).length,10);
 release();await Promise.all(background);assert.deepEqual(fills,{active:0,bytes:0});
});
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
 const abort=new AbortController(),timer=setTimeout(()=>abort.abort(),5);
 try{await assert.rejects(pool.withHost(async()=>{leases++;},{signal:abort.signal}),/DeadlineExceeded/);}finally{clearTimeout(timer);}
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

test("critical path spans and overlapping physical I/O retain separate clocks and waves",async()=>{
 const {CriticalPath}=await import("../trace.mjs");let now=0;const trace=new CriticalPath({clock:()=>now});await trace.span("read",async()=>{now=7;});assert.equal(trace.report().spans[0].duration_ms,7);
 const counters={};let release;const blocked=new Promise(resolve=>release=resolve),io=instrumentTransport(async()=>{await blocked;return{body:Uint8Array.of(1)};},counters);
 const first=io({op:"get"}),second=io({op:"get"});release();await Promise.all([first,second]);assert.equal(counters.io_waves,1);assert.equal(counters.concurrency_peak,2);assert.ok(counters.wait_union_ms<=counters.wait_ms);
});

test("native cold recovery reads a bounded contiguous window and still fences stale owners",async()=>{
 const bucket=new Bucket(),owner=Array(16).fill(7),initial=new R2CoordinatorStore(module,bucket,"read-ahead",{owner,clock:()=>100000});
 for(let i=0;i<24;i++)await initial.transaction(tx=>tx.put("value",i));
 const counters={},transport=instrumentTransport(createR2Transport({database:bucket}),counters),reopened=new R2CoordinatorStore(module,bucket,"read-ahead",{transport,clock:()=>100000});
 assert.equal(await reopened.transaction(tx=>tx.get("value")),23);assert.ok(counters.concurrency_peak>=8);assert.ok(counters.io_waves<counters.calls/2,"immutable prefix recovery must overlap storage waits");
 await assert.rejects(initial.transaction(tx=>tx.put("value",99)),/ImmutableLogFenced/);
});

test("read ahead observes a fresh seal at a hole even with later speculative frames",async()=>{
 const {recoveryReadAhead}=await import("../recovery-read-ahead.mjs"),{ObjectStoreError}=await import("../../../zig/lib/objectstore/wasm/r2.mjs");
 const epoch=Array(16).fill(1),owner=Array(16).fill(2),base="db",prefix=base+"/epochs/"+"01".repeat(16)+"/";let sealReads=0,seal;
 const frame=i=>({kind:"commit",epoch,slot:i,parent_revision:i+1,state:{epoch,owner,next:i+1,revision:i+2,root:"null"}}),reply=value=>({body:utf8.encode(JSON.stringify(value))});
 const transport=recoveryReadAhead(async request=>{if(request.key===base+"/CHECKPOINT")return reply({state:{epoch,owner,next:0,revision:1,root:"null"}});if(request.key.endsWith("SEALED")){sealReads++;if(seal)return reply(seal);throw new ObjectStoreError(1,"missing");}const i=Number(request.key.slice(-25,-5));if(i===0||i===3)return reply(frame(i));throw new ObjectStoreError(1,"hole");});
 await transport({op:"get",key:base+"/CHECKPOINT"});await assert.rejects(transport({op:"get",key:prefix+"SEALED"}),error=>error.status===1);
 await transport({op:"get",key:prefix+"00000000000000000000.json"});seal={kind:"seal",state:{epoch:Array(16).fill(3)}};
 assert.equal(JSON.parse(new TextDecoder().decode((await transport({op:"get",key:prefix+"SEALED"})).body)).kind,"seal");assert.equal(sealReads,2);
});

test("native WAL fast commit keeps generic pages stable and atomic outbox through recovery",async()=>{
 const {encode}=await import("../r2-coordinator-store.mjs"),{metadataTreeRoot}=await import("../metadata-root.mjs");const bucket=new Bucket(),store=new R2CoordinatorStore(module,bucket,"hot-root");await store.transaction(tx=>tx.put("generic",42));
 const first=record(JSON.stringify({latest_lsn:1,padding:'x'.repeat(1024)})),second=record(JSON.stringify({latest_lsn:2,padding:'y'.repeat(1024)}));let result=await store.commitWal({wal_key:"wal",value:encode(first),publication_key:"publication",watermark:"1",now_ms:1000,id:"job"});assert.equal(result.applied,true);assert.equal(result.publication.enqueue,true);
 assert.ok(store.state.root.length<4096,"inline UTF-8 records must fit the bounded log root");const root=metadataTreeRoot(store.state);result=await store.commitWal({wal_key:"wal",expected_etag:first.etag,value:encode(second),publication_key:"publication",watermark:"2",now_ms:1001,id:"other"});assert.equal(result.publication.enqueue,false);assert.deepEqual(metadataTreeRoot(store.state),root);
 const lost=await store.commitWal({wal_key:"wal",expected_etag:first.etag,value:encode(first),publication_key:"publication",watermark:"3",now_ms:1002,id:"lost"});assert.equal(lost.applied,false);
 const reopened=new R2CoordinatorStore(module,bucket,"hot-root");assert.deepEqual(await reopened.transaction(tx=>tx.get("wal")),second);assert.equal(await reopened.transaction(tx=>tx.get("generic")),42);assert.equal((await reopened.transaction(tx=>tx.get("publication"))).id,"job");
});


test("native WAL commit survives lost acknowledgement and fences the old owner",async()=>{
 const {encode}=await import("../r2-coordinator-store.mjs");const bucket=new Bucket(),raw=createR2Transport({database:bucket});let lose=false;
 const store=new R2CoordinatorStore(module,bucket,"hot-lost-ack",{transport:async(request,body)=>{const reply=await raw(request,body);if(lose&&request.op==="put"&&/epochs\/.*\/\d+\.json$/.test(request.key)){lose=false;throw new Error("lost acknowledgement");}return reply;}});
 await store.transaction(tx=>tx.put("generic",42));lose=true;const head=record(JSON.stringify({latest_lsn:1}));
 await assert.rejects(store.commitWal({wal_key:"wal",value:encode(head),publication_key:"publication",watermark:"1",now_ms:1000,id:"job"}),/ObjectStoreTransportFailed/);
 const reopened=new R2CoordinatorStore(module,bucket,"hot-lost-ack");assert.deepEqual(await reopened.transaction(tx=>tx.get("wal")),head);assert.equal((await reopened.transaction(tx=>tx.get("publication"))).id,"job");
 await assert.rejects(store.transaction(tx=>tx.put("generic",99)),/ImmutableLogFenced/);
});


test("expired native WAL admission performs no recovery I/O",async()=>{
 const bucket=new Bucket();let calls=0;const store=new R2CoordinatorStore(module,bucket,"expired-hot",{clock:()=>1000,transport:async()=>{calls++;throw Error("unexpected I/O");}});
 await assert.rejects(store.commitWal({}, {deadlineMs:999}),/DeadlineExceeded/);assert.equal(calls,0);
});

test('shared kernel admission rotates tenants and lets aged maintenance run',async()=>{
 const {RequestPool}=await import('../request-pool.mjs');let now=0,enter,release;const started=new Promise(r=>enter=r),blocked=new Promise(r=>release=r),order=[];
 const pool=new RequestPool(async()=>({usable:()=>true,memoryBytes:()=>65536}),1,{clock:()=>now});
 const held=pool.withHost(async()=>{enter();await blocked;},{fairKey:'a',priority:'publication'});await started;
 const queued=[['a1','a','publication'],['a2','a','publication'],['b','b','publication'],['maintenance','c','maintenance']].map(([name,fairKey,priority])=>pool.withHost(()=>order.push(name),{fairKey,priority}));
 now=6000;release();await Promise.all([held,...queued]);assert.deepEqual(order,['b','a1','maintenance','a2']);
});

test('read cohorts close before observation and never reuse a frontier for late callers',async()=>{
 const{FreshReadAdmissions}=await import('../fresh-read-admissions.mjs');const admissions=new FreshReadAdmissions({delayMs:1});let frontier=1,entered,release,calls=0;
 const started=new Promise(r=>entered=r),blocked=new Promise(r=>release=r);
 const read=async traceIds=>{calls++;const captured={frontier,traceIds};entered();await blocked;return captured;};
 const first=admissions.run('latest',read,'a'),second=admissions.run('latest',read,'b');await started;frontier=2;
 const later=await admissions.run('latest',async()=>{calls++;return{frontier};});assert.equal(later.frontier,2);release();const a=await first,b=await second;a.frontier=99;assert.equal(b.frontier,1);assert.deepEqual(b.traceIds,["a","b"]);assert.equal(calls,2);
 await assert.rejects(admissions.run('error',()=>{throw Error('failed read');}),/failed read/);assert.equal((await admissions.run('error',()=>({frontier:3}))).frontier,3);
});

test('streamed ingress bytes remain bounded across requests and recover after rejection',async()=>{
 const{IngressBudget}=await import('../ingress-budget.mjs'),budget=new IngressBudget(100),a=budget.open(),b=budget.open();a.add(60);b.add(30);assert.equal(budget.used,90);
 assert.throws(()=>b.add(20),/CoordinatorCommitBusy/);assert.equal(budget.used,90);a.release();a.release();assert.equal(budget.used,30);b.add(70);assert.equal(budget.used,100);b.release();assert.equal(budget.used,0);assert.throws(()=>a.add(1),/IngressReservationClosed/);
});

test('prefetched read admission keeps its original grace deadline across kernel queueing',async()=>{
 const view={format:1,entries:[['antfly/progress/ns/HEAD',record('7')]]};let calls=0;
 const coordinator={openReadView:async()=>{calls++;throw Error('unexpected refresh');}};
 const transport=withReadView(async()=>{throw Error('unexpected fallback');},coordinator,'ns',{initial:view,admittedAt:performance.now()});
 assert.equal(new TextDecoder().decode((await transport({op:'get',key:'antfly/progress/ns/HEAD'})).body),'7');assert.equal(calls,0);
 const expired=withReadView(async()=>{},coordinator,'ns',{initial:view,admittedAt:performance.now()-30001});await assert.rejects(expired({op:'get',key:'antfly/progress/ns/HEAD'}),error=>error.status===8);assert.equal(calls,0);
});
test('queued read preparation preserves pinned identity and fails closed on transport or fencing errors',async()=>{
 const{prepareReadAdmission}=await import('../read-admission.mjs'),{CriticalPath}=await import('../trace.mjs');let options,calls=0;
 const stub={async openReadView(value){calls++;options=value;return{format:1,version:value.version??'1',entries:[]};}},env={COORDINATOR_BACKEND:'r2',ANTFLY_INCARNATION:'ns',COMMITS:{getByName(){return stub;}}};
 const trace=new CriticalPath(),admitted=await prepareReadAdmission(env,{op:'search',exact_state:{generation:'7'}},trace);
 assert.equal(admitted.view.version,'7');assert.equal(options.trace_id,trace.id);assert.equal(admitted.physical.rpc_calls,1);
 assert.equal(await prepareReadAdmission(env,{op:'lookup',cursor:'opaque'},trace),undefined);assert.equal(calls,1);
 const fenced=Error('ImmutableLogFenced');stub.openReadView=async()=>{throw fenced;};await assert.rejects(prepareReadAdmission(env,{op:'lookup'},trace),error=>error===fenced);
 stub.openReadView=async()=>{throw Error('openReadView does not implement this RPC');};assert.equal((await prepareReadAdmission(env,{op:'lookup'},trace)).legacy,true);
});


test("weighted kernel reservations bound retained memory and let a large waiting variant drain",async()=>{
 const{RequestPool}=await import('../request-pool.mjs');let now=0,releaseFirst,releaseSecond,firstEntered,secondEntered;const firstStarted=new Promise(r=>firstEntered=r),secondStarted=new Promise(r=>secondEntered=r),firstGate=new Promise(r=>releaseFirst=r),secondGate=new Promise(r=>releaseSecond=r),order=[];
 const pool=new RequestPool(async job=>({usable:()=>true,memoryBytes:()=>job.reservationBytes}),2,{clock:()=>now,maxReservedBytes:32,reservationBytes:32,recycleBytes:100});
 const compact={memoryReservationBytes:16,kernelVariant:'compact'};
 const first=pool.withHost(async()=>{firstEntered();await firstGate;},compact),second=pool.withHost(async()=>{secondEntered();await secondGate;},compact);await Promise.all([firstStarted,secondStarted]);assert.equal(pool.reservedBytes,32);assert.equal(pool.memoryBytes(),32);
 const large=pool.withHost(()=>{order.push('large');assert.equal(pool.reservedBytes,32);assert.ok(pool.memoryBytes()<=32);},{kernelVariant:'full'});now=6000;
 releaseFirst();await first;const small=pool.withHost(()=>{order.push('small');assert.ok(pool.memoryBytes()<=32);},compact);await new Promise(r=>setTimeout(r,0));assert.deepEqual(order,[],'aged large reservation must drain running smaller work');
 releaseSecond();await Promise.all([second,large,small]);assert.deepEqual(order,['large','small']);assert.equal(pool.reservedBytes,0);assert.ok(pool.memoryBytes()<=32);
 await assert.rejects(pool.withHost(()=>{}, {memoryReservationBytes:33}),/InvalidKernelMemoryReservation/);
});

test("cancelled weighted reservations release waiting work without opening another host",async()=>{
 const{RequestPool}=await import('../request-pool.mjs');let release,entered;const started=new Promise(r=>entered=r),gate=new Promise(r=>release=r),controller=new AbortController();let now=0;
 const pool=new RequestPool(async job=>({usable:()=>true,memoryBytes:()=>job.reservationBytes}),2,{clock:()=>now,maxReservedBytes:32,reservationBytes:16});
 const held=pool.withHost(async()=>{entered();await gate;});await started;
 const blocked=assert.rejects(pool.withHost(()=>assert.fail('cancelled reservation ran'),{memoryReservationBytes:32,signal:controller.signal}),/DeadlineExceeded/);now=6000;
 const available=pool.withHost(()=>42);controller.abort();assert.equal(await available,42);await blocked;release();await held;assert.equal(pool.reservedBytes,0);
});


test("eight MiB observation kernels preserve fresh reads and fencing over a populated metadata tree",async()=>{
 const{RequestPool}=await import('../request-pool.mjs'),{instantiateHost}=await import('../../../zig/lib/objectstore/wasm/asyncify.mjs');
 const observe=await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless-observe.wasm',import.meta.url))),pool=new RequestPool(()=>instantiateHost(observe,null),2,{maxReservedBytes:16*1024*1024,reservationBytes:8*1024*1024});
 const imports={};for(const item of WebAssembly.Module.imports(observe)){imports[item.module]??={};imports[item.module][item.name]=()=>0;}const instance=await WebAssembly.instantiate(observe,imports);assert.throws(()=>instance.exports.memory.grow(128),RangeError);
 const bucket=new Bucket(),store=new R2CoordinatorStore(module,bucket,'bounded-observer',{readKernel:pool});
 for(let batch=0;batch<8;batch++)await store.transaction(tx=>{for(let i=0;i<128;i++)tx.put('row/'+(batch*128+i),{text:'x'.repeat(1024)});});
 const values=await Promise.all(Array.from({length:8},(_,i)=>store.readOnly(tx=>tx.get('row/'+(i*128)))));assert.ok(values.every(v=>v.text==='x'.repeat(1024)));assert.ok(pool.memoryBytes()<=16*1024*1024);assert.ok(pool.slots.every(s=>!s.host||s.host.memoryBytes()<=8*1024*1024));
 const replacement=new R2CoordinatorStore(module,bucket,'bounded-observer');await replacement.transaction(tx=>tx.get('row/0'));await assert.rejects(store.readOnly(tx=>tx.get('row/0')),/ImmutableLogFenced/);
});


test("shared writer reservations reuse matching variants and prioritize durable commits",async()=>{
 const{RequestPool}=await import('../request-pool.mjs');const created=[];
 const pool=new RequestPool(async job=>{created.push(job.hostKey);return{usable:()=>true,memoryBytes:()=>job.reservationBytes};},3,{maxReservedBytes:48,reservationBytes:16,defaultKernelVariant:'compact',recycleBytes:100});
 const full={memoryReservationBytes:32,kernelVariant:'full'};
 for(const options of[{},full,{},full])await pool.withHost(()=>assert.ok(pool.memoryBytes()<=48),options);
 assert.deepEqual(created,['compact','full']);assert.equal(pool.memoryBytes(),48);
 let entered,release;const began=new Promise(r=>entered=r),blocked=new Promise(r=>release=r),order=[];
 const lane=new RequestPool(async()=>({usable:()=>true,memoryBytes:()=>16}),1,{maxReservedBytes:16,reservationBytes:16});
 const held=lane.withHost(async()=>{entered();await blocked;});await began;
 const prepare=lane.withHost(()=>order.push('prepare'),{priority:'interactive'}),commit=lane.withHost(()=>order.push('commit'),{priority:'commit'});release();await Promise.all([held,prepare,commit]);assert.deepEqual(order,['commit','prepare']);
});

test("native reader views authenticate local metadata without writer ownership or RPC",async()=>{
 const {NativeReadViews}=await import('../native-read-views.mjs'),{RequestPool}=await import('../request-pool.mjs'),{instantiateHost}=await import('../../../zig/lib/objectstore/wasm/asyncify.mjs'),{openAuthority}=await import('../coordinator-authority.mjs');
 const observe=await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless-observe.wasm',import.meta.url))),bucket=new Bucket(),originalGet=bucket.get.bind(bucket);
 const originalPut=bucket.put.bind(bucket);bucket.put=(key,body,options)=>originalPut(key,typeof body==='string'?utf8.encode(body):body,options);
 bucket.get=async(...args)=>{const value=await originalGet(...args);if(value)value.json=async()=>JSON.parse(new TextDecoder().decode(bucket.objects.get(args[0]).body));return value;};
 const env={DATABASE:bucket,ANTFLY_INCARNATION:'direct-reader'},keys=await metadataKeys(env.ANTFLY_INCARNATION),writer=new R2CoordinatorStore(module,bucket,env.ANTFLY_INCARNATION);
 await openAuthority(env,'r2');await writer.transaction(tx=>{tx.put(keys.head,record('7'));tx.put(keys.progress+'READ_PINS/7',record(String(BigInt(Date.now()+600000)*1000000n)));});
 const pool=new RequestPool(job=>instantiateHost(job.hostKey==='observe'?observe:module,null),4,{maxReservedBytes:32*1024*1024,reservationBytes:16*1024*1024,recycleBytes:12*1024*1024}),views=new NativeReadViews(observe,env,{kernel:pool}),writes=bucket.writes;
 views.authorityAdmission.value={descriptor:{mode:'sqlite'}};await assert.rejects(views.open(),/CoordinatorMigrationRequired/);assert.equal(views.authorityAdmission.value,null);
 const results=await Promise.all(Array.from({length:8},()=>views.open()));assert.ok(results.every(v=>v.version==='7'));assert.equal(bucket.writes,writes);assert.equal(views.store.acquired,false);assert.equal(views.store.state,null);assert.ok(pool.memoryBytes()<=32*1024*1024);
 const replacement=new R2CoordinatorStore(module,bucket,env.ANTFLY_INCARNATION);await replacement.transaction(tx=>{tx.put(keys.head,record('8'));tx.put(keys.progress+'READ_PINS/8',record(String(BigInt(Date.now()+600000)*1000000n)));});assert.equal((await views.open()).version,'8');await assert.rejects(writer.readOnly(tx=>tx.get(keys.head)),/ImmutableLogFenced/);
 await replacement.transaction(tx=>tx.put('__antfly_r2_import',{phase:'freeze'}));await assert.rejects(views.open(),/CoordinatorMigrationRequired/);await replacement.transaction(tx=>tx.put('__antfly_r2_import',{phase:'active'}));assert.equal((await views.open()).version,'8');
 views.store.observedState.root='forged';await assert.rejects(views.open(),/InvalidImmutableLog/);
});

test("local read admission falls back only for pin admission or unavailable authority",async()=>{
 const{prepareReadAdmission}=await import('../read-admission.mjs'),{CriticalPath}=await import('../trace.mjs');let rpc=0,local=0;const view={format:1,version:'7',entries:[]},env={COORDINATOR_BACKEND:'r2',ANTFLY_INCARNATION:'local',COMMITS:{getByName:()=>({openReadView:async()=>{rpc++;return view;}})}};
 for(const message of['ReadSnapshotNeedsPin','ReadSnapshotNeedsAdmission','CoordinatorAuthorityMissing','CoordinatorRecovering']){
  const admitted=await prepareReadAdmission(env,{op:'lookup'},new CriticalPath(),{open:async()=>{local++;throw Error(message);}});assert.equal(admitted.view,view);assert.equal(admitted.physical.rpc_calls,1);
 }
 const admitted=await prepareReadAdmission(env,{op:'lookup'},new CriticalPath(),{open:async()=>{local++;return view;}});assert.equal(admitted.physical.rpc_calls,0);assert.equal(admitted.physical.native_metadata_views,1);assert.equal(rpc,4);
 for(const message of['InvalidImmutableLog','ImmutableLogFenced','CoordinatorMigrationRequired','ObjectStoreTransportFailed','OutOfMemory'])await assert.rejects(prepareReadAdmission(env,{op:'lookup'},new CriticalPath(),{open:async()=>{throw Error(message);}}),new RegExp(message));assert.equal(rpc,4);assert.equal(local,5);
});

test("read-only authority checks never initialize a missing database",async()=>{
 const{openAuthority}=await import('../coordinator-authority.mjs');const bucket=new Bucket();await assert.rejects(openAuthority({DATABASE:bucket,ANTFLY_INCARNATION:'uninitialized'},'r2',{allowCreate:false}),/CoordinatorAuthorityMissing/);assert.equal(bucket.writes,0);assert.equal(bucket.objects.size,0);
});

test("warm unowned observations prefetch authenticated tails across owner and epoch changes",async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket}),now=Date.now();let sealReads=0,active=0,peak=0;
 const writer=new R2CoordinatorStore(module,bucket,'warm-observer-tail',{clock:()=>now});await writer.transaction(tx=>tx.put('value',0));
 const observer=new R2CoordinatorStore(module,bucket,'warm-observer-tail',{clock:()=>now,transport:async(request,body)=>{if(request.op!=='get')throw Error('observer wrote');if(request.key.endsWith('/SEALED'))sealReads++;active++;peak=Math.max(peak,active);try{await new Promise(resolve=>setTimeout(resolve,1));return await raw(request,body);}finally{active--;}}});
 assert.equal(await observer.readOnly(tx=>tx.get('value')),0);for(let i=1;i<=80;i++)await writer.transaction(tx=>tx.put('value',i));sealReads=0;peak=0;
 assert.equal(await observer.readOnly(tx=>tx.get('value')),80);assert.ok(peak>1&&peak<=9);assert.ok(sealReads<20,`bounded windows should cover contiguous commits: ${sealReads} seal reads`);assert.equal(observer.acquired,false);
 const replacement=new R2CoordinatorStore(module,bucket,'warm-observer-tail',{clock:()=>now});await replacement.transaction(tx=>tx.put('value',81));assert.equal(await observer.readOnly(tx=>tx.get('value')),81);await assert.rejects(writer.readOnly(tx=>tx.get('value')),/ImmutableLogFenced/);
});

test('warm unowned proof reads overlap checkpoint and tail without reusing negative results',async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket});let now=Date.now(),hold=false,release,started;
 const writer=new R2CoordinatorStore(module,bucket,'parallel-observer-proof',{clock:()=>now});await writer.transaction(tx=>tx.put('value',1));
 const starts=[],held=new Promise(r=>release=r),entered=new Promise(r=>started=r);
 const observer=new R2CoordinatorStore(module,bucket,'parallel-observer-proof',{clock:()=>now,transport:async(request,body)=>{if(request.op!=='get')throw Error('observer wrote');starts.push(request.key);if(hold&&request.key.endsWith('/CHECKPOINT')){started();await held;}return raw(request,body);}});
 assert.equal(await observer.readOnly(tx=>tx.get('value')),1);starts.length=0;hold=true;
 const reading=observer.readOnly(tx=>tx.get('value'));await entered;await new Promise(r=>setTimeout(r,0));
 try{assert.ok(starts.some(k=>k.endsWith('/SEALED')),'seal probe must start before checkpoint completes');assert.ok(starts.some(k=>/\/[0-9]{20}\.json$/.test(k)),'tail probe must start before checkpoint completes');}finally{hold=false;release();}
 assert.equal(await reading,1);await writer.transaction(tx=>tx.put('value',2));assert.equal(await observer.readOnly(tx=>tx.get('value')),2,'negative probes must not survive another request');
 now+=1001;await writer.checkpointHint();assert.equal(await observer.readOnly(tx=>tx.get('value')),2,'advanced checkpoint remains authoritative');
 const replacement=new R2CoordinatorStore(module,bucket,'parallel-observer-proof',{clock:()=>now});await replacement.transaction(tx=>tx.put('value',3));assert.equal(await observer.readOnly(tx=>tx.get('value')),3,'native recovery follows a replaced owner');assert.equal(observer.acquired,false);
});

test('ambiguous metadata deadlines retain native reservations and recover the late durable commit', {timeout:5000},async()=>{
 const{RequestPool}=await import('../request-pool.mjs'),{instantiateHost}=await import('../../../zig/lib/objectstore/wasm/asyncify.mjs'),compact=await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless-compact.wasm',import.meta.url)));
 const bucket=new Bucket(),raw=createR2Transport({database:bucket});let created=0,hold=false,release,entered;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r);
 const pool=new RequestPool(async()=>{created++;return instantiateHost(compact,null);},1,{maxReservedBytes:16*1024*1024,reservationBytes:16*1024*1024,recycleBytes:12*1024*1024}),store=new R2CoordinatorStore(module,bucket,'late-native-commit',{kernel:pool,transport:async(req,body)=>{if(hold&&req.op==='put'&&req.key.endsWith('.json')&&JSON.parse(new TextDecoder().decode(body)).kind==='commit'){entered();await held;}return raw(req,body);}});
 await store.transaction(tx=>tx.put('value',1));hold=true;
 const writing=store.transaction(tx=>{tx.put('value',2);store.deadline=Date.now()+25;});const failed=assert.rejects(writing,/DeadlineExceeded/);await started;
 try{await failed;assert.ok(store.ioDrain);assert.equal(pool.reservedBytes,16*1024*1024);assert.equal(pool.slots[0].busy,true);assert.equal(pool.slots[0].host.usable(),false);assert.ok(pool.memoryBytes()>0);await assert.rejects(store.transaction(tx=>tx.put('value',3)),/CoordinatorRecovering/);assert.equal(created,1,'a draining host must not open replacement capacity');}
 finally{hold=false;release();await failed;await store.ioDrain;}
 assert.equal(pool.reservedBytes,0);assert.equal(await store.transaction(tx=>tx.get('value')),2,'fresh native recovery must authenticate the late commit');assert.equal(created,2);assert.equal(pool.reservedBytes,0);
});

test('late storage resource failures remain observable after native deadlines',{timeout:5000},async()=>{
 const{instantiateHost}=await import('../../../zig/lib/objectstore/wasm/asyncify.mjs'),{resourceFailureCode}=await import('../performance.mjs');
 let release,entered;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r),events=[],counters={},failure=Error('out of memory: SQLITE_NOMEM secret-must-not-be-logged');
 const io=instrumentTransport(async()=>{entered();await held;throw failure;},counters,{onResourceFailure:sample=>events.push(sample)}),host=await instantiateHost(module,io),operation=host.request({op:'coordinate',bucket:'database',prefix:'antfly',namespace:'late-resource',working_set_bytes:4*1024*1024,coordinate:{action:'begin',owner:Array(16).fill(1)}},{deadlineMs:Date.now()+25});
 const expired=assert.rejects(operation,/DeadlineExceeded/);await started;
 try{await expired;assert.ok(host.draining());assert.deepEqual(events,[]);assert.equal(counters.resource_failures,undefined);}finally{release();await expired;await host.draining();}
 assert.equal(counters.resource_failures,1);assert.deepEqual(events,[{event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM',op:'get'}]);assert.ok(!JSON.stringify(events).includes('secret'));
 for(const message of['OutOfMemory','MemoryLimitExceeded','memory limit exceeded','CPULimitExceeded','CPU time limit exceeded'])assert.ok(resourceFailureCode(Error(message)));
 for(const message of['DeadlineExceeded','ObjectIoBudgetExceeded','key missing'])assert.equal(resourceFailureCode(Error(message)),null);
 const same=Error('CPULimitExceeded private'),observed=instrumentTransport(async()=>{throw same;},{},{onResourceFailure:()=>{throw Error('observer failed');}});await assert.rejects(observed({op:'get'}),error=>error===same);
});

test('native inline read views avoid metadata pages across logical storage identities and preserve migration barriers',async()=>{
 const{NativeReadViews}=await import('../native-read-views.mjs'),{openAuthority}=await import('../coordinator-authority.mjs'),{inlineReadSet}=await import('../read-view.mjs'),{MIGRATION}=await import('../r2-coordinator-migration.mjs'),{RequestPool}=await import('../request-pool.mjs'),{instantiateHost}=await import('../../../zig/lib/objectstore/wasm/asyncify.mjs');
 const small=await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless-observe.wasm',import.meta.url))),bucket=new Bucket(),namespace='logical-capsule',storage='distinct-storage-capsule',keys=await metadataKeys(namespace),now=Date.now(),env={DATABASE:bucket,ANTFLY_INCARNATION:namespace,COORDINATOR_STORAGE_ID:storage,COORDINATOR_BACKEND:'r2'};
 await openAuthority(env,'r2');const writer=new R2CoordinatorStore(module,bucket,storage,{inlineReadSet:inlineReadSet(namespace)});
 const pin=version=>keys.progress+'READ_PINS/'+version;await writer.transaction(tx=>{tx.put(keys.head,record('7'));tx.put(keys.wal,record('{"latest_lsn":1}'));tx.put(keys.floor,record('0'));tx.put(pin('7'),record(String(BigInt(now+600000)*1000000n)));});
 const events=[],pool=new RequestPool(()=>instantiateHost(small,null),2,{reservationBytes:8*1024*1024,maxReservedBytes:16*1024*1024}),observer=new NativeReadViews(small,env,{kernel:pool,clock:()=>now,onMetrics:e=>events.push(e),cohortDelayMs:0}),writes=bucket.writes;
 assert.equal((await observer.open()).version,'7');assert.equal(bucket.writes,writes);assert.ok(events.some(e=>e.action==='read'));assert.ok(events.filter(e=>e.action==='read').every(e=>e.operations===0),'authenticated rows must not fetch ordered metadata pages');assert.equal(events.findLast(e=>e.event==='antfly.coordinator_snapshot').inline_root_has_pin,true);
 await writer.transaction(tx=>{tx.put(keys.head,record('8'));tx.put(pin('8'),record(String(BigInt(now+600000)*1000000n)));});assert.equal((await observer.open()).version,'8');assert.ok(events.filter(e=>e.action==='read').every(e=>e.operations===0));
 await writer.transaction(tx=>tx.put(MIGRATION,{phase:'freeze'}));await assert.rejects(observer.open(),/CoordinatorMigrationRequired/);assert.equal(writer.acquired,true);assert.equal(observer.store.acquired,false);
});

test('metadata activation qualifies full compact and observation binaries beyond legacy root limits',async()=>{
 const{verifyInlineMetadataReaders}=await import('../qualification/metadata-compatibility.mjs'),[compact,observe]=await Promise.all(['antfly-serverless-compact.wasm','antfly-serverless-observe.wasm'].map(async name=>WebAssembly.compile(await readFile(new URL('../dist/'+name,import.meta.url)))));
 const result=await verifyInlineMetadataReaders(module,compact,observe);assert.equal(result.format,3);assert.equal(result.readers,3);assert.equal(result.physical_row_reads,0);assert.ok(result.root_bytes>4096&&result.root_bytes<=12288);await assert.rejects(verifyInlineMetadataReaders(module,compact),/ReaderCoverageRequired/);
});

test("point reads keep stable bounded placement across app isolate restarts",async()=>{
 const{readerSlot}=await import('../runtime-routing.mjs');for(const slots of[1,2,3,4]){const seen=new Set();for(let i=0;i<512;i++){const input={op:'lookup',doc_id:'gate/'+i},slot=readerSlot(input,slots,i);assert.ok(slot>=0&&slot<slots);assert.equal(readerSlot(input,slots,0),slot);assert.equal(readerSlot(input,slots,123456),slot);seen.add(slot);}assert.equal(seen.size,slots);}
 for(let i=0;i<512;i+=2)assert.notEqual(readerSlot({op:'lookup',doc_id:'gate/'+i},2),readerSlot({op:'lookup',doc_id:'gate/'+(i+1)},2));assert.equal(readerSlot({op:'count'},2,3),1);assert.equal(readerSlot({op:'lookup',doc_id:'😀/é'},4),readerSlot({op:'lookup',doc_id:'😀/é'},4,9));
});

test("one idle commit advances the native recovery floor without delaying its acknowledgement",async()=>{
 const bucket=new Bucket(),work=[];let now=Date.now();const store=new R2CoordinatorStore(module,bucket,'one-idle-commit',{clock:()=>now,background:promise=>work.push(promise)});
 await store.transaction(tx=>tx.put('value',1));const revision=store.state.revision,checkpoint=()=>JSON.parse(new TextDecoder().decode(bucket.objects.get(store.base+'/CHECKPOINT').body));assert.ok(checkpoint().state.revision<revision);
 now+=1001;await new Promise(resolve=>setTimeout(resolve,0));assert.equal(work.length,1);await Promise.all(work);assert.equal(checkpoint().state.revision,revision);assert.equal(store.idleCheckpoint.last,revision);assert.equal(await store.readOnly(tx=>tx.get('value')),1);
});

test("publication hints retain only confirmed immutable objects within the existing admission charge",async()=>{
 const{PublicationReadHints,normalizeReadHints,readHintBudget}=await import('../publication-read-hints.mjs');const hints=new PublicationReadHints('hints'),key='antfly/artifacts/sha256/aa/'+'b'.repeat(62);let fail=false;
 const transport=hints.wrap(async(request,body)=>{if(fail)throw Error('ambiguous upload');return{json:{etag:'confirmed',content_length:body?.byteLength},body};});
 await transport({op:'put',key,if_none_match:true},utf8.encode('immutable'));await transport({op:'put',key:'antfly/progress/hints/HEAD',if_none_match:true},utf8.encode('7'));assert.equal(hints.entries.size,1);
 fail=true;await assert.rejects(transport({op:'put',key:'antfly/artifacts/sha256/cc/'+'d'.repeat(62),if_none_match:true},utf8.encode('unconfirmed')),/ambiguous/);assert.equal(hints.entries.size,1);fail=false;
 for(let i=0;i<20;i++)await transport({op:'put',key:'antfly/artifacts/sha256/aa/'+i.toString(16).padStart(62,'0'),if_none_match:true},new Uint8Array(1024));assert.ok(hints.bytes<=readHintBudget);assert.ok(hints.entries.size<=8);assert.ok(normalizeReadHints('hints','7',hints.forVersion('7')));
 assert.equal(hints.forHead({op:'put',key:'antfly/progress/other/HEAD'},utf8.encode('7')),undefined);assert.equal(hints.forVersion('18446744073709551616'),undefined);
});

test("publication hints prioritize the current manifest and native facts root over small page churn",async()=>{
 const{PublicationReadHints,normalizeReadHints}=await import('../publication-read-hints.mjs'),hints=new PublicationReadHints('priority'),manifest='antfly/manifests/priority/manifests/7.bin',facts='antfly/artifacts/sha256/aa/'+'b'.repeat(62),root=new Uint8Array(576);root.set(utf8.encode('AFDFACTA'));
 const transport=hints.wrap(async()=>({json:{etag:'durable'}}));await transport({op:'put',key:manifest,if_none_match:true},new Uint8Array(1000));await transport({op:'put',key:facts,if_none_match:true},root);
 for(let i=0;i<30;i++)await transport({op:'put',key:'antfly/artifacts/graph/'+'a'.repeat(64)+'/'+'b'.repeat(32)+'/'+i.toString(16).padStart(64,'0'),if_none_match:true},new Uint8Array(1024));
 const records=new Map(hints.forVersion('7').records);assert.ok(records.has(manifest)&&records.has(facts));assert.equal(normalizeReadHints('priority','8',hints.forVersion('7')),null);assert.ok(!new Map(hints.forVersion('8').records).has(manifest));
});

test("immutable read hints preserve conditional ranges, deadlines and fresh mutable authority",async()=>{
 const key='antfly/artifacts/sha256/aa/'+'b'.repeat(62),manifest='antfly/manifests/hinted/manifests/7.bin';let fallbacks=0;const view={format:1,version:'7',entries:[['antfly/progress/hinted/HEAD',record('7')]],immutable:[[key,{etag:'immutable',body:utf8.encode('abcdef'),contentType:null}],[manifest,{etag:'manifest',body:utf8.encode('manifest'),contentType:null}]]};
 const transport=withReadView(async()=>{fallbacks++;return{};},{},'hinted',{initial:view});const result=await transport({op:'get',bucket:'database',key,range:{offset:2,length:2},if_match_etag:'immutable'});assert.equal(new TextDecoder().decode(result.body),'cd');assert.equal(result.json.content_length,6);assert.equal((await transport({op:'head',key,max_response_bytes:0})).json.etag,'immutable');
 await assert.rejects(transport({op:'get',key,if_match_etag:'wrong'}),error=>error.status===2);await assert.rejects(transport({op:'get',key,max_response_bytes:2}),error=>error.status===4);await assert.rejects(transport({op:'get',key,range:{offset:6,length:1}}),error=>error.status===3);assert.equal(fallbacks,0);await transport({op:'get',key,bucket:'unbound'});assert.equal(fallbacks,1);
 const expired=withReadView(async()=>{throw Error('expired hints must not fall back');},{},'hinted',{initial:view,admittedAt:performance.now()-30001});await assert.rejects(expired({op:'get',key}),error=>error.status===8);
 const poisoned={...view,immutable:[['antfly/progress/hinted/HEAD',record('999')]]},safe=withReadView(async()=>{fallbacks++;return{};},{},'hinted',{initial:poisoned});assert.equal(new TextDecoder().decode((await safe({op:'get',key:'antfly/progress/hinted/HEAD'})).body),'7');await safe({op:'get',key});assert.equal(fallbacks,2);
});

test("advisory immutable hints share the bounded row cache and discard uncharged fields",async()=>{
 const{store}=fixture(),key='antfly/artifacts/sha256/aa/'+'b'.repeat(62),hints={version:'7',records:[[key,{etag:'confirmed',body:new Uint8Array(2048),contentType:null,extra:()=>{throw Error('must never clone');}}]]};
 for(let i=0;i<256;i++)store.remember('row/'+i,{text:'x'.repeat(2048)});assert.ok(store.setReadHints('cache','7',hints));assert.ok(store.rowBytes+store.hintBytes<=512*1024);assert.equal(store.readHints.records[0][1].extra,undefined);hints.records[0][1].body[0]=99;assert.equal(store.readHints.records[0][1].body[0],0);
 assert.equal(store.setReadHints('cache','6',{version:'6',records:[]}),false);assert.equal(store.readHints.version,'7');assert.equal(store.setReadHints('cache','8',{version:'8',records:[['antfly/progress/cache/HEAD',record('8')]]}),false);
});

test("variant recycling retires compact kernels below their hard cap without changing accepted results",async()=>{
 const{RequestPool}=await import('../request-pool.mjs');let created=0;
 const pool=new RequestPool(async job=>{created++;let bytes=job.hostKey==='full'?20:8;return{usable:()=>true,memoryBytes:()=>bytes,grow:value=>{bytes=value;}};},1,{maxReservedBytes:32,reservationBytes:16,defaultKernelVariant:'compact',recycleBytes:job=>job.hostKey==='full'?24:12});
 assert.equal(await pool.withHost(()=> 'accepted'),'accepted');assert.equal(created,1);assert.equal(await pool.withHost(host=>{host.grow(13);return 'accepted';}),'accepted');assert.equal(created,1);assert.equal(pool.memoryBytes(),0);assert.equal(pool.reservedBytes,0);
 await pool.withHost(()=>{});assert.equal(created,2);const full={kernelVariant:'full',memoryReservationBytes:32};await pool.withHost(()=>{},full);assert.equal(created,3);assert.equal(pool.memoryBytes(),20);await pool.withHost(host=>host.grow(25),full);assert.equal(pool.memoryBytes(),0);assert.equal(pool.reservedBytes,0);
 const invalid=new RequestPool(()=>{throw Error('must not instantiate');},1,{recycleBytes:()=>{throw Error('invalid recycle policy');}});await assert.rejects(invalid.withHost(()=>{}),/invalid recycle policy/);assert.equal(invalid.reservedBytes,0);assert.equal(invalid.pending.length,0);
});


test("platform diagnostics preserve bounded flags and references without leaking inputs or changing failures",async()=>{
 const {CriticalPath,platformErrorDetails}=await import("../trace.mjs"),secret="private-token-document-body";
 const failure=Object.assign(Error("internal error; reference = gvaf2p4c90nvb07cffsfrtq0 "+secret),{retryable:true,overloaded:false,remote:true,headers:{Authorization:secret}});
 const trace=new CriticalPath();await assert.rejects(trace.span("rpc:commit",async()=>{throw failure;}),error=>error===failure);
 assert.deepEqual(trace.report().errors,[{phase:"rpc:commit",retryable:true,overloaded:false,remote:true,reference:"gvaf2p4c90nvb07cffsfrtq0"}]);
 assert.ok(!JSON.stringify(trace.report()).includes(secret));assert.equal(trace.report().spans.length,1);
 for(let i=0;i<16;i++)trace.error("http",failure);assert.equal(trace.report().errors.length,8);
 assert.deepEqual(platformErrorDetails(Object.assign(Error("remote application failure"),{remote:true})),{remote:true});
 assert.equal(platformErrorDetails(Error("Worker exceeded memory limit")).resource_limit,"memory");
 assert.equal(platformErrorDetails(Error("Worker exceeded CPU time limit")).resource_limit,"cpu");
 assert.deepEqual(platformErrorDetails(null),{});
});


test("runtime placement separates compute generations from R2 identities and composes with tenant and physical epochs",async()=>{
 const {placementEnv}=await import("../runtime-placement.mjs"),{epochEnv,actorEpochEnv}=await import("../qualification/physical-epochs.mjs"),{runtimeName}=await import("../runtime-routing.mjs");
 const calls=[],id=name=>({name,equals:other=>other.name===name}),binding={idFromName:name=>id(name),getByName(name,options){calls.push({name,options});return this.idFromName(name);},get(value,options){calls.push({name:value.name,options});return value;}};
 const root={COORDINATOR_BACKEND:"r2",ANTFLY_INCARNATION:"qualification-placement",COORDINATOR_STORAGE_ID:"stable-storage",R2_LOCATION_HINT:"wnam",RUNTIME_GENERATION:"one",ANTFLY_TENANTS:JSON.stringify({a:{incarnation:"qualification-placement-a",storage_id:"storage-a"}}),COMMITS:binding,READERS:binding,BUILDERS:binding};
 const placed=placementEnv(root);const copied={...placed};assert.equal(placementEnv(copied),copied);
 assert.equal(placementEnv(placed),placed);assert.equal(placed.COORDINATOR_STORAGE_ID,root.COORDINATOR_STORAGE_ID);assert.equal(placed.ANTFLY_INCARNATION,root.ANTFLY_INCARNATION);
 for(const role of ["COMMITS","READERS","BUILDERS"]){const stub=placed[role].getByName("logical",{locationHint:"enam",other:true});assert.ok(stub.equals(placed[role].idFromName("logical")));placed[role].get(stub);}
 assert.ok(calls.every(call=>call.options.locationHint==="wnam"));assert.equal(calls[0].options.other,true);
 const newer=placementEnv({...root,RUNTIME_GENERATION:"two"});assert.ok(!newer.COMMITS.idFromName("logical").equals(placed.COMMITS.idFromName("logical")));
 assert.equal(tenantEnv(placed,"a").COORDINATOR_STORAGE_ID,"storage-a");
 for(const epoch of [0,1,100])for(const [role,key]of [["writer","COMMITS"],["reader","READERS"],["builder","BUILDERS"]]){
  const env=epochEnv(root,epoch),tenant=tenantEnv(env,"a"),name=role==="writer"?tenant.ANTFLY_INCARNATION:runtimeName(tenant.ANTFLY_INCARNATION,0),physical=tenant[key].getByName(name,{other:true});
  const recovered=tenantEnv(actorEpochEnv({id:physical},root,role),"a");assert.ok(recovered[key].idFromName(name).equals(physical));assert.equal(recovered.COORDINATOR_STORAGE_ID,"storage-a");assert.equal(calls.at(-1).options.other,true);
 }
 assert.ok(!epochEnv(root,1).COMMITS.idFromName("logical").equals(epochEnv(root,2).COMMITS.idFromName("logical")));
 for(const bad of [{RUNTIME_GENERATION:undefined},{R2_LOCATION_HINT:"unknown"},{RUNTIME_GENERATION:""},{COORDINATOR_BACKEND:"sqlite"}])assert.throws(()=>placementEnv({...root,...bad}),/InvalidRuntimePlacement/);
 assert.throws(()=>placementEnv({...placed,RUNTIME_GENERATION:"two"}),/RuntimePlacementAlreadyConfigured/);
 const none={COMMITS:binding};assert.equal(placementEnv(none),none);
});


test("queued kernel work resumes in its submitting context rather than the releasing actor",async()=>{
 const{AsyncLocalStorage}=await import('node:async_hooks'),{RequestPool}=await import('../request-pool.mjs'),context=new AsyncLocalStorage(),seen=[];let release,entered;
 const started=new Promise(resolve=>entered=resolve),held=new Promise(resolve=>release=resolve),pool=new RequestPool(async()=>({usable:()=>true,memoryBytes:()=>0}),1);
 const first=context.run('actor-a',()=>pool.withHost(async()=>{seen.push(context.getStore());entered();await held;seen.push(context.getStore());return'a';}));await started;
 const second=context.run('actor-b',()=>pool.withHost(async()=>{seen.push(context.getStore());await new Promise(resolve=>setTimeout(resolve,0));seen.push(context.getStore());return'b';}));
 const third=context.run('actor-c',()=>pool.withHost(async()=>{seen.push(context.getStore());throw Error('caller-c');}));
 release();assert.deepEqual(await Promise.all([first,second]),['a','b']);await assert.rejects(third,/caller-c/);
 assert.deepEqual(seen,['actor-a','actor-a','actor-b','actor-b','actor-c']);assert.equal(pool.reservedBytes,0);assert.equal(pool.slots[0].busy,false);
});


test("expired read leases return recoverable admission without reclassifying corruption or opaque platform failures",async()=>{
 const{retryableRuntimeFailure}=await import('../performance.mjs');
 for(const message of ['ManifestReadLeaseExpired','DeadlineExceeded','WorkLeaseLost','ImmutableLogFenced','ObjectIoBudgetExceeded'])assert.equal(retryableRuntimeFailure(Error(message)),true);
 for(const message of ['InvalidImmutableLog','ManifestVersionRetired','OutOfMemory','internal error; reference = opaque','InvalidDocumentFactsRoot'])assert.equal(retryableRuntimeFailure(Error(message)),false);
 assert.equal(retryableRuntimeFailure(Object.assign(Error('temporary platform failure'),{retryable:true})),true);assert.equal(retryableRuntimeFailure(Object.assign(Error('overloaded'),{retryable:true,overloaded:true})),false);assert.equal(retryableRuntimeFailure(Object.assign(Error('OutOfMemory'),{retryable:true})),false);
});


test("authority admission bounds pending callers and detaches timeouts while one slow load recovers",async()=>{
 const{AuthorityAdmission}=await import('../authority-admission.mjs');let release,calls=0;const held=new Promise(resolve=>release=resolve),admission=new AuthorityAdmission(()=>{calls++;return held;},{timeoutMs:10,maxWaiters:2});
 const first=admission.get(),second=admission.get();await assert.rejects(admission.get(),/CoordinatorCommitBusy/);
 const expired=await Promise.allSettled([first,second]);assert.ok(expired.every(r=>r.status==='rejected'&&r.reason.message==='CoordinatorRecovering'));assert.equal(admission.waiters.size,0);assert.equal(calls,1);
 for(let i=0;i<3;i++){await assert.rejects(admission.get(),/CoordinatorRecovering/);assert.equal(admission.waiters.size,0);assert.equal(calls,1);}
 const pending=admission.get(),authority={descriptor:{mode:'r2'}};release(authority);assert.equal(await pending,authority);assert.equal(await admission.get(),authority);assert.equal(admission.waiters.size,0);assert.equal(calls,1);
 let attempts=0;const retry=new AuthorityAdmission(async()=>{if(++attempts===1)throw Error('CoordinatorAuthorityInvalid');return authority;});await assert.rejects(retry.get(),/CoordinatorAuthorityInvalid/);assert.equal(await retry.get(),authority);assert.equal(attempts,2);
});

test("committed immutable hints survive metadata eviction without writer ownership and roll back with HEAD",async()=>{
 const{writeReadHints}=await import('../publication-read-hints.mjs'),{captureReadView}=await import('../read-view.mjs'),{bucket,store}=fixture(),namespace='durable-hints',keys=await metadataKeys(namespace),manifest=key=>`antfly/manifests/${namespace}/manifests/${key}.bin`,artifact='antfly/artifacts/sha256/aa/'+'b'.repeat(62),hint={version:'7',records:[[manifest(7),{etag:'manifest',body:utf8.encode('manifest')}],[artifact,{etag:'artifact',body:new Uint8Array([0,128,255])}]]};
 await store.transaction(async tx=>{tx.put(keys.head,record('7'));writeReadHints(tx,namespace,'7',hint);await captureReadView(tx,keys);});
 const reopened=new R2CoordinatorStore(module,bucket,'performance'),writes=bucket.writes,view=await reopened.readOnly(tx=>captureReadView(tx,keys));assert.equal(reopened.acquired,false);assert.equal(bucket.writes,writes);assert.equal(view.immutable.length,2);assert.deepEqual(view.immutable[1][1].body,new Uint8Array([0,128,255]));
 const metadataOnly=await reopened.readOnly(tx=>captureReadView({...tx,getMany:names=>{assert.ok(!names.includes(keys.hints));return tx.getMany(names);}},keys,{immutable:false}));assert.equal(metadataOnly.immutable,undefined);
 const transport=withReadView(()=>{throw Error('unexpected R2 fetch');},{},namespace,{initial:view});assert.deepEqual((await transport({op:'get',key:artifact})).body,new Uint8Array([0,128,255]));
 const newer=new R2CoordinatorStore(module,bucket,'performance');await newer.transaction(tx=>{tx.put(keys.head,record('8'));writeReadHints(tx,namespace,'8',undefined);});assert.equal((await openReadView(newer,namespace)).immutable,undefined);
 await assert.rejects(newer.transaction(tx=>{tx.put(keys.head,record('9'));writeReadHints(tx,namespace,'7',hint);throw Error('rollback');}),/rollback/);assert.equal((await openReadView(newer,namespace)).version,'8');assert.equal(await newer.transaction(tx=>tx.get(keys.hints)),undefined);
});
test("durable hint encoding bounds compact metadata and rejects corrupt or mismatched snapshots",async()=>{
 const{encodeReadHints,decodeReadHints}=await import('../publication-read-hints.mjs'),namespace='encoded-hints',key=`antfly/manifests/${namespace}/manifests/7.bin`,body=new Uint8Array(4096).fill(255),hint={version:'7',records:[[key,{etag:'etag',body}]]},encoded=encodeReadHints(namespace,'7',hint);
 assert.ok(utf8.encode(JSON.stringify(encoded)).byteLength<16384);assert.deepEqual(decodeReadHints(namespace,'7',encoded).records[0][1].body,body);assert.equal(decodeReadHints(namespace,'8',encoded),null);assert.equal(decodeReadHints('foreign','7',encoded),null);
 for(const mutate of[x=>x.records[0][1].body_base64='!!!',x=>x.records[0][1].body_base64='A'.repeat(5468),x=>x.records.push(x.records[0]),x=>x.format=2]){const invalid=structuredClone(encoded);mutate(invalid);assert.equal(decodeReadHints(namespace,'7',invalid),null);}
 assert.equal(encodeReadHints(namespace,'8',hint),null);
});


test("actor-version ownership resumes after reconstruction and still fences different deployments",async()=>{
 const{actorOwner}=await import('../actor-owner.mjs'),actor='a'.repeat(64),version='01234567-0123-4567-89ab-0123456789ab',namespace='stable-actor',identity=await actorOwner(actor,version,namespace),bucket=new Bucket();
 assert.deepEqual(await actorOwner(actor.toUpperCase(),version.toUpperCase(),namespace),identity);assert.equal(actorOwner(actor,undefined,namespace),undefined);assert.notDeepEqual(await actorOwner('b'.repeat(64),version,namespace),identity);assert.notDeepEqual(await actorOwner(actor,version,'other'),identity);
 const first=new R2CoordinatorStore(module,bucket,namespace,{owner:actorOwner(actor,version,namespace)});await first.transaction(tx=>tx.put('a',1));const epoch=structuredClone(first.state.epoch);
 const reopened=new R2CoordinatorStore(module,bucket,namespace,{owner:actorOwner(actor,version,namespace)});await reopened.transaction(tx=>tx.put('b',2));assert.deepEqual(reopened.state.epoch,epoch);assert.deepEqual(reopened.owner,identity);assert.deepEqual(await reopened.transaction(tx=>tx.getMany(['a','b'])),[1,2]);
 const replacement=new R2CoordinatorStore(module,bucket,namespace,{owner:actorOwner(actor,'01234567-0123-4567-89ab-0123456789ac',namespace)});await replacement.transaction(tx=>tx.put('c',3));await assert.rejects(first.transaction(tx=>tx.put('stale',true)),/ImmutableLogFenced/);assert.deepEqual(await replacement.transaction(tx=>tx.getMany(['a','b','c','stale'])),[1,2,3,undefined]);
});
test("overlapping reconstructed owners cannot acknowledge forked roots and retry preserves both updates",async()=>{
 const bucket=new Bucket(),namespace='same-owner-overlap',owner=Array(16).fill(1),seed=new R2CoordinatorStore(module,bucket,namespace,{owner});await seed.transaction(tx=>tx.put('seed',0));
 const left=new R2CoordinatorStore(module,bucket,namespace,{owner:Promise.resolve(owner)}),right=new R2CoordinatorStore(module,bucket,namespace,{owner:Promise.resolve(owner)});let entered=0,release;const barrier=new Promise(resolve=>release=resolve),write=(store,key,value)=>store.transaction(async tx=>{if(++entered===2)release();await barrier;tx.put(key,value);});
 const result=await Promise.allSettled([write(left,'left',1),write(right,'right',2)]);assert.equal(result.filter(r=>r.status==='fulfilled').length,1);assert.equal(result.filter(r=>r.status==='rejected').length,1);const index=result.findIndex(r=>r.status==='rejected'),loser=index===0?left:right;await loser.transaction(tx=>tx.put(index===0?'left':'right',index===0?1:2));
 const check=new R2CoordinatorStore(module,bucket,namespace,{owner});assert.deepEqual(await check.transaction(tx=>tx.getMany(['seed','left','right'])),[0,1,2]);
});
test("idle read observers probe one tail slot and grow only after an authenticated contiguous prefix",async()=>{
 const{recoveryReadAhead}=await import('../recovery-read-ahead.mjs'),{ObjectStoreError}=await import('../../../zig/lib/objectstore/wasm/r2.mjs'),state={format:1,epoch:Array(16).fill(1),owner:Array(16).fill(2),next:30,revision:100,root:''},prefix='db/epochs/'+'01'.repeat(16)+'/',calls=[];let live=false;
 const raw=async request=>{calls.push(request.key);if(request.key==='db/CHECKPOINT')return{body:utf8.encode(JSON.stringify({state})),json:{}};if(live&&request.key===prefix+'00000000000000000030.json')return{body:utf8.encode(JSON.stringify({kind:'commit',epoch:state.epoch,slot:30,parent_revision:100,state:{...state,next:31,revision:101}})),json:{}};throw new ObjectStoreError(1,'missing');};
 const read=recoveryReadAhead(raw,{window:8,initialWindow:1,cursor:state,followSeals:true});await read({op:'get',key:'db/CHECKPOINT'});await assert.rejects(read({op:'get',key:prefix+'SEALED'}),e=>e.status===1);await assert.rejects(read({op:'get',key:prefix+'00000000000000000030.json'}),e=>e.status===1);assert.equal(calls.length,3);
 calls.length=0;live=true;await read({op:'get',key:'db/CHECKPOINT'});await assert.rejects(read({op:'get',key:prefix+'SEALED'}),e=>e.status===1);await read({op:'get',key:prefix+'00000000000000000030.json'});await assert.rejects(read({op:'get',key:prefix+'SEALED'}),e=>e.status===1);assert.ok(calls.includes(prefix+'00000000000000000031.json'));assert.ok(calls.includes(prefix+'00000000000000000032.json'));assert.ok(!calls.includes(prefix+'00000000000000000033.json'));assert.equal(calls.filter(key=>key.endsWith('/SEALED')).length,2);
 assert.throws(()=>recoveryReadAhead(raw,{window:8,initialWindow:9}),/InvalidRecoveryWindow/);
});


test("background checkpoint progress survives a continuously occupied foreground queue",async()=>{
 const bucket=new Bucket(),work=[];let now=Date.now(),release,entered;
 const{RequestPool}=await import('../request-pool.mjs'),{instantiateHost}=await import('../../../zig/lib/objectstore/wasm/asyncify.mjs');
 const observe=await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless-observe.wasm',import.meta.url))),pool=new RequestPool(()=>instantiateHost(observe,null,{clock:()=>now}),2,{reservationBytes:8*1024*1024,maxReservedBytes:16*1024*1024,recycleBytes:7*1024*1024});
 const held=new Promise(r=>release=r),started=new Promise(r=>entered=r),store=new R2CoordinatorStore(module,bucket,'checkpoint-under-load',{clock:()=>now,readKernel:pool,background:p=>work.push(p)});
 await store.transaction(tx=>tx.put('value',1));const revision=store.state.revision;now+=1001;
 const foreground=store.transaction(async tx=>{entered();await held;return tx.get('value');});await started;
 try{await work[0];assert.equal(store.running,true);const cp=JSON.parse(new TextDecoder().decode(bucket.objects.get(store.base+'/CHECKPOINT').body));assert.equal(cp.state.revision,revision);assert.equal(store.idleCheckpoint.last,revision);assert.ok(pool.memoryBytes()>0&&pool.memoryBytes()<=16*1024*1024);assert.ok(pool.slots.every(s=>!s.host||s.host.memoryBytes()<=8*1024*1024));}finally{release();}
 assert.equal(await foreground,1);
});

test("a suspended checkpoint hint neither holds foreground admission nor changes its accounting",async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket}),work=[];let now=Date.now(),block=false,release,entered;
 const held=new Promise(r=>release=r),started=new Promise(r=>entered=r);
 const store=new R2CoordinatorStore(module,bucket,'checkpoint-independent',{clock:()=>now,background:p=>work.push(p),transport:async(request,body)=>{if(block&&request.op==='put'&&request.key.endsWith('/CHECKPOINT')){entered();await held;}return raw(request,body);}});
 await store.transaction(tx=>tx.put('value',1));now+=1001;block=true;await started;
 const before=store.state.revision;
 try{await Promise.race([store.transaction(tx=>tx.put('value',2)),new Promise((_,reject)=>setTimeout(()=>reject(Error('checkpoint blocked foreground')),1000))]);assert.ok(store.state.revision>before);}finally{block=false;release();}
 const usage={operations:store.usedOperations,bytes:store.usedBytes};await work[0];assert.equal(store.usedOperations,usage.operations);assert.equal(store.usedBytes,usage.bytes);
 await Promise.all(work);assert.equal(await store.transaction(tx=>tx.get('value')),2);await Promise.all(work);
});

test("background checkpoint hints reject a replaced owner without rewinding recovery",async()=>{
 const bucket=new Bucket();let now=Date.now();const old=new R2CoordinatorStore(module,bucket,'checkpoint-stale',{clock:()=>now});await old.transaction(tx=>tx.put('value',1));
 now+=1001;const replacement=new R2CoordinatorStore(module,bucket,'checkpoint-stale',{clock:()=>now});await replacement.transaction(tx=>tx.put('value',2));
 const before=bucket.objects.get(old.base+'/CHECKPOINT').body.slice();await assert.rejects(old.checkpointHint(),/ImmutableLogFenced/);assert.deepEqual(bucket.objects.get(old.base+'/CHECKPOINT').body,before);assert.equal(await replacement.readOnly(tx=>tx.get('value')),2);
});


test("checkpoint completion schedules the latest committed revision without another foreground call",async()=>{
 const{IdleCheckpoint}=await import('../idle-checkpoint.mjs');let revision=1,release,entered,calls=0;const work=[],held=new Promise(r=>release=r),started=new Promise(r=>entered=r);
 const checkpoints=new IdleCheckpoint({revision:()=>revision,isIdle:()=>true,background:p=>work.push(p),delay:1,checkpoint:async()=>{const captured=revision;if(++calls===1){entered();await held;}return{done:true,revision:captured};}});
 checkpoints.schedule();await started;revision=2;checkpoints.schedule();release();await work[0];assert.equal(work.length,2);await work[1];assert.equal(checkpoints.last,2);assert.equal(calls,2);assert.equal(checkpoints.pending,false);
});

test('a failed transaction owner stays pending through the surviving group commit',async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket});let block=false,release,entered,rejected=false;
 const held=new Promise(r=>release=r),started=new Promise(r=>entered=r),store=new R2CoordinatorStore(module,bucket,'failed-batch-owner',{transport:async(request,body)=>{if(block&&request.op==='put'&&request.key.endsWith('.json')){let frame;try{frame=JSON.parse(new TextDecoder().decode(body));}catch{}if(frame?.kind==='commit'){entered();await held;}}return raw(request,body);}});
 await store.transaction(tx=>tx.put('seed',1));block=true;
 const failed=store.transaction(tx=>{tx.put('rolled-back',true);throw Error('callback failed');}).then(()=>assert.fail('callback must fail'),error=>{rejected=true;assert.match(error.message,/callback failed/);});
 const survivor=store.transaction(tx=>tx.put('survivor',true));await started;
 try{assert.equal(rejected,false);}finally{block=false;release();}
 await Promise.all([failed,survivor]);assert.equal(rejected,true);assert.equal(await store.transaction(tx=>tx.get('rolled-back')),undefined);assert.equal(await store.transaction(tx=>tx.get('survivor')),true);
});

test('transaction queue hands off ownership before exposing a completed owner response',async()=>{
 const events=[],traceA=crypto.randomUUID(),traceB=crypto.randomUUID();let entered,release;const started=new Promise(r=>entered=r),held=new Promise(r=>release=r);
 class Store extends R2CoordinatorStore{async native(input){const state=this.state??{epoch:Array(16).fill(1),owner:this.owner,revision:1,next:1,root:''};if(input.action==='begin'){events.push('begin:'+(this.trace.id===traceA?'a':'b'));if(this.trace.id===traceA){entered();await held;}return{state,has_more:false,base:'base',page_prefix:'page'};}if(input.action==='commit')return{state:{...state,revision:state.revision+1,next:state.next+1}};throw Error('unexpected native action');}}
 const store=new Store(module,new Bucket(),'ownership-before-response');const first=store.transaction(tx=>tx.put('a',1),{priority:'maintenance',traceId:traceA}).then(()=>events.push('response:a'));await started;const second=store.transaction(tx=>tx.put('b',2),{traceId:traceB});release();await Promise.all([first,second]);assert.ok(events.indexOf('begin:b')<events.indexOf('response:a'),JSON.stringify(events));assert.equal(store.pending.length,0);assert.equal(store.running,false);
});


test('bounded role caches protect data across metadata churn without increasing shared memory',async()=>{
 const{createRoleCache}=await import('../role-cache.mjs'),cache=createRoleCache({maxBytes:65536,maxEntries:64}),bucket={},scoped=cache.forBucket(bucket),dataKey='antfly/artifacts/sha256/aa/'+'b'.repeat(62);let dataGets=0;
 const data=scoped.wrap(async req=>{dataGets++;return{body:new Uint8Array(4096).fill(7),json:{etag:'data'}};},'data'),metadata=scoped.metadata.wrap(async()=>({body:new Uint8Array(8192),json:{etag:'metadata'}}),'coordinator/meta');
 await data({op:'get',bucket:'database',key:dataKey});for(let i=0;i<32;i++)await metadata({op:'get',bucket:'database',key:'antfly/artifacts/sha256/cc/'+i.toString(16).padStart(62,'0')});assert.equal((await data({op:'get',bucket:'database',key:dataKey})).body[0],7);assert.equal(dataGets,1);assert.ok(cache.retainedBytes()<=65536);assert.ok(cache.metadataBytes()<=16384);assert.ok(cache.dataBytes()<=49152);cache.clear();assert.equal(cache.retainedBytes(),0);
});
test('role cache binding scopes isolate identical immutable names across R2 buckets',async()=>{
 const{createRoleCache}=await import('../role-cache.mjs'),cache=createRoleCache({maxBytes:16384,maxEntries:16}),a={},b={},key='antfly/manifests/same/manifests/1.bin';let gets=0;const transport=label=>async()=>{gets++;return{body:utf8.encode(label),json:{etag:label}};};
 const left=cache.wrap(transport('a'),'same',a),right=cache.wrap(transport('b'),'same',b);assert.equal(new TextDecoder().decode((await left({op:'get',bucket:'database',key})).body),'a');assert.equal(new TextDecoder().decode((await right({op:'get',bucket:'database',key})).body),'b');await left({op:'get',bucket:'database',key});await right({op:'get',bucket:'database',key});assert.equal(gets,2);assert.ok(cache.retainedBytes()<=16384);
});
