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
import {R2CoordinatorStore} from "../r2-coordinator-store.mjs";
import {R2CollectionJournal} from "../r2-collection-journal.mjs";
import {collectCoordinator} from "../r2-coordinator-gc.mjs";
import {migrateCoordinator, MIGRATION} from "../r2-coordinator-migration.mjs";
import {openAuthority} from "../coordinator-authority.mjs";
import {Bucket} from "./object-fixture.mjs";
import {createR2Transport} from "../../../zig/lib/objectstore/wasm/r2.mjs";
const module = await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless.wasm", import.meta.url)));
const storeFor = (bucket, options = {}) => new R2CoordinatorStore(module, bucket, "native-coordinator", options);
async function retry(store, run) {
  for (let i=0; i<100; i++) {
    try {return await store.transaction(run);} catch(error) {if (!/CoordinatorRecovering/.test(String(error))) throw error;}
  }
  throw new Error("bounded recovery failed to converge");
}
test("native coordinator commits atomic batches, reopens, paginates and fences a stale owner", async () => {
  const bucket = new Bucket(); let store = storeFor(bucket);
  await store.transaction(async tx => {tx.put("a", {body: Uint8Array.of(1,2), text: "durable"}); tx.put("b", 2);});
  await assert.rejects(store.transaction(async tx => {tx.put("a", "uncommitted"); throw new Error("rollback");}), /rollback/);
  await store.transaction(async tx => {
    const a = await tx.get("a"); assert.deepEqual(a.body, Uint8Array.of(1,2));
    for (let i=0; i<130; i++) tx.put(`row/${String(i).padStart(4,"0")}`, i);
    tx.put("row/😀", 131); tx.put("row/𐀀", 132);
  });
  const old = store; store = storeFor(bucket);
  assert.equal(await retry(store, tx => tx.get("b")), 2);
  await assert.rejects(old.transaction(tx => tx.put("stale", true)), /ImmutableLogFenced/);
  await store.transaction(async tx => {
    const first = await tx.list({prefix: "row/", limit: 128}); assert.equal(first.size, 128);
    const second = await tx.list({prefix: "row/", startAfter: [...first.keys()].at(-1), limit: 128}); assert.equal(second.size, 4);
    for (const key of first.keys()) tx.delete(key);
    assert.equal((await tx.list({prefix: "row/", limit: 128})).size, 4, "overlay deletions must not truncate a paginated scan");
  });
});
test("native coordinator recovery stays bounded across many epoch rotations", async () => {
  const bucket = new Bucket(), samples = []; const now = Date.now();
  let checkpointWrites=0;const put=bucket.put.bind(bucket);bucket.put=async(key,...args)=>{if(key.endsWith("/CHECKPOINT"))checkpointWrites++;return put(key,...args);};
  const store = storeFor(bucket, {clock: () => now});
  for (let i=0; i<5000; i++) await store.transaction(tx => tx.put("latest", i));
  const reopened = storeFor(bucket, {clock: () => now, onMetrics: sample => samples.push(sample)});
  await assert.rejects(reopened.transaction(tx => tx.get("latest")), /CoordinatorRecovering/, "a cold actor must yield after its bounded recovery walk");
  assert.equal(await retry(reopened, tx => tx.get("latest")), 4999);
  assert.equal(checkpointWrites,1,"a frozen one-second window cannot overwrite the hot checkpoint key");
  assert.ok(samples.every(sample => sample.operations <= 300), "each recovery step must bound object operations");
});
test("a lost immutable commit response preserves the complete durable outcome", async () => {
  const bucket = new Bucket(), raw = createR2Transport({database: bucket}, {maxPutBytes: 4*1024*1024});
  let fail = true;
  const store = storeFor(bucket, {transport: async (request, body) => {
    const result = await raw(request, body);
    if (fail && request.op === "put" && request.key.includes("/epochs/") && request.key.endsWith(".json") && JSON.parse(new TextDecoder().decode(body)).kind === "commit") {fail = false; throw new Error("lost commit acknowledgement");}
    return result;
  }});
  await assert.rejects(store.transaction(tx => {tx.put("left", 1); tx.put("right", 2);}));
  const reopened = storeFor(bucket);
  assert.deepEqual(await retry(reopened, tx => tx.getMany(["left","right"])), [1,2]);
});
test("takeover rejects a writer paused while uploading native pages", async () => {
  const bucket = new Bucket(), raw = createR2Transport({database: bucket}, {maxPutBytes: 4*1024*1024});
  let pause = false, release, reached;
  const gate = new Promise(resolve => {release = resolve;}), pending = new Promise(resolve => {reached = resolve;});
  const old = storeFor(bucket, {transport: async (request, body) => {
    if (pause && request.op === "put" && request.key.includes("/artifacts/")) {pause = false; reached(); await gate;}
    return raw(request, body);
  }});
  await old.transaction(tx => tx.put("value", "initial")); pause = true;
  const stale = old.transaction(tx => tx.put("value", "old"));
  await pending;
  const winner = storeFor(bucket);
  await retry(winner, tx => tx.put("value", "winner"));
  release(); await assert.rejects(stale, /ImmutableLogFenced|PreconditionFailed/);
  assert.equal(await winner.transaction(tx => tx.get("value")), "winner");
});
test("a late PUT cannot acknowledge a recreated slot after takeover and collection", async () => {
  const bucket = new Bucket(), raw = createR2Transport({database: bucket}, {maxPutBytes: 4*1024*1024});
  const epoch = Date.now(); let now = epoch, pause = false, release, reached, oldKey;
  const gate = new Promise(resolve => {release = resolve;}), pending = new Promise(resolve => {reached = resolve;});
  const old = storeFor(bucket, {clock: () => epoch, transport: async (request, body) => {
    if (pause && request.op === "put" && request.key.includes("/epochs/") && request.key.endsWith(".json") && JSON.parse(new TextDecoder().decode(body)).kind === "commit") {pause = false; oldKey = request.key; reached(); await gate;}
    return raw(request, body);
  }});
  await old.transaction(tx => tx.put("value", "initial")); pause = true;
  const stale = old.transaction(tx => tx.put("value", "stale")); await pending;
  const winner = storeFor(bucket, {clock: () => now});
  await retry(winner, tx => tx.put("value", "winner"));
  await winner.transaction(tx => tx.put("new", true));
  let collected = false;
  for (let step=0; step<128; step++) {
    const result = await collectCoordinator(winner, bucket, () => now);
    if (result.delay_seconds) now += result.delay_seconds*1000+1;
    if (result.done) {collected = true; break;}
  }
  assert.ok(collected); assert.equal(bucket.objects.has(oldKey), false, "collection retired the old seal slot");
  release(); await assert.rejects(stale, /ImmutableLogFenced/);
  assert.ok(bucket.objects.has(oldKey), "the delayed provider PUT recreated an unreachable orphan");
  assert.equal(await winner.transaction(tx => tx.get("value")), "winner");
  const reopened = storeFor(bucket, {clock: () => now});
  assert.equal(await retry(reopened, tx => tx.get("value")), "winner");
  const checkpoint = `${reopened.base}/CHECKPOINT`;
  await bucket.delete(checkpoint);
  await assert.rejects(storeFor(bucket, {clock: () => now}).transaction(tx => tx.get("value")), /ImmutableLogCheckpointMissing/, "a missing required checkpoint must never initialize an empty database");
});
test("the native frontier resumes completed parents, exact marks and cleanup after eviction", async () => {
  const bucket = new Bucket(); let now = Date.now(), store = storeFor(bucket, {clock: () => now});
  let maximumMutations=0;const bounded={transaction:run=>store.transaction(async tx=>{const result=await run(tx);maximumMutations=Math.max(maximumMutations,tx.mutationCount());return result;})};
  let journal = new R2CollectionJournal(bounded, () => now);
  const claim = await journal.rpc("claim"), token = claim.token;
  const plan = {roots: [{kind:"manifest",version:"1"}], floor:"1", retired_before:"5", pin_prefix:"pins/", artifact_prefix:"artifacts/", manifest_prefix:"manifests/"};
  await journal.rpc("prepared", {token, plan}); await journal.rpc("pins", {token});
  const parent = await journal.rpc("next", {token});
  const children=Array.from({length:128}, (_,i)=>({kind:"blob",artifact:{artifact_id:`body/${i}`}})),rootTrace={key:"root",children};
  await journal.rpc("traced", {token, id:parent.id, result:rootTrace});
  const partial=await store.transaction(tx=>tx.get("__antfly_gc/tasks/"+parent.id));assert.equal(partial.done,false);assert.ok(partial.child_offset>0&&partial.child_offset<128);
  assert.equal(await store.transaction(tx=>tx.get("__antfly_gc/marks/root")),undefined,"partial parent must not be marked complete");
  await assert.rejects(journal.rpc("traced", {token,id:(await journal.rpc("next",{token})).id,result:{key:"rollback",children:[undefined]}}));
  await journal.rpc("release", {token});
  store = storeFor(bucket, {clock: () => now}); journal = new R2CollectionJournal(bounded, () => now);
  const resumed = await journal.rpc("claim");
  const pendingBatch=await journal.rpc("nextBatch",{token:resumed.token});
  await assert.rejects(journal.rpc("tracedBatch",{token:resumed.token,entries:pendingBatch.slice(0,2).map((task,i)=>({id:task.id,result:{key:`batch-rollback/${i}`,children:[],wal_keep_from:i?"invalid":"1"}}))}));
  assert.deepEqual((await journal.rpc("nextBatch",{token:resumed.token})).map(task=>task.id),pendingBatch.map(task=>task.id));
  assert.equal(await store.transaction(tx=>tx.get("__antfly_gc/marks/batch-rollback/0")),undefined);
  for (let batch=0;batch<128;batch++) {const tasks=await journal.rpc("nextBatch",{token:resumed.token});if(!tasks.length)break;await journal.rpc("tracedBatch",{token:resumed.token,entries:tasks.map(task=>({id:task.id,result:task.task.kind==='manifest'?rootTrace:{key:task.task.artifact.artifact_id,children:[]}}))});}
  assert.equal(await journal.rpc("next",{token:resumed.token}),null);
  assert.equal(await store.transaction(tx=>tx.get("__antfly_gc/marks/root")),true);
  assert.ok((await store.transaction(tx=>tx.getMany(children.map(task=>"__antfly_gc/tasks/"+JSON.stringify(task))))).every(task=>task.done));
  await journal.rpc("marked",{token:resumed.token});
  await journal.rpc("walPrepared",{token:resumed.token,result:{has_more:false,wal_prefix:"wal/",retired_before:"5",roots:[]}});
  await journal.rpc("marked",{token:resumed.token}); now+=61000;
  await journal.rpc("release", {token:resumed.token}).catch(()=>{});
  const sweep = await journal.rpc("claim");
  assert.equal(await journal.rpc("ready",{token:sweep.token}),0);
  const oldBody=`artifacts/${"0000000000000001"+"1".repeat(16)}/${"a".repeat(64)}`;
  assert.deepEqual(await journal.rpc("deletable",{token:sweep.token,keys:[oldBody]}),[oldBody]);
  for (let i=0;i<3;i++) await journal.rpc("swept",{token:sweep.token,cursor:null});
  let done=false;for(let i=0;i<8;i++) if(await journal.rpc("cleanup",{token:sweep.token})){done=true;break;}
  assert.ok(done);
  assert.ok(maximumMutations<=64,"large fanout and cleanup must checkpoint within the maintenance mutation budget");
});
test("partial collection fanout survives a lost ACK and conservatively restarts changed enumeration",async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket});let now=Date.now(),lose=false;
 let store=storeFor(bucket,{clock:()=>now,transport:async(request,body)=>{const result=await raw(request,body);if(lose&&request.op==='put'&&request.key.endsWith('.json')&&JSON.parse(new TextDecoder().decode(body)).kind==='commit'){lose=false;throw Error('lost fanout ACK');}return result;}}),journal=new R2CollectionJournal(store,()=>now);
 const{token}=await journal.rpc('claim');await journal.rpc('prepared',{token,plan:{roots:[{kind:'manifest',version:'1'}],pin_prefix:'pins/'}});await journal.rpc('pins',{token});
 const parent=await journal.rpc('next',{token}),children=Array.from({length:128},(_,i)=>({kind:'blob',artifact:{artifact_id:'body/'+i}}));
 for(const wal_keep_from of['invalid','-1','18446744073709551616'])await assert.rejects(journal.rpc('traced',{token,id:parent.id,result:{key:'root',children:[],wal_keep_from}}),/InvalidCollectionTrace/);
 lose=true;await assert.rejects(journal.rpc('traced',{token,id:parent.id,result:{key:'root',children}}),/ObjectStoreTransportFailed/);
 now+=31001;store=storeFor(bucket,{clock:()=>now});journal=new R2CollectionJournal(store,()=>now);const resumed=await journal.rpc('claim');
 const prior=await store.transaction(tx=>tx.get('__antfly_gc/tasks/'+parent.id));assert.equal(prior.done,false);assert.ok(prior.child_offset>0&&prior.child_offset<128);
 const added={kind:'blob',artifact:{artifact_id:'new-child'}},changed=[added,...children];
 await journal.rpc('traced',{token:resumed.token,id:parent.id,result:{key:'root',children:changed}});
 assert.ok(await store.transaction(tx=>tx.get('__antfly_gc/tasks/'+JSON.stringify(added))),'a changed prefix must not be skipped by the previous offset');
 for(let i=0;i<128;i++){const tasks=await journal.rpc('nextBatch',{token:resumed.token});if(!tasks.length)break;await journal.rpc('tracedBatch',{token:resumed.token,entries:tasks.map(task=>({id:task.id,result:task.id===parent.id?{key:'root',children:changed}:{key:task.task.artifact.artifact_id,children:[]}}))});}
 assert.equal(await journal.rpc('next',{token:resumed.token}),null);
 assert.ok((await store.transaction(tx=>tx.getMany(changed.map(task=>'__antfly_gc/tasks/'+JSON.stringify(task))))).every(task=>task.done));
});
test("migration freezes first, imports in bounded pages, and activates only after all phases", async () => {
  const bucket = new Bucket(), store = storeFor(bucket); let frozen=false, completed=false;
  const source={
    async freezeForR2(namespace){frozen=true;return{token:"source",namespace,ready_at:0};},
    async exportFrozenR2({kind}){assert.ok(frozen);return{rows:kind==="kv"?[["head",{etag:"old-etag",body:Uint8Array.of(9)}]]:kind==="tasks"?[["task",{task:{kind:"blob"},done:false}]]:[["marked",true]],complete:true,next:"end"};},
    async completeR2Migration(){completed=true;},
  };
  let done=false;
  for(let i=0;i<8;i++){const result=await migrateCoordinator(store,source,"db");if(result.done){done=true;break;}}
  assert.ok(done&&completed);
  await store.transaction(async tx=>{assert.equal((await tx.get(MIGRATION)).phase,"active");assert.equal((await tx.get("head")).etag,"old-etag");assert.deepEqual((await tx.get("head")).body,Uint8Array.of(9));});
});

test("concurrent metadata operations share bounded commits and isolate callback rollback", async()=>{
  const bucket=new Bucket(),store=storeFor(bucket);
  await store.transaction(tx=>tx.put("counter",{n:0}));
  const commits=()=>[...bucket.objects].filter(([key,value])=>key.includes("/epochs/")&&key.endsWith(".json")&&JSON.parse(new TextDecoder().decode(value.body)).kind==="commit").length;
  const before=commits();
  const work=Array.from({length:32},(_,i)=>store.transaction(async tx=>{const counter=await tx.get("counter");counter.n++;tx.put("counter",counter);if(i===10){tx.put("rollback",true);throw new Error("callback failed");}return counter.n;}));
  const outcomes=await Promise.allSettled(work);
  assert.equal(outcomes.filter(result=>result.status==="rejected").length,1);
  assert.ok(commits()-before<=2,"32 queued operations should share two durable commits");
  await store.transaction(async tx=>{assert.equal((await tx.get("counter")).n,31);assert.equal(await tx.get("rollback"),undefined);});
});


test("metadata collection accepts native UTF-8 string page identities", async()=>{
  const page={attempt:"a".repeat(16),digest:"b".repeat(32)},child={attempt:"é".repeat(8),digest:"c".repeat(32)};
  const id=ref=>`${Buffer.from(ref.attempt).toString("hex")}/${Buffer.from(ref.digest).toString("hex")}/0`;
  const tasks=new Map([["__antfly_coord_gc/tasks/"+id(page),{page,offset:0}]]),marks=new Map();
  const tx={get:async()=>({phase:"mark",barrier:"1"}),mutationCount:()=>0,list:async()=>new Map(tasks),trace:async ref=>({key:"pages/"+id(ref).slice(0,-2),children:ref===page?[child]:[]}),getMany:async keys=>keys.map(key=>marks.get(key)),put:(key,value)=>{if(key.startsWith("__antfly_coord_gc/tasks/"))tasks.set(key,value);if(key.startsWith("__antfly_coord_gc/marks/"))marks.set(key,value);},delete:key=>tasks.delete(key)};
  await collectCoordinator({pagePrefix:"pages/",transaction:run=>run(tx)},{});
  assert.equal(tasks.size,0);
  assert.ok(marks.has("__antfly_coord_gc/marks/pages/"+id(child).slice(0,-2)));
});


test("late metadata Queue deliveries cannot restart a completed collection", async()=>{
  const bucket=new Bucket();let now=Date.now();const store=storeFor(bucket,{clock:()=>now});
  await store.transaction(tx=>tx.put("keep",true));
  let job,done=false;
  for(let i=0;i<128;i++){const result=await collectCoordinator(store,bucket,()=>now,job);job=result.job_id;if(result.delay_seconds)now+=result.delay_seconds*1000+1;if(result.done){done=true;break;}}
  assert.ok(done&&job);const writes=bucket.writes;
  assert.deepEqual(await collectCoordinator(store,bucket,()=>now,job),{done:true,job_id:job});
  assert.equal(bucket.writes,writes,"an obsolete delivery must not create another durable job");
  const next=await collectCoordinator(store,bucket,()=>now);assert.notEqual(next.job_id,job);
});


test("missing authority fails closed across backend switches while permitting legacy SQL adoption",async()=>{
  const bucket=new Bucket(),env={DATABASE:bucket,ANTFLY_INCARNATION:"authority-fixture"};
  const put=bucket.put.bind(bucket);bucket.put=(key,body,options)=>put(key,typeof body==="string"?new TextEncoder().encode(body):body,options);
  const get=bucket.get.bind(bucket);bucket.get=async key=>{const object=await get(key);return object?{...object,json:async()=>JSON.parse(new TextDecoder().decode(bucket.objects.get(key).body))}:null;};
  const descriptor=await openAuthority(env,"sqlite");
  await assert.rejects(openAuthority(env,"r2"),/AuthorityMismatch/);
  await bucket.delete(descriptor.key);
  const digest=Buffer.from(await crypto.subtle.digest("SHA-256",new TextEncoder().encode(env.ANTFLY_INCARNATION))).toString("hex");
  await bucket.put(`antfly/wal-v1/${digest}/pages/old`,Uint8Array.of(1));
  await assert.rejects(openAuthority(env,"sqlite"),/MigrationRequired/);
  await openAuthority(env,"sqlite",{allowExistingSqlite:true});
  await bucket.delete(descriptor.key);
  await bucket.put(`antfly/coord-v1/${digest}/CHECKPOINT`,Uint8Array.of(1));
  await assert.rejects(openAuthority(env,"sqlite",{allowExistingSqlite:true}),/MigrationRequired/);
  await assert.rejects(openAuthority(env,"r2"),/MigrationRequired/);
});

test("metadata retirement overlaps bounded immutable proofs without skipping native validation",async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket});let now=Date.now(),enabled=false,active=0,peak=0;const reads=[];
 const store=storeFor(bucket,{clock:()=>now,transport:async(request,body)=>{if(!enabled||request.op!=="get")return raw(request,body);active++;peak=Math.max(peak,active);reads.push(request.key);try{await new Promise(resolve=>setTimeout(resolve,2));return await raw(request,body);}finally{active--;}}});
 for(let i=0;i<40;i++)await store.transaction(tx=>tx.put("value",i));now+=1001;await store.transaction(tx=>tx.checkpoint());
 const before=String(store.state.revision),keys=[...bucket.objects.keys()].filter(key=>key.startsWith(store.base+"/epochs/")).sort().slice(0,32);store.cache.clear();enabled=true;
 const retired=await store.transaction(tx=>tx.retirableLogs(keys,before));assert.ok(retired.length>0);assert.ok(peak>1&&peak<=9);assert.equal(active,0);assert.ok(reads.includes(store.base+"/CHECKPOINT"));
 for(const key of retired){const value=JSON.parse(new TextDecoder().decode(bucket.objects.get(key).body));assert.ok(BigInt(value.state?.revision??value.revision)<BigInt(before));}
 store.cache.clear();reads.length=0;await assert.rejects(store.transaction(tx=>tx.retirableLogs(keys,String(BigInt(before)+1000n))),/InvalidImmutableLog/);assert.equal(active,0,"speculative reads must drain before the next transaction");
 const operations=reads.filter(key=>key.includes("/epochs/"));assert.equal(new Set(operations).size,operations.length,"one proof must not duplicate a prefetched read");
});

test("metadata sweep persists deletion intent and releases writers across lost acknowledgements and eviction",async()=>{
 const bucket=new Bucket();let now=Date.now(),release,entered,releaseList,enteredList,blockList=true,blocked=true,loseAck=true,lists=0;const listGate=new Promise(resolve=>releaseList=resolve),listed=new Promise(resolve=>enteredList=resolve),gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve),originalDelete=bucket.delete.bind(bucket),originalList=bucket.list.bind(bucket),deletions=[];
 bucket.list=async options=>{lists++;if(blockList){blockList=false;enteredList();await listGate;}return originalList(options);};bucket.delete=async keys=>{if(Array.isArray(keys)){deletions.push([...keys]);if(blocked){blocked=false;entered();await gate;}await originalDelete(keys);if(loseAck){loseAck=false;throw Error('InjectedDeleteAcknowledgementLost');}return;}return originalDelete(keys);};
 let store=storeFor(bucket,{clock:()=>now});for(let i=0;i<160;i++)await store.transaction(tx=>tx.put('keep',i));let job;
 const collecting=(async()=>{for(let i=0;i<256;i++){const result=await collectCoordinator(store,bucket,()=>now,job);job=result.job_id;if(result.delay_seconds)now+=result.delay_seconds*1000+1;if(result.done)return;}})().then(()=>({done:true}),error=>({error}));
 try{
  await Promise.race([listed,collecting.then(result=>{throw result.error??Error('fixture produced no sweep listing');})]);
  let listTimer;try{await Promise.race([store.transaction(tx=>tx.put('during-list',43)),new Promise((_,reject)=>listTimer=setTimeout(()=>reject(Error('writer blocked by metadata listing')),1000))]);}finally{clearTimeout(listTimer);}releaseList();
  await Promise.race([started,collecting.then(result=>{throw result.error??Error('fixture produced no retired objects');})]);
  const state=await store.transaction(tx=>tx.get('__antfly_coord_gc/state')),intent=state.pending;assert.equal(state.phase,'delete');assert.ok(intent.keys.length>100);assert.ok(Buffer.byteLength(JSON.stringify(state))<32768);let timer;
  try{await Promise.race([store.transaction(tx=>tx.put('during-delete',42)),new Promise((_,reject)=>timer=setTimeout(()=>reject(Error('writer blocked by physical metadata deletion')),1000))]);}finally{clearTimeout(timer);}
  release();const lost=await collecting;assert.match(String(lost.error),/InjectedDeleteAcknowledgementLost/);assert.equal((await store.transaction(tx=>tx.get('__antfly_coord_gc/state'))).pending.id,intent.id);
  store=storeFor(bucket,{clock:()=>now});const beforeLists=lists;await collectCoordinator(store,bucket,()=>now,job);assert.equal(lists,beforeLists,'a persisted intent must resume without relisting');assert.deepEqual(deletions[1],deletions[0]);
  const resumed=await store.transaction(tx=>tx.get('__antfly_coord_gc/state'));assert.equal(resumed.stats.scanned,(state.stats?.scanned??0)+intent.scanned);assert.equal(resumed.pending,undefined);
  let done=false;for(let i=0;i<256;i++){const result=await collectCoordinator(store,bucket,()=>now,job);if(result.delay_seconds)now+=result.delay_seconds*1000+1;if(result.done){done=true;break;}}assert.ok(done);assert.equal(await store.transaction(tx=>tx.get('during-delete')),42);assert.equal(await store.transaction(tx=>tx.get('during-list')),43);assert.equal(await store.transaction(tx=>tx.get('keep')),159);
 }finally{releaseList();release();await collecting;}
});

test("metadata deletion intents fail closed on scope and cursor corruption",async()=>{
 const bucket=new Bucket(),store=storeFor(bucket);let deletes=0;bucket.delete=async()=>{deletes++;};
 const pending={format:1,id:'intent',phase:'pages',barrier:'1',cursor:null,keys:['outside-domain'],scanned:1,deleted:1,reclaimed_bytes:1,next_cursor:null,next_phase:'logs'};
 await store.transaction(tx=>tx.put('__antfly_coord_gc/state',{id:'job',phase:'delete',barrier:'1',cursor:null,pending}));await assert.rejects(collectCoordinator(store,bucket),/InvalidCoordinatorCollectionIntent/);assert.equal(deletes,0);
 pending.keys=[];pending.deleted=0;pending.cursor='another-batch';await store.transaction(tx=>tx.put('__antfly_coord_gc/state',{id:'job',phase:'delete',barrier:'1',cursor:null,pending}));await assert.rejects(collectCoordinator(store,bucket),/InvalidCoordinatorCollectionIntent/);assert.equal(deletes,0);
});


test('conditional authority is explicit, permanent, and cannot fork a journal namespace',async()=>{
 const bucket=new Bucket(),env={DATABASE:bucket,ANTFLY_INCARNATION:'conditional-authority',METADATA_ROOT_BACKEND:'conditional'};
 const authority=await openAuthority(env,'r2-conditional');assert.equal(authority.descriptor.read_lease_policy.format,2);
 assert.equal((await openAuthority(env,'r2-conditional')).descriptor.mode,'r2-conditional');
 await assert.rejects(openAuthority(env,'r2'),/CoordinatorAuthorityMismatch/);
 const journal={...env,ANTFLY_INCARNATION:'existing-journal'};await openAuthority(journal,'r2');
 await assert.rejects(openAuthority(journal,'r2-conditional'),/CoordinatorAuthorityMismatch/);
});

test('conditional metadata commits reopen, cross kernel pools, and fence stale writers',async()=>{
 const bucket=new Bucket(),env={DATABASE:bucket,ANTFLY_INCARNATION:'native-coordinator'};
 const authority=await openAuthority(env,'r2-conditional');
 const make=()=>{const store=storeFor(bucket);store.configureAuthority(authority.descriptor);return store;};
 const first=make();await first.transaction(tx=>{tx.put('left',1);tx.put('right',2);});
 assert.ok(first.rootCursor?.revision);assert.ok(first.rootCursor?.auth);
 assert.equal((await first.readOnly(tx=>tx.get('left'))),1);
 const second=make();assert.deepEqual(await second.transaction(tx=>tx.getMany(['left','right'])),[1,2]);
 await second.transaction(tx=>tx.put('right',3));
 await assert.rejects(first.transaction(tx=>tx.put('left',9)),/ImmutableLogFenced|PreconditionFailed/);
 assert.deepEqual(await make().transaction(tx=>tx.getMany(['left','right'])),[1,3]);
 assert.ok([...bucket.objects.keys()].some(key=>key.includes('/coord-v2/')&&key.endsWith('/ROOT')));
 assert.ok(![...bucket.objects.keys()].some(key=>key.includes('/epochs/')||key.endsWith('/CHECKPOINT')));
});

test('conditional mutable ROOT is never admitted to immutable or edge caches',async()=>{
 const {isImmutableObjectKey}=await import('../../../zig/lib/objectstore/wasm/cache.mjs');
 const base='antfly/coord-v2/'+'a'.repeat(64);
 for(const includeCommitLog of[false,true]){
  assert.equal(isImmutableObjectKey(base+'/ROOT',{includeCommitLog}),false);
  assert.equal(isImmutableObjectKey(base+'/artifacts/graph/'+'b'.repeat(64)+'/'+'c'.repeat(32)+'/'+'d'.repeat(64),{includeCommitLog}),true);
 }
});

test('bounded journal commits reopen, cross kernel pools, and fence stale writers',async()=>{
 const bucket=new Bucket(),env={DATABASE:bucket,ANTFLY_INCARNATION:'native-coordinator'};
 const authority=await openAuthority(env,'r2-bounded');
 const make=()=>{const store=storeFor(bucket);store.configureAuthority(authority.descriptor);return store;};
 const first=make();await first.transaction(tx=>{tx.put('left',1);tx.put('right',2);});
 assert.ok(first.journalProof?.auth);
 assert.equal((await first.readOnly(tx=>tx.get('left'))),1);
 const second=make();assert.deepEqual(await second.transaction(tx=>tx.getMany(['left','right'])),[1,2]);
 await second.transaction(tx=>tx.put('right',3));
 await assert.rejects(first.transaction(tx=>tx.put('left',9)),/ImmutableLogFenced|PreconditionFailed/);
 assert.deepEqual(await make().transaction(tx=>tx.getMany(['left','right'])),[1,3]);
 assert.ok([...bucket.objects.keys()].some(key=>key.includes('/coord-v1/')&&key.includes('/epochs/')));
 assert.ok(![...bucket.objects.keys()].some(key=>key.endsWith('/ROOT')));
});

test('bounded journal makes progress under a frozen per-key overwrite limit',async()=>{
 const bucket=new Bucket(),put=bucket.put.bind(bucket);
 bucket.put=async(key,body,options={})=>{
  if(bucket.objects.has(key)&&options.onlyIf?.etagDoesNotMatch!=='*'){
   const error=new Error('Too Many Requests (10058)');error.code=10058;throw error;
  }
  return put(key,body,options);
 };
 const authority=await openAuthority({DATABASE:bucket,ANTFLY_INCARNATION:'native-coordinator'},'r2-bounded'),store=storeFor(bucket);store.configureAuthority(authority.descriptor);
 for(let i=0;i<80;i++)await retry(store,tx=>tx.put('latest',i));
 assert.equal(await store.transaction(tx=>tx.get('latest')),79);
 assert.ok(![...bucket.objects.keys()].some(key=>key.endsWith('/ROOT')));
 // A rate-limited best-effort checkpoint may defer; accepted commits use new
 // slot keys and remain recoverable through the original seal protocol.
 const reopened=storeFor(bucket);reopened.configureAuthority(authority.descriptor);assert.equal(await retry(reopened,tx=>tx.get('latest')),79);
});

test('resident authority permanently binds one provider-serialized actor and rejects legacy adapters',async()=>{
 const bucket=new Bucket(),actor='a'.repeat(64),env={DATABASE:bucket,ANTFLY_INCARNATION:'resident-authority',COORDINATOR_BACKEND:'r2',METADATA_ROOT_BACKEND:'bounded-journal',INLINE_METADATA_ROOTS:'true',RESIDENT_SERVING:'true',COMMITS:{idFromName:()=>({toString:()=>actor})}};
 const {descriptor}=await openAuthority(env,'r2-bounded');assert.equal(descriptor.format,2);assert.equal(descriptor.serving.owner_id,actor);
 await assert.rejects(openAuthority({...env,RESIDENT_SERVING:'false'},'r2-bounded'),/CoordinatorAuthorityInvalid/);
 await assert.rejects(openAuthority({...env,COMMITS:{idFromName:()=>({toString:()=>'b'.repeat(64)})}},'r2-bounded'),/ResidentServingMigrationRequired/);
 let calls=0;const raw=createR2Transport({database:bucket});const store=storeFor(bucket,{exclusiveOwnerId:actor,transport:async(...args)=>{calls++;return raw(...args);}});store.configureAuthority(descriptor);
 assert.throws(()=>storeFor(bucket,{exclusiveOwnerId:'b'.repeat(64)}).configureAuthority(descriptor),/ResidentServingOwnerMismatch/);
 await retry(store,tx=>tx.put('value',1));
 calls=0;
 assert.equal(await store.ownerRead(tx=>tx.get('value')),1);assert.equal(calls,0);
 await assert.rejects(store.ownerRead(tx=>tx.put('value',2)),/OwnerReadWriteForbidden/);
 await retry(store,tx=>tx.put('value',3));assert.equal(await store.ownerRead(tx=>tx.get('value')),3);
 const observer=storeFor(bucket);observer.configureAuthority(descriptor);await assert.rejects(observer.transaction(()=>1),/ResidentServingOwnerRequired/);
 const reopened=storeFor(bucket,{exclusiveOwnerId:actor});reopened.configureAuthority(descriptor);assert.equal(await reopened.ownerRead(tx=>tx.get('value')),3);
});
test('resident serving cannot silently activate on an existing namespace',async()=>{
 const bucket=new Bucket(),env={DATABASE:bucket,ANTFLY_INCARNATION:'existing-resident',METADATA_ROOT_BACKEND:'bounded-journal'};
 await openAuthority(env,'r2-bounded');
 await assert.rejects(openAuthority({...env,COORDINATOR_BACKEND:'r2',INLINE_METADATA_ROOTS:'true',RESIDENT_SERVING:'true',COMMITS:{idFromName:()=>({toString:()=>'a'.repeat(64)})}},'r2-bounded'),/ResidentServingMigrationRequired/);
 await assert.rejects(storeFor(bucket).ownerRead(()=>1),/ResidentServingOwnerRequired/);
});
test('resident owner recovers a generic metadata commit whose durable acknowledgement was lost',async()=>{
 const bucket=new Bucket(),actor='c'.repeat(64),env={DATABASE:bucket,ANTFLY_INCARNATION:'resident-ambiguous',COORDINATOR_BACKEND:'r2',METADATA_ROOT_BACKEND:'bounded-journal',INLINE_METADATA_ROOTS:'true',RESIDENT_SERVING:'true',COMMITS:{idFromName:()=>({toString:()=>actor})}};
 const authority=await openAuthority(env,'r2-bounded'),raw=createR2Transport({database:bucket});let lose=false;
 const store=storeFor(bucket,{exclusiveOwnerId:actor,transport:async(request,body)=>{const result=await raw(request,body);if(lose&&request.op==='put'&&/\/epochs\/[^/]+\/[0-9]+\.json$/.test(request.key)){lose=false;throw Error('lost durable acknowledgement');}return result;}});store.configureAuthority(authority.descriptor);
 await retry(store,tx=>tx.put('value',1));lose=true;
 await assert.rejects(store.transaction(tx=>tx.put('value',2)));assert.equal(lose,false,'fault must follow the actual durable journal-slot PUT');assert.equal(store.acquired,false);
 assert.equal(await store.ownerRead(tx=>tx.get('value')),2);
});
