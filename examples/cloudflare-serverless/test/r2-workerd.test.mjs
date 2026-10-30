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
import {Miniflare,convertV4MiniflareOptions} from "miniflare";
import {mkdtemp,rm,readdir} from "node:fs/promises";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {fileURLToPath} from "node:url";
async function fixture(namespace, migration = false, inlineRoots = false) {
  const persist=await mkdtemp(join(tmpdir(),"antfly-r2-workerd-"));
  const output=fileURLToPath(new URL("../dist/worker/",import.meta.url));
  const wasm=(await readdir(output)).filter(name=>name.endsWith(".wasm"));
  const options={modulesRoot:output,modules:[{type:"ESModule",path:join(output,"r2-harness.mjs"),contents:`
    import worker, {R2CommitCoordinator as R2Base,CommitCoordinator as LegacyBase,R2ReadRuntime,R2BuildRuntime} from "./worker.js";
    export {R2ReadRuntime,R2BuildRuntime};
    export class R2CommitCoordinator extends R2Base {
      constructor(ctx,env){
        // Native coordination must remain independent of the platform SQL/KV
        // database, including initialization and recovery after actor eviction.
        for(const key of ["sql","kv"])Object.defineProperty(ctx.storage,key,{get(){throw Error("UnexpectedPlatformDatabaseAccess:"+key);}});
        super(ctx,env);
      }
      collectMetadata(...args){this.metadataCalls=(this.metadataCalls??0)+1;return super.collectMetadata(...args);}
      maintenanceDiagnostics(){return{collect_metadata:this.metadataCalls??0};}
      commitMutations(inputs,deadline){this.groupSizes??=[];this.groupSizes.push(inputs.length);return super.commitMutations(inputs,deadline);}
      mutationDiagnostics(){return this.groupSizes??[];}
      openReadView(options){this.views=(this.views??0)+1;return super.openReadView(options);}
      beginPublication(...args){this.begins=(this.begins??0)+1;return super.beginPublication(...args);}
      finalizePublication(...args){this.finals=(this.finals??0)+1;return super.finalizePublication(...args);}
      publicationCall(...args){this.legacyCalls=(this.legacyCalls??0)+1;return super.publicationCall(...args);}
      latencyDiagnostics(){return{views:this.views??0,begins:this.begins??0,finals:this.finals??0,legacy:this.legacyCalls??0};}
      loseMaintenanceFinish(){this.loseFinish=true;return true;}
      finishMaintenance(kind,ticket){if(this.loseFinish){this.loseFinish=false;throw Error("InjectedMaintenanceFinishLoss");}return super.finishMaintenance(kind,ticket);}
      rootFormat(){return this.store.transaction(tx=>JSON.parse(tx.snapshot().root).format??0);}
      async erasePlatformStorage(){await this.ctx.storage.deleteAll();}
      diagnostics(){return this.store.transaction(async tx=>({state:await tx.get("__antfly_collection"),tasks:(await tx.list({prefix:"__antfly_gc/tasks/"})).size,pending:(await tx.list({prefix:"__antfly_gc/pending/"})).size,marks:(await tx.list({prefix:"__antfly_gc/marks/"})).size}));}
    }
    export class CommitCoordinator extends LegacyBase {
      advanceMigration(){const state=this.ctx.storage.kv.get("__antfly_r2_migration");state.ready_at=Date.now()-1;this.ctx.storage.kv.put("__antfly_r2_migration",state);}
      diagnostics(){return {state:this.journal.state(),...this.ctx.storage.sql.exec("SELECT COUNT(*) AS tasks, SUM(done=0) AS pending FROM collection_tasks").toArray()[0],...this.ctx.storage.sql.exec("SELECT COUNT(*) AS marks FROM collection_marks").toArray()[0]};}
    }
    export default {async fetch(request,env,ctx){
      const path=new URL(request.url).pathname,stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION),source=(env.LEGACY_COMMITS??env.COMMITS).getByName(env.ANTFLY_INCARNATION);
      if(path==="/scheduled-queue"){
        const sent=[],local={...env,MAINTENANCE:{async send(message){sent.push(message);}}};
        const denied={getByName(){throw Error("ScheduledDatabaseIoForbidden");}};
        await worker.scheduled({scheduledTime:3600000},{...local,COMMITS:denied,READERS:denied,BUILDERS:denied,DATABASE:new Proxy({}, {get(){throw Error("ScheduledDatabaseIoForbidden");}})});
        const hints=structuredClone(sent);sent.length=0;let ack=false,retry=false;
        await worker.queue({messages:[{body:hints[0],ack(){ack=true;},retry(){retry=true;}}]},local);
        return Response.json({hints,sent,ack,retry,metadata:await stub.maintenanceDiagnostics()});
      }
      if(path==="/runtime-capabilities"){for(const [binding,op] of [["READERS","mutate"],["BUILDERS","lookup"]]){try{await env[binding].getByName(JSON.stringify(["runtime-v1",env.ANTFLY_INCARNATION,0])).execute({op});return Response.json({rejected:false});}catch(error){if(!String(error).includes("RuntimeCapabilityUnsupported"))throw error;}}return Response.json({rejected:true});}
      if(path==="/latency-counts")return Response.json(await stub.latencyDiagnostics());
      if(path==="/mutation-groups"){const tenant=request.headers.get("X-Antfly-Tenant"),name=tenant?JSON.parse(env.ANTFLY_TENANTS)[tenant].incarnation:env.ANTFLY_INCARNATION;return Response.json(await env.COMMITS.getByName(name).mutationDiagnostics());}
      if(path==="/wipe-platform"){await stub.erasePlatformStorage();return Response.json({done:true});}
      if(path==="/advance"){await source.advanceMigration();return Response.json({done:true});}
      if(path==="/diagnostics")return Response.json(await stub.diagnostics());
      if(path==="/source-diagnostics")return Response.json(await source.diagnostics());
      if(path==="/source-frozen-write"){try {await source.objectCall({op:"put",key:"antfly/progress/test/HEAD"},new Uint8Array([1]));return Response.json({accepted:true});}catch(error){return Response.json({error:String(error)});}}
      if(path==="/profile")return Response.json(await stub.ensureEmbeddingProfile({model:"@cf/test/profile",dimensions:4,revision:"1"}));
      if(path==="/native-root")return Response.json(await stub.rootFormat());
      if(path==="/prepare-compaction-message"){const ticket=await stub.requestMaintenance("compact");await stub.loseMaintenanceFinish();return Response.json({incarnation:env.ANTFLY_INCARNATION,kind:"compact",maintenance:{id:ticket.id,step:ticket.step}});}
      if(path==="/deliver-compaction-message"){let ack=0,retry=0;await worker.queue({messages:[{body:await request.json(),ack(){ack++;},retry(){retry++;}}]},env);return Response.json({ack,retry});}
      return worker.fetch(request,{...env,MAINTENANCE:{async send(){}}},ctx);
    }};
  `},{type:"ESModule",path:join(output,"worker.js")},...wasm.map(name=>({type:"CompiledWasm",path:join(output,name)}))],compatibilityDate:"2026-10-23",bindings:{...(inlineRoots?{INLINE_METADATA_ROOTS:"true"}:{}),...(process.env.ANTFLY_TEST_TRACE?{LOG_METRICS:"true"}:{}),EDGE_IMMUTABLE_CACHE_SCOPE:"01234567-0123-4567-89ab-0123456789aa",EDGE_IMMUTABLE_CACHE:"true",API_TOKEN:"test-r2",ANTFLY_INCARNATION:namespace,PAGED_PROJECTIONS:"true",PUBLICATION_RECORDS:"8",COORDINATOR_BACKEND:migration?"sqlite":"r2"},r2Buckets:["DATABASE"],durableObjects:{COMMITS:{className:migration?"CommitCoordinator":"R2CommitCoordinator",useSQLite:true,unsafeUniqueKey:migration?"legacy":"native"}},resourcePersistencePath:join(persist,"resources"),isolatedResourcePersistencePath:join(persist,"isolated")};
  options.durableObjects.READERS={className:"R2ReadRuntime",useSQLite:true,unsafeUniqueKey:"readers"};options.durableObjects.BUILDERS={className:"R2BuildRuntime",useSQLite:true,unsafeUniqueKey:"builders"};
  let mf;
  const reopen=async()=>{if(mf)await mf.dispose();mf=new Miniflare(convertV4MiniflareOptions(options));};
  const call=async(body,path="/api",allowed=200,tenant)=>{
    let lastError;for(let attempt=0;attempt<20;attempt++){
      const response=await mf.dispatchFetch("https://test.invalid"+path,{method:"POST",headers:{Authorization:"Bearer test-r2",...(tenant?{"X-Antfly-Tenant":tenant}:{})},body:JSON.stringify(body)});
      const result=await response.json();lastError=result.error;
      if(response.status===503&&/CoordinatorRecovering|ObjectStoreTransportFailed/.test(result.error)&&allowed===200){continue;}
      assert.equal(response.status,allowed,JSON.stringify(result));return result;
    }
    throw new Error("recovery did not converge: "+lastError);
  };
  await reopen();
  return {options,reopen,call,bucket:()=>mf.getR2Bucket("DATABASE"),close:async()=>{if(mf)await mf.dispose();await rm(persist,{recursive:true,force:true});}};
}
test('native read capsules activate after compatible readers and survive erased platform storage',{timeout:120000},async()=>{
 const f=await fixture('capsule-rollout');
 try{
  const before={op:'mutate',operation_id:'before-capsules',mutations:[{kind:'upsert',doc_id:'before',body:'{"text":"before"}'}]},first=await f.call(before);while((await f.call({op:'publish'})).has_more){}assert.equal(await f.call({},'/native-root'),2);
  // All binaries already parse both formats; writer activation is a separate
  // profile transition over the same persisted R2 data and table identity.
  f.options.bindings.INLINE_METADATA_ROOTS='true';await f.reopen();
  const after={op:'mutate',operation_id:'after-capsules',mutations:[{kind:'upsert',doc_id:'after',body:'{"text":"after"}'}]},second=await f.call(after);assert.equal(JSON.parse((await f.call({op:'lookup',doc_id:'after',required_visibility:second.visibility})).body).text,'after');while((await f.call({op:'publish'})).has_more){}assert.equal(await f.call({},'/native-root'),3);
  await f.call({},'/wipe-platform');await f.reopen();assert.equal((await f.call({op:'count'})).count,'2');assert.equal((await f.call(before)).visibility,first.visibility);assert.equal((await f.call(after)).visibility,second.visibility);assert.equal(await f.call({},'/native-root'),3);
 }finally{await f.close();}
});

test('HTTP callers cannot activate experimental inline formats or compaction probes',{timeout:120000},async()=>{
 const f=await fixture('internal-latency-settings',false,true);
 try{f.options.bindings.PAGED_TEXT='false';f.options.bindings.PAGED_PROJECTIONS='false';await f.reopen();await f.call({op:'mutate',operation_id:'seed',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"durable"}'}]});while((await f.call({op:'publish',inline_point_bodies:true,canonical_point_bodies:true})).has_more){}
  const bucket=await f.bucket();for(const row of(await bucket.list({prefix:'antfly/artifacts/',limit:1000})).objects){const object=await bucket.get(row.key),bytes=new Uint8Array(await object.arrayBuffer());assert.ok(!['AFDFACTB','AFDFACTC'].includes(new TextDecoder().decode(bytes.subarray(0,8))));}
  assert.equal((await f.call({op:'compact',compaction_probe_only:true})).published,true);assert.equal((await f.call({op:'lookup',doc_id:'one'})).body,'{"text":"durable"}');
 }finally{await f.close();}
});

for(const paged of[false,true])test(`native compaction plan preserves the no-op and stages actual publication (paged=${paged})`,{timeout:120000},async()=>{
 const f=await fixture('planned-compact-'+paged,false,true);
 try{f.options.bindings.PAGED_TEXT=String(paged);f.options.bindings.PAGED_PROJECTIONS=String(paged);f.options.bindings.NATIVE_COMPACTION_PLAN='true';await f.reopen();
  await f.call({op:'mutate',operation_id:'seed',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"durable"}'}]});while((await f.call({op:'publish'})).has_more){}
  const before=await f.call({},'/latency-counts'),result=await f.call({op:'compact'}),after=await f.call({},'/latency-counts');assert.equal(result.published,!paged);assert.equal(after.begins-before.begins,paged?0:1);assert.equal(after.finals-before.finals,paged?0:1);assert.equal(after.legacy-before.legacy,0);
  await f.call({op:'publish'});await f.call({},'/wipe-platform');await f.reopen();assert.equal((await f.call({op:'lookup',doc_id:'one'})).body,'{"text":"durable"}');
 }finally{await f.close();}
});

test('optimistic mutation preparation skips a second snapshot while final native commit fences source changes',{timeout:120000},async()=>{
 const f=await fixture('optimistic-wal',false,true);
 try{f.options.bindings.OPTIMISTIC_WAL='true';await f.reopen();await f.call({op:'mutate',operation_id:'seed',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"first"}'}]});while((await f.call({op:'publish'})).has_more){}
  const before=await f.call({},'/latency-counts');const mutation={op:'mutate',operation_id:'second',mutations:[{kind:'upsert',doc_id:'two',body:'{"text":"second"}'}]},receipt=await f.call(mutation),after=await f.call({},'/latency-counts');assert.equal(after.views,before.views,'retained committed capsule is only a preparation hint');
  assert.equal((await f.call(mutation)).visibility,receipt.visibility);while((await f.call({op:'publish'})).has_more){}await f.call({},'/wipe-platform');await f.reopen();assert.equal((await f.call({op:'count'})).count,'2');assert.equal((await f.call(mutation)).visibility,receipt.visibility);
 }finally{await f.close();}
});

test('keyed compaction exposes durable status and replays its original outcome after later publication',{timeout:120000},async()=>{
 const f=await fixture('compaction-receipts');
 try{
  f.options.bindings.PAGED_TEXT='true';await f.reopen();
  const input={op:'compact',operation_id:'maintenance/first'};
  assert.deepEqual(await f.call({op:'compact_status',operation_id:input.operation_id}),{state:'unknown'});
  await f.call({op:'mutate',operation_id:'first',mutations:[{kind:'upsert',doc_id:'first',body:'{"text":"first"}'}]});
  while((await f.call({op:'publish'})).has_more){}
  const result=await f.call(input);assert.equal(result.published,false);
  assert.deepEqual(await f.call({op:'compact_status',operation_id:input.operation_id}),{state:'complete',result});
  await f.call({op:'mutate',operation_id:'second',mutations:[{kind:'upsert',doc_id:'second',body:'{"text":"second"}'}]});
  while((await f.call({op:'publish'})).has_more){}
  await f.call({},'/wipe-platform');await f.reopen();
  assert.deepEqual(await f.call(input),result);assert.equal((await f.call({op:'count'})).count,'2');
  f.options.bindings.COMPACTION_MAX_DOCUMENTS='2000';await f.reopen();
  assert.match((await f.call(input,'/api',409)).error,/CompactionOperationConflict/);
 }finally{await f.close();}
});

test('compaction queue redelivery recovers its completed receipt after ticket completion fails',{timeout:120000},async()=>{
 const f=await fixture('compaction-queue-receipts');
 try{
  f.options.bindings.PAGED_TEXT='true';await f.reopen();
  await f.call({op:'mutate',operation_id:'first',mutations:[{kind:'upsert',doc_id:'first',body:'{"text":"first"}'}]});while((await f.call({op:'publish'})).has_more){}
  const message=await f.call({},'/prepare-compaction-message'),operation_id='queue/compact/'+message.maintenance.id+'/'+message.maintenance.step;
  assert.deepEqual(await f.call(message,'/deliver-compaction-message'),{ack:0,retry:1});
  const completed=await f.call({op:'compact_status',operation_id});assert.equal(completed.state,'complete');
  await f.call({op:'mutate',operation_id:'second',mutations:[{kind:'upsert',doc_id:'second',body:'{"text":"second"}'}]});while((await f.call({op:'publish'})).has_more){}
  await f.reopen();assert.deepEqual(await f.call(message,'/deliver-compaction-message'),{ack:1,retry:0});
  assert.deepEqual(await f.call({op:'compact_status',operation_id}),completed);assert.equal((await f.call({op:'count'})).count,'2');
 }finally{await f.close();}
});

test("R2-native coordination survives eviction, both collectors, reindex and erased platform storage",{timeout:240000},async()=>{
  const f=await fixture("r2-native-fixture",false,true);
  try {
    f.options.bindings.INLINE_POINT_BODIES="true";f.options.bindings.CANONICAL_POINT_BODIES="true";await f.reopen();
    const mutation={op:"mutate",operation_id:"native-source",mutations:Array.from({length:40},(_,i)=>({kind:"upsert",doc_id:`doc/${i}`,body:JSON.stringify({text:"durable native",embedding:[1,i/40],sparse_embedding:{durable:1}})}))};
    const receipt=await f.call(mutation);
    while((await f.call({op:"publish"})).has_more){}
    await f.reopen();
    assert.deepEqual(await f.call(mutation),receipt);
    assert.equal((await f.call({op:"count"})).count,"40");
    const pinnedPage=await f.call({op:"search",text:"durable",limit:2,required_visibility:receipt.visibility});assert.ok(pinnedPage.cursor);
    const reindex=await f.call({op:"reindex"});
    await f.call({op:"reindex",reindex_action:"step",reindex_job_id:reindex.job_id});
    await f.call({op:"mutate",operation_id:"native-edit",mutations:[{kind:"upsert",doc_id:"needle",body:'{"text":"replacementneedle","embedding":[0,1],"sparse_embedding":{"needle":1}}'}]});
    while((await f.call({op:"publish"})).has_more){}
    const nextPage=await f.call({op:"search",text:"durable",limit:2,required_visibility:receipt.visibility,cursor:pinnedPage.cursor});assert.equal(nextPage.visibility,pinnedPage.visibility);assert.equal(new Set([...pinnedPage.hits,...nextPage.hits].map(hit=>hit.doc_id)).size,4);
    let dataDone=false,metaDone=false,dataDelay=0,metaDelay=0;
    for(let i=0;i<128;i++){const r=await f.call({op:"collect"});if(r.done){dataDone=true;break;}if(r.delay_seconds){dataDelay=r.delay_seconds;break;}}
    for(let i=0;i<128;i++){const r=await f.call({op:"collect_metadata"});if(r.done){metaDone=true;break;}if(r.delay_seconds){metaDelay=r.delay_seconds;break;}}
    f.options.bindings.CANONICAL_POINT_BODIES="false";
    await f.call({},"/wipe-platform"); await f.reopen();
    const wait=Math.max(dataDelay,metaDelay);if(wait)await new Promise(resolve=>setTimeout(resolve,wait*1000+200));
    for(let i=0;i<512&&(!dataDone||!metaDone);i++){
      let delay=0;
      if(!dataDone){const r=await f.call({op:"collect"});dataDone=!!r.done;delay=Math.max(delay,r.delay_seconds??0);}
      if(!metaDone){const r=await f.call({op:"collect_metadata"});metaDone=!!r.done;delay=Math.max(delay,r.delay_seconds??0);}
      if(delay)await new Promise(resolve=>setTimeout(resolve,delay*1000+100));
    }
    assert.ok(dataDone&&metaDone);
    for(let i=0;i<32;i++){const r=await f.call({op:"reindex",reindex_action:"step",reindex_job_id:reindex.job_id});if(!r.has_more)break;if(i===31)throw new Error("reindex incomplete");}
    await f.reopen();
    assert.equal((await f.call({op:"count"})).count,"41");
    assert.equal((await f.call({op:"search",text:"replacementneedle"})).hits[0].doc_id,"needle");
    assert.equal((await f.call({op:"search",mode:"vector",vector:[0,1]})).hits[0].doc_id,"needle");
    assert.equal((await f.call({op:"search",mode:"sparse",sparse:[{term:"needle",weight:1}]})).hits[0].doc_id,"needle");
    assert.deepEqual(await f.call(mutation),receipt);
  } finally {await f.close();}
});
test("SQLite migration freezes the source and preserves receipts, profiles and an active frontier",{timeout:120000},async()=>{
  const f=await fixture("r2-migration-fixture",true);
  try{
    const mutation={op:"mutate",operation_id:"migration-source",mutations:Array.from({length:16},(_,i)=>({kind:"upsert",doc_id:`doc/${i}`,body:'{"text":"migrated durable"}'}))};
    const receipt=await f.call(mutation);
    while((await f.call({op:"publish"})).has_more){}
    assert.equal(await f.call({},"/profile"),true);
    for(let i=0;i<3;i++)await f.call({op:"collect"});
    const before=await f.call({},"/source-diagnostics");
    f.options.durableObjects.LEGACY_COMMITS={className:"CommitCoordinator",useSQLite:true,unsafeUniqueKey:"legacy"};
    f.options.durableObjects.COMMITS={className:"R2CommitCoordinator",useSQLite:true,unsafeUniqueKey:"native"};
    Object.assign(f.options.bindings,{COORDINATOR_BACKEND:"r2",COORDINATOR_MIGRATION_REQUIRED:"true",COORDINATOR_STORAGE_ID:"migrated-native-root"});
    await f.reopen();
    assert.match((await f.call({op:"status"},"/api",503)).error,/CoordinatorMigrationRequired|ObjectStoreTransportFailed/);
    const frozen=await f.call({op:"migrate_coordinator"});assert.equal(frozen.phase,"freeze");assert.ok(frozen.delay_seconds>0);
    assert.match((await f.call({},"/source-frozen-write")).error,/CoordinatorMigrationFrozen/);
    await f.call({},"/advance");
    let done=false;
    for(let i=0;i<16;i++){const r=await f.call({op:"migrate_coordinator"});if(r.done){done=true;break;}}
    assert.ok(done);
    const after=await f.call({},"/diagnostics");
    assert.equal(after.tasks,before.tasks);assert.equal(after.pending,before.pending);assert.equal(after.marks,before.marks);assert.equal(after.state.id,before.state.id);
    await f.reopen();
    assert.deepEqual(await f.call(mutation),receipt);
    assert.equal((await f.call({op:"count"})).count,"16");
    assert.equal(await f.call({},"/profile"),true);
    assert.match((await f.call({},"/source-frozen-write")).error,/CoordinatorMigrationFrozen/);
  }finally{await f.close();}
});


test("tenant commit domains isolate identical IDs and admit grouped writers",{timeout:120000},async()=>{
 const f=await fixture("tenant-root");try{
  f.options.bindings.ANTFLY_TENANTS=JSON.stringify({a:{incarnation:"tenant-a",storage_id:"tenant-store-a"},b:{incarnation:"tenant-b",storage_id:"tenant-store-b"}});await f.reopen();
  assert.equal((await f.call({},"/runtime-capabilities")).rejected,true);
  const mutation=label=>({op:"mutate",operation_id:"same-operation",mutations:[{kind:"upsert",doc_id:"same-document",body:JSON.stringify({text:label})}]});
  const a=await f.call(mutation("first"),"/api",200,"a"),b=await f.call(mutation("second"),"/api",200,"b");assert.notEqual(a.visibility,b.visibility);
  while((await f.call({op:"publish"},"/api",200,"a")).has_more){}while((await f.call({op:"publish"},"/api",200,"b")).has_more){}
  assert.equal(JSON.parse((await f.call({op:"lookup",doc_id:"same-document"},"/api",200,"a")).body).text,"first");assert.equal(JSON.parse((await f.call({op:"lookup",doc_id:"same-document"},"/api",200,"b")).body).text,"second");
  const concurrent=Array.from({length:8},(_,i)=>({op:"mutate",operation_id:`concurrent/${7-i}`,mutations:[{kind:"upsert",doc_id:`writer/${i}`,body:'{"text":"concurrent"}'}]}));
  const receipts=await Promise.all(concurrent.map(input=>f.call(input,"/api",200,"a")));
  assert.ok((await f.call({},"/mutation-groups",200,"a")).some(size=>size>1),"concurrent callers must share an actual native WAL construction");
  for(let i=0;i<8;i++)assert.deepEqual(await f.call(concurrent[i],"/api",200,"a"),receipts[i]);
  assert.equal((await f.call({op:"count"},"/api",200,"a")).count,"9");assert.equal((await f.call({op:"count"},"/api",200,"b")).count,"1");assert.equal((await f.call({op:"count"})).count,"0");
  assert.match((await f.call({op:"lookup",doc_id:"same-document",required_visibility:a.visibility},"/api",409,"b")).error,/VisibilityIncarnationMismatch/);
  await f.reopen();assert.deepEqual(await f.call(mutation("first"),"/api",200,"a"),a);assert.match((await f.call({op:"count"},"/api",409,"missing")).error,/UnknownTenant/);
 }finally{await f.close();}
});


for(const adaptive of[false,true])test('native publication checkpoints survive both collectors, cancellation and erased platform storage (adaptive='+adaptive+')',{timeout:240000},async()=>{
 const f=await fixture('publication-continuations',false,true);
 try{
  Object.assign(f.options.bindings,{FUSED_READ_ADMISSION:String(adaptive),NATIVE_READ_STATE:String(adaptive),WAL_DOCUMENT_INDEX:String(adaptive),FOREGROUND_CACHE_POLICY:String(adaptive),NATIVE_WRITE_PLAN:'true',NATIVE_READ_PLAN:'true',LOCAL_METADATA_READS:'true',PUBLICATION_CONTINUATIONS:'true',PUBLICATION_PREPARED_SLICES:String(adaptive),PUBLICATION_ADAPTIVE_SLICES:String(adaptive),PUBLICATION_CONTINUATION_FORCE:'true',PUBLICATION_UNITS:'128',PAGED_TEXT:'true'});await f.reopen();
  const body=JSON.stringify({text:Array.from({length:1024},(_,i)=>'term'+i).join(' '),embedding:[1,0],sparse_embedding:{needle:2}}),input={op:'mutate',operation_id:'large',mutations:[{kind:'upsert',doc_id:'one',body}]},accepted=await f.call(input);
  let started=await f.call({op:'publish'});assert.ok(started.job_id);const abandoned=started.job_id;
  const cancelled=await f.call({op:'publish',publication_action:'cancel',publication_job_id:abandoned});assert.equal(cancelled.published,false);assert.equal(cancelled.phase,'cancelled');
  assert.deepEqual(await f.call({op:'publish',publication_action:'status',publication_job_id:abandoned}),cancelled);
  started=await f.call({op:'publish'});assert.notEqual(started.job_id,abandoned);const job=started.job_id;
  for(let i=0;i<5;i++)await f.call({op:'publish',publication_job_id:job});
  // Trace both live collectors with an incomplete private tree, then discard
  // platform storage and all native instances. R2 is the sole durable source.
  for(let i=0;i<6;i++){await f.call({op:'collect'});await f.call({op:'collect_metadata'});}
  await f.call({},'/wipe-platform');await f.reopen();
  let completed;
  for(let i=0;i<128;i++){const result=await f.call({op:'publish',publication_job_id:job});if(result.published){completed=result;break;}assert.equal(result.wal_end_lsn,'0');}
  assert.ok(completed);assert.equal((await f.call({op:'lookup',doc_id:'one',required_visibility:accepted.visibility})).body,body);
  assert.equal((await f.call({op:'search',text:'term1023'})).hits[0].doc_id,'one');
  assert.equal((await f.call({op:'search',mode:'sparse',sparse:[{term:'needle',weight:1}]})).hits[0].doc_id,'one');
  assert.equal((await f.call({op:'search',mode:'vector',vector:[1,0]})).hits[0].doc_id,'one');
  assert.deepEqual(await f.call(input),accepted);assert.deepEqual(await f.call({op:'publish',publication_action:'status',publication_job_id:job}),completed);
  f.options.bindings.PUBLICATION_CONTINUATION_FORCE='false';await f.reopen();
  await f.call({op:'mutate',operation_id:'small-after-job',mutations:[{kind:'upsert',doc_id:'small',body:'{"text":"small"}'}]});
  const before=await f.call({},'/latency-counts');assert.equal((await f.call({op:'publish'})).published,true);const after=await f.call({},'/latency-counts');
  assert.equal(after.begins-before.begins,1);assert.equal(after.finals-before.finals,1,'ordinary records retain prepared batching under the continuation flag');

 }finally{await f.close();}
});


test('R2 Cron dispatch performs no database I/O and Queue delivery owns maintenance recovery',{timeout:30000},async()=>{
 const f=await fixture('queued-scheduled');
 try{const result=await f.call({},'/scheduled-queue');assert.deepEqual(result.hints,[{incarnation:'queued-scheduled',kind:'maintenance_tick',scheduled_time:3600000}]);assert.equal(result.ack,true);assert.equal(result.retry,false);assert.equal(result.metadata.collect_metadata,1);assert.ok(result.sent.every(m=>m.incarnation==='queued-scheduled'));}
 finally{await f.close();}
});

test('conditional ROOT supports native plans, bounded read cuts and reopen without platform SQL or KV',{timeout:120000},async()=>{
 const f=await fixture('conditional-root-end-to-end',false,true);
 try{
  Object.assign(f.options.bindings,{METADATA_ROOT_BACKEND:'conditional',NATIVE_WRITE_PLAN:'true',NATIVE_WRITE_READ_AHEAD:'true',NATIVE_READ_PLAN:'true',FUSED_READ_ADMISSION:'true',NATIVE_READ_STATE:'true',WAL_DOCUMENT_INDEX:'true',CANONICAL_POINT_BODIES:'true'});await f.reopen();
  const mutation={op:'mutate',operation_id:'conditional-first',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"conditional durable body"}'}]},first=await f.call(mutation);
  while((await f.call({op:'publish'})).has_more){}
  assert.equal((await f.call({op:'lookup',doc_id:'one',required_visibility:first.visibility})).body,mutation.mutations[0].body);
  const updated=await f.call({op:'mutate',operation_id:'conditional-second',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"new fence"}'}]});
  assert.equal((await f.call({op:'lookup',doc_id:'one',required_visibility:updated.visibility})).body,'{"text":"new fence"}');
  await f.call({},'/wipe-platform');await f.reopen();
  assert.equal((await f.call(mutation)).visibility,first.visibility);
  assert.equal((await f.call({op:'lookup',doc_id:'one'})).body,'{"text":"new fence"}');
  while((await f.call({op:'publish'})).has_more){}assert.equal((await f.call({op:'count'})).count,'1');
  const bucket=await f.bucket(),objects=(await bucket.list({prefix:'antfly/coord-v2/',limit:1000})).objects;
  assert.ok(objects.some(o=>o.key.endsWith('/ROOT')));assert.ok(!objects.some(o=>o.key.includes('/epochs/')));
 }finally{await f.close();}
});

test('bounded immutable journal supports native plans, bounded read cuts and reopen without platform SQL or KV',{timeout:120000},async()=>{
 const f=await fixture('bounded-journal-end-to-end',false,true);
 try{
  Object.assign(f.options.bindings,{METADATA_ROOT_BACKEND:'bounded-journal',NATIVE_WRITE_PLAN:'true',NATIVE_WRITE_READ_AHEAD:'true',NATIVE_READ_PLAN:'true',FUSED_READ_ADMISSION:'true',NATIVE_READ_STATE:'true',WAL_DOCUMENT_INDEX:'true',CANONICAL_POINT_BODIES:'true'});await f.reopen();
  const mutation={op:'mutate',operation_id:'conditional-first',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"conditional durable body"}'}]},first=await f.call(mutation);
  while((await f.call({op:'publish'})).has_more){}
  assert.equal((await f.call({op:'lookup',doc_id:'one',required_visibility:first.visibility})).body,mutation.mutations[0].body);
  const updated=await f.call({op:'mutate',operation_id:'conditional-second',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"new fence"}'}]});
  assert.equal((await f.call({op:'lookup',doc_id:'one',required_visibility:updated.visibility})).body,'{"text":"new fence"}');
  await f.call({},'/wipe-platform');await f.reopen();
  assert.equal((await f.call(mutation)).visibility,first.visibility);
  assert.equal((await f.call({op:'lookup',doc_id:'one'})).body,'{"text":"new fence"}');
  while((await f.call({op:'publish'})).has_more){}assert.equal((await f.call({op:'count'})).count,'1');
  const bucket=await f.bucket(),objects=(await bucket.list({prefix:'antfly/coord-v1/',limit:1000})).objects;
  assert.ok(objects.some(o=>o.key.includes('/epochs/')));assert.ok(!objects.some(o=>o.key.endsWith('/ROOT')));
 }finally{await f.close();}
});
