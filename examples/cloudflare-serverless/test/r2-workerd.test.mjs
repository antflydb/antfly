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
async function fixture(namespace, migration = false) {
  const persist=await mkdtemp(join(tmpdir(),"antfly-r2-workerd-"));
  const output=fileURLToPath(new URL("../dist/worker/",import.meta.url));
  const wasm=(await readdir(output)).filter(name=>name.endsWith(".wasm"));
  const options={modulesRoot:output,modules:[{type:"ESModule",path:join(output,"r2-harness.mjs"),contents:`
    import worker, {R2CommitCoordinator as R2Base,CommitCoordinator as LegacyBase} from "./worker.js";
    export class R2CommitCoordinator extends R2Base {
      commitMutations(inputs){this.groupSizes??=[];this.groupSizes.push(inputs.length);return super.commitMutations(inputs);}
      mutationDiagnostics(){return this.groupSizes??[];}
      async erasePlatformStorage(){await this.ctx.storage.deleteAll();}
      diagnostics(){return this.store.transaction(async tx=>({state:await tx.get("__antfly_collection"),tasks:(await tx.list({prefix:"__antfly_gc/tasks/"})).size,pending:(await tx.list({prefix:"__antfly_gc/pending/"})).size,marks:(await tx.list({prefix:"__antfly_gc/marks/"})).size}));}
    }
    export class CommitCoordinator extends LegacyBase {
      advanceMigration(){const state=this.ctx.storage.kv.get("__antfly_r2_migration");state.ready_at=Date.now()-1;this.ctx.storage.kv.put("__antfly_r2_migration",state);}
      diagnostics(){return {state:this.journal.state(),...this.ctx.storage.sql.exec("SELECT COUNT(*) AS tasks, SUM(done=0) AS pending FROM collection_tasks").toArray()[0],...this.ctx.storage.sql.exec("SELECT COUNT(*) AS marks FROM collection_marks").toArray()[0]};}
    }
    export default {async fetch(request,env,ctx){
      const path=new URL(request.url).pathname,stub=env.COMMITS.getByName(env.ANTFLY_INCARNATION),source=(env.LEGACY_COMMITS??env.COMMITS).getByName(env.ANTFLY_INCARNATION);
      if(path==="/mutation-groups"){const tenant=request.headers.get("X-Antfly-Tenant"),name=tenant?JSON.parse(env.ANTFLY_TENANTS)[tenant].incarnation:env.ANTFLY_INCARNATION;return Response.json(await env.COMMITS.getByName(name).mutationDiagnostics());}
      if(path==="/wipe-platform"){await stub.erasePlatformStorage();return Response.json({done:true});}
      if(path==="/advance"){await source.advanceMigration();return Response.json({done:true});}
      if(path==="/diagnostics")return Response.json(await stub.diagnostics());
      if(path==="/source-diagnostics")return Response.json(await source.diagnostics());
      if(path==="/source-frozen-write"){try {await source.objectCall({op:"put",key:"antfly/progress/test/HEAD"},new Uint8Array([1]));return Response.json({accepted:true});}catch(error){return Response.json({error:String(error)});}}
      if(path==="/profile")return Response.json(await stub.ensureEmbeddingProfile({model:"@cf/test/profile",dimensions:4,revision:"1"}));
      return worker.fetch(request,{...env,MAINTENANCE:{async send(){}}},ctx);
    }};
  `},{type:"ESModule",path:join(output,"worker.js")},...wasm.map(name=>({type:"CompiledWasm",path:join(output,name)}))],compatibilityDate:"2026-10-23",bindings:{API_TOKEN:"test-r2",ANTFLY_INCARNATION:namespace,PAGED_PROJECTIONS:"true",PUBLICATION_RECORDS:"8",COORDINATOR_BACKEND:migration?"sqlite":"r2"},r2Buckets:["DATABASE"],durableObjects:{COMMITS:{className:migration?"CommitCoordinator":"R2CommitCoordinator",useSQLite:true,unsafeUniqueKey:migration?"legacy":"native"}},resourcePersistencePath:join(persist,"resources"),isolatedResourcePersistencePath:join(persist,"isolated")};
  let mf;
  const reopen=async()=>{if(mf)await mf.dispose();mf=new Miniflare(convertV4MiniflareOptions(options));};
  const call=async(body,path="/api",allowed=200,tenant)=>{
    for(let attempt=0;attempt<20;attempt++){
      const response=await mf.dispatchFetch("https://test.invalid"+path,{method:"POST",headers:{Authorization:"Bearer test-r2",...(tenant?{"X-Antfly-Tenant":tenant}:{})},body:JSON.stringify(body)});
      const result=await response.json();
      if(response.status===503&&/CoordinatorRecovering|ObjectStoreTransportFailed/.test(result.error)&&allowed===200){continue;}
      assert.equal(response.status,allowed,JSON.stringify(result));return result;
    }
    throw new Error("recovery did not converge");
  };
  await reopen();
  return {options,reopen,call,close:async()=>{if(mf)await mf.dispose();await rm(persist,{recursive:true,force:true});}};
}
test("R2-native coordination survives eviction, both collectors, reindex and erased platform storage",{timeout:240000},async()=>{
  const f=await fixture("r2-native-fixture");
  try {
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
