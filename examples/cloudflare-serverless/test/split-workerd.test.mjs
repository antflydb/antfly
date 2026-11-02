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

// Two Miniflare startups compile all role modules. Budget fixture startup and
// eviction separately from the unchanged five-second writer-progress assertion.
for(const blocks of [false,true])test(`split scripts keep writer and reader capacity available behind a suspended builder (blocks=${blocks})`,{timeout:240000},async t=>{
 const checkpoint=message=>{t.diagnostic(message);if(process.env.ANTFLY_TEST_TRACE)console.log(JSON.stringify({checkpoint:message,at:Date.now()}));};
 const output=fileURLToPath(new URL("../dist/worker/",import.meta.url)),persist=await mkdtemp(join(tmpdir(),"antfly-split-")),wasm=(await readdir(output)).filter(name=>name.endsWith(".wasm"));
 const bindings={...(process.env.ANTFLY_TEST_TRACE?{LOG_METRICS:"true"}:{}),API_TOKEN:"split-test",ANTFLY_INCARNATION:"split",COORDINATOR_BACKEND:"r2",SPLIT_RUNTIME:"true",PUBLICATION_RECORDS:"8",PAGED_TEXT:"true",...(blocks?{METADATA_ROOT_BACKEND:"bounded-journal",NATIVE_WRITE_PLAN:"true",WAL_PACKED_PAGES:"true",WAL_DEFERRED_INDEXES:"true",POINT_READ_BLOCKS:"true",CANONICAL_POINT_BODIES:"true",QUALIFICATION_PLACEMENT_PROBE:"true"}:{})};
 const durableObjects={COMMITS:{className:"R2CommitCoordinator",scriptName:"writer",useSQLite:true},READERS:{className:"R2ReadRuntime",scriptName:"reader",useSQLite:true},BUILDERS:{className:"R2BuildRuntime",scriptName:"builder",useSQLite:true}};
 const entries={
  app:`import app,{configureKernels} from "./worker.js";configureKernels("app");export default{async fetch(request,env,ctx){const path=new URL(request.url).pathname,stub=env.BUILDERS.getByName(JSON.stringify(["runtime-v1",env.ANTFLY_INCARNATION,0]));if(path==="/hold")return Response.json(await stub.holdPublish());if(path==="/blocked")return Response.json(await stub.blocked());if(path==="/release"){await stub.release();return Response.json({released:true});}return app.fetch(request,{...env,MAINTENANCE:{async send(){}}},ctx);}};`,
  writer:`import{configureKernels}from"./worker.js";configureKernels("writer");export{R2CommitCoordinator}from"./worker.js";export default{fetch(){return new Response("private",{status:404});}};`,
  reader:`import{configureKernels}from"./worker.js";configureKernels("reader");export{R2ReadRuntime}from"./worker.js";export default{fetch(){return new Response("private",{status:404});}};`,
  builder:`import{configureKernels}from"./worker.js";configureKernels("builder");import{R2BuildRuntime as Base}from"./worker.js";export class R2BuildRuntime extends Base{constructor(ctx,env){let actor;const database=new Proxy(env.DATABASE,{get(target,key){const value=target[key];return typeof value==="function"?async(...args)=>{if(key==="get"&&actor?.gate){actor.waiting=true;await actor.gate;}return value.apply(target,args);}:value;}});super(ctx,{...env,DATABASE:database});actor=this;}async holdPublish(){this.gate=new Promise(resolve=>this.unblock=resolve);try{return await this.execute({op:"publish"});}finally{this.gate=null;this.waiting=false;}}blocked(){return !!this.waiting;}release(){this.unblock?.();this.gate=null;}}export default{fetch(){return new Response("private",{status:404});}};`
 };
 const options={workers:Object.entries(entries).map(([name,contents])=>({name,modulesRoot:output,modules:[{type:"ESModule",path:join(output,`split-${name}.mjs`),contents},{type:"ESModule",path:join(output,"worker.js")},...wasm.map(file=>({type:"CompiledWasm",path:join(output,file)}))],compatibilityDate:"2026-10-23",bindings,r2Buckets:{DATABASE:"shared-split"},durableObjects})),resourcePersistencePath:join(persist,"resources"),isolatedResourcePersistencePath:join(persist,"isolated")};
 let mf=new Miniflare(convertV4MiniflareOptions(options));
 const call=async(input,path="/api")=>{const response=await mf.dispatchFetch("https://split.invalid"+path,{method:"POST",headers:{Authorization:"Bearer split-test"},body:JSON.stringify(input)});const value=await response.json();assert.equal(response.status,200,JSON.stringify(value));return value;};
 let holding=false;
 try{
  await call({op:"mutate",operation_id:"seed",mutations:[{kind:"upsert",doc_id:"one",body:'{"text":"first"}'}]});while((await call({op:"publish"})).has_more){}
  checkpoint('seed publication completed');
  if(blocks){const placement=await call({},'/_qualification/placement');assert.equal(placement.qualification,false);assert.equal(placement.samples.length,4);assert.ok(placement.samples.every(s=>s.authority_present));assert.equal(placement.actual_bucket_region,null);}
  await call({op:"mutate",operation_id:"pending",mutations:[{kind:"upsert",doc_id:"pending",body:'{"text":"pending"}'}]});
  holding=true;const held=call({},"/hold").then(value=>({value}),error=>({error}));
  let blocked=false;for(let i=0;i<100;i++){if(await call({},"/blocked")){blocked=true;break;}await new Promise(resolve=>setTimeout(resolve,5));}assert.ok(blocked,"builder must suspend inside its native R2 read");
  checkpoint('builder suspended');
  const input={op:"mutate",operation_id:"independent",mutations:[{kind:"upsert",doc_id:"independent",body:'{"text":"writer progressed"}'}]};
  const receipt=await Promise.race([call(input),new Promise((_,reject)=>setTimeout(()=>reject(Error("writer blocked behind builder")),5000))]);
  checkpoint('independent writer committed');
  assert.equal(JSON.parse((await call({op:"lookup",doc_id:"independent",required_visibility:receipt.visibility})).body).text,"writer progressed");
  await call({},"/release");holding=false;await held;while((await call({op:"publish"})).has_more){}
  checkpoint('publication drained after release');
  assert.equal((await call({op:"count"})).count,"3");checkpoint("exact count verified before eviction");await mf.dispose();checkpoint("first isolate disposed");if(blocks){bindings.WAL_PACKED_PAGES="false";bindings.WAL_DEFERRED_INDEXES="false";bindings.POINT_READ_BLOCKS="false";}mf=new Miniflare(convertV4MiniflareOptions(options));assert.deepEqual(await call(input),receipt);checkpoint("receipt replay verified after eviction");
 }finally{if(holding)await call({},"/release").catch(()=>{});await mf.dispose();checkpoint("final isolate disposed");await rm(persist,{recursive:true,force:true});}
});
