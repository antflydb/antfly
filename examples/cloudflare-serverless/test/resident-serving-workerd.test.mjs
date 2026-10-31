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

import{test}from'node:test';import assert from'node:assert/strict';import{readFile,mkdtemp,rm}from'node:fs/promises';import{resolve,join}from'node:path';import{build}from'esbuild';import{Miniflare,convertV4MiniflareOptions}from'miniflare';
test('resident owner serves native zero-I/O points with receipts through updates deletion publication expiry and restart',{timeout:180000},async()=>{
 const root=process.cwd(),dir=await mkdtemp(resolve('dist','resident-serving-workerd-')),bindings={API_TOKEN:'local-resident',ANTFLY_INCARNATION:'resident-fixture',COORDINATOR_BACKEND:'r2',SPLIT_RUNTIME:'true',DIRECT_READS:'true',LOCAL_METADATA_READS:'true',METADATA_ROOT_BACKEND:'bounded-journal',RESIDENT_SERVING:'true',INLINE_METADATA_ROOTS:'true',NATIVE_WRITE_PLAN:'true',WAL_INLINE_TAIL:'true',NATIVE_READ_STATE:'true',WAL_DOCUMENT_INDEX:'true',LOG_METRICS:'true',WORKER_VERSION:{id:'01234567-0123-4567-89ab-0123456789ab'}},classes={writer:'R2CommitCoordinator',builder:'R2BuildRuntime'},durableObjects=Object.fromEntries(Object.entries({COMMITS:'writer',BUILDERS:'builder'}).map(([binding,role])=>[binding,{className:classes[role],scriptName:role,useSQLite:true}])),workers=[];
 for(const role of['app','writer','builder']){
  const source=role==='app'?"import app from'./worker.mjs';import{configureKernels}from'./kernels.mjs';import{takeRpcData}from'./rpc-data.mjs';configureKernels('app');export default{async fetch(request,env,ctx){if(new URL(request.url).pathname==='/other'){try{await takeRpcData(env.COMMITS.getByName('different-owner').lookupResident({op:'lookup',doc_id:'one'}));return Response.json({unexpected:true});}catch(error){return Response.json({error:String(error)});}}if(new URL(request.url).pathname==='/metrics')return Response.json(await takeRpcData(env.COMMITS.getByName(env.ANTFLY_INCARNATION).metrics()));return app.fetch(request,{...env,MAINTENANCE:{async send(){}}},ctx);}};":role==='writer'?"import{configureKernels,kernelMetrics}from'./kernels.mjs';import{R2CommitCoordinator as Base}from'./r2-coordinator.mjs';configureKernels('writer');const metrics=[];let io=0;console.log=value=>{try{const r=JSON.parse(value);if(r.event==='antfly.operation'&&r.resident_owner)metrics.push(r);}catch{}};export class R2CommitCoordinator extends Base{constructor(ctx,env){for(const key of['sql','kv'])Object.defineProperty(ctx.storage,key,{get(){throw Error('UnexpectedPlatformDatabaseAccess:'+key);}});super(ctx,{...env,DATABASE:new Proxy(env.DATABASE,{get(target,key){const value=target[key];return typeof value==='function'?(...args)=>{io++;return value.apply(target,args);}:value;}})});this.instance=crypto.randomUUID();this.store.idleCheckpoint=null;}metrics(){return{owner_term:[...this.store.owner],io,kernels:kernelMetrics(),instance:this.instance,reads:metrics,state:!!this.store.state};}}export default{fetch(){return new Response('private',{status:404});}};":"import{configureKernels}from'./kernels.mjs';configureKernels('builder');export{R2BuildRuntime}from'./runtime-actors.mjs';export default{fetch(){return new Response('private',{status:404});}};";
  const bundle=await build({stdin:{contents:source,resolveDir:root},bundle:true,write:false,format:'esm',platform:'neutral',external:['cloudflare:workers','*.wasm']});workers.push({name:role,modulesRoot:root,modules:[{type:'ESModule',path:resolve(root,'resident-'+role+'.mjs'),contents:bundle.outputFiles[0].text},...await Promise.all(['antfly-serverless.wasm','antfly-serverless-compact.wasm','antfly-serverless-observe.wasm'].map(async name=>({type:'CompiledWasm',path:resolve(root,'dist',name),contents:await readFile(resolve(root,'dist',name))})))],compatibilityDate:'2026-10-23',bindings,r2Buckets:{DATABASE:'resident-fixture'},durableObjects});
 }
 const options={workers,resourcePersistencePath:join(dir,'resources'),isolatedResourcePersistencePath:join(dir,'isolated')};let mf=new Miniflare(convertV4MiniflareOptions(options));
 const call=async(input,expected=200,recoverKeyed=false)=>{for(let attempt=0;attempt<4;attempt++){const response=await mf.dispatchFetch('https://resident.invalid/api',{method:'POST',headers:{Authorization:'Bearer '+bindings.API_TOKEN},body:JSON.stringify(input)}),value=await response.json();if(recoverKeyed&&input.op==='mutate'&&input.operation_id&&response.status===503&&value.error==='CoordinatorRecovering')continue;assert.equal(response.status,expected,JSON.stringify(value));return value;}throw Error('KeyedRecoveryDidNotConverge');};
 const metrics=async()=>await(await mf.dispatchFetch('https://resident.invalid/metrics')).json();
 try{
  assert.equal((await call({op:'lookup',doc_id:'missing'})).body,null);
  let receipt=await call({op:'mutate',operation_id:'one',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"first"}'}]});
  for(let i=0;i<6;i++)assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility})).body,'{"text":"first"}');
  const warm=await metrics();assert.equal(warm.kernels.writer_pool_limit_bytes,32*1024*1024);assert.equal(warm.kernels.resident_pool_limit_bytes,16*1024*1024);assert.ok(warm.kernels.wasm_pool_linear_bytes<=warm.kernels.wasm_pool_limit_bytes);assert.ok(warm.reads.some(r=>r.resident_read_cache_hits>0&&!(r.physical?.calls>0)),JSON.stringify(warm));
  const beforeHit=await metrics();await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility});assert.equal((await metrics()).io,beforeHit.io,'warm owner read must perform zero physical object-store calls');
  const other=await(await mf.dispatchFetch('https://resident.invalid/other')).json();assert.match(other.error,/ResidentServingOwnerMismatch/);
  for(let steps=0;;steps++){assert.ok(steps<64);if(!(await call({op:'publish'})).has_more)break;}
  receipt=await call({op:'mutate',operation_id:'two',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"second"}'}]});
  assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility,resident_cut:Array(32).fill(0)})).body,'{"text":"second"}');
  for(let steps=0;;steps++){assert.ok(steps<64);if(!(await call({op:'publish'})).has_more)break;}
  assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility})).body,'{"text":"second"}');
  let dataDone=false,metadataDone=false;for(let i=0;i<3;i++){dataDone=!!(await call({op:'collect'})).done;metadataDone=!!(await call({op:'collect_metadata'})).done;}
  receipt=await call({op:'mutate',operation_id:'delete',mutations:[{kind:'delete',doc_id:'one'}]});
  for(let i=0;i<4;i++)assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility})).body,null);
  const wrong=receipt.visibility.slice(0,2)+'0'.repeat(64)+receipt.visibility.slice(66);const bad=await call({op:'lookup',doc_id:'one',required_visibility:wrong},409);assert.match(bad.error,/VisibilityIncarnationMismatch/);
  const future=receipt.visibility.slice(0,-16)+'ffffffffffffffff';const ahead=await call({op:'lookup',doc_id:'one',required_visibility:future},409);assert.match(ahead.error,/WalVisibilityUnavailable/);
  const before=await metrics();for(let i=0;i<8;i++){await new Promise(r=>setTimeout(r,4000));assert.equal((await metrics()).instance,before.instance);}
  assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility})).body,null);
  const gcDeadline=Date.now()+90000;while(!dataDone||!metadataDone){assert.ok(Date.now()<gcDeadline,'both collectors must converge');if(!dataDone)dataDone=!!(await call({op:'collect'})).done;if(!metadataDone)metadataDone=!!(await call({op:'collect_metadata'})).done;assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility})).body,null);if(!dataDone||!metadataDone)await new Promise(r=>setTimeout(r,2000));}
  await Promise.all(Array.from({length:8},async(_,i)=>{const r=await call({op:'mutate',operation_id:'concurrent/'+i,mutations:[{kind:'upsert',doc_id:'key/'+i,body:JSON.stringify({i})}]});assert.equal((await call({op:'lookup',doc_id:'key/'+i,required_visibility:r.visibility})).body,JSON.stringify({i}));}));
  const largeBody=JSON.stringify({text:'large '.repeat(20000)}),large=await call({op:'mutate',operation_id:'large-body',mutations:[{kind:'upsert',doc_id:'large',body:largeBody}]},200,true);for(let i=0;i<3;i++)assert.equal((await call({op:'lookup',doc_id:'large',required_visibility:large.visibility})).body,largeBody);const bounded=(await metrics()).kernels;assert.ok(bounded.wasm_pool_linear_bytes<=bounded.wasm_pool_limit_bytes);
  const oldOwnerTerm=(await metrics()).owner_term;
  await mf.dispose();mf=new Miniflare(convertV4MiniflareOptions(options));assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility})).body,null);assert.notDeepEqual((await metrics()).owner_term,oldOwnerTerm,'reconstruction must acquire a fresh native fence');
  assert.equal((await call({op:'lookup',doc_id:'large',required_visibility:large.visibility})).body,largeBody);
  for(let i=0;i<8;i++)assert.equal((await call({op:'lookup',doc_id:'key/'+i})).body,JSON.stringify({i}));
 }finally{await mf.dispose();await rm(dir,{recursive:true,force:true});}
});
