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
test('split native readers authenticate advisory commit locations and recover after losing every hint',{timeout:120000},async()=>{
 const root=process.cwd(),dir=await mkdtemp(resolve('dist','metadata-hint-workerd-')),bindings={API_TOKEN:'local-metadata-hints',ANTFLY_INCARNATION:'metadata-hints',COORDINATOR_BACKEND:'r2',SPLIT_RUNTIME:'true',DIRECT_READS:'true',LOCAL_METADATA_READS:'true',METADATA_ROOT_BACKEND:'bounded-journal',METADATA_COMMIT_HINTS:'true',INLINE_METADATA_ROOTS:'true',FUSED_READ_ADMISSION:'true',NATIVE_WRITE_PLAN:'true',WAL_INLINE_TAIL:'true',FENCED_READ_LEASES:'false',LOG_METRICS:'true'},classes={writer:'R2CommitCoordinator',reader:'R2ReadRuntime',builder:'R2BuildRuntime'},durableObjects=Object.fromEntries(Object.entries({COMMITS:'writer',READERS:'reader',BUILDERS:'builder'}).map(([binding,role])=>[binding,{className:classes[role],scriptName:role,useSQLite:true}])),workers=[];
 for(const role of['app','writer','reader','builder']){
  const source=role==='app'?"import app from'./worker.mjs';import{configureKernels}from'./kernels.mjs';import{directReadViews}from'./direct-read-views.mjs';configureKernels('app');const observations=[];console.log=value=>{try{const row=JSON.parse(value);if(row.event==='antfly.coordinator_snapshot')observations.push(row);}catch{}};export default{fetch(request,env,ctx){if(new URL(request.url).pathname==='/metrics')return Response.json({views:directReadViews.metrics(),observations});return app.fetch(request,{...env,MAINTENANCE:{async send(){}}},ctx);}};":role==='writer'?"import{configureKernels}from'./kernels.mjs';import{R2CommitCoordinator as Base}from'./r2-coordinator.mjs';configureKernels('writer');export class R2CommitCoordinator extends Base{constructor(ctx,env){for(const key of['sql','kv'])Object.defineProperty(ctx.storage,key,{get(){throw Error('UnexpectedPlatformDatabaseAccess:'+key);}});super(ctx,env);this.store.idleCheckpoint=null;}}export default{fetch(){return new Response('private',{status:404});}};":`import{configureKernels}from'./kernels.mjs';configureKernels('${role}');export{${classes[role]}}from'./runtime-actors.mjs';export default{fetch(){return new Response('private',{status:404});}};`;
  const bundle=await build({stdin:{contents:source,resolveDir:root},bundle:true,write:false,format:'esm',platform:'neutral',external:['cloudflare:workers','*.wasm']});workers.push({name:role,modulesRoot:root,modules:[{type:'ESModule',path:resolve(root,'metadata-hint-'+role+'.mjs'),contents:bundle.outputFiles[0].text},...await Promise.all(['antfly-serverless.wasm','antfly-serverless-compact.wasm','antfly-serverless-observe.wasm'].map(async name=>({type:'CompiledWasm',path:resolve(root,'dist',name),contents:await readFile(resolve(root,'dist',name))})))],compatibilityDate:'2026-10-23',bindings,r2Buckets:{DATABASE:'metadata-hints'},durableObjects});
 }
 const options={workers,resourcePersistencePath:join(dir,'resources'),isolatedResourcePersistencePath:join(dir,'isolated')};let mf=new Miniflare(convertV4MiniflareOptions(options));const call=async input=>{const response=await mf.dispatchFetch('https://metadata-hints.invalid/api',{method:'POST',headers:{Authorization:'Bearer '+bindings.API_TOKEN},body:JSON.stringify(input)}),value=await response.json();assert.equal(response.status,200,JSON.stringify(value));assert.equal(Object.hasOwn(value,'__metadata_hint'),false);return value;};
 try{
  let receipt;for(let i=0;i<8;i++)receipt=await call({op:'mutate',operation_id:'hint/'+i,mutations:[{kind:'upsert',doc_id:'one',body:JSON.stringify({text:'value '+i})}]});
  assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility})).body,'{"text":"value 7"}');const metrics=await(await mf.dispatchFetch('https://metadata-hints.invalid/metrics')).json();assert.ok(metrics.observations.some(row=>row.journal_hint_accepted===1),JSON.stringify(metrics));assert.ok(metrics.views.app_read_view_hint_bytes>0&&metrics.views.app_read_view_hint_bytes<=4096);
  await mf.dispose();mf=new Miniflare(convertV4MiniflareOptions(options));assert.equal((await call({op:'lookup',doc_id:'one',required_visibility:receipt.visibility})).body,'{"text":"value 7"}');const cold=await(await mf.dispatchFetch('https://metadata-hints.invalid/metrics')).json();assert.equal(cold.views.app_read_view_hint_bytes,0);assert.ok(cold.observations.every(row=>!row.journal_hint_accepted));
 }finally{await mf.dispose();await rm(dir,{recursive:true,force:true});}
});
