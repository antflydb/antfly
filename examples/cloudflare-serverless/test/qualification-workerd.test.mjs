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

import{test}from'node:test';
import assert from'node:assert/strict';
import{build}from'esbuild';
import{Miniflare,convertV4MiniflareOptions}from'miniflare';
import{fileURLToPath}from'node:url';
import{join}from'node:path';
const root=fileURLToPath(new URL('../',import.meta.url));
test('qualification epochs reopen one logical R2 database and fence previous actors',{timeout:120000},async()=>{
 const bindings={API_TOKEN:'qualification-test',ANTFLY_INCARNATION:'qualification-epochs',COORDINATOR_BACKEND:'r2',SPLIT_RUNTIME:'true',PUBLICATION_RECORDS:'8',PAGED_TEXT:'true',WORKER_VERSION:{id:'01234567-0123-4567-89ab-0123456789ab'}};
 const classes={writer:'R2CommitCoordinator',reader:'R2ReadRuntime',builder:'R2BuildRuntime'},durableObjects={COMMITS:{className:classes.writer,scriptName:'writer',useSQLite:true},READERS:{className:classes.reader,scriptName:'reader',useSQLite:true},BUILDERS:{className:classes.builder,scriptName:'builder',useSQLite:true}};
 const workers=await Promise.all(['app','writer','reader','builder'].map(async role=>{
  const contents=role==='app'?`import app from './worker.mjs';import{configureKernels}from'./kernels.mjs';import{qualificationApp}from'./qualification/physical-epochs.mjs';configureKernels('app');const wrapped=qualificationApp(app);export default{fetch(request,env,ctx){return wrapped.fetch(request,{...env,DEAD_LETTERS:{async metrics(){return{backlogCount:0};}},MAINTENANCE:{async send(){},async metrics(){return{backlogCount:0,backlogBytes:0};}}},ctx);}};`:`import{${classes[role]} as Base}from'./${role==='writer'?'r2-coordinator':'runtime-actors'}.mjs';import{configureKernels}from'./kernels.mjs';import{actorEpochEnv}from'./qualification/physical-epochs.mjs';configureKernels('${role}');export class ${classes[role]} extends Base{constructor(ctx,env){super(ctx,actorEpochEnv(ctx,env,'${role}'));}}export default{fetch(){return new Response('private');}};`;
  const output=await build({stdin:{contents,resolveDir:root,sourcefile:'qualification-'+role+'.mjs'},outfile:join(root,'qualification-'+role+'.js'),bundle:true,write:false,format:'esm',platform:'neutral',target:'es2022',external:['cloudflare:workers','*.wasm']});
  return{name:role,modulesRoot:root,modules:[{type:'ESModule',path:join(root,'qualification-'+role+'.js'),contents:output.outputFiles[0].text},...['antfly-serverless.wasm','antfly-serverless-compact.wasm'].map(name=>({type:'CompiledWasm',path:join(root,'dist',name)}))],compatibilityDate:'2026-10-23',bindings,durableObjects,r2Buckets:{DATABASE:'qualification-epochs'}};
 }));
 const mf=new Miniflare(convertV4MiniflareOptions({workers}));
 const call=async(input,epoch=0,path='/api')=>{const response=await mf.dispatchFetch('https://qualification.invalid'+path,{method:'POST',headers:{Authorization:'Bearer qualification-test','X-Antfly-Physical-Epoch':String(epoch)},body:JSON.stringify(input)});return{status:response.status,value:await response.json()};};
 try{
  const input={op:'mutate',operation_id:'seed',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"durable"}'}]},seed=await call(input);assert.equal(seed.status,200);
  assert.equal((await call({op:'publish'})).status,200);
  for(const epoch of[1,2]){const count=await call({op:'count',required_visibility:seed.value.visibility},epoch);assert.equal(count.status,200,JSON.stringify(count.value));assert.equal(count.value.count,'1');assert.equal((await call(input,epoch)).value.visibility,seed.value.visibility);}
  const stale=await call({op:'count'},1);assert.equal(stale.status,503);assert.match(stale.value.error,/ImmutableLogFenced|ObjectStoreTransportFailed/);
  const metrics=await call({},0,'/_qualification/queue');assert.equal(metrics.status,200);assert.equal(metrics.value.backlog_count,0);
 }finally{await mf.dispose();}
});
