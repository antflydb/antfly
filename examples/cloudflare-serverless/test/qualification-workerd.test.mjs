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
for(const batched of[true,false])test('qualification epochs reopen one logical R2 database and fence previous writers ('+(batched?'prepared':'legacy')+')',{timeout:120000},async()=>{
 const bindings={LOCAL_METADATA_READS:String(batched),WAL_INLINE_TAIL:'true',BATCHED_PUBLICATION:String(batched),API_TOKEN:'qualification-test',ANTFLY_INCARNATION:'qualification-epochs',COORDINATOR_BACKEND:'r2',SPLIT_RUNTIME:'true',PUBLICATION_RECORDS:'8',PAGED_TEXT:'true',PAGED_PROJECTIONS:'true',EXACT_SCAN_RECORDS:'1',EXACT_SCAN_INLINE_STEPS:'1',WORKER_VERSION:{id:'01234567-0123-4567-89ab-0123456789ab'}};
 const classes={writer:'R2CommitCoordinator',reader:'R2ReadRuntime',builder:'R2BuildRuntime'},durableObjects={COMMITS:{className:classes.writer,scriptName:'writer',useSQLite:true},READERS:{className:classes.reader,scriptName:'reader',useSQLite:true},BUILDERS:{className:classes.builder,scriptName:'builder',useSQLite:true}};
 const workers=await Promise.all(['app','writer','reader','builder'].map(async role=>{
  const contents=role==='app'?`import app from './worker.mjs';import{configureKernels,kernelMetrics}from'./kernels.mjs';import{qualificationApp}from'./qualification/physical-epochs.mjs';configureKernels('app');const wrapped=qualificationApp(app);export default{async fetch(request,env,ctx){if(new URL(request.url).pathname==='/_test/admissions'){if(request.headers.get('Authorization')!=='Bearer qualification-test')return new Response('unauthorized',{status:401});const {runtimeName}=await import('./runtime-routing.mjs'),reader=env.READERS.getByName(runtimeName(env.ANTFLY_INCARNATION,0)),writer=env.COMMITS.getByName(env.ANTFLY_INCARNATION),body=await request.json();if(body.action==='execute')return Response.json(await reader.execute(body.input));if(body.action==='release'){await reader.releaseHeldRead();return Response.json({released:true});}return Response.json({holding:await reader.isHolding(),count:await writer.readAdmissionCount(),kernels:await writer.poolBudget(),local_count:await reader.localAdmissionCount(),reader_kernels:await reader.poolBudget(),immutable_hints:await writer.immutableHintCount()});}return wrapped.fetch(request,{...env,DEAD_LETTERS:{async metrics(){return{backlogCount:0};}},MAINTENANCE:{async send(){},async metrics(){return{backlogCount:0,backlogBytes:0};}}},ctx);}};`:`import{${classes[role]} as Base}from'./${role==='writer'?'r2-coordinator':'runtime-actors'}.mjs';import{configureKernels,kernelMetrics}from'./kernels.mjs';import{captureReadView,metadataKeys}from'./read-view.mjs';import{actorEpochEnv,withPublicationDiagnostics}from'./qualification/physical-epochs.mjs';configureKernels('${role}');export class ${classes[role]} extends ${role==='builder'?'withPublicationDiagnostics(Base)':'Base'}{constructor(ctx,env){super(ctx,actorEpochEnv(ctx,env,'${role}'));${role==='reader'?`if(this.readViews){const open=this.readViews.open.bind(this.readViews);this.readViews.open=(...args)=>{this.localAdmissions=(this.localAdmissions??0)+1;return open(...args);};}`:''}}${role==='writer'?`async objectCallInTransaction(tx,request,body){const result=await super.objectCallInTransaction(tx,request,body);if(result.status===0&&request.op==='put'&&request.key==='antfly/progress/'+this.env.ANTFLY_INCARNATION+'/HEAD')await captureReadView({...tx,put(){throw Error('PublishedGenerationUnprotected');}},await metadataKeys(this.env.ANTFLY_INCARNATION));return result;}openReadView(options){this.admissionCalls=(this.admissionCalls??0)+1;return super.openReadView(options);}readAdmissionCount(){return this.admissionCalls??0;}immutableHintCount(){return this.store.readHints?.records.length??0;}poolBudget(){return kernelMetrics();}`:role==='reader'?`localAdmissionCount(){return this.localAdmissions??0;}poolBudget(){return kernelMetrics();}async perform(input,collection,signal,trace,admission){if(input.__test_hold){this.heldReads=(this.heldReads??0)+1;await new Promise(resolve=>(this.releaseReads??=[]).push(resolve));this.heldReads--;}const{__test_hold,...nativeInput}=input;return super.perform(nativeInput,collection,signal,trace,admission);}isHolding(){return this.heldReads??0;}releaseHeldRead(){for(const release of this.releaseReads??[])release();this.releaseReads=[];}`:''}}export default{fetch(){return new Response('private');}};`;
  const output=await build({stdin:{contents,resolveDir:root,sourcefile:'qualification-'+role+'.mjs'},outfile:join(root,'qualification-'+role+'.js'),bundle:true,write:false,format:'esm',platform:'neutral',target:'es2022',external:['cloudflare:workers','*.wasm']});
  return{name:role,modulesRoot:root,modules:[{type:'ESModule',path:join(root,'qualification-'+role+'.js'),contents:output.outputFiles[0].text},...['antfly-serverless.wasm','antfly-serverless-compact.wasm','antfly-serverless-observe.wasm'].map(name=>({type:'CompiledWasm',path:join(root,'dist',name)}))],compatibilityDate:'2026-10-23',bindings,durableObjects,r2Buckets:{DATABASE:'qualification-epochs'}};
 }));
 const mf=new Miniflare(convertV4MiniflareOptions({workers}));
 const call=async(input,epoch=0,path='/api')=>{const response=await mf.dispatchFetch('https://qualification.invalid'+path,{method:'POST',headers:{Authorization:'Bearer qualification-test','X-Antfly-Physical-Epoch':String(epoch)},body:JSON.stringify(input)});return{status:response.status,value:await response.json()};};
 try{
  const input={op:'mutate',operation_id:'seed',mutations:[{kind:'upsert',doc_id:'one',body:'{"text":"durable","embedding":[0,1]}'},{kind:'upsert',doc_id:'a',body:'{"embedding":[1,1.0002]}'},{kind:'upsert',doc_id:'z',body:'{"embedding":[1,1.0001]}'}]},seed=await call(input);assert.equal(seed.status,200);
  assert.equal((await call({op:'publish'})).status,200);
  const diagnostic=await call({},0,'/_qualification/publication');assert.equal(diagnostic.status,200);assert.ok(diagnostic.value.budget>0);assert.equal(diagnostic.value.publication.last_execution.result.published,true);assert.ok(diagnostic.value.publication.last_execution.trace.spans.length>0);
  const admissionPath='/_test/admissions',prior=(await call({action:'counts'},0,admissionPath)).value,before=prior.count,localBefore=prior.local_count;
  assert.ok(prior.immutable_hints>=2,"confirmed publication artifacts must reach the coordinator cache");assert.equal(prior.reader_kernels.reader_pool_slots,4);assert.equal(prior.reader_kernels.reader_pool_limit_bytes,32*1024*1024);
  const budget=(await call({action:'counts'},0,admissionPath)).value.kernels;assert.equal(budget.writer_pool_slots,3);assert.equal(budget.observation_pool_slots,2);assert.equal(budget.writer_pool_limit_bytes,48*1024*1024);assert.equal(budget.wasm_pool_limit_bytes,64*1024*1024);assert.ok(budget.writer_pool_linear_bytes<=48*1024*1024);assert.ok(budget.observation_pool_linear_bytes<=16*1024*1024);assert.ok(budget.wasm_pool_linear_bytes<=64*1024*1024);
  const waiting=[call({action:'execute',input:{op:'lookup',doc_id:'one',__test_hold:true}},0,admissionPath)];
  const until=async check=>{const end=Date.now()+5000;while(!await check()){if(Date.now()>=end)throw Error('read admission did not overlap queued native work');await new Promise(resolve=>setTimeout(resolve,10));}};
  try{
   await until(async()=>(await call({action:'counts'},0,admissionPath)).value.holding);
   waiting.push(call({action:'execute',input:{op:'lookup',doc_id:'a',__test_hold:true}},0,admissionPath));waiting.push(call({action:'execute',input:{op:'lookup',doc_id:'z'}},0,admissionPath));
   await until(async()=>{const state=(await call({action:'counts'},0,admissionPath)).value;return state.count===before+(batched?0:3)&&state.local_count===localBefore+(batched?3:0)&&state.holding===2;});
  }finally{await call({action:'release'},0,admissionPath);await Promise.allSettled(waiting);}
  for(const response of await Promise.all(waiting))assert.equal(response.status,200,JSON.stringify(response.value));
  const query={op:'search',mode:'vector',vector:[1,0],search_effort:1,limit:1,fresh:true};
  let scan=await call(query);assert.equal(scan.status,200,JSON.stringify(scan.value));assert.equal(scan.value.complete,false);assert.deepEqual(scan.value.hits,[]);const original=scan.value.cursor;
  assert.equal((await call({op:'mutate',operation_id:'after-scan',mutations:[{kind:'upsert',doc_id:'a',body:'{"embedding":[1,0]}'}]})).status,200);
  assert.equal((await call({op:'publish'})).status,200);
  const tampered=await call({...query,cursor:original.slice(0,-4)+'AAAA'});assert.equal(tampered.status,409);
  let steps=1;while(!scan.value.complete){scan=await call({...query,cursor:scan.value.cursor},1);assert.equal(scan.status,200,JSON.stringify(scan.value));assert.ok(++steps<10);}
  assert.equal(scan.value.hits[0].doc_id,'z');assert.equal(scan.value.cursor,null);
  assert.equal((await call(query,1)).value.complete,false);
  for(const epoch of[1,2]){const count=await call({op:'count',required_visibility:seed.value.visibility},epoch);assert.equal(count.status,200,JSON.stringify(count.value));assert.equal(count.value.count,'3');assert.equal((await call(input,epoch)).value.visibility,seed.value.visibility);}
  if(batched){const freshObserver=await call({op:'count',required_visibility:seed.value.visibility},1);assert.equal(freshObserver.status,200);assert.equal(freshObserver.value.count,'3');}else{const staleRead=await call({op:'count'},1);assert.equal(staleRead.status,503);assert.match(staleRead.value.error,/ImmutableLogFenced|ObjectStoreTransportFailed/);}
  const stale=await call(input,1);assert.equal(stale.status,503);assert.match(stale.value.error,/ImmutableLogFenced|ObjectStoreTransportFailed/);
  const metrics=await call({},0,'/_qualification/queue');assert.equal(metrics.status,200);assert.equal(metrics.value.backlog_count,0);
 }finally{await mf.dispose();}
});
