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

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {readdir,readFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Miniflare,convertV4MiniflareOptions} from 'miniflare';
const output=fileURLToPath(new URL('../dist/sql-worker/',import.meta.url));
const persist=fileURLToPath(new URL('../dist/sql-workerd-state/',import.meta.url));
const fixture=JSON.parse(await readFile(new URL('../dist/sql-fixture.json',import.meta.url)));
const secret='sql-workerd-private-service-secret-32bytes';
function headers(){
 const now=Math.floor(Date.now()/1000),claims={iss:'sql:workerd',sub:'native',aud:'antfly-internal-v1',principal_kind:'service',admin:true,iat:now,exp:now+60};
 const bytes=[{alg:'HS256',typ:'JWT'},claims].map(v=>Buffer.from(JSON.stringify(v)).toString('base64url')).join('.');
 return{'X-Antfly-Trusted-Principal':bytes+'.'+createHmac('sha256',secret).update(bytes).digest('base64url'),'Content-Type':'application/json'};
}
test('workerd coordinates retained lake SQL through alarms, Queues and R2 across restart',{timeout:120000},async()=>{
 await rm(persist,{recursive:true,force:true});
 const wasm=(await readdir(output)).filter(name=>name.endsWith('.wasm'));
 const options={modulesRoot:output,modules:[{type:'ESModule',path:join(output,'sql-worker.js')},...wasm.map(name=>({type:'CompiledWasm',path:join(output,name)}))],compatibilityDate:'2026-10-19',bindings:{SQL_SERVICE_SECRET:secret,SQL_SERVICE_ISSUER:'sql:workerd',SQL_CONFIG:JSON.stringify(fixture.config),SQL_BUCKET_BINDINGS:JSON.stringify({execution:'SQL_EXECUTION',source:'SQL_SOURCE'})},resourcePersistencePath:join(persist,'resources'),isolatedResourcePersistencePath:join(persist,'isolated'),r2Buckets:['SQL_EXECUTION','SQL_SOURCE'],durableObjects:{SQL_COORDINATORS:{className:'SQLCoordinator',useSQLite:true}},queueProducers:{SQL_TASKS:'sql-tasks'},queueConsumers:{'sql-tasks':{maxBatchSize:1,maxBatchTimeout:0,maxRetries:10}}};
 let mf=new Miniflare(convertV4MiniflareOptions(options));
 const call=async(path,body)=>{
  const response=await mf.dispatchFetch('https://sql/internal/v1/sql/'+path,{method:'POST',headers:headers(),body:typeof body==='string'?body:JSON.stringify(body)});
  const result=await response.json();assert.equal(response.status,200,JSON.stringify(result));return result;
 };
 try{
  const buckets={source:await mf.getR2Bucket('SQL_SOURCE'),execution:await mf.getR2Bucket('SQL_EXECUTION')};
  for(const item of fixture.objects)await buckets[item.bucket].put(item.key,Buffer.from(item.body_base64,'base64'));
  const unsigned=await mf.dispatchFetch('https://sql/internal/v1/sql/queries/submit',{method:'POST',body:fixture.raw_plan});assert.equal(unsigned.status,401);
  const malformed=JSON.parse(fixture.raw_plan);malformed.join.condition={version:2,instructions:[{type:{kind:'boolean'},operation:{call:{function:'is_missing',args:[]}}}],root:0,output:{kind:'boolean'}};
  for(const invalid of [JSON.stringify(malformed),'{','null','[]','{}',JSON.stringify({tenant:fixture.tenant})]){
   const response=await mf.dispatchFetch('https://sql/internal/v1/sql/queries/submit',{method:'POST',headers:headers(),body:invalid});assert.equal(response.status,400,await response.text());
  }
  const {query}=await call('queries/submit',fixture.raw_plan),locator={tenant:fixture.tenant,query};
  let result;
  const deadline=Date.now()+60000;
  // No control-turn requests: alarms and queue completions must make progress.
  while(Date.now()<deadline){result=await call('queries/result',locator);if(result)break;await new Promise(resolve=>setTimeout(resolve,100));}
  assert.ok(result,'durable alarms and queue processing did not converge');assert.equal(result.rows,2);assert.equal(result.pages.length,1);
  await mf.dispose();mf=new Miniflare(convertV4MiniflareOptions(options));
  assert.deepEqual(await call('queries/result',locator),result);
  const bucket=await mf.getR2Bucket('SQL_EXECUTION');
  const originalId=typeof query.id==='string'?query.id:String.fromCharCode(...query.id);
  const baseRecord=await(await bucket.get(`${fixture.config.state.prefix}/sql-execution-v2/${String(query.expires_ms).padStart(20,'0')}/${originalId}/query.json`)).json();
  for(const [digit,expires_ms,stage,exists]of [['c',query.expires_ms,1,true],['d',1,0,true],['e',query.expires_ms,0,false]]){
   const id=Array(64).fill(digit.charCodeAt(0)),retired={id,expires_ms};
   if(exists){
    const record={...baseRecord,plan:id,expires_ms,stage,status:'pending',owner:'',epoch:0,until_ms:0,cursor:0,pending:[],output:null};
    await bucket.put(`${fixture.config.state.prefix}/sql-execution-v2/${String(expires_ms).padStart(20,'0')}/${digit.repeat(64)}/query.json`,JSON.stringify(record));
   }
   const response=await mf.dispatchFetch('https://sql/internal/v1/sql/tasks/execute',{method:'POST',headers:headers(),body:JSON.stringify({tenant:fixture.tenant,query:retired,stage:0,task:0})});
   assert.equal(response.status,204,'a fenced, expired or collected task was reported as a worker failure');
   assert.equal(await response.text(),'');
  }
  const cancelledPlan=JSON.parse(fixture.raw_plan);cancelledPlan.expires_ms-=1;
  const cancelled=await call('queries/submit',JSON.stringify(cancelledPlan)),abandoned={tenant:fixture.tenant,query:cancelled.query};
  await call('queries/cancel',abandoned);
  assert.deepEqual(await call('queries/turn',abandoned),{complete:true});
  const cancelledResult=await mf.dispatchFetch('https://sql/internal/v1/sql/queries/result',{method:'POST',headers:headers(),body:JSON.stringify(abandoned)});
  assert.equal(cancelledResult.status,408);
  const expiry=String(cancelled.query.expires_ms).padStart(20,'0'),hash=typeof cancelled.query.id==='string'?cancelled.query.id:String.fromCharCode(...cancelled.query.id);
  const entries=await(await mf.getR2Bucket('SQL_EXECUTION')).list({prefix:`state/sql-execution-v2/${expiry}/${hash}/stage/`});
  assert.equal(entries.objects.length,0,'cancellation before the first turn must not dispatch tasks');
 }finally{await mf.dispose();await rm(persist,{recursive:true,force:true});}
});

test('workerd drains single-lane GC backlogs through alarms after restart and cron',{timeout:120000},async()=>{
 const state=fileURLToPath(new URL('../dist/sql-gc-workerd-state/',import.meta.url));
 await rm(state,{recursive:true,force:true});
 const wasm=(await readdir(output)).filter(name=>name.endsWith('.wasm'));
 const harness=`import worker,{SQLCoordinator} from './sql-worker.js';
 export {SQLCoordinator};
 export default {async fetch(request,env,ctx){
  if(new URL(request.url).pathname==='/collect')return Response.json(await env.SQL_COORDINATORS.getByName('collector').collect());
  await worker.scheduled({},env,ctx);return new Response('scheduled');
 }};`;
 const options={modulesRoot:output,modules:[{type:'ESModule',path:join(output,'sql-gc-harness.mjs'),contents:harness},{type:'ESModule',path:join(output,'sql-worker.js')},...wasm.map(name=>({type:'CompiledWasm',path:join(output,name)}))],compatibilityDate:'2026-10-19',bindings:{SQL_SERVICE_SECRET:secret,SQL_SERVICE_ISSUER:'sql:workerd',SQL_CONFIG:JSON.stringify(fixture.config),SQL_BUCKET_BINDINGS:JSON.stringify({execution:'SQL_EXECUTION',source:'SQL_SOURCE'})},resourcePersistencePath:join(state,'resources'),isolatedResourcePersistencePath:join(state,'isolated'),r2Buckets:['SQL_EXECUTION','SQL_SOURCE'],durableObjects:{SQL_COORDINATORS:{className:'SQLCoordinator',useSQLite:true}},queueProducers:{SQL_TASKS:'sql-tasks'},queueConsumers:{'sql-tasks':{maxBatchSize:1,maxBatchTimeout:0,maxRetries:10}}};
 let mf=new Miniflare(convertV4MiniflareOptions(options));
 const expiredState=`${fixture.config.state.prefix}/sql-execution-v2/00000000000000000001/`;
 const expiredExchange=`${fixture.config.exchange.prefix}/sql-exchange/00000000000000000001/`;
 const seed=async(bucket,prefix,count)=>{
  for(let i=0;i<count;i++)await bucket.put(prefix+i.toString(16).padStart(64,'0')+'/expired.json','{}');
 };
 const drain=async(bucket,prefix)=>{
  const deadline=Date.now()+60000;
  while(Date.now()<deadline){
   if((await bucket.list({prefix,limit:1})).objects.length===0)return;
   await new Promise(resolve=>setTimeout(resolve,50));
  }
  assert.fail('persisted collector alarms did not drain '+prefix);
 };
 try{
  let bucket=await mf.getR2Bucket('SQL_EXECUTION');
  const source=await mf.getR2Bucket('SQL_SOURCE');
  await source.put('artifacts/keep','source');
  await seed(bucket,expiredState,1000);
  const live=`${fixture.config.state.prefix}/sql-execution-v2/${String(Date.now()+3600000).padStart(20,'0')}/${'b'.repeat(64)}/query.json`;
  await bucket.put(live,'live');
  const first=await(await mf.dispatchFetch('https://sql/collect')).json();
  assert.equal(first.deleted,64);assert.equal(first.more,true);
  await mf.dispose();mf=new Miniflare(convertV4MiniflareOptions(options));
  bucket=await mf.getR2Bucket('SQL_EXECUTION');
  // No second kick: the persisted continuation must survive process restart.
  await drain(bucket,expiredState);
  assert.ok(await bucket.head(live));
  assert.ok(await(await mf.getR2Bucket('SQL_SOURCE')).head('artifacts/keep'));
  await seed(bucket,expiredExchange,129);
  assert.equal((await mf.dispatchFetch('https://sql/schedule')).status,200);
  await drain(bucket,expiredExchange);
  assert.ok(await bucket.head(live));
 }finally{await mf.dispose();await rm(state,{recursive:true,force:true});}
});
