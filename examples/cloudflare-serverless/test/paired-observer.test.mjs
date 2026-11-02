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

import{test}from'node:test';import assert from'node:assert/strict';
import{observerScopes,fetchObserver,persistObserverTail}from'../qualification/paired-observer.mjs';import{Bucket}from'./object-fixture.mjs';
const pair='antfly-1278-g-fixture-1',scopes=['b','c'].flatMap(side=>['','-writer','-reader','-builder'].map(role=>({source:pair+'-'+side+role,capture_id:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i})),token='local-disposable-fixture',version=crypto.randomUUID(),env=()=>({API_TOKEN:token,SOURCE_SCOPES:JSON.stringify(scopes),LOGS:new Bucket(),WORKER_VERSION:{id:version}}),req=(p,body)=>new Request('https://observer.invalid'+p,{method:'POST',headers:{Authorization:'Bearer '+token},body:JSON.stringify(body)});
const envelope=()=>({resourceSpans:scopes.map(s=>({resource:{attributes:[{key:'service.name',value:{stringValue:s.source}}]},scopeSpans:[{spans:[{traceId:'a'.repeat(32),spanId:'b'.repeat(16),startTimeUnixNano:'1000000000',endTimeUnixNano:'1010000000',attributes:[]}]}]}))});
test('paired observer isolates all eight exact producer prefixes and rejects foreign or incomplete scopes',async()=>{const e=env();assert.equal(observerScopes(e).length,8);assert.equal((await fetchObserver(req('/v1/traces',envelope()),e)).status,200);assert.equal(e.LOGS.objects.size,8);for(const s of scopes)assert.ok([...e.LOGS.objects.keys()].some(k=>k.startsWith('probe-v1/'+s.capture_id+'/')));const bad=envelope();bad.resourceSpans[7].resource.attributes[0].value.stringValue='production';assert.equal((await fetchObserver(req('/v1/traces',bad),e)).status,400);assert.equal([...e.LOGS.objects.keys()].filter(k=>k.startsWith('probe-v1/')).length,8);assert.throws(()=>observerScopes({...e,SOURCE_SCOPES:JSON.stringify(scopes.slice(1))}));assert.equal((await fetchObserver(new Request('https://observer.invalid/health'),e)).status,401);});
test('paired native tail populations retain own versions and late resource failures in separate immutable ledgers',async()=>{const e=env();await persistObserverTail(scopes.map(s=>({scriptName:s.source,scriptVersion:{id:version},eventTimestamp:1000,outcome:'ok',cpuTime:1,wallTime:20,logs:[{message:[JSON.stringify({event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM'})]}]})),e);assert.equal(e.LOGS.objects.size,8);for(const object of e.LOGS.objects.values()){const v=JSON.parse(new TextDecoder().decode(object.body));assert.equal(v.count,1);assert.equal(v.foreign_events,0);assert.ok(JSON.stringify(v).includes('SQLITE_NOMEM'));}await assert.rejects(persistObserverTail([{scriptName:'foreign'}],e));});
test('private source witnesses carry no credentials and retirement allows only bounded cleanup',async()=>{const e=env(),s=scopes[1];e[s.binding]={fetch:async(url,options)=>{assert.equal(options.headers,undefined);assert.equal(new URL(url).origin,'https://qualification-witness.invalid');return new Response('not found',{status:404});}};const r=await(await fetchObserver(req('/source/'+s.source+'/counter-witness',{id:crypto.randomUUID()}),e)).json();assert.equal(r.status,404);assert.equal(r.producer,s.source);assert.match(r.attempt_id_sha256,/^[a-f0-9]{64}$/);await e.LOGS.put('owned','{}');assert.equal((await fetchObserver(req('/drain-all',{}),e)).status,404);e.RETIRE_MODE='true';assert.equal((await fetchObserver(req('/source/'+s.source+'/counter-witness',{}),e)).status,410);assert.equal((await(await fetchObserver(req('/drain-all',{}),e)).json()).deleted,1);});

test('tail rejection markers preserve fixed resource causes without leaking exception text',async()=>{
 const worker=(await import('../qualification/paired-observer-worker.mjs')).default,e=env(),put=e.LOGS.put.bind(e.LOGS);let pending;
 e.LOGS.put=async(key,...args)=>{if(key.includes('/counter/'))throw Error('SQLITE_NOMEM secret-bucket');return put(key,...args);};
 worker.tail([{scriptName:scopes[0].source,eventTimestamp:1000,outcome:'ok',cpuTime:1,wallTime:2}],e,{waitUntil(p){pending=p;}});
 await assert.rejects(pending,/ObserverTailFailed/);
 const marker=JSON.parse(new TextDecoder().decode([...e.LOGS.objects.values()][0].body));assert.equal(marker.cause,'SQLITE_NOMEM');assert.ok(!JSON.stringify(marker).includes('secret-bucket'));
});

test('runtime-only cleanup preserves export obligations and full cleanup resumes without deployment access',async()=>{
 const {mkdir,writeFile,readFile,access,rm}=await import('node:fs/promises'),{resolve,join}=await import('node:path'),{createHash}=await import('node:crypto'),{spawnSync}=await import('node:child_process');
 const id='antfly-1278-observe-test'+crypto.randomUUID().replaceAll('-',''),dir=resolve('dist/qualification',id),path=join(dir,'run.json'),hook=join(dir,'fixture.mjs'),calls=join(dir,'calls.json'),bundle='export default {};',capture=crypto.randomUUID(),account='a'.repeat(32),destinations=Object.fromEntries(['logs','traces'].map(signal=>[signal,{name:id+'-'+signal,slug:'fixture-'+signal}]));
 await mkdir(join(dir,'bundle'),{recursive:true});
 const state={schema:1,id,dir,account,scopes,paths:[join(dir,'baseline.json'),join(dir,'candidate.json')],token_file:join(dir,'api-token'),evidence_layout:'window-v1',archive_bucket:id+'-archive',observation_capture_id:capture,retired:true,runtime_deleted:true,deleted:false,destinations,cleanup_steps:{logs_bucket:true,observer:true},bundle_hashes:{'worker.js':createHash('sha256').update(bundle).digest('hex')}};
 try{
  await writeFile(join(dir,'bundle/worker.js'),bundle);for(const p of state.paths)await writeFile(p,JSON.stringify({deleted:true}));
  await writeFile(join(dir,'observer.json'),JSON.stringify({name:id,account_id:account,r2_buckets:[{binding:'LOGS',bucket_name:id},{binding:'ARCHIVE',bucket_name:state.archive_bucket}],vars:{SOURCE_SCOPES:JSON.stringify(scopes),OBSERVATION_CAPTURE_ID:capture,WINDOWED_EVIDENCE:'true'}}));
  const rows=Object.entries(destinations).map(([signal,d])=>({...d,scripts:[],configuration:{logpushDataset:'opentelemetry-'+signal}}));
  await writeFile(hook,`import cp from'node:child_process';import{syncBuiltinESMExports}from'node:module';import{writeFileSync}from'node:fs';cp.spawn=()=>{throw Error('UnexpectedDeploymentCommand');};syncBuiltinESMExports();let rows=${JSON.stringify(rows)},calls=[];globalThis.fetch=async(url,init)=>{if(process.env.ALLOW_ACCOUNT_FIXTURE!=='yes')throw Error('UnexpectedAccountRequest');const u=new URL(url);if(u.origin!=='https://api.cloudflare.com'||!u.pathname.startsWith('/client/v4/accounts/${account}/workers/observability/destinations'))throw Error('UnexpectedDestination');calls.push({method:init.method,path:u.pathname});if(init.method==='DELETE'){const slug=u.pathname.split('/').at(-1);if(!rows.some(r=>r.slug===slug))throw Error('UnexpectedDelete');rows=rows.filter(r=>r.slug!==slug);}else if(init.method!=='GET')throw Error('UnexpectedMethod');writeFileSync(${JSON.stringify(calls)},JSON.stringify(calls));return Response.json({success:true,result:init.method==='GET'?rows:null});};`);
  const run=(action,allowed=false)=>spawnSync(process.execPath,['--import',hook,resolve('qualification/paired-observer-lifecycle.mjs'),action,path],{encoding:'utf8',env:{...process.env,ANTFLY_OBSERVABILITY_API_TOKEN:'test-only-observability-token',CLOUDFLARE_API_TOKEN:'test-only-no-deployment-token',ALLOW_ACCOUNT_FIXTURE:allowed?'yes':'no'}});
  state.retired=false;await writeFile(path,JSON.stringify(state));assert.notEqual(run('cleanup-runtime').status,0);state.retired=true;await writeFile(path,JSON.stringify(state));
  for(let i=0;i<2;i++){const r=run('cleanup-runtime');assert.equal(r.status,0,r.stderr);assert.equal(JSON.parse(r.stdout).pending_destinations.length,2);assert.equal(JSON.parse(await readFile(path)).deleted,false);await assert.rejects(access(calls));}
  const final=run('cleanup',true);assert.equal(final.status,0,final.stderr);assert.equal(JSON.parse(await readFile(path)).deleted,true);const requests=JSON.parse(await readFile(calls));assert.deepEqual(requests.map(r=>r.method),['GET','DELETE','GET','DELETE']);assert.deepEqual(requests.filter(r=>r.method==='DELETE').map(r=>r.path.split('/').at(-1)),['fixture-logs','fixture-traces']);
 }finally{await rm(dir,{recursive:true,force:true});}
});
