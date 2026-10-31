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
import{evaluateTrial,evaluateGate,evaluateWarmPrerequisites}from'../qualification/gate-policy.mjs';
import{GateClient,resumeSeedExecution}from'../qualification/gate-client.mjs';
test('WAL format activation probes the actual full writer and compact reader',async()=>{
 const{verifyWalTailReaders}=await import('../qualification/wal-compatibility.mjs'),{readFile}=await import('node:fs/promises');
 const modules=await Promise.all(['antfly-serverless.wasm','antfly-serverless-compact.wasm'].map(async name=>WebAssembly.compile(await readFile(new URL('../dist/'+name,import.meta.url)))));
 assert.deepEqual(await verifyWalTailReaders(...modules),{writer_format:3,reader_count:'1'});
 assert.deepEqual(await verifyWalTailReaders(modules[1],modules[1]),{writer_format:3,reader_count:'1'});
});
test('setup replay requires a completely acknowledged corpus and no offered workload',()=>{
 const baseline={id:'baseline',wasm_sha256:'a'.repeat(64),reader_wasm_sha256:'b'.repeat(64)},candidate={...baseline,id:'candidate'},execution_id=crypto.randomUUID();
 for(const run of[baseline,candidate]){run.last_execution_id=execution_id;run.last_phase='setup';}
 const side=run=>({execution_id,run:run.id,wasm_sha256:run.wasm_sha256,reader_wasm_sha256:run.reader_wasm_sha256,converged:false,cold:[],errors:[],resource_errors:[],samples:['a','b','c','d'].flatMap(tenant=>Array.from({length:8},()=>({phase:'seed',op:'mutate',tenant})))});
 const r={schema:1,documents:512,soak_ms:0,execution_id,fatal:'publication did not converge',baseline:side(baseline),candidate:side(candidate)};
 assert.equal(resumeSeedExecution(r,baseline,candidate),execution_id);
 assert.throws(()=>resumeSeedExecution(r,baseline,{...candidate,corpus:{id:crypto.randomUUID()}}),/corpus changed/);
 assert.throws(()=>resumeSeedExecution(r,baseline,{...candidate,last_phase:'workload'}));
 assert.throws(()=>resumeSeedExecution(r,baseline,{...candidate,last_execution_id:crypto.randomUUID()}));
 for(const change of[x=>x.candidate.samples.pop(),x=>x.candidate.samples.push({phase:'write'}),x=>x.baseline.cold.push({epoch:1}),x=>x.candidate.errors.push({phase:'seed'}),x=>x.candidate.run='foreign',x=>x.workload={},x=>{x.baseline.converged=x.candidate.converged=true;}]){const bad=structuredClone(r);change(bad);assert.throws(()=>resumeSeedExecution(bad,baseline,candidate));}
});
test('setup replay keeps mutation identities while measured execution remains fresh',async()=>{
 const client=new GateClient({},'disposable-fixture'),seedExecution=crypto.randomUUID(),measuredExecution=client.executionId,inputs=[];
 client.call=async(tenant,input)=>{if(input.op==='status')return{value:{published_lsn:'512'}};inputs.push({tenant,input});return{value:{visibility:'seed-'+BigInt(Number(input.operation_id.split('/').at(-1))+input.mutations.length).toString(16).padStart(16,'0')}};};
 await client.seed(512,{executionId:seedExecution});
 assert.equal(inputs.length,32);assert.equal(client.expected.size,2048);
 assert.ok(inputs.every(({input})=>input.operation_id.startsWith('gate/'+seedExecution+'/seed/')));
 assert.equal(client.executionId,measuredExecution);assert.notEqual(client.executionId,seedExecution);
});
function report(){
 const execution_id=crypto.randomUUID(),version='01234567-0123-4567-89ab-0123456789ab',samples=[...Array.from({length:1000},()=>({phase:'read',ms:100,version,attempts:1})),...Array.from({length:300},()=>({phase:'write',ms:500,version,attempts:1}))];
 const side={execution_id,version,samples,errors:[],resource_errors:[],verification:{bodies:2052,receipts:8,passed:true},converged:true,lag:["a","b","c","d"].flatMap(tenant=>Array.from({length:60},(_,minute)=>({minute,tenant,lag:0}))),queue:Array.from({length:60},(_,minute)=>({minute,backlog_count:0,oldest_ms:0,dead_letter_count:0})),cold:Array.from({length:100},(_,epoch)=>({epoch:epoch+1,ms:1000,version,count:'512',attempts:1}))};
 const point={samples:10,baseline_recall:.9,candidate_recall:.9,baseline_gets:100,candidate_gets:100,baseline_bytes:1000,candidate_bytes:1000},points=[8,32,"all"].map(probes=>({...point,probes}));
 return{schema:1,execution_id,diagnostics:{passed:true,streams:['baseline','candidate'].flatMap(run=>['app','writer','reader','builder'].map(role=>({run,role,connected:true,gap:false,resource_failures:0})))},trial:1,documents:512,soak_ms:3600000,offered_period_ms:30000,workload:{period_ms:30000,rounds:120,requested_ms:3600000,started_at:1,offered_end_at:3600001},baseline:{...structuredClone(side),run:'baseline'},candidate:{...side,run:'candidate'},faults:{passed:true,artifact_sha256:'a'.repeat(64)},ann:{datasets:[{kind:"synthetic",documents:10000,points},{kind:"model-embeddings",documents:1000,fixture_sha256:"b".repeat(64),points}],samples:10,exact_control:true,baseline_recall:.9,candidate_recall:.9,baseline_gets:100,candidate_gets:100,baseline_bytes:1000,candidate_bytes:1000}};
}
test('gate requires three independent complete trials',()=>{const a=report();assert.equal(evaluateTrial(a).passed,true);assert.equal(evaluateGate([a]).passed,false);const b=report(),c=report();b.trial=2;c.trial=3;assert.equal(evaluateGate([a,b,c]).passed,true);assert.equal(evaluateGate([a,a,a]).passed,false);});
test('fresh corpus retries preserve resource parameters and old recovery markers',async()=>{
 const{planCorpus}=await import('../qualification/corpus.mjs'),execution_id=crypto.randomUUID(),id=crypto.randomUUID(),routes=Object.fromEntries(['a','b','c','d'].map(t=>[t,{incarnation:'test-'+t,storage_id:'store-'+t}])),run={id:'candidate',paused:true,deployed:true,last_execution_id:execution_id,cold_execution_id:null,bundle_hashes:{app:'unchanged'},version:'unchanged'},configs=Array.from({length:4},()=>({vars:{ANTFLY_TENANTS:JSON.stringify(routes),ANTFLY_INCARNATION:'root',PUBLICATION_RECORDS:'64',WORKING_SET_BYTES:'20971520'},limits:{cpu_ms:300000}})),source={execution_id,fatal:'setup failed',candidate:{run:run.id,execution_id}},before=structuredClone({run,configs});
 const next=planCorpus(run,configs,source,id);assert.deepEqual({run,configs},before);assert.equal(next.run.cold_execution_id,null);assert.deepEqual(next.run.bundle_hashes,run.bundle_hashes);assert.equal(next.run.version,run.version);assert.equal(next.run.deployed,false);
 for(const c of next.configs){assert.deepEqual({...c.vars,ANTFLY_INCARNATION:'root',COORDINATOR_STORAGE_ID:undefined,ANTFLY_TENANTS:configs[0].vars.ANTFLY_TENANTS},{...configs[0].vars,COORDINATOR_STORAGE_ID:undefined});assert.equal(c.vars.ANTFLY_INCARNATION,'root-'+id);assert.equal(c.vars.COORDINATOR_STORAGE_ID,'root-'+id);assert.deepEqual(c.limits,configs[0].limits);for(const[t,r]of Object.entries(JSON.parse(c.vars.ANTFLY_TENANTS))){assert.equal(r.incarnation,routes[t].incarnation+'-'+id);assert.equal(r.storage_id,routes[t].storage_id+'-'+id);}}
 const second=planCorpus(next.run,next.configs,source,crypto.randomUUID());assert.equal(second.run.initial_corpus_routes.a.incarnation,'test-a');assert.ok(second.run.corpus.tenants.a.incarnation.length<100,'retries do not accumulate identity suffixes');
});
test('corpus preparation rejects active stale consumed and mismatched role histories',async()=>{
 const{planCorpus}=await import('../qualification/corpus.mjs'),execution_id=crypto.randomUUID(),run={id:'candidate',paused:true,last_execution_id:execution_id},routes=Object.fromEntries(['a','b','c','d'].map(t=>[t,{incarnation:t,storage_id:'s'+t}])),configs=Array.from({length:4},()=>({vars:{ANTFLY_INCARNATION:'root',ANTFLY_TENANTS:JSON.stringify(routes)}})),r={execution_id,fatal:'failed',candidate:{run:run.id,execution_id}},id=crypto.randomUUID();
 for(const bad of[{...run,paused:false},{...run,cold_execution_id:'used'},{...run,last_execution_id:crypto.randomUUID()},{...run,corpus:{id,tenants:{}}}])assert.throws(()=>planCorpus(bad,configs,r,id));
 assert.throws(()=>planCorpus(run,configs,{...r,fatal:undefined},id));const changed=structuredClone(configs);changed[1].vars.ANTFLY_TENANTS='{}';assert.throws(()=>planCorpus(run,changed,r,id));assert.throws(()=>planCorpus(run,configs,r,'not-an-id'));
 const diagnostic={execution_id,stage:'complete',finished_at:Date.now(),sides:[r.candidate]};assert.equal(planCorpus(run,configs,diagnostic,id).run.corpus.id,id);assert.throws(()=>planCorpus(run,configs,{...diagnostic,stage:'workload'},id));
 const short={execution_id,soak_ms:300000,workload:{finished_at:Date.now()},candidate:{...r.candidate,verification:{passed:true}},baseline:{verification:{passed:true}}};assert.equal(planCorpus(run,configs,short,id).run.corpus.id,id);assert.throws(()=>planCorpus(run,configs,{...short,soak_ms:3600000},id));assert.throws(()=>planCorpus(run,configs,{...short,baseline:{}},id));
});
test('paired corpus provenance must match and remain distinct between trials',()=>{
 const reports=Array.from({length:3},(_,i)=>{const r=report();r.trial=i+1;r.baseline.corpus={id:crypto.randomUUID()};r.candidate.corpus=structuredClone(r.baseline.corpus);return r;});assert.equal(evaluateGate(reports).passed,true);
 reports[1].candidate.corpus.id=reports[0].candidate.corpus.id;assert.equal(evaluateTrial(reports[1]).passed,false);reports[1].baseline.corpus.id=reports[0].candidate.corpus.id;assert.equal(evaluateGate(reports).passed,false);
});
test('the paired control must also satisfy publication and Queue evidence',()=>{
 for(const mutate of[r=>r.baseline.queue[0].dead_letter_count=1,r=>r.baseline.queue.pop(),r=>r.baseline.queue[0].oldest_ms=900001,r=>r.baseline.lag[0].lag=4097,r=>r.baseline.converged=false,r=>r.baseline.queue.slice(-10).forEach(s=>s.backlog_count=20)]){const r=report();mutate(r);assert.equal(evaluateTrial(r).passed,false);}
});
for(const[name,change]of[
 ['changed arrival cadence',r=>r.offered_period_ms=60000],['invented soak duration',r=>r.workload.offered_end_at=60000],['non-finite latency',r=>r.candidate.samples[0].ms=NaN],['missing attempt count',r=>delete r.candidate.samples[0].attempts],['changed artifacts',r=>r.candidate.provenance_errors=['wrong bundle']],['short soak',r=>r.soak_ms=60000],['missing cold evidence',r=>r.candidate.cold=[]],['duplicate cold epochs',r=>r.candidate.cold.forEach(c=>c.epoch=1)],['mixed deployment',r=>r.candidate.samples[0].version='old'],['retry-inclusive tail',r=>r.candidate.samples.filter(s=>s.phase==='read').slice(0,60).forEach(s=>s.ms=2000)],['recall regression',r=>r.ann.candidate_recall=.8],['extra ANN bytes',r=>r.ann.candidate_bytes=1001],['missing fault evidence',r=>r.faults=undefined],['resource failure',r=>r.candidate.resource_errors.push('OutOfMemory')],['growing backlog',r=>r.candidate.queue.slice(-10).forEach(s=>s.backlog_count=20)],['missing minute coverage',r=>r.candidate.lag.forEach(s=>s.minute=0)]
])test('gate rejects '+name,()=>{const r=report();change(r);assert.equal(evaluateTrial(r).passed,false);});

test('qualification endpoints deny missing credentials including Bearer undefined',async()=>{
 const{qualificationApp}=await import('../qualification/physical-epochs.mjs');const app=qualificationApp({fetch(){throw Error('unauthenticated request reached application');}});
 for(const path of['/_qualification/drain','/_qualification/queue','/_qualification/version']){
  const response=await app.fetch(new Request('https://test.invalid'+path,{method:'POST',headers:{Authorization:'Bearer undefined'}}),{ANTFLY_INCARNATION:'qualification-test'});assert.equal(response.status,401);
 }
});

test('gate lifecycle scope rejects mismatched credentials and cloud destinations',async()=>{
 const{assertGateRun,assertGateConfig}=await import('../qualification/gate-scope.mjs');const id='antfly-1278-g-owned-1-c',run={id,account:'a'.repeat(32),bucket:id,worker:id,queues:[id,id+'-dead'],dir:'/tmp/gate/owned',token_file:'/tmp/gate/owned/api-token',services:['app','writer','reader','builder'].map(role=>({role,name:id+(role==='app'?'':'-'+role)}))};
 assert.doesNotThrow(()=>assertGateRun(run,{account:run.account,dir:'/tmp/gate'}));
 for(const change of[r=>r.token_file='/tmp/account-token',r=>r.bucket='production',r=>r.services[0].name='production']){const bad=structuredClone(run);change(bad);assert.throws(()=>assertGateRun(bad),/ScopeMismatch/);}
 const service=run.services[0],config={name:service.name,account_id:run.account,r2_buckets:[{bucket_name:id}]};assert.doesNotThrow(()=>assertGateConfig(run,service,config));assert.throws(()=>assertGateConfig(run,service,{...config,account_id:'b'.repeat(32)}),/ScopeMismatch/);
});


test('overlapping arrivals retain durable commit order when HTTP acknowledgements reverse',async()=>{
 const{GateClient}=await import('../qualification/gate-client.mjs');
 const client=new GateClient({},'disposable-fixture');let release;
 const delayed=new Promise(resolve=>release=resolve),visibility=lsn=>'fixture'+lsn.toString(16).padStart(16,'0');
 for(const tenant of ['a','b','c','d'])client.receipts[tenant]=visibility(0);
 client.call=async(tenant,input,phase)=>{
  if(phase==='write'){const older=input.operation_id.endsWith('/0');if(older)await delayed;return{value:{committed:true,visibility:visibility(older?1:2)}};}
  return{value:phase==='read'?{body:client.body(tenant,1)}:{}};
 };
 const older=client.round(0,512,Date.now());await client.round(512,512,Date.now());release();await older;
 for(const tenant of ['a','b','c','d']){assert.equal(client.receipts[tenant],visibility(2));assert.equal(JSON.parse(client.expected.get(tenant+'/0')).text,'round '+client.executionId+' 512');assert.match(client.latest[tenant].input.operation_id,/512$/);}
 client.recordCommit('a',1,'different-document',{}, {committed:true,visibility:visibility(1)});
 assert.equal(client.expected.get('a/1'),'different-document');assert.equal(client.receipts.a,visibility(2));
});

test('fixed-arrival driver bounds outstanding rounds and records overload',async()=>{
 const{runOfferedWorkload}=await import('../qualification/offered-workload.mjs');let active=0,peak=0;
 const side={errors:[],async round(){peak=Math.max(peak,++active);await new Promise(r=>setTimeout(r,15));active--;},async progress(){}};
 const work=await runOfferedWorkload({sides:[side],seconds:1,period:2,maxPendingRounds:1});
 assert.equal(peak,1);assert.equal(active,0);assert.ok(side.errors.some(e=>e.phase==='client-admission'));assert.ok(work.offered_end_at-work.started_at>=1000);assert.ok(work.rounds>10);
});

test('fixed-arrival driver drains in-flight work before reporting a monitor failure',async()=>{
 const{runOfferedWorkload}=await import('../qualification/offered-workload.mjs');let drained=false;
 const side={errors:[],async round(){await new Promise(r=>setTimeout(r,25));drained=true;},async progress(){}};
 await assert.rejects(runOfferedWorkload({sides:[side],seconds:1,onProgress(){throw Error('report disk unavailable');}}),/disk unavailable/);assert.equal(drained,true);
});

test('capacity summaries distinguish scheduled delay, execution and retries',async()=>{
 const{summarizeWorkload}=await import('../qualification/workload-summary.mjs');
 const result=summarizeWorkload({samples:[{tenant:'a',phase:'read',ms:1000,execution_ms:600,scheduling_delay_ms:400,attempts:2}],errors:[],resource_errors:[]});
 assert.equal(result.phases.read.p95_ms,1000);assert.equal(result.phases.read.execution_p95_ms,600);assert.equal(result.phases.read.scheduling_delay_p95_ms,400);assert.equal(result.phases.read.retried,1);assert.equal(result.tenants.a.read.samples,1);
});

test('sealed artifact verification detects file replacement and module/report mismatches',async()=>{
 const{mkdtemp,mkdir,writeFile,readFile,rm}=await import('node:fs/promises'),{tmpdir}=await import('node:os'),{resolve}=await import('node:path'),{createHash}=await import('node:crypto');
 const{verifySealedRun,verifyReportedRun}=await import('../qualification/gate-provenance.mjs');
 const dir=await mkdtemp(resolve(tmpdir(),'antfly-gate-seal-'));
 try{
  const native=new Uint8Array([0,97,115,109,1,0,0,0]),compact=new Uint8Array([...native,0,1,0]);
  const run={dir,id:'owned',version:'version',wasm_sha256:createHash('sha256').update(native).digest('hex'),reader_wasm_sha256:createHash('sha256').update(compact).digest('hex'),bundle_hashes:{},services:[]},bytes='export default {};',hash=createHash('sha256').update(bytes).digest('hex');
  for(const role of ['app','writer','reader','builder']){const bundle=resolve(dir,'bundle-'+role),config=resolve(dir,role+'.json');await mkdir(bundle);await writeFile(resolve(bundle,'index.js'),bytes);await writeFile(config,JSON.stringify({main:resolve(bundle,'index.js')}));run.services.push({role,config});await writeFile(resolve(bundle,'test-antfly-serverless.wasm'),native);await writeFile(resolve(bundle,'test-antfly-serverless-compact.wasm'),compact);run.bundle_hashes[role]={'index.js':hash,'test-antfly-serverless.wasm':run.wasm_sha256,'test-antfly-serverless-compact.wasm':run.reader_wasm_sha256};}
  assert.deepEqual(await verifySealedRun(run),[]);
  run.corpus={id:crypto.randomUUID(),tenants:{a:{incarnation:'table',storage_id:'store'}}};assert.match((await verifySealedRun(run)).join(','),/corpus identity changed/);assert.ok(verifyReportedRun({},run).includes('corpus identity changed'));delete run.corpus;
  const configPath=run.services[0].config,originalConfig=JSON.parse(await readFile(configPath,'utf8'));
  await writeFile(configPath,JSON.stringify({...originalConfig,vars:{RUNTIME_GENERATION:'one',R2_LOCATION_HINT:'wnam'}}));assert.match((await verifySealedRun(run)).join(','),/runtime placement policy changed/);await writeFile(configPath,JSON.stringify(originalConfig));
  assert.ok(verifyReportedRun({runtime_placement:{generation:'one',location_hint:'wnam'}},run).includes('runtime placement profile changed'));
  await writeFile(configPath,JSON.stringify({...originalConfig,vars:{EDGE_IMMUTABLE_CACHE:'true',LOCAL_METADATA_READS:'true'}}));assert.match((await verifySealedRun(run)).join(','),/runtime placement policy changed/);await writeFile(configPath,JSON.stringify(originalConfig));
  const{runtimePlacement}=await import('../qualification/gate-provenance.mjs');assert.deepEqual(runtimePlacement({vars:{EDGE_IMMUTABLE_CACHE:'true',LOCAL_METADATA_READS:'false'}}),{generation:null,location_hint:null,immutable_cache:true,immutable_cache_scope:null,local_metadata_reads:false});
  assert.deepEqual(runtimePlacement({vars:{INLINE_METADATA_ROOTS:'false'}}),{generation:null,location_hint:null,inline_metadata_roots:false});await writeFile(configPath,JSON.stringify({...originalConfig,vars:{INLINE_METADATA_ROOTS:'true'}}));assert.match((await verifySealedRun(run)).join(','),/runtime placement policy changed/);await writeFile(configPath,JSON.stringify(originalConfig));
  assert.deepEqual(runtimePlacement({vars:{DIRECT_READS:'true'}}),{generation:null,location_hint:null,direct_reads:true});assert.deepEqual(runtimePlacement({vars:{LOCAL_METADATA_READS:'true'}}),{generation:null,location_hint:null,local_metadata_reads:true});assert.equal(runtimePlacement({vars:{READ_VIEW_BATCH_MS:'10'}}).read_view_batch_ms,10);await writeFile(configPath,JSON.stringify({...originalConfig,vars:{DIRECT_READS:'true'}}));assert.match((await verifySealedRun(run)).join(','),/runtime placement policy changed/);await writeFile(configPath,JSON.stringify(originalConfig));
  const expectedNative=run.wasm_sha256;run.wasm_sha256='f'.repeat(64);assert.match((await verifySealedRun(run)).join(','),/declared native module missing/);run.wasm_sha256=expectedNative;
  const side={run:run.id,version:run.version,wasm_sha256:run.wasm_sha256,reader_wasm_sha256:run.reader_wasm_sha256,bundle_hashes:structuredClone(run.bundle_hashes)};
  assert.deepEqual(verifyReportedRun(side,run),[]);side.reader_wasm_sha256='c'.repeat(64);assert.ok(verifyReportedRun(side,run).length);side.reader_wasm_sha256=run.reader_wasm_sha256;side.bundle_hashes.reader['index.js']='d'.repeat(64);assert.ok(verifyReportedRun(side,run).length);
  await writeFile(resolve(dir,'bundle-reader/index.js'),'modified');assert.match((await verifySealedRun(run)).join(','),/reader: artifact hash changed/);
 }finally{await rm(dir,{recursive:true,force:true});}
});


test('ANN evidence rejects partial corpora, failed update stages and non-finite traffic',async()=>{
 const{validateAnnReport}=await import('../qualification/ann-evidence.mjs');
 const stages=['ingest-and-publication','accepted-visibility','cold-query','cold-vector-query','vector-recall','cold-sparse-query','warm-query','cold-point-updates','cold-delete','compaction'].map(name=>({name,ok:true}));
 const report={cases:[{documents:1000,accepted_documents:1000,published_lsn:'1000',stages,vector_recall:[8,32,'all'].flatMap(num_probes=>Array.from({length:10},(_,query)=>({num_probes,query,recall_at_10:1,r2_gets:10,r2_bytes:100})))}]};
 assert.doesNotThrow(()=>validateAnnReport(report));
 for(const mutate of[r=>r.cases[0].accepted_documents=999,r=>r.cases[0].published_lsn='999',r=>r.cases[0].stages.at(-2).ok=false,r=>r.cases[0].stages.pop(),r=>r.cases[0].vector_recall[0].r2_bytes=NaN]){const copy=structuredClone(report);mutate(copy);assert.throws(()=>validateAnnReport(copy));}
});


test('account model fixture uses bounded official API requests without redirects or token logging',async()=>{
 const{providerEmbeddings}=await import('../qualification/provider-embeddings.mjs'),account='a'.repeat(32),token='test-only-placeholder';let called=0;
 const result=await providerEmbeddings(account,['generated text'],token,{fetcher:async(url,request)=>{called++;assert.equal(url,'https://api.cloudflare.com/client/v4/accounts/'+account+'/ai/run/@cf/baai/bge-small-en-v1.5');assert.equal(request.redirect,'error');assert.equal(request.headers.Authorization,'Bearer '+token);assert.equal(request.headers['cf-aig-gateway-id'],'default');assert.deepEqual(JSON.parse(request.body),{text:['generated text']});return Response.json({success:true,result:{data:[[1,2]]}});}});
 assert.equal(called,1);assert.deepEqual(result,{data:[[1,2]]});
 await assert.rejects(providerEmbeddings(account,['text'],token,{fetcher:async()=>new Response(token,{status:403})}),error=>/HTTP 403/.test(error.message)&&!error.message.includes(token));
 await assert.rejects(providerEmbeddings('invalid',['text'],token),/invalid bounded/);
});


test('recovery actors cannot be reused as fresh evidence or resumed into fenced old owners',async()=>{
 const{assertUnusedRecovery}=await import('../qualification/gate-provenance.mjs');
 assert.doesNotThrow(()=>assertUnusedRecovery({}));assert.throws(()=>assertUnusedRecovery({cold_execution_id:crypto.randomUUID()}),/fresh deployment pair/);
});

test('ANN pairs reject different workloads and query populations',async()=>{
 const{validateAnnPair}=await import('../qualification/ann-evidence.mjs');
 const stages=['ingest-and-publication','accepted-visibility','cold-query','cold-vector-query','vector-recall','cold-sparse-query','warm-query','cold-point-updates','cold-delete','compaction'].map(name=>({name,ok:true}));
 const baseline={dimensions:384,body_bytes:1024,publication_records:64,cases:[{documents:1000,accepted_documents:1000,published_lsn:'1000',stages,vector_recall:[8,32,'all'].flatMap(num_probes=>Array.from({length:10},(_,query)=>({num_probes,query,recall_at_10:1,r2_gets:10,r2_bytes:100})))}]};
 assert.doesNotThrow(()=>validateAnnPair(baseline,structuredClone(baseline)));
 for(const key of ['dimensions','body_bytes','publication_records','max_put_bytes','recycle_at_bytes']){const changed=structuredClone(baseline);changed[key]=1;assert.throws(()=>validateAnnPair(baseline,changed),/ANN workload mismatch/);}
 const changed=structuredClone(baseline);changed.cases[0].vector_recall[0].query=20;assert.throws(()=>validateAnnPair(baseline,changed),/ANN workload mismatch: queries/);
 for(const key of['coarse_probes','search_effort']){const changed=structuredClone(baseline);changed.cases[0].vector_recall[0][key]=1;assert.throws(()=>validateAnnPair(baseline,changed),/ANN workload mismatch/);}
});


test('qualification rejects cancelled or incomplete Node test runs even when fail is zero',async()=>{
 const{passingNodeTestLog}=await import('../qualification/gate-policy.mjs');
 const complete='ℹ tests 6\nℹ pass 6\nℹ fail 0\nℹ cancelled 0\nℹ skipped 0\n';
 assert.equal(passingNodeTestLog(complete,6),true);
 for(const log of[complete.replace('pass 6','pass 5').replace('cancelled 0','cancelled 1'),complete.replace('fail 0','fail 1'),complete.replace('skipped 0','skipped 1'),complete.replace('tests 6','tests 7'),complete.replace('cancelled 0','')])assert.equal(passingNodeTestLog(log,6),false);
 assert.equal(passingNodeTestLog(complete,7),false);
});


test('cold preflight preserves unused actors on every warm failure and cannot become a final gate pass',async()=>{
 const {evaluateWarmPrerequisites}=await import('../qualification/gate-policy.mjs'),r=report();r.candidate.cold=[];
 assert.equal(evaluateWarmPrerequisites(r).passed,true);assert.equal(evaluateTrial(r).passed,false);
 for(const change of[x=>x.candidate.errors.push('internal error'),x=>x.candidate.resource_errors.push('memory'),x=>x.soak_ms=60000,x=>x.candidate.verification.passed=false,x=>x.candidate.queue[0].dead_letter_count=1,x=>delete x.ann,x=>delete x.faults,x=>x.candidate.samples.filter(s=>s.phase==='read').forEach(s=>s.ms=1001)]){const bad=structuredClone(r);change(bad);assert.equal(evaluateWarmPrerequisites(bad).passed,false);}
});
test('local gate evidence fails closed on missing changed or foreign artifacts before cold admission',async()=>{
 const{validatedEvidence}=await import('../qualification/validated-evidence.mjs'),{mkdtemp,writeFile,rm}=await import('node:fs/promises'),{tmpdir}=await import('node:os'),{join}=await import('node:path'),{createHash}=await import('node:crypto'),dir=await mkdtemp(join(tmpdir(),'antfly-evidence-'));
 try{
  const artifacts=[];for(let i=0;i<3;i++){const path=join(dir,String(i)),body='completed proof '+i;await writeFile(path,body);artifacts.push({path,sha256:createHash('sha256').update(body).digest('hex')});}
  const baseline={wasm_sha256:'a'.repeat(64)},candidate={wasm_sha256:'b'.repeat(64)},r={baseline,candidate},evidence={baseline_wasm_sha256:baseline.wasm_sha256,candidate_wasm_sha256:candidate.wasm_sha256,artifacts,faults:{passed:true,artifact_sha256:createHash('sha256').update(JSON.stringify(artifacts)).digest('hex')},ann:{exact_control:true}};
  assert.deepEqual(await validatedEvidence(evidence,r,baseline,candidate),{faults:evidence.faults,ann:evidence.ann});
  assert.deepEqual(await validatedEvidence({...evidence,artifacts:[]},r,baseline,candidate),{});
  assert.deepEqual(await validatedEvidence(evidence,r,baseline,{wasm_sha256:'c'.repeat(64)}),{});
  assert.deepEqual(await validatedEvidence({...evidence,faults:{passed:true,artifact_sha256:'c'.repeat(64)}},r,baseline,candidate),{});
  await writeFile(artifacts[0].path,'changed');assert.deepEqual(await validatedEvidence(evidence,r,baseline,candidate),{});
 }finally{await rm(dir,{recursive:true,force:true});}
});


test('paired preparation bounds unpublished data without changing the corpus or seed identities',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab',accepted={},published={},active=new Set(),identities=new Set();let maximum=0;
 const client=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async(url,request)=>{const tenant=request.headers['X-Antfly-Tenant'],input=JSON.parse(request.body);if(input.op==='mutate'){assert.ok(active.size===0||active.has(tenant),'a preparation lane must drain before admitting another tenant');const lag=(accepted[tenant]??0)-(published[tenant]??0)+input.mutations.length;assert.ok(lag<=64,'a slow publisher must not receive an unbounded WAL tail');maximum=Math.max(maximum,lag);active.add(tenant);accepted[tenant]=(accepted[tenant]??0)+input.mutations.length;identities.add(tenant+'/'+input.operation_id);return Response.json({committed:true,visibility:'receipt-'+accepted[tenant].toString(16).padStart(16,'0')},{headers:{'X-Antfly-Version':version}});}assert.equal(input.op,'status');published[tenant]=accepted[tenant];active.delete(tenant);return Response.json({published_lsn:String(published[tenant])},{headers:{'X-Antfly-Version':version}});}});
 client.executionId=crypto.randomUUID();await client.seed(512);assert.equal(maximum,64);assert.equal(client.expected.size,2048);assert.equal(identities.size,32);assert.ok(Object.values(published).every(n=>n===512));assert.equal(client.converged,true);assert.equal(client.errors.length,0);assert.equal(client.report().setup_policy.deadline_ms,1800000);
});

test('warm transport retries retain original input and include all retry and scheduling time',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab',bodies=[],client=new GateClient({origin:'https://fixture.invalid',version},'disposable-fixture',{fetch:async(url,request)=>{bodies.push(request.body);if(bodies.length===1)throw new DOMException('network timed out','TimeoutError');return Response.json({count:'512'},{headers:{'X-Antfly-Version':version}});}});
 const {sample}=await client.call('a',{op:'count'},'read',{scheduled:Date.now()-40});assert.equal(sample.attempts,2);assert.ok(sample.ms>=280);assert.ok(sample.execution_ms>=240);assert.deepEqual(bodies,[bodies[0],bodies[0]]);assert.equal(client.errors.length,0);
});
test('transport replay requires a stable mutation identity and preserves its receipt',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab',bodies=[],client=new GateClient({origin:'https://fixture.invalid',version},'disposable-fixture',{fetch:async(url,request)=>{bodies.push(request.body);if(bodies.length===1)throw new TypeError('fetch failed');return Response.json({committed:true,visibility:'receipt-0000000000000001'},{headers:{'X-Antfly-Version':version}});}}),input={op:'mutate',operation_id:'fixed-receipt',mutations:[]};
 const result=await client.call('a',input,'write');assert.equal(result.sample.attempts,2);assert.equal(result.value.visibility,'receipt-0000000000000001');assert.deepEqual(bodies,[JSON.stringify(input),JSON.stringify(input)]);
 let calls=0;const unsafe=new GateClient({},'fixture',{fetch:async()=>{calls++;throw new TypeError('fetch failed',{cause:{code:'UND_ERR_SOCKET',headers:{Authorization:'must-not-retain'}}});}});unsafe.run.origin='https://fixture.invalid';await assert.rejects(unsafe.call('a',{op:'mutate'},'write'),/fetch failed/);assert.equal(calls,1);assert.equal(unsafe.errors[0].cause_code,'UND_ERR_SOCKET');assert.ok(!JSON.stringify(unsafe.errors).includes('must-not-retain'));await assert.rejects(unsafe.call('a',{op:'compact'},'compact'),/fetch failed/);assert.equal(calls,2);
});
test('cold attempts never retry transport or backpressure and warm recovery retains resource failures',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab';
 for(const failure of ['network','backpressure']){let calls=0;const cold=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async()=>{calls++;if(failure==='network')throw new DOMException('timeout','TimeoutError');return Response.json({error:'CoordinatorRecovering'},{status:503});}});await assert.rejects(cold.call('a',{op:'count'},'cold',{epoch:1}));assert.equal(calls,1);assert.equal(cold.errors[0].attempts,1);}
 let calls=0;const warm=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async()=>++calls===1?Response.json({error:'SQLITE_NOMEM'},{status:503}):Response.json({count:'512'},{headers:{'X-Antfly-Version':version}})});assert.equal((await warm.call('a',{op:'count'},'read')).sample.attempts,2);assert.equal(warm.resourceErrors.length,1);
});

test('constructor reset replay preserves stable warm identities and excludes cold or unsafe requests',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab',error='A call to blockConcurrencyWhile() in a Durable Object waited for too long. The call was canceled and the Durable Object was reset.';
 for(const input of [{op:'count'},{op:'mutate',operation_id:'same-committed-receipt',mutations:[]}]){
  const bodies=[],client=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async(url,request)=>{bodies.push(request.body);return bodies.length===1?Response.json({error},{status:409}):Response.json({committed:true,visibility:'receipt-0000000000000001'},{headers:{'X-Antfly-Version':version}});}});
  const {sample,value}=await client.call('a',input,'read',{scheduled:Date.now()-40});assert.equal(sample.attempts,2);assert.ok(sample.ms>=280);assert.deepEqual(bodies,[JSON.stringify(input),JSON.stringify(input)]);assert.equal(value.visibility,'receipt-0000000000000001');assert.equal(client.errors.length,0);assert.equal(client.report().transport_policy.constructor_reset_replay,'idempotent-warm-only');
 }
 for(const [input,phase,message]of [[{op:'count'},'cold',error],[{op:'mutate'},'write',error],[{op:'compact'},'compact',error],[{op:'count'},'read','unknown constructor failure'],[{op:'count'},'read',error+' OutOfMemory']]){
  let calls=0;const client=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async()=>{calls++;return Response.json({error:message},{status:409});}});await assert.rejects(client.call('a',input,phase),/HTTP 409/);assert.equal(calls,1);assert.equal(client.errors.length,1);assert.equal(client.resourceErrors.length,message.includes('OutOfMemory')?1:0);
 }
});


test('Queue metrics overload uses bounded logical retries while unknown failures remain fatal',async()=>{
 const{qualificationQueueMetrics}=await import('../qualification/physical-epochs.mjs');let calls=0;
 const env={WORKER_VERSION:{id:'v'},MAINTENANCE:{async metrics(){if(++calls===1)throw Error('Queue is overloaded. Please back off. (10250)');return{backlogCount:2,backlogBytes:64};}},DEAD_LETTERS:{async metrics(){return{backlogCount:0};}}};
 const client=new GateClient({id:'test',origin:'https://metrics.invalid',version:'v'},'test',{fetch:()=>qualificationQueueMetrics(env)});
 const{value,sample}=await client.call('a',{},'queue',{path:'/_qualification/queue'});assert.equal(sample.attempts,2);assert.equal(client.errors.length,0);assert.deepEqual(value,{dead_letter_count:0,backlog_count:2,backlog_bytes:64,oldest_ms:0});
 env.MAINTENANCE.metrics=async()=>{throw Error('OutOfMemory');};await assert.rejects(qualificationQueueMetrics(env),/OutOfMemory/);
 env.MAINTENANCE.metrics=async()=>{throw Error('unrecognized platform failure');};await assert.rejects(qualificationQueueMetrics(env),/unrecognized platform failure/);
});


test('paired phase cancels and settles peer I/O before returning the original failure',async()=>{
 const{runPairedPhase}=await import('../qualification/paired-phase.mjs');let release,settled=false;const held=new Promise((_,reject)=>release=reject),failure=Error('original failure');
 const a={stop(){}},b={stop(reason){release(reason);}};await assert.rejects(runPairedPhase([a,b],async side=>{if(side===a)throw failure;try{await held;}finally{settled=true;}}),error=>error===failure);assert.equal(settled,true);
});
test('stopped qualification clients cancel active transport and never start another request',async()=>{
 let started,calls=0;const entered=new Promise(r=>started=r),client=new GateClient({id:'test',origin:'https://stopped.invalid',version:'v'},'do-not-log',{fetch:(_url,options)=>{calls++;started();return new Promise((_,reject)=>options.signal.addEventListener('abort',()=>reject(options.signal.reason),{once:true}));}});
 const pending=client.call('a',{op:'status'},'seed');await entered;client.stop(Error('PeerPhaseCancelled'));await assert.rejects(pending,/PeerPhaseCancelled/);await assert.rejects(client.call('a',{op:'status'},'seed'),/PeerPhaseCancelled/);assert.equal(calls,1);assert.equal(client.samples.length,0);assert.ok(!JSON.stringify(client.errors).includes('do-not-log'));
 let retries=0,backoff;backoff=new GateClient({id:'test',origin:'https://stopped.invalid',version:'v'},'test',{fetch:async()=>{retries++;queueMicrotask(()=>backoff.stop(Error('PeerPhaseCancelled')));return Response.json({error:'CoordinatorRecovering'},{status:503});}});
 await assert.rejects(backoff.call('a',{op:'status'},'seed'),/PeerPhaseCancelled/);assert.equal(retries,1);assert.equal(backoff.errors[0].attempts,1);
});

test('gate rejects missing gapped duplicate or resource-failed live diagnostics',()=>{
 for(const change of[r=>delete r.diagnostics,r=>r.diagnostics.streams.pop(),r=>r.diagnostics.streams[0].gap=true,r=>r.diagnostics.streams[0].resource_failures=1,r=>r.diagnostics.streams[0]=r.diagnostics.streams[1]]){const r=report();change(r);assert.equal(evaluateWarmPrerequisites(r).passed,false);assert.equal(evaluateTrial(r).passed,false);}
});

test('expired read admission retries only stable warm identities within the original budget',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab';
 for(const input of[{op:'lookup',doc_id:'same-document',required_visibility:'same-fence'},{op:'mutate',operation_id:'same-operation',mutations:[]}]){
  const bodies=[],client=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async(url,request)=>{bodies.push(request.body);return bodies.length===1?Response.json({error:'ManifestReadLeaseExpired'},{status:409}):Response.json({ok:true},{headers:{'X-Antfly-Version':version}});}});
  const{sample}=await client.call('a',input,'verify',{scheduled:Date.now()-40});assert.equal(sample.attempts,2);assert.ok(sample.ms>=280);assert.deepEqual(bodies,[JSON.stringify(input),JSON.stringify(input)]);assert.equal(client.errors.length,0);
 }
 for(const[input,phase,error]of[[{op:'lookup'},'cold','ManifestReadLeaseExpired'],[{op:'mutate'},'write','ManifestReadLeaseExpired'],[{op:'compact'},'compact','ManifestReadLeaseExpired'],[{op:'count'},'read','ManifestReadLeaseExpired OutOfMemory']]){
  let calls=0;const client=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async()=>{calls++;return Response.json({error},{status:409});}});await assert.rejects(client.call('a',input,phase),/HTTP 409/);assert.equal(calls,1);assert.equal(client.resourceErrors.length,error.includes('OutOfMemory')?1:0);
 }
});

test('report persistence does not block fixed arrivals and is joined before completion',async()=>{
 const{runOfferedWorkload}=await import('../qualification/offered-workload.mjs');let release,entered,next,completed=false,calls=0;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r),second=new Promise(r=>next=r),side={errors:[],round:async()=>{if(++calls===2)next();},progress:async()=>{}};
 const work=runOfferedWorkload({sides:[side],seconds:1,period:100,onProgress:async()=>{entered();await held;completed=true;}});await started;
 try{assert.equal(await Promise.race([second.then(()=>true),new Promise(r=>setTimeout(()=>r(false),500))]),true,'a held journal must not block the next offered round');assert.equal(completed,false);}finally{release();await work;}
 assert.equal(completed,true);assert.equal(calls,10);assert.deepEqual(side.errors,[]);
});
test('cancelled offered workloads stop before another arrival and join outstanding work',async()=>{
 const{runOfferedWorkload}=await import('../qualification/offered-workload.mjs'),abortController=new AbortController();let calls=0,settled=false;const side={abortController,errors:[],round:async()=>{calls++;await new Promise(r=>setTimeout(r,20));settled=true;},progress:async()=>{}};
 await assert.rejects(runOfferedWorkload({sides:[side],seconds:60,period:100,onProgress:()=>abortController.abort(Error('QualificationInterrupted'))}),/QualificationInterrupted/);assert.equal(calls,1);assert.equal(settled,true);
});


test("cleanup-only draining rejects unauthorized and non-disposable calls before storage access",async()=>{
 const drain=(await import("../qualification/drain-worker.mjs")).default;
 const env={API_TOKEN:"disposable",ANTFLY_INCARNATION:"qualification-drain",COORDINATOR_BACKEND:"r2",DATABASE:{list(){throw Error("UnexpectedStorageAccess");}}};
 const req=(path="/_qualification/drain",token="disposable",method="POST")=>new Request("https://cleanup.invalid"+path,{method,headers:{Authorization:"Bearer "+token}});
 for(const [request,settings]of[[req("/api"),env],[req(undefined,"wrong"),env],[req(undefined,undefined,"GET"),env],[req(),{...env,API_TOKEN:""}],[req(),{...env,ANTFLY_INCARNATION:"production"}],[req(),{...env,COORDINATOR_BACKEND:"sqlite"}]])assert.equal((await drain.fetch(request,settings)).status,401);
});
test("cleanup-only draining bounds deletes and confirms emptiness after completed storage operations",async()=>{
 const drain=(await import("../qualification/drain-worker.mjs")).default;
 let rows=Array.from({length:1005},(_,i)=>({key:"object/"+i,size:2})),fail=false;
 const env={API_TOKEN:"disposable",ANTFLY_INCARNATION:"qualification-drain",COORDINATOR_BACKEND:"r2",DATABASE:{async list({limit}){assert.ok(limit>=1&&limit<=1000);const count=Math.min(limit,259);return{objects:rows.slice(0,count),truncated:rows.length>count};},async delete(keys){if(fail)throw Error("DeletionUnavailable");assert.ok(keys.length<=1000);const removed=new Set(keys);rows=rows.filter(r=>!removed.has(r.key));}}};
 const request=()=>new Request("https://cleanup.invalid/_qualification/drain",{method:"POST",headers:{Authorization:"Bearer disposable"}});
 fail=true;await assert.rejects(drain.fetch(request(),env),/DeletionUnavailable/);assert.equal(rows.length,1005);fail=false;
 assert.deepEqual(await(await drain.fetch(request(),env)).json(),{done:false,deleted:1000,bytes:2000});
 assert.deepEqual(await(await drain.fetch(request(),env)).json(),{done:false,deleted:5,bytes:10});
 assert.deepEqual(await(await drain.fetch(request(),env)).json(),{done:true,deleted:0,bytes:0});
});

test("disposable drain retries lost acknowledgements without claiming exact deletion counts",async()=>{
 const{drainBucket}=await import("../qualification/drain-bucket.mjs");let calls=0;
 const report=await drainBucket(async()=>{calls++;if(calls===1)throw new TypeError("fetch failed",{cause:{code:"ECONNRESET"}});return calls===2?{done:false,deleted:4,bytes:8}:{done:true,deleted:0,bytes:0};},{delay:async()=>{}});
 assert.equal(calls,3);assert.equal(report.objects,4);assert.equal(report.transport_retries,1);assert.equal(report.counts_complete,false);
 let attempts=0;await assert.rejects(drainBucket(async()=>{attempts++;throw new TypeError("fetch failed");},{delay:async()=>{}}),/fetch failed/);assert.equal(attempts,3);
 attempts=0;await assert.rejects(drainBucket(async()=>{attempts++;throw Error("CleanupDrainStatus401");},{delay:async()=>{}}),/401/);assert.equal(attempts,1);
 await assert.rejects(drainBucket(async()=>({done:true,deleted:1,bytes:2})),/InvalidCleanupDrainPage/);
 await assert.rejects(drainBucket(async()=>({done:false,deleted:1,bytes:2}),{maxPages:1}),/CleanupDrainPageLimitExceeded/);
});


test('canonical body provenance is explicit without changing historical optimization profiles',async()=>{
 const{runtimePlacement}=await import('../qualification/gate-provenance.mjs');
 const prior=runtimePlacement({vars:{INLINE_POINT_BODIES:'true'}}).native_optimizations;
 assert.deepEqual(Object.keys(prior),['FUSED_READS','OPTIMISTIC_WAL','INLINE_POINT_BODIES','NATIVE_COMPACTION_PLAN']);
 const enabled=runtimePlacement({vars:{CANONICAL_POINT_BODIES:'true'}}).native_optimizations;
 assert.equal(enabled.CANONICAL_POINT_BODIES,true);
 assert.equal(runtimePlacement({vars:{CANONICAL_POINT_BODIES:'false'}}).native_optimizations.CANONICAL_POINT_BODIES,false);
});

test('native protocol flags bind new profiles without changing historical optimization profiles',async()=>{
 const{runtimePlacement}=await import('../qualification/gate-provenance.mjs');
 const prior=runtimePlacement({vars:{INLINE_POINT_BODIES:'true'}}).native_optimizations;assert.equal(Object.hasOwn(prior,'NATIVE_READ_PLAN'),false);assert.equal(runtimePlacement({vars:{PUBLICATION_UNITS:'32'}}).publication_units,32);
 for(const flag of['METADATA_COMMIT_HINTS','NATIVE_METADATA_CONTINUATIONS','READ_VIEW_CACHE_SEEDING','NATIVE_POINT_CONTINUATIONS','IMMUTABLE_PREPARED_REUSE','REPLAY_COST_CHECKPOINTS','INLINE_FACTS_LEAVES','NATIVE_WRITE_OVERLAP','NATIVE_READ_PLAN','NATIVE_READ_DETACHED','FUSED_READ_ADMISSION','DETACHED_METADATA_PROOFS','NATIVE_READ_STATE','NATIVE_READ_AFFINITY','WAL_DOCUMENT_INDEX','FOREGROUND_CACHE_POLICY','OPPORTUNISTIC_READ_PREPARE','NATIVE_WRITE_PLAN','PUBLICATION_CONTINUATIONS','PUBLICATION_CONTINUATION_FORCE']){
  assert.equal(runtimePlacement({vars:{[flag]:'true'}}).native_optimizations[flag],true);
  assert.equal(runtimePlacement({vars:{[flag]:'false'}}).native_optimizations[flag],false);
 }
});


test('warm platform-envelope retries preserve cold single attempts and keyed-write admission',async()=>{
 const{retryablePlatformEnvelope,GateClient}=await import('../qualification/gate-client.mjs');
 for(const message of ['get: internal error (10001)','Network connection lost.','Internal error while starting up Durable Object storage caused object to be reset; reference = abc123'])assert.equal(retryablePlatformEnvelope(message),true);
 for(const message of ['internal error; reference = abc123','WalIdempotencyConflict','OutOfMemory (10001)','denied (10003)'])assert.equal(retryablePlatformEnvelope(message),false);
 for(const phase of ['read','cold']){let calls=0;const version='version',client=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async()=>++calls===1?Response.json({error:'get: provider internal error (10001)'},{status:409}):Response.json({count:'512'},{headers:{'X-Antfly-Version':version}})});
  if(phase==='cold'){await assert.rejects(client.call('a',{op:'count'},phase),/HTTP 409/);assert.equal(calls,1);}else{assert.equal((await client.call('a',{op:'count'},phase)).sample.attempts,2);assert.equal(client.invocations.report().attempts.length,2);}
 }
 let calls=0;const unkeyed=new GateClient({origin:'https://fixture.invalid',version:'version'},'fixture',{fetch:async()=>{calls++;return Response.json({error:'get: provider internal error (10001)'},{status:409});}});
 await assert.rejects(unkeyed.call('a',{op:'mutate'},'write'),/HTTP 409/);assert.equal(calls,1);
});


test('cold quiescence requires advertised Queue-only dispatch and preserves code versions',async()=>{
 const{quiesceQueueMaintenance}=await import('../qualification/queue-quiescence.mjs');
 let pauses=0;const callbacks={verify:async()=>[],pause:async()=>{pauses++;}},run={kind:'candidate',capabilities:{queued_maintenance:true},versions:{app:'app',builder:'builder'}};
 for(const invalid of [{...run,kind:'baseline'},{...run,capabilities:{}},{...run,deleted:true}])await assert.rejects(quiesceQueueMaintenance(invalid,callbacks),/QueueOnlyMaintenanceRequired/);
 await assert.rejects(quiesceQueueMaintenance(run,{...callbacks,verify:async()=>['altered bundle']}),/ProvenanceFailed/);assert.equal(pauses,0);
 const versions=structuredClone(run.versions);await quiesceQueueMaintenance(run,callbacks);assert.equal(pauses,1);assert.equal(run.dispatch_paused,true);assert.deepEqual(run.versions,versions);
 const failed={...run,paused:false};await assert.rejects(quiesceQueueMaintenance(failed,{...callbacks,pause:async()=>{throw Error('pause failed');}}),/pause failed/);assert.equal(failed.paused,false);
});
