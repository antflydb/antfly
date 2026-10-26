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
import{evaluateTrial,evaluateGate}from'../qualification/gate-policy.mjs';
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
 assert.throws(()=>resumeSeedExecution(r,baseline,{...candidate,last_phase:'workload'}));
 assert.throws(()=>resumeSeedExecution(r,baseline,{...candidate,last_execution_id:crypto.randomUUID()}));
 for(const change of[x=>x.candidate.samples.pop(),x=>x.candidate.samples.push({phase:'write'}),x=>x.baseline.cold.push({epoch:1}),x=>x.candidate.errors.push({phase:'seed'}),x=>x.candidate.run='foreign',x=>x.workload={},x=>{x.baseline.converged=x.candidate.converged=true;}]){const bad=structuredClone(r);change(bad);assert.throws(()=>resumeSeedExecution(bad,baseline,candidate));}
});
test('setup replay keeps mutation identities while measured execution remains fresh',async()=>{
 const client=new GateClient({},'disposable-fixture'),seedExecution=crypto.randomUUID(),measuredExecution=client.executionId,inputs=[];
 client.call=async(tenant,input)=>{inputs.push({tenant,input});return{value:{visibility:'seed-receipt'}};};client.converge=async()=>{};
 await client.seed(512,{executionId:seedExecution});
 assert.equal(inputs.length,32);assert.equal(client.expected.size,2048);
 assert.ok(inputs.every(({input})=>input.operation_id.startsWith('gate/'+seedExecution+'/seed/')));
 assert.equal(client.executionId,measuredExecution);assert.notEqual(client.executionId,seedExecution);
});
function report(){
 const execution_id=crypto.randomUUID(),version='01234567-0123-4567-89ab-0123456789ab',samples=[...Array.from({length:1000},()=>({phase:'read',ms:100,version,attempts:1})),...Array.from({length:300},()=>({phase:'write',ms:500,version,attempts:1}))];
 const side={execution_id,version,samples,errors:[],resource_errors:[],verification:{bodies:2052,receipts:8,passed:true},converged:true,lag:["a","b","c","d"].flatMap(tenant=>Array.from({length:60},(_,minute)=>({minute,tenant,lag:0}))),queue:Array.from({length:60},(_,minute)=>({minute,backlog_count:0,oldest_ms:0,dead_letter_count:0})),cold:Array.from({length:100},(_,epoch)=>({epoch:epoch+1,ms:1000,version,count:'512',attempts:1}))};
 const point={samples:10,baseline_recall:.9,candidate_recall:.9,baseline_gets:100,candidate_gets:100,baseline_bytes:1000,candidate_bytes:1000},points=[8,32,"all"].map(probes=>({...point,probes}));
 return{schema:1,execution_id,trial:1,documents:512,soak_ms:3600000,offered_period_ms:30000,workload:{period_ms:30000,rounds:120,requested_ms:3600000,started_at:1,offered_end_at:3600001},baseline:structuredClone(side),candidate:side,faults:{passed:true,artifact_sha256:'a'.repeat(64)},ann:{datasets:[{kind:"synthetic",documents:10000,points},{kind:"model-embeddings",documents:1000,fixture_sha256:"b".repeat(64),points}],samples:10,exact_control:true,baseline_recall:.9,candidate_recall:.9,baseline_gets:100,candidate_gets:100,baseline_bytes:1000,candidate_bytes:1000}};
}
test('gate requires three independent complete trials',()=>{const a=report();assert.equal(evaluateTrial(a).passed,true);assert.equal(evaluateGate([a]).passed,false);const b=report(),c=report();b.trial=2;c.trial=3;assert.equal(evaluateGate([a,b,c]).passed,true);assert.equal(evaluateGate([a,a,a]).passed,false);});
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
  const configPath=run.services[0].config,originalConfig=JSON.parse(await readFile(configPath,'utf8'));
  await writeFile(configPath,JSON.stringify({...originalConfig,vars:{RUNTIME_GENERATION:'one',R2_LOCATION_HINT:'wnam'}}));assert.match((await verifySealedRun(run)).join(','),/runtime placement policy changed/);await writeFile(configPath,JSON.stringify(originalConfig));
  assert.ok(verifyReportedRun({runtime_placement:{generation:'one',location_hint:'wnam'}},run).includes('runtime placement profile changed'));
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


test('warm transport retries retain original input and include all retry and scheduling time',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab',bodies=[],client=new GateClient({origin:'https://fixture.invalid',version},'disposable-fixture',{fetch:async(url,request)=>{bodies.push(request.body);if(bodies.length===1)throw new DOMException('network timed out','TimeoutError');return Response.json({count:'512'},{headers:{'X-Antfly-Version':version}});}});
 const {sample}=await client.call('a',{op:'count'},'read',{scheduled:Date.now()-40});assert.equal(sample.attempts,2);assert.ok(sample.ms>=280);assert.ok(sample.execution_ms>=240);assert.deepEqual(bodies,[bodies[0],bodies[0]]);assert.equal(client.errors.length,0);
});
test('transport replay requires a stable mutation identity and preserves its receipt',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab',bodies=[],client=new GateClient({origin:'https://fixture.invalid',version},'disposable-fixture',{fetch:async(url,request)=>{bodies.push(request.body);if(bodies.length===1)throw new TypeError('fetch failed');return Response.json({committed:true,visibility:'receipt-0000000000000001'},{headers:{'X-Antfly-Version':version}});}}),input={op:'mutate',operation_id:'fixed-receipt',mutations:[]};
 const result=await client.call('a',input,'write');assert.equal(result.sample.attempts,2);assert.equal(result.value.visibility,'receipt-0000000000000001');assert.deepEqual(bodies,[JSON.stringify(input),JSON.stringify(input)]);
 let calls=0;const unsafe=new GateClient({},'fixture',{fetch:async()=>{calls++;throw new TypeError('fetch failed');}});unsafe.run.origin='https://fixture.invalid';await assert.rejects(unsafe.call('a',{op:'mutate'},'write'),/fetch failed/);assert.equal(calls,1);
});
test('cold attempts never retry transport or backpressure and warm recovery retains resource failures',async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab';
 for(const failure of ['network','backpressure']){let calls=0;const cold=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async()=>{calls++;if(failure==='network')throw new DOMException('timeout','TimeoutError');return Response.json({error:'CoordinatorRecovering'},{status:503});}});await assert.rejects(cold.call('a',{op:'count'},'cold',{epoch:1}));assert.equal(calls,1);assert.equal(cold.errors[0].attempts,1);}
 let calls=0;const warm=new GateClient({origin:'https://fixture.invalid',version},'fixture',{fetch:async()=>++calls===1?Response.json({error:'SQLITE_NOMEM'},{status:503}):Response.json({count:'512'},{headers:{'X-Antfly-Version':version}})});assert.equal((await warm.call('a',{op:'count'},'read')).sample.attempts,2);assert.equal(warm.resourceErrors.length,1);
});
