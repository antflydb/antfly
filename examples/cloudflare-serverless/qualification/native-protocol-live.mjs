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

// Disposable root-namespace correctness experiment, separate from tenants a-d.
// Physical actor replacement is not a claim of complete platform eviction.
import{readFile,writeFile,rename}from'node:fs/promises';
import{resolve}from'node:path';import{createHash}from'node:crypto';
import assert from'node:assert/strict';
import{GateClient}from'./gate-client.mjs';
import{startLiveDiagnostics}from'./live-diagnostics.mjs';
import{InvocationLedger,attemptParameter}from'./invocation-accounting.mjs';
const path=process.argv[2];if(!path)throw Error('usage: native-protocol-live CANDIDATE_RUN_JSON');
const client=await GateClient.open(path),run=client.run;
if(run.kind!=='candidate'||!run.paused||run.cold_execution_id)throw Error('RequirePausedUnusedCandidate');
const configs=await Promise.all(run.services.map(async service=>JSON.parse(await readFile(service.config))));
if(configs.some(config=>config.vars.PUBLICATION_CONTINUATIONS!=='true'||config.vars.NATIVE_WRITE_PLAN!=='true'))throw Error('RequireNativeProtocols');
const execution=crypto.randomUUID(),output=resolve(run.dir,'native-protocol-'+execution+'.json'),ledger=new InvocationLedger(),deadline=Date.now()+15*60*1000;
const report={schema:1,qualification:false,execution_id:execution,run:run.id,source_revision:run.revision,source_versions:structuredClone(run.versions),source_corpus:structuredClone(run.corpus),native_sha256:run.wasm_sha256,started_at:Date.now(),finished_at:null,root_only:true,gate_tenants_touched:false,physical_epochs:[0,1,2],samples:[],checks:{},fatal:null,limitations:['Fresh actor identities do not establish complete platform isolate eviction.','A discarded successful response models client acknowledgement loss; no network or storage fault was injected.','The root namespace and pending root messages must be retired before the four-tenant diagnostic.']};
let live;
const save=async()=>{report.invocation_ledger=ledger.report();await writeFile(output+'.tmp',JSON.stringify(report,null,2),{mode:0o600});await rename(output+'.tmp',output);};
async function call(input,epoch=0,phase='protocol'){
 if(Date.now()>=deadline)throw Error('ProtocolExperimentDeadline');
 const attempt=ledger.begin({phase,op:input.op,tenant:'root'}),url=new URL('/api',run.origin);url.searchParams.set('__antfly_qualification_attempt',attemptParameter(attempt.id));
 const started=performance.now();let response,value;
 try{response=await client.fetch(url,{method:'POST',headers:{Authorization:'Bearer '+client.token,'Content-Type':'application/json','X-Antfly-Physical-Epoch':String(epoch)},body:JSON.stringify(input),signal:AbortSignal.timeout(Math.min(35000,deadline-Date.now()))});ledger.response(attempt,response);value=await response.json();ledger.finish(attempt);}
 catch(error){ledger.finish(attempt,{transportError:true});throw error;}
 report.samples.push({op:input.op,action:input.publication_action??null,epoch,status:response.status,ms:performance.now()-started,request_phase:phase,phase:value.phase??null,offset:value.offset??null,version:value.version??null,wal_end_lsn:value.wal_end_lsn??null,published:value.published??null});
 if(live?.report().streams.some(stream=>stream.resource_failures>0))throw Error('ProtocolResourceFailure');
 if(response.headers.get('X-Antfly-Version')!==run.version)throw Error('ProtocolVersionMismatch');
 if(response.status!==200){const code=/^[A-Za-z][A-Za-z0-9_]{0,80}$/.test(value?.error??'')?value.error:'ProtocolRequestFailed';throw Error(code);}
 return value;
}
try{
 const gate=JSON.parse(await readFile(resolve(run.dir,'../gate.json'))),baseline=await GateClient.open(gate.trials[0].baseline);live=await startLiveDiagnostics([baseline.run,run],execution,gate.dir);
 const source=await readFile(new URL(import.meta.url));report.driver_sha256=createHash('sha256').update(source).digest('hex');await writeFile(resolve(run.dir,'native-protocol-'+execution+'.mjs'),source,{mode:0o600,flag:'wx'});
 const initial=await call({op:'count'});assert.equal(initial.count,'0','root correctness namespace must be fresh');
 const body=JSON.stringify({text:Array.from({length:12000},(_,i)=>'word'+i).join(' '),sparse_embedding:{needle:1}}),input={op:'mutate',operation_id:'native-protocol/'+execution,mutations:[{kind:'upsert',doc_id:'large',body}]};
 report.body_bytes=Buffer.byteLength(body);report.body_sha256=createHash('sha256').update(body).digest('hex');
 const receipt=await call(input);assert.equal(receipt.committed,true);report.visibility=receipt.visibility;report.checks.accepted=true;
 // Deliberately do not retain the first publication response's job identity.
 await call({op:'publish'});const discovered=await call({op:'publish',publication_action:'status'});assert.ok(discovered.job_id);const job=discovered.job_id;report.job_id=job;report.checks.discovered_checkpoint=true;await save();
 let replaced=false,complete;
 for(let step=0;step<256;step++){
  const epoch=replaced?1:0,result=await call({op:'publish',publication_job_id:job},epoch);
  if(result.published){complete=result;break;}
  assert.equal(result.wal_end_lsn,'0','private publication must retain the old public cutoff');
  if(!replaced&&result.offset>0){replaced=true;report.checks.replacement_after_internal_progress=true;}
  if(step%4===0){await save();console.log(JSON.stringify({op:'publication-progress',step,phase:result.phase,offset:result.offset,epoch}));}
 }
 assert.ok(complete,'large record publication must finish within the step bound');assert.ok(replaced,'replacement must follow progress inside the record');
 report.completion=complete;report.checks.resumed_after_actor_replacement=true;
 const found=await call({op:'lookup',doc_id:'large',required_visibility:receipt.visibility},1);assert.equal(found.body,body);report.checks.exact_body=true;
 assert.equal((await call({op:'search',text:'word11999'},1)).hits[0].doc_id,'large');report.checks.final_term=true;
 assert.equal((await call({op:'search',mode:'sparse',sparse:[{term:'needle',weight:1}]},1)).hits[0].doc_id,'large');report.checks.sparse_projection=true;
 assert.equal((await call(input,1)).visibility,receipt.visibility);report.checks.receipt_replay=true;
 assert.deepEqual(await call({op:'publish',publication_action:'status',publication_job_id:job},1),complete);report.checks.durable_completion=true;
 const smallBody=JSON.stringify({text:'small probe',payload:'x'.repeat(1024)}),small=await call({op:'mutate',operation_id:'native-protocol/'+execution+'/small',mutations:[{kind:'upsert',doc_id:'small',body:smallBody}]},1);
 for(let i=0;;i++){assert.ok(i<32);if(!(await call({op:'publish'},1)).has_more)break;}
 const cold=await call({op:'lookup',doc_id:'small',required_visibility:small.visibility},2,'read-cold');assert.equal(cold.body,smallBody);
 for(let i=0;i<16;i++)assert.equal((await call({op:'lookup',doc_id:'small',required_visibility:small.visibility},2,'read-warm')).body,smallBody);
 const reads=report.samples.filter(sample=>sample.request_phase==='read-warm').map(sample=>sample.ms).sort((a,b)=>a-b);
 report.read_sample_ms={fresh_physical_actor:report.samples.find(sample=>sample.request_phase==='read-cold').ms,reused_actor:{n:reads.length,p50:reads[Math.ceil(reads.length*.5)-1],p95:reads[Math.ceil(reads.length*.95)-1]}};
 assert.deepEqual(await call({op:'publish',publication_action:'status',publication_job_id:job},2),complete);report.checks.receipt_stable_after_later_publication=true;

}catch(error){report.fatal=/^[A-Za-z][A-Za-z0-9_]{0,80}$/.test(error?.message??'')?error.message:'ProtocolAssertionFailed';process.exitCode=1;}
if(live)try{report.diagnostics=await live.finish();if(!report.diagnostics.passed){report.fatal??='ProtocolDiagnosticsIncomplete';process.exitCode=1;}}catch{report.fatal??='ProtocolDiagnosticsFinalizationFailed';process.exitCode=1;}
report.finished_at=Date.now();await save();console.log(JSON.stringify({output,checks:report.checks,fatal:report.fatal}));
