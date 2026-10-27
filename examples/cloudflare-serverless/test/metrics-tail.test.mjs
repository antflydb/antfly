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

import{test}from'node:test';import assert from'node:assert/strict';import{sanitizeTailEvent,tailFrames}from'../qualification/metrics-tail.mjs';
test('streaming metrics discard HTTP credentials bodies and arbitrary text before persistence',()=>{
 const secret='Bearer do-not-persist',event={outcome:'exception',event:{request:{headers:{authorization:secret},url:secret}},exceptions:[{name:'Error',message:'unexpected '+secret}],logs:[{timestamp:42,message:[secret,JSON.stringify({event:'antfly.operation',op:'lookup',elapsed_ms:5,body:secret,token:secret,unknown:secret,physical:{calls:2,headers:{authorization:secret},name:'do-not-persist',event:'do-not-persist'},failure_code:'Error: WorkLeaseLost '+secret})]}]};
 const safe=sanitizeTailEvent(event);assert.ok(!JSON.stringify(safe).includes('do-not-persist'));const value=JSON.parse(safe.logs[0].message[0]);assert.equal(value.elapsed_ms,5);assert.equal(value.physical.calls,2);assert.equal(value.failure_code,'WorkLeaseLost');assert.equal(safe.exceptions[0].message,'RedactedError');assert.equal(value.token,undefined);
});
test('sanitized tails retain late resource codes without raw error text',()=>{
 const safe=sanitizeTailEvent({outcome:'ok',logs:[{timestamp:1,message:[JSON.stringify({event:'antfly.resource_failure',op:'get',failure_code:'SQLITE_NOMEM secret-must-not-persist',error:'CPU time limit exceeded secret-must-not-persist'})]}]});
 const value=JSON.parse(safe.logs[0].message[0]);assert.equal(value.event,'antfly.resource_failure');assert.equal(value.failure_code,'SQLITE_NOMEM');assert.ok(!JSON.stringify(safe).includes('secret-must-not-persist'));
 const legacy=sanitizeTailEvent({outcome:'ok',logs:[{message:['Antfly object operation failed','get','Error: out of memory: SQLITE_NOMEM secret-must-not-persist']}]});assert.equal(JSON.parse(legacy.logs[0].message[0]).failure_code,'SQLITE_NOMEM');assert.ok(!JSON.stringify(legacy).includes('secret-must-not-persist'));
});
test('tail framing bounds buffers and handles fragmented quoted objects',()=>{
 const out=[],parse=tailFrames(v=>out.push(v),{maxBytes:256});parse('CLI banner\n'+JSON.stringify({event:{body:'x'.repeat(512)}}));const raw=JSON.stringify({outcome:'ok',logs:[{timestamp:1,message:[JSON.stringify({event:'antfly.trace',scope:'http',elapsed_ms:5})]}]});for(let i=0;i<raw.length;i+=3)parse(raw.slice(i,i+3));assert.equal(out.length,1);assert.equal(out[0].outcome,'ok');assert.equal(JSON.parse(out[0].logs[0].message[0]).scope,'http');
});

test('live diagnostic coverage rejects a late resource failure after a successful HTTP outcome',async()=>{
 const{DiagnosticCoverage}=await import('../qualification/live-diagnostics.mjs'),coverage=new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]);
 for(const run of['baseline','candidate'])for(const role of['app','writer','reader','builder'])coverage.connected(run,role);
 assert.equal(coverage.report().passed,true);coverage.observe('candidate','writer',sanitizeTailEvent({outcome:'ok',logs:[{message:[JSON.stringify({event:'antfly.resource_failure',op:'put',failure_code:'SQLITE_NOMEM'})]}]}));
 assert.equal(coverage.report().passed,false);assert.throws(()=>coverage.assertClean(),/LiveResourceDiagnosticsFailed/);assert.equal(coverage.report().streams.find(s=>s.run==='candidate'&&s.role==='writer').resource_failures,1);
});
test('live diagnostic coverage preserves connection gaps and requires all eight roles',async()=>{
 const{DiagnosticCoverage}=await import('../qualification/live-diagnostics.mjs'),coverage=new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]);
 assert.equal(coverage.report().passed,false);for(const run of['baseline','candidate'])for(const role of['app','writer','reader','builder'])coverage.connected(run,role);
 assert.equal(coverage.report().passed,true);coverage.lost('baseline','reader','connection_closed',1006);coverage.connected('baseline','reader');assert.equal(coverage.report().passed,false);const gap=coverage.report().streams.find(s=>s.run==='baseline'&&s.role==='reader');assert.deepEqual(gap.gap_reasons,{connection_closed:1});assert.equal(gap.last_close_code,1006);
 const cpu=new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]);for(const run of['baseline','candidate'])for(const role of['app','writer','reader','builder'])cpu.connected(run,role);cpu.observe('candidate','builder',{outcome:'exceededCpu'});assert.equal(cpu.report().passed,false);
 const truncated=sanitizeTailEvent({outcome:'ok',logs:Array.from({length:257},(_,i)=>({message:[i===256?'SQLITE_NOMEM':'ordinary output']}))});assert.equal(truncated.truncated,true);const lost=new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]);for(const run of['baseline','candidate'])for(const role of['app','writer','reader','builder'])lost.connected(run,role);lost.observe('candidate','app',truncated);assert.equal(lost.report().passed,false);
 const pending=new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]);for(const run of['baseline','candidate'])for(const role of['app','writer','reader','builder'])pending.connected(run,role);pending.observe('candidate','writer',{logs:[{message:[JSON.stringify({event:'antfly.coordinator',failure_code:'DeadlineExceeded',io_draining:true})]}]});assert.equal(pending.report().passed,false);
 const completed=new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]);for(const run of['baseline','candidate'])for(const role of['app','writer','reader','builder'])completed.connected(run,role);for(const value of[{failure_code:'Canceled',io_draining:false},{failure_code:'Canceled'},{failure_code:'DeadlineExceeded',io_draining:false}])completed.observe('baseline','builder',{logs:[{message:[JSON.stringify({event:'antfly.operation',...value})]}]});assert.equal(completed.report().passed,true);
});

test('completed live diagnostic evidence rejects altered missing or substituted journals',async()=>{
 const{verifyDiagnosticArtifacts}=await import('../qualification/live-diagnostics.mjs'),{mkdtemp,mkdir,writeFile,rm}=await import('node:fs/promises'),{resolve}=await import('node:path'),{createHash}=await import('node:crypto');
 const dir=await mkdtemp(resolve('dist','diagnostic-evidence-')),id=crypto.randomUUID(),runs=[{id:'baseline'},{id:'candidate'}],bytes=Buffer.from('{"outcome":"ok"}\n'),artifacts=[];
 try{await mkdir(resolve(dir,'diagnostics-'+id));for(const run of runs)for(const role of['app','writer','reader','builder']){const path=resolve(dir,'diagnostics-'+id,run.id+'-'+role+'.jsonl');await writeFile(path,bytes);artifacts.push({path,bytes:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex')});}
  const proof={artifacts};assert.equal(await verifyDiagnosticArtifacts(proof,runs,id,dir),true);
  assert.equal(await verifyDiagnosticArtifacts({artifacts:artifacts.slice(1)},runs,id,dir),false);assert.equal(await verifyDiagnosticArtifacts({artifacts:[artifacts[0],...artifacts.slice(0,7)]},runs,id,dir),false);
  assert.equal(await verifyDiagnosticArtifacts({artifacts:artifacts.map((a,i)=>i? a:{...a,path:resolve(dir,'substituted.jsonl')})},runs,id,dir),false);
  await writeFile(artifacts[0].path,'{"outcome":"exceededMemory"}\n');assert.equal(await verifyDiagnosticArtifacts(proof,runs,id,dir),false);
  await rm(artifacts[0].path);assert.equal(await verifyDiagnosticArtifacts(proof,runs,id,dir),false);
 }finally{await rm(dir,{recursive:true,force:true});}
});
