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
 event.logs[0].message.push(JSON.stringify({event:'antfly.coordinator_snapshot',action:'observe_read',proof_prefetch:true,proof_prefetch_reserved_bytes:102400,physical_r2:{calls:3,bytes_read:42,token:secret}}));
 const safe=sanitizeTailEvent(event);assert.ok(!JSON.stringify(safe).includes('do-not-persist'));const value=JSON.parse(safe.logs[0].message[0]);assert.equal(value.elapsed_ms,5);assert.equal(value.physical.calls,2);assert.equal(value.failure_code,'WorkLeaseLost');assert.equal(safe.exceptions[0].message,'RedactedError');assert.equal(value.token,undefined);assert.deepEqual(JSON.parse(safe.logs[1].message[0]).physical_r2,{calls:3,bytes_read:42});const proof=JSON.parse(safe.logs[1].message[0]);assert.equal(proof.action,'observe_read');assert.equal(proof.proof_prefetch,true);assert.equal(proof.proof_prefetch_reserved_bytes,102400);
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

test('redundant diagnostic coverage requires uninterrupted overlap and retains every failure',async()=>{
 const{DiagnosticCoverage}=await import('../qualification/live-diagnostics.mjs'),coverage=new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]);
 for(const run of['baseline','candidate'])for(const role of['app','writer','reader','builder']){coverage.connected(run,role);coverage.connected(run,role);}
 coverage.disconnected('baseline','writer','connection_closed',1006);assert.equal(coverage.report().passed,true);assert.equal(coverage.stream('baseline','writer').active_connections,1);coverage.connected('baseline','writer');
 coverage.disconnected('baseline','writer','connection_closed',1006);coverage.disconnected('baseline','writer','heartbeat_timeout');assert.equal(coverage.report().passed,false);coverage.connected('baseline','writer');coverage.connected('baseline','writer');assert.equal(coverage.report().passed,false);assert.deepEqual(coverage.stream('baseline','writer').gap_reasons,{heartbeat_timeout:1});
 assert.throws(()=>new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]).disconnected('baseline','writer','connection_closed'),/Underflow/);
});
test('overlapping diagnostic sockets repair one connection and close unique provider sessions',async()=>{
 const{startLiveDiagnostics,verifyDiagnosticArtifacts}=await import('../qualification/live-diagnostics.mjs'),{EventEmitter}=await import('node:events'),{mkdtemp,readFile,rm}=await import('node:fs/promises'),{resolve}=await import('node:path'),dir=await mkdtemp(resolve('dist','diagnostic-replicas-')),sockets=[],created=new Set(),deleted=new Set(),execution=crypto.randomUUID();
 const runs=['b','c'].map(side=>{const id='antfly-1278-g-test-1-'+side,d=resolve(dir,id);return{id,account:'a'.repeat(32),bucket:id,worker:id,queues:[id,id+'-dead'],dir:d,token_file:resolve(d,'api-token'),services:['app','writer','reader','builder'].map(role=>({role,name:id+(role==='app'?'':'-'+role)}))};});
 class Socket extends EventEmitter{constructor(){super();this.readyState=0;sockets.push(this);queueMicrotask(()=>{if(this.readyState===3)return;this.readyState=1;this.emit('open');});}send(){}ping(value){this.emit('pong',value);}terminate(){if(this.readyState===3)return;this.readyState=3;queueMicrotask(()=>this.emit('close',1006));}}
 const fetch=async(url,options)=>{if(options.method==='POST'){const id=crypto.randomUUID();created.add(id);return Response.json({success:true,result:{id,url:'wss://diagnostic.invalid/'+id}});}const id=url.split('/').at(-1);assert.ok(created.has(id));assert.ok(!deleted.has(id));deleted.add(id);return Response.json({success:true,result:{}});};let live;
 try{live=await startLiveDiagnostics(runs,execution,dir,{authenticate:async()=>'not-a-real-token',fetch,Socket});assert.equal(sockets.length,16);sockets[0].terminate();await new Promise(r=>setTimeout(r,1100));assert.equal(sockets.length,17);assert.equal(live.report().passed,true);assert.equal(live.report().streams[0].active_connections,2);assert.equal(live.report().streams[0].replica_disconnects,1);
  sockets[16].emit('message',Buffer.from(JSON.stringify({outcome:'ok',event:{request:{headers:{Authorization:'secret'}}},logs:[{message:[JSON.stringify({event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM'})]}]})));assert.equal(live.report().passed,false);
  const report=await live.finish();live=null;assert.equal(created.size,8);assert.deepEqual(deleted,created);assert.equal(report.artifacts.length,8);assert.equal(await verifyDiagnosticArtifacts(report,runs,execution,dir),true);const text=await readFile(report.artifacts[0].path,'utf8');assert.ok(text.includes('SQLITE_NOMEM'));assert.ok(!text.includes('secret'));assert.ok(!text.includes('Authorization'));assert.ok(sockets.every(s=>s.readyState===3));assert.ok(JSON.parse(await readFile(resolve(dir,'diagnostics-'+execution,'sessions.json'))).every(s=>s.id===null));
  let failed=false;const second=crypto.randomUUID();live=await startLiveDiagnostics(runs,second,dir,{authenticate:async()=>'not-a-real-token',Socket,fetch:async(url,options)=>{if(options.method==='DELETE'&&!failed){failed=true;return Response.json({success:false},{status:503});}return fetch(url,options);}});const incomplete=await live.finish();live=null;assert.equal(incomplete.passed,false);assert.equal(incomplete.cleanup_errors.length,1);assert.equal(incomplete.artifacts.length,8);assert.equal(await verifyDiagnosticArtifacts(incomplete,runs,second,dir),true);assert.equal(JSON.parse(await readFile(resolve(dir,'diagnostics-'+second,'sessions.json'))).filter(s=>s.id!==null).length,1,'preserve the exact outstanding provider ID without losing journals');
 }finally{if(live)await live.finish();await rm(dir,{recursive:true,force:true});}
});
test('persisted diagnostic access probes constrain scope and never expose provider rows or credentials',async()=>{
 const{persistedDiagnosticQuery,probePersistedDiagnostics}=await import('../qualification/persisted-diagnostic-probe.mjs'),{resolve}=await import('node:path'),id='antfly-1278-g-test-1-b',dir=resolve('dist',id),run={id,account:'a'.repeat(32),bucket:id,worker:id,queues:[id,id+'-dead'],dir,token_file:resolve(dir,'api-token'),services:['app','writer','reader','builder'].map(role=>({role,name:id+(role==='app'?'':'-'+role)}))},time={from:1000,to:2000},secret='secret-do-not-persist-12345';
 const query=persistedDiagnosticQuery(run,time);assert.equal(query.dry,true);assert.equal(query.view,'calculations');assert.deepEqual(query.parameters.filters,[{key:'$metadata.service',type:'string',operation:'eq',value:id}]);assert.throws(()=>persistedDiagnosticQuery({...run,id:'production'},time),/Scope/);assert.throws(()=>persistedDiagnosticQuery(run,{from:0,to:86400001}),/Timeframe/);
 const report=await probePersistedDiagnostics(run,secret,time,{fetch:async(url,options)=>{assert.ok(url.endsWith('/workers/observability/telemetry/query'));assert.equal(options.headers.Authorization,'Bearer '+secret);return Response.json({success:true,result:{run:{secret},statistics:{},calculations:[{row:secret}]}},{status:200});}});assert.equal(report.success,true);assert.equal(report.qualification,false);assert.ok(!JSON.stringify(report).includes(secret));
 const denied=await probePersistedDiagnostics(run,secret,time,{fetch:async()=>Response.json({success:false,errors:[{code:10000,message:secret}]},{status:403})});assert.equal(denied.success,false);assert.deepEqual(denied.error_codes,[10000]);assert.ok(!JSON.stringify(denied).includes(secret));await assert.rejects(probePersistedDiagnostics(run,secret,time,{fetch:async()=>new Response('x'.repeat(4*1024*1024+1))}),/ResponseExceeded/);
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

test('native protocol diagnostics preserve bounded progress and timing labels without arbitrary strings',()=>{
 const id='a'.repeat(32),events=[{event:'antfly.trace',scope:'wal',spans:[{name:'native_wal_plan',duration_ms:12},{name:'native_wal_finalize',duration_ms:4},{name:'rpc:openPublicationPlan',duration_ms:3},{name:'private document text',duration_ms:1}]},{event:'antfly.publication_progress',job_id:id,phase:'text',offset:1024,published:false,body:'private document text'},{event:'antfly.publication_batch_plan',operations:20,transferred_bytes:400}];
 const safe=sanitizeTailEvent({logs:events.map(value=>({message:[JSON.stringify(value)]}))}),values=safe.logs.map(log=>JSON.parse(log.message[0]));
 assert.deepEqual(values[0].spans.slice(0,3).map(span=>span.name),['native_wal_plan','native_wal_finalize','rpc:openPublicationPlan']);assert.equal(values[0].spans[3].name,undefined);
 assert.equal(values[1].job_id,id);assert.equal(values[1].phase,'text');assert.equal(values[1].offset,1024);assert.equal(values[1].body,undefined);assert.ok(!JSON.stringify(safe).includes('private document text'));
});

test('diagnostic tails retain finite platform timing and version fields without copying provider context',()=>{
 const id='01234567-0123-4567-89ab-0123456789ab',safe=sanitizeTailEvent({outcome:'ok',cpuTime:3,wallTime:90,scriptVersion:{id,private:'discard'},request:{body:'discard'}});
 assert.deepEqual(safe,{outcome:'ok',cpuTime:3,wallTime:90,scriptVersion:{id}});
 assert.deepEqual(sanitizeTailEvent({outcome:'ok',cpuTime:Infinity,wallTime:-1,scriptVersion:{id:'discard'}}),{outcome:'ok'});
});

test('publication diagnostics retain content identities without accepting payload strings',()=>{
 const safe=sanitizeTailEvent({logs:[{message:[JSON.stringify({event:'antfly.publication_progress',checkpoint_sha256:'a'.repeat(64),receipt_sha256:'b'.repeat(64),posting_quantum:1024,body:'private-body',other:'private-value'})]}]});
 const metric=JSON.parse(safe.logs[0].message[0]);assert.equal(metric.checkpoint_sha256,'a'.repeat(64));assert.equal(metric.receipt_sha256,'b'.repeat(64));assert.equal(metric.posting_quantum,1024);assert.ok(!JSON.stringify(safe).includes('private'));
});


test('physical I/O coverage requires matched settlement and retains late resource failures',async()=>{
 const{DiagnosticCoverage}=await import('../qualification/live-diagnostics.mjs');
 const create=()=>{const c=new DiagnosticCoverage([{id:'baseline'},{id:'candidate'}]);for(const run of ['baseline','candidate'])for(const role of ['app','writer','reader','builder'])c.connected(run,role);return c;};
 const id=crypto.randomUUID(),event=value=>sanitizeTailEvent({outcome:'ok',logs:[{message:[JSON.stringify(value)]}]}),start=event({event:'antfly.operation',io_draining:true,io_lifecycle_id:id}),end=event({event:'antfly.io_settled',io_lifecycle_id:id});
 for(const order of [[start,end],[end,start]]){const c=create();c.observe('candidate','app',order[0]);assert.equal(c.report().passed,false);assert.doesNotThrow(()=>c.assertHealthy());c.observe('candidate','app',order[1]);c.observe('candidate','app',order[0]);assert.equal(c.report().passed,true);c.observe('candidate','app',event({event:'antfly.resource_failure',failure_code:'OutOfMemory'}));assert.equal(c.report().passed,false);assert.throws(()=>c.assertHealthy(),/DiagnosticsFailed/);}
 const c=create();c.observe('candidate','writer',start);c.observe('candidate','reader',end);assert.equal(c.report().passed,false);assert.equal(c.report().streams.find(s=>s.role==='writer'&&s.run==='candidate').pending_io,1);
 const missing=create();missing.observe('candidate','app',event({event:'antfly.operation',io_draining:true}));assert.equal(missing.report().passed,false);missing.observe('candidate','app',end);assert.equal(missing.report().passed,false);
 const overflow=create();for(let i=0;i<4097;i++)overflow.observe('candidate','app',event({event:'antfly.io_settled',io_lifecycle_id:crypto.randomUUID()}));assert.equal(overflow.report().streams.find(s=>s.role==='app'&&s.run==='candidate').gap_reasons.io_lifecycle_capacity,1);
});


test('lease and writer phase diagnostics retain only fixed labels',()=>{
 const value={event:'antfly.operation',op:'lookup',physical:{read_view_lease_hit:1,read_admission_fallback_reason:'ReadSnapshotNeedsPin'}},trace={event:'antfly.trace',scope:'wal',spans:['write_admission','writer_authority','write_refresh','native_wal_plan','native_wal_finalize'].map(name=>({name,duration_ms:2}))};
 const safe=sanitizeTailEvent({logs:[{message:[JSON.stringify(value),JSON.stringify(trace)]}]});assert.equal(JSON.parse(safe.logs[0].message[0]).physical.read_admission_fallback_reason,'ReadSnapshotNeedsPin');assert.equal(JSON.parse(safe.logs[1].message[0]).spans.length,5);assert.ok(JSON.parse(safe.logs[1].message[0]).spans.every(s=>s.name));
 value.physical.read_admission_fallback_reason='private arbitrary payload';assert.ok(!JSON.stringify(sanitizeTailEvent({logs:[{message:[JSON.stringify(value)]}]})).includes('private arbitrary payload'));
});

test('point dependency lifecycle and immutable reuse counters remain diagnostic evidence',()=>{
 const value=sanitizeTailEvent({outcome:'ok',logs:[{message:[JSON.stringify({event:'antfly.point_dependency',point_dependency_gets:1,point_dependency_reserved_bytes:4096,io_draining:true,io_lifecycle_id:'01234567-0123-4567-89ab-0123456789ab'})]}]});
 assert.ok(JSON.stringify(value).includes('antfly.point_dependency'));assert.ok(JSON.stringify(value).includes('point_dependency_gets'));
});

test('cold phase diagnostics retain scoped timings and classify provider memory errors without secrets',()=>{
 const value=sanitizeTailEvent({outcome:'exception',logs:[{message:[JSON.stringify({event:'antfly.trace',scope:'resident:lookup',spans:[{name:'bootstrap_decode',duration_ms:7},{name:'bootstrap_serving_import_native',duration_ms:5}]}),JSON.stringify({event:'antfly.durable_storage_error',phase:'put_intent',error_message:'SQLITE_NOMEM credential=secret',token:'secret'})]}]});
 const logs=value.logs.map(l=>JSON.parse(l.message[0]));assert.equal(logs[0].scope,'resident:lookup');assert.equal(logs[0].spans[0].name,'bootstrap_decode');assert.equal(logs[1].phase,'put_intent');assert.equal(logs[1].error_message,'SQLITE_NOMEM');assert.ok(!JSON.stringify(value).includes('secret'));
});

test('cold cohort diagnostics preserve only bounded caller UUIDs',()=>{
 const id='01234567-0123-4567-89ab-0123456789ab',safe=sanitizeTailEvent({logs:[{message:[JSON.stringify({event:'antfly.operation',trace_ids:[id,'secret'],resident_cohort_trace_ids:[id,'secret']})]}]});const value=JSON.parse(safe.logs[0].message[0]);assert.deepEqual(value.trace_ids,[id]);assert.deepEqual(value.resident_cohort_trace_ids,[id]);
});

test('sanitized phase traces preserve platform resource flags',()=>{
 const safe=sanitizeTailEvent({logs:[{message:[JSON.stringify({event:'antfly.trace',scope:'resident:lookup',errors:[{phase:'resident',resource_limit:'memory',remote:true,retryable:false}]})]}]});const row=JSON.parse(safe.logs[0].message[0]);assert.equal(row.errors[0].resource_limit,'memory');assert.equal(row.errors[0].remote,true);
});
