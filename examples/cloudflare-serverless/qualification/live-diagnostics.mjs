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

// Qualification owns each tail session and treats a coverage gap as a failure.
// Raw events and session URLs remain transient; only sanitized JSON is saved.
import WebSocket from 'ws';
import{spawn}from'node:child_process';
import{mkdir,open,writeFile}from'node:fs/promises';
import{createReadStream}from'node:fs';
import{resolve}from'node:path';
import{createHash}from'node:crypto';
import{sanitizeTailEvent}from'./metrics-tail.mjs';
import{assertGateRun}from'./gate-scope.mjs';
import{resourceFailureCode}from'../performance.mjs';
const roles=['app','writer','reader','builder'];
export class DiagnosticCoverage{
 constructor(runs){this.ioLifecycles=new WeakMap();this.streams=runs.flatMap(run=>roles.map(role=>({run:run.id,role,connected:false,active_connections:0,replica_disconnects:0,gap:false,events:0,resource_failures:0,gap_reasons:{}})));}
 stream(run,role){const row=this.streams.find(s=>s.run===run&&s.role===role);if(!row)throw Error('UnknownDiagnosticStream');return row;}
 connected(run,role){const row=this.stream(run,role);row.connected=true;row.active_connections++;}
 disconnected(run,role,reason,code){const row=this.stream(run,role);if(row.active_connections<1)throw Error('DiagnosticConnectionUnderflow');row.active_connections--;row.replica_disconnects++;if(Number.isInteger(code))row.last_close_code=code;if(row.active_connections===0)this.lost(run,role,reason,code);}
 lost(run,role,reason='connection_closed',code){const row=this.stream(run,role);row.gap=true;row.gap_reasons[reason]=(row.gap_reasons[reason]??0)+1;if(Number.isInteger(code))row.last_close_code=code;}
 observe(run,role,event){
  const row=this.stream(run,role);row.events++;if(event.truncated===true)this.lost(run,role,'log_truncated');
  if(['exceededMemory','exceededCpu'].includes(event.outcome))row.resource_failures++;
  for(const error of event.exceptions??[])if(resourceFailureCode(error.message))row.resource_failures++;
  for(const log of event.logs??[])for(const message of log.message??[]){let value;try{value=JSON.parse(message);}catch{continue;}
   if(resourceFailureCode(value.failure_code)||resourceFailureCode(value.error)||value.physical?.resource_failures>0||value.physical?.edge_cache_resource_errors>0)row.resource_failures++;
   // Completion must match the same physical lifetime, role and execution.
   // Retain completed IDs to tolerate duplicate tail replicas and reordering.
   if(value.io_draining===true||value.event==='antfly.io_settled'||value.io_lifecycle_id){
    const id=value.io_lifecycle_id;
    if(typeof id!=='string'||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(id)){this.lost(run,role,'native_io_draining');continue;}
    const state=value.event==='antfly.io_settled'?2:typeof value.io_draining==='boolean'?1:0;
    if(!state){this.lost(run,role,'invalid_io_lifecycle');continue;}
    let records=this.ioLifecycles.get(row);if(!records)this.ioLifecycles.set(row,records=new Map());
    if(!records.has(id)&&records.size>=4096){this.lost(run,role,'io_lifecycle_capacity');continue;}
    records.set(id,(records.get(id)??0)|state);
   }
  }
 }
 report({allowPending=false}={}){const streams=this.streams.map(row=>{const states=[...(this.ioLifecycles.get(row)?.values()??[])];const pending=states.filter(s=>s===1).length,unmatched=states.filter(s=>s===2).length;return{...row,gap:row.gap||!allowPending&&(pending>0||unmatched>0),gap_reasons:{...row.gap_reasons,...(!allowPending&&(pending||unmatched)?{io_lifetime_incomplete:pending+unmatched}:{})},pending_io:pending,unmatched_io_settlements:unmatched};});return{passed:streams.length===8&&streams.every(s=>s.connected&&!s.gap&&s.resource_failures===0&&(allowPending||s.pending_io===0&&s.unmatched_io_settlements===0)),streams};}
 assertHealthy(){if(!this.report({allowPending:true}).passed)throw Error('LiveResourceDiagnosticsFailed');}
 assertClean(){if(!this.report().passed)throw Error('LiveResourceDiagnosticsFailed');}
}
async function accountToken(){
 return new Promise((ok,bad)=>{
  const child=spawn(process.execPath,[resolve('node_modules/wrangler/bin/wrangler.js'),'auth','token','--json'],{timeout:30000,stdio:['ignore','pipe','ignore'],env:{...process.env,WRANGLER_SEND_METRICS:'false'}});let text='';
  child.stdout.on('data',part=>{text+=part;if(text.length>65536){text='';child.kill();bad(Error('DiagnosticCredentialResponseExceeded'));}});
  child.on('error',()=>bad(Error('DiagnosticCredentialLookupFailed')));child.on('exit',code=>{if(code!==0)return bad(Error('DiagnosticCredentialLookupFailed'));try{const auth=JSON.parse(text);if(!['oauth','api_token'].includes(auth.type)||typeof auth.token!=='string')throw Error();ok(auth.token);}catch{bad(Error('DiagnosticCredentialResponseInvalid'));}});
 });
}
export async function startLiveDiagnostics(runs,executionId,dir,{authenticate=accountToken,fetch:fetchImpl=fetch,Socket=WebSocket}={}){
 if(runs.length!==2||runs[0].id===runs[1].id||runs[0].account!==runs[1].account||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(executionId))throw Error('DiagnosticScopeMismatch');
 for(const run of runs)assertGateRun(run);
 const token=await authenticate(),coverage=new DiagnosticCoverage(runs),sessions=[],journals=[],directory=resolve(dir,'diagnostics-'+executionId);let closing=false;
 await mkdir(directory,{recursive:false});
 async function api(run,path,method,body){
  const response=await fetchImpl('https://api.cloudflare.com/client/v4/accounts/'+run.account+'/workers/scripts/'+encodeURIComponent(run.id+(path.role==='app'?'':'-'+path.role))+'/tails'+(path.id?'/'+encodeURIComponent(path.id):''),{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});
  let value;try{value=await response.json();}catch{throw Error('DiagnosticApiMalformed');}if(!response.ok||value.success!==true)throw Error('DiagnosticApiFailedStatus'+response.status);return value.result;
 }
 async function close(){
  closing=true;const errors=[];
  await Promise.all(sessions.map(async s=>{
   clearInterval(s.ping);clearTimeout(s.reconnectTimer);
   if(s.socket&&s.socket.readyState!==WebSocket.CLOSED)await new Promise(done=>{const timer=setTimeout(()=>{coverage.lost(s.run.id,s.role);done();},5000);s.socket.once('close',()=>{clearTimeout(timer);done();});s.socket.terminate();});
  }));
  for(const journal of journals){if(journal.id)try{await api(journal.run,{role:journal.role,id:journal.id},'DELETE');journal.id=null;}catch{errors.push('TailSessionCleanupFailed');}await journal.writes;await journal.file.close();}
  await writeFile(resolve(directory,'sessions.json'),JSON.stringify(journals.map(j=>({run:j.run.id,role:j.role,id:j.id??null}))),{mode:0o600});
  return errors;
 }
 function connect(session,url){
  if(closing)return Promise.resolve();
  const{run,role,journal}=session,socket=session.socket=new Socket(url,'trace-v1',{maxPayload:1024*1024,handshakeTimeout:30000});let healthy=false,ended=false;
  const reconnect=()=>{if(closing||session.reconnectTimer)return;session.reconnectTimer=setTimeout(()=>{session.reconnectTimer=null;connect(session,url).catch(reconnect);},1000);};
  const lost=(reason,code)=>{if(ended)return;ended=true;clearInterval(session.ping);if(healthy)coverage.disconnected(run.id,role,reason,code);socket.terminate();reconnect();};
  socket.on('message',data=>{
   try{const safe=sanitizeTailEvent(JSON.parse(data.toString()));if(!safe)return;coverage.observe(run.id,role,safe);const encoded=JSON.stringify({...safe,diagnostic_replica:session.replica})+'\n',bytes=Buffer.byteLength(encoded);
    if((journal.pendingBytes??0)+bytes>4*1024*1024){coverage.lost(run.id,role,'journal_capacity');socket.terminate();return;}
    journal.pendingBytes=(journal.pendingBytes??0)+bytes;journal.writes=journal.writes.then(()=>journal.file.writeFile(encoded)).catch(()=>{coverage.lost(run.id,role,'journal_write');}).finally(()=>journal.pendingBytes-=bytes);}catch{coverage.lost(run.id,role,'frame_parse');}
  });
  socket.on('error',error=>{if(error.code==='WS_ERR_UNSUPPORTED_MESSAGE_LENGTH')coverage.lost(run.id,role,'frame_size');lost('connection_error');});socket.on('close',code=>{if(!closing)lost('connection_closed',code);});
  return new Promise((ok,bad)=>{socket.once('open',()=>{
   if(closing||ended){socket.terminate();bad(Error('DiagnosticConnectionClosed'));return;}
   socket.send(JSON.stringify({debug:false}));healthy=true;coverage.connected(run.id,role);
   const pingMessage=Buffer.from('wrangler tail ping');let pong=performance.now();socket.on('pong',data=>{if(data.equals(pingMessage))pong=performance.now();});session.ping=setInterval(()=>{if(performance.now()-pong>45000)lost('heartbeat_timeout');else if(socket.readyState===WebSocket.OPEN)socket.ping(pingMessage);},10000);ok();
  });socket.once('error',()=>bad(Error('DiagnosticConnectionFailed')));socket.once('close',()=>bad(Error('DiagnosticConnectionClosed')));});
 }
 try{
  // Bounded sequential creation avoids a credential/session request burst.
  for(const run of runs)for(const role of roles){
   const journal={run,role,writes:Promise.resolve(),path:resolve(directory,run.id+'-'+role+'.jsonl')};journal.file=await open(journal.path,'ax',0o600);journals.push(journal);
   // The provider reuses a session ID for concurrent creates on one script.
   // Overlapping sockets protect transport continuity, not a shared tail reset.
   const record=await api(run,{role},'POST',{filters:[]});
   if(typeof record.id!=='string'||!/^[a-zA-Z0-9-]{1,128}$/.test(record.id))throw Error('DiagnosticSessionInvalid');journal.id=record.id;
   if(typeof record.url!=='string'||new URL(record.url).protocol!=='wss:')throw Error('DiagnosticSessionInvalid');
   await writeFile(resolve(directory,'sessions.json'),JSON.stringify(journals.map(j=>({run:j.run.id,role:j.role,id:j.id}))),{mode:0o600});
   for(let replica=0;replica<2;replica++){
   const session={run,role,journal,replica};sessions.push(session);
   await connect(session,record.url);
   }
  }
  coverage.assertClean();
 }catch(error){await close().catch(()=>{});throw error;}
 return{
  report:()=>coverage.report(),assertClean:()=>coverage.assertClean(),assertHealthy:()=>coverage.assertHealthy(),
  async finish(){const cleanup_errors=await close(),artifacts=[];for(const s of journals){const hash=createHash('sha256');let bytes=0;for await(const chunk of createReadStream(s.path)){bytes+=chunk.length;if(bytes>256*1024*1024)throw Error('DiagnosticJournalExceeded');hash.update(chunk);}artifacts.push({path:s.path,bytes,sha256:hash.digest('hex')});}const report={...coverage.report(),passed:coverage.report().passed&&cleanup_errors.length===0,cleanup_errors,artifacts};await writeFile(resolve(directory,'audit.json'),JSON.stringify(report,null,2));return report;},
 };
}

// Final evaluation verifies completed journals independently of the live process.
export async function verifyDiagnosticArtifacts(diagnostics,runs,executionId,dir){
 try{
  if(!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(executionId??'')||diagnostics?.artifacts?.length!==8||runs.length!==2)return false;
  const expected=runs.flatMap(run=>roles.map(role=>resolve(dir,'diagnostics-'+executionId,run.id+'-'+role+'.jsonl'))),seen=new Set();
  for(const artifact of diagnostics.artifacts){
   if(!expected.includes(artifact.path)||seen.has(artifact.path)||!/^[a-f0-9]{64}$/.test(artifact.sha256??'')||!Number.isSafeInteger(artifact.bytes)||artifact.bytes<0||artifact.bytes>256*1024*1024)return false;
   seen.add(artifact.path);const hash=createHash('sha256');let bytes=0;
   for await(const chunk of createReadStream(artifact.path)){bytes+=chunk.length;if(bytes>artifact.bytes)return false;hash.update(chunk);}
   if(bytes!==artifact.bytes||hash.digest('hex')!==artifact.sha256)return false;
  }
  return true;
 }catch{return false;}
}
