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
 constructor(runs){this.streams=runs.flatMap(run=>roles.map(role=>({run:run.id,role,connected:false,gap:false,events:0,resource_failures:0})));}
 stream(run,role){const row=this.streams.find(s=>s.run===run&&s.role===role);if(!row)throw Error('UnknownDiagnosticStream');return row;}
 connected(run,role){this.stream(run,role).connected=true;}
 lost(run,role){this.stream(run,role).gap=true;}
 observe(run,role,event){
  const row=this.stream(run,role);row.events++;if(event.truncated===true)row.gap=true;
  if(['exceededMemory','exceededCpu'].includes(event.outcome))row.resource_failures++;
  for(const error of event.exceptions??[])if(resourceFailureCode(error.message))row.resource_failures++;
  for(const log of event.logs??[])for(const message of log.message??[]){let value;try{value=JSON.parse(message);}catch{continue;}
   if(resourceFailureCode(value.failure_code)||resourceFailureCode(value.error)||value.physical?.resource_failures>0)row.resource_failures++;
  }
 }
 report(){const streams=this.streams.map(row=>({...row}));return{passed:streams.length===8&&streams.every(s=>s.connected&&!s.gap&&s.resource_failures===0),streams};}
 assertClean(){if(!this.report().passed)throw Error('LiveResourceDiagnosticsFailed');}
}
async function accountToken(){
 return new Promise((ok,bad)=>{
  const child=spawn(process.execPath,[resolve('node_modules/wrangler/bin/wrangler.js'),'auth','token','--json'],{timeout:30000,stdio:['ignore','pipe','ignore'],env:{...process.env,WRANGLER_SEND_METRICS:'false'}});let text='';
  child.stdout.on('data',part=>{text+=part;if(text.length>65536){text='';child.kill();bad(Error('DiagnosticCredentialResponseExceeded'));}});
  child.on('error',()=>bad(Error('DiagnosticCredentialLookupFailed')));child.on('exit',code=>{if(code!==0)return bad(Error('DiagnosticCredentialLookupFailed'));try{const auth=JSON.parse(text);if(!['oauth','api_token'].includes(auth.type)||typeof auth.token!=='string')throw Error();ok(auth.token);}catch{bad(Error('DiagnosticCredentialResponseInvalid'));}});
 });
}
export async function startLiveDiagnostics(runs,executionId,dir){
 if(runs.length!==2||runs[0].id===runs[1].id||runs[0].account!==runs[1].account||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(executionId))throw Error('DiagnosticScopeMismatch');
 for(const run of runs)assertGateRun(run);
 const token=await accountToken(),coverage=new DiagnosticCoverage(runs),sessions=[],directory=resolve(dir,'diagnostics-'+executionId);let closing=false;
 await mkdir(directory,{recursive:false});
 async function api(run,path,method,body){
  const response=await fetch('https://api.cloudflare.com/client/v4/accounts/'+run.account+'/workers/scripts/'+encodeURIComponent(run.id+(path.role==='app'?'':'-'+path.role))+'/tails'+(path.id?'/'+encodeURIComponent(path.id):''),{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});
  let value;try{value=await response.json();}catch{throw Error('DiagnosticApiMalformed');}if(!response.ok||value.success!==true)throw Error('DiagnosticApiFailedStatus'+response.status);return value.result;
 }
 async function close(){
  closing=true;const errors=[];
  await Promise.all(sessions.map(async s=>{
   clearInterval(s.ping);
   if(s.socket&&s.socket.readyState!==WebSocket.CLOSED)await new Promise(done=>{const timer=setTimeout(()=>{coverage.lost(s.run.id,s.role);done();},5000);s.socket.once('close',()=>{clearTimeout(timer);done();});s.socket.terminate();});
   if(s.id)try{await api(s.run,{role:s.role,id:s.id},'DELETE');s.id=null;}catch{errors.push('TailSessionCleanupFailed');}
   if(s.file){await s.writes;await s.file.close();s.file=null;}
  }));
  if(errors.length)throw Error(errors.join('; '));
 }
 try{
  // Bounded sequential creation avoids a credential/session request burst.
  for(const run of runs)for(const role of roles){
   const session={run,role,writes:Promise.resolve(),path:resolve(directory,run.id+'-'+role+'.jsonl')};sessions.push(session);
   const record=await api(run,{role},'POST',{filters:[]});
   if(typeof record.id!=='string'||!/^[a-zA-Z0-9-]{1,128}$/.test(record.id))throw Error('DiagnosticSessionInvalid');session.id=record.id;
   if(typeof record.url!=='string'||new URL(record.url).protocol!=='wss:')throw Error('DiagnosticSessionInvalid');
   session.file=await open(session.path,'ax',0o600);
   const socket=session.socket=new WebSocket(record.url,'trace-v1',{maxPayload:1024*1024,handshakeTimeout:30000});
   socket.on('message',data=>{
    try{const safe=sanitizeTailEvent(JSON.parse(data.toString()));if(!safe)return;coverage.observe(run.id,role,safe);const encoded=JSON.stringify(safe)+'\n',bytes=Buffer.byteLength(encoded);
     if((session.pendingBytes??0)+bytes>4*1024*1024){coverage.lost(run.id,role);socket.terminate();return;}
     session.pendingBytes=(session.pendingBytes??0)+bytes;session.writes=session.writes.then(()=>session.file.writeFile(encoded)).catch(()=>{coverage.lost(run.id,role);}).finally(()=>session.pendingBytes-=bytes);}catch{coverage.lost(run.id,role);}
   });
   socket.on('error',()=>coverage.lost(run.id,role));socket.on('close',()=>{if(!closing)coverage.lost(run.id,role);});
   await new Promise((ok,bad)=>{socket.once('open',()=>{socket.send(JSON.stringify({debug:false}));coverage.connected(run.id,role);ok();});socket.once('error',()=>bad(Error('DiagnosticConnectionFailed')));socket.once('close',()=>bad(Error('DiagnosticConnectionClosed')));});
   session.pong=Date.now();socket.on('pong',()=>session.pong=Date.now());session.ping=setInterval(()=>{if(Date.now()-session.pong>45000){coverage.lost(run.id,role);socket.terminate();}else socket.ping();},15000);
  }
  coverage.assertClean();
 }catch(error){await close().catch(()=>{});throw error;}
 return{
  report:()=>coverage.report(),assertClean:()=>coverage.assertClean(),
  async finish(){await close();const artifacts=[];for(const s of sessions){const hash=createHash('sha256');let bytes=0;for await(const chunk of createReadStream(s.path)){bytes+=chunk.length;if(bytes>256*1024*1024)throw Error('DiagnosticJournalExceeded');hash.update(chunk);}artifacts.push({path:s.path,bytes,sha256:hash.digest('hex')});}const report={...coverage.report(),artifacts};await writeFile(resolve(directory,'audit.json'),JSON.stringify(report,null,2));return report;},
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
