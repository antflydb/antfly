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

import {createHash} from 'node:crypto';
import{reconcileCounterPopulations}from'./independent-counters.mjs';
const hash=value=>createHash('sha256').update(value).digest('hex');
export function rayIdentity(value){return typeof value==='string'&&/^[a-f0-9]{16,32}(?:-[A-Z]{3})?$/i.test(value)?hash(value.split('-')[0].toLowerCase()):null;}
export function versionIdentity(value){return typeof value==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(value)?value.toLowerCase():null;}
export const invocationTypes=new Set(['fetch','scheduled','alarm','cron','queue','email','tail','rpc','jsrpc','websocket','workflow','unknown']);
export function attemptIdentity(value){return versionIdentity(value)?hash(value.toLowerCase()):null;}
// Lowercase base36 with a non-hex prefix avoids the provider's documented
// UUID/hex and mixed-case base64 redaction heuristics. The entropy is unchanged.
export function attemptParameter(value){const id=versionIdentity(value);return id?'q'+BigInt('0x'+id.replaceAll('-','')).toString(36).padStart(25,'0'):null;}
export function requestAttemptIdentity(value){try{const parameter=new URL(value).searchParams.get('__antfly_qualification_attempt');if(versionIdentity(parameter))return attemptIdentity(parameter);if(!/^q[0-9a-z]{25}$/.test(parameter??''))return null;let number=0n;for(const digit of parameter.slice(1))number=number*36n+BigInt(parseInt(digit,36));if(number>=1n<<128n)return null;const hex=number.toString(16).padStart(32,'0'),id=[hex.slice(0,8),hex.slice(8,12),hex.slice(12,16),hex.slice(16,20),hex.slice(20)].join('-');return attemptParameter(id)===parameter?attemptIdentity(id):null;}catch{return null;}}
// One entry per physical attempt, including HTTP admission retries. Never
// retain bodies, URLs, raw headers or credentials.
export class InvocationLedger{
 constructor({limit=20000,clock=Date.now}={}){if(!Number.isInteger(limit)||limit<1||limit>20000||typeof clock!=="function")throw Error("InvalidInvocationLedgerLimit");this.limit=limit;this.clock=clock;this.rows=[];this.overflow=false;}
 begin({phase,op,tenant}){if(this.rows.length>=this.limit){this.overflow=true;return null;}const row={id:crypto.randomUUID(),phase:/^[a-z][a-z-]{0,31}$/.test(phase)?phase:'other',op:/^[a-z_]{1,32}$/.test(op??'')?op:'other',tenant:['a','b','c','d'].includes(tenant)?tenant:'other',started_at:this.clock(),response_at:null,finished_at:null,status:null,ray_id_sha256:null,version_id:null,transport_error:false};this.rows.push(row);return row;}
 response(row,response){if(!row)return;row.response_at=this.clock();row.status=response.status;row.ray_id_sha256=rayIdentity(response.headers.get('cf-ray'));row.version_id=versionIdentity(response.headers.get('X-Antfly-Version'));const placement=response.headers.get("cf-placement");if(/^(local|remote)-[A-Z]{3}$/.test(placement??""))row.compute_placement=placement;}
 finish(row,{transportError=false}={}){if(row){row.finished_at=this.clock();row.transport_error=transportError;}}
 report(){return{schema:1,overflow:this.overflow,attempts:this.rows.map(row=>({...row}))};}
}
export function reconcileInvocations(run,ledger,windows,{roleCounts,timeframe,ledgerTimeBasis='started_at'}={}){
 const failures=[],observed={},seen=new Map();
 if(!['started_at','response_at'].includes(ledgerTimeBasis))throw Error('InvalidInvocationLedgerTimeBasis');
 const attemptTime=row=>ledgerTimeBasis==='response_at'?(row.response_at??row.finished_at):row.started_at;
 const validRange=range=>Number.isSafeInteger(range?.from)&&Number.isSafeInteger(range?.to)&&range.from>=0&&range.to>=range.from;
 const sameRange=(a,b)=>validRange(a)&&validRange(b)&&a.from===b.from&&a.to===b.to;
 if(!validRange(timeframe))failures.push('InvocationTimeframeMissing');
 if(windows.some(w=>!['app','writer','reader','builder'].includes(w.role)))failures.push('InvocationWindowInvalid');
 if(ledger?.schema!==1||ledger.overflow||!Array.isArray(ledger.attempts))return{qualification:false,complete:false,failures:['InvocationLedgerIncomplete']};
 for(const role of ['app','writer','reader','builder']){
  const rows=[],ranges=[];
  if(!windows.some(w=>w.role===role))failures.push('InvocationRoleMissing');
  for(const window of windows.filter(w=>w.role===role)){
   if(![2,3].includes(window.schema)||!window.reconciled||!validRange(window.timeframe)||!Array.isArray(window.rows)||window.worker!==run.id+(role==='app'?'':'-'+role)){failures.push('InvocationWindowInvalid');continue;}
   ranges.push(window.timeframe);
   for(const row of window.rows){if(!Number.isSafeInteger(row.timestamp)||row.timestamp<window.timeframe.from||row.timestamp>window.timeframe.to||!['event_id_sha256','invocation_id_sha256'].every(k=>/^[a-f0-9]{64}$/.test(row[k]??''))){failures.push('InvocationRowInvalid');continue;}const key=role+'/'+row.event_id_sha256,encoded=JSON.stringify(row);if(seen.has(key)){if(seen.get(key)!==encoded)failures.push('InvocationDuplicateConflict');continue;}seen.set(key,encoded);if(!row.invocation)continue;if(!(run.accepted_versions?.[role]??[run.versions?.[role]]).includes(row.version_id))failures.push('InvocationVersionMismatch');if(!invocationTypes.has(row.event_type)||row.event_type==='unknown')failures.push('InvocationTypeMissing');rows.push(row);}
  }
  if(validRange(timeframe)){let next=timeframe.from;for(const range of ranges.sort((a,b)=>a.from-b.from)){if(range.from<timeframe.from||range.to>timeframe.to||range.from>next)failures.push('InvocationWindowGap');next=Math.max(next,range.to+1);}if(next!==timeframe.to+1)failures.push('InvocationWindowGap');}
  observed[role]={invocations:rows.length,types:Object.fromEntries([...invocationTypes].map(type=>[type,rows.filter(row=>row.event_type===type).length]))};
  if(new Set(rows.map(row=>row.invocation_id_sha256)).size!==rows.length)failures.push('InvocationIdentityRepeated');
  if(roleCounts?.[role]?.populations)failures.push(...reconcileCounterPopulations(run,role,rows,roleCounts[role],timeframe));
  else if(!roleCounts||!Number.isSafeInteger(roleCounts[role]?.count)||roleCounts[role].sample_interval!==1||roleCounts[role].source!=='independent-platform-counter'||roleCounts[role].worker!==run.id+(role==='app'?'':'-'+role)||!sameRange(roleCounts[role].timeframe,timeframe))failures.push('IndependentInvocationCountMissing');else if(roleCounts[role].count!==rows.length)failures.push('InvocationCountMismatch');
  if(role!=='app')continue;
  const http=rows.filter(row=>row.event_type==='fetch'),byAttempt=new Map(),expected=new Set();
  for(const row of http){if(!row.attempt_id_sha256)failures.push('InvocationAttemptIdentityMissing');else{if(byAttempt.has(row.attempt_id_sha256))failures.push('InvocationAttemptRepeated');byAttempt.set(row.attempt_id_sha256,row);}}
  const attemptIds=new Set();
  for(const row of ledger.attempts){if(!validRange(timeframe)||!Number.isSafeInteger(attemptTime(row))||attemptTime(row)<timeframe.from||attemptTime(row)>timeframe.to)failures.push('InvocationAttemptTimeframeMismatch');const id=attemptIdentity(row.id),invocation=byAttempt.get(id);if(!id||attemptIds.has(id))failures.push('InvocationAttemptRepeated');attemptIds.add(id);expected.add(id);
   if(row.status!==null){if(!row.ray_id_sha256)failures.push('AcknowledgedRayMissing');if(!invocation)failures.push('AcknowledgedInvocationMissing');else if(invocation.ray_id_sha256!==row.ray_id_sha256)failures.push('InvocationRayMismatch');if((row.status>=200&&row.status<300||row.version_id)&&row.version_id!==run.version)failures.push('ResponseVersionMismatch');}
   // An unacknowledged attempt can be classified as unobserved only when the
   // independent, unsampled app total also reconciles every stored invocation.
   if(row.transport_error&&!invocation&&(ledgerTimeBasis==='response_at'||!roleCounts?.app))failures.push('InvocationTransportAmbiguous');
  }
  if(http.some(row=>!expected.has(row.attempt_id_sha256)))failures.push('UnexpectedHttpInvocation');
  if(ledger.attempts.some(row=>row.finished_at===null))failures.push('InvocationAttemptUnsettled');
 }
 return{schema:1,qualification:false,ledger_time_basis:ledgerTimeBasis,complete:failures.length===0,failures:[...new Set(failures)],observed,acknowledged_http:ledger.attempts.filter(row=>row.status!==null).length};
}
