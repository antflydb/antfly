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

import{readFile}from'node:fs/promises';import{createHash}from'node:crypto';import{reconcileTailCounterSnapshots}from'./tail-counter-evidence.mjs';import{attemptIdentity}from'./invocation-accounting.mjs';
const sha=b=>createHash('sha256').update(b).digest('hex');
export async function observerCall(observer,path,body={}, {fetch:request=fetch}={}){
 const token=(await readFile(observer.token_file,'utf8')).trim(),origin=new URL(observer.origin);if(origin.protocol!=='https:'||!origin.hostname.startsWith(observer.id+'.')||!origin.hostname.endsWith('.workers.dev'))throw Error('ObserverOriginInvalid');
 const deadline=Date.now()+30000,readOnly=path==='/health'||path.endsWith('/page');
 for(let attempt=0;;attempt++)try{
  const response=await request(new URL(path,origin),{method:'POST',headers:{Authorization:'Bearer '+token},body:JSON.stringify(body),signal:AbortSignal.timeout(Math.max(1,deadline-Date.now()))});if(!response.ok){await response.body?.cancel();throw Error('ObserverHttpStatus'+response.status);}let bytes=0,parts=[];for await(const part of response.body){bytes+=part.byteLength;if(bytes>5*1024*1024)throw Error('ObserverResponseBudgetExceeded');parts.push(part);}return JSON.parse(Buffer.concat(parts));
 }catch(error){const transport=error instanceof TypeError&&error.message==='fetch failed'||['ECONNRESET','EAI_AGAIN','UND_ERR_SOCKET','UND_ERR_CONNECT_TIMEOUT'].includes(error.cause?.code);if(!readOnly||!transport||attempt>=2||Date.now()>=deadline)throw error;await new Promise(ok=>setTimeout(ok,100));}

}
export async function collectObserverSource(observer,scope){
 const start=Date.now(),objects=[],seen=new Set(),cursors=new Set();let cursor=null,pages=0,bytes=0;
 do{if(++pages>2000||Date.now()-start>120000)throw Error('ObserverSnapshotBudgetExceeded');const page=await observerCall(observer,'/source/'+scope.source+'/page',cursor?{cursor}:{});if(page.capture_id!==scope.capture_id||page.collector_version!==observer.version||!Array.isArray(page.objects)||page.objects.length>32)throw Error('ObserverSnapshotScopeInvalid');for(const o of page.objects){if(typeof o.key!=='string'||!o.key.startsWith('probe-v1/'+scope.capture_id+'/')||seen.has(o.key)||objects.length&&objects.at(-1).key>=o.key)throw Error('ObserverSnapshotObjectInvalid');seen.add(o.key);bytes+=Buffer.byteLength(JSON.stringify(o));if(bytes>64*1024*1024||objects.length>=20000)throw Error('ObserverSnapshotBudgetExceeded');objects.push(o);}cursor=page.cursor;if(cursor!==null){if(typeof cursor!=='string'||!cursor||cursors.has(cursor))throw Error('ObserverCursorInvalid');cursors.add(cursor);}}while(cursor!==null);
 return{qualification:false,capture_id:scope.capture_id,collector_version:observer.version,snapshot_started_at:start,snapshot_finished_at:Date.now(),pages,bytes,objects,objects_sha256:sha(JSON.stringify(objects))};
}
export async function reconcileObserverSource(first,second,{source,captureId,version,counterVersion,witnesses,ledger}={}){
 const failures=[],objects=second.objects??[],rows=objects.flatMap(o=>o.value?.rows??[]),records=objects.filter(o=>o.value?.source==='provider-tail-dispatch').flatMap(o=>o.value.records??[]),witness=hash=>records.filter(r=>r.attempt_id_sha256===hash),before=witness(witnesses?.before),after=witness(witnesses?.after);
 if(before.length!==1||after.length!==1||before[0].timestamp>=after[0].timestamp||after[0].timestamp-before[0].timestamp>3600000)return{qualification:false,complete:false,failures:['ObserverBoundaryWitnessMissing']};
 const timeframe={from:before[0].timestamp+1,to:after[0].timestamp-1},roots=new Map(),validSha=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
 for(const row of rows){if(row.script!==source)failures.push('ObserverSourceMismatch');if(row.start<timeframe.from||row.start>timeframe.to)continue;if(row.dropped||row.warning&&(row.terminal||row.warning_type!=='span_not_ended'))failures.push('ObserverTelemetryIncomplete');if(!row.terminal)continue;
  if(![row.invocation_id_sha256,row.trace_id_sha256,row.span_id_sha256].every(validSha)||row.version_id!==version||!Number.isSafeInteger(row.start)||!Number.isSafeInteger(row.end)||row.end<row.start||row.handler==='unknown'||!['ok','exception','canceled','exceededCpu','exceededMemory'].includes(row.outcome)||!Number.isFinite(row.event?.cpuTime)||!Number.isFinite(row.event?.wallTime))failures.push('ObserverTerminalInvalid');
  const prior=roots.get(row.invocation_id_sha256);if(prior&&JSON.stringify(prior)!==JSON.stringify(row))failures.push('ObserverIdentityConflict');roots.set(row.invocation_id_sha256,row);
 }
 if(objects.some(o=>o.value?.rejection))failures.push('ObserverIngestionRejected');
 const windows=[];for(let from=timeframe.from;from<=timeframe.to;from+=60000){const to=Math.min(timeframe.to,from+59999);if(to<=from){failures.push('ObserverWindowTooShort');break;}const proof=await reconcileTailCounterSnapshots(first,second,{source,version,counterVersion,captureId,timeframe:{from,to},witnesses});windows.push({timeframe:{from,to},...proof});failures.push(...proof.failures);}
 const selected=records.filter(r=>r.timestamp>=timeframe.from&&r.timestamp<=timeframe.to),resources=selected.flatMap(r=>[r.event]).reduce((n,e)=>n+(e.outcome==='exceededMemory'||e.outcome==='exceededCpu'?1:0),0);
 // The strict native verifier retains recovered resource failures as well as
 // terminal outcomes. Recompute each window instead of trusting stored flags.
 const{reconcileTailCounter}=await import('./tail-counter-evidence.mjs');let resourceFailures=0;for(const w of windows){const p=reconcileTailCounter(objects,rows,{source,version,counterVersion,captureId,timeframe:w.timeframe,settledAt:second.snapshot_finished_at,witnesses});resourceFailures+=p.resource_failures??0;}
 if(resourceFailures||resources)failures.push('ObserverResourceFailure');
 let attempts=null;if(ledger){const byAttempt=new Map();for(const r of roots.values()){if(r.handler!=='fetch'||!validSha(r.attempt_id_sha256)||byAttempt.has(r.attempt_id_sha256))failures.push('ObserverHttpIdentityInvalid');byAttempt.set(r.attempt_id_sha256,r);}if(ledger.schema!==1||ledger.overflow||!Array.isArray(ledger.attempts))failures.push('ObserverLedgerIncomplete');else{const ids=new Set();for(const a of ledger.attempts){const id=attemptIdentity(a.id);if(!id||ids.has(id)||a.finished_at==null||!byAttempt.has(id))failures.push('ObserverPhysicalAttemptMissing');ids.add(id);if(a.status>=200&&a.status<300&&a.version_id!==version)failures.push('ObserverResponseVersionMismatch');}if(byAttempt.size!==ids.size||[...byAttempt.keys()].some(k=>!ids.has(k)))failures.push('ObserverUnexpectedHttpInvocation');attempts=ledger.attempts.length;}}
 return{schema:1,qualification:false,source,timeframe,terminals:roots.size,native_terminals:selected.length,physical_attempts:attempts,outcomes:Object.fromEntries(['ok','exception','canceled','exceededCpu','exceededMemory'].map(outcome=>[outcome,[...roots.values()].filter(r=>r.outcome===outcome).length])),resource_failures:resourceFailures,windows,complete:failures.length===0,failures:[...new Set(failures)]};
}
