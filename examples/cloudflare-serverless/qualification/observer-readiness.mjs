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

// Prove that both independent telemetry paths are active before database setup.
// Every failed attempt remains in the report. Resource failures are terminal;
// an ordinary startup population gap may trigger one fresh bounded attempt.
import{writeFile,mkdir}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';
import{observerCall,collectObserverSource,reconcileObserverSource}from'./paired-observer-client.mjs';import{readCanonicalSnapshot}from'./canonical-window-client.mjs';
const sha=b=>createHash('sha256').update(b).digest('hex');
export async function observerReadiness(observer,{versions,dir,signal,call=observerCall,collect=collectObserverSource,reconcile=reconcileObserverSource,persist=async()=>{},clock=Date.now,sleep=ms=>new Promise(ok=>setTimeout(ok,ms)),maxAttempts=2,timeoutMs=300000}={}){
 if(observer.scopes?.length!==8||!versions||!dir||!Number.isInteger(maxAttempts)||maxAttempts<1||maxAttempts>2||!Number.isInteger(timeoutMs)||timeoutMs<1||timeoutMs>300000)throw Error('ObserverReadinessScopeInvalid');await mkdir(dir,{recursive:true,mode:0o700});
 const report={schema:1,qualification:false,kind:'Preflight only; not workload or physical cold qualification',started_at:clock(),limits:{attempts:maxAttempts,attempt_ms:timeoutMs,closure_ms:60000,separation_ms:30000},attempts:[],passed:false};
 const save=async()=>{await writeFile(resolve(dir,'report.json'),JSON.stringify(report,null,2));await persist(report);};
 for(let attempt=0;attempt<maxAttempts;attempt++){
  const current={attempt,started_at:clock(),probes:{},captures:[],diagnostics:[],failure:null};report.attempts.push(current);const captureSignal=AbortSignal.any([AbortSignal.timeout(timeoutMs),...signal?[signal]:[]]),check=()=>captureSignal.throwIfAborted(),wait=async ms=>{const end=clock()+ms;while(clock()<end){check();await sleep(Math.min(1000,end-clock()));}};
  try{
   for(const scope of observer.scopes){const probes={};current.probes[scope.source]=probes;for(const phase of['before','sample','after']){check();const row=await call(observer,'/source/'+scope.source+'/counter-witness',{id:crypto.randomUUID()},{signal:captureSignal});if(row.producer!==scope.source||![401,404].includes(row.status)||!/^[a-f0-9]{64}$/.test(row.attempt_id_sha256??''))throw Error('ObserverReadinessWitnessInvalid');probes[phase]=row;}await save();}
   await wait(60000);const snapshots=[new Map(),new Map()];
   for(let pass=0;pass<2;pass++){
    if(pass)await wait(30000);
    for(const scope of observer.scopes){check();const probes=current.probes[scope.source],witnesses={before:probes.before.attempt_id_sha256,after:probes.after.attempt_id_sha256},snapshot=await collect(observer,scope,{signal:captureSignal,witnesses}),body=JSON.stringify(snapshot),path=resolve(dir,scope.source+'-'+attempt+'-'+pass+'.json');await writeFile(path,body,{flag:'wx',mode:0o600});snapshots[pass].set(scope.source,snapshot);current.captures.push({source:scope.source,pass,path,sha256:sha(body),bytes:Buffer.byteLength(body)});
     if(pass){const first=snapshots[0].get(scope.source),proof=await reconcile(first,snapshot,{source:scope.source,captureId:scope.capture_id,version:versions[scope.source],counterVersion:observer.version,witnesses}),positive=await Promise.all([first,snapshot].map(s=>readinessSample(s,probes.sample.attempt_id_sha256)));current.diagnostics.push({source:scope.source,proof,positive,complete:proof.complete&&positive.every(p=>p.native===1&&p.canonical===1)});}await save();
    }
   }
   const health=await call(observer,'/health',{}, {signal:captureSignal});current.health=health;
   if(observer.observed_resource_failures?.length||current.diagnostics.some(d=>d.proof.resource_failures>0||d.proof.failures?.some(f=>/ResourceFailure/.test(f))))throw Error('ObserverReadinessResourceFailure');
   current.passed=current.diagnostics.length===8&&current.diagnostics.every(d=>d.complete)&&health.rejections_present===false;
   if(current.passed){report.passed=true;report.finished_at=clock();await save();return report;}
   current.failure='ObserverReadinessUnproven';
  }catch(error){current.failure=String(error);if(signal?.aborted||observer.observed_resource_failures?.length||/SQLITE_NOMEM|OutOfMemory|MemoryLimitExceeded|CPULimitExceeded|ObserverReadinessResourceFailure/.test(String(error))){report.finished_at=clock();await save();return report;}}
  current.finished_at=clock();await save();
 }
 report.finished_at=clock();await save();return report;
}
async function readinessSample(snapshot,id){
 const objects=new Map();if(snapshot.windows){for(const entry of snapshot.windows)for(const object of(await readCanonicalSnapshot(entry)).objects){const old=objects.get(object.key);if(old&&JSON.stringify(old)!==JSON.stringify(object))throw Error('ObserverSnapshotChanged');objects.set(object.key,object);}}else for(const object of snapshot.objects??[])objects.set(object.key,object);
 const roots=new Set();let native=0;for(const object of objects.values()){for(const row of object.value?.records??[])if(row.attempt_id_sha256===id)native++;for(const row of object.value?.rows??[])if(row.terminal&&row.attempt_id_sha256===id)roots.add(row.invocation_id_sha256);}
 return{native,canonical:roots.size};
}
