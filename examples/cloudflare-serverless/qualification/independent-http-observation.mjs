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

import{mkdir}from'node:fs/promises';import{resolve,relative,isAbsolute,sep}from'node:path';import{observerReadiness}from'./observer-readiness.mjs';import{observerCall,collectObserverSource,reconcileObserverSource}from'./paired-observer-client.mjs';import{observerScopes}from'./paired-observer.mjs';
const pause=async(ms,signal)=>{const end=Date.now()+ms;while(Date.now()<end){signal?.throwIfAborted();await new Promise(r=>setTimeout(r,Math.min(1000,end-Date.now())));}};
export function portableSnapshots(snapshots,root,{restore=false}={}){const value=structuredClone(snapshots);for(const snapshot of Object.values(value))for(const window of snapshot?.windows??[]){if(restore){if(typeof window.relative_path!=='string'||isAbsolute(window.relative_path))throw Error('IndependentEvidencePathInvalid');const path=resolve(root,window.relative_path);if(!path.startsWith(resolve(root)+sep)||!path.endsWith('.json'))throw Error('IndependentEvidencePathInvalid');window.path=path;}else{const path=relative(root,window.path);if(path.startsWith('..')||isAbsolute(path))throw Error('IndependentEvidencePathInvalid');window.relative_path=path;}}return value;}
export async function prepareIndependentObservation(observer,versions,dir,{signal}={}){
 observerScopes({SOURCE_SCOPES:JSON.stringify(observer.scopes),SOURCE_LAYOUT:observer.source_layout});if(observer.evidence_layout!=='window-v1')throw Error('IndependentWindowedObserverRequired');await mkdir(dir,{recursive:true});const readiness=await observerReadiness(observer,{versions,dir:resolve(dir,'readiness'),signal,maxAttempts:1});if(!readiness.passed)throw Error('IndependentObserverReadinessFailed');const before={};for(const scope of observer.scopes){signal?.throwIfAborted();const w=await observerCall(observer,'/source/'+scope.source+'/counter-witness',{id:crypto.randomUUID()},{signal});if(w.producer!==scope.source||![401,404].includes(w.status))throw Error('IndependentWitnessInvalid');before[scope.source]=w.attempt_id_sha256;}return{readiness,before,started_at:Date.now()};
}
export async function finishIndependentObservation(observer,context,reports,{signal,maxPasses=20}={}){
 const witnesses={};for(const scope of observer.scopes){const w=await observerCall(observer,'/source/'+scope.source+'/counter-witness',{id:crypto.randomUUID()},{signal});if(w.producer!==scope.source||![401,404].includes(w.status))throw Error('IndependentWitnessInvalid');witnesses[scope.source]={before:context.before[scope.source],after:w.attempt_id_sha256};}
 const finished_at=Date.now();await pause(60000,signal);let previous={},last={},diagnostics=[];
 for(let pass=0;pass<maxPasses;pass++){
  last={};diagnostics=[];for(const scope of observer.scopes){const snapshot=await collectObserverSource(observer,scope,{signal,witnesses:witnesses[scope.source]});last[scope.source]=snapshot;if(previous[scope.source]){const report=reports.find(r=>r.run===scope.run);if(!report)throw Error('IndependentSourceMismatch');const proof=await reconcileObserverSource(previous[scope.source],snapshot,{source:scope.source,captureId:scope.capture_id,version:report.source_versions[scope.role],counterVersion:observer.version,witnesses:witnesses[scope.source],...(scope.role==='app'?{ledger:report.invocation_ledger}:{})});diagnostics.push({role:scope.role,...proof});}}
  const health=await observerCall(observer,'/health',{}, {signal});if(diagnostics.length===8&&diagnostics.every(d=>d.complete)&&health.rejections_present===false&&!observer.observed_resource_failures?.length)return{qualification:false,complete:true,started_at:context.started_at,finished_at,witnesses,first:previous,second:last,diagnostics,health};if(health.rejections_present===true||diagnostics.some(d=>d.resource_failures>0)||observer.observed_resource_failures?.length)return{qualification:false,complete:false,started_at:context.started_at,finished_at,witnesses,first:previous,second:last,diagnostics,health};if(pass+1<maxPasses){previous=last;await pause(30000,signal);}
 }
 return{qualification:false,complete:false,started_at:context.started_at,finished_at,witnesses,first:previous,second:last,diagnostics};
}
