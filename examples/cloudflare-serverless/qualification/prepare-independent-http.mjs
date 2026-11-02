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

import{observerScopes}from'./paired-observer.mjs';
import{readFile,writeFile,mkdir}from'node:fs/promises';import{resolve,join}from'node:path';import{execFileSync}from'node:child_process';import{assertObservationRun,verifyObservationRun}from'./observation-runs.mjs';
const[action,baselinePath,candidatePath,output]=process.argv.slice(2);if(!['pair','plan'].includes(action)||!baselinePath||!candidatePath||!output)throw Error('UsageIndependentPairOrPlanBaselineCandidateOutput');
const runs=await Promise.all([baselinePath,candidatePath].map(async p=>JSON.parse(await readFile(resolve(p)))));for(const r of runs){assertObservationRun(r);if(r.deleted||!r.deployed||(await verifyObservationRun(r)).length)throw Error('IndependentSealedPairRequired');}if(runs[0].account!==runs[1].account||runs[0].id===runs[1].id||runs[0].profile!=='approved-control'||runs[1].profile!=='durable-candidate')throw Error('IndependentPairMismatch');
let value;if(action==='pair')value={schema:1,kind:'independent-http-pair',account:runs[0].account,baseline:resolve(baselinePath),candidate:resolve(candidatePath)};
else{if(runs.some(r=>!r.seeded||!r.token_installed||!r.receipts||!r.versions?.app))throw Error('IndependentPreparedCorpusRequired');const documents=runs[0].documents??64;if((runs[1].documents??64)!==documents)throw Error('IndependentCorpusMismatch');value={schema:1,kind:'independent-http-plan',execution_id:crypto.randomUUID(),runner_source_revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),created_at:Date.now(),documents,reads:256,rps:32,write_every:16,max_in_flight:64,max_dispatch_lag_ms:25,...Object.fromEntries(runs.map((r,i)=>[i?'candidate':'baseline',{run:r.id,origin:r.origin,versions:r.versions,receipts:r.receipts,source:r.source,module_and_bundle_hashes:r.seal}]))};}
if(action==='plan'){for(const[i,r]of runs.entries()){const config=JSON.parse(await readFile(r.services.find(s=>s.role==='app').config));value[i?'candidate':'baseline'].require_profile=config.vars?.QUALIFICATION_PHASE_PROFILE==='true';}const observerPath=process.argv.find(a=>a.startsWith('--observer='))?.slice(11);if(observerPath){const o=JSON.parse(await readFile(resolve(observerPath)));if(o.account!==runs[0].account||o.deleted||!o.attached_at||!o.receiver_version||o.evidence_layout!=='window-v1'||o.scopes?.some(s=>!runs.some(r=>r.id===s.run)))throw Error('IndependentAttachedObserverRequired');observerScopes({SOURCE_LAYOUT:o.source_layout,SOURCE_SCOPES:JSON.stringify(o.scopes)});value.observer=Object.fromEntries(['id','origin','reader_id','reader_origin','version','reader_version','source_layout','evidence_layout','observation_capture_id','scopes'].filter(k=>o[k]!=null).map(k=>[k,o[k]]));}}
const path=resolve(output);await mkdir(resolve(path,'..'),{recursive:true});await writeFile(path,JSON.stringify(value,null,2),{flag:'wx',mode:0o600});console.log(JSON.stringify({path,kind:value.kind,credentials_included:false}));
