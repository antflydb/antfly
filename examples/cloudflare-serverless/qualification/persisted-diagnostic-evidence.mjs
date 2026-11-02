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

import {readFile,stat,realpath} from 'node:fs/promises';
import {resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
import {assertGateRun} from './gate-scope.mjs';
import {DiagnosticCoverage} from './live-diagnostics.mjs';
import {sanitizeTailEvent} from './metrics-tail.mjs';
import {versionIdentity,invocationTypes} from './invocation-accounting.mjs';

const hash=value=>createHash('sha256').update(value).digest('hex');
// This verifies local evidence integrity and repeatability, not unsampled
// platform delivery. evaluate-gate deliberately does not accept these reports.
export async function verifyPersistedProbe(directory,runs){
 try{
  if(runs.length!==2||runs[0].id===runs[1].id||runs[0].account!==runs[1].account)return false;
  for(const run of runs)assertGateRun(run);
  const root=resolve(directory),report=JSON.parse(await readFile(resolve(root,'audit.json')));
  if(report.schema!==1||report.qualification!==false||report.stable!==true||report.error!==null||report.artifacts?.length!==16)return false;
  const {from,to}=report.timeframe??{};
  if(!Number.isSafeInteger(from)||!Number.isSafeInteger(to)||from<0||from>=to||to-from>60000)return false;
  const expected=new Map(runs.flatMap(run=>['app','writer','reader','builder'].flatMap(role=>[0,1].map(pass=>[
   resolve(root,run.id+'-'+role+'-'+pass+'.json'),{run,role,pass,worker:run.id+(role==='app'?'':'-'+role)},
  ]))));
  const snapshots=new Map();let total=0,resources=0,gaps=0,invocationErrors=0;
  for(const artifact of report.artifacts){
   const scope=expected.get(artifact.path);if(!scope)return false;expected.delete(artifact.path);
   if(scope.worker!==artifact.worker||scope.role!==artifact.role||scope.pass!==artifact.pass||!Number.isSafeInteger(artifact.bytes)||artifact.bytes<1||artifact.bytes>64*1024*1024)return false;
   total+=artifact.bytes;if(total>256*1024*1024)return false;
   const info=await stat(artifact.path);if(!info.isFile()||info.size!==artifact.bytes||await realpath(artifact.path)!==artifact.path)return false;
   const bytes=await readFile(artifact.path);if(bytes.length!==artifact.bytes||hash(bytes)!==artifact.sha256)return false;
   const window=JSON.parse(bytes);
   if(![1,2,3].includes(window.schema)||window.qualification!==false||window.reconciled!==true||window.worker!==scope.worker||window.role!==scope.role||!isDeepStrictEqual(window.timeframe,report.timeframe)||!Array.isArray(window.rows)||window.rows.length>20000||window.events!==window.rows.length||window.events!==artifact.events||window.invocations!==artifact.invocations||window.rows_sha256!==hash(JSON.stringify(window.rows)))return false;
   if(!Number.isSafeInteger(window.collected_started_at)||!Number.isSafeInteger(window.collected_finished_at)||window.collected_started_at<to+60000||window.collected_finished_at<window.collected_started_at||window.collected_finished_at-window.collected_started_at>120000)return false;
   const seen=new Set(),invocations=new Set(),coverage=new DiagnosticCoverage([scope.run]);
   for(const row of window.rows){
    const keys=window.schema===1?'event,event_id_sha256,invocation,invocation_id_sha256,timestamp':'attempt_id_sha256,event,event_id_sha256,event_type,invocation,invocation_id_sha256,ray_id_sha256,timestamp,version_id';
    if(Object.keys(row).sort().join(',')!==keys||!Number.isSafeInteger(row.timestamp)||row.timestamp<from||row.timestamp>to||typeof row.invocation!=='boolean'||!isDeepStrictEqual(sanitizeTailEvent(row.event)??{},row.event))return false;
    if(window.schema>=2){for(const id of[row.ray_id_sha256,row.attempt_id_sha256])if(id!==null&&!/^[a-f0-9]{64}$/.test(id))return false;if(row.version_id!==null&&!versionIdentity(row.version_id)||!invocationTypes.has(row.event_type))return false;}
    for(const id of[row.event_id_sha256,row.invocation_id_sha256])if(!/^[a-f0-9]{64}$/.test(id))return false;
    if(seen.has(row.event_id_sha256))return false;seen.add(row.event_id_sha256);
    if(row.invocation){if(invocations.has(row.invocation_id_sha256))return false;invocations.add(row.invocation_id_sha256);}
    coverage.observe(scope.run.id,scope.role,row.event);
   }
   const observation=coverage.report().streams.find(s=>s.run===scope.run.id&&s.role===scope.role);
   const errors=window.rows.filter(row=>row.invocation&&row.event.outcome!=='ok').length;
   if(invocations.size!==window.invocations||observation.resource_failures!==window.resource_failures||observation.gap!==window.gap||errors!==window.invocation_errors)return false;
   resources+=observation.resource_failures;gaps+=Number(observation.gap);invocationErrors+=errors;
   const key=scope.worker,prior=snapshots.get(key);
   if(prior){const first=scope.pass===0?window:prior,second=scope.pass===1?window:prior;if(first.rows_sha256!==second.rows_sha256||second.collected_started_at<first.collected_finished_at+30000)return false;}
   snapshots.set(key,window);
  }
  return expected.size===0&&snapshots.size===8&&resources===report.resource_failures&&gaps===report.gaps&&invocationErrors===report.invocation_errors;
 }catch{return false;}
}
