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

// Replay a saved full source catalog against identical immutable proof objects.
// Network and binding delays are injected independently; no cloud calls or gate
// claims. Legacy and pipelined retrieval share the same current bounded reader.
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import reader from './paired-evidence-reader.mjs';
import {collectObserverSource} from './paired-observer-client.mjs';
import {readCanonicalSnapshot} from './canonical-window-client.mjs';
import {canonicalLayout,proofPrefix} from './canonical-windows.mjs';
import {diagnosticReadResponses} from './diagnostic-memory.mjs';
import {Bucket} from '../test/object-fixture.mjs';
const arg=(name,fallback)=>process.argv.find(a=>a.startsWith('--'+name+'='))?.slice(name.length+3)??fallback;
const reportPath=arg('report'),observerPath=arg('observer');if(!reportPath||!observerPath)throw Error('ReportAndObserverRequired');
const rtt=Number(arg('rtt','50')),binding=Number(arg('binding','20')),reverse=arg('order','control-first')==='candidate-first';if(![rtt,binding].every(n=>Number.isInteger(n)&&n>=0&&n<=200))throw Error('InvalidInjectedDelay');
const out=resolve(arg('out','dist/qualification/evidence-retrieval-'+Date.now()));await mkdir(out,{recursive:true});
const hash=b=>createHash('sha256').update(b).digest('hex'),raw=await readFile(reportPath),rehearsal=JSON.parse(raw),observer=JSON.parse(await readFile(observerPath)),capture=rehearsal.captures.find(c=>c.source===(arg('source')??rehearsal.captures[0]?.source));if(!capture)throw Error('SourceCaptureRequired');
const catalogBytes=await readFile(capture.path);if(hash(catalogBytes)!==capture.sha256)throw Error('SourceCatalogChanged');const catalog=JSON.parse(catalogBytes),scope=observer.scopes.find(s=>s.source===capture.source);if(!scope||scope.capture_id!==catalog.capture_id)throw Error('SourceScopeMismatch');
const objects=new Map();for(const entry of catalog.windows)for(const object of(await readCanonicalSnapshot(entry)).objects){if(objects.has(object.key)&&JSON.stringify(objects.get(object.key))!==JSON.stringify(object.value))throw Error('SourceObjectChanged');objects.set(object.key,object.value);}
const witnesses=Object.fromEntries(['before','after'].map(k=>[k,rehearsal.witnesses[k][scope.source].attempt_id_sha256]));
objects.set(proofPrefix(scope.capture_id)+'LAYOUT.json',{schema:1,evidence_layout:canonicalLayout,capture_id:scope.capture_id,producer:scope.source});
for(const witness of Object.values(witnesses)){
 const matches=[...objects].flatMap(([key,value])=>(value.records??[]).filter(r=>r.attempt_id_sha256===witness).map(row=>({key,row})));if(matches.length!==1)throw Error('SourceWitnessInvalid');const{key,row}=matches[0];
 objects.set(proofPrefix(scope.capture_id)+'witnesses/'+witness+'/replay.json',{schema:1,evidence_layout:canonicalLayout,capture_id:scope.capture_id,producer:scope.source,attempt_id_sha256:witness,timestamp:row.timestamp,counter_key:key});
}
const fixtureHash=hash(JSON.stringify([...objects].sort(([a],[b])=>a.localeCompare(b)))),sleep=ms=>new Promise(ok=>setTimeout(ok,ms));
const result={qualification:false,kind:'Local immutable evidence replay with injected network/binding latency; not a hosted qualification',source_revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),dirty:!!execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim(),arguments:process.argv.slice(2),original_report_sha256:hash(raw),source:scope.source,fixture_sha256:fixtureHash,fixture_objects:objects.size,rtt_ms:rtt,binding_ms:binding,page_limits:{objects:32,bytes:4*1024*1024,decoded_pages:1,client_requests:4},cases:[]};
await writeFile(resolve(out,'experiment.mjs'),await readFile(new URL(import.meta.url)));
for(const optimized of(reverse?[true,false]:[false,true])){
 const bucket=new Bucket();for(const[key,value]of objects)await bucket.put(key,JSON.stringify(value));
 const counts={get:0,list:0,head:0,http:0,window_objects:0,peak_requests:0};let active=0;for(const op of['get','list','head']){const original=bucket[op].bind(bucket);bucket[op]=async(...args)=>{counts[op]++;if(binding)await sleep(binding);return original(...args);};}
 const env={LOGS:bucket,API_TOKEN:'local-evidence-replay',WINDOWED_EVIDENCE:'true',WORKER_VERSION:{id:catalog.collector_version},SOURCE_SCOPES:JSON.stringify(observer.scopes),OBSERVATION_CAPTURE_ID:observer.observation_capture_id};
 const dir=resolve(out,optimized?'pipelined':'legacy');await mkdir(dir);await writeFile(resolve(dir,'token'),'local-evidence-replay',{mode:0o600});
 const run={...observer,dir,token_file:resolve(dir,'token')},previous=globalThis.fetch;const started=performance.now();
 try{
  globalThis.fetch=async(url,options)=>{counts.http++;counts.peak_requests=Math.max(counts.peak_requests,++active);if(new URL(url).pathname.endsWith('/window-objects'))counts.window_objects++;try{const response=await reader.fetch(new Request(url,options),env),body=await response.text();let text=body;if(!optimized&&new URL(url).pathname.endsWith('/layout')){const value=JSON.parse(body);delete value.capabilities;text=JSON.stringify(value);}if(rtt)await sleep(rtt);return new Response(text,{status:response.status,headers:response.headers});}finally{active--;}};
  const snapshot=await collectObserverSource(run,scope,{witnesses});if(JSON.stringify(snapshot.timeframe)!==JSON.stringify(catalog.timeframe))throw Error('ReplayBoundsChanged');
  let verified=0;for(const entry of snapshot.windows){const window=await readCanonicalSnapshot(entry);for(const o of window.objects){if(JSON.stringify(o.value)!==JSON.stringify(objects.get(o.key)))throw Error('ReplayBodyChanged');verified++;}}
  if(active!==0||diagnosticReadResponses.report().active!==0)throw Error('ReplayUnsettled');
  const row={profile:optimized?'pipelined':'legacy',elapsed_ms:performance.now()-started,windows:snapshot.windows.length,verified_objects:verified,counts,reader_admission:diagnosticReadResponses.report()};result.cases.push(row);await writeFile(resolve(out,'report.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(row));
 }finally{globalThis.fetch=previous;}
}
console.log(JSON.stringify({directory:out,qualification:false}));
