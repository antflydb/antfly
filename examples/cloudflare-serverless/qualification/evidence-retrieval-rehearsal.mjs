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

// Offline full-source replay in actual workerd. Frozen input is stable by
// construction: this verifies retrieval/identity rules and throughput, never
// live delivery completeness or the original failed rehearsal.
import{readFile,writeFile,mkdir}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import assert from'node:assert/strict';import{build}from'esbuild';import{Miniflare,convertV4MiniflareOptions}from'miniflare';
import{collectObserverSource,reconcileObserverSource}from'./paired-observer-client.mjs';import{readCanonicalSnapshot}from'./canonical-window-client.mjs';import{canonicalLayout,proofPrefix}from'./canonical-windows.mjs';
const arg=(k,f)=>process.argv.find(a=>a.startsWith('--'+k+'='))?.slice(k.length+3)??f,sha=b=>createHash('sha256').update(b).digest('hex'),out=resolve(arg('out','dist/qualification/evidence-rehearsal-'+Date.now())),delay=Number(arg('binding','20')),rtt=Number(arg('rtt','50'));if(![delay,rtt].every(n=>Number.isInteger(n)&&n>=0&&n<=200)||!arg('report')||!arg('observer'))throw Error('ReplayArgumentsInvalid');await mkdir(out,{recursive:true});
const raw=await readFile(arg('report')),original=JSON.parse(raw),old=JSON.parse(await readFile(arg('observer'))),late=JSON.parse(await readFile(arg('postmortem'))),entries=[...original.captures.filter(c=>c.pass===0),...late.captures],objects=new Map(),specs=[];
if(old.scopes.length!==8||new Set(entries.map(e=>e.source)).size!==8)throw Error('EightSourceFixturesRequired');
for(const scope of old.scopes){
 const entry=entries.find(e=>e.source===scope.source);if(!entry)throw Error('SourceFixtureMissing');const body=await readFile(entry.path);if(sha(body)!==entry.sha256)throw Error('SourceCatalogChanged');const catalog=JSON.parse(body),local=new Map();
 for(const window of catalog.windows)for(const object of(await readCanonicalSnapshot(window)).objects){if(local.has(object.key)&&JSON.stringify(local.get(object.key))!==JSON.stringify(object.value))throw Error('SourceObjectChanged');local.set(object.key,object.value);}
 const witnesses=Object.fromEntries(['before','after'].map(k=>[k,original.witnesses[k][scope.source].attempt_id_sha256])),counterVersions=new Set([...local.values()].filter(v=>v.source==='provider-tail-dispatch').map(v=>v.collector_version));if(counterVersions.size!==1)throw Error('CounterVersionMismatch');
 local.set(proofPrefix(scope.capture_id)+'LAYOUT.json',{schema:1,evidence_layout:canonicalLayout,capture_id:scope.capture_id,producer:scope.source});
 for(const hash of Object.values(witnesses)){const matches=[...local].flatMap(([key,v])=>(v.records??[]).filter(r=>r.attempt_id_sha256===hash).map(row=>({key,row})));if(matches.length!==1)throw Error('WitnessInvalid');const{key,row}=matches[0];local.set(proofPrefix(scope.capture_id)+'witnesses/'+hash+'/replay.json',{schema:1,evidence_layout:canonicalLayout,capture_id:scope.capture_id,producer:scope.source,attempt_id_sha256:hash,timestamp:row.timestamp,counter_key:key});}
 for(const[key,value]of local){if(objects.has(key))throw Error('SourceScopeCollision');objects.set(key,value);}
 const side=original.sides.find(s=>s.run===scope.run);if(!side)throw Error('SideMissing');specs.push({scope,witnesses,version:side.source_versions[scope.role],counterVersion:[...counterVersions][0],ledger:scope.role==='app'?side.invocation_ledger:undefined});
}
const token='local-evidence-rehearsal',version=crypto.randomUUID(),id='antfly-1278-observe-localreplay',readerId=id+'-reader',origin='https://'+readerId+'.example.workers.dev',tokenFile=resolve(out,'token');await writeFile(tokenFile,token,{mode:0o600});
const code="import reader from'./qualification/paired-evidence-reader.mjs';import{diagnosticReadResponses}from'./qualification/diagnostic-memory.mjs';export default{fetch(request,env){if(new URL(request.url).pathname==='/metrics')return Response.json(diagnosticReadResponses.report());const storage=env.LOGS,LOGS=new Proxy(storage,{get(target,key){const value=target[key];if(typeof value!=='function')return value;return async(...args)=>{if(['get','list','head'].includes(key)&&env.DELAY)await new Promise(ok=>setTimeout(ok,env.DELAY));return value.apply(target,args);};}});return reader.fetch(request,{...env,LOGS});}};",bundle=await build({stdin:{contents:code,resolveDir:process.cwd()},bundle:true,write:false,format:'esm',platform:'neutral'});await writeFile(resolve(out,'reader.mjs'),bundle.outputFiles[0].text);
const mf=new Miniflare(convertV4MiniflareOptions({name:'reader',modules:[{type:'ESModule',path:resolve(out,'reader.mjs'),contents:bundle.outputFiles[0].text}],compatibilityDate:'2026-10-23',bindings:{API_TOKEN:token,SOURCE_SCOPES:JSON.stringify(old.scopes),WINDOWED_EVIDENCE:'true',WORKER_VERSION:{id:version},OBSERVATION_CAPTURE_ID:old.observation_capture_id,DELAY:delay},r2Buckets:{LOGS:'replay-proof'}})),previous=globalThis.fetch;
const script=await readFile(new URL(import.meta.url)),report={qualification:false,kind:'Actual-workerd offline eight-source replay; frozen stable fixtures are not live delivery evidence',source_revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),dirty:!!execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim(),arguments:process.argv.slice(2),experiment_sha256:sha(script),original_report_sha256:sha(raw),fixture_sha256:sha(JSON.stringify([...objects].sort(([a],[b])=>a.localeCompare(b)))),fixture_objects:objects.size,binding_ms:delay,rtt_ms:rtt,aggregate_deadline_ms:600000,observation_separation_ms:31000,captures:[],diagnostics:[],fatal:null};await writeFile(resolve(out,'experiment.mjs'),script);const save=()=>writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));
try{
 const bucket=await mf.getR2Bucket('LOGS'),items=[...objects];for(let from=0;from<items.length;from+=32){await Promise.all(items.slice(from,from+32).map(([key,v])=>bucket.put(key,JSON.stringify(v))));if(from%1024===0)console.log(JSON.stringify({seeded:Math.min(from+32,items.length),total:items.length}));}
 let active=0,peak=0,calls=0;globalThis.fetch=async(url,options)=>{const target=new URL(url);if(target.origin!==origin){if(['127.0.0.1','localhost','[::1]'].includes(target.hostname))return previous(url,options);throw Error('RemoteFetchForbidden');}calls++;peak=Math.max(peak,++active);try{const response=await mf.dispatchFetch(url,options),body=await response.arrayBuffer();if(rtt)await new Promise(ok=>setTimeout(ok,rtt));return new Response(body,{status:response.status,headers:response.headers});}finally{active--;}};
 const observer={id,reader_id:readerId,reader_origin:origin,reader_version:version,dir:out,token_file:tokenFile,evidence_layout:canonicalLayout},signal=AbortSignal.timeout(600000),snapshots=[new Map(),new Map()];report.started_at=Date.now();await save();
 for(let pass=0;pass<2;pass++){
  if(pass)await new Promise(ok=>setTimeout(ok,31000));
  for(const spec of specs){const start=performance.now(),snapshot=await collectObserverSource(observer,spec.scope,{witnesses:spec.witnesses,signal}),path=resolve(out,spec.scope.source+'-'+pass+'.json');await writeFile(path,JSON.stringify(snapshot));snapshots[pass].set(spec.scope.source,snapshot);const row={source:spec.scope.source,pass,elapsed_ms:performance.now()-start,windows:snapshot.windows.length,path};report.captures.push(row);await save();console.log(JSON.stringify(row));}
 }
 for(const spec of specs)report.diagnostics.push(await reconcileObserverSource(snapshots[0].get(spec.scope.source),snapshots[1].get(spec.scope.source),{source:spec.scope.source,captureId:spec.scope.capture_id,version:spec.version,counterVersion:spec.counterVersion,witnesses:spec.witnesses,ledger:spec.ledger}));
 report.finished_at=Date.now();report.elapsed_ms=report.finished_at-report.started_at;report.observed_resource_failures=observer.observed_resource_failures??[];report.http={calls,peak,active};report.reader_admission=await(await mf.dispatchFetch('https://reader.invalid/metrics')).json();report.passed=report.diagnostics.length===8&&report.diagnostics.every(d=>d.complete)&&!report.observed_resource_failures.length&&active===0&&report.reader_admission.peak_active===1&&report.reader_admission.active===0;await save();console.log(JSON.stringify({passed:report.passed,elapsed_ms:report.elapsed_ms,diagnostics:report.diagnostics.map(d=>({source:d.source,complete:d.complete,failures:d.failures})),http:report.http,reader_admission:report.reader_admission}));if(!report.passed)process.exitCode=1;
}catch(error){report.fatal=String(error);await save();throw error;}finally{globalThis.fetch=previous;await mf.dispose();}
