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

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,readFile,rm} from 'node:fs/promises';
import {resolve} from 'node:path';
import {compactRows,compactEvent,persistCanonicalRows,persistCanonicalTail,windowPrefix,proofPrefix,sha} from '../qualification/canonical-windows.mjs';
import {collectObserverSource,reconcileObserverSource} from '../qualification/paired-observer-client.mjs';
import {persistTailDelivery} from '../qualification/tail-counter.mjs';
import {attemptIdentity} from '../qualification/invocation-accounting.mjs';
import reader from '../qualification/paired-evidence-reader.mjs';
import {Bucket} from './object-fixture.mjs';
const source='antfly-1278-g-windows-1-b',version=crypto.randomUUID(),counterVersion=crypto.randomUUID(),readerVersion=crypto.randomUUID(),captureId=crypto.randomUUID(),witnesses={before:'b'.repeat(64),after:'c'.repeat(64)},attempt=crypto.randomUUID();
const row=(start=1500)=>({signal:'traces',script:source,version_id:version,start,end:start+20,handler:'fetch',outcome:'ok',terminal:true,invocation_id_sha256:'a'.repeat(64),trace_id_sha256:'d'.repeat(64),span_id_sha256:'e'.repeat(64),attempt_id_sha256:attemptIdentity(attempt),event:{outcome:'ok',cpuTime:1,wallTime:20},attribute_schema:['verbose']});
function fixture(){const LOGS=new Bucket(),ARCHIVE=new Bucket(),scopes=['b','c'].flatMap(side=>['','-writer','-reader','-builder'].map(role=>({source:'antfly-1278-g-windows-1-'+side+role,capture_id:side==='b'&&!role?captureId:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i}));return{LOGS,ARCHIVE,SOURCE_WORKER:source,CAPTURE_ID:captureId,WINDOWED_EVIDENCE:'true',WORKER_VERSION:{id:counterVersion},SOURCE_SCOPES:JSON.stringify(scopes),API_TOKEN:'local-window-only',scopes};}
const envelope=rows=>({schema:1,qualification:false,signal:'traces',collector_version:counterVersion,received_at:Date.now(),raw_batch_sha256:'f'.repeat(64),rows});
const records=(event={})=>[1000,1500,2000].map((timestamp,i)=>({script:source,timestamp,version_id:version,attempt_id_sha256:[witnesses.before,attemptIdentity(attempt),witnesses.after][i],event:{outcome:'ok',cpuTime:1,wallTime:20,...i===1?event:{}}}));
const delivery=records=>({schema:1,qualification:false,source:'provider-tail-dispatch',delivery_id:crypto.randomUUID(),producer:source,received_at:Date.now(),collector_version:counterVersion,foreign_events:0,count:records.length,records});
const scope={source,captureId,version,counterVersion,witnesses,ledger:{schema:1,overflow:false,attempts:[{id:attempt,finished_at:1700,status:200,version_id:version}]}};
async function captures(env,run){const dir=await mkdtemp(resolve('dist','window-test-')),previousFetch=globalThis.fetch,previousNow=Date.now;await writeFile(resolve(dir,'token'),'local-window-only',{mode:0o600});const observer={id:'antfly-1278-observe-windows',reader_id:'antfly-1278-observe-windows-reader',reader_origin:'https://antfly-1278-observe-windows-reader.example.workers.dev',reader_version:readerVersion,dir,token_file:resolve(dir,'token'),evidence_layout:'window-v1'};globalThis.fetch=(url,options)=>reader.fetch(new Request(url,options),{...env,WORKER_VERSION:{id:readerVersion}});const clock=previousNow()+120000;try{Date.now=()=>clock;const first=await collectObserverSource(observer,env.scopes[0],{witnesses});Date.now=()=>clock+31000;const second=await collectObserverSource(observer,env.scopes[0],{witnesses});await run(first,second,observer);}finally{globalThis.fetch=previousFetch;Date.now=previousNow;await rm(dir,{recursive:true,force:true});}}
async function seed(env,event){await persistCanonicalRows(env,envelope([row()]));await persistCanonicalTail(env,delivery(records(event)),Object.values(witnesses));}

test('canonical projection archives benign children but preserves all terminal identities and diagnostic hazards',async()=>{
 const healthy=row(),child={...row(),terminal:false,event:{outcome:'ok',logs:[{message:[JSON.stringify({event:'antfly.phase',elapsed_ms:10})]}]}};
 assert.equal((await compactRows([child])).length,0);
 const compact=(await compactRows([healthy]))[0];assert.equal(compact.attribute_schema,undefined);assert.equal(compact.source_row_sha256,await sha(JSON.stringify(healthy)));
 for(const event of[{logs:[{message:[JSON.stringify({physical:{resource_failures:1},failure_code:'SQLITE_NOMEM'})]}]},{exceptions:[{message:'SQLITE_NOMEM'}]},{truncated:true},{logs:[{message:['unknown diagnostic']}]},{logs:[{message:[JSON.stringify({event:'antfly.io_settled',io_lifecycle_id:'life'})]}]},{outcome:'exceededMemory'}])assert.equal((await compactRows([{...child,event}])).length,1);
 await assert.rejects(compactRows([{...child,start:null}]),/TimestampInvalid/);
 assert.deepEqual(compactEvent({outcome:'ok',cpuTime:1,wallTime:2}),{outcome:'ok',cpuTime:1,wallTime:2});
});
test('window catalog reconciles native witnesses, terminal counts and physical attempts without reading archives or old history',async()=>{
 const env=fixture();await seed(env);await env.ARCHIVE.put('verbose-history',new Uint8Array(65*1024*1024));await persistCanonicalRows(env,envelope([{...row(600001),invocation_id_sha256:'9'.repeat(64)}]));let oldReads=0;const original=env.LOGS.get.bind(env.LOGS);env.LOGS.get=(key,...args)=>{if(key.includes('/0000000000600000/'))oldReads++;return original(key,...args);};
 await captures(env,async(a,b)=>{assert.equal(a.objects,undefined);assert.equal(a.windows.length,1);const proof=await reconcileObserverSource(a,b,scope);assert.equal(proof.complete,true,JSON.stringify(proof));assert.equal(proof.terminals,1);assert.equal(proof.physical_attempts,1);assert.ok(a.windows[0].bytes<10000);});assert.equal(env.ARCHIVE.reads,0);assert.equal(oldReads,0);
});
test('recovered memory, truncation and unfinished physical I/O remain gate failures after projection',async()=>{
 for(const event of[{logs:[{message:[JSON.stringify({failure_code:'SQLITE_NOMEM'})]}]},{truncated:true},{logs:[{message:[JSON.stringify({io_draining:true,io_lifecycle_id:crypto.randomUUID()})]}]}]){const env=fixture();await seed(env,event);await captures(env,async(a,b)=>{const proof=await reconcileObserverSource(a,b,scope);assert.equal(proof.complete,false);assert.ok(proof.failures.some(f=>/ResourceFailure|DiagnosticGap/.test(f)),JSON.stringify(proof));});}
});
test('archive faults, fanout and wrong layout fail closed before clean proof publication',async()=>{
 const env=fixture();await assert.rejects(persistCanonicalRows({...env,ARCHIVE:env.LOGS},envelope([row()])),/ArchiveRequired/);
 await assert.rejects(persistCanonicalRows({...env,ARCHIVE:{put:async()=>{throw Error('archive unavailable');}}},envelope([row()])),/archive unavailable/);
 assert.equal([...env.LOGS.objects.keys()].some(k=>k.includes('/batches/')),false);
 await assert.rejects(persistCanonicalRows(env,envelope(Array.from({length:33},(_,i)=>row(i*60000)))),/PartitionBudgetExceeded/);
 await seed(env);await env.LOGS.put(proofPrefix(captureId)+'batches/legacy.json','{}');await assert.rejects(captures(env,async()=>{}),/ObserverHttpStatus503/);
});
test('rejection records, modified snapshot files and lost settlement cannot be approved',async()=>{
 const env=fixture();await seed(env);await env.LOGS.put(proofPrefix(captureId)+'rejections/error.json',JSON.stringify({rejection:true}));await captures(env,async(a,b)=>{assert.equal((await reconcileObserverSource(a,b,scope)).complete,false);await writeFile(b.windows[0].path,'{}');await assert.rejects(reconcileObserverSource(a,b,scope),/SnapshotChanged/);});
 const clean=fixture();await seed(clean);await captures(clean,async(a,b)=>{const value=JSON.parse(await readFile(b.windows[0].path));value.snapshot_started_at=a.snapshot_finished_at+1;value.snapshot_finished_at=value.snapshot_started_at;const text=JSON.stringify(value);await writeFile(b.windows[0].path,text);b.windows[0].sha256=await sha(text);b.windows[0].bytes=Buffer.byteLength(text);assert.equal((await reconcileObserverSource(a,b,scope)).complete,false);});
});
test('full sanitized source hashes retain duplicate-root conflicts despite compact projection',async()=>{
 const env=fixture();await seed(env);await persistCanonicalRows(env,envelope([{...row(),attribute_schema:['different verbose attributes']} ]));await captures(env,async(a,b)=>{const proof=await reconcileObserverSource(a,b,scope);assert.equal(proof.complete,false);assert.ok(proof.failures.includes('ObserverIdentityConflict'));});
});
test('boundary witness indexes cannot substitute for missing or duplicated provider deliveries',async()=>{
 const env=fixture();await seed(env);await persistCanonicalTail(env,delivery(records()),Object.values(witnesses));await assert.rejects(captures(env,async()=>{}),/ObserverHttpStatus503/);
 const missing=fixture();await seed(missing);for(const key of missing.LOGS.objects.keys())if(key.includes('/counter/'))await missing.LOGS.delete(key);await assert.rejects(captures(missing,async()=>{}),/ObserverHttpStatus503/);
});
test('native Tail capture uses the new format only when explicitly enabled and has bounded witness indexing',async()=>{
 const env=fixture(),id=crypto.randomUUID(),event={scriptName:source,eventTimestamp:1500,scriptVersion:{id:version},outcome:'ok',cpuTime:1,wallTime:2,event:{request:{url:'https://example.invalid/?__antfly_qualification_boundary=1&__antfly_qualification_attempt='+id}}};
 await persistTailDelivery([event],env);assert.equal([...env.LOGS.objects.keys()].filter(k=>k.includes('/witnesses/')).length,1);assert.equal([...env.LOGS.objects.keys()].some(k=>k.startsWith(windowPrefix(captureId,1500)+'counter/')),true);
 await assert.rejects(persistTailDelivery(Array(9).fill(event),env),/WitnessBudgetExceeded/);
});

test('successive closed windows deduplicate boundary partitions and prove the whole app ledger',async()=>{
 const env=fixture(),starts=[1500,61500,121500],attempts=starts.map(()=>crypto.randomUUID()),rows=starts.map((start,i)=>({...row(start),invocation_id_sha256:String(i+1).repeat(64),attempt_id_sha256:attemptIdentity(attempts[i])}));
 await persistCanonicalRows(env,envelope(rows));await persistCanonicalTail(env,delivery([records()[0],...rows.map(r=>({script:source,version_id:version,timestamp:r.start,attempt_id_sha256:r.attempt_id_sha256,event:r.event})),{...records()[2],timestamp:122000}]),Object.values(witnesses));
 const windowScans=[],list=env.LOGS.list.bind(env.LOGS);env.LOGS.list=options=>{if(options.prefix.includes('/windows/'))windowScans.push(options.prefix);return list(options);};await captures(env,async(a,b,observer)=>{assert.equal(a.windows.length,3);assert.equal(a.window_partition,'utc-minute');const ledger={schema:1,overflow:false,attempts:attempts.map(id=>({id,finished_at:130000,status:200,version_id:version}))},proof=await reconcileObserverSource(a,b,{...scope,ledger});assert.equal(proof.complete,true,JSON.stringify(proof));assert.equal(proof.terminals,3);assert.equal(proof.native_terminals,3);assert.equal(proof.physical_attempts,3);
  const missing=await reconcileObserverSource(a,b,{...scope,ledger:{...ledger,attempts:[...ledger.attempts,{id:crypto.randomUUID(),finished_at:130000,status:200,version_id:version}]}});assert.equal(missing.complete,false);assert.ok(missing.failures.includes('ObserverPhysicalAttemptMissing'));assert.equal(missing.failures.includes('ObserverUnexpectedHttpInvocation'),false);const unexpected=await reconcileObserverSource(a,b,{...scope,ledger:{...ledger,attempts:ledger.attempts.slice(1)}});assert.equal(unexpected.complete,false);assert.ok(unexpected.failures.includes('ObserverUnexpectedHttpInvocation'));
  const shifted=structuredClone(b);shifted.windows[1].timeframe.from++;assert.equal((await reconcileObserverSource(a,shifted,{...scope,ledger})).complete,false);
  const{partitionCanonicalTimeframe}=await import('../qualification/canonical-window-client.mjs');
  async function legacy(catalog){const all=new Map();let template;for(const entry of catalog.windows){const snap=JSON.parse(await readFile(entry.path));template??=snap;for(const object of snap.objects)all.set(object.key,object);}const result={...catalog};delete result.window_partition;result.windows=[];for(const [i,timeframe]of partitionCanonicalTimeframe(catalog.timeframe,'elapsed-minute').entries()){const objects=[...all.values()],snapshot={...template,timeframe,objects,bytes:objects.reduce((n,o)=>n+Buffer.byteLength(JSON.stringify(o)),0)},body=JSON.stringify(snapshot),path=resolve(observer.dir,'legacy-'+catalog.snapshot_started_at+'-'+i+'.json');await writeFile(path,body,{flag:'wx'});result.windows.push({timeframe,path,sha256:await sha(body),bytes:Buffer.byteLength(body)});}return result;}
  assert.equal((await reconcileObserverSource(await legacy(a),await legacy(b),{...scope,ledger})).complete,true,'old elapsed-minute catalogs remain verifiable');

 });assert.equal(windowScans.length,6);assert.equal(new Set(windowScans).size,3);
});
test('unsafe OTLP children, rejected rows and changed canonical identities cannot disappear behind healthy native counts',async()=>{
 for(const patch of[{terminal:false,event:{logs:[{message:[JSON.stringify({failure_code:'SQLITE_NOMEM'})]}]}},{terminal:false,dropped:true},{terminal:false,warning:true,warning_type:'unexpected'}]){const env=fixture();await seed(env);await persistCanonicalRows(env,envelope([{...row(),...patch}]));await captures(env,async(a,b)=>{assert.equal((await reconcileObserverSource(a,b,scope)).complete,false);});}
});
test('a forged minute, changed provider version or missing full-row hash invalidates both observations',async()=>{
 for(const patch of[value=>{value.minute=60000;},value=>{value.rows[0].version_id=crypto.randomUUID();},value=>{delete value.rows[0].source_row_sha256;}]){const env=fixture();await seed(env);for(const[key,object]of env.LOGS.objects)if(key.includes('/batches/')){const value=JSON.parse(new TextDecoder().decode(object.body));patch(value);await env.LOGS.put(key,JSON.stringify(value));}await captures(env,async(a,b)=>{assert.equal((await reconcileObserverSource(a,b,scope)).complete,false);});}
});
test('the original 20000 terminal limit still applies inside a compact closed window',async()=>{
 const {reconcileTailCounterSnapshots}=await import('../qualification/tail-counter-evidence.mjs'),root={...row(),source_row_sha256:'f'.repeat(64)},objects=[{key:windowPrefix(captureId,0)+'batches/a.json',value:{schema:2,evidence_layout:'window-v1',archive_sha256:'f'.repeat(64),minute:0,rows:Array(1000).fill(root)}}];
 for(let i=1;i<21;i++)objects.push({...objects[0],key:windowPrefix(captureId,0)+'batches/'+i+'.json'});
 const first={qualification:false,evidence_layout:'window-v1',capture_id:captureId,collector_version:readerVersion,snapshot_started_at:62000,snapshot_finished_at:63000,objects},second={...first,snapshot_started_at:94000,snapshot_finished_at:95000};
 const proof=await reconcileTailCounterSnapshots(first,second,{...scope,timeframe:{from:1001,to:1999}});assert.equal(proof.complete,false);assert.ok(proof.failures.includes('TailCounterScopeInvalid'));
});


test('UTC windows read each physical minute once and preserve single-millisecond edges',async()=>{
 const{partitionCanonicalTimeframe}=await import('../qualification/canonical-window-client.mjs');
 const time={from:1001,to:121999},cuts=partitionCanonicalTimeframe(time),legacy=partitionCanonicalTimeframe(time,'elapsed-minute');
 assert.deepEqual(cuts,[{from:1001,to:59999},{from:60000,to:119999},{from:120000,to:121999}]);
 const scans=windows=>windows.flatMap(w=>{const a=[];for(let m=Math.floor(w.from/60000)*60000;m<=w.to;m+=60000)a.push(m);return a;});
 assert.equal(scans(legacy).length,5);assert.equal(scans(cuts).length,3);assert.equal(new Set(scans(cuts)).size,3);
 assert.deepEqual(partitionCanonicalTimeframe({from:59999,to:60000}),[{from:59999,to:59999},{from:60000,to:60000}]);
 assert.throws(()=>partitionCanonicalTimeframe(time,'unknown'),/ScopeInvalid/);
});

test('window inventory pins every page ETag and rejects cross-minute or oversized bodies',async()=>{
 const env=fixture();await seed(env);const request=(mode,value)=>reader.fetch(new Request('https://reader.invalid/source/'+source+'/'+mode,{method:'POST',headers:{Authorization:'Bearer '+env.API_TOKEN},body:JSON.stringify(value)}),env);
 const inventory=await(await request('window-inventory',{minute:0})).json();assert.ok(inventory.objects.length>0);assert.ok(inventory.objects.every(o=>o.etag&&o.key.startsWith(windowPrefix(captureId,0))));
 const response=await request('window-objects',{minute:0,objects:inventory.objects});assert.equal(response.status,200);assert.equal((await response.json()).objects.length,inventory.objects.length);
 const item=inventory.objects[0];await env.LOGS.put(item.key,JSON.stringify({changed:true}));
 const changed=await request('window-objects',{minute:0,objects:[item]});assert.equal(changed.status,503);await changed.body.cancel();
 for(const value of[{minute:60000,objects:[item]},{minute:0,objects:Array(33).fill(item)},{minute:0,objects:[{...item,size:4*1024*1024+1}]},{minute:0,objects:[{...item,etag:''}]}]){const r=await request('window-objects',value);assert.equal(r.status,503);await r.body.cancel();}
});

test('pipelined window capture retains the original page cap and independently re-reads all objects',async()=>{
 const env=fixture();await seed(env);
 for(let i=0;i<97;i++)await env.LOGS.put(windowPrefix(captureId,0)+'batches/extra-'+String(i).padStart(3,'0')+'.json',JSON.stringify({schema:2,rows:[]}));
 const original=globalThis.fetch;
 // captures installs its own fetch adapter; instrument the backing binding to
 // prove all immutable bodies are re-read in each independent observation.
 const get=env.LOGS.get.bind(env.LOGS);let reads=0;env.LOGS.get=async(...args)=>{reads++;return get(...args);};
 await captures(env,async(a,b)=>{assert.equal(a.windows.length,1);assert.equal(b.windows.length,1);assert.ok(reads>=2*97);const one=JSON.parse(await readFile(a.windows[0].path));assert.equal(one.objects.length,JSON.parse(await readFile(b.windows[0].path)).objects.length);assert.ok(one.pages>=6);});
 assert.equal(globalThis.fetch,original);
});

test('a resource failure in a pipelined proof page remains visible and all other started reads settle',async()=>{
 const env=fixture();await seed(env);for(let i=0;i<100;i++)await env.LOGS.put(windowPrefix(captureId,0)+'batches/extra-'+String(i).padStart(3,'0')+'.json',JSON.stringify({rows:[]}));
 const get=env.LOGS.get.bind(env.LOGS);let active=0,failed=false;
 env.LOGS.get=async(key,...args)=>{active++;try{await new Promise(ok=>setTimeout(ok,1));if(!failed&&key.endsWith('extra-035.json')){failed=true;throw Error('SQLITE_NOMEM');}return await get(key,...args);}finally{active--;}};
 await assert.rejects(captures(env,async()=>{}),/ObserverResourceFailure:SQLITE_NOMEM/);assert.equal(active,0);const{diagnosticReadResponses}=await import('../qualification/diagnostic-memory.mjs');assert.equal(diagnosticReadResponses.report().active,0);assert.equal(diagnosticReadResponses.report().pending,0);
});
