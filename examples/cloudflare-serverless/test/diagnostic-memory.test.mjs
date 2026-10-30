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
import {gzipSync} from 'node:zlib';
import {DiagnosticAdmission,diagnosticAdmission,exportAdmission,checkDiagnosticJson,parseDiagnosticJson} from '../qualification/diagnostic-memory.mjs';
import {diagnosticStorage} from '../qualification/diagnostic-storage.mjs';
import {readBody} from '../qualification/logpush-probe-collector.mjs';
import {sanitizeTailEvent} from '../qualification/metrics-tail.mjs';
import {Bucket} from './object-fixture.mjs';
import receiver from '../qualification/paired-export-receiver.mjs';
const pair='antfly-1278-g-memory-1',scopes=['b','c'].flatMap(side=>['','-writer','-reader','-builder'].map(role=>({source:pair+'-'+side+role,capture_id:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i}));
const gate=()=>{let release;const promise=new Promise(resolve=>release=resolve);return {promise,release};};

test('isolate diagnostic admission retains FIFO tickets without reading waiting request bodies',async()=>{
 const admission=new DiagnosticAdmission(),hold=gate(),entered=gate(),order=[];
 const first=admission.run(async()=>{order.push(1);entered.release();await hold.promise;});await entered.promise;
 const second=admission.run(async()=>order.push(2)),third=admission.run(async()=>order.push(3));await new Promise(resolve=>setTimeout(resolve,10));assert.deepEqual(order,[1]);assert.equal(admission.active,1);assert.equal(admission.pending.length,2);
 hold.release();await Promise.all([first,second,third]);assert.deepEqual(order,[1,2,3]);assert.equal(admission.active,0);assert.equal(admission.pending.length,0);assert.equal(admission.peak,1);
});
test('diagnostic admission bounds queue capacity, cancels waiters and releases failed admitted work',async()=>{
 const admission=new DiagnosticAdmission({maxQueued:1}),hold=gate(),entered=gate(),abort=new AbortController();const first=admission.run(async()=>{entered.release();await hold.promise;});await entered.promise;
 const second=admission.run(()=>assert.fail('canceled waiter ran'),{signal:abort.signal});await assert.rejects(admission.run(()=>assert.fail('overflow ran')),/AdmissionExceeded/);abort.abort();await assert.rejects(second,/DiagnosticCanceled/);assert.equal(admission.pending.length,0);hold.release();await first;
 await assert.rejects(admission.run(()=>{throw Error('physical failure');}),/physical failure/);assert.equal(admission.active,0);
 let now=0;const expiry=new DiagnosticAdmission({waitMs:10,clock:()=>now,sleep:async()=>{now=11;}});const held=gate(),active=expiry.run(()=>held.promise);await assert.rejects(expiry.run(()=>assert.fail('expired waiter ran')),/AdmissionExpired/);held.release();await active;assert.equal(expiry.pending.length,0);
});
test('JSON preflight bounds nesting and allocation nodes across the complete NDJSON payload',()=>{
 assert.deepEqual(parseDiagnosticJson('{"text":"braces [ {} and escaped \\\" quotes","a":[1,true,null]}'),{text:'braces [ {} and escaped " quotes',a:[1,true,null]});
 assert.deepEqual(parseDiagnosticJson('{"a":1}\n{"b":2}',{lines:true}),[{a:1},{b:2}]);
 assert.throws(()=>parseDiagnosticJson('[0,0,0,0]',{maxNodes:4}),/ComplexityExceeded/);
 assert.throws(()=>parseDiagnosticJson('{"a":1}\n{"b":2}',{lines:true,maxNodes:5}),/ComplexityExceeded/);
 assert.throws(()=>checkDiagnosticJson('['.repeat(33)+'0'+']'.repeat(33)),/ComplexityExceeded/);
 assert.throws(()=>parseDiagnosticJson('{"a":1,}'),SyntaxError);
 assert.throws(()=>parseDiagnosticJson('[0'+',0'.repeat(262144)+']'),/ComplexityExceeded/);
});
test('gzip magic can span chunks and compressed/decompressed byte limits cancel the source',async()=>{
 const value='{"text":"'+('bounded '.repeat(10000))+'"}',zip=gzipSync(value);let index=0;
 const body=new ReadableStream({pull(controller){if(index===zip.length)controller.close();else{const end=Math.min(zip.length,index+(index===0?1:1024));controller.enqueue(zip.subarray(index,end));index=end;}}});
 const bytes=await readBody(new Request('https://fixture.invalid',{method:'POST',body,duplex:'half'}));assert.equal(new TextDecoder().decode(bytes),value);
 const bomb=gzipSync('x'.repeat(6*1024*1024+1));await assert.rejects(readBody(new Request('https://fixture.invalid',{method:'POST',body:bomb})),/BodyExceeded/);
 let canceled=false;const oversized=new ReadableStream({pull(controller){controller.enqueue(new Uint8Array(1024*1024));},cancel(){canceled=true;}});await assert.rejects(readBody(new Request('https://fixture.invalid',{method:'POST',body:oversized,duplex:'half'})),/BodyExceeded/);assert.equal(canceled,true);
});
test('receiver waits before touching its body and resource errors retain safe exact boundary attribution',async()=>{
 const held=gate(),entered=gate(),first=exportAdmission.run(async()=>{entered.release();await held.promise;});await entered.promise;
 let bodyReads=0;const fake={headers:new Headers({Authorization:'Bearer local-only'}),url:'https://fixture.invalid/v1/traces',method:'POST',get body(){bodyReads++;return new ReadableStream({start(controller){controller.enqueue(new TextEncoder().encode('{}'));controller.close();}});}};
 const request=receiver.fetch(fake,{API_TOKEN:'local-only',SOURCE_SCOPES:JSON.stringify(scopes),LOGS:new Bucket()});await new Promise(resolve=>setTimeout(resolve,10));assert.equal(bodyReads,0);held.release();await first;assert.equal((await request).status,400);assert.ok(bodyReads>0);
 const failure=Error('SQLITE_NOMEM private-secret'),samples=[],bucket={put:async()=>{throw failure;}};const env=diagnosticStorage({LOGS:bucket,ARCHIVE:bucket},'tail-observer',{emit:sample=>samples.push(sample)});assert.equal(env.LOGS,env.ARCHIVE);await assert.rejects(env.LOGS.put('private-key','private-body'),e=>e===failure);
 assert.deepEqual(samples,[{event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM',boundary:'r2-binding',component:'tail-observer',storage:'proof',op:'put'}]);assert.ok(!JSON.stringify(samples).includes('private'));
 const safe=sanitizeTailEvent({outcome:'ok',logs:[{message:[JSON.stringify(samples[0])]}]});assert.deepEqual(JSON.parse(safe.logs[0].message[0]),samples[0]);
});
test('complexity rejection is durable and unsafe log expansion remains a diagnostic gap',async()=>{
 const bucket=new Bucket(),body='['.repeat(33)+'0'+']'.repeat(33),response=await receiver.fetch(new Request('https://fixture.invalid/v1/traces',{method:'POST',headers:{Authorization:'Bearer local-only'},body}),{API_TOKEN:'local-only',SOURCE_SCOPES:JSON.stringify(scopes),LOGS:bucket});assert.equal(response.status,400);assert.equal((await response.json()).error,'TelemetryJsonComplexityExceeded');assert.equal([...bucket.objects.keys()].filter(k=>k.startsWith('receiver-v1/rejections/')).length,1);
 const event=sanitizeTailEvent({outcome:'ok',logs:[{message:[body]}]});assert.equal(event.truncated,true);
});

test('reader health includes receiver rejections beyond the successful shape sample',async()=>{
 const reader=(await import('../qualification/paired-evidence-reader.mjs')).default,bucket=new Bucket(),token='local-reader';
 for(let i=0;i<8;i++)await bucket.put('receiver-v1/shapes/000'+i+'.json',JSON.stringify({accepted:true}));
 await bucket.put('receiver-v1/rejections/late.json',JSON.stringify({accepted:false,error:'TelemetryIngestionFailed',failure_code:'SQLITE_NOMEM'}));
 const response=await reader.fetch(new Request('https://fixture.invalid/health',{headers:{Authorization:'Bearer '+token}}),{API_TOKEN:token,SOURCE_SCOPES:JSON.stringify(scopes),LOGS:bucket}),health=await response.json();assert.equal(health.rejections_present,true);assert.equal(health.rejection_codes['TelemetryIngestionFailed:SQLITE_NOMEM'],1);
});
test('an injected reader allocation failure survives a later healthy response and blocks the run verdict',async()=>{
 const {observerCall,observerDiagnosticsComplete}=await import('../qualification/paired-observer-client.mjs'),reader=(await import('../qualification/paired-evidence-reader.mjs')).default,{mkdtemp,writeFile,rm}=await import('node:fs/promises'),{resolve}=await import('node:path'),dir=await mkdtemp(resolve('dist','reader-resource-test-')),token='local-reader-resource';await writeFile(resolve(dir,'token'),token,{mode:0o600});
 const observer={id:'antfly-1278-observe-test',reader_id:'antfly-1278-observe-test-reader',reader_origin:'https://antfly-1278-observe-test-reader.example.workers.dev',token_file:resolve(dir,'token')},bucket=new Bucket();await bucket.put('probe-v1/'+scopes[0].capture_id+'/x.json','{}');let failed=true;
 const env={API_TOKEN:token,SOURCE_SCOPES:JSON.stringify(scopes),LOGS:{list:o=>bucket.list(o),get:async key=>{if(failed)throw Error('SQLITE_NOMEM [test-only injected read failure] secret');return bucket.get(key);}}};
 try{const request=(url,options)=>reader.fetch(new Request(url,options),env);await assert.rejects(observerCall(observer,'/source/'+scopes[0].source+'/page',{}, {fetch:request}),/ObserverResourceFailure:SQLITE_NOMEM/);assert.deepEqual(observer.observed_resource_failures.map(r=>[r.component,r.failure_code]),[['evidence-reader','SQLITE_NOMEM']]);failed=false;const healthy=await observerCall(observer,'/health',{}, {fetch:request});assert.equal(healthy.rejections_present,false);assert.equal(observerDiagnosticsComplete(observer,healthy,Array(8).fill({complete:true}),{complete:true}),false);assert.ok(!JSON.stringify(observer.observed_resource_failures).includes('secret'));
 }finally{await rm(dir,{recursive:true,force:true});}
});
test('unauthorized observer requests do not enter diagnostic admission or create rejection records',async()=>{
 const observer=(await import('../qualification/paired-observer-worker.mjs')).default,bucket=new Bucket(),before=diagnosticAdmission.pending.length,response=await observer.fetch(new Request('https://fixture.invalid/v1/traces',{method:'POST',body:'{}'}),{API_TOKEN:'local-only',LOGS:bucket});assert.equal(response.status,401);assert.equal(bucket.writes,0);assert.equal(diagnosticAdmission.pending.length,before);
});

test('byte-charged Tail pipeline overlaps slow writes without retaining parser permits or leaking reservations',async()=>{
 const {persistObserverTail}=await import('../qualification/paired-observer.mjs'),{diagnosticWrites,tailAdmission,tailInputMetrics}=await import('../qualification/diagnostic-memory.mjs'),bucket=new Bucket(),archive=new Bucket(),env={API_TOKEN:'local',SOURCE_SCOPES:JSON.stringify(scopes),WINDOWED_EVIDENCE:'true',LOGS:bucket,ARCHIVE:archive},put=bucket.put.bind(bucket);let active=0,peak=0;
 bucket.put=async(...args)=>{active++;peak=Math.max(peak,active);try{await new Promise(resolve=>setTimeout(resolve,10));return await put(...args);}finally{active--;}};
 const calls=Array.from({length:32},()=>persistObserverTail(scopes.map(s=>({scriptName:s.source,eventTimestamp:1500,outcome:'ok',cpuTime:1,wallTime:2})),env));await Promise.all(calls);
 assert.ok(peak>1);assert.equal([...bucket.objects.keys()].filter(k=>k.includes('/counter/')).length,32*8);assert.equal(tailAdmission.peak,1);assert.equal(diagnosticWrites.active,0);assert.equal(diagnosticWrites.bytes,0);assert.ok(diagnosticWrites.peakActive<=16);assert.ok(diagnosticWrites.peakBytes<=16*1024*1024);assert.equal(tailInputMetrics().reserved_bytes,0);
});
test('prepared-write and decoded-input byte budgets reject overflow and remain held until publication settles',async()=>{
 const {DiagnosticWriteBudget,reserveTailInput}=await import('../qualification/diagnostic-memory.mjs'),budget=new DiagnosticWriteBudget({maxBytes:16,maxActive:2}),first=await budget.reserve(12);let entered=false;const pending=budget.reserve(8).then(release=>{entered=true;return release;});await new Promise(resolve=>setTimeout(resolve,10));assert.equal(entered,false);assert.equal(budget.bytes,12);first();const second=await pending;assert.equal(budget.bytes,8);second();second();assert.equal(budget.bytes,0);await assert.rejects(budget.reserve(17),/WriteBudgetExceeded/);
 assert.throws(()=>reserveTailInput({body:'x'.repeat(1024)},{limitBytes:1000}),/InputBudgetExceeded/);const release=reserveTailInput({body:'small'});release();release();
});
test('new capture generations retain failed prefixes while checking only their own immutable evidence',async()=>{
 const {diagnosticPrefix}=await import('../qualification/diagnostic-scope.mjs'),reader=(await import('../qualification/paired-evidence-reader.mjs')).default,bucket=new Bucket(),id=crypto.randomUUID(),env={API_TOKEN:'local',SOURCE_SCOPES:JSON.stringify(scopes),LOGS:bucket,OBSERVATION_CAPTURE_ID:id};
 await bucket.put('observer-v1/rejections/historical.json',JSON.stringify({error:'ObserverTailFailed'}));const request=()=>reader.fetch(new Request('https://fixture.invalid/health',{headers:{Authorization:'Bearer local'}}),env);const clean=await(await request()).json();assert.equal(clean.rejections_present,false);assert.equal(clean.observation_capture_id,id);await bucket.put(diagnosticPrefix(env,'receiver')+'rejections/new.json',JSON.stringify({error:'TelemetryDiagnosticAdmissionExceeded'}));assert.equal((await(await request()).json()).rejections_present,true);assert.ok(bucket.objects.has('observer-v1/rejections/historical.json'));assert.throws(()=>diagnosticPrefix({...env,OBSERVATION_CAPTURE_ID:'invalid'},'observer'),/ScopeInvalid/);
});

test('a failed parallel Tail write retains its byte permit until all remaining physical writes settle',async()=>{
 const {persistObserverTail}=await import('../qualification/paired-observer.mjs'),{diagnosticWrites}=await import('../qualification/diagnostic-memory.mjs'),hold=gate(),entered=gate(),bucket=new Bucket(),put=bucket.put.bind(bucket);let calls=0;
 bucket.put=async(...args)=>{if(++calls===1)throw Error('SQLITE_NOMEM [test-only injected write failure]');entered.release();await hold.promise;return put(...args);};
 const result=persistObserverTail(scopes.slice(0,4).map(s=>({scriptName:s.source,eventTimestamp:1500,outcome:'ok',cpuTime:1,wallTime:1})),{SOURCE_SCOPES:JSON.stringify(scopes),LOGS:bucket});await entered.promise;assert.equal(diagnosticWrites.active,1);assert.ok(diagnosticWrites.bytes>0);hold.release();await assert.rejects(result,/SQLITE_NOMEM/);assert.equal(diagnosticWrites.active,0);assert.equal(diagnosticWrites.bytes,0);
});
