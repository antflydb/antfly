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

import{test}from'node:test';import assert from'node:assert/strict';import receiver,{partitionExport}from'../qualification/paired-export-receiver.mjs';import{Bucket}from'./object-fixture.mjs';
const pair='antfly-1278-g-export-1',scopes=['b','c'].flatMap(side=>['','-writer','-reader','-builder'].map(role=>({source:pair+'-'+side+role,capture_id:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i})),attr=(key,v)=>({key,value:{stringValue:v}}),group=(n=1,serial=crypto.randomUUID())=>({resource:{attributes:[attr('service.name',scopes[0].source)]},scopeSpans:[{spans:Array.from({length:n},(_,i)=>({traceId:'a'.repeat(32),spanId:'b'.repeat(16),startTimeUnixNano:'1000000000',endTimeUnixNano:'1010000000',attributes:[attr('faas.invocation_id','provider-'+serial+'-'+i)]}))}]});
test('large provider resource and record batches retain every record in the existing sanitized chunk bounds',()=>{for(const input of[{resourceSpans:Array.from({length:2048},()=>group())},{resourceSpans:[group(4001)]}]){const p=partitionExport([input],'traces');assert.equal(p.records,input.resourceSpans.reduce((n,g)=>n+g.scopeSpans[0].spans.length,0));assert.ok(p.chunks.every(c=>c.resourceSpans.length<=128&&c.resourceSpans.reduce((n,g)=>n+g.scopeSpans[0].spans.length,0)<=1000));assert.equal(p.chunks.flatMap(c=>c.resourceSpans).flatMap(g=>g.scopeSpans).flatMap(s=>s.spans).length,p.records);}assert.equal(partitionExport([group(3)],'traces').records,3);assert.throws(()=>partitionExport([{resourceSpans:[group(10001)]}],'traces'));});
test('receiver supports JSON arrays and persists only sanitized data and bounded payload shapes',async()=>{const env={API_TOKEN:'local-only',SOURCE_SCOPES:JSON.stringify(scopes),LOGS:new Bucket()},payload=[{resourceSpans:Array.from({length:256},()=>group())}],response=await receiver.fetch(new Request('https://receiver.invalid/v1/traces',{method:'POST',headers:{Authorization:'Bearer local-only'},body:JSON.stringify(payload)}),env);assert.equal(response.status,200);const shapes=[...env.LOGS.objects.entries()].filter(([k])=>k.startsWith('receiver-v1/shapes/')).map(([,o])=>JSON.parse(new TextDecoder().decode(o.body)));assert.equal(shapes[0].resources,256);assert.equal(shapes[0].records,256);assert.equal(shapes[0].accepted,true);assert.equal(shapes[0].top_kind,'array');const rows=[...env.LOGS.objects.entries()].filter(([k])=>k.startsWith('probe-v1/')).flatMap(([,o])=>JSON.parse(new TextDecoder().decode(o.body)).rows);assert.equal(rows.length,256);assert.ok(!JSON.stringify(shapes).includes('provider-'));});

test('receiver health reports late rejections outside its bounded shape sample',async()=>{
 const env={API_TOKEN:'local-only',SOURCE_SCOPES:JSON.stringify(scopes),LOGS:new Bucket()};
 for(let i=0;i<8;i++)await env.LOGS.put('receiver-v1/shapes/000'+i+'.json',JSON.stringify({accepted:true}));
 const response=await receiver.fetch(new Request('https://receiver.invalid/v1/traces',{method:'POST',headers:{Authorization:'Bearer local-only'},body:'{}'}),env);assert.equal(response.status,400);
 const health=await(await receiver.fetch(new Request('https://receiver.invalid/health',{headers:{Authorization:'Bearer local-only'}}),env)).json();assert.equal(health.shapes.length,8);assert.equal(health.rejections_present,true);
});

test('windowed preparation coalesces source rows across decode chunks before bounded publication',async()=>{
 const{prepareExport,publishExport}=await import('../qualification/prepare-export.mjs'),bucket=new Bucket(),archive=new Bucket(),env={API_TOKEN:'local',SOURCE_SCOPES:JSON.stringify(scopes),LOGS:bucket,ARCHIVE:archive,WINDOWED_EVIDENCE:'true'},input=JSON.stringify({resourceSpans:Array.from({length:1000},(_,i)=>{const value=group();value.resource.attributes=[attr('service.name',scopes[i%8].source)];return value;})}),prepared=await prepareExport(input,'traces',env,{digest:'a'.repeat(64)});
 assert.equal(prepared.summary.records,1000);assert.equal(prepared.summary.chunks,8);assert.equal(prepared.entries.length,8);assert.ok(prepared.summary.peak_sanitized_buffer_bytes<=4*1024*1024);assert.equal(bucket.writes,0);assert.equal(archive.writes,0);await publishExport(env,prepared);assert.equal(archive.writes,8);let rows=0;for(const object of archive.objects.values())rows+=JSON.parse(new TextDecoder().decode(object.body)).rows.length;assert.equal(rows,1000);
});

test('receiver publication drains every peer after failure before releasing encoded-byte ownership',async()=>{
 const{publishExport}=await import('../qualification/prepare-export.mjs'),{prepareCanonicalRows}=await import('../qualification/canonical-windows.mjs'),bucket=new Bucket(),archive=new Bucket(),env={WINDOWED_EVIDENCE:'true',LOGS:bucket,ARCHIVE:archive};let release,entered;const hold=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve),put=archive.put.bind(archive);let calls=0,finished=false;
 const entries=await Promise.all(scopes.slice(0,4).map(async scope=>({scope,prepared:await prepareCanonicalRows({...env,SOURCE_WORKER:scope.source,CAPTURE_ID:scope.capture_id},{signal:'logs',received_at:Date.now(),raw_batch_sha256:'a'.repeat(64),rows:[]})})));archive.put=async(...args)=>{if(++calls===1)throw Error('SQLITE_NOMEM [test-only injected archive failure]');entered();await hold;return put(...args);};const result=publishExport(env,{entries}).finally(()=>finished=true);await started;assert.equal(finished,false);release();await assert.rejects(result,/SQLITE_NOMEM/);assert.equal(calls,4);
});
