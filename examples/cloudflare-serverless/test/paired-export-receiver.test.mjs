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
