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

import{test}from'node:test';import assert from'node:assert/strict';
import collector,{sanitizeLogpushProbe}from'../qualification/logpush-probe-collector.mjs';import{Bucket}from'./object-fixture.mjs';
const source='antfly-1278-telemetry-fixture-source',token='disposable-local-fixture-only',id=crypto.randomUUID(),capture=crypto.randomUUID();
const row=()=>({ScriptName:source,EventTimestampMs:1234,EventType:'fetch',Entrypoint:'default',Outcome:'ok',ScriptVersion:{id},CPUTimeMs:1,WallTimeMs:10,Event:{request:{headers:{Authorization:'secret-must-not-persist','cf-ray':'a'.repeat(16)},url:'https://private.invalid'},requestId:'private-request-id'},Logs:[{timestamp:1234,message:[JSON.stringify({event:'antfly.qualification_probe',id,kind:'fetch',phase:'late'}),'secret-must-not-persist']}],Exceptions:[]});
const env=()=>({API_TOKEN:token,CAPTURE_ID:capture,SOURCE_WORKER:source,LOGS:new Bucket()});
const request=(path,body,headers={})=>new Request('https://collector.invalid'+path,{method:'POST',headers:{Authorization:'Bearer '+token,...headers},body});
test('Logpush ingestion sanitizes and deduplicates durable batches without persisting secrets',async()=>{
 const e=env(),body=JSON.stringify(row());for(let i=0;i<2;i++)assert.equal((await collector.fetch(request('/ingest',body),e)).status,200);assert.equal(e.LOGS.objects.size,1);
 const report=await(await collector.fetch(request('/report','{}'),e)).json();const text=JSON.stringify(report);assert.ok(!text.includes('secret-must-not-persist'));assert.ok(!text.includes('private.invalid'));assert.ok(!text.includes('private-request-id'));assert.equal(report.qualification,false);assert.equal(report.objects[0].value.rows[0].probes[0].id,id);assert.match(report.objects[0].value.rows[0].identity_candidates.event_request_id_sha256,/^[a-f0-9]{64}$/);
});
test('Logpush gzip validation and source boundaries cannot create unscoped durable data',async()=>{
 const e=env(),compressed=await new Response(new Blob(['{\n  "content": "tests"\n}']).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer();assert.equal((await collector.fetch(request('/ingest',compressed,{'Content-Encoding':'gzip'}),e)).status,200);assert.equal(e.LOGS.objects.size,0);
 const wrong={...row(),ScriptName:'production'};assert.equal((await collector.fetch(request('/ingest',JSON.stringify(wrong)),e)).status,400);assert.equal(e.LOGS.objects.size,1);assert.deepEqual(JSON.parse(new TextDecoder().decode([...e.LOGS.objects.values()][0].body)),{schema:1,qualification:false,rejection:'TelemetrySourceMismatch',encoding:'absent'});
 assert.equal((await collector.fetch(new Request('https://collector.invalid/ingest',{method:'POST',body:JSON.stringify(row())}),e)).status,401);assert.equal(e.LOGS.objects.size,1);
 assert.equal((await collector.fetch(request('/ingest','{invalid'),e)).status,400);assert.equal((await collector.fetch(request('/ingest','x'.repeat(6*1024*1024+1)),e)).status,413);assert.equal(e.LOGS.objects.size,3);assert.ok([...e.LOGS.objects.keys()].every(key=>key.startsWith('probe-v1/'+capture+'/rejections/')));
});
test('Logpush preserves late resource codes and marks unknown invocation schemas as unknown',async()=>{
 const value=row();value.EventType='new-unknown-type';value.Logs.push({message:[JSON.stringify({event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM secret-must-not-persist'})]});const safe=await sanitizeLogpushProbe(value,source);assert.equal(safe.event_type,'unknown');assert.ok(JSON.stringify(safe).includes('SQLITE_NOMEM'));assert.ok(!JSON.stringify(safe).includes('secret-must-not-persist'));
});
test('probe plans require exact private-source replies and drain only the owned capture prefix',async()=>{
 const e=env();e.FIXTURE={fetch:async url=>Response.json({id:new URL(url).searchParams.get('nonce'),rpc:crypto.randomUUID()})};const exercise=await(await collector.fetch(request('/exercise',JSON.stringify({count:2})),e)).json();assert.equal(exercise.expected.length,2);await e.LOGS.put('unrelated','keep');assert.equal((await(await collector.fetch(request('/drain','{}'),e)).json()).deleted,1);assert.equal((await(await collector.fetch(request('/drain','{}'),e)).json()).done,true);assert.ok(e.LOGS.objects.has('unrelated'));
});

test('retirement closes ingestion and execution while retaining authenticated drain',async()=>{const e=env();e.RETIRE_MODE='true';assert.equal((await collector.fetch(request('/ingest',JSON.stringify(row())),e)).status,410);assert.equal((await collector.fetch(request('/exercise','{}'),e)).status,410);assert.equal((await collector.fetch(request('/drain','{}'),e)).status,200);assert.equal(e.LOGS.objects.size,0);});
