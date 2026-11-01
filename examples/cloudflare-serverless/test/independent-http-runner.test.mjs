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

import{test}from'node:test';import assert from'node:assert/strict';import http from'node:http';import{nodeHttpTransport}from'../qualification/node-http-transport.mjs';import{offeredSchedule,runIndependent}from'../qualification/independent-http-runner.mjs';import{documentBody}from'../qualification/http-trial-driver.mjs';import{requestAttemptUuid}from'../qualification/attempt-token.mjs';
const version=crypto.randomUUID(),fence='01'+'a'.repeat(64)+'0000000000000001',plan={side:'candidate',id:'node-fixture',version,steps:[{op:'lookup',tenant:'a',id:1,visibility:fence}]},receipts=Object.fromEntries(['a','b','c','d'].map(t=>[t,fence])),token='c'.repeat(64),origin='https://antfly-1278-cold-fixture-1234.example.workers.dev';
test('independent Node client physically overlaps sixteen requests without a Worker connection limit',async()=>{
 let active=0,peak=0,calls=0;const server=http.createServer(async(req,res)=>{const chunks=[];for await(const c of req)chunks.push(c);const input=JSON.parse(Buffer.concat(chunks));assert.equal(req.headers.authorization,'Bearer '+token);assert.ok(requestAttemptUuid('https://fixture.invalid'+req.url));calls++;peak=Math.max(peak,++active);await new Promise(r=>setTimeout(r,40));res.writeHead(200,{'Content-Type':'application/json','X-Antfly-Version':version,'CF-Ray':'aabbccdd00112233-SJC'});res.end(JSON.stringify({body:documentBody(req.headers['x-antfly-tenant'],Number(input.doc_id.slice(4))),visibility:fence}));active--;});await new Promise(r=>server.listen(0,'127.0.0.1',r));const local='http://127.0.0.1:'+server.address().port,pool=nodeHttpTransport({allowLoopbackHttp:true});
 try{const tasks=Array.from({length:16},(_,i)=>({attempt_id:crypto.randomUUID(),offset_ms:0,step:{op:'lookup',tenant:['a','b','c','d'][i%4],id:1}})),result=await runIndependent({plan,tasks,receipts,maxDispatchLagMs:1000,transport:{...pool,origin,token,fetch:(url,init)=>pool.fetch(local+new URL(url).pathname+new URL(url).search,init)}});assert.equal(result.passed,true,JSON.stringify(result.failures));assert.equal(calls,16);assert.equal(peak,16);assert.equal(result.transport.peak_queued,0);assert.equal(result.invocation_ledger.attempts.length,16);assert.ok(result.invocation_ledger.attempts.every(a=>a.finished_at&&a.status===200&&a.ray_id_sha256));assert.ok(!JSON.stringify(result).includes(token));}finally{pool.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
});
test('offered arrivals retain overload instead of waiting for slow replies or dropping samples',async()=>{
 const tasks=Array.from({length:5},()=>({attempt_id:crypto.randomUUID(),offset_ms:0,step:{op:'lookup',tenant:'a',id:1}}));let sent=0;const transport={origin,token,report:()=>({peak_queued:0}),fetch:async()=>{sent++;await new Promise(r=>setTimeout(r,20));return Response.json({body:documentBody('a',1),visibility:fence},{headers:{'X-Antfly-Version':version}});}};const result=await runIndependent({plan,tasks,receipts,transport,maxInFlight:1,maxDispatchLagMs:1000});assert.equal(sent,1);assert.equal(result.samples.length,5);assert.equal(result.unsent_attempt_ids.length,4);assert.ok(result.failures.includes('IndependentRequestFailed'));assert.equal(result.samples.filter(s=>s.error==='IndependentClientOverloaded').length,4);
});
test('transport, journal and scheduling failures cannot become a successful latency report',async()=>{
 const tasks=offeredSchedule({reads:2,rps:500,writeEvery:0});const transport={origin,token,report:()=>({peak_queued:1}),fetch:async()=>Response.json({body:'wrong',visibility:fence},{headers:{'X-Antfly-Version':version}})};const result=await runIndependent({plan,tasks,receipts,transport,maxDispatchLagMs:0,terminal:async()=>{throw Error('disk');}});assert.equal(result.passed,false);assert.ok(result.failures.includes('IndependentTransportQueued'));assert.ok(result.failures.includes('IndependentTerminalWriteFailed'));assert.ok(result.failures.includes('IndependentRequestFailed'));
});
test('schedule bounds arrivals and writes; oversized responses and redirects are not replayed',async()=>{
 const tasks=offeredSchedule({reads:32,rps:32,writeEvery:16});assert.equal(tasks.length,34);assert.equal(tasks[0].offset_ms,0);assert.equal(tasks.at(-1).offset_ms,31*1000/32);assert.equal(new Set(tasks.map(t=>t.attempt_id)).size,34);assert.throws(()=>offeredSchedule({reads:100000}),/Invalid/);const transport=nodeHttpTransport();await assert.rejects(transport.fetch('https://example.com',{redirect:'follow'}),/Redirect/);await assert.rejects(transport.fetch('http://example.com',{redirect:'manual'}),/Origin/);transport.close();
});

test('response body failure, overflow and cancellation preserve headers and settle transport once',async()=>{
 let calls=0;const server=http.createServer((req,res)=>{calls++;res.writeHead(200,{'X-Antfly-Version':version});res.write('x'.repeat(2048));if(req.url==='/overflow')res.end();else if(req.url==='/broken')setTimeout(()=>res.destroy(),10);});await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port,pool=nodeHttpTransport({allowLoopbackHttp:true,maxResponseBytes:1024});
 try{const overflow=await pool.fetch(origin+'/overflow',{redirect:'manual'});assert.equal(overflow.status,200);assert.equal(overflow.headers.get('X-Antfly-Version'),version);await assert.rejects(overflow.text(),/ResponseExceeded/);pool.close();}finally{server.closeAllConnections();await new Promise(r=>server.close(r));}assert.equal(calls,1);assert.equal(pool.report().completed,1);assert.equal(pool.report().active,0);assert.equal(pool.report().failures,1);
 const hanging=http.createServer((req,res)=>{res.writeHead(200);res.write('x');if(req.url==='/broken')setTimeout(()=>res.destroy(),10);});await new Promise(r=>hanging.listen(0,'127.0.0.1',r));const second=nodeHttpTransport({allowLoopbackHttp:true}),url='http://127.0.0.1:'+hanging.address().port;
 try{const broken=await second.fetch(url+'/broken',{redirect:'manual'});assert.equal(broken.status,200);await assert.rejects(broken.text(),/BodyFailed/);const cancel=await second.fetch(url+'/cancel',{redirect:'manual'});await cancel.body.cancel();await new Promise(r=>setTimeout(r,10));assert.equal(second.report().completed,2);assert.equal(second.report().active,0);assert.equal(second.report().failures,2);}finally{second.close();hanging.closeAllConnections();await new Promise(r=>hanging.close(r));}
});

test('a committed write from another incarnation never advances the offered-load receipt',async()=>{
 const other='01'+'b'.repeat(64)+'0000000000000002',tasks=[{attempt_id:crypto.randomUUID(),offset_ms:0,step:{op:'mutate',tenant:'a',sequence:1}}],transport={origin,token,report:()=>({peak_queued:0}),fetch:async()=>Response.json({committed:true,visibility:other},{headers:{'X-Antfly-Version':version}})},result=await runIndependent({plan,tasks,receipts,transport,maxDispatchLagMs:1000});assert.equal(result.passed,false);assert.equal(result.samples[0].error,'TrialFenceMismatch');assert.equal(result.receipts.a,fence);
});
