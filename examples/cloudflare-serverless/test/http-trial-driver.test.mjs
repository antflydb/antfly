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

import{test}from'node:test';import assert from'node:assert/strict';import driver,{validatePlan,runPlan,runRounds,documentBody}from'../qualification/http-trial-driver.mjs';
const version='01234567-0123-4567-89ab-0123456789ab',fence='01'+'a'.repeat(64)+'0000000000000001',token='b'.repeat(64),origin='https://antfly-1278-cold-abc-1234.example.workers.dev',plan={side:'baseline',id:'paired-one',version,steps:[{op:'lookup',tenant:'a',id:1,visibility:fence}]};
test('remote driver validates bodies fences and versions without exposing credentials',async()=>{
 const fetch=async(url,init)=>{assert.equal(init.redirect,'manual');assert.equal(init.headers.Authorization,'Bearer '+token);assert.equal(new URL(url).origin,origin);return Response.json({body:documentBody('a',1),visibility:fence},{headers:{'X-Antfly-Version':version}});};const result=await runPlan(plan,{origin,token,fetch});assert.equal(result.passed,true);assert.ok(!JSON.stringify(result).includes(token));
});
test('remote driver retains every failed member and does not retry or replay writes',async()=>{
 let calls=0;const result=await runPlan({...plan,steps:[plan.steps[0],{op:'mutate',tenant:'b',sequence:1}]},{origin,token,fetch:async()=>{calls++;return Response.json({error:'SQLITE_NOMEM secret='+token},{status:503});}});assert.equal(calls,2);assert.equal(result.passed,false);assert.deepEqual(result.samples.map(s=>s.error),['SQLITE_NOMEM','SQLITE_NOMEM']);assert.ok(!JSON.stringify(result).includes(token));
});
test('remote driver rejects mismatched generations bodies and rollout versions',async()=>{
 for(const[value,headers,error]of[[{body:'wrong',visibility:fence},{'X-Antfly-Version':version},'TrialBodyMismatch'],[{body:documentBody('a',1),visibility:'01'+'c'.repeat(64)+'0000000000000001'},{'X-Antfly-Version':version},'TrialFenceMismatch'],[{body:documentBody('a',1),visibility:fence},{'X-Antfly-Version':'old'},'TrialVersionMismatch']]){const r=await runPlan(plan,{origin,token,fetch:async()=>Response.json(value,{headers})});assert.equal(r.samples[0].error,error);}
});
test('remote driver rejects foreign destinations arbitrary operations and unauthenticated calls',async()=>{
 await assert.rejects(runPlan(plan,{origin:'https://example.com',token}),/InvalidTrialDestination/);assert.throws(()=>validatePlan({...plan,steps:[{op:'delete',tenant:'a'}]}),/InvalidTrialStep/);assert.throws(()=>validatePlan({...plan,steps:Array(17).fill(plan.steps[0])}),/InvalidTrialPlan/);assert.equal((await driver.fetch(new Request('https://driver.invalid/run',{method:'POST',body:JSON.stringify(plan)}),{})).status,401);
});

test('remote driver rejects redirects without forwarding credentials',async()=>{
 let calls=0;const r=await runPlan(plan,{origin,token,fetch:async(url,init)=>{calls++;assert.equal(init.redirect,'manual');return new Response(null,{status:302,headers:{Location:'https://example.com'}});}});assert.equal(calls,1);assert.equal(r.samples[0].error,'TrialRedirectRejected');
});

test('remote rounds advance visibility only after completed durable writes and never replay a sequence',async()=>{
 const seen=[],steps=[plan.steps[0],{op:'mutate',tenant:'a',sequence:20}],r=await runRounds({...plan,steps,rounds:2},{origin,token,fetch:async(url,init)=>{const input=JSON.parse(init.body);seen.push(input);return Response.json(input.op==='mutate'?{committed:true,visibility:fence.slice(0,-16)+'0000000000000002'}:{body:documentBody('a',1),visibility:input.required_visibility},{headers:{'X-Antfly-Version':version}});}});assert.equal(r.passed,true);assert.equal(r.samples.length,4);assert.equal(seen.filter(s=>s.op==='lookup')[1].required_visibility.slice(-16),'0000000000000002');assert.deepEqual(seen.filter(s=>s.op==='mutate').map(s=>s.operation_id),['paired/paired-one/a/20','paired/paired-one/a/21']);
});

test('remote driver cancels oversized streamed input before dispatching operations',async()=>{
 let cancelled=false;const stream=new ReadableStream({start(c){c.enqueue(new Uint8Array(16385));},cancel(){cancelled=true;}}),response=await driver.fetch(new Request('https://driver.invalid/run',{method:'POST',headers:{Authorization:'Bearer '+token},body:stream,duplex:'half'}),{API_TOKEN:token});assert.equal(response.status,400);assert.equal(cancelled,true);
});
