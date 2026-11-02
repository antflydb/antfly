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

import{test}from'node:test';import assert from'node:assert/strict';import{runPlatformBaseline}from'../qualification/platform-baseline-client.mjs';import{baselineJson,baselineSettings}from'../qualification/platform-baseline-common.mjs';
const response=input=>Response.json({schema:1,qualification:false,id:input.id,op:input.op,payload:'x'.repeat(1024),version:'version-a',http_ms:0,http_stages:[],settings:{payload_bytes:1024,linear_pages:0,retained_bytes:0}});
test('a recovered memory failure and version change remain failed samples without workload retry',async()=>{
 let calls=0;const result=await runPlatformBaseline({origin:'https://fixture.invalid',token:'fixture',samples:8,rates:[],ops:['echo'],fetch:async(url,init)=>{calls++;const input=JSON.parse(init.body);if(calls===1)return Response.json({error:'SQLITE_NOMEM'},{status:503});if(calls===3){const value=await response(input).json();return Response.json({...value,version:'version-b'});}return response(input);}});assert.equal(calls,8);assert.equal(result.complete,true);assert.equal(result.passed,false);assert.equal(result.cases[0].samples.filter(s=>s.outcome==='ok').length,6);assert.equal(result.cases[0].samples[0].error,'SQLITE_NOMEM');assert.equal(result.cases[0].samples[2].error,'BaselineVersionChanged');
});
test('offered load records every overload without silently moving arrivals',async()=>{
 let clock=0,calls=0;const result=await runPlatformBaseline({origin:'https://fixture.invalid',token:'fixture',samples:8,rates:[128],ops:['echo'],maxInFlight:1,clock:()=>clock,sleep:async ms=>{clock+=ms;},fetch:async(url,init)=>{calls++;if(calls>8)await new Promise(r=>setTimeout(r,10));return response(JSON.parse(init.body));}});assert.equal(result.cases.length,2);assert.equal(result.cases[1].samples.length,8);assert.equal(result.cases[1].samples.filter(s=>s.error==='BaselineClientOverloaded').length,7);assert.equal(result.passed,false);
});
test('request decoding cancels oversized streamed input before materializing it and rejects unsafe pressure settings',async()=>{
 let canceled=false;const body=new ReadableStream({start(c){c.enqueue(new Uint8Array(4096));},cancel(){canceled=true;}});await assert.rejects(baselineJson(new Request('https://fixture.invalid',{method:'POST',body,duplex:'half'})),/Exceeded/);assert.equal(canceled,true);for(const env of[{BASELINE_LINEAR_PAGES:2048},{BASELINE_RETAINED_BYTES:128*1024*1024},{BASELINE_PAYLOAD_BYTES:10*1024*1024}])assert.throws(()=>baselineSettings(env),/Invalid/);
});

test('cancellation joins an already dispatched physical request before the baseline returns',async()=>{
 let clock=0,calls=0,release,observed;const held=new Promise(r=>release=r),reached=new Promise(r=>observed=r),abort=new AbortController();let done=false;const running=runPlatformBaseline({origin:'https://fixture.invalid',token:'fixture',samples:8,rates:[8],ops:['echo'],signal:abort.signal,maxInFlight:1,clock:()=>clock,sleep:async ms=>{clock+=ms;abort.abort();observed();},fetch:async(url,init)=>{if(++calls>8)await held;return response(JSON.parse(init.body));}}).then(r=>{done=true;return r;});await reached;await new Promise(r=>setImmediate(r));assert.equal(done,false);release();const result=await running;assert.equal(result.passed,false);assert.equal(calls,9);
});
