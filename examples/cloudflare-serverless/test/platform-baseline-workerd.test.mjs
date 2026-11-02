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

import{test}from'node:test';import assert from'node:assert/strict';import{localPlatformBaseline}from'../qualification/platform-baseline-local.mjs';import{runPlatformBaseline}from'../qualification/platform-baseline-client.mjs';
test('workerd primitive baselines validate every response under bounded storage/WASM pressure',{timeout:30000},async()=>{
 const fixture=await localPlatformBaseline({payload:262144,pages:512,retained:8*1024*1024});try{const send=(path,body,auth=true)=>fixture.fetch(new URL(path,fixture.origin),{method:'POST',headers:auth?{Authorization:'Bearer '+fixture.token}:{},body:body==null?undefined:JSON.stringify(body)});assert.equal((await send('/prepare',null,false)).status,401);assert.equal((await send('/prepare')).status,200);const result=await runPlatformBaseline({...fixture,samples:8,rates:[],maxDispatchLagMs:1000});assert.equal(result.passed,true,JSON.stringify(result.cases.filter(c=>!c.passed)));assert.equal(result.cases.length,9);for(const row of result.cases.find(c=>c.op==='owner_put').samples){assert.equal(row.memory.linear_bytes,32*1024*1024);assert.equal(row.memory.retained_bytes,8*1024*1024);assert.equal(row.memory.total_isolate_bytes,null);}const fresh=await send('/probe',{id:crypto.randomUUID(),op:'owner_echo',padding:'x'.repeat(1024),fresh:true});assert.equal(fresh.status,503);assert.equal((await send('/probe',{id:crypto.randomUUID(),op:'sql',padding:'x'.repeat(1024)})).status,503);}finally{await fixture.close();}
});
