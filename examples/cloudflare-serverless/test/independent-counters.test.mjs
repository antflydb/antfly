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

import{test}from'node:test';import assert from'node:assert/strict';import{resolve}from'node:path';
import{counterQuery,parseCounter,collectIndependentCounters}from'../qualification/independent-counters.mjs';
const id='antfly-1278-g-counter-1-c',dir=resolve('dist',id),run={id,account:'a'.repeat(32),bucket:id,worker:id,queues:[id,id+'-dead'],dir,token_file:resolve(dir,'api-token'),services:['app','writer','reader','builder'].map(role=>({role,name:id+(role==='app'?'':'-'+role)}))},timeframe={from:1000,to:60999};
const data=(count=5,interval=1)=>({data:{viewer:{accounts:[{workersInvocationsAdaptive:[{sum:{requests:count,errors:0},avg:{sampleInterval:interval}}]}]}}});
test('independent counters bind exact disposable script, account and bounded window',()=>{
 const query=counterQuery(run,'builder',timeframe).query;assert.ok(query.includes('scriptName:"'+id+'-builder"'));assert.ok(query.includes('accountTag:"'+'a'.repeat(32)+'"'));assert.throws(()=>counterQuery({...run,id:'production'},'app',timeframe),/Scope/);assert.throws(()=>counterQuery(run,'unknown',timeframe),/Scope/);assert.throws(()=>counterQuery(run,'app',{from:0,to:60001}),/Scope/);
});
test('sampled, empty, fractional and unknown counters cannot establish independent coverage',()=>{
 assert.equal(parseCounter(data(),run,'writer',timeframe).complete,true);assert.equal(parseCounter(data(5,2),run,'writer',timeframe).complete,false);
 const empty=data();empty.data.viewer.accounts[0].workersInvocationsAdaptive=[];assert.deepEqual([parseCounter(empty,run,'app',timeframe).sample_interval,parseCounter(empty,run,'app',timeframe).complete],[null,false]);
 assert.throws(()=>parseCounter(data(1.2),run,'app',timeframe),/Schema/);assert.throws(()=>parseCounter({errors:[{message:'private'}]},run,'app',timeframe),/Query/);
});
test('independent counter collection bounds responses and never persists credentials or provider text',async()=>{
 const token='private-credential-do-not-persist',result=await collectIndependentCounters(run,token,timeframe,{fetch:async(url,options)=>{assert.equal(url,'https://api.cloudflare.com/client/v4/graphql');assert.equal(options.headers.Authorization,'Bearer '+token);return Response.json(data());}});assert.equal(Object.keys(result.roles).length,4);assert.equal(result.failures.length,0);assert.ok(!JSON.stringify(result).includes(token));
 const denied=await collectIndependentCounters(run,token,timeframe,{fetch:async()=>Response.json({errors:[{message:token}]},{status:403})});assert.equal(denied.failures.length,4);assert.ok(!JSON.stringify(denied).includes(token));
 const big=await collectIndependentCounters(run,token,timeframe,{fetch:async()=>new Response('x'.repeat(65537))});assert.ok(big.failures.every(failure=>failure.reason==='IndependentCounterResponseExceeded'));
});
