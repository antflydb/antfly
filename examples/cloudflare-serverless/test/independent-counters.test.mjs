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

test('Durable Object counters bind their namespace and require explicit unsampled metadata',async()=>{
 const{durableCounterQuery,parseDurableCounter}=await import('../qualification/independent-counters.mjs'),binding={id:'b'.repeat(32),worker:run.id+'-writer',source:'platform-namespace-api'};
 const value={data:{viewer:{accounts:[{durableObjectsInvocationsAdaptiveGroups:[{sum:{requests:7,errors:0},avg:{sampleInterval:1},confidence:{sum:{requests:{estimate:7,lower:7,upper:7,sampleSize:7}}}}]}]}}};
 assert.ok(durableCounterQuery(run,'writer',binding.id,timeframe).query.includes(binding.id));
 assert.equal(parseDurableCounter(value,run,'writer',binding,timeframe).complete,true);
 delete value.data.viewer.accounts[0].durableObjectsInvocationsAdaptiveGroups[0].avg;
 assert.throws(()=>parseDurableCounter(value,run,'writer',binding,timeframe),/Schema/,'matching confidence estimates cannot replace explicit sampling evidence');
 assert.throws(()=>durableCounterQuery(run,'app',binding.id,timeframe),/Scope/);
 assert.throws(()=>parseDurableCounter(value,run,'writer',{...binding,worker:run.id+'-reader'},timeframe),/Scope/);
});
test('mixed invocation accounting reconciles disjoint Worker and DO populations without inferred zeroes',async()=>{
 const{collectInvocationPopulations,reconcileCounterPopulations}=await import('../qualification/independent-counters.mjs');
 const result=await collectInvocationPopulations(run,'private-test-token-value',timeframe,{fetch:async(url,options)=>{
  if(url.includes('/namespaces?'))return Response.json({success:true,result:['writer','reader','builder'].map(role=>({id:({writer:'b',reader:'c',builder:'d'})[role].repeat(32),script:run.id+'-'+role})),result_info:{total_pages:1}});
  if(JSON.parse(options.body).query.includes('durableObjectsInvocationsAdaptiveGroups'))return Response.json({data:{viewer:{accounts:[{durableObjectsInvocationsAdaptiveGroups:[{sum:{requests:3,errors:0},avg:{sampleInterval:1}}]}]}}});
  return Response.json(data(2));
 }});
 assert.equal(result.failures.length,0);const counter=result.roles.builder;
 const rows=[...Array.from({length:2},()=>({event_type:'queue'})),...Array.from({length:3},()=>({event_type:'jsrpc'}))];
 assert.deepEqual(reconcileCounterPopulations(run,'builder',rows,counter,timeframe),[]);
 assert.ok(reconcileCounterPopulations(run,'builder',[...rows,{event_type:'workflow'}],counter,timeframe).includes('InvocationPopulationUncovered'));
 const sampled=structuredClone(counter);sampled.populations[1].sample_interval=2;assert.ok(reconcileCounterPopulations(run,'builder',rows,sampled,timeframe).includes('IndependentInvocationCountMissing'));
 const duplicate=structuredClone(counter);duplicate.populations.push(duplicate.populations[0]);assert.ok(reconcileCounterPopulations(run,'builder',rows,duplicate,timeframe).includes('InvocationPopulationInvalid'));
 const quiet=structuredClone(counter);quiet.populations[0]={...quiet.populations[0],count:0,complete:false,sample_interval:null};assert.ok(reconcileCounterPopulations(run,'builder',rows.slice(2),quiet,timeframe).includes('IndependentInvocationCountMissing'));
 assert.ok(!JSON.stringify(result).includes('private-test-token-value'));
});
test('namespace discovery failures and ambiguous bindings remain accounting failures',async()=>{
 const{collectInvocationPopulations}=await import('../qualification/independent-counters.mjs');
 for(const result of[{success:false,result:[]},{success:true,result:[{id:'b'.repeat(32),script:run.id+'-writer'},{id:'c'.repeat(32),script:run.id+'-writer'}]}]){
  const report=await collectInvocationPopulations(run,'private-test-token-value',timeframe,{fetch:async url=>Response.json(url.includes('/namespaces?')?result:data())});assert.ok(report.failures.some(f=>f.reason==='NamespaceBindingUnknown'));assert.ok(!JSON.stringify(report).includes('private-test-token-value'));
 }
});
