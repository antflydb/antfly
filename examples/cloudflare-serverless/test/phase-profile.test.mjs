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

import{test}from'node:test';import assert from'node:assert/strict';import{CriticalPath}from'../trace.mjs';import{phaseProfile,profileResult,phaseProfileHeaders,readPhaseProfile}from'../phase-profile.mjs';import{MutationGroups}from'../mutation-groups.mjs';
const env={QUALIFICATION_PHASE_PROFILE:'true',API_TOKEN:'must-not-appear',RESIDENT_SERVING:'true'};
test('response attribution keeps isolate-local spans and safe counters, strips internals and is opt-in',()=>{
 let now=0;const trace=new CriticalPath({clock:()=>now,scope:'http'});now=10;trace.record('resident_rpc',0);trace.record('rpc:openReadView',0);const owner=new CriticalPath({id:trace.id,scope:'resident:lookup',clock:()=>now});now=12;owner.record('owner_admission',10);const result=profileResult({body:'private-data'},owner,env,{physical:{calls:2,wait_union_ms:1,key:'secret-key'},metrics:{operations:3},traceIds:[trace.id]});const headers=phaseProfileHeaders(result,trace,env),value=readPhaseProfile(headers['X-Antfly-Profile']);assert.equal(value.owner.physical.calls,2);assert.equal(value.http.elapsed_ms,12);assert.equal(value.owner.elapsed_ms,2);assert.equal(result.__antfly_profile,undefined);assert.ok(!JSON.stringify(headers).includes('secret-key'));assert.ok(!JSON.stringify(headers).includes(env.API_TOKEN));assert.ok(!JSON.stringify(headers).includes('private-data'));assert.equal(phaseProfile(trace,{}),undefined);const disabled={...result,__antfly_profile:{private:true}};assert.deepEqual(phaseProfileHeaders(disabled,trace,{}),{});assert.equal(disabled.__antfly_profile,undefined);
});
test('truncated, error-bearing and mismatched attribution never becomes complete',()=>{
 const trace=new CriticalPath({scope:'http'});for(let i=0;i<200;i++)trace.record('stage',trace.started);const r=profileResult({},trace,env);assert.throws(()=>readPhaseProfile(phaseProfileHeaders(r,trace,env)['X-Antfly-Profile']),/Incomplete/);const a=new CriticalPath({scope:'http'}),b=new CriticalPath({scope:'wal'});assert.throws(()=>readPhaseProfile(phaseProfileHeaders(profileResult({},b,env),a,env)['X-Antfly-Profile']),/Mismatch/);a.error('storage',Error('SQLITE_NOMEM'));assert.throws(()=>readPhaseProfile(phaseProfileHeaders({},a,env)['X-Antfly-Profile']),/Incomplete/);
});
test('queued mutation groups reuse time already spent coalescing without answering before durability',async()=>{
 let now=0,finish;const sleeps=[],commits=[],groups=new MutationGroups(async inputs=>{commits.push(inputs);if(commits.length===1)await new Promise(r=>finish=r);return inputs.map(()=>({committed:true}));},{reuseElapsedWindow:true,monotonic:()=>now,sleep:async ms=>{sleeps.push(ms);now+=ms;}});
 let replied=false;const first=groups.submit({mutations:[]}).then(v=>{replied=true;return v;});await new Promise(r=>setImmediate(r));assert.equal(replied,false);const next=groups.submit({mutations:[]});now+=20;finish();await Promise.all([first,next]);assert.deepEqual(sleeps,[2]);assert.equal(commits.length,2);assert.equal(groups.pending,0);assert.equal(groups.bytes,0);
});

test('phase summaries use same-request residuals and retain missing attribution',async()=>{
 const{attributeRequestPhases}=await import('../qualification/phase-attribution.mjs');let now=0;const h=new CriticalPath({scope:'http',clock:()=>now}),o=new CriticalPath({id:h.id,scope:'wal',clock:()=>now});now=30;o.record('native_wal_finalize',10);const result=profileResult({},o,env);now=50;h.record('writer_rpc',0);const profile=readPhaseProfile(phaseProfileHeaders(result,h,env)['X-Antfly-Profile']),summary=attributeRequestPhases([{op:'write',ms:100,profile}]);assert.equal(summary.complete,true);assert.equal(summary.metrics['write/outside_http_ms'].p95,50);assert.equal(summary.metrics['write/rpc_outside_owner_ms'].p95,20);assert.equal(attributeRequestPhases([{op:'read',ms:1}]).complete,false);assert.equal(summary.full_gate_passed,false);
});
