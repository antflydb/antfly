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

import{test}from'node:test';import assert from'node:assert/strict';import{placementProbe,placementStorageProbe}from'../placement-probe.mjs';
test('placement probes are opt-in read-only and preserve unknown geography',async()=>{
 let reads=0,now=0;const roles=[],env={QUALIFICATION_PLACEMENT_PROBE:'true',COORDINATOR_BACKEND:'r2',ANTFLY_INCARNATION:'private-namespace',R2_LOCATION_HINT:'wnam',DO_LOCATION_HINT:'enam',RUNTIME_GENERATION:'probe-1',WORKER_VERSION:{id:'version'},DATABASE:{async head(key){assert.match(key,/^antfly\/authority-v1\/[a-f0-9]{64}\/CURRENT$/);reads++;now+=3;return{};},put(){throw Error('write forbidden');}}};
 for(const[role,key]of [['writer','COMMITS'],['reader','READERS'],['builder','BUILDERS']])env[key]={getByName(){return{async placementProbe(){roles.push(role);return placementStorageProbe(env,role,'us',{clock:()=>now});}};}};
 const report=await placementProbe({cf:{colo:'SFO'}},env,{clock:()=>now});assert.equal(reads,4);assert.deepEqual(roles,['writer','reader','builder']);assert.equal(report.app_colo,'SFO');assert.equal(report.actual_bucket_region,null);assert.equal(report.compute_location_hint,'enam');assert.equal(report.bucket_location_hint,'wnam');assert.equal(report.qualification,false);assert.equal(report.consumes_compute_identities,true);assert.ok(report.samples.every(s=>s.storage_head_ms===3&&s.actual_actor_region===null));assert.ok(!JSON.stringify(report).includes('private-namespace'));
 await assert.rejects(placementProbe({}, {...env,QUALIFICATION_PLACEMENT_PROBE:'false'}),/PlacementProbeDisabled/);assert.equal(reads,4);
});
test('a failed placement probe preserves failure and starts no later actor calls',async()=>{
 let later=0;const env={QUALIFICATION_PLACEMENT_PROBE:'true',COORDINATOR_BACKEND:'r2',ANTFLY_INCARNATION:'n',DATABASE:{head:async()=>null},COMMITS:{getByName(){return{placementProbe:async()=>{throw Error('storage failure');}};}},READERS:{getByName(){later++;}},BUILDERS:{getByName(){later++;}}};await assert.rejects(placementProbe({},env),/storage failure/);assert.equal(later,0);
});


test('placement collection checks immutable configuration and preserves measured versus hinted fields',async()=>{
 const{collectPlacement}=await import('../qualification/probe-placement.mjs');let calls=0;const value={qualification:false,consumes_compute_identities:true,actual_bucket_region:null,runtime_generation:'generation',bucket_location_hint:'wnam',compute_location_hint:'enam',samples:['app','writer','reader','builder'].map(role=>({role,worker_version:'v',storage_head_ms:3,rpc_ms:role==='app'?null:5,actual_actor_region:null}))};
 const report=await collectPlacement('https://worker.invalid','private-token',{samples:3,fetcher:async(url,options)=>{calls++;assert.equal(url.pathname,'/_qualification/placement');assert.equal(options.headers.Authorization,'Bearer private-token');return Response.json(value);}});assert.equal(calls,3);assert.equal(report.summary.writer.rpc_ms.p95,5);assert.equal(report.actual_geography_verified,false);assert.ok(!JSON.stringify(report).includes('private-token'));
 calls=0;await assert.rejects(collectPlacement('https://worker.invalid','t',{samples:3,fetcher:async()=>Response.json({...value,runtime_generation:String(calls++)})}),/PlacementProbeVersionChanged/);
 await assert.rejects(collectPlacement('http://remote.invalid','t'),/InvalidPlacementProbe/);
});


test('placement HTTP resource causes remain in failed evidence without raw exception disclosure',async()=>{
 const{collectPlacement}=await import('../qualification/probe-placement.mjs');await assert.rejects(collectPlacement('https://worker.invalid','token',{fetcher:async()=>Response.json({error:'PlacementProbeFailed',failure_code:'SQLITE_NOMEM'},{status:503})}),/PlacementProbeHttp503:SQLITE_NOMEM/);
});
