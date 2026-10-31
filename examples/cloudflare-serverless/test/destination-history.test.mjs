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

import{test}from'node:test';import assert from'node:assert/strict';import{evaluateDestinationHistory,startDestinationMonitor}from'../qualification/destination-history.mjs';
const scope={id:'observer-test',captureId:crypto.randomUUID(),from:120000,to:180000};
const status=at=>({id:scope.id,observation_capture_id:scope.captureId,at,destinations:['logs','traces'].map(signal=>({signal,enabled:true,dataset:'opentelemetry-'+signal,last_complete:new Date(at).toISOString(),last_error:''}))});
const samples=()=>[120000,150000,180000,210000].map(at=>({requested_at:at,at:at+100,status:status(at)}));
test('delivery history retains a provider error after last_error is cleared on recovery',()=>{const s=samples();assert.equal(evaluateDestinationHistory(s,scope).complete,true);s[1].status.destinations[1].last_error=new Date(140000).toISOString();const p=evaluateDestinationHistory(s,scope);assert.equal(s.at(-1).status.destinations[1].last_error,'');assert.equal(p.complete,false);assert.ok(p.failures.includes('ExportDeliveryFailureObserved'));});
test('delivery histories reject missed, unavailable, foreign, disabled and truncated observations',()=>{for(const change of[s=>s.splice(0,1),s=>s[1].status=null,s=>s[1].status.observation_capture_id=crypto.randomUUID(),s=>s[1].status.destinations[0].enabled=false,s=>s.splice(1,2),s=>s.length=1,s=>s[2].requested_at=s[1].requested_at]){const s=samples();change(s);assert.equal(evaluateDestinationHistory(s,scope).complete,false);}const s=samples();s[1].status.destinations[0].last_error=new Date(60000).toISOString();assert.equal(evaluateDestinationHistory(s,scope).complete,true,'errors predating this capture remain historical');});

test('the driver serializes delivery polls, persists unavailable samples and joins the final request',async()=>{
 const observations=[];let active=0,peak=0,calls=0,saved=0;
 const monitor=await startDestinationMonitor('scoped-manifest',{samples:observations,intervalMs:1,persist:async()=>{saved++;},readStatus:async path=>{assert.equal(path,'scoped-manifest');active++;peak=Math.max(peak,active);await new Promise(ok=>setTimeout(ok,3));active--;if(++calls===2)throw Error('private provider text');return status(Date.now());}});
 while(calls<3)await new Promise(ok=>setTimeout(ok,2));await monitor.stop();assert.equal(active,0);assert.equal(peak,1);assert.equal(saved,observations.length);assert.ok(observations.length>=4);assert.ok(observations.some(s=>s.error==='ExportDeliveryStatusUnavailable'));assert.ok(!JSON.stringify(observations).includes('private provider text'));
});
