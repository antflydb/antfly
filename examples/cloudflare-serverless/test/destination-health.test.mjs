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

import{test}from'node:test';import assert from'node:assert/strict';import{evaluateDestinationHealth}from'../qualification/destination-health.mjs';import{observerDiagnosticsComplete}from'../qualification/paired-observer-client.mjs';
const scope={id:'observer-test',captureId:crypto.randomUUID(),from:120000,to:240000};
const fixture=()=>({id:scope.id,observation_capture_id:scope.captureId,at:400000,destinations:['logs','traces'].map(signal=>({signal,enabled:true,dataset:'opentelemetry-'+signal,last_complete:new Date(300000).toISOString(),last_error:new Date(60000).toISOString()}))});
test('delivery status requires both owned signals to complete after the measured window',()=>{assert.equal(evaluateDestinationHealth(fixture(),scope).complete,true);for(const change of[s=>s.destinations.pop(),s=>s.destinations[1].enabled=false,s=>s.destinations[1].last_complete=new Date(230000).toISOString(),s=>s.observation_capture_id=crypto.randomUUID(),s=>s.destinations[1].dataset='other']){const s=fixture();change(s);assert.equal(evaluateDestinationHealth(s,scope).complete,false);}});
test('a recovered export delivery error remains disqualifying after a later successful upload',()=>{const s=fixture();s.destinations[1].last_error=new Date(200000).toISOString();const proof=evaluateDestinationHealth(s,scope);assert.equal(proof.complete,false);assert.ok(proof.failures.includes('ExportDeliveryFailureObserved'));const diagnostics=Array(8).fill({complete:true});assert.equal(observerDiagnosticsComplete({}, {rejections_present:false},diagnostics,proof),false);assert.equal(observerDiagnosticsComplete({}, {rejections_present:false},diagnostics,{...evaluateDestinationHealth(fixture(),scope),history_checked:true}),true);assert.equal(observerDiagnosticsComplete({}, {rejections_present:false},diagnostics),false);});
