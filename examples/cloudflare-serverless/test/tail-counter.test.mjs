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

import {test}from'node:test';import assert from'node:assert/strict';
import{persistTailDelivery}from'../qualification/tail-counter.mjs';import{Bucket}from'./object-fixture.mjs';
const source='antfly-1278-telemetry-fixture-source',version=crypto.randomUUID(),capture=crypto.randomUUID();
const item=()=>({scriptName:source,scriptVersion:{id:version},eventTimestamp:1000,outcome:'ok',cpuTime:1,wallTime:10,event:{request:{url:'https://secret.invalid',headers:{Authorization:'private-secret'}}},logs:[],exceptions:[]});
test('provider tail counters persist immutable delivery counts without claiming invocation identities',async()=>{const env={CAPTURE_ID:capture,SOURCE_WORKER:source,LOGS:new Bucket()};await persistTailDelivery([item(),item(),{...item(),scriptName:'unrelated'}],env);const page=await env.LOGS.list({prefix:'probe-v1/'+capture+'/counter/'}),value=await(await env.LOGS.get(page.objects[0].key)).json();assert.equal(value.count,2);assert.equal(value.foreign_events,1);assert.equal(value.source,'provider-tail-dispatch');assert.equal(value.records[0].version_id,version);for(const forbidden of ['secret.invalid','private-secret','invocation_id'])assert.ok(!JSON.stringify(value).includes(forbidden));await persistTailDelivery([item()],env);assert.equal(env.LOGS.objects.size,2);});
test('tail delivery failures, source bounds and malformed source timestamps remain failures',async()=>{const env={CAPTURE_ID:capture,SOURCE_WORKER:source,LOGS:new Bucket()};await assert.rejects(persistTailDelivery([item()],{...env,SOURCE_WORKER:'production'}),/Scope/);await assert.rejects(persistTailDelivery([{...item(),eventTimestamp:NaN}],env),/Timestamp/);await assert.rejects(persistTailDelivery([item()],{...env,LOGS:{put:async()=>{throw Error('lost ack');}}}),/lost ack/);await assert.rejects(persistTailDelivery([item()],{...env,LOGS:{put:async()=>null}}),/Collision/);});
test('independent tail records preserve late resource outcomes and truncation',async()=>{const env={CAPTURE_ID:capture,SOURCE_WORKER:source,LOGS:new Bucket()},e=item();e.outcome='exceededMemory';e.truncated=true;e.logs=[{message:[JSON.stringify({event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM private-secret'})]}];await persistTailDelivery([e],env);const value=await(await env.LOGS.get([...env.LOGS.objects.keys()][0])).json();assert.equal(value.records[0].event.outcome,'exceededMemory');assert.equal(value.records[0].event.truncated,true);assert.ok(JSON.stringify(value).includes('SQLITE_NOMEM'));assert.ok(!JSON.stringify(value).includes('private-secret'));});

test('portable witness tokens match the existing physical-attempt identity without raw URLs',async()=>{const {attemptParameter,portableAttemptIdentity}=await import('../qualification/attempt-token.mjs'),{attemptIdentity}=await import('../qualification/invocation-accounting.mjs');const id=crypto.randomUUID(),e=item(),env={CAPTURE_ID:capture,SOURCE_WORKER:source,LOGS:new Bucket()};e.event.request.url='https://private.invalid/?__antfly_qualification_attempt='+attemptParameter(id);await persistTailDelivery([e],env);const value=await(await env.LOGS.get([...env.LOGS.objects.keys()][0])).json();assert.equal(value.records[0].attempt_id_sha256,attemptIdentity(id));assert.equal(await portableAttemptIdentity(id),attemptIdentity(id));assert.ok(!JSON.stringify(value).includes('private.invalid'));});
