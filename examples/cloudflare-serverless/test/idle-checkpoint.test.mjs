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

import{test}from'node:test';import assert from'node:assert/strict';import{IdleCheckpoint,replayCheckpointDelay}from'../idle-checkpoint.mjs';
function fixture(checkpoint,options={}){let time=1000;const work=[],errors=[];const state=new IdleCheckpoint({revision:()=>7,isIdle:()=>true,checkpoint,background:p=>work.push(p),onError:e=>errors.push(e.message),clock:()=>time,monotonic:()=>time,sleep:async ms=>{time+=ms;},delay:1,retryDelay:2,...options});return{state,work,errors};}
test('checkpoint busy deferrals preserve the dirty revision until a later bounded retry succeeds',async()=>{let calls=0;const f=fixture(async()=>++calls<=2?{busy:true}:{done:true,revision:7});f.state.schedule();await f.work[0];assert.equal(calls,3);assert.equal(f.state.last,7);assert.equal(f.state.deferrals,2);assert.equal(f.state.pending,false);});
test('a busy admission gets retried without a new foreground transaction',async()=>{let checks=0,calls=0;const f=fixture(async()=>{calls++;return{done:true,revision:7};},{isIdle:()=>++checks>2});f.state.schedule();await f.work[0];assert.equal(calls,1);assert.equal(f.state.last,7);assert.equal(f.state.deferrals,2);});
test('checkpoint failures remain observable after recovery and fenced owners stop immediately',async()=>{let calls=0;const f=fixture(async()=>{if(++calls===1)throw Error('SQLITE_NOMEM');return{done:true,revision:7};});f.state.schedule();await f.work[0];assert.deepEqual(f.errors,['SQLITE_NOMEM']);assert.equal(f.state.last,7);assert.equal(f.state.failures,1);const fenced=fixture(async()=>{throw Error('ImmutableLogFenced');});fenced.state.schedule();await fenced.work[0];assert.equal(fenced.state.attempts,1);assert.equal(fenced.state.last,0);});
test('checkpoint retries keep one original time budget and leave an exhausted target dirty',async()=>{let calls=0;const f=fixture(async({deadlineMs})=>{assert.equal(deadlineMs,1004);calls++;return{busy:true};},{timeoutMs:4});f.state.schedule();await f.work[0];assert.equal(calls,2);assert.equal(f.state.last,0);assert.equal(f.state.pending,false);assert.equal(f.work.length,1);});

test('replay scheduling bounds quiet age and advances high debt without changing native authority',async()=>{
 assert.equal(replayCheckpointDelay({commits:1,ageMs:100}),900);assert.equal(replayCheckpointDelay({commits:8}),0);assert.equal(replayCheckpointDelay({bytes:65536}),0);assert.equal(replayCheckpointDelay({waves:4}),0);assert.equal(replayCheckpointDelay({ageMs:1001}),0);assert.throws(()=>replayCheckpointDelay({waves:NaN}),/InvalidCheckpointReplayCost/);
 let deadline;const f=fixture(async options=>{deadline=options.deadlineMs;return{done:true,revision:7};},{replayCost:()=>({bytes:65536}),delay:250});f.state.schedule();await f.work[0];assert.equal(f.state.last,7);assert.equal(deadline,11000);
});

test('optional durable checkpoints report errors without transparent retries',async()=>{
 const f=fixture(async()=>{throw Error('database is busy');},{shouldRetry:()=>false});
 f.state.schedule();await f.work[0];assert.equal(f.state.attempts,1);assert.equal(f.state.last,0);assert.equal(f.state.pending,false);assert.deepEqual(f.errors,['database is busy']);
});
