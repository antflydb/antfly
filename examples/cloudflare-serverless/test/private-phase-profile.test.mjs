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

import{test}from'node:test';import assert from'node:assert/strict';
import{applicationTracePhases,profilePrivateRecords}from'../qualification/private-phase-profile.mjs';
const record=(logs,extra={})=>({event:{outcome:'ok',cpuTime:2,wallTime:20,logs:[{message:logs.map(v=>JSON.stringify(v))}],...extra}});
test('private phase profiling follows application traces without inventing provider identities',()=>{
 const app={...record([{event:'antfly.trace',scope:'http',trace_id:'logical-write'}]),attempt_id_sha256:'attempt'},mapping=applicationTracePhases([app],new Map([['attempt',{phase:'write'}]]));
 const r=profilePrivateRecords([record([{event:'antfly.trace',trace_id:'logical-write',scope:'wal',spans:[{name:'write_admission',duration_ms:7},{name:'native_wal_plan',duration_ms:11},{name:'native_wal_finalize',duration_ms:3}]},{event:'antfly.coordinator_group',queue_wait_max_ms:4,physical_r2:{calls:3,wait_union_ms:8},trace:{trace_id:'logical-write',scope:'coordinator',spans:[{name:'wal_commit',duration_ms:5}]}}])],mapping.phases);
 assert.equal(r.qualification,false);assert.equal(r.correlated_native_records,1);assert.equal(r.metrics['write/trace:wal/write_admission'].p95,7);assert.equal(r.metrics['write/trace:coordinator/wal_commit'].p95,5);assert.equal(r.metrics['write/antfly.coordinator_group/physical.wait_union_ms'].p95,8);assert.equal(r.metrics['write/terminal.cpu_ms'].samples,1);assert.equal(r.metrics['write/terminal.cpu_ms'].p95,2);
});
test('conflicting trace classifications are excluded rather than assigned to a convenient phase',()=>{
 const a={...record([{trace_id:'conflict'}]),attempt_id_sha256:'a'},b={...record([{trace_id:'conflict'}]),attempt_id_sha256:'b'};const mapping=applicationTracePhases([a,b,a],new Map([['a',{phase:'read'}],['b',{phase:'write'}]]));assert.equal(mapping.conflicts,1);assert.equal(mapping.phases.has('conflict'),false);
});
test('mixed private invocation phases retain phase spans but do not duplicate terminal CPU samples',()=>{
 const r=profilePrivateRecords([record([{event:'antfly.trace',trace_id:'a',scope:'wal',spans:[{name:'native_wal_plan',duration_ms:2}]},{event:'antfly.trace',trace_id:'b',scope:'coordinator',spans:[{name:'recovery',duration_ms:3}]}])],new Map([['a','write'],['b','read']]));assert.equal(r.mixed_phase_native_records,1);assert.equal(r.metrics['read/terminal.cpu_ms'],undefined);assert.equal(r.metrics['write/terminal.cpu_ms'],undefined);assert.equal(r.metrics['read/trace:coordinator/recovery'].p95,3);
});
test('unmatched private resource failures and truncated records remain visible',()=>{
 const rows=[record([{event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM'}]),record([],{outcome:'exceededCpu',truncated:true}),{event:{exceptions:[{message:'OutOfMemory'}],logs:[{message:['invalid json']}]}}];const r=profilePrivateRecords(rows,new Map());assert.equal(r.received_resource_records,3);assert.equal(r.truncated_records,1);assert.equal(r.correlated_native_records,0);assert.deepEqual(r.metrics,{});
});
