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

import{test}from'node:test';import assert from'node:assert/strict';import{reserveIoBranch}from'../io-branch.mjs';
test('concurrent preparation and recovery reserve disjoint original I/O allowances',()=>{
 const parent={operations:0,bytes:0},claim=reserveIoBranch(parent);assert.equal(parent.operations,512);assert.equal(1024-claim.budget.operations,512);assert.equal(32*1024*1024-parent.bytes,16*1024*1024);
 parent.operations+=8;parent.bytes+=4096;claim.budget.operations+=3;claim.budget.bytes+=2048;claim.join();assert.deepEqual(parent,{operations:11,bytes:6144});claim.join();assert.deepEqual(parent,{operations:11,bytes:6144});
});
test('I/O branch reservation rejects exhaustion and refuses to forgive child overspend',()=>{
 assert.throws(()=>reserveIoBranch({operations:513,bytes:0}),/CoordinatorIoBudgetExceeded/);const parent={operations:0,bytes:0},claim=reserveIoBranch(parent);claim.budget.operations=1025;assert.throws(()=>claim.join(),/CoordinatorIoBudgetExceeded/);assert.equal(parent.operations,512);
});
