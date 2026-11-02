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

import{test}from'node:test';import assert from'node:assert/strict';import{takeRpcData}from'../rpc-data.mjs';
test('RPC data owns typed bytes and nested values before its source group is disposed',async()=>{
 let disposed=0;const source={body:new Uint8Array([1,2,3]),nested:[{root:'accepted'}]};Object.defineProperty(source,Symbol.dispose,{value(){disposed++;source.body.fill(0);source.nested[0].root='released';}});
 const value=await takeRpcData(Promise.resolve(source));assert.equal(disposed,1);assert.deepEqual([...value.body],[1,2,3]);assert.equal(value.nested[0].root,'accepted');assert.equal(value[Symbol.dispose],undefined);
});
test('unsupported RPC capabilities still dispose on materialization failure and rejection is retained',async()=>{
 let disposed=0;const source={remote(){}};Object.defineProperty(source,Symbol.dispose,{value(){disposed++;}});await assert.rejects(takeRpcData(Promise.resolve(source)),/RpcDataContainsCapability/);assert.equal(disposed,1);await assert.rejects(takeRpcData(Promise.reject(Error('SQLITE_NOMEM'))),/SQLITE_NOMEM/);
});
test('primitive and local data results preserve their existing representation',async()=>{assert.equal(await takeRpcData(Promise.resolve(7)),7);const value={plain:true};assert.equal(await takeRpcData(value),value);assert.equal(await takeRpcData(null),null);});
test('both the pipelined promise and result group are released, including rejected promises',async()=>{let pendingDisposed=0,resultDisposed=0;const value={ok:true};Object.defineProperty(value,Symbol.dispose,{value(){resultDisposed++;}});const pending=Promise.resolve(value);pending[Symbol.dispose]=()=>pendingDisposed++;assert.deepEqual(await takeRpcData(pending),{ok:true});assert.equal(resultDisposed,1);assert.equal(pendingDisposed,1);const failed=Promise.reject(Error('failed'));failed[Symbol.dispose]=()=>pendingDisposed++;await assert.rejects(takeRpcData(failed),/failed/);assert.equal(pendingDisposed,2);});
