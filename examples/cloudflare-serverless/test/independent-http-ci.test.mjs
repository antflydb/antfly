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

import{test}from'node:test';import assert from'node:assert/strict';import{mkdtemp,readFile,rm,access}from'node:fs/promises';import{resolve}from'node:path';import{spawnSync}from'node:child_process';
const script=resolve('qualification/ci-independent-http-plan.mjs'),source='a'.repeat(40),plan=()=>({schema:1,kind:'independent-http-plan',observer:{id:'fixture'},execution_id:crypto.randomUUID(),created_at:Date.now(),runner_source_revision:source});
test('CI claims a fresh public plan on a clean checkout and cannot overwrite that claim',async()=>{
 const dir=await mkdtemp(resolve('dist/independent-ci-test-'));try{const value=plan(),run=()=>spawnSync(process.execPath,[script],{cwd:dir,env:{...process.env,ANTFLY_HTTP_TRIAL_PLAN:JSON.stringify(value),EXPECTED_SOURCE_SHA:source},encoding:'utf8'});const first=run();assert.equal(first.status,0,first.stderr);const claim=JSON.parse(await readFile(resolve(dir,'dist/independent-ci/claim.json')));assert.equal(claim.execution_id,value.execution_id);assert.equal(claim.source,source);assert.notEqual(run().status,0);}finally{await rm(dir,{recursive:true});}
});
test('CI rejects credential-bearing, stale or mismatched-source plans before creating artifacts',async()=>{
 for(const change of[{observer:{token:'test-only-private-value'}},{runner_source_revision:'b'.repeat(40)},{created_at:Date.now()-3600001}]){const dir=await mkdtemp(resolve('dist/independent-ci-test-'));try{const result=spawnSync(process.execPath,[script],{cwd:dir,env:{...process.env,ANTFLY_HTTP_TRIAL_PLAN:JSON.stringify({...plan(),...change}),EXPECTED_SOURCE_SHA:source},encoding:'utf8'});assert.notEqual(result.status,0);assert.ok(!result.stderr.includes('test-only-private-value'));await assert.rejects(access(resolve(dir,'dist/independent-ci')));}finally{await rm(dir,{recursive:true});}}
});
