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

import{mkdir,writeFile}from'node:fs/promises';import{resolve}from'node:path';import{execFileSync}from'node:child_process';import{loadavg}from'node:os';import{localPlatformBaseline}from'./platform-baseline-local.mjs';import{runPlatformBaseline}from'./platform-baseline-client.mjs';
const root=resolve('dist/qualification/platform-baseline-'+Date.now());await mkdir(root,{recursive:true});const report={qualification:false,hosted:false,full_gate_passed:false,kind:'actual workerd; local SQLite/R2 emulation; synthetic WASM pressure, not Antfly engine memory',source:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),dirty:!!execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim(),load_start:loadavg(),cases:[],complete:false};const save=()=>writeFile(resolve(root,'report.json'),JSON.stringify(report,null,2));
for(const profile of[{name:'primitives',payload:1024,pages:0,retained:0},{name:'large-storage',payload:262144,pages:0,retained:0},{name:'linear-pressure',payload:262144,pages:512,retained:8*1024*1024}]){
 const logs=[],fixture=await localPlatformBaseline({...profile,logs});try{const prepared=await(await fixture.fetch(new URL('/prepare',fixture.origin),{method:'POST',headers:{Authorization:'Bearer '+fixture.token}})).json();if(!prepared.prepared)throw Error('BaselinePreparationFailed');const result=await runPlatformBaseline({...fixture,samples:16,rates:[16,64],ops:profile.name==='primitives'?undefined:['owner_echo','owner_get','owner_put','owner_r2_get','owner_r2_put','r2_get','r2_put'],maxDispatchLagMs:25});report.cases.push({profile,prepared,result,logs});await save();if(profile.name==='primitives'){const cold=await runPlatformBaseline({...fixture,samples:16,rates:[],ops:['owner_echo'],freshOwner:true});report.cases.push({profile:{name:'fresh-owner-no-storage'},result:cold});await save();}}finally{await fixture.close();}
}
report.load_end=loadavg();report.complete=true;report.passed=report.cases.every(c=>c.result.passed);await save();console.log(JSON.stringify({root,passed:report.passed,cases:report.cases.length,hosted:false}));if(!report.passed)process.exitCode=1;
