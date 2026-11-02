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

import{mkdir,writeFile,appendFile}from'node:fs/promises';import{createHash}from'node:crypto';
const raw=process.env.ANTFLY_HTTP_TRIAL_PLAN;if(!raw||Buffer.byteLength(raw)>65536)throw Error('IndependentPublicPlanRequired');const plan=JSON.parse(raw),now=Date.now();
function publicOnly(value){if(!value||typeof value!=='object')return;for(const[k,v]of Object.entries(value)){if(/token|secret|password|authorization|credential/i.test(k))throw Error('IndependentPlanContainsCredentialField');publicOnly(v);}}
publicOnly(plan);if(plan.schema!==1||plan.kind!=='independent-http-plan'||!plan.observer||plan.runner_source_revision!==process.env.EXPECTED_SOURCE_SHA||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(plan.execution_id??'')||!Number.isSafeInteger(plan.created_at)||now-plan.created_at>3600000||plan.created_at>now+300000)throw Error('IndependentPublicPlanInvalid');
await mkdir('dist',{recursive:true});await mkdir('dist/independent-ci',{recursive:false});await writeFile('dist/independent-ci/plan.json',JSON.stringify(plan),{flag:'wx',mode:0o600});await writeFile('dist/independent-ci/claim.json',JSON.stringify({execution_id:plan.execution_id,plan_sha256:createHash('sha256').update(raw).digest('hex'),source:plan.runner_source_revision}),{flag:'wx',mode:0o600});if(process.env.GITHUB_OUTPUT)await appendFile(process.env.GITHUB_OUTPUT,'execution_id='+plan.execution_id+'\n');
