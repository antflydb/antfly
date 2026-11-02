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

import{resolve,join}from'node:path';import{readFile}from'node:fs/promises';import{createHash}from'node:crypto';import{assertGateRun}from'./gate-scope.mjs';import{verifySealedRun}from'./gate-provenance.mjs';
const hash=b=>createHash('sha256').update(b).digest('hex');
export function measuredConfig(config){const{observability,tail_consumers,...runtime}=config;return runtime;}
export function assertObservationRun(run,{account,dir}={}){
 if(run.kind!=='cold-pilot-not-gate')return assertGateRun(run,{account,dir});
 if(!/^antfly-1278-cold-[a-z0-9]+-[a-f0-9]{4}$/.test(run.id??'')||!/^[a-f0-9]{32}$/.test(run.account??'')||account&&account!==run.account||run.bucket!==run.id||resolve(run.dir??'')!==resolve('dist/qualification',run.id)||run.token_file!==join(run.dir,'api-token')||!Array.isArray(run.services)||run.services.length!==4||new Set(run.services.map(s=>s.role)).size!==4||!run.services.every(s=>['app','writer','reader','builder'].includes(s.role)&&s.name===run.id+(s.role==='app'?'':'-'+s.role)&&s.config===join(run.dir,s.role+'.json')))throw Error('ObservationRunScopeInvalid');
 if(run.profile==='approved-control'&&run.source?.revision!=='c44896ac8c13898c549b7896ab633b1b8b25c04c')throw Error('ObservationControlChanged');
}
export async function verifyObservationRun(run){
 if(run.kind!=='cold-pilot-not-gate')return verifySealedRun(run);
 try{assertObservationRun(run);for(const service of run.services){const record=run.seal?.[service.role];if(!record)throw Error();const original=await readFile(join(run.dir,'sealed-config-'+service.role+'.json'));if(hash(original)!==record.config)throw Error();const current=JSON.parse(await readFile(service.config));if(JSON.stringify(measuredConfig(current))!==JSON.stringify(measuredConfig(JSON.parse(original))))throw Error();if(run.profile==='approved-control'){const wasm=Object.entries(record.files).filter(([n])=>n.endsWith('.wasm')).map(([,h])=>h);if(wasm.length!==2||!wasm.includes('563936db3958bdab50c404db4a6ccef15e095e798e49198ab2304d87322dac97')||!wasm.includes('789469d743cf69bb9cdcc3e3c59caa4f05663fe4e73958908800d780b1d85f77'))throw Error();}for(const[name,expected]of Object.entries(record.files)){if(name!==name.split('/').at(-1)||name.includes('\\')||hash(await readFile(join(run.dir,'bundle-'+service.role,name)))!==expected)throw Error();}}return[];}catch{return['ObservationArtifactOrRuntimeChanged'];}
}
