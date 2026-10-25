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

import{readFile}from'node:fs/promises';
import{resolve,basename,dirname}from'node:path';
import{createHash}from'node:crypto';
import{isDeepStrictEqual}from'node:util';
export async function verifySealedRun(run){
 const errors=[];
 for(const service of run.services??[]){
  const hashes=run.bundle_hashes?.[service.role],dir=resolve(run.dir,'bundle-'+service.role);
  if(!hashes||!Object.keys(hashes).some(name=>name.endsWith('.js'))){errors.push(service.role+': sealed bundle missing');continue;}
  try{
   const config=JSON.parse(await readFile(service.config));
   if(dirname(resolve(config.main))!==dir||!hashes[basename(config.main)])errors.push(service.role+': entry does not select sealed bundle');
   for(const[name,expected]of Object.entries(hashes)){
    if(basename(name)!==name||!/^([a-f0-9]{64})$/.test(expected)||!/[.](js|wasm)$/.test(name)){errors.push(service.role+': invalid artifact descriptor');continue;}
    const actual=createHash('sha256').update(await readFile(resolve(dir,name))).digest('hex');
    if(actual!==expected)errors.push(service.role+': artifact hash changed: '+name);
   }
  }catch{errors.push(service.role+': sealed artifact unavailable');}
 }
 if(run.services?.length!==4)errors.push('four sealed services required');
 return errors;
}
export function verifyReportedRun(side,run){
 const errors=[];
 if(side?.run!==run.id||side.version!==run.version)errors.push('deployment identity/version changed');
 if(side?.wasm_sha256!==run.wasm_sha256||side.reader_wasm_sha256!==run.reader_wasm_sha256)errors.push('native module identity changed');
 if(!run.bundle_hashes||!isDeepStrictEqual(side?.bundle_hashes,run.bundle_hashes))errors.push('sealed service hashes changed');
 return errors;
}

export function assertUnusedRecovery(run){
 if(run.cold_execution_id)throw Error('physical recovery actors already consumed; use a fresh deployment pair');
}
