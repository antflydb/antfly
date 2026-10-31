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
function legacyPlacement(config){return config.vars?.RUNTIME_GENERATION!=null||config.vars?.R2_LOCATION_HINT!=null||config.vars?.DO_LOCATION_HINT!=null||config.vars?.EDGE_IMMUTABLE_CACHE!=null||config.vars?.DIRECT_READS!=null||config.vars?.READ_VIEW_BATCH_MS!=null||config.vars?.LOCAL_METADATA_READS!=null||config.vars?.INLINE_METADATA_ROOTS!=null?{generation:config.vars?.RUNTIME_GENERATION??null,location_hint:config.vars?.R2_LOCATION_HINT??null,...(config.vars?.DO_LOCATION_HINT!=null?{compute_location_hint:config.vars.DO_LOCATION_HINT}:{}),...(config.vars?.EDGE_IMMUTABLE_CACHE!=null?{immutable_cache:config.vars.EDGE_IMMUTABLE_CACHE==="true",immutable_cache_scope:config.vars.EDGE_IMMUTABLE_CACHE_SCOPE??null,local_metadata_reads:config.vars.LOCAL_METADATA_READS==="true"}:{}),...(config.vars?.LOCAL_METADATA_READS!=null?{local_metadata_reads:config.vars.LOCAL_METADATA_READS==="true"}:{}),...(config.vars?.DIRECT_READS!=null?{direct_reads:config.vars.DIRECT_READS==="true"}:{}),...(config.vars?.READ_VIEW_BATCH_MS!=null?{read_view_batch_ms:Number(config.vars.READ_VIEW_BATCH_MS)}:{}),...(config.vars?.INLINE_METADATA_ROOTS!=null?{inline_metadata_roots:config.vars.INLINE_METADATA_ROOTS==="true"}:{})}:null;}
const optionalOptimizationKeys=['CANONICAL_POINT_BODIES','POINT_READ_BLOCKS','WAL_PACKED_PAGES','WAL_DEFERRED_INDEXES','QUALIFICATION_PLACEMENT_PROBE','NATIVE_READ_PLAN','NATIVE_READ_DETACHED','FUSED_READ_ADMISSION','FENCED_READ_LEASES','DETACHED_METADATA_PROOFS','NATIVE_READ_STATE','NATIVE_READ_AFFINITY','WAL_DOCUMENT_INDEX','FOREGROUND_CACHE_POLICY','OPPORTUNISTIC_READ_PREPARE','NATIVE_WRITE_PLAN','NATIVE_WRITE_READ_AHEAD','NATIVE_METADATA_READ_BATCH','METADATA_COMMIT_HINTS','NATIVE_METADATA_CONTINUATIONS','READ_VIEW_CACHE_SEEDING','NATIVE_POINT_CONTINUATIONS','IMMUTABLE_PREPARED_REUSE','REPLAY_COST_CHECKPOINTS','INLINE_FACTS_LEAVES','NATIVE_WRITE_OVERLAP','PUBLICATION_CONTINUATIONS','PUBLICATION_CONTINUATION_FORCE','PUBLICATION_ADAPTIVE_SLICES','PUBLICATION_PREPARED_SLICES'];
const optimizationKeys=['FUSED_READS','OPTIMISTIC_WAL','INLINE_POINT_BODIES','NATIVE_COMPACTION_PLAN'];
export function runtimePlacement(config){
 const base=legacyPlacement(config),configured=config.vars?.METADATA_ROOT_BACKEND!=null||[...optimizationKeys,...optionalOptimizationKeys].some(key=>config.vars?.[key]!=null);
 if(!configured&&!config.placement&&config.vars?.PUBLICATION_UNITS==null)return base;
 return{...(base??{generation:null,location_hint:null}),...(config.vars?.METADATA_ROOT_BACKEND!=null?{metadata_root_backend:config.vars.METADATA_ROOT_BACKEND}:{}),...(config.vars?.PUBLICATION_UNITS!=null?{publication_units:Number(config.vars.PUBLICATION_UNITS)}:{}),...(configured?{native_optimizations:Object.fromEntries([...optimizationKeys,...optionalOptimizationKeys.filter(key=>config.vars?.[key]!=null)].map(key=>[key,config.vars?.[key]==="true"]))}:{}),...(config.placement?{worker_placement:structuredClone(config.placement)}:{})};
}
export async function verifySealedRun(run){
 const errors=[];
 for(const service of run.services??[]){
  const hashes=run.bundle_hashes?.[service.role],dir=resolve(run.dir,'bundle-'+service.role);
  if(!hashes||!Object.keys(hashes).some(name=>name.endsWith('.js'))){errors.push(service.role+': sealed bundle missing');continue;}
  try{
   const config=JSON.parse(await readFile(service.config));
   if(run.corpus&&(!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(run.corpus.id??'')||!isDeepStrictEqual(JSON.parse(config.vars?.ANTFLY_TENANTS??'{}'),run.corpus.tenants)))errors.push(service.role+': corpus identity changed');
   if(run.corpus?.root&&!isDeepStrictEqual({incarnation:config.vars?.ANTFLY_INCARNATION,storage_id:config.vars?.COORDINATOR_STORAGE_ID??config.vars?.ANTFLY_INCARNATION},run.corpus.root))errors.push(service.role+': corpus root identity changed');
   if(!isDeepStrictEqual(runtimePlacement(config),run.runtime_placement??null))errors.push(service.role+': runtime placement policy changed');
   if(dirname(resolve(config.main))!==dir||!hashes[basename(config.main)])errors.push(service.role+': entry does not select sealed bundle');
   for(const[name,expected]of Object.entries(hashes)){
    if(basename(name)!==name||!/^([a-f0-9]{64})$/.test(expected)||!/[.](js|wasm)$/.test(name)){errors.push(service.role+': invalid artifact descriptor');continue;}
    const actual=createHash('sha256').update(await readFile(resolve(dir,name))).digest('hex');
    if(actual!==expected)errors.push(service.role+': artifact hash changed: '+name);
   }
   for(const[suffix,expected]of[['-antfly-serverless.wasm',run.wasm_sha256],['-antfly-serverless-compact.wasm',run.reader_wasm_sha256],...(run.observation_wasm_sha256?[['-antfly-serverless-observe.wasm',run.observation_wasm_sha256]]:[])])if(!Object.entries(hashes).some(([name,hash])=>name.endsWith(suffix)&&hash===expected))errors.push(service.role+': declared native module missing: '+suffix);
  }catch{errors.push(service.role+': sealed artifact unavailable');}
 }
 if(run.services?.length!==4)errors.push('four sealed services required');
 return errors;
}
export function verifyReportedRun(side,run){
 const errors=[];
 if(run.revision!=null&&side?.source_revision!==run.revision)errors.push("source revision changed");
 if(!isDeepStrictEqual(side?.corpus??null,run.corpus??null))errors.push('corpus identity changed');
 if(!isDeepStrictEqual(side?.runtime_placement??null,run.runtime_placement??null))errors.push('runtime placement profile changed');
 if(side?.run!==run.id||side.version!==run.version)errors.push('deployment identity/version changed');
 if(side?.wasm_sha256!==run.wasm_sha256||side.reader_wasm_sha256!==run.reader_wasm_sha256)errors.push('native module identity changed');
 if(!run.bundle_hashes||!isDeepStrictEqual(side?.bundle_hashes,run.bundle_hashes))errors.push('sealed service hashes changed');
 return errors;
}

export function assertUnusedRecovery(run){
 if(run.cold_execution_id)throw Error('physical recovery actors already consumed; use a fresh deployment pair');
}
