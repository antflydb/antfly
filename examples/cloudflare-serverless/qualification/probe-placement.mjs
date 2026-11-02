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

import{resourceFailureCode}from'../performance.mjs';
import{readFile,writeFile}from'node:fs/promises';import{pathToFileURL}from'node:url';
// Separate from a qualification workload: this consumes/warmups actor IDs.
export async function collectPlacement(base,token,{samples=5,tenant,fetcher=fetch,onSample=()=>{}}={}){
 const url=new URL('/_qualification/placement',base);if(url.username||url.password||url.protocol!=='https:'&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname)||!Number.isInteger(samples)||samples<3||samples>8)throw Error('InvalidPlacementProbe');
 const results=[];let identity;
 for(let i=0;i<samples;i++){
  const start=performance.now(),response=await fetcher(url,{method:'POST',headers:{Authorization:'Bearer '+token,...(tenant?{'X-Antfly-Tenant':tenant}:{})},signal:AbortSignal.timeout(30000)});
  if(!response.ok){const failure=await response.json().catch(()=>null),code=resourceFailureCode(failure?.failure_code??'');throw Error('PlacementProbeHttp'+response.status+(code?':'+code:''));}const value=await response.json();
  if(value.qualification!==false||value.consumes_compute_identities!==true||!Array.isArray(value.samples)||value.samples.length!==4||new Set(value.samples.map(s=>s.role)).size!==4||value.samples.some(s=>!['app','writer','reader','builder'].includes(s.role)||!Number.isFinite(s.storage_head_ms)||s.storage_head_ms<0||s.actual_actor_region!==null)||value.actual_bucket_region!==null)throw Error('InvalidPlacementProbeResponse');
  const current=JSON.stringify([value.runtime_generation,value.bucket_location_hint,value.compute_location_hint,value.samples.map(s=>[s.role,s.worker_version])]);if(identity&&identity!==current)throw Error('PlacementProbeVersionChanged');identity=current;
  const placement=response.headers?.get?.("cf-placement");const row={ordinal:i,elapsed_ms:performance.now()-start,...value,placement_header_present:typeof placement==="string"&&placement.length>0,placement_header:typeof placement==="string"&&/^(?:remote|local)-[A-Z]{3}$/.test(placement)?placement:null};results.push(row);await onSample(row);
 }
 const summary={};for(const role of ['app','writer','reader','builder']){const rows=results.map(r=>r.samples.find(s=>s.role===role)),stats=key=>{const v=rows.map(r=>r[key]).filter(Number.isFinite).sort((a,b)=>a-b);return{n:v.length,p50:v[Math.ceil(v.length*.5)-1]??null,p95:v[Math.ceil(v.length*.95)-1]??null};};summary[role]={storage_head_ms:stats('storage_head_ms'),rpc_ms:stats('rpc_ms')};}
 return{qualification:false,kind:'read-only placement latency probe',actual_geography_verified:false,consumes_compute_identities:true,summary,samples:results};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 const[base,tokenPath,out,tenant]=process.argv.slice(2);if(!base||!tokenPath||!out)throw Error('Usage: node qualification/probe-placement.mjs <base-url> <token-file> <report.json> [tenant]');
 const token=(await readFile(tokenPath,'utf8')).trim(),partial=[];
 try{const report=await collectPlacement(base,token,{tenant,onSample:async row=>{partial.push(row);await writeFile(out,JSON.stringify({qualification:false,complete:false,samples:partial},null,2)+'\n');}});await writeFile(out,JSON.stringify({...report,complete:true},null,2)+'\n');console.log('Saved read-only placement latency evidence.');}
 catch(error){await writeFile(out,JSON.stringify({qualification:false,complete:false,error:error.message,samples:partial},null,2)+'\n');throw error;}
}
