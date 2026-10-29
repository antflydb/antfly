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

// Read-only capability inspection. Configuration is never delivery proof.
import{assertGateRun}from'./gate-scope.mjs';
const requiredFields=['Event','EventTimestampMs','EventType','Exceptions','Logs','Outcome','ScriptName','ScriptVersion'];
const fixedCodes=value=>(Array.isArray(value?.errors)?value.errors:[]).slice(0,16).map(e=>e.code).filter(Number.isSafeInteger);
export function telemetryJobTemplate(runs){
 for(const run of runs)assertGateRun(run);
 if(!runs.length||new Set(runs.map(r=>r.account)).size!==1)throw Error('TelemetryScopeMismatch');
 const names=[...new Set(runs.flatMap(r=>r.services.map(s=>s.name)))].sort();
 return{name:'antfly-1278-qualification',dataset:'workers_trace_events',enabled:false,filter:JSON.stringify({where:{key:'ScriptName',operator:'in',value:names}}),output_options:{field_names:[...requiredFields,'CPUTimeMs','WallTimeMs'],sample_rate:1}};
}
export async function inspectTelemetryCapabilities(runs,token,{fetch:request=fetch}={}){
 const proposal=telemetryJobTemplate(runs),account=runs[0].account;
 if(typeof token!=='string'||token.length<20||token.length>4096)throw Error('TelemetryCredentialInvalid');
 const results=[];let fields=[],jobs=[];
 for(const path of ['logpush/datasets/workers_trace_events/fields','logpush/jobs']){
  let response,value;
  try{
   response=await request('https://api.cloudflare.com/client/v4/accounts/'+account+'/'+path,{headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(30000)});
   let bytes=0;const parts=[];for await(const part of response.body){bytes+=part.byteLength;if(bytes>65536)throw Error('TelemetryCapabilityResponseExceeded');parts.push(part);}
   value=JSON.parse(Buffer.concat(parts));
  }catch(error){results.push({path,error:error.message==='TelemetryCapabilityResponseExceeded'?error.message:'TelemetryCapabilityReadFailed'});continue;}
  const success=response.ok&&value.success===true;
  results.push({path,status:response.status,success,error_codes:fixedCodes(value)});
  if(!success)continue;
  if(path.endsWith('/fields'))fields=Object.keys(value.result??{}).filter(k=>requiredFields.includes(k));
  else if(Array.isArray(value.result))for(const job of value.result){
   if(job.dataset!=='workers_trace_events'||!Number.isSafeInteger(job.id))continue;
   let scoped=false;try{const filter=JSON.parse(job.filter);scoped=JSON.stringify(filter)===proposal.filter;}catch{}
   // Never retain destination URLs, credentials, filters or provider text.
   jobs.push({id:job.id,enabled:job.enabled===true,exact_scope:scoped,sample_rate:Number.isFinite(job.output_options?.sample_rate)?job.output_options.sample_rate:null,required_fields:requiredFields.every(k=>job.output_options?.field_names?.includes(k)),destination_type:typeof job.destination_conf==='string'&&job.destination_conf.startsWith('r2://')?'r2':'other'});
  }
 }
 return{schema:1,qualification:false,read_only:true,results,required_fields:requiredFields,available_required_fields:fields,existing_jobs:jobs,configuration_inspectable:results.length===2&&results.every(r=>r.success),delivery_contract_qualified:false,proposal,limitations:['Export permissions and field availability do not prove complete invocation delivery.','Fetch, Durable Object RPC, quiet populations, unique identities, late failures and delivery settlement need real fixtures.','No job, destination, plan, credential or Worker was changed.']};
}


async function main(){
 const{readFile,writeFile,mkdir,open}=await import('node:fs/promises'),{constants}=await import('node:fs'),{resolve}=await import('node:path'),{readCounterToken}=await import('./independent-counters.mjs');
 const gatePath=process.argv[2],tokenPath=process.argv.find(a=>a.startsWith('--token-file='))?.slice(13),out=resolve(process.argv.find(a=>a.startsWith('--out='))?.slice(6)??'dist/qualification/telemetry-capabilities-'+Date.now());
 if(!gatePath||!out.startsWith(resolve('dist/qualification')+'/'))throw Error('UsageTelemetryCapabilitiesGateAndOwnedOutput');
 const gate=JSON.parse(await readFile(gatePath)),runs=await Promise.all(gate.trials.flatMap(t=>[t.baseline,t.candidate]).map(async p=>JSON.parse(await readFile(p))));
 let token;
 if(tokenPath){
  const file=await open(resolve(tokenPath),constants.O_RDONLY|constants.O_NOFOLLOW);
  try{const info=await file.stat();if(!info.isFile()||info.size>4096||(info.mode&0o077)!==0)throw Error('TelemetryTokenMustBePrivateFile');token=(await file.readFile('utf8')).trim();if(!/^[A-Za-z0-9_-]{20,256}$/.test(token))throw Error('TelemetryCredentialInvalid');}finally{await file.close();}
 }else token=await readCounterToken();
 const report=await inspectTelemetryCapabilities(runs,token);await mkdir(out,{mode:0o700});await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2),{mode:0o600,flag:'wx'});
 console.log(JSON.stringify({directory:out,qualification:false,configuration_inspectable:report.configuration_inspectable,delivery_contract_qualified:false,results:report.results}));if(!report.configuration_inspectable)process.exitCode=1;
}
const{fileURLToPath}=await import('node:url'),{resolve:resolveModulePath}=await import('node:path');
if(process.argv[1]&&fileURLToPath(import.meta.url)===resolveModulePath(process.argv[1]))await main();
