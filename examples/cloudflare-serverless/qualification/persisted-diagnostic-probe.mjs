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

// Read-only access probe, not qualification evidence. Never persists log rows,
// account credentials, session URLs or arbitrary provider error text.
import {readFile,writeFile,stat,realpath} from 'node:fs/promises';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {assertGateRun} from './gate-scope.mjs';

export function persistedDiagnosticQuery(run,{from,to}){
 assertGateRun(run);
 if(!Number.isSafeInteger(from)||!Number.isSafeInteger(to)||from<0||from>=to||to-from>86400000)throw Error('InvalidDiagnosticTimeframe');
 return{queryId:crypto.randomUUID(),dry:true,timeframe:{from,to},chartType:'aggregate',limit:1,parameters:{view:'calculations',calculations:[{operator:'count',alias:'events'}],filterCombination:'and',filters:[{key:'$metadata.service',type:'string',operation:'eq',value:run.id}]}};
}
export async function probePersistedDiagnostics(run,token,timeframe,{fetch:request=fetch}={}){
 const query=persistedDiagnosticQuery(run,timeframe);
 if(typeof token!=='string'||! /^[A-Za-z0-9_-]{20,256}$/.test(token))throw Error('InvalidObservabilityToken');
 const response=await request('https://api.cloudflare.com/client/v4/accounts/'+run.account+'/workers/observability/telemetry/query',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify(query),signal:AbortSignal.timeout(30000)});
 let bytes=0,chunks=[];for await(const chunk of response.body){bytes+=chunk.byteLength;if(bytes>4*1024*1024)throw Error('DiagnosticQueryResponseExceeded');chunks.push(chunk);}
 let value;try{value=JSON.parse(Buffer.concat(chunks).toString());}catch{throw Error('DiagnosticQueryResponseMalformed');}
 return{worker:run.id,timeframe,read_only:true,qualification:false,status:response.status,success:response.ok&&value.success===true,error_codes:(value.errors??[]).map(e=>e.code).filter(Number.isSafeInteger),result_fields:Object.keys(value.result??{}).filter(k=>['run','statistics','calculations','events','invocations','traces','agents','distribution'].includes(k))};
}

export async function readObservabilityToken(gate){
 const tokenPath=resolve(gate.dir,'observability-token'),info=await stat(tokenPath);
 if(!info.isFile()||info.size>4096||(info.mode&0o077)!==0||await realpath(tokenPath)!==tokenPath)throw Error('ObservabilityTokenMustBePrivateRegularFile');
 return(await readFile(tokenPath,'utf8')).trim();
}
async function main(){
 const path=process.argv[2];if(!path||process.argv.length!==3)throw Error('usage: persisted-diagnostic-probe GATE_JSON');
 const gate=JSON.parse(await readFile(path)),run=JSON.parse(await readFile(gate.trials[0].baseline));assertGateRun(run,{account:gate.account,dir:gate.dir});
 const token=await readObservabilityToken(gate),to=Date.now(),report=await probePersistedDiagnostics(run,token,{from:to-3600000,to});
 await writeFile(resolve(gate.dir,'persisted-diagnostic-access.json'),JSON.stringify(report,null,2),{mode:0o600});console.log(JSON.stringify(report));if(!report.success)process.exitCode=1;
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))await main();
