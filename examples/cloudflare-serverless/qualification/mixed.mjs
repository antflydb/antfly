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

import {readFile,writeFile} from "node:fs/promises";
import assert from "node:assert/strict";
const run=JSON.parse(await readFile(process.argv[2],"utf8")),origin=process.argv[3],token=(await readFile(run.token_file,"utf8")).trim();
if(!run.id.startsWith("antfly-1278-")||!new URL(origin).hostname.startsWith(run.id+".")||!new URL(origin).hostname.endsWith(".workers.dev"))throw new Error("qualification identity mismatch");
const tenants=(process.env.ANTFLY_QUALIFICATION_TENANTS??"a,b,c,d").split(","),documents=Number(process.env.ANTFLY_QUALIFICATION_DOCUMENTS??512),rounds=Number(process.env.ANTFLY_QUALIFICATION_ROUNDS??8);
if(tenants.length>32||!tenants.every(t=>/^[a-z0-9-]+$/.test(t))||!Number.isSafeInteger(documents)||documents<1||documents>100000||!Number.isSafeInteger(rounds)||rounds<1||rounds>10000)throw new Error("invalid workload");
const scenario=process.argv.find(arg=>arg.startsWith("--scenario="))?.slice(11)??"mixed",reuse=process.argv.find(arg=>arg.startsWith("--reuse="))?.slice(8);
if(!/^[a-z0-9-]{1,64}$/.test(scenario)||reuse&&!/^[a-z0-9-]{1,64}$/.test(reuse))throw new Error("invalid scenario");
const expectedVersion=process.argv.find(arg=>arg.startsWith("--expected-version="))?.slice(19);
if(expectedVersion&&!/^[a-f0-9-]{36}$/.test(expectedVersion))throw new Error("invalid expected version");
const verify=process.argv.includes("--verify"),measurements=[],receipts={},seedVisibility={},cold=[];let seedScenario=scenario;
const updateText=tenant=>`durable ${tenant} updated${scenario==="mixed"?"":" "+scenario}`;
const reportPath=run.dir+`/${scenario}-report.json`;
async function call(tenant,input,phase){const started=performance.now();for(let i=0;i<180;i++){
 const requestStarted=performance.now();const response=await fetch(new URL("/api",origin),{method:"POST",headers:{Authorization:`Bearer ${token}`,"Content-Type":"application/json","X-Antfly-Tenant":tenant},body:JSON.stringify(input),signal:AbortSignal.timeout(35000)}),result=await response.json();
 if(response.status===503||response.status===409&&/WorkLeaseLost|WalCommitContended|PreconditionFailed/.test(result.error)){await new Promise(r=>setTimeout(r,250));continue;}
 assert.equal(response.status,200,JSON.stringify(result));if(expectedVersion&&response.headers.get("X-Antfly-Version")!==expectedVersion){await new Promise(r=>setTimeout(r,250));continue;}measurements.push({tenant,phase,op:input.op,ms:performance.now()-started,attempt_ms:performance.now()-requestStarted,attempts:i+1,version:response.headers.get("X-Antfly-Version")});return result;
 }throw new Error("admission retries exhausted");}
async function published(tenant,visibility){for(let i=0;i<600;i++){const result=await call(tenant,{op:"status"},"publication-wait");if(BigInt(result.published_lsn??0)>=BigInt("0x"+visibility.slice(-16)))return;await new Promise(r=>setTimeout(r,500));}throw new Error("publication did not converge");}
// This probe does not instantiate WASM or address any Durable Object.
if(expectedVersion){let ready=false;for(let attempt=0;attempt<120;attempt++){try{const response=await fetch(new URL("/_qualification/version",origin),{method:"POST",headers:{Authorization:`Bearer ${token}`},signal:AbortSignal.timeout(10000)});if(response.ok&&(await response.json()).version===expectedVersion){ready=true;break;}}catch{}await new Promise(r=>setTimeout(r,250));}if(!ready)throw new Error("deployment version did not converge");}
// This FIRST request has no benchmark warmups. Fresh deployment is an external
// precondition: a new HTTP connection alone does not imply a cold isolate.
for(const tenant of tenants){const result=await call(tenant,{op:"count"},"first-request");cold.push({...measurements.at(-1),count:result.count});}
if(!verify){
 if(reuse){const prior=JSON.parse(await readFile(run.dir+`/${reuse}-report.json`,"utf8"));assert.equal(prior.documents_per_tenant,documents);assert.deepEqual(prior.tenants,tenants);Object.assign(receipts,prior.receipts);seedScenario=prior.seed_scenario??reuse;for(const tenant of tenants)seedVisibility[tenant]=prior.seed_visibility?.[tenant]??(receipts[tenant].slice(0,-16)+(BigInt("0x"+receipts[tenant].slice(-16))-BigInt(prior.rounds)).toString(16).padStart(16,"0"));}
 else for(const tenant of tenants){for(let start=0;start<documents;start+=64){const result=await call(tenant,{op:"mutate",operation_id:`${run.id}/${seedScenario}/seed/${start}`,mutations:Array.from({length:Math.min(64,documents-start)},(_,i)=>({kind:"upsert",doc_id:`mixed/${start+i}`,body:JSON.stringify({text:`durable ${tenant} ${start+i}`,tenant,payload:"x".repeat(1024)})}))},"seed");receipts[tenant]=result.visibility;}seedVisibility[tenant]=receipts[tenant];await published(tenant,receipts[tenant]);console.log("seeded",tenant,documents);}
 for(let round=0;round<rounds;round++){
  await Promise.all(tenants.map(async tenant=>{
   const work=[call(tenant,{op:"mutate",operation_id:`${run.id}/${scenario}/round/${round}`,mutations:[{kind:"upsert",doc_id:`mixed/${round%documents}`,body:JSON.stringify({text:updateText(tenant),tenant,payload:"x".repeat(1024)})}]},"mixed-write")];
   for(let i=0;i<3;i++)work.push(call(tenant,{op:"lookup",doc_id:`mixed/${(round*37+i)%documents}`,required_visibility:receipts[tenant]},"mixed-read").then(result=>assert.equal(JSON.parse(result.body).tenant,tenant)));
   // Compact shares the maintenance lane and exercises bounded maintenance
   // while writers and readers remain active. Data GC also runs by cron when
   // AUTO_GC is enabled; metadata GC is qualified separately by run.mjs.
   if(round%4===0)work.push(call(tenant,{op:"compact"},"maintenance"));
   const result=await Promise.all(work);receipts[tenant]=result[0].visibility;
  }));console.log("mixed round",round+1,"of",rounds);
 }
}else {
 const prior=JSON.parse(await readFile(reportPath,"utf8"));assert.equal(prior.documents_per_tenant,documents);assert.equal(prior.rounds,rounds);assert.deepEqual(prior.tenants,tenants);Object.assign(receipts,prior.receipts);seedScenario=prior.seed_scenario??scenario;for(const tenant of tenants)seedVisibility[tenant]=prior.seed_visibility?.[tenant]??(receipts[tenant].slice(0,-16)+(BigInt("0x"+receipts[tenant].slice(-16))-BigInt(rounds)).toString(16).padStart(16,"0"));
 for(const tenant of tenants){
  const latest=await call(tenant,{op:"mutate",operation_id:`${run.id}/${scenario}/round/${rounds-1}`,mutations:[{kind:"upsert",doc_id:`mixed/${(rounds-1)%documents}`,body:JSON.stringify({text:updateText(tenant),tenant,payload:"x".repeat(1024)})}]},"receipt-replay");assert.equal(latest.visibility,receipts[tenant]);
  const start=Math.floor((documents-1)/64)*64,seed=await call(tenant,{op:"mutate",operation_id:`${run.id}/${seedScenario}/seed/${start}`,mutations:Array.from({length:documents-start},(_,i)=>({kind:"upsert",doc_id:`mixed/${start+i}`,body:JSON.stringify({text:`durable ${tenant} ${start+i}`,tenant,payload:"x".repeat(1024)})}))},"receipt-replay");
  assert.equal(seed.visibility,seedVisibility[tenant]);
 }
}
for(const tenant of tenants){await published(tenant,receipts[tenant]);assert.equal((await call(tenant,{op:"count",required_visibility:receipts[tenant]},"verify")).count,String(documents));for(let i=0;i<Math.min(32,documents);i++){const result=await call(tenant,{op:"lookup",doc_id:`mixed/${i}`,required_visibility:receipts[tenant]},"verify");const body=JSON.parse(result.body);assert.equal(body.tenant,tenant);assert.equal(body.payload,"x".repeat(1024));if(i<Math.min(rounds,documents))assert.equal(body.text,updateText(tenant));}}
const summaries=Object.fromEntries([...new Set(measurements.map(m=>m.phase))].map(phase=>{const values=measurements.filter(m=>m.phase===phase),sorted=values.map(m=>m.ms).sort((a,b)=>a-b);return [phase,{samples:values.length,p50:sorted[Math.ceil(sorted.length*.5)-1],p95:sorted[Math.ceil(sorted.length*.95)-1],p99:sorted[Math.ceil(sorted.length*.99)-1],retries:values.reduce((n,m)=>n+m.attempts-1,0)}];}));
const report={profile:"deployed-multi-tenant-mixed",expected_version:expectedVersion,scenario,reuse,seed_scenario:seedScenario,seed_visibility:seedVisibility,verify,documents_per_tenant:documents,tenants,rounds,receipts,cold_first_requests:cold,finished_at:new Date().toISOString(),summaries,measurements,total_isolate_memory:null,billed_r2_cost:null,limitations:["First-request samples require an externally fresh deployment; they do not prove isolate eviction for every tenant.","Physical R2 counts must be joined with complete coordinator and operation logs; they are not an invoice.","WASM memory excludes JS heap and platform allocations; use analytics.mjs separately for sampled whole-isolate memory."]};
await writeFile(verify?run.dir+`/${scenario}-verify-report.json`:reportPath,JSON.stringify(report,null,2));console.log(JSON.stringify({...report,measurements:measurements.length},null,2));
