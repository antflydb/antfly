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
const manifest=process.argv[2],origin=process.argv[3],run=JSON.parse(await readFile(manifest,"utf8")),token=(await readFile(run.token_file,"utf8")).trim();
if(!run.id.startsWith("antfly-1278-")||!new URL(origin).hostname.startsWith(run.id+".")||!new URL(origin).hostname.endsWith(".workers.dev"))throw new Error("qualification identity mismatch");
const measurements=[];
async function call(input){const totalStarted=performance.now();for(let i=0;i<60;i++){const started=performance.now(),response=await fetch(new URL("/api",origin),{method:"POST",headers:{Authorization:`Bearer ${token}`,"Content-Type":"application/json"},body:JSON.stringify(input),signal:AbortSignal.timeout(35000)});const result=await response.json();if(response.status===503||response.status===409&&/WorkLeaseLost|WalCommitContended/.test(result.error)){await new Promise(r=>setTimeout(r,1000));continue;}assert.equal(response.status,200,JSON.stringify(result));measurements.push({op:input.op,ms:performance.now()-totalStarted,attempt_ms:performance.now()-started,attempts:i+1,version:response.headers.get("X-Antfly-Version")});return result;}throw new Error("admission retries exhausted");}
const mutation={op:"mutate",operation_id:run.id+"/performance/initial",mutations:Array.from({length:256},(_,i)=>({kind:"upsert",doc_id:`perf/${String(i).padStart(4,"0")}`,body:JSON.stringify({text:`performance durable document ${i}`,payload:"x".repeat(256)})}))};
const receipt=await call(mutation);assert.equal(receipt.committed,true);
const reopen=process.argv.includes("--reopen");
if(reopen){const prior=JSON.parse(await readFile(run.dir+"/performance-report.json","utf8"));assert.equal(receipt.visibility,prior.receipt.visibility);}
for(let i=0;i<180;i++){const status=await call({op:"status"});if(status.published&&BigInt(status.published_lsn)>=BigInt("0x"+receipt.visibility.slice(-16)))break;if(i===179)throw new Error("publication did not converge");await new Promise(r=>setTimeout(r,1000));}
for(let i=0;i<32;i++)await call({op:"lookup",doc_id:`perf/${String(i*37%256).padStart(4,"0")}`,required_visibility:receipt.visibility});
const samples=[];
for(let i=0;i<100;i++){const result=await call({op:"lookup",doc_id:`perf/${String(i*37%256).padStart(4,"0")}`,required_visibility:receipt.visibility});assert.equal(JSON.parse(result.body).payload.length,256);samples.push(measurements.at(-1).ms);}
let writes=[],burstWallMs=null;
if(!reopen){const started=performance.now();writes=await Promise.all(Array.from({length:8},(_,i)=>call({op:"mutate",operation_id:run.id+`/performance/writer/${i}`,mutations:[{kind:"upsert",doc_id:`writer/${i}`,body:JSON.stringify({text:`concurrent writer ${i}`})}]})));burstWallMs=performance.now()-started;console.log("eight-writer wall ms",Math.round(burstWallMs));for(let i=0;i<8;i++)assert.ok((await call({op:"lookup",doc_id:`writer/${i}`,required_visibility:writes[i].visibility})).body);}
assert.equal((await call({op:"count"})).count,"264");
const sorted=samples.toSorted((a,b)=>a-b),percentile=p=>sorted[Math.ceil(p*sorted.length)-1];
const report={profile:"r2-native-performance",reopen,finished_at:new Date().toISOString(),receipt,writes,burst_wall_ms:burstWallMs,lookup_ms:{samples:samples.length,p50:percentile(.5),p95:percentile(.95),p99:percentile(.99)},checks:{fenced_reads:true,exact_bodies:true,count_264:true,original_receipt:true},measurements};
await writeFile(run.dir+(reopen?"/reopen-performance-report.json":"/performance-report.json"),JSON.stringify(report,null,2));console.log(JSON.stringify({...report,measurements:measurements.length},null,2));
