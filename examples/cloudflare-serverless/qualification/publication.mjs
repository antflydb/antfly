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
const measurements=[];
async function call(input){const start=performance.now();for(let attempt=0;attempt<120;attempt++){
 const response=await fetch(new URL("/api",origin),{method:"POST",headers:{Authorization:`Bearer ${token}`,"Content-Type":"application/json"},body:JSON.stringify(input),signal:AbortSignal.timeout(35000)}),result=await response.json();
 if(response.status===503||response.status===409&&/WorkLeaseLost|WalCommitContended|PreconditionFailed/.test(result.error)){await new Promise(r=>setTimeout(r,250));continue;}
 assert.equal(response.status,200,JSON.stringify(result));measurements.push({op:input.op,ms:performance.now()-start,attempts:attempt+1,version:response.headers.get("X-Antfly-Version")});return result;
}throw new Error("admission retries exhausted");}
assert.equal((await call({op:"count"})).count,"0","publication profile requires a fresh default namespace");
const mutation={op:"mutate",operation_id:`${run.id}/lease-publication`,mutations:Array.from({length:64},(_,i)=>({kind:"upsert",doc_id:`publication/${i}`,body:JSON.stringify({text:`publication durable ${i}`,payload:"x".repeat(1024)})}))};
const receipt=await call(mutation);assert.equal(receipt.committed,true);
const started=performance.now();let steps=0,result;
do{result=await call({op:"publish"});if(++steps>64)throw new Error("publication did not converge");}while(result.has_more);
const wall=performance.now()-started;assert.equal(result.wal_end_lsn,"64");assert.equal((await call({op:"count",required_visibility:receipt.visibility})).count,"64");
for(let i=0;i<64;i++){const body=JSON.parse((await call({op:"lookup",doc_id:`publication/${i}`,required_visibility:receipt.visibility})).body);assert.deepEqual(body,{text:`publication durable ${i}`,payload:"x".repeat(1024)});}
assert.equal((await call(mutation)).visibility,receipt.visibility);
const report={profile:"controlled-native-publication",documents:64,body_payload_bytes:1024,publication_steps:steps,publication_wall_ms:wall,receipt,checks:{exact_bodies:true,exact_count:true,receipt_replay:true},queue_delivery:"externally paused",finished_at:new Date().toISOString(),measurements};
await writeFile(run.dir+"/publication-report.json",JSON.stringify(report,null,2));console.log(JSON.stringify({...report,measurements:measurements.length},null,2));
