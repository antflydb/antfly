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

// Controlled storage-component comparison. Injected delay is not live R2.
import{readFile,writeFile,mkdir}from"node:fs/promises";
import{Bucket}from"../test/object-fixture.mjs";
import{R2CoordinatorStore,encode}from"../r2-coordinator-store.mjs";
import{PublicationScheduler}from"../publication-scheduler.mjs";
import{createR2Transport}from"../../../zig/lib/objectstore/wasm/r2.mjs";
import{instrumentTransport}from"../performance.mjs";
import assert from"node:assert/strict";
const module=await WebAssembly.compile(await readFile(new URL("../dist/antfly-serverless.wasm",import.meta.url)));
const delay=Number(process.env.ANTFLY_TEST_R2_DELAY_MS??5),sleep=()=>new Promise(resolve=>setTimeout(resolve,delay)),epoch=1000000;
if(!Number.isInteger(delay)||delay<0||delay>50)throw Error("invalid delay");
function measured(bucket){const raw=createR2Transport({database:bucket}),counters={};return{counters,transport:instrumentTransport(async(request,body)=>{await sleep();return raw(request,body);},counters)};}
const report={profile:"controlled native storage components",injected_io_delay_ms:delay,recovery:[],metadata_commits:[],limitations:["Injected storage delay and local execution do not establish deployed latency or billing.","Metadata component excludes immutable document/WAL data uploads and HTTP routing."]};
for(const tail of[0,7,31,63])for(const window of[0,8]){
 const bucket=new Bucket(),source=new R2CoordinatorStore(module,bucket,"recovery",{clock:()=>epoch});await source.transaction(tx=>tx.get("value"));for(let i=0;i<tail;i++)await source.transaction(tx=>tx.put("value",i));
 const io=measured(bucket),reopened=new R2CoordinatorStore(module,bucket,"recovery",{...io,clock:()=>epoch,recoveryWindow:window});const start=performance.now();let value;
 for(let i=0;i<8;i++){try{value=await reopened.transaction(tx=>tx.get("value"));break;}catch(error){if(!String(error).includes("CoordinatorRecovering"))throw error;}}
 assert.equal(value,tail?tail-1:undefined);report.recovery.push({tail_commits:tail,window,ms:performance.now()-start,...io.counters});
}
const record=(i)=>({etag:"etag-"+i,body:new TextEncoder().encode(JSON.stringify({latest_lsn:i,padding:"x".repeat(512)})),contentType:null});
for(const native of[false,true]){
 const bucket=new Bucket(),io=measured(bucket),store=new R2CoordinatorStore(module,bucket,"commit",{...io,clock:()=>epoch}),scheduler=new PublicationScheduler(store,"commit",()=>epoch);
 await store.transaction(tx=>{for(let i=0;i<32;i++)tx.put("meta/"+i,"x".repeat(64));});
 if(native)await store.commitWal({wal_key:"wal",value:encode(record(0)),publication_key:"__antfly_maintenance/publication",watermark:"1",now_ms:epoch,id:"job"});
 else await store.transaction(async tx=>{tx.put("wal",record(0));await scheduler.reserve(tx,"1");});
 for(const key of Object.keys(io.counters))delete io.counters[key];const start=performance.now();
 for(let i=1;i<=20;i++){
  if(native)assert.equal((await store.commitWal({wal_key:"wal",expected_etag:"etag-"+(i-1),value:encode(record(i)),publication_key:"__antfly_maintenance/publication",watermark:String(i+1),now_ms:epoch,id:"job"})).applied,true);
  else await store.transaction(async tx=>{assert.equal((await tx.get("wal")).etag,"etag-"+(i-1));tx.put("wal",record(i));await scheduler.reserve(tx,String(i+1));});
 }
 const ms=performance.now()-start,observed=structuredClone(io.counters);assert.equal((await store.transaction(tx=>tx.get("wal"))).etag,"etag-20");report.metadata_commits.push({native,commits:20,ms,...observed});
}
await mkdir("dist/qualification",{recursive:true});await writeFile("dist/qualification/critical-path.json",JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
