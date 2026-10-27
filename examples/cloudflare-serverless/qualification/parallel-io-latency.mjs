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

// Exercise the existing native immutable-upload plan at serial vs the current
// bounded four-way host execution. Same ABI/limits; no production cap increase.
import{readFile,writeFile}from'node:fs/promises';import assert from'node:assert/strict';
import{Bucket}from'../test/object-fixture.mjs';import{instantiateHost}from'../../../zig/lib/objectstore/wasm/asyncify.mjs';import{createR2Transport}from'../../../zig/lib/objectstore/wasm/r2.mjs';
const module=await WebAssembly.compile(await readFile('dist/antfly-serverless.wasm')),delay=20,report={qualification:false,kind:'actual native immutable publication; injected I/O delay',delay_ms:delay,cases:[]};
for(const concurrency of[1,4])for(let repeat=0;repeat<3;repeat++){
 const bucket=new Bucket(),raw=createR2Transport({database:bucket});let enabled=false,active=0,peak=0,operations=0,puts=0;const transport=async(request,body)=>{if(enabled){operations++;puts+=request.op==='put';peak=Math.max(peak,++active);await new Promise(r=>setTimeout(r,delay));}try{return await raw(request,body);}finally{if(enabled)active--;}};
 const host=await instantiateHost(module,transport,{batchConcurrency:concurrency}),req=(op,extra={})=>({op,bucket:'database',prefix:'antfly',namespace:'parallel',paged_text:true,...extra});await host.request(req('mutate',{operation_id:'seed',mutations:Array.from({length:32},(_,i)=>({kind:'upsert',doc_id:'doc/'+i,body:JSON.stringify({text:'document '+i,payload:'x'.repeat(1024)})}))}));enabled=true;const started=performance.now();await host.request(req('publish',{publication_records:32}));const elapsed=performance.now()-started;enabled=false;assert.equal(active,0);assert.ok(peak<=concurrency);assert.equal((await host.request(req('count'))).count,'32');for(const i of[0,15,31])assert.equal(JSON.parse((await host.request(req('lookup',{doc_id:'doc/'+i}))).body).text,'document '+i);report.cases.push({concurrency,repeat,ms:elapsed,operations,puts,peak});
}
await writeFile('dist/qualification/latency-lab/parallel-io.json',JSON.stringify(report,null,2));console.log(JSON.stringify(report));
