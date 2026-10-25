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

// Opt-in native metadata qualification: node qualification/r2-scale.mjs [rows].
// Add --compact --idle-checkpoint for the production pooling/checkpoint policy.
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {RequestPool} from "../request-pool.mjs";
import {instantiateHost} from "../../../zig/lib/objectstore/wasm/asyncify.mjs";
import {R2CoordinatorStore} from "../r2-coordinator-store.mjs";
import {collectCoordinator} from "../r2-coordinator-gc.mjs";
import {Bucket} from "../test/object-fixture.mjs";
const rows=Number(process.argv[2]??16384);
const checkpointCommits=Number(process.argv.find(arg=>arg.startsWith("--inline-checkpoint-commits="))?.slice(28)??64);
assert.ok([8,16,32,64].includes(checkpointCommits));
assert.ok(Number.isSafeInteger(rows)&&rows>=1024&&rows<=1000000);
const module=await WebAssembly.compile(await readFile(new URL(process.argv.includes("--compact")?"../dist/antfly-serverless-compact.wasm":"../dist/antfly-serverless.wasm",import.meta.url)));
const bucket=new Bucket(),peaks={operations:0,transferred_bytes:0,working_set_peak_bytes:0,wasm_linear_bytes:0};let now=Date.now(),violations=0;
const backgroundTasks=new Set(),idle=process.argv.includes("--idle-checkpoint");
const background=idle?promise=>{backgroundTasks.add(promise);promise.finally(()=>backgroundTasks.delete(promise));}:undefined;
const settleBackground=()=>Promise.all([...backgroundTasks]);
const kernel=process.argv.includes("--compact")?new RequestPool(()=>instantiateHost(module,null,{clock:()=>now}),1,{clock:()=>now}):undefined;
const reopen=()=>new R2CoordinatorStore(module,bucket,"r2-scale",{kernel,checkpointCommits,background,clock:()=>now,onMetrics:sample=>{for(const key of Object.keys(peaks))peaks[key]=Math.max(peaks[key],sample[key]??0);if(sample.operations>1024||sample.transferred_bytes>32*1024*1024)violations++;}});
let store=reopen();const started=performance.now(),payload="x".repeat(768),key=i=>`record/${String(i).padStart(8,"0")}`;
async function run(callback){for(let i=0;i<100;i++){try{return await store.transaction(callback);}catch(error){if(!/CoordinatorRecovering/.test(String(error)))throw error;}}throw new Error("recovery failed");}
for(let base=0;base<rows;base+=128)await run(tx=>{for(let i=base;i<Math.min(rows,base+128);i++)tx.put(key(i),{i,payload});});
const writeMs=performance.now()-started;
await settleBackground();store=reopen();assert.equal((await run(tx=>tx.get(key(rows-1)))).i,rows-1);
const root=JSON.parse(store.state.root),branches=[root];let maxChildren=0;
while(branches.length){const page=branches.shift(),result=await run(tx=>tx.trace(page));maxChildren=Math.max(maxChildren,result.children.length);if(maxChildren>128)break;branches.push(...result.children.filter(child=>child.height>0));}
if(rows>=16384)assert.ok(maxChildren>128,"qualification must exercise a branch requiring bounded child continuation");
await run(tx=>{for(let i=0;i<128;i++)tx.delete(key(i));});
const bytes=()=>[...bucket.objects.values()].reduce((total,object)=>total+object.body.length,0),beforeBytes=bytes();
let steps=0,done=false;const cycles=[];
for(let cycle=0;cycle<3;cycle++){
 done=false;let cycleSteps=0;
for(;cycleSteps<10000;cycleSteps++,steps++){
 const result=await collectCoordinator(store,bucket,()=>now);
 if(result.delay_seconds)now+=result.delay_seconds*1000+1;
 if(result.done){done=true;break;}
 if(cycleSteps===4||cycleSteps===16){await settleBackground();store=reopen();}
}
assert.ok(done,"metadata collection did not finish");
cycles.push({steps:cycleSteps+1,bytes:bytes(),objects:bucket.objects.size});
}
assert.ok(cycles[2].bytes<cycles[1].bytes*1.2,"repeated maintenance must stabilize retained storage");
await settleBackground();store=reopen();let after="",count=0;
for(;;){const page=await run(tx=>tx.list({prefix:"record/",startAfter:after}));if(!page.size)break;for(const [k,value]of page){assert.equal(k,key(value.i));assert.equal(value.payload,payload);assert.ok(value.i>=128);count++;after=k;}}
assert.equal(count,rows-128);
assert.equal(violations,0);
await settleBackground();
console.log(JSON.stringify({rows,compact:process.argv.includes("--compact"),idle_checkpoint:idle,inline_checkpoint_commits:checkpointCommits,remaining:count,write_ms:Math.round(writeMs),total_ms:Math.round(performance.now()-started),collection_steps:steps+3,cycles,max_branch_children:maxChildren,objects:bucket.objects.size,before_bytes:beforeBytes,after_bytes:bytes(),peak_operations:peaks.operations,peak_transfer_bytes:peaks.transferred_bytes,peak_working_bytes:peaks.working_set_peak_bytes,peak_linear_bytes:peaks.wasm_linear_bytes},null,2));
