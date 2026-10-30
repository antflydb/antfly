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

// Identical immutable fixture and fresh kernels, with the previous candidate
// as control. Measures kernel point reads, not app admission or a cloud SLO.
import{readFile,writeFile,mkdir}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import assert from'node:assert/strict';
import{instantiateHost}from'../../../zig/lib/objectstore/wasm/asyncify.mjs';import{createR2Transport}from'../../../zig/lib/objectstore/wasm/r2.mjs';import{Bucket}from'../test/object-fixture.mjs';
const arg=(k,f)=>process.argv.find(a=>a.startsWith('--'+k+'='))?.slice(k.length+3)??f,sha=b=>createHash('sha256').update(b).digest('hex'),out=resolve(arg('out','dist/qualification/overlay-point-'+Date.now())),delay=Number(arg('delay','100')),samples=Number(arg('samples','4'));if(!Number.isInteger(delay)||delay<0||delay>200||!Number.isInteger(samples)||samples<2||samples>16)throw Error('InvalidOverlayExperiment');await mkdir(out,{recursive:true});
const paths=[arg('control','dist/qualification/twofold-serving-1793396451217/modules/candidate/antfly-serverless.wasm'),arg('candidate','dist/antfly-serverless.wasm')],modules=await Promise.all(paths.map(async(path,i)=>{const bytes=await readFile(path);await writeFile(resolve(out,i?'candidate.wasm':'control.wasm'),bytes);return{path,sha256:sha(bytes),module:await WebAssembly.compile(bytes)};})),native=(op,args={})=>({op,bucket:'database',prefix:'scope',namespace:'overlay-latency-uniform',...args}),body=text=>JSON.stringify({text,payload:'x'.repeat(1024)}),fixture=new Bucket(),writer=await instantiateHost(modules[0].module,createR2Transport({database:fixture},{maxPutBytes:4*1024*1024}));
await writer.request(native('mutate',{operation_id:'seed',mutations:['updated','deleted','stable'].map(doc_id=>({kind:'upsert',doc_id,body:body(doc_id)}))}));await writer.request(native('publish'));const changed=await writer.request(native('mutate',{operation_id:'change',mutations:[{kind:'upsert',doc_id:'updated',body:body('fresh')},{kind:'delete',doc_id:'deleted'}]}));
const script=await readFile(new URL(import.meta.url)),report={qualification:false,kind:'Native point kernel only; frozen identical fixture, alternating order, no immutable byte cache',source_revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),dirty:!!execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim(),arguments:process.argv.slice(2),experiment_sha256:sha(script),fixture_sha256:sha(JSON.stringify([...fixture.objects].sort(([a],[b])=>a.localeCompare(b)).map(([key,value])=>({key,etag:value.etag,sha256:sha(value.body)})))),modules:modules.map(({module,...m})=>m),delay_ms:delay,samples,limits:{deadline_ms:5000,operations:128,transfer_bytes:8*1024*1024},cases:[]};await writeFile(resolve(out,'experiment.mjs'),script);
for(const id of['updated','deleted','stable','absent'])for(let sample=0;sample<samples;sample++)for(const which of(sample%2?[1,0]:[0,1])){
 const bucket=new Bucket();bucket.objects=new Map([...fixture.objects].map(([k,v])=>[k,{...v,body:v.body.slice()}]));bucket.revision=fixture.revision;let gets=0,baseRoots=0,active=0;const get=bucket.get.bind(bucket);bucket.get=async(key,...args)=>{gets++;active++;try{if(new TextDecoder().decode(bucket.objects.get(key)?.body.subarray(0,7))==='AFDFACT')baseRoots++;if(delay)await new Promise(ok=>setTimeout(ok,delay));return await get(key,...args);}finally{active--;}};
 let metrics;const host=await instantiateHost(modules[which].module,createR2Transport({database:bucket}),{onMetrics:m=>metrics=m}),start=performance.now(),response=await host.request(native('lookup',{doc_id:id,required_visibility:changed.visibility}),{deadlineMs:Date.now()+5000,maxOperations:128,maxTransferBytes:8*1024*1024}),elapsed=performance.now()-start;assert.equal(active,0);assert.equal(response.body,id==='updated'?body('fresh'):id==='stable'?body('stable'):null);assert.equal(response.visibility,changed.visibility);
 const row={id,sample,variant:which?'candidate':'control',elapsed_ms:elapsed,verified:true,gets,base_facts_root_gets:baseRoots,metrics};report.cases.push(row);await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({id,sample,variant:row.variant,elapsed_ms:elapsed,gets,base_facts_root_gets:baseRoots}));
}
console.log(JSON.stringify({directory:out,qualification:false}));
