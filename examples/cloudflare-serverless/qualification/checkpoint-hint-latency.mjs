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

// Same immutable namespace/corpus, independent physical stores and alternating
// order. This isolates checkpoint repair; it is not an app/provider SLO.
import{readFile,writeFile,mkdir}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import assert from'node:assert/strict';
import{R2CoordinatorStore}from'../r2-coordinator-store.mjs';import{Bucket}from'../test/object-fixture.mjs';
const arg=(k,f)=>process.argv.find(a=>a.startsWith('--'+k+'='))?.slice(k.length+3)??f,sha=b=>createHash('sha256').update(b).digest('hex'),out=resolve(arg('out','dist/qualification/checkpoint-hint-'+Date.now())),delay=Number(arg('delay','100')),samples=Number(arg('samples','4'));if(!Number.isInteger(delay)||delay<0||delay>200||!Number.isInteger(samples)||samples<2||samples>16)throw Error('InvalidCheckpointExperiment');await mkdir(out,{recursive:true});
const paths=[arg('control','dist/qualification/twofold-serving-1793396451217/modules/candidate/antfly-serverless.wasm'),arg('candidate','dist/antfly-serverless.wasm')],modules=await Promise.all(paths.map(async path=>{const bytes=await readFile(path);return{path,sha256:sha(bytes),module:await WebAssembly.compile(bytes)};})),script=await readFile(new URL(import.meta.url)),report={qualification:false,kind:'Native checkpoint hint only; injected binding delay; no app/hosted gate claim',source_revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),dirty:!!execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim(),arguments:process.argv.slice(2),experiment_sha256:sha(script),modules:modules.map(({module,...m})=>m),delay_ms:delay,limits:{operations:32,transfer_bytes:1024*1024,deadline_ms:10000},cases:[]};await writeFile(resolve(out,'experiment.mjs'),script);
for(let sample=0;sample<samples;sample++)for(const which of(sample%2?[1,0]:[0,1])){
 let offset=0,injected=0,active=0,peak=0,metrics;const bucket=new Bucket(),counts={get:0,put:0},clock=()=>Date.now()+offset;for(const op of['get','put']){const original=bucket[op].bind(bucket);bucket[op]=async(...args)=>{if(injected){counts[op]++;peak=Math.max(peak,++active);}try{if(injected)await new Promise(ok=>setTimeout(ok,injected));return await original(...args);}finally{if(injected)active--;}};}
 const writer=new R2CoordinatorStore(modules[which].module,bucket,'checkpoint-latency-uniform',{clock,owner:Array(16).fill(1),onMetrics:m=>{if(m.event==='antfly.checkpoint_hint')metrics=m;}});writer.configureAuthority({mode:'r2-bounded'});await writer.transaction(tx=>tx.put('value',1));offset+=1100;await writer.checkpointHint();await writer.transaction(tx=>tx.put('value',2));offset+=1100;
 injected=delay;const start=performance.now(),result=await writer.checkpointHint(),elapsed=performance.now()-start;injected=0;assert.equal(active,0);assert.equal(result.done,true);assert.equal(result.revision,writer.state.revision);assert.equal(await writer.readOnly(tx=>tx.get('value')),2);const row={sample,variant:which?'candidate':'control',elapsed_ms:elapsed,verified:true,certified:writer.checkpointHintSupported===true,counts,peak_parallel_io:peak,metrics};report.cases.push(row);await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({sample,variant:row.variant,elapsed_ms:elapsed,certified:row.certified,counts,peak}));
}
console.log(JSON.stringify({directory:out,qualification:false}));
