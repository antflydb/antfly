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

// Native WASM metadata recovery model; this does not measure cloud or app SLOs.
import{readFile,writeFile,mkdir}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';import assert from'node:assert/strict';
import{execFileSync}from'node:child_process';
import{Bucket}from'../test/object-fixture.mjs';import{R2CoordinatorStore}from'../r2-coordinator-store.mjs';import{resourceFailureCode}from'../performance.mjs';
const arg=(name,fallback)=>process.argv.find(a=>a.startsWith('--'+name+'='))?.slice(name.length+3)??fallback;
const control=resolve(arg('control','../../.zig-cache/twofold-control/antfly-serverless.wasm')),candidate=resolve(arg('candidate','dist/antfly-serverless.wasm')),out=resolve(arg('out','dist/qualification/metadata-recovery-'+Date.now())),delays=arg('delays','0,50,100,200').split(',').map(Number),lags=arg('lags','1,8,32').split(',').map(Number),samples=Number(arg('samples','3'));
if(delays.some(n=>!Number.isInteger(n)||n<0||n>200)||lags.some(n=>!Number.isInteger(n)||n<0||n>63)||!Number.isInteger(samples)||samples<1||samples>16)throw Error('InvalidMetadataRecoveryExperiment');await mkdir(out,{recursive:true});
const sha=b=>createHash('sha256').update(b).digest('hex'),modules=await Promise.all([control,candidate].map(async path=>{const bytes=await readFile(path);return{path,sha256:sha(bytes),module:await WebAssembly.compile(bytes)};}));
const clone=b=>{const copy=new Bucket();copy.revision=b.revision;copy.objects=new Map([...b.objects].map(([k,v])=>[k,{...v,body:v.body.slice()}]));return copy;};
const fixtureHash=b=>sha(Buffer.from(JSON.stringify([...b.objects].sort(([a],[b])=>a.localeCompare(b)).map(([key,v])=>({key,etag:v.etag,sha256:sha(v.body)})))));
const experiment=await readFile(new URL(import.meta.url));await writeFile(resolve(out,'experiment.mjs'),experiment);
const provenance={revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),dirty:!!execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim(),experiment_sha256:sha(experiment),arguments:process.argv.slice(2)};
const report={schema:1,qualification:false,source:provenance,kind:'Native WASM + isolated R2-binding model; metadata-only dependency-depth experiment',modules:modules.map(({module,...m})=>m),limits:{snapshot_deadline_ms:5000,metadata_operations:254,metadata_transfer_bytes:8*1024*1024,parallel_binding_calls:4},samples_per_case:samples,cases:[]};
const stats=values=>{const v=values.toSorted((a,b)=>a-b);return{n:v.length,p50:v[Math.ceil(v.length*.5)-1]??null,p95:v[Math.ceil(v.length*.95)-1]??null};};
for(const lag of lags){
 let offset=0;const clock=()=>Date.now()+offset,seed=new Bucket(),writer=new R2CoordinatorStore(modules[0].module,seed,'metadata-recovery-uniform',{clock,owner:Array(16).fill(1)});writer.configureAuthority({mode:'r2-bounded'});await writer.transaction(tx=>tx.put('value',0));offset+=1100;await writer.checkpointHint();const before=clone(seed);
 for(let i=1;i<=lag;i++)await writer.transaction(tx=>tx.put('value',i));const tail=clone(seed);offset+=1100;await writer.checkpointHint();const advanced=clone(seed);
 for(const mode of['cold','idle','advanced-floor'])for(const delay of delays){
  const result={lag,mode,delay_ms:delay,fixture_sha256:fixtureHash(mode==='advanced-floor'?advanced:tail),orders:[],variants:[]};const measurements=[[],[]];
  for(let sample=0;sample<samples;sample++){
   const order=sample%2?[1,0]:[0,1];result.orders.push(order);
   for(const which of order){let readerOffset=0,active=0,peak=0,gets=0,misses=0,elapsed=0,snapshot;const b=clone(mode==='cold'?tail:before),original=b.get.bind(b);let injected=0;b.get=async(...args)=>{gets++;peak=Math.max(peak,++active);try{if(injected)await new Promise(ok=>setTimeout(ok,injected));const v=await original(...args);if(!v)misses++;return v;}finally{active--;}};
    const observer=new R2CoordinatorStore(modules[which].module,b,'metadata-recovery-uniform',{clock:()=>Date.now()+readerOffset,onMetrics:s=>{if(s.event==='antfly.coordinator_snapshot')snapshot=s;}});observer.configureAuthority({mode:'r2-bounded'});
    if(mode!=='cold'){assert.equal(await observer.readOnly(tx=>tx.get('value')),0);const full=clone(mode==='advanced-floor'?advanced:tail);b.objects=full.objects;b.revision=full.revision;readerOffset+=31000;}
    if(which&&process.argv.includes('--commit-hints'))observer.offerRootHint(writer.exportRootHint());
    gets=misses=peak=0;injected=delay;const started=performance.now();let failure=null,value;try{value=await observer.readOnly(tx=>tx.get('value'));assert.equal(value,lag);}catch(error){failure=String(error);elapsed=performance.now()-started;await error?.ioSettled;if(resourceFailureCode(error))throw error;}if(!failure)elapsed=performance.now()-started;assert.equal(active,0,'every physical read must settle');
    measurements[which].push({sample,elapsed_ms:elapsed,settlement_ms:Math.max(0,performance.now()-started-elapsed),verified:!failure,failure,gets,misses,peak_parallel_gets:peak,snapshot});
   }
  }
  result.variants=measurements.map((rows,which)=>({name:which?'candidate':'control',module_sha256:modules[which].sha256,elapsed_ms:stats(rows.filter(r=>r.verified).map(r=>r.elapsed_ms)),failures:rows.filter(r=>!r.verified).length,measurements:rows}));report.cases.push(result);await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({lag,mode,delay_ms:delay,variants:result.variants.map(v=>({name:v.name,elapsed_ms:v.elapsed_ms,failures:v.failures,gets:v.measurements.map(m=>m.gets),peak:v.measurements.map(m=>m.peak_parallel_gets)}))}));
 }
}
console.log(JSON.stringify({directory:out,qualification:false,cases:report.cases.length}));
