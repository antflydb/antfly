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

// Actual WASM with bounded injected storage delay. This is a component
// experiment, not a prediction of Cloudflare latency, billing or placement.
import{readFile,writeFile,mkdir}from'node:fs/promises';
import{resolve}from'node:path';import{createHash}from'node:crypto';import assert from'node:assert/strict';
import{Bucket}from'../test/object-fixture.mjs';
import{instantiateHost}from'../../../zig/lib/objectstore/wasm/asyncify.mjs';
import{createR2Transport}from'../../../zig/lib/objectstore/wasm/r2.mjs';
import{createImmutableReadCache}from'../../../zig/lib/objectstore/wasm/cache.mjs';
const directory=resolve(process.argv.find(a=>a.startsWith('--output-dir='))?.slice(13)??'dist/qualification/latency-lab'),delay=Number(process.argv.find(a=>a.startsWith('--delay='))?.slice(8)??20),size=Number(process.argv.find(a=>a.startsWith('--documents='))?.slice(12)??512);
if(!Number.isInteger(delay)||delay<0||delay>200||!Number.isInteger(size)||size<64||size>4096)throw Error('InvalidLatencyExperiment');
await mkdir(directory,{recursive:true});const hash=b=>createHash('sha256').update(b).digest('hex'),stats=a=>{a=[...a].sort((a,b)=>a-b);return{n:a.length,p50:a[Math.ceil(a.length*.5)-1]??0,p95:a[Math.ceil(a.length*.95)-1]??0,max:a.at(-1)??0};};
const report={qualification:false,kind:'actual native WASM; serial per-object injected delay',delay_ms:delay,documents:size,body_bytes:1024,cases:[]};
for(const variant of[{name:'before',path:resolve(directory,'baseline/antfly-serverless.wasm'),inline:false},{name:'current-no-inline',path:'dist/antfly-serverless.wasm',inline:false},{name:'inline',path:'dist/antfly-serverless.wasm',inline:true}]){
 const bytes=await readFile(variant.path),module=await WebAssembly.compile(bytes),bucket=new Bucket(),raw=createR2Transport({database:bucket});let measuring=false,io,active=0,peak=0;const traces=[];
 const transport=async(req,body)=>{if(measuring){io.operations++;if(req.op==='get')io.gets++;if(req.op==='put')io.puts++;peak=Math.max(peak,++active);if(delay)await new Promise(r=>setTimeout(r,delay));}try{return await raw(req,body);}finally{if(measuring)active--;}};
 const cache=createImmutableReadCache(),host=await instantiateHost(module,cache.wrap(transport,'latency'),{onMetrics:m=>{if(measuring)traces.push({...m,wasm_linear_bytes:host.memoryBytes()});}}),body=id=>JSON.stringify({id,text:'document '+id,payload:'x'.repeat(1024)}),request=(op,extra={})=>({op,bucket:'database',prefix:'antfly',namespace:'latency',wal_inline_tail:true,paged_text:true,pack_small_bodies:true,...(variant.inline?{inline_point_bodies:true}:{}),...extra});
 for(let base=0;base<size;base+=64){await host.request(request('mutate',{operation_id:'seed/'+base,mutations:Array.from({length:Math.min(64,size-base)},(_,j)=>({kind:'upsert',doc_id:'doc/'+(base+j),body:body(base+j)}))}));while((await host.request(request('publish',{publication_records:64}))).has_more){}}
 const uploaded_bytes=[...bucket.objects.values()].reduce((n,v)=>n+v.body.length,0),groups=[];
 for(const cold of[true,false]){cache.clear();const samples=[];for(let i=0;i<32;i++){if(cold)cache.clear();io={operations:0,gets:0,puts:0};active=0;peak=0;measuring=true;const started=performance.now(),id=(i*37)%size;const value=await host.request(request('lookup',{doc_id:'doc/'+id,working_set_bytes:8*1024*1024}));const ms=performance.now()-started;measuring=false;assert.equal(value.body,body(id));assert.equal(active,0);samples.push({ms,...io,peak});}groups.push({cold,latency_ms:stats(samples.map(s=>s.ms)),gets:stats(samples.map(s=>s.gets)),operations:stats(samples.map(s=>s.operations)),peak_concurrency:stats(samples.map(s=>s.peak))});}
 // The inline reader and ordinary readers/collectors share the same durable
 // facts/body identities. Exercise a changed body and a missing document too.
 await host.request(request('mutate',{operation_id:'update',mutations:[{kind:'upsert',doc_id:'doc/0',body:'{"text":"changed"}'},{kind:'delete',doc_id:'doc/1'}]}));while((await host.request(request('publish',{publication_records:64}))).has_more){}cache.clear();assert.equal((await host.request(request('lookup',{doc_id:'doc/0'}))).body,'{"text":"changed"}');assert.equal((await host.request(request('lookup',{doc_id:'doc/1'}))).body,null);assert.equal((await host.request(request('count'))).count,String(size-1));
 report.cases.push({variant:variant.name,wasm_sha256:hash(bytes),inline:variant.inline,retained_payload_bytes:uploaded_bytes,objects:bucket.objects.size,queries:groups,native_peaks:{working:Math.max(...traces.map(t=>t.working_set_peak_bytes)),linear:Math.max(...traces.map(t=>t.wasm_linear_bytes))}});
 await writeFile(resolve(directory,'point-reads-'+size+'.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({variant:variant.name,groups}));
}
