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

import{test}from'node:test';import assert from'node:assert/strict';import{build}from'esbuild';import{Miniflare,convertV4MiniflareOptions}from'miniflare';import{resolve}from'node:path';import{mkdtemp,rm}from'node:fs/promises';
test('workerd Logpush collector ingests gzip and exercises a private Worker with real DO RPC',{timeout:30000},async()=>{
 const source='antfly-1278-telemetry-local-source',token='local-only-disposable-fixture',capture=crypto.randomUUID(),workers=[];
 for(const [name,file]of [['collector','logpush-probe-collector.mjs'],['counter','tail-counter-worker.mjs'],[source,'logpush-probe-source.mjs']]){const result=await build({entryPoints:['qualification/'+file],bundle:true,write:false,format:'esm',platform:'neutral',external:['cloudflare:workers']});workers.push({name,modules:[{type:'ESModule',path:resolve(name+'.mjs'),contents:result.outputFiles[0].text}],compatibilityDate:'2026-10-23',...(name==='counter'?{bindings:{CAPTURE_ID:capture,SOURCE_WORKER:source},r2Buckets:{LOGS:'telemetry'},serviceBindings:{PRODUCER:source}}:name==='collector'?{bindings:{API_TOKEN:token,CAPTURE_ID:capture,SOURCE_WORKER:source},r2Buckets:{LOGS:'telemetry'},serviceBindings:{FIXTURE:source,COUNTER:'counter'}}:{bindings:{API_TOKEN:token},tailConsumers:[{worker:'counter'}],durableObjects:{PROBES:{className:'Probe',useSQLite:true}}})});}
 const persist=await mkdtemp(resolve('dist','logpush-workerd-')),options=convertV4MiniflareOptions({workers,resourcePersistencePath:persist});options.workers.find(w=>w.config.name===source).config.tailConsumers=[{worker:'counter'}];const mf=new Miniflare(options);
 const call=(path,body,headers={})=>mf.dispatchFetch('https://local.invalid'+path,{method:'POST',headers:{Authorization:'Bearer '+token,...headers},body});
 try{
  const privateSource=await mf.getWorker(source);assert.equal((await privateSource.fetch('https://probe.invalid/?nonce='+crypto.randomUUID(),{method:'POST'})).status,401);
  const compressed=await new Response(new Blob(['{"content": "test"}']).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer();assert.equal((await call('/ingest',compressed)).status,200);
  const exercised=await(await call('/exercise','{"count":2}')).json();assert.equal(exercised.expected.length,2);assert.ok(exercised.expected.every(v=>v.id!==v.rpc));
  const row={ScriptName:source,EventTimestampMs:Date.now(),EventType:'worker_rpc',Entrypoint:'Probe',Outcome:'ok',Logs:[{message:[JSON.stringify({event:'antfly.qualification_probe',id:exercised.expected[0].rpc,kind:'rpc',phase:'complete'})]}]};assert.equal((await call('/ingest',JSON.stringify(row))).status,200);
  const attr=(key,value)=>({key,value:{stringValue:value}}),otel={resourceSpans:[{resource:{attributes:[attr('service.name',source)]},scopeSpans:[{spans:[{traceId:'a'.repeat(32),spanId:'b'.repeat(16),startTimeUnixNano:'1000000000',endTimeUnixNano:'1020000000',attributes:[attr('faas.invocation_id','private-provider-id'),attr('cloudflare.outcome','ok')]}]}]}]};assert.equal((await call('/v1/traces',JSON.stringify(otel))).status,200);assert.equal((await call('/v1/logs','{"resourceLogs":[]}')).status,200);
  const report=await(await call('/report','{}')).json();assert.equal(report.objects.filter(o=>!o.key.includes('/counter/')).length,3);
  const witness=await(await call('/counter-witness',JSON.stringify({id:crypto.randomUUID()}))).json();assert.equal(witness.status,401);assert.match(witness.attempt_id_sha256,/^[a-f0-9]{64}$/);
  // workerd does not populate producer scriptName locally. Keep production
  // scope checks strict; prove actual callback cardinality without substituting
  // a name or claiming these foreign rows are qualified producer counters.
  let deliveries=[];for(let n=0;n<100;n++){const current=await(await call('/report','{}')).json();deliveries=current.objects.filter(o=>o.value.source==='provider-tail-dispatch').map(o=>o.value);if(deliveries.reduce((sum,d)=>sum+d.count+d.foreign_events,0)===6)break;await new Promise(r=>setTimeout(r,10));}assert.equal(deliveries.reduce((sum,d)=>sum+d.count+d.foreign_events,0),6);assert.equal(deliveries.reduce((sum,d)=>sum+d.count,0),0);
  assert.equal(report.qualification,false);assert.ok(!JSON.stringify(report).includes(token));
 }finally{await mf.dispose();await rm(persist,{recursive:true,force:true});}
});
