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
 for(const [name,file]of [['collector','logpush-probe-collector.mjs'],[source,'logpush-probe-source.mjs']]){const result=await build({entryPoints:['qualification/'+file],bundle:true,write:false,format:'esm',platform:'neutral',external:['cloudflare:workers']});workers.push({name,modules:[{type:'ESModule',path:resolve(name+'.mjs'),contents:result.outputFiles[0].text}],compatibilityDate:'2026-10-23',...(name==='collector'?{bindings:{API_TOKEN:token,CAPTURE_ID:capture,SOURCE_WORKER:source},r2Buckets:{LOGS:'telemetry'},serviceBindings:{FIXTURE:source}}:{bindings:{API_TOKEN:token},durableObjects:{PROBES:{className:'Probe',useSQLite:true}}})});}
 const persist=await mkdtemp(resolve('dist','logpush-workerd-')),mf=new Miniflare(convertV4MiniflareOptions({workers,resourcePersistencePath:persist}));
 const call=(path,body,headers={})=>mf.dispatchFetch('https://local.invalid'+path,{method:'POST',headers:{Authorization:'Bearer '+token,...headers},body});
 try{
  const privateSource=await mf.getWorker(source);assert.equal((await privateSource.fetch('https://probe.invalid/?nonce='+crypto.randomUUID(),{method:'POST'})).status,401);
  const compressed=await new Response(new Blob(['{"content": "test"}']).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer();assert.equal((await call('/ingest',compressed)).status,200);
  const exercised=await(await call('/exercise','{"count":2}')).json();assert.equal(exercised.expected.length,2);assert.ok(exercised.expected.every(v=>v.id!==v.rpc));
  const row={ScriptName:source,EventTimestampMs:Date.now(),EventType:'worker_rpc',Entrypoint:'Probe',Outcome:'ok',Logs:[{message:[JSON.stringify({event:'antfly.qualification_probe',id:exercised.expected[0].rpc,kind:'rpc',phase:'complete'})]}]};assert.equal((await call('/ingest',JSON.stringify(row))).status,200);
  const attr=(key,value)=>({key,value:{stringValue:value}}),otel={resourceSpans:[{resource:{attributes:[attr('service.name',source)]},scopeSpans:[{spans:[{traceId:'a'.repeat(32),spanId:'b'.repeat(16),startTimeUnixNano:'1000000000',endTimeUnixNano:'1020000000',attributes:[attr('faas.invocation_id','private-provider-id'),attr('cloudflare.outcome','ok')]}]}]}]};assert.equal((await call('/v1/traces',JSON.stringify(otel))).status,200);assert.equal((await call('/v1/logs','{"resourceLogs":[]}')).status,200);
  const report=await(await call('/report','{}')).json();assert.equal(report.objects.length,3);assert.equal(report.qualification,false);assert.ok(!JSON.stringify(report).includes(token));
 }finally{await mf.dispose();await rm(persist,{recursive:true,force:true});}
});
