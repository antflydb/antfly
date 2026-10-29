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

import{test}from'node:test';import assert from'node:assert/strict';import{build}from'esbuild';import{Miniflare,convertV4MiniflareOptions}from'miniflare';import{resolve}from'node:path';
test('workerd bulk export receiver partitions real R2 writes while preserving provider records',{timeout:30000},async()=>{const pair='antfly-1278-g-bulk-1',scopes=['b','c'].flatMap(side=>['','-writer','-reader','-builder'].map(role=>({source:pair+'-'+side+role,capture_id:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i})),token='local-bulk-only',bundle=await build({entryPoints:['qualification/paired-export-receiver.mjs'],bundle:true,write:false,format:'esm',platform:'neutral'}),mf=new Miniflare(convertV4MiniflareOptions({name:'receiver',modules:[{type:'ESModule',path:resolve('bulk-receiver.mjs'),contents:bundle.outputFiles[0].text}],compatibilityDate:'2026-10-23',bindings:{API_TOKEN:token,SOURCE_SCOPES:JSON.stringify(scopes)},r2Buckets:{LOGS:'telemetry'}})),payload=[{resourceSpans:Array.from({length:256},(_,i)=>({resource:{attributes:[{key:'service.name',value:{stringValue:scopes[0].source}}]},scopeSpans:[{spans:[{traceId:'a'.repeat(32),spanId:'b'.repeat(16),startTimeUnixNano:'1000000000',endTimeUnixNano:'1010000000',attributes:[{key:'faas.invocation_id',value:{stringValue:'provider-'+i}}]}]}]}))}];try{const response=await mf.dispatchFetch('https://receiver.invalid/v1/traces',{method:'POST',headers:{Authorization:'Bearer '+token},body:JSON.stringify(payload)});assert.equal(response.status,200);const health=await(await mf.dispatchFetch('https://receiver.invalid/health',{headers:{Authorization:'Bearer '+token}})).json();assert.equal(health.shapes[0].resources,256);assert.equal(health.shapes[0].chunks,2);assert.equal(health.shapes[0].accepted,true);const bucket=await mf.getR2Bucket('LOGS'),page=await bucket.list({prefix:'probe-v1/'+scopes[0].capture_id+'/'});assert.equal(page.objects.length,2);let rows=0;for(const item of page.objects)rows+=(await(await bucket.get(item.key)).json()).rows.length;assert.equal(rows,256);}finally{await mf.dispose();}});
