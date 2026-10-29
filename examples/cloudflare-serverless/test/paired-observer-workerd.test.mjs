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
test('workerd paired observer routes eight private source bindings and isolated durable evidence',{timeout:30000},async()=>{
 const pair='antfly-1278-g-local-1',scopes=['b','c'].flatMap(side=>['','-writer','-reader','-builder'].map(role=>({source:pair+'-'+side+role,capture_id:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i})),token='local-only-disposable',bundle=await build({entryPoints:['qualification/paired-observer-worker.mjs'],bundle:true,write:false,format:'esm',platform:'neutral',external:['cloudflare:workers']}),workers=[{name:'observer',modules:[{type:'ESModule',path:resolve('observer.mjs'),contents:bundle.outputFiles[0].text}],compatibilityDate:'2026-10-23',bindings:{API_TOKEN:token,SOURCE_SCOPES:JSON.stringify(scopes)},r2Buckets:{LOGS:'telemetry'},serviceBindings:Object.fromEntries(scopes.map(s=>[s.binding,s.source]))},...scopes.map(s=>({name:s.source,modules:[{type:'ESModule',path:resolve(s.source+'.mjs'),contents:'export default{fetch(request){return new Response(request.headers.has("Authorization")?"credential leaked":"not found",{status:404});}}'}],compatibilityDate:'2026-10-23'}))],mf=new Miniflare(convertV4MiniflareOptions({workers}));
 const call=(path,body)=>mf.dispatchFetch('https://local.invalid'+path,{method:'POST',headers:{Authorization:'Bearer '+token},body:JSON.stringify(body)});
 try{for(const s of scopes){const response=await call('/source/'+s.source+'/counter-witness',{id:crypto.randomUUID()});assert.equal(response.status,200);const value=await response.json();assert.equal(value.status,404);assert.equal(value.producer,s.source);const page=await(await call('/source/'+s.source+'/page',{})).json();assert.equal(page.capture_id,s.capture_id);assert.deepEqual(page.objects,[]);}assert.equal((await(await call('/health',{})).json()).rejections_present,false);assert.equal((await call('/source/production/page',{})).status,404);}finally{await mf.dispose();}
});
