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

import{test}from'node:test';import assert from'node:assert/strict';import{Miniflare,convertV4MiniflareOptions}from'miniflare';import{resolve}from'node:path';import{documentBody}from'../qualification/http-trial-driver.mjs';
test('remote HTTP driver runs under workerd and preserves sibling failures',{timeout:30000},async()=>{
 const version='01234567-0123-4567-89ab-0123456789ab',fence='01'+'a'.repeat(64)+'0000000000000001',token='c'.repeat(64),origin='https://antfly-1278-cold-abc-1234.example.workers.dev';let calls=0;
 const mf=new Miniflare(convertV4MiniflareOptions({modules:true,scriptPath:resolve('qualification/http-trial-driver.mjs'),compatibilityDate:'2026-10-23',compatibilityFlags:['global_fetch_strictly_public'],bindings:{API_TOKEN:token,BASELINE_TOKEN:token,BASELINE_ORIGIN:origin},outboundService:async request=>{calls++;assert.equal(request.headers.get('Authorization'),'Bearer '+token);const input=await request.json();if(input.doc_id==='doc/2')return Response.json({error:'SQLITE_NOMEM'},{status:503});return Response.json({body:documentBody('a',1),visibility:fence},{headers:{'X-Antfly-Version':version}});}}));
 try{const response=await mf.dispatchFetch('https://driver.invalid/run',{method:'POST',headers:{Authorization:'Bearer '+token},body:JSON.stringify({side:'baseline',id:'probe',version,steps:[1,2].map(id=>({op:'lookup',tenant:'a',id,visibility:fence}))})});assert.equal(response.status,200);const result=await response.json();assert.equal(calls,2);assert.equal(result.passed,false);assert.equal(result.samples[0].outcome,'ok');assert.equal(result.samples[1].error,'SQLITE_NOMEM');assert.ok(!JSON.stringify(result).includes(token));}finally{await mf.dispose();}
});
