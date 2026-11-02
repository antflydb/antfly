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

import{test}from'node:test';import assert from'node:assert/strict';import{build}from'esbuild';import{Miniflare,convertV4MiniflareOptions}from'miniflare';
test('immutable flights wake independent workerd requests with owned cache bytes and preserve resource failures',async()=>{
 const bundle=await build({stdin:{resolveDir:process.cwd(),contents:`
 import{ImmutableFlights}from'../../zig/lib/objectstore/wasm/immutable-flights.mjs';
 import{ObjectStoreError}from'../../zig/lib/objectstore/wasm/r2.mjs';
 import{ReadCohorts}from'./read-cohorts.mjs';
 const flights=new ImmutableFlights(),cache=new Map();let calls=0,cohorts;
 export default{async fetch(request,env){const url=new URL(request.url);if(url.pathname==='/seed'){await env.DATA.put('antfly/artifacts/sha256/aa/'+'b'.repeat(62),'owned');return new Response('ok');}if(url.pathname==='/metrics')return Response.json({calls,active:flights.entries.size});
 if(url.pathname==='/cohort'){cohorts??=new ReadCohorts(async members=>{await new Promise(r=>setTimeout(r,30));const object=await env.DATA.get('antfly/artifacts/sha256/aa/'+'b'.repeat(62)),body=await object.text();return members.map(()=>({body}));});return new Response((await cohorts.submit({op:'lookup',doc_id:'one'})).body);}
 const fail=url.pathname==='/fail',key='antfly/artifacts/sha256/aa/'+(fail?'c':'b').repeat(62);
 const read=flights.wrap(async req=>{if(cache.has(req.key))return{body:cache.get(req.key).slice()};calls++;await new Promise(r=>setTimeout(r,75));const object=await env.DATA.get(req.key);if(fail)throw new ObjectStoreError(8,'ObjectIoBudgetExceeded');const body=new Uint8Array(await object.arrayBuffer());cache.set(req.key,body);return{body:body.slice()};},'fixture');
 try{return new Response((await read({op:'get',bucket:'database',key})).body);}catch(error){return Response.json({error:error.message,status:error.status},{status:503});}}};`},bundle:true,write:false,format:'esm',platform:'neutral'});
 const mf=new Miniflare(convertV4MiniflareOptions({workers:[{name:'fixture',modules:true,script:bundle.outputFiles[0].text,compatibilityDate:'2026-10-23',r2Buckets:['DATA']}]}));
 try{
  await mf.dispatchFetch('https://fixture/seed');
  const responses=await Promise.all(Array.from({length:4},()=>mf.dispatchFetch('https://fixture/read')));assert.deepEqual(await Promise.all(responses.map(r=>r.text())),Array(4).fill('owned'));
  assert.deepEqual(await(await mf.dispatchFetch('https://fixture/metrics')).json(),{calls:1,active:0});
  const failures=await Promise.all(Array.from({length:3},()=>mf.dispatchFetch('https://fixture/fail')));for(const response of failures){assert.equal(response.status,503);assert.deepEqual(await response.json(),{error:'ObjectIoBudgetExceeded',status:8});}
  assert.deepEqual(await(await mf.dispatchFetch('https://fixture/metrics')).json(),{calls:2,active:0});
  const grouped=await Promise.all(Array.from({length:4},()=>mf.dispatchFetch('https://fixture/cohort')));assert.deepEqual(await Promise.all(grouped.map(r=>r.text())),Array(4).fill('owned'));
 }finally{await mf.dispose();}
});
