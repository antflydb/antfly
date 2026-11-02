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

import{baselineInput,baselineResult,baselineSettings,timedStage,baselineJson}from'./platform-baseline-common.mjs';
import{resourceFailureCode}from'../performance.mjs';
let active=0;
export default{async fetch(request,env){
 if(!env.API_TOKEN||request.headers.get('Authorization')!=='Bearer '+env.API_TOKEN)return new Response('unauthorized',{status:401});
 if(request.method!=='POST')return new Response('not found',{status:404});if(active>=16)return Response.json({error:'BaselineBusy'},{status:503});active++;
 const started=performance.now(),stages=[];
 try{
  const settings=baselineSettings(env),path=new URL(request.url).pathname;
  if(path==='/ready')return Response.json({schema:1,qualification:false,version:env.WORKER_VERSION?.id??null,settings});
  // Fixed keys only. No arbitrary URLs, object names, SQL, memory sizes or credentials.
  if(path==='/prepare'){await env.DATA.put('seed','x'.repeat(settings.payload_bytes));return Response.json({schema:1,qualification:false,prepared:true,owner:await env.OWNERS.getByName('warm',env.BASELINE_LOCATION_HINT?{locationHint:env.BASELINE_LOCATION_HINT}:undefined).prepare(),settings,version:env.WORKER_VERSION?.id??null});}
  if(path==='/verify'){const value=await baselineJson(request);if(!['r2_put','owner_put','owner_r2_put'].includes(value?.op)||!Array.isArray(value.ids)||value.ids.length>32||value.ids.some(id=>!/^[a-f0-9-]{36}$/.test(id)))throw Error('BaselineVerificationInvalid');if(value.op.startsWith('owner_'))return Response.json(await env.OWNERS.getByName('warm',env.BASELINE_LOCATION_HINT?{locationHint:env.BASELINE_LOCATION_HINT}:undefined).verify(value.ids,value.op));for(const id of value.ids){const object=await env.DATA.get('sample/'+id);if(await object?.text()!=='x'.repeat(settings.payload_bytes))throw Error('BaselineStoredValueMismatch');}return Response.json({verified:true,count:value.ids.length});}
  if(path!=='/probe')return new Response('not found',{status:404});
  const input=baselineInput(await baselineJson(request));let result=baselineResult(input,env);
  if(input.op==='service_echo'){const end=timedStage(stages,'service_rpc'),r=await env.SERVICE.fetch('https://private.invalid/',{method:'POST',body:JSON.stringify(input)});if(!r.ok)throw Error('BaselineServiceFailed');result=await r.json();end();}
  else if(input.op.startsWith('owner_')){if(input.fresh&&(input.op!=='owner_echo'||settings.linear_pages||settings.retained_bytes))throw Error('BaselineFreshOwnerEchoOnly');const end=timedStage(stages,'owner_rpc'),owner=env.OWNERS.getByName(input.fresh?'fresh/'+input.id:'warm',env.BASELINE_LOCATION_HINT?{locationHint:env.BASELINE_LOCATION_HINT}:undefined);result=await owner.probe(input);end();}
  else if(input.op==='r2_get'){const end=timedStage(stages,'r2_get'),object=await env.DATA.get('seed'),value=await object?.text();end();if(value!=='x'.repeat(settings.payload_bytes))throw Error('BaselineStoredValueMismatch');}
  else if(input.op==='r2_put'){const end=timedStage(stages,'r2_put'),object=await env.DATA.put('sample/'+input.id,'x'.repeat(settings.payload_bytes),{onlyIf:{etagDoesNotMatch:'*'}});end();if(!object?.etag)throw Error('BaselineConditionalPutFailed');}
  return Response.json({...result,version:env.WORKER_VERSION?.id??null,private_version:input.op==='service_echo'||input.op.startsWith('owner_')?result.version:null,http_ms:Math.max(0,performance.now()-started),http_stages:stages.map(s=>({...s,start_ms:s.start_ms-started})),settings,app_colo:request.cf?.colo??null,placement_hint:env.BASELINE_LOCATION_HINT??null,actual_owner_region:null},{headers:{'Cache-Control':'no-store'}});
 }catch(error){const failure_code=resourceFailureCode(error);if(failure_code)console.error(JSON.stringify({event:'antfly.baseline_resource_failure',failure_code}));return Response.json({error:failure_code??(/^Baseline[A-Za-z]+$/.test(error.message)?error.message:'BaselineFailed')},{status:503});}finally{active--;}
}};
