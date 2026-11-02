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

import{DurableObject}from'cloudflare:workers';
import{baselineInput,baselineResult,baselineSettings,timedStage}from'./platform-baseline-common.mjs';
import{resourceFailureCode}from'../performance.mjs';
export class PlatformBaselineOwner extends DurableObject{
 constructor(ctx,env){super(ctx,env);this.instance=crypto.randomUUID();this.settings=baselineSettings(env);this.memory=this.settings.linear_pages?new WebAssembly.Memory({initial:this.settings.linear_pages,maximum:this.settings.linear_pages}):null;this.retained=new Uint8Array(this.settings.retained_bytes);this.retained.fill(1);if(this.memory)new Uint8Array(this.memory.buffer).fill(1);this.active=0;this.peak=0;}
 async prepare(){await this.ctx.storage.put('seed','x'.repeat(this.settings.payload_bytes));await this.ctx.storage.sync();return{instance:this.instance,version:this.env.WORKER_VERSION?.id??null,...this.settings};}
 async verify(ids,op='owner_put'){if(!Array.isArray(ids)||ids.length>32||ids.some(id=>!/^[a-f0-9-]{36}$/.test(id)))throw Error('BaselineVerificationInvalid');for(const id of ids){const value=op==='owner_r2_put'?await(await this.env.DATA.get('owner-sample/'+id))?.text():await this.ctx.storage.get('sample/'+id);if(value!=='x'.repeat(this.settings.payload_bytes))throw Error('BaselineStoredValueMismatch');}return{verified:true,count:ids.length};}
 async probe(input){
  baselineInput(input);const stages=[],started=performance.now();this.peak=Math.max(this.peak,++this.active);
  try{
   if(input.op==='owner_get'){const end=timedStage(stages,'storage_get'),value=await this.ctx.storage.get('seed');end();if(value!=='x'.repeat(this.settings.payload_bytes))throw Error('BaselineStoredValueMismatch');}
   else if(input.op==='owner_put'){const key='sample/'+input.id;let end=timedStage(stages,'storage_put');await this.ctx.storage.put(key,'x'.repeat(this.settings.payload_bytes));end();end=timedStage(stages,'storage_sync');await this.ctx.storage.sync();end();}
   else if(input.op==='owner_r2_get'){const end=timedStage(stages,'owner_r2_get'),object=await this.env.DATA.get('seed'),value=await object?.text();end();if(value!=='x'.repeat(this.settings.payload_bytes))throw Error('BaselineStoredValueMismatch');}
   else if(input.op==='owner_r2_put'){const end=timedStage(stages,'owner_r2_put'),object=await this.env.DATA.put('owner-sample/'+input.id,'x'.repeat(this.settings.payload_bytes),{onlyIf:{etagDoesNotMatch:'*'}});end();if(!object?.etag)throw Error('BaselineConditionalPutFailed');}
   else if(input.op!=='owner_echo')throw Error('BaselineOwnerOperationInvalid');
   return baselineResult(input,this.env,{instance:this.instance,owner_ms:Math.max(0,performance.now()-started),stages:stages.map(s=>({...s,start_ms:s.start_ms-started})),memory:{linear_bytes:this.memory?.buffer.byteLength??0,retained_bytes:this.retained.length,payload_bytes:this.settings.payload_bytes,active:this.active,peak_active:this.peak,total_isolate_bytes:null}});
  }catch(error){const failure_code=resourceFailureCode(error);if(failure_code)console.error(JSON.stringify({event:'antfly.baseline_resource_failure',failure_code}));throw error;}finally{this.active--;}
 }
}
export default{async fetch(request,env){if(request.method!=='POST')return new Response('private',{status:404});const input=baselineInput(await request.json());if(input.op!=='service_echo')return new Response('private',{status:404});return Response.json(baselineResult(input,env));}};
