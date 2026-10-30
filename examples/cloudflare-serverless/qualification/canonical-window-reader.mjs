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

import {canonicalLayout, minuteOf, proofPrefix, windowPrefix} from './canonical-windows.mjs';

// No archive binding or producer calls: every body read is from a bounded,
// scope-validated canonical prefix. ETags pin bodies to their list/head result.
export async function readCanonicalWindow(env, scope, mode, input) {
 const prefix=proofPrefix(scope.capture_id), version=env.WORKER_VERSION?.id??null;
 const reply=value=>Response.json({qualification:false,capture_id:scope.capture_id,collector_version:version,evidence_layout:canonicalLayout,...value});
 const get=async item=>{if(!item||!Number.isSafeInteger(item.size)||item.size>4*1024*1024)throw Error('ObserverPageBudgetExceeded');const object=await env.LOGS.get(item.key,{onlyIf:{etagMatches:item.etag}});if(!object?.body||object.etag!==item.etag||object.size!==item.size)throw Error('ObserverObjectChanged');return {key:item.key,value:await object.json()};};
 if(mode==='layout') {
  if(Object.keys(input).length)throw Error('ObserverCursorInvalid');
  const key=prefix+'LAYOUT.json',meta=await env.LOGS.head(key);if(!meta||meta.size>4096)throw Error('ObserverLayoutInvalid');
  const [{value},batches,counter]=await Promise.all([get({...meta,key}),env.LOGS.list({prefix:prefix+'batches/',limit:1}),env.LOGS.list({prefix:prefix+'counter/',limit:1})]);
  if(value.schema!==1||value.evidence_layout!==canonicalLayout||value.capture_id!==scope.capture_id||value.producer!==scope.source||batches.objects.length||counter.objects.length)throw Error('ObserverLayoutInvalid');
  return reply({layout:value});
 }
 if(mode==='witnesses') {
  if(Object.keys(input).length!==1||typeof input.hash!=='string'||!/^[a-f0-9]{64}$/.test(input.hash))throw Error('ObserverWitnessInvalid');
  const page=await env.LOGS.list({prefix:prefix+'witnesses/'+input.hash+'/',limit:2});if(page.truncated||page.objects.length!==1)throw Error('ObserverWitnessInvalid');
  const pointer=(await get(page.objects[0])).value;
  if(pointer.schema!==1||pointer.evidence_layout!==canonicalLayout||pointer.capture_id!==scope.capture_id||pointer.producer!==scope.source||pointer.attempt_id_sha256!==input.hash||typeof pointer.counter_key!=='string'||!pointer.counter_key.startsWith(windowPrefix(scope.capture_id,pointer.timestamp)+'counter/'))throw Error('ObserverWitnessInvalid');
  const object=await env.LOGS.head(pointer.counter_key);if(!object||object.size>4*1024*1024)throw Error('ObserverWitnessInvalid');return reply({pointer,object:{key:pointer.counter_key,size:object.size,etag:object.etag}});
 }
 const keys=mode==='window'?['minute','cursor']:['cursor'];
 if(Object.keys(input).some(k=>!keys.includes(k))||input.cursor!=null&&(typeof input.cursor!=='string'||input.cursor.length>4096))throw Error('ObserverCursorInvalid');
 if(mode==='window'&&minuteOf(input.minute)!==input.minute)throw Error('ObserverWindowInvalid');
 const selected=mode==='window'?windowPrefix(scope.capture_id,input.minute):prefix+'rejections/';
 let limit=32,page;
 for(;;){page=await env.LOGS.list({prefix:selected,limit,...(input.cursor?{cursor:input.cursor}:{})});if(page.objects.reduce((n,o)=>n+o.size,0)<=4*1024*1024)break;if(limit===1)throw Error('ObserverPageBudgetExceeded');limit=Math.max(1,Math.floor(limit/2));}
 if(page.truncated&&(!page.cursor||page.cursor===input.cursor))throw Error('ObserverCursorInvalid');
 // A provider cursor must never escape the selected partition.
 if(page.objects.some(o=>!o.key.startsWith(selected)))throw Error('ObserverCursorInvalid');
 const outcomes=await Promise.allSettled(page.objects.map(get));if(outcomes.some(o=>o.status==='rejected'))throw Error('ObserverPageReadFailed');
 return reply({objects:outcomes.map(o=>o.value),cursor:page.truncated?page.cursor:null});
}
