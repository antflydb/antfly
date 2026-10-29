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

// An independent provider-terminal delivery ledger, never an application
// invocation ID. Each delivery owns an immutable R2 object; no shared counter
// row or write is placed on the producer's critical path.
import {sanitizeTailEvent} from './metrics-tail.mjs';
const uuid=v=>typeof v==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(v);
export async function persistTailDelivery(events,env){
 if(!uuid(env.CAPTURE_ID)||!/^antfly-1278-(?:telemetry-[a-z0-9]+-source|g-[a-z0-9]+-[1-3]-[bc](?:-(?:writer|reader|builder))?)$/.test(env.SOURCE_WORKER??''))throw Error('CounterScopeInvalid');
 if(!Array.isArray(events)||events.length>1000)throw Error('CounterDeliveryBudgetExceeded');
 const records=[];let foreign=0;
 for(const item of events){
  if(item.scriptName!==env.SOURCE_WORKER){foreign++;continue;}
  if(!Number.isSafeInteger(item.eventTimestamp)||item.eventTimestamp<0)throw Error('CounterTimestampInvalid');
  const event=sanitizeTailEvent(item)??{};
  records.push({script:item.scriptName,timestamp:item.eventTimestamp,version_id:uuid(item.scriptVersion?.id)?item.scriptVersion.id.toLowerCase():null,event});
 }
 const delivery=crypto.randomUUID(),value=JSON.stringify({schema:1,qualification:false,source:'provider-tail-dispatch',delivery_id:delivery,producer:env.SOURCE_WORKER,received_at:Date.now(),collector_version:env.WORKER_VERSION?.id??null,foreign_events:foreign,count:records.length,records});
 if(new TextEncoder().encode(value).byteLength>4*1024*1024)throw Error('CounterDeliveryBudgetExceeded');
 const result=await env.LOGS.put('probe-v1/'+env.CAPTURE_ID+'/counter/'+delivery+'.json',value,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'application/json'}});
 if(!result)throw Error('CounterDeliveryCollision');
 return{delivery_id:delivery,count:records.length};
}
export default{tail(events,env,ctx){ctx.waitUntil(persistTailDelivery(events,env));},fetch(){return new Response('not found',{status:404});}};
