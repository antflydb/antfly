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

import{reserveInput}from'./request-pool.mjs';
// Bounded data tickets; every submitting request owns its timers and I/O.
export class ReadCohorts {
 constructor(run){this.run=run;this.queue=[];this.pending=0;this.bytes=0;this.running=0;this.scheduled=false;}
 async submit(input,traceId){
  const bytes=new TextEncoder().encode(JSON.stringify(input)).byteLength;
  if(bytes>4096||this.pending>=16||this.bytes+bytes>65536)throw Error('CoordinatorCommitBusy');
  const release=reserveInput(bytes);this.pending++;this.bytes+=bytes;
  const item={input,traceId,wall:Date.now(),mono:performance.now(),bytes,leader:false,done:false};this.queue.push(item);this.schedule();
  try{
   while(!item.done){
    await new Promise(resolve=>setTimeout(resolve,2));
    if(item.leader){item.leader=false;await this.drain();}
   }
   if(item.failure)throw Error(item.failure);
   if(item.result.error)throw Error(item.result.error);
   return item.result;
  }finally{release();}
 }
 schedule(){if(!this.scheduled&&this.running<2&&this.queue.length){this.scheduled=true;this.queue[0].leader=true;}}
 async drain(){
  this.scheduled=false;this.running++;const group=this.queue.splice(0,16);this.schedule();let results,failure;
  try{results=await this.run(group);if(!Array.isArray(results)||results.length!==group.length||!results.every(value=>value&&typeof value==='object'))throw Error('InvalidReadCohortResults');}catch(e){failure=String(e.message??e).slice(0,2048);}
  this.running--;this.pending-=group.length;this.bytes-=group.reduce((n,x)=>n+x.bytes,0);
  for(let i=0;i<group.length;i++){group[i].failure=failure;group[i].result=results?.[i];group[i].done=true;}
  this.schedule();
 }
}
