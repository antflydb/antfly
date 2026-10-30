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

// Shared per isolate, rather than per source or request. Waiting callers leave
// their HTTP bodies unread. Only immutable tickets cross request contexts:
// timers, promises, cancellation and I/O stay with the submitting invocation.
export class DiagnosticAdmission {
 constructor({maxQueued=4,waitMs=15000,pollMs=5,clock=()=>performance.now(),sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms))}={}){this.maxQueued=maxQueued;this.waitMs=waitMs;this.pollMs=pollMs;this.clock=clock;this.sleep=sleep;this.pending=[];this.active=0;this.peak=0;this.queued=0;this.queuePeak=0;}
 report(){return {active:this.active,pending:this.pending.length,peak_active:this.peak,queued:this.queued,peak_pending:this.queuePeak};}
 async run(work,{signal}={}){
  if(this.pending.length+this.active>=this.maxQueued+1)throw Error('TelemetryDiagnosticAdmissionExceeded');
  if(this.active||this.pending.length)this.queued++;
  const ticket=Object.freeze({}),deadline=this.clock()+this.waitMs;this.pending.push(ticket);this.queuePeak=Math.max(this.queuePeak,this.pending.length);let entered=false;
  try{
   for(;;){if(signal?.aborted)throw Error('TelemetryDiagnosticCanceled');if(this.clock()>=deadline)throw Error('TelemetryDiagnosticAdmissionExpired');if(!this.active&&this.pending[0]===ticket){this.pending.shift();this.active=1;this.peak=Math.max(this.peak,this.active);entered=true;break;}await this.sleep(this.pollMs);}
   return await work();
  }finally{if(entered)this.active=0;else{const index=this.pending.indexOf(ticket);if(index>=0)this.pending.splice(index,1);}}
 }
}
export const diagnosticAdmission=new DiagnosticAdmission();

// A byte cap alone does not bound JSON.parse's heap amplification. Preflight
// the complete payload (including NDJSON) without building a token tree.
export function checkDiagnosticJson(text,{maxNodes=262144,maxDepth=32}={}){
 if(typeof text!=='string'||text.length>6*1024*1024)throw Error('TelemetryBodyExceeded');
 let nodes=0,depth=0;
 for(let i=0;i<text.length;i++){
  const c=text[i];if(c===' '||c==='\n'||c==='\r'||c==='\t'||c===','||c===':')continue;
  if(c===']'||c==='}'){depth--;continue;}
  if(++nodes>maxNodes)throw Error('TelemetryJsonComplexityExceeded');
  if(c==='['||c==='{'){if(++depth>maxDepth)throw Error('TelemetryJsonComplexityExceeded');continue;}
  if(c==='"'){for(i++;i<text.length;i++){if(text[i]==='\\'){i++;continue;}if(text[i]==='"')break;}continue;}
  while(i+1<text.length&&!/[\s,\]}:]/.test(text[i+1]))i++;
 }
}
export function parseDiagnosticJson(text,{lines=false,...limits}={}){
 checkDiagnosticJson(text,limits);
 try{return JSON.parse(text);}catch(error){if(!lines)throw error;return text.trim().split('\n').map(line=>JSON.parse(line));}
}
