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
export const exportAdmission=new DiagnosticAdmission({maxQueued:64});

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

// Prepared bytes are charged through completion of every physical write. The
// parser can admit another caller once this independent I/O permit is owned.
export class DiagnosticWriteBudget {
 constructor({maxBytes=16*1024*1024,maxActive=16,waitMs=15000,pollMs=5,clock=()=>performance.now(),sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms))}={}){Object.assign(this,{maxBytes,maxActive,waitMs,pollMs,clock,sleep});this.bytes=0;this.active=0;this.peakBytes=0;this.peakActive=0;this.pending=[];}
 report(){return {active:this.active,reserved_bytes:this.bytes,peak_active:this.peakActive,peak_reserved_bytes:this.peakBytes,limit_bytes:this.maxBytes,limit_active:this.maxActive};}
 async reserve(bytes){
  if(!Number.isSafeInteger(bytes)||bytes<0||bytes>this.maxBytes)throw Error('TelemetryDiagnosticWriteBudgetExceeded');
  const ticket=Object.freeze({}),deadline=this.clock()+this.waitMs;this.pending.push(ticket);
  try{for(;;){if(this.clock()>=deadline)throw Error('TelemetryDiagnosticWriteAdmissionExpired');if(this.pending[0]===ticket&&this.active<this.maxActive&&bytes<=this.maxBytes-this.bytes){this.pending.shift();this.bytes+=bytes;this.active++;this.peakBytes=Math.max(this.peakBytes,this.bytes);this.peakActive=Math.max(this.peakActive,this.active);let owned=true;return()=>{if(owned){owned=false;this.bytes-=bytes;this.active--;}};}await this.sleep(this.pollMs);}}
  finally{const index=this.pending.indexOf(ticket);if(index>=0)this.pending.splice(index,1);}
 }
}
export const diagnosticWrites=new DiagnosticWriteBudget();

// Tail events arrive already decoded by the provider. More small deliveries
// can wait for the CPU lane only while their aggregate input charge fits.
export const tailAdmission=new DiagnosticAdmission({maxQueued:64});
let tailInputBytes=0,tailInputPeak=0;
export function reserveTailInput(value,{limitBytes=16*1024*1024}={}){
 let bytes=0,nodes=0;
 const visit=(item,depth)=>{if(depth>32||++nodes>262144)throw Error('TelemetryJsonComplexityExceeded');bytes+=32;if(typeof item==='string')bytes+=item.length*2;else if(item&&typeof item==='object'){for(const key in item){bytes+=key.length*2+16;if(bytes>limitBytes)throw Error('TelemetryDiagnosticInputBudgetExceeded');visit(item[key],depth+1);}}if(bytes>limitBytes)throw Error('TelemetryDiagnosticInputBudgetExceeded');};visit(value,0);
 if(bytes>limitBytes-tailInputBytes)throw Error('TelemetryDiagnosticInputBudgetExceeded');tailInputBytes+=bytes;tailInputPeak=Math.max(tailInputPeak,tailInputBytes);let held=true;
 return()=>{if(held){held=false;tailInputBytes-=bytes;}};
}
export const tailInputMetrics=()=>({reserved_bytes:tailInputBytes,peak_reserved_bytes:tailInputPeak,limit_bytes:16*1024*1024});
