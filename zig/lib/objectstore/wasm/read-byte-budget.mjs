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

export class ReadByteBudget {
 constructor({maxBytes=204800,reserveQueued}={}){if(!Number.isSafeInteger(maxBytes)||maxBytes<4096||maxBytes>204800)throw Error("InvalidReadByteBudget");this.maxBytes=maxBytes;this.reserveQueued=reserveQueued;this.bytes=0;this.peakBytes=0;this.active=0;this.pending=[];this.lastKey=null;}
 acquire(key,bytes){
  if(!Number.isSafeInteger(bytes)||bytes<1||bytes>this.maxBytes)throw Error('InvalidReadByteReservation');
  if(this.pending.length>=32)throw Error('ReadAheadCapacityExceeded');
  const immediate=!this.pending.length&&this.active<32&&bytes<=this.maxBytes-this.bytes;const queued=immediate?undefined:this.reserveQueued?.();if(!immediate&&this.reserveQueued&&!queued)throw Error('ReadAheadCapacityExceeded');
  const ticket={key,bytes,at:performance.now(),queued,release:null,canceled:false};this.pending.push(ticket);this.drain();
  return{ready:(async()=>{while(!ticket.release&&!ticket.canceled)await new Promise(r=>setTimeout(r,2));if(ticket.canceled)throw Error('ReadAheadCanceled');return ticket.release;})(),cancel:()=>{if(ticket.canceled)return;ticket.canceled=true;const i=this.pending.indexOf(ticket);if(i>=0){this.pending.splice(i,1);ticket.queued?.();}ticket.release?.();this.drain();}};
 }
 drain(){
  while(this.pending.length&&this.active<32){
   const room=this.maxBytes-this.bytes,aged=this.pending.find(t=>performance.now()-t.at>=5000);if(aged&&aged.bytes>room)return;
   let i=aged?this.pending.indexOf(aged):this.pending.findIndex(t=>t.bytes<=room&&t.key!==this.lastKey);
   if(i<0)i=this.pending.findIndex(t=>t.bytes<=room);if(i<0)return;
   const t=this.pending.splice(i,1)[0];t.queued?.();this.bytes+=t.bytes;this.active++;this.peakBytes=Math.max(this.peakBytes,this.bytes);this.lastKey=t.key;let held=true;
   t.release=()=>{if(!held)return;held=false;this.bytes-=t.bytes;this.active--;this.drain();};
  }
 }
}
const budgets=new WeakMap();
export function readByteBudgetFor(kernel){let b=budgets.get(kernel);if(!b){b=new ReadByteBudget({reserveQueued:kernel.reserveQueuedAdmission?.bind(kernel)});budgets.set(kernel,b);}return b;}
export function readByteMetrics(kernel){const b=budgets.get(kernel);return{point_graph_active:b?.active??0,point_graph_queued:b?.pending.length??0,point_graph_bytes:b?.bytes??0,point_graph_peak_bytes:b?.peakBytes??0,point_graph_limit_bytes:b?.maxBytes??204800};}
