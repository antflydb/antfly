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

// Native-normalized committed data only. A gap/eviction is a miss, never absence.
export class ResidentTail {
 constructor({maxBytes=64*1024,maxRecords=128,onCharge=()=>{}}={}){Object.assign(this,{maxBytes,maxRecords,onCharge});this.clear();}
 clear(){this.value=null;this.epoch=null;this.onCharge(0);}
 install(value,epoch){
  if(!value){this.clear();return;}
  if(!Array.isArray(value.scope)||value.scope.length!==32||!value.scope.every(x=>Number.isInteger(x)&&x>=0&&x<=255)||!Array.isArray(value.mutations)||!value.mutations.length||value.mutations.length>this.maxRecords||!['start_lsn','end_lsn'].every(k=>typeof value[k]==='string'&&/^[1-9][0-9]{0,19}$/.test(value[k])))throw Error('InvalidResidentTail');
  const start=BigInt(value.start_lsn),end=BigInt(value.end_lsn);if(end>18446744073709551615n||end-start+1n!==BigInt(value.mutations.length))throw Error('InvalidResidentTail');
  const identity=JSON.stringify(epoch),same=this.epoch===identity&&JSON.stringify(this.value?.scope)===JSON.stringify(value.scope);
  if(same&&end<=BigInt(this.value.end_lsn))return;
  let next=structuredClone(value);
  if(same&&start===BigInt(this.value.end_lsn)+1n)next={...next,start_lsn:this.value.start_lsn,mutations:[...this.value.mutations,...next.mutations]};
  while(next.mutations.length>this.maxRecords||this.charge(next)>this.maxBytes){if(next.mutations.length===1){this.clear();return;}next.mutations.shift();next.start_lsn=String(BigInt(next.start_lsn)+1n);}
  this.value=next;this.epoch=identity;this.onCharge(this.charge(next));
 }
 charge(value){return new TextEncoder().encode(JSON.stringify(value)).byteLength*2+value.mutations.length*256+256;}
 snapshot(epoch){return this.epoch===JSON.stringify(epoch)?structuredClone(this.value):null;}
}
