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

// Preparation is speculative. Native final commit authenticates fresh ownership,
// the expected WAL ETag and every retained source dependency before acknowledgement.
export class OptimisticWal {
 constructor({clock=Date.now}={}){this.clock=clock;this.value=null;}
 clear(){this.value=null;}
 remember(view,walKey,head){
  const copy=structuredClone(view);if(head){const row=copy.entries.find(row=>row[0]===walKey);if(!row)return this.clear();row[1]=structuredClone(head);}
  const pin=copy.entries.find(([key])=>/\/READ_PINS\//.test(key))?.[1];let expiry;
  try{expiry=Number(BigInt(new TextDecoder().decode(pin.body))/1000000n);}catch{return this.clear();}
  if(expiry<this.clock()+60000||JSON.stringify(copy).length>24576)return this.clear();
  this.value={view:copy,expires:Math.min(expiry-60000,this.clock()+120000)};
 }
 prepare(){if(!this.value||this.clock()>=this.value.expires){this.clear();return null;}return structuredClone(this.value.view);}
 dependencies(view,walKey,encode){return view.entries.filter(([key])=>key!==walKey).map(([key,value])=>({key,value:value?encode(value):null}));}
}
