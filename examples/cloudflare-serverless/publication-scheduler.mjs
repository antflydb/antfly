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

import {metadataKeys} from "./read-view.mjs";
const KEY="__antfly_maintenance/publication",text=new TextDecoder();
export class PublicationScheduler{
 constructor(store,namespace,clock=Date.now){this.store=store;this.namespace=namespace;this.clock=clock;this.keys=metadataKeys(namespace);}
 async reserve(tx,watermark,{force=false}={}){const previous=await tx.get(KEY);
  if(!force&&watermark!=null&&BigInt(watermark)<=BigInt(previous?.last_published??"0"))return{enqueue:false};
  if(previous?.id&&previous.expires>this.clock())return{id:previous.id,enqueue:false};
  const state={id:crypto.randomUUID(),expires:this.clock()+120000,last_published:previous?.last_published??"0"};tx.put(KEY,state);return{id:state.id,enqueue:true};}
 async request(options={}){const {wal}=await this.keys;return this.store.transaction(async tx=>this.reserve(tx,this.latest(await tx.get(wal)),options));}
 latest(head){const match=head&&/"latest_lsn"\s*:\s*("?[0-9]+"?)/.exec(text.decode(head.body));if(head&&!match)throw new Error("InvalidWal");return match?match[1].replaceAll('"',''):"0";}
 valid(id){return this.store.transaction(async tx=>{const state=await tx.get(KEY);if(state?.id!==id)return false;if(state.expires-this.clock()<60000){state.expires=this.clock()+120000;tx.put(KEY,state);}return true;});}
 abandon(id){return this.store.transaction(async tx=>{const state=await tx.get(KEY);if(state?.id===id)tx.put(KEY,{id:null,expires:0,last_published:state.last_published??"0"});});}
 async finish(id,published){const {wal}=await this.keys;return this.store.transaction(async tx=>{
  const state=await tx.get(KEY);if(state?.id!==id)return{done:true};
  const latest=BigInt(this.latest(await tx.get(wal)));
  if(latest>BigInt(published??"0"))return{has_more:true};
  tx.put(KEY,{id:null,expires:0,last_published:String(BigInt(published??"0")>BigInt(state.last_published??"0")?BigInt(published??"0"):BigInt(state.last_published??"0"))});return{done:true};
 });}
}
