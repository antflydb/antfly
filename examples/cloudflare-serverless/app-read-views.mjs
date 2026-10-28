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

// Keep only bounded observer state and binding handles across HTTP requests.
// Completed views, request contexts and request-owned I/O handles are not kept.
export class AppReadViews {
 constructor(create,{maxStores=8}={}){if(typeof create!=="function"||!Number.isInteger(maxStores)||maxStores<1||maxStores>8)throw Error("InvalidAppReadViewLimit");this.create=create;this.maxStores=maxStores;this.entries=[];this.attempts=0;this.evictions=0;this.fallbacks=0;}
 get(env){
  if(env.LOCAL_METADATA_READS!=="true"||env.READ_ONLY_SNAPSHOTS==="false"||env.__ANTFLY_PHYSICAL_EPOCH>0)return;
  const config=Object.fromEntries(["DATABASE","ANTFLY_INCARNATION","COORDINATOR_STORAGE_ID","COORDINATOR_MIGRATION_REQUIRED","LEGACY_COMMITS","EDGE_IMMUTABLE_CACHE","EDGE_IMMUTABLE_CACHE_SCOPE","LOG_METRICS","READ_VIEW_BATCH_MS","INLINE_METADATA_ROOTS","FUSED_READ_ADMISSION"].map(key=>[key,env[key]]));
  const key=JSON.stringify(Object.fromEntries(Object.entries(config).filter(([key])=>!["DATABASE","LEGACY_COMMITS"].includes(key))));
  let index=this.entries.findIndex(e=>e.key===key&&e.bucket===config.DATABASE&&e.legacy===config.LEGACY_COMMITS),entry;
  if(index>=0){[entry]=this.entries.splice(index,1);}else{
   if(this.entries.length>=this.maxStores){const idle=this.entries.findIndex(e=>e.users===0);if(idle<0){this.fallbacks++;return;}this.entries.splice(idle,1);this.evictions++;}
   const reader=this.create(config);entry={key,bucket:config.DATABASE,legacy:config.LEGACY_COMMITS,users:0};
   entry.adapter={open:async options=>{entry.users++;this.attempts++;try{return await reader.open(options);}finally{entry.users--;}}};
  }
  this.entries.push(entry);return entry.adapter;
 }
 metrics(){return{app_read_view_stores:this.entries.length,app_read_view_active:this.entries.reduce((n,e)=>n+e.users,0),app_read_view_attempts:this.attempts,app_read_view_evictions:this.evictions,app_read_view_capacity_fallbacks:this.fallbacks};}
}
