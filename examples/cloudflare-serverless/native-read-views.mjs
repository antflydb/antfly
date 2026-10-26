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

import {R2CoordinatorStore} from "./r2-coordinator-store.mjs";
import {openAuthority} from "./coordinator-authority.mjs";
import {metadataKeys,captureReadView} from "./read-view.mjs";
import {MIGRATION} from "./r2-coordinator-migration.mjs";
import {FreshReadAdmissions} from "./fresh-read-admissions.mjs";

// A reader authenticates committed metadata locally. It owns no mutable session
// and can never create authority, acquire ownership or extend a durable pin.
export class NativeReadViews {
 constructor(module,env,{kernel,cache,clock=Date.now,onMetrics}={}){
  this.env=env;this.clock=clock;this.cohorts=new FreshReadAdmissions();
  const observation={request:(input,options)=>kernel.request(input,{...options,kernelVariant:"observe",memoryReservationBytes:8*1024*1024})};
  this.store=new R2CoordinatorStore(module,env.DATABASE,env.COORDINATOR_STORAGE_ID??env.ANTFLY_INCARNATION,{clock,readKernel:observation,cache,onMetrics});
 }
 open(options={}){
  return this.cohorts.run(options.version??"latest",async traceIds=>{
   this.authority??=openAuthority(this.env,"r2",{allowCreate:false}).catch(error=>{this.authority=null;throw error;});
   const authority=await this.authority;
   if(authority.descriptor.mode!=="r2"){this.authority=null;throw Error("CoordinatorMigrationRequired");}
   const keys=await metadataKeys(this.env.ANTFLY_INCARNATION);
   return this.store.readOnly(async tx=>{
    const[migration]=await tx.getMany([MIGRATION,keys.head,keys.wal,keys.floor]);
    if(migration&&migration.phase!=="active"||this.env.COORDINATOR_MIGRATION_REQUIRED==="true"&&!migration||authority.descriptor.mode!=="r2")throw Error("CoordinatorMigrationRequired");
    return captureReadView(tx,keys,options,this.clock);
   },{traceId:options.trace_id,traceIds});
  },options.trace_id);
 }
}
