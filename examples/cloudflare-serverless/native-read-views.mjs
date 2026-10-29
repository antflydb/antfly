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

import {edgeCacheForEnv} from "./edge-immutable-cache.mjs";
import {AuthorityAdmission} from "./authority-admission.mjs";
import {R2CoordinatorStore} from "./r2-coordinator-store.mjs";
import {openAuthority} from "./coordinator-authority.mjs";
import {metadataKeys,captureReadView} from "./read-view.mjs";
import {MIGRATION} from "./r2-coordinator-migration.mjs";
import {FreshReadAdmissions} from "./fresh-read-admissions.mjs";
import {readAheadBudgetFor} from "../../zig/lib/objectstore/wasm/read-ahead.mjs";

// A reader authenticates committed metadata locally. It owns no mutable session
// and can never create authority, acquire ownership or extend a durable pin.
export class NativeReadViews {
 constructor(module,env,{kernel,cache,clock=Date.now,onMetrics,background,cohortDelayMs=2}={}){
  this.env=env;this.clock=clock;this.cohorts=new FreshReadAdmissions({delayMs:cohortDelayMs});this.authorityAdmission=new AuthorityAdmission(()=>openAuthority(env,"r2",{allowCreate:false}));
  const observation={request:(input,options)=>kernel.request(input,{...options,kernelVariant:"observe",memoryReservationBytes:8*1024*1024,reservationClass:"authority"})};
  this.background=background;
  this.store=new R2CoordinatorStore(module,env.DATABASE,env.COORDINATOR_STORAGE_ID??env.ANTFLY_INCARNATION,{clock,readKernel:observation,cache,onMetrics,proofBudget:env.DETACHED_METADATA_PROOFS==="true"&&kernel.slots?.length?readAheadBudgetFor(kernel,Math.min(4,kernel.slots.length)):undefined,remoteCache:edgeCacheForEnv(env,{background})});
 }
 open(options={}){
  return this.cohorts.run(options.version??"latest",async(traceIds,close,publish)=>{
   const authority=await this.authorityAdmission.get();
   if(authority.descriptor.mode!=="r2"){this.authorityAdmission.value=null;throw Error("CoordinatorMigrationRequired");}
   const keys=await metadataKeys(this.env.ANTFLY_INCARNATION);
   return this.store.readOnly(async tx=>{
    const[migration]=await tx.getMany([MIGRATION,keys.head,keys.wal,keys.floor,keys.hints]);
    if(migration&&migration.phase!=="active"||this.env.COORDINATOR_MIGRATION_REQUIRED==="true"&&!migration||authority.descriptor.mode!=="r2")throw Error("CoordinatorMigrationRequired");
    const view=await captureReadView(tx,keys,options,this.clock);publish(view);return view;
   },{traceId:options.trace_id,traceIds,onAdmission:close,background:options.background??this.background,deadlineMs:options.deadlineMs,...(this.env.FUSED_READ_ADMISSION==="true"?{readKeys:[MIGRATION,keys.head,keys.wal,keys.floor,keys.hints],readPrefix:keys.progress+"READ_PINS/"}:{})});
  },options.trace_id,{untilAdmission:true,onPrepared:options.onPrepared,onAdmitted:options.onAdmitted});
 }
}
