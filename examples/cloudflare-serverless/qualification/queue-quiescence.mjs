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

// Dispatch-only quiescence preserves deployed code and live diagnostic tails.
export async function quiesceQueueMaintenance(run,{verify,pause}){
 if(run.deleted||run.kind!=="candidate"||run.capabilities?.queued_maintenance!==true)throw Error("QueueOnlyMaintenanceRequired");
 const errors=await verify(run);if(errors.length)throw Error("QuiescenceProvenanceFailed");
 const versions=JSON.stringify(run.versions);
 await pause(run);
 if(JSON.stringify(run.versions)!==versions)throw Error("QuiescenceChangedCodeVersion");
 run.paused=true;run.dispatch_paused=true;
 return{queue_delivery_paused:true,code_versions_unchanged:true};
}
