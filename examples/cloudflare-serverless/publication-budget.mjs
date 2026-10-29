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

// These are guard bands inside the unchanged host limits, not larger limits.
const operations=1024*.75,bytes=32*1024*1024*.75;
const nativeDeadlineGuardMs=30000*.75;
export function nextPublicationBudget(current,maximum,sample,{targetComputeMs=2000,workingSetBytes=20*1024*1024}={}){
 if(!Number.isSafeInteger(maximum)||maximum<1||maximum>256||!Number.isSafeInteger(current)||current<1||current>256)throw Error('InvalidPublicationBudget');
 if(!Number.isSafeInteger(targetComputeMs)||targetComputeMs<250||targetComputeMs>10000)throw Error('InvalidPublicationSlice');
 if(!Number.isSafeInteger(workingSetBytes)||workingSetBytes<1||workingSetBytes>32*1024*1024)throw Error('InvalidHostedWorkingSet');
 current=Math.min(current,maximum);
 if(!sample||!Number.isSafeInteger(sample.records)||sample.records<1||sample.records>256||!Number.isSafeInteger(sample.operations)||sample.operations<1||!Number.isSafeInteger(sample.transferred_bytes)||sample.transferred_bytes<0||!Number.isSafeInteger(sample.working_set_peak_bytes)||sample.working_set_peak_bytes<0||!Number.isFinite(sample.native_elapsed_ms)||sample.native_elapsed_ms<0||!Number.isFinite(sample.native_io_wait_ms)||sample.native_io_wait_ms<0||sample.native_io_wait_ms>sample.native_elapsed_ms)return current;
 const active=Math.max(0,sample.native_elapsed_ms-sample.native_io_wait_ms);
 const load=Math.max(sample.operations/operations,sample.transferred_bytes/bytes,sample.working_set_peak_bytes/(workingSetBytes*.75),active/targetComputeMs);
 // Network wait does not consume compute, but it still consumes the unchanged
 // native deadline. Do not grow back into a prefix that repeatedly times out.
 const wallLoad=sample.native_elapsed_ms/nativeDeadlineGuardMs;
 const boundedLoad=Math.max(load,wallLoad);
 if(boundedLoad>1)return Math.max(1,Math.min(current,Math.floor(sample.records/boundedLoad*.8)));
 // A short WAL tail is not evidence that the configured prefix is too large.
 // Grow only when a complete admitted prefix actually used little capacity.
 if(sample.records>=current&&load<.5&&wallLoad<.8)return Math.min(maximum,current*2);
 return current;
}
export function publicationExecutionFailure(error,metrics){
 if(/Canceled|DeadlineExceeded/.test(String(error))&&metrics?.elapsed_ms>=20000)return new Error('PublicationExecutionBudgetExceeded',{cause:error});
 return error;
}
export function publicationFailureReducesBudget(error){return /ObjectIoBudgetExceeded|OutOfMemory|HostedWorkingSetBudgetExceeded|ResponseTooLarge|PublicationExecutionBudgetExceeded/.test(String(error));}
