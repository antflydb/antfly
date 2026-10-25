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

// Physical transport counters sit below immutable caches; logical WASM I/O
// remains separate. Observers cannot affect a committed operation.
const transportActivity=new WeakMap();
export function instrumentTransport(transport,counters={}){
 let activity=transportActivity.get(counters);if(!activity){activity={active:0,waveStarted:0};transportActivity.set(counters,activity);}
 return async(request,body)=>{
  if(request.op==="bucket_exists")return transport(request,body);
  counters.calls=(counters.calls??0)+1;
  counters.by_op??={};counters.by_op[request.op]=(counters.by_op[request.op]??0)+1;
  const started=performance.now();
  if(activity.active++===0){activity.waveStarted=started;counters.io_waves=(counters.io_waves??0)+1;}
  counters.concurrency_peak=Math.max(counters.concurrency_peak??0,activity.active);
  counters.bytes_written=(counters.bytes_written??0)+(body?.byteLength??0);
  try{const result=await transport(request,body);counters.bytes_read=(counters.bytes_read??0)+(result.body?.byteLength??0);return result;}finally{const ended=performance.now();counters.wait_ms=(counters.wait_ms??0)+ended-started;if(--activity.active===0)counters.wait_union_ms=(counters.wait_union_ms??0)+ended-activity.waveStarted;}
 };
}

export function unsupportedRpc(error,method){return error?.status==null&&String(error).includes(method)&&/does not implement|not a function|not defined|not found|does not exist|not available/.test(String(error));}

// Labels contain only protocol record types, never tenant names or document IDs.
export function countMetadataRpc(counters,request){
 const match=/\/(HEAD_FENCE|MANIFEST_GC_FLOOR|GC_WATERMARK|REINDEX|HEAD|READ_PINS\/[0-9]+|DOC_OFFSETS\/[0-9]+)$/.exec(request?.key??"");
 const kind=/\/wal-v1\//.test(request?.key??"")?"WAL_HEAD":match?.[1].split("/")[0]??"OTHER";
 counters.metadata_by_op??={};const label=`${request?.op??"unknown"}:${kind}`;counters.metadata_by_op[label]=(counters.metadata_by_op[label]??0)+1;
}
