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
export function instrumentTransport(transport,counters={}){
 return async(request,body)=>{
  if(request.op==="bucket_exists")return transport(request,body);
  counters.calls=(counters.calls??0)+1;const started=performance.now();
  counters.bytes_written=(counters.bytes_written??0)+(body?.byteLength??0);
  try{const result=await transport(request,body);counters.bytes_read=(counters.bytes_read??0)+(result.body?.byteLength??0);return result;}finally{counters.wait_ms=(counters.wait_ms??0)+performance.now()-started;}
 };
}

export function unsupportedRpc(error,method){return error?.status==null&&String(error).includes(method)&&/does not implement|not a function|not defined|not found|does not exist|not available/.test(String(error));}
