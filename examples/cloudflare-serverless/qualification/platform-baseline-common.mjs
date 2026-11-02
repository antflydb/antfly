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

export const baselineOps=['echo','service_echo','owner_echo','owner_get','owner_put','owner_r2_get','owner_r2_put','r2_get','r2_put'];
export function baselineSettings(env){
 const payload=Number(env.BASELINE_PAYLOAD_BYTES??1024),pages=Number(env.BASELINE_LINEAR_PAGES??0),retained=Number(env.BASELINE_RETAINED_BYTES??0);
 if(![1024,65536,262144].includes(payload)||![0,256,512].includes(pages)||![0,1024*1024,8*1024*1024].includes(retained))throw Error('BaselineSettingsInvalid');return{payload_bytes:payload,linear_pages:pages,retained_bytes:retained};
}
export function baselineInput(value){if(!value||!baselineOps.includes(value.op)||!/^[a-f0-9-]{36}$/.test(value.id??'')||typeof value.padding!=='string'||value.padding.length!==1024||value.fresh!=null&&typeof value.fresh!=='boolean')throw Error('BaselineInputInvalid');return value;}
export function timedStage(stages,name){const start=performance.now();return()=>stages.push({name,start_ms:start,duration_ms:Math.max(0,performance.now()-start)});}
export function baselineResult(input,env,extra={}){return{schema:1,qualification:false,id:input.id,op:input.op,payload:'x'.repeat(1024),version:env.WORKER_VERSION?.id??null,...extra};}

export async function baselineJson(request){const reader=request.body?.getReader();if(!reader)throw Error('BaselineInputInvalid');const chunks=[];let bytes=0;try{for(;;){const part=await reader.read();if(part.done)break;bytes+=part.value.byteLength;if(bytes>2048){await reader.cancel();throw Error('BaselineInputExceeded');}chunks.push(part.value);}}finally{reader.releaseLock();}const result=new Uint8Array(bytes);let at=0;for(const part of chunks){result.set(part,at);at+=part.byteLength;}return JSON.parse(new TextDecoder().decode(result));}
