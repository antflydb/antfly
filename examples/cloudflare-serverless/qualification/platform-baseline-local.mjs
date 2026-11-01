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

import{build}from'esbuild';import{Miniflare,convertV4MiniflareOptions,Log,LogLevel}from'miniflare';import{resolve}from'node:path';
export async function localPlatformBaseline({payload=1024,pages=0,retained=0,logs=[],delayMs=0}={}){
 const workers=[];for(const role of['app','private']){const bundle=await build({entryPoints:['qualification/platform-baseline-'+(role==='app'?'worker':'private')+'.mjs'],bundle:true,write:false,format:'esm',platform:'neutral',external:['cloudflare:workers']});workers.push({name:role,modules:[{type:'ESModule',path:resolve('platform-'+role+'.mjs'),contents:bundle.outputFiles[0].text}],compatibilityDate:'2026-10-23',bindings:{API_TOKEN:'local-only',WORKER_VERSION:{id:role+'-local-version'},BASELINE_PAYLOAD_BYTES:String(payload),BASELINE_LINEAR_PAGES:String(pages),BASELINE_RETAINED_BYTES:String(retained)},r2Buckets:{DATA:'baseline'},durableObjects:role==='app'?{OWNERS:{className:'PlatformBaselineOwner',scriptName:'private',useSQLite:true}}:{OWNERS:{className:'PlatformBaselineOwner',useSQLite:true}},...(role==='app'?{serviceBindings:{SERVICE:'private'}}:{}),unsafeEvalBinding:undefined});}
 class CaptureLog extends Log{constructor(){super(LogLevel.NONE);}log(message){logs.push(String(message));}error(message){logs.push(String(message));}}
 const mf=new Miniflare(convertV4MiniflareOptions({workers,log:new CaptureLog()}));
 return{mf,origin:'https://baseline.invalid',token:'local-only',fetch:async(url,init)=>{if(delayMs)await new Promise(r=>setTimeout(r,delayMs));return mf.dispatchFetch(url,init);},close:()=>mf.dispose()};
}
