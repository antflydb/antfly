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

import http from'node:http';import https from'node:https';
// Every process owns its pool. Headers are returned before the bounded body,
// preserving acknowledged status/ray/version when a later body read fails.
export function nodeHttpTransport({connections=64,maxResponseBytes=256*1024,allowLoopbackHttp=false}={}){
 if(!Number.isInteger(connections)||connections<16||connections>128||!Number.isInteger(maxResponseBytes)||maxResponseBytes<1024||maxResponseBytes>1024*1024)throw Error('NodeTransportConfigurationInvalid');
 const options={keepAlive:true,maxSockets:connections,maxTotalSockets:connections,maxFreeSockets:connections},agents={http:new http.Agent(options),https:new https.Agent(options)},metrics={requests:0,completed:0,active:0,peak_active:0,peak_queued:0,failures:0};let closed=false;
 return{async fetch(input,init={}){
  if(closed)throw Error('TrialTransportClosed');const url=new URL(input);if(url.protocol!=='https:'&&!(allowLoopbackHttp&&url.protocol==='http:'&&['127.0.0.1','[::1]'].includes(url.hostname)))throw Error('TrialTransportOriginInvalid');if(init.redirect!=='manual')throw Error('TrialTransportRedirectPolicyInvalid');
  metrics.requests++;metrics.peak_active=Math.max(metrics.peak_active,++metrics.active);let ended=false;const finish=failed=>{if(ended)return;ended=true;metrics.active--;metrics.completed++;if(failed)metrics.failures++;};
  return new Promise((resolve,reject)=>{
   const protocol=url.protocol==='https:'?'https':'http',agent=agents[protocol],request=(protocol==='https'?https:http).request(url,{method:init.method,headers:init.headers,agent,signal:init.signal},response=>{
    let size=0,stream,bodyClosed=false;const noBody=[204,205,304].includes(response.statusCode);
    if(!noBody)stream=new ReadableStream({start(controller){const fail=message=>{if(bodyClosed)return;bodyClosed=true;controller.error(Error(message));finish(true);};response.on('data',chunk=>{if(bodyClosed)return;size+=chunk.length;if(size>maxResponseBytes){fail('TrialTransportResponseExceeded');request.destroy();return;}controller.enqueue(new Uint8Array(chunk));});response.on('end',()=>{if(bodyClosed)return;bodyClosed=true;controller.close();finish(false);});response.on('error',()=>fail('TrialTransportBodyFailed'));},cancel(){bodyClosed=true;request.destroy();finish(true);}});
    else{response.resume();response.on('end',()=>finish(false));response.on('error',()=>finish(true));}
    try{resolve(new Response(stream??null,{status:response.statusCode,headers:Object.entries(response.headers).filter(([,v])=>v!=null).map(([k,v])=>[k,Array.isArray(v)?v.join(', '):v])}));}catch{request.destroy();finish(true);reject(Error('TrialTransportResponseInvalid'));}
   });request.on('error',error=>{finish(true);reject(Error(/^TrialTransport/.test(error.message)?error.message:'TrialTransport'+(/^[A-Z0-9_]{1,48}$/.test(error.code??'')?error.code:'Failed')));});request.end(init.body);metrics.peak_queued=Math.max(metrics.peak_queued,Object.values(agent.requests).reduce((n,list)=>n+list.length,0));
  });
 },report(){return{kind:'independent-node-http',max_connections:connections,...metrics};},close(){closed=true;agents.http.destroy();agents.https.destroy();}};
}
