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

import{diagnosticPrefix}from'./diagnostic-scope.mjs';
import{diagnosticAdmission}from'./diagnostic-memory.mjs';
import{diagnosticStorage}from'./diagnostic-storage.mjs';
import{resourceFailureCode}from'../performance.mjs';
import{fetchObserver,persistObserverTail}from'./paired-observer.mjs';
export default{async fetch(request,env){if(!env.API_TOKEN||request.headers.get('Authorization')!=='Bearer '+env.API_TOKEN)return new Response('unauthorized',{status:401});env=diagnosticStorage(env,'tail-observer');try{return await diagnosticAdmission.run(()=>fetchObserver(request,env),{signal:request.signal});}catch(error){await tailRejected(env,error);return Response.json({error:'ObserverAdmissionFailed'},{status:503});}},tail(events,env,ctx){env=diagnosticStorage(env,'tail-observer');ctx.waitUntil(persistObserverTail(events,env).catch(async(error)=>{await tailRejected(env,error);throw Error('ObserverTailFailed');}));}};
async function tailRejected(env,error){await env.LOGS.put(diagnosticPrefix(env,'observer')+'rejections/'+crypto.randomUUID()+'.json',JSON.stringify({at:Date.now(),error:'ObserverTailFailed',cause:/^(?:Counter|Observer|Telemetry)[A-Za-z]+$/.test(error?.message??'')?error.message:resourceFailureCode(error)??'ObserverStorageFailed'}));}
