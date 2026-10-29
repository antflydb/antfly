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

import {DurableObject} from 'cloudflare:workers';
const marker=(id,kind,phase)=>console.log(JSON.stringify({event:'antfly.qualification_probe',id,kind,phase}));
export class Probe extends DurableObject {
 run(id){marker(id,'rpc','complete');return{id};}
}
export default {async fetch(request,env,ctx){
 const id=new URL(request.url).searchParams.get('nonce');
 if(request.method!=='POST'||!isUuid(id))return new Response('invalid probe',{status:400});
 const rpc=crypto.randomUUID();marker(id,'fetch','start');
 const result=await env.PROBES.getByName('probe').run(rpc);
 if(result.id!==rpc)throw Error('ProbeIdentityMismatch');
 ctx.waitUntil(new Promise(resolve=>setTimeout(()=>{marker(id,'fetch','late');resolve();},20)));
 return Response.json({id,rpc});
}};
function isUuid(value){return typeof value==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(value);}
