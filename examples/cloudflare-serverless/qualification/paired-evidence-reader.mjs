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

// Read-only evidence service, separate from the frozen Tail/export receiver.
// Parallel gets stay within the existing 32-object / 4 MiB page bounds.
import{observerScopes}from'./paired-observer.mjs';
export default{async fetch(request,env){
 if(!env.API_TOKEN||request.headers.get('Authorization')!=='Bearer '+env.API_TOKEN)return new Response('unauthorized',{status:401});
 try{const scopes=observerScopes(env),path=new URL(request.url).pathname;if(path==='/health'){const page=await env.LOGS.list({prefix:'observer-v1/rejections/',limit:1});return Response.json({qualification:false,rejections_present:page.objects.length>0,scopes:scopes.map(s=>({source:s.source,capture_id:s.capture_id})),collector_version:env.WORKER_VERSION?.id??null});}
 const match=/^\/source\/([^/]+)\/page$/.exec(path),scope=match&&scopes.find(s=>s.source===match[1]);if(request.method!=='POST'||!scope)return new Response('not found',{status:404});let size=0,parts=[];for await(const chunk of request.body??[]){size+=chunk.byteLength;if(size>8192)throw Error('ObserverCursorInvalid');parts.push(chunk);}const bytes=new Uint8Array(size);let offset=0;for(const p of parts){bytes.set(p,offset);offset+=p.byteLength;}const input=JSON.parse(new TextDecoder().decode(bytes)||'{}');if(Object.keys(input).some(k=>k!=='cursor')||input.cursor!=null&&(typeof input.cursor!=='string'||input.cursor.length>4096))throw Error('ObserverCursorInvalid');
 const prefix='probe-v1/'+scope.capture_id+'/';let limit=32,page;for(;;){page=await env.LOGS.list({prefix,limit,...(input.cursor?{cursor:input.cursor}:{})});if(page.objects.reduce((n,o)=>n+o.size,0)<=4*1024*1024)break;if(limit===1)throw Error('ObserverPageBudgetExceeded');limit=Math.max(1,Math.floor(limit/2));}if(page.truncated&&(!page.cursor||page.cursor===input.cursor))throw Error('ObserverCursorInvalid');
 const settled=await Promise.allSettled(page.objects.map(async item=>{const object=await env.LOGS.get(item.key);if(!object||object.size!==item.size)throw Error('ObserverObjectChanged');return{key:item.key,value:await object.json()};}));if(settled.some(r=>r.status==='rejected'))throw Error('ObserverPageReadFailed');return Response.json({qualification:false,capture_id:scope.capture_id,collector_version:env.WORKER_VERSION?.id??null,objects:settled.map(r=>r.value),cursor:page.truncated?page.cursor:null});
 }catch(error){return Response.json({error:/^Observer[A-Za-z]+$/.test(error.message)?error.message:'ObserverPageReadFailed'},{status:503});}
}};
