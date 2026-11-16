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

// Cleanup-only entry point. No native modules or actor bindings are used.
// The lifecycle driver validates the exact disposable bucket/account scope.
export default {
 async fetch(request,env){
  if(request.method!=="POST"||new URL(request.url).pathname!=="/_qualification/drain"||
     !env.API_TOKEN||request.headers.get("Authorization")!=="Bearer "+env.API_TOKEN||
     env.COORDINATOR_BACKEND!=="r2"||!env.ANTFLY_INCARNATION?.startsWith("qualification-"))
   return new Response("unauthorized",{status:401});
  const started=Date.now();let deleted=0,bytes=0;
  // R2 can return fewer objects than requested. Bound both total work and
  // continuation count while amortizing the client round trip across small pages.
  for(let step=0;step<8&&deleted<1000;step++){
   const page=await env.DATABASE.list({limit:1000-deleted});
   if(!page.objects.length){if(page.truncated)throw Error("EmptyTruncatedCleanupPage");break;}
   await env.DATABASE.delete(page.objects.map(item=>item.key));
   deleted+=page.objects.length;bytes+=page.objects.reduce((sum,object)=>sum+object.size,0);
   if(Date.now()-started>=2000)break;
  }
  // An empty invocation is the final proof; do not infer completion from a
  // partial listing, or before an awaited delete has completed.
  return Response.json({done:deleted===0,deleted,bytes});
 }
};
