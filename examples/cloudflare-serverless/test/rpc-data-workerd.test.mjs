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

import{test}from'node:test';import assert from'node:assert/strict';import{resolve}from'node:path';import{build}from'esbuild';import{Miniflare,convertV4MiniflareOptions}from'miniflare';
test('workerd materializes plain RPC data and disposes unexpected returned capabilities',{timeout:30000},async()=>{
 const source="import{takeRpcData}from'./rpc-data.mjs';export default{async fetch(request,env){if(new URL(request.url).pathname==='/bad'){const pending=env.DATA.bad(),raw=await pending;let failure;try{await takeRpcData(pending);}catch(error){failure=error.message;}let closed=false;try{await raw.capability.ping();}catch{closed=true;}return Response.json({error:failure,closed});}if(new URL(request.url).pathname==='/count')return Response.json({count:await env.DATA.count()});const pending=env.DATA.value(),raw=await pending,disposable=typeof raw[Symbol.dispose]==='function',data=await takeRpcData(pending);return Response.json({disposable,owned:typeof data[Symbol.dispose]!=='function',bytes:[...data.bytes],nested:data.nested});}};",client=await build({stdin:{contents:source,resolveDir:process.cwd()},bundle:true,write:false,format:'esm',platform:'neutral'}),server="import{WorkerEntrypoint,RpcTarget}from'cloudflare:workers';let disposed=0;class Capability extends RpcTarget{ping(){return 1;}[Symbol.dispose](){disposed++;}}export default class Server extends WorkerEntrypoint{value(){return{bytes:new Uint8Array([1,2,3]),nested:{source:'owned'}};}bad(){return{capability:new Capability()};}count(){return disposed;}}",mf=new Miniflare(convertV4MiniflareOptions({workers:[{name:'client',modules:[{type:'ESModule',path:resolve('rpc-data-client.mjs'),contents:client.outputFiles[0].text}],compatibilityDate:'2026-10-23',serviceBindings:{DATA:'server'}},{name:'server',modules:[{type:'ESModule',path:resolve('rpc-data-server.mjs'),contents:server}],compatibilityDate:'2026-10-23'}]}));
 try{assert.deepEqual(await(await mf.dispatchFetch('https://rpc.invalid/')).json(),{disposable:true,owned:true,bytes:[1,2,3],nested:{source:'owned'}});assert.deepEqual(await(await mf.dispatchFetch('https://rpc.invalid/bad')).json(),{error:'RpcDataContainsCapability',closed:true});}finally{await mf.dispose();}
});
