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

import {isImmutableObjectKey} from "../../zig/lib/objectstore/wasm/cache.mjs";
import {ObjectStoreError} from "../../zig/lib/objectstore/wasm/r2.mjs";

const encode=new TextEncoder(),decode=new TextDecoder("utf-8",{fatal:true});
const resourceError=error=>/OutOfMemory|MemoryLimitExceeded|CPULimitExceeded|memory limit|CPU.*limit/i.test(String(error));
const base64=bytes=>btoa(Array.from(bytes,b=>String.fromCharCode(b)).join(""));
const digest=async value=>Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",typeof value==="string"?encode.encode(value):value)),b=>b.toString(16).padStart(2,"0")).join("");

// An optional disposable cache of confirmed immutable bytes, below the memory
// cache and above physical R2 instrumentation. No pending promise or I/O handle
// is retained across actors. Mutable floors/heads and misses always reach R2.
export function createEdgeImmutableCache({scope,storage=globalThis.caches,background,onMetrics=()=>{},maxObjectBytes=65536}={}){
 if(typeof scope!=="string"||!scope.length||!Number.isSafeInteger(maxObjectBytes)||maxObjectBytes<1||maxObjectBytes>65536)throw Error("InvalidEdgeImmutableCache");
 return (transport,{onMetrics:observe=onMetrics}={})=>{
 const metric=value=>{try{observe(value);}catch{}};
 const cacheFailure=error=>{if(resourceError(error)){metric({edge_cache_resource_errors:1});throw error;}metric({edge_cache_errors:1});};
 return async(request,body=new Uint8Array())=>{
  if(!storage?.open||!isImmutableObjectKey(request.key??"",{includeCommitLog:true})||!["get","put"].includes(request.op))return transport(request,body);
  const put=request.op==="put";
  if(put&&(!request.if_none_match||request.if_match_etag!=null||request.guard_key!=null||request.guard_etag!=null||body.byteLength>maxObjectBytes))return transport(request,body);
  const identity=JSON.stringify([scope,request.bucket,request.key,put?null:request.range??null,put?null:request.if_match_etag??null]);
  let cache,key;
  try{cache=await storage.open("antfly-immutable-v1");key=new Request("https://antfly-immutable.invalid/v1/"+await digest(identity));}
  catch(error){cacheFailure(error);return transport(request,body);}
  if(!put){
   let hit,response;
   try{
    response=await cache.match(key);
    if(response){
     const header=response.headers.get("x-antfly-object");
     if(response.status!==200||!header||header.length>8192)throw Error("InvalidEdgeCacheMetadata");
     const record=JSON.parse(decode.decode(Uint8Array.from(atob(header),c=>c.charCodeAt(0))));
     const sizeHeader=response.headers.get("Content-Length"),size=Number(sizeHeader);
     if(record.format!==1||record.identity!==identity||record.json?.key!==request.key||record.json?.bucket!==request.bucket||typeof record.json?.etag!=="string"||sizeHeader==null||!Number.isSafeInteger(size)||size<0||size>maxObjectBytes)throw Error("InvalidEdgeCacheMetadata");
     // Do not let a malformed cache entry allocate an unbounded response.
     const reader=response.body?.getReader(),chunks=[];let length=0;
     if(!reader)throw Error("InvalidEdgeCacheBody");
     try{for(;;){const chunk=await reader.read();if(chunk.done)break;if(chunk.value.byteLength>maxObjectBytes-length){reader.cancel().catch(()=>{});throw Error("InvalidEdgeCacheBody");}length+=chunk.value.byteLength;chunks.push(chunk.value);}}
     finally{reader.releaseLock();}
     if(length!==size)throw Error("InvalidEdgeCacheBody");
     const bytes=new Uint8Array(length);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
     if(typeof record.body_sha256!=="string"||record.body_sha256!==await digest(bytes))throw Error("InvalidEdgeCacheBody");
     hit={json:record.json,body:bytes};
    }
   }catch(error){if(response?.body&&!response.bodyUsed)response.body.cancel().catch(()=>{});cacheFailure(error);}
   if(hit){if(hit.body.byteLength>(request.max_response_bytes??Infinity))throw new ObjectStoreError(4,"cached response exceeds budget");metric({edge_cache_hits:1,edge_cache_bytes:hit.body.byteLength});return hit;}
   metric({edge_cache_misses:1});
  }
  const result=await transport(request,body);
  const bytes=put?body:result.body;
  if(!(bytes instanceof Uint8Array)||bytes.byteLength>maxObjectBytes||typeof result.json?.etag!=="string")return result;
  const json=put?{bucket:request.bucket,key:request.key,etag:result.json.etag,content_length:bytes.byteLength,content_type:request.content_type??null,last_modified_unix_ms:null}:result.json;
  const header=base64(encode.encode(JSON.stringify({format:1,identity,json,body_sha256:await digest(bytes)})));
  if(header.length>8192)return result;
  const response=new Response(bytes.slice(),{headers:{"Content-Type":"application/octet-stream","Content-Length":String(bytes.byteLength),"Cache-Control":"public, max-age=3600","x-antfly-object":header}});
  const save=Promise.resolve().then(()=>cache.put(key,response)).then(()=>metric({edge_cache_writes:1}),cacheFailure);
  // Foreground writes are acknowledged by R2, independently of cache success.
  // Observe failures even when a host does not provide a background lifetime.
  if(background){background(save);save.catch(error=>{if(resourceError(error))console.error("Antfly edge cache resource failure",String(error));});}else await save;
  return result;
 };
 };
}

export function edgeCacheForEnv(env,options={}){
 return env.EDGE_IMMUTABLE_CACHE==="true"?createEdgeImmutableCache({scope:env.COORDINATOR_STORAGE_ID??env.ANTFLY_INCARNATION,...options}):transport=>transport;
}
