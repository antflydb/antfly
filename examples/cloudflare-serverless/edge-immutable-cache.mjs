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
import {resourceFailureCode} from "./performance.mjs";

const sharedFills={active:0,bytes:0};
const encode=new TextEncoder(),decode=new TextDecoder("utf-8",{fatal:true});
const resourceError=error=>resourceFailureCode(error)!==null;
const base64=bytes=>btoa(Array.from(bytes,b=>String.fromCharCode(b)).join(""));
const digest=async value=>Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",typeof value==="string"?encode.encode(value):value)),b=>b.toString(16).padStart(2,"0")).join("");

// An optional disposable cache of confirmed immutable bytes, below the memory
// cache and above physical R2 instrumentation. No pending promise or I/O handle
// is retained across actors. Mutable floors/heads and misses always reach R2.
export function createEdgeImmutableCache({scope,storage=globalThis.caches,background,onMetrics=()=>{},maxObjectBytes=65536,fills=sharedFills,budgetKey}={}){
 if(typeof scope!=="string"||!scope.length||!Number.isSafeInteger(maxObjectBytes)||maxObjectBytes<1||maxObjectBytes>65536)throw Error("InvalidEdgeImmutableCache");
 return (transport,{onMetrics:observe=onMetrics,budgetKey:requestBudgetKey=budgetKey,background:requestBackground=background}={})=>{
 const localBudget={};
 const budget=()=>requestBudgetKey?.()??localBudget;
 const metric=value=>{try{observe(value);}catch{}};
 const claim=()=>{const value=budget();if((value.edge_cache_calls??0)>=32||(value.calls??0)+(value.edge_cache_calls??0)>=960)return false;value.edge_cache_calls=(value.edge_cache_calls??0)+1;return true;};
 const remote=(request,body)=>{const value=budget();if((value.calls??0)+(value.edge_cache_calls??0)>=960)throw new ObjectStoreError(8,"combined object/cache I/O budget exceeded");return transport(request,body);};
 const cacheFailure=error=>{if(resourceError(error)){metric({edge_cache_resource_errors:1});try{console.error(JSON.stringify({event:"antfly.resource_failure",failure_code:resourceFailureCode(error)}));}catch{}throw error;}metric({edge_cache_errors:1});};
 return async(request,body=new Uint8Array())=>{
  if(!storage?.open||!isImmutableObjectKey(request.key??"",{includeCommitLog:true})||!["get","put"].includes(request.op))return remote(request,body);
  const put=request.op==="put";
  if(put&&(!request.if_none_match||request.if_match_etag!=null||request.guard_key!=null||request.guard_etag!=null||body.byteLength>maxObjectBytes))return remote(request,body);
  if(!claim()){metric({edge_cache_skipped:1});return remote(request,body);}
  const identity=JSON.stringify([scope,request.bucket,request.key,put?null:request.range??null,put?null:request.if_match_etag??null]);
  let cache,key;
  try{cache=await storage.open("antfly-immutable-v1");key=new Request("https://antfly-immutable.invalid/v1/"+await digest(identity));}
  catch(error){cacheFailure(error);return remote(request,body);}
  if(!put){
   const fullIdentity=JSON.stringify([scope,request.bucket,request.key,null,null]),identities=identity===fullIdentity?[identity]:[fullIdentity,identity];let hit;
   for(let index=0;index<identities.length&&!hit;index++){
    if(index&&!claim())break;
    const selected=identities[index];let response;
    try{
     response=await cache.match(selected===identity?key:new Request("https://antfly-immutable.invalid/v1/"+await digest(selected)));
     if(!response)continue;
     const header=response.headers.get("x-antfly-object");
     if(response.status!==200||!header||header.length>8192)throw Error("InvalidEdgeCacheMetadata");
     const record=JSON.parse(decode.decode(Uint8Array.from(atob(header),c=>c.charCodeAt(0))));
     const sizeHeader=response.headers.get("Content-Length"),size=Number(sizeHeader);
     if(record.format!==1||record.identity!==selected||record.json?.key!==request.key||record.json?.bucket!==request.bucket||typeof record.json?.etag!=="string"||sizeHeader==null||!Number.isSafeInteger(size)||size<0||size>maxObjectBytes)throw Error("InvalidEdgeCacheMetadata");
     if(request.if_match_etag!=null&&record.json.etag!==request.if_match_etag){response.body?.cancel().catch(()=>{});continue;}
     // Full uploads can satisfy conditional/range reads after validating their
     // complete bytes. No R2 read amplification or cached negative is required.
     const reader=response.body?.getReader(),chunks=[];let length=0;
     if(!reader)throw Error("InvalidEdgeCacheBody");
     try{for(;;){const chunk=await reader.read();if(chunk.done)break;if(chunk.value.byteLength>maxObjectBytes-length){reader.cancel().catch(()=>{});throw Error("InvalidEdgeCacheBody");}length+=chunk.value.byteLength;chunks.push(chunk.value);}}
     finally{reader.releaseLock();}
     if(length!==size)throw Error("InvalidEdgeCacheBody");
     let bytes=new Uint8Array(length);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
     if(typeof record.body_sha256!=="string"||record.body_sha256!==await digest(bytes))throw Error("InvalidEdgeCacheBody");
     if(selected===fullIdentity){
      if(record.json.content_length!==bytes.byteLength)throw Error("InvalidEdgeCacheBody");
      if(request.range!=null){const {offset,length}=request.range;if(!Number.isSafeInteger(offset)||offset<0||offset>=bytes.byteLength||length!=null&&(!Number.isSafeInteger(length)||length<=0))continue;bytes=bytes.subarray(offset,length==null?undefined:offset+length);}
     }
     hit={json:record.json,body:bytes,cachedBytes:size};
    }catch(error){if(response?.body&&!response.bodyUsed)response.body.cancel().catch(()=>{});cacheFailure(error);}
   }
   if(hit){if(hit.body.byteLength>(request.max_response_bytes??Infinity))throw new ObjectStoreError(4,"cached response exceeds budget");metric({edge_cache_hits:1,edge_cache_bytes:hit.cachedBytes,edge_cache_served_bytes:hit.body.byteLength});return{json:hit.json,body:hit.body};}
   metric({edge_cache_misses:1});
  }
  const result=await remote(request,body);
  const bytes=put?body:result.body;
  if(!(bytes instanceof Uint8Array)||bytes.byteLength>maxObjectBytes||typeof result.json?.etag!=="string"||!put&&request.if_match_etag!=null&&request.if_match_etag!==result.json.etag)return result;
  if(fills.active>=2||!put&&!claim()){metric({edge_cache_skipped:1});return result;}
  const json=put?{bucket:request.bucket,key:request.key,etag:result.json.etag,content_length:bytes.byteLength,content_type:request.content_type??null,last_modified_unix_ms:null}:result.json;
  const header=base64(encode.encode(JSON.stringify({format:1,identity,json,body_sha256:await digest(bytes)})));
  if(header.length>8192)return result;
  const charge=2*bytes.byteLength+2*header.length+2048;
  if(fills.active>=2||charge>512*1024-fills.bytes){metric({edge_cache_skipped:1});return result;}
  const response=new Response(bytes.slice(),{headers:{"Content-Type":"application/octet-stream","Content-Length":String(bytes.byteLength),"Cache-Control":"public, max-age=3600","x-antfly-object":header}});
  fills.active++;fills.bytes+=charge;
  const save=Promise.resolve().then(()=>cache.put(key,response)).then(()=>metric({edge_cache_writes:1}),cacheFailure).finally(()=>{fills.active--;fills.bytes-=charge;});
  // Foreground writes are acknowledged by R2, independently of cache success.
  // Observe failures even when a host does not provide a background lifetime.
  if(requestBackground){requestBackground(save);save.catch(()=>{});}else await save;
  return result;
 };
 };
}

export function edgeCacheForEnv(env,options={}){
 if(env.EDGE_IMMUTABLE_CACHE!=="true")return transport=>transport;
 const incarnation=env.EDGE_IMMUTABLE_CACHE_SCOPE;
 if(typeof incarnation!=="string"||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(incarnation))throw Error("EdgeImmutableCacheScopeRequired");
 return createEdgeImmutableCache({scope:JSON.stringify([incarnation.toLowerCase(),env.COORDINATOR_STORAGE_ID??env.ANTFLY_INCARNATION]),...options});
}
