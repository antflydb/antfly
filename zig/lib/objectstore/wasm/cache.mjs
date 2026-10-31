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

import { ObjectStoreError } from "./r2.mjs";
// Cache only immutable object names, never catalog/progress/WAL HEAD records.
// Values contain copied bytes and plain metadata, not request-owned I/O handles.
export const isImmutableObjectKey = (key,{includeCommitLog=false}={}) => /\/manifests\/.+\/manifests\/[0-9]+\.bin$/.test(key) || (includeCommitLog && /\/coord-v1\/[a-f0-9]{64}\/epochs\/[a-f0-9]{32}\/(?:[0-9]{20}\.json|START|SEALED)$/.test(key)) || /\/wal-v1\/[a-f0-9]{64}\/pages\/(?:[a-f0-9]{32}\/)?[a-f0-9]{64}$/.test(key) ||
    /\/artifacts\/(?:sha256\/[a-f0-9]{2}\/[a-f0-9]{62}|graph\/[a-f0-9]{64}\/[a-f0-9]{32}\/[a-f0-9]{64})$/.test(key);
export function createImmutableReadCache({ maxBytes = 8 * 1024 * 1024, maxEntries = 128, includeCommitLog = false, writeThrough = false } = {}) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0 || !Number.isSafeInteger(maxEntries) || maxEntries < 0) throw new TypeError("invalid cache budget");
  const entries=new Map(),probation=new Map(),protectedEntries=new Map();let retained=0,protectedBytes=0;
  // Commit log names are immutable within the native checkpoint floor.
  // Only the coordinator opts in; it reads that mutable floor uncached before
  // accepting cached cursors or acknowledging a write to a retired epoch.
  const eligible = key => isImmutableObjectKey(key,{includeCommitLog});
  function evict(key){const item=entries.get(key);if(!item)return;retained-=item.charge;if(item.protected)protectedBytes-=item.charge;entries.delete(key);probation.delete(key);protectedEntries.delete(key);}
  const cacheKey=(scope,request)=>JSON.stringify([scope, request.bucket, request.key, request.range ?? null, request.if_match_etag ?? null]);
  function remember(key,bytes,json,preserveProtected=false){
    const charge=bytes.byteLength+key.length*2+JSON.stringify(json).length*2+256;
    if(charge>maxBytes||maxEntries<1)return false;
    if(preserveProtected){
      const previous=entries.get(key),chargeWithoutPrevious=protectedBytes-(previous?.protected?previous.charge:0),countWithoutPrevious=protectedEntries.size-(previous?.protected?1:0);
      if(charge+chargeWithoutPrevious>maxBytes||countWithoutPrevious>=maxEntries)return false;
    }
    evict(key);
    while(entries.size>=maxEntries||charge>maxBytes-retained){
      // Reused foreground blocks outlive probationary data. A maintenance
      // wrapper may consult this cache without changing its admission order.
      const victim=probation.keys().next().value??protectedEntries.keys().next().value;evict(victim);
    }
    const item={body:bytes.slice(),json:{...json},charge,protected:false};entries.set(key,item);probation.set(key,item);retained+=charge;return true;
  }
  return {
    clear(){entries.clear();probation.clear();protectedEntries.clear();retained=0;protectedBytes=0;},
    retainedBytes: () => retained,
    // Completed full-object bytes only. This grants no authority and retains no
    // transport/context. Admission uses the existing entry/byte limits; callers
    // must still check leases/floors before using a cached immutable object.
    admit(scope,request,result,{populate=true,preserveProtected=false}={}){
      if(!populate||maxBytes===0||maxEntries===0||request.op!=="get"||request.range!=null||!eligible(request.key??"")||!(result.body instanceof Uint8Array))return false;
      if(result.json?.content_length!=null&&result.json.content_length!==result.body.byteLength)return false;
      const key=cacheKey(scope,request);
      if(entries.has(key))return false;
      return remember(key,result.body,JSON.parse(JSON.stringify(result.json??{})),preserveProtected);
    },
    wrap(transport, scope, bindingOrOptions, options=bindingOrOptions??{}) {
      const {populate=true,protect=false,preserveProtected=false}=options;
      return async (request, body) => {
        if (!eligible(request.key ?? "")) return transport(request, body);
        if(request.op==="put"&&writeThrough&&request.if_none_match===true&&request.if_match_etag==null){
          const result=await transport(request,body);
          // Internal immutable-page readers do not depend on modification time.
          // Admit only a confirmed durable immutable PUT, never an ambiguous ACK.
          if(typeof result.json?.etag==="string"&&body instanceof Uint8Array){
            const json={bucket:request.bucket,key:request.key,etag:result.json.etag,content_length:body.byteLength,content_type:request.content_type??null,last_modified_unix_ms:null};
            if(populate)remember(cacheKey(scope,{...request,range:null,if_match_etag:null}),body,json,preserveProtected);
          }
          return result;
        }
        if(request.op!=="get")return transport(request,body);
        const key = cacheKey(scope,request);
        let item = entries.get(key);
        if (item) {
          if (request.max_response_bytes != null && item.body.byteLength > request.max_response_bytes) throw new ObjectStoreError(4, "cached response exceeds budget");
          if(populate){
            entries.delete(key);entries.set(key,item);
            if(protect){
              if(item.protected)protectedEntries.delete(key);else{probation.delete(key);item.protected=true;protectedBytes+=item.charge;}
              protectedEntries.set(key,item);
              while(protectedBytes>maxBytes*.75||protectedEntries.size>maxEntries*.75){
                const [oldest,value]=protectedEntries.entries().next().value;protectedEntries.delete(oldest);protectedBytes-=value.charge;value.protected=false;probation.set(oldest,value);
              }
            }else if(item.protected){protectedEntries.delete(key);protectedEntries.set(key,item);}else{probation.delete(key);probation.set(key,item);}
          }
          return { json: { ...item.json }, body: item.body.slice() };
        }
        const result = await transport(request, body);
        const bytes = result.body;
        if (!bytes) return result;
        if(populate)remember(key,bytes,result.json,preserveProtected);
        return result;
      };
    },
  };
}
