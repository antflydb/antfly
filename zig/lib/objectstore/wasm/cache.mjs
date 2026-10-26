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
  const entries = new Map(); let retained = 0;
  // Commit log names are immutable within the native checkpoint floor.
  // Only the coordinator opts in; it reads that mutable floor uncached before
  // accepting cached cursors or acknowledging a write to a retired epoch.
  const eligible = key => isImmutableObjectKey(key,{includeCommitLog});
  function evict(key) { const item = entries.get(key); if (item) retained -= item.charge; entries.delete(key); }
  const cacheKey=(scope,request)=>JSON.stringify([scope, request.bucket, request.key, request.range ?? null, request.if_match_etag ?? null]);
  function remember(key,bytes,json){
    const charge=bytes.byteLength+key.length*2+JSON.stringify(json).length*2+256;
    if(charge>maxBytes||maxEntries<1)return;
    evict(key);
    while(entries.size>=maxEntries||charge>maxBytes-retained)evict(entries.keys().next().value);
    entries.set(key,{body:bytes.slice(),json:{...json},charge});retained+=charge;
  }
  return {
    clear() { entries.clear(); retained = 0; },
    retainedBytes: () => retained,
    wrap(transport, scope) {
      return async (request, body) => {
        if (!eligible(request.key ?? "")) return transport(request, body);
        if(request.op==="put"&&writeThrough&&request.if_none_match===true&&request.if_match_etag==null){
          const result=await transport(request,body);
          // Internal immutable-page readers do not depend on modification time.
          // Admit only a confirmed durable immutable PUT, never an ambiguous ACK.
          if(typeof result.json?.etag==="string"&&body instanceof Uint8Array){
            const json={bucket:request.bucket,key:request.key,etag:result.json.etag,content_length:body.byteLength,content_type:request.content_type??null,last_modified_unix_ms:null};
            remember(cacheKey(scope,{...request,range:null,if_match_etag:null}),body,json);
          }
          return result;
        }
        if(request.op!=="get")return transport(request,body);
        const key = cacheKey(scope,request);
        let item = entries.get(key);
        if (item) {
          if (request.max_response_bytes != null && item.body.byteLength > request.max_response_bytes) throw new ObjectStoreError(4, "cached response exceeds budget");
          entries.delete(key); entries.set(key, item);
          return { json: { ...item.json }, body: item.body.slice() };
        }
        const result = await transport(request, body);
        const bytes = result.body;
        if (!bytes) return result;
        remember(key,bytes,result.json);
        return result;
      };
    },
  };
}
