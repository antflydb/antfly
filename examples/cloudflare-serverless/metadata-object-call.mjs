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

export async function metadataObjectCall(tx,request,body,validCollection=async()=>{throw Error("CollectionUnsupported");}) {
      if(!(/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(request.key??"")||/\/progress\//.test(request.key??"")))return{status:3};
      if(body.byteLength>4096)return{status:4};
      if (request.collection != null) {try {await validCollection(tx, request.collection);} catch {return {status: 5};}}
      const previous = await tx.get(request.key);
      if (request.op === "put") {
        if (request.guard_key != null || request.guard_etag != null) {
          const target = /^(.*)\/(?:REINDEX|PUBLICATION_PLAN|PUBLICATION_RESULTS\/[a-f0-9]{32})$/.exec(request.key);
          if (!target || request.guard_key !== `${target[1]}/HEAD` || typeof request.guard_etag !== "string") return {status: 3};
          if ((await tx.get(request.guard_key))?.etag !== request.guard_etag) return {status: 2};
        }
        if ((request.if_none_match && previous) || (request.if_match_etag != null && previous?.etag !== request.if_match_etag)) return {status: 2};
        const etag = crypto.randomUUID(); tx.put(request.key, {etag, body, contentType: request.content_type ?? null});
        return {status: 0, json: {etag}};
      }
      if (request.op === "get" || request.op === "head") {
        if (!previous) return {status: 1};
        if (request.if_match_etag != null && request.if_match_etag !== previous.etag) return {status: 2};
        return {status: 0, json: {bucket: request.bucket, key: request.key, etag: previous.etag, content_length: previous.body.byteLength, content_type: previous.contentType}, body: request.op === "get" ? previous.body : undefined};
      }
      return {status: 3};
  }
