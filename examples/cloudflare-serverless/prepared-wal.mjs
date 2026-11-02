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

// Data pages are uploaded before entering the authoritative transaction. The
// captured WAL HEAD ETag also fences GC epoch rotation; a failed CAS leaves
// unreferenced immutable pages, never a partially committed receipt group.
export class PreparedWal {
 constructor(walKey,entries){this.key=walKey;this.records=new Map(entries);this.original=this.records.get(walKey);this.head=null;}
 objectCall(request,body=new Uint8Array()){
  if(request.op==="put"){
   if(request.key!==this.key||request.guard_key!=null||request.guard_etag!=null||request.collection!=null)return {status:3};
   if(body.byteLength>4096)return {status:4};
   const previous=this.records.get(this.key);
   if((request.if_none_match&&previous)||(request.if_match_etag!=null&&request.if_match_etag!==previous?.etag))return {status:2};
   if(!request.if_none_match&&request.if_match_etag==null)return {status:3};
   const record={etag:crypto.randomUUID(),body:body.slice(),contentType:request.content_type??null};
   this.records.set(this.key,record);this.head=record;return {status:0,json:{etag:record.etag}};
  }
  if(request.op!=="get"&&request.op!=="head"||!this.records.has(request.key))return {status:3};
  const record=this.records.get(request.key);if(!record)return {status:1};
  if(request.if_match_etag!=null&&request.if_match_etag!==record.etag)return {status:2};
  if(request.max_response_bytes!=null&&record.body.byteLength>request.max_response_bytes)return {status:4};
  return {status:0,json:{bucket:request.bucket,key:request.key,etag:record.etag,content_length:record.body.byteLength,content_type:record.contentType},body:request.op==="get"?record.body.slice():undefined};
 }
 async commit(tx,objectCall){
  if(!this.head)return true;
  const result=await objectCall(tx,{op:"put",bucket:"database",key:this.key,if_none_match:!this.original,if_match_etag:this.original?.etag,content_type:this.head.contentType},this.head.body);
  if(result.status===2)return false;if(result.status!==0)throw new Error("PreparedWalCommitRejected:"+result.status);return true;
 }
}
