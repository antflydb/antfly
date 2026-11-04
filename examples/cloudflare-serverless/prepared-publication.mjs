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

// One build owns the tentative metadata. Immutable uploads stay invisible
// until the coordinator validates this read set and the native work lease.
import{captureReadView}from"./read-view.mjs";
import{publicationKeyAllowed}from"./publication-view.mjs";
export class PreparedPublication {
 constructor(initial,{read,foldWal=false}={}){
  if(![2,3].includes(initial?.format)||!initial.lease)throw Error("InvalidPublicationSession");
  this.format=initial.format;this.foldWal=foldWal===true;this.read=read;this.keys=initial.entries.map(([key])=>key);
  this.initial=new Map(initial.entries);this.records=new Map(initial.entries);this.expected=new Map();this.writes=new Map();this.lease=initial.lease;this.bytes=0;
  for(const[key,record]of initial.entries.slice(0,5))if(!/\/wal-v1\//.test(key))this.expected.set(key,record?.etag??null);
 }
 async objectCall(request,body=new Uint8Array()){
  const key=request.key;
  if(!this.records.has(key)){
   if(this.format!==3||!publicationKeyAllowed(this.keys,key)||typeof this.read!=="function"||this.records.size>=16||!["get","head"].includes(request.op))throw Error("PublicationSnapshotIncomplete");
   const result=await this.read({...request,op:"get",max_response_bytes:4096},body);
   if(![0,1].includes(result.status))return result;
   const record=result.status===1?null:{etag:result.json.etag,body:result.body?.slice()??new Uint8Array(),contentType:result.json.content_type??null};
   this.records.set(key,record);this.initial.set(key,record);
  }
  const previous=this.records.get(key);
  if(!/\/wal-v1\//.test(key))this.expected.set(key,this.initial.get(key)?.etag??null);
  if(request.op==="get"||request.op==="head"){
   if(!previous)return{status:1};
   if(request.if_match_etag!=null&&request.if_match_etag!==previous.etag)return{status:2};
   if(previous.body.byteLength>(request.max_response_bytes??Infinity))return{status:4};
   return{status:0,json:{bucket:request.bucket,key,etag:previous.etag,content_length:previous.body.byteLength,content_type:previous.contentType},body:request.op==="get"?previous.body.slice():undefined};
  }
  const allowedWrite=this.format===3?publicationKeyAllowed(this.keys,key):/(?:\/HEAD|\/(?:READ_PINS|DOC_OFFSETS)\/[0-9]+)$/.test(key);
  const walFold=this.foldWal&&key===this.keys[1]&&previous!=null&&request.if_match_etag===previous.etag&&!request.if_none_match;
  if(request.op!=="put"||!allowedWrite||/\/wal-v1\//.test(key)&&!walFold||request.collection!=null)return{status:3};
  if(walFold)this.expected.set(key,this.initial.get(key)?.etag??null);
  if(request.guard_key!=null||request.guard_etag!=null){
   if(this.format!==3||request.guard_key!==this.keys[0]||typeof request.guard_etag!=="string")return{status:3};
   if(this.records.get(request.guard_key)?.etag!==request.guard_etag)return{status:2};
  }
  if(body.byteLength>4096)return{status:4};
  if(request.if_none_match&&previous||request.if_match_etag!=null&&request.if_match_etag!==previous?.etag)return{status:2};
  const record={etag:crypto.randomUUID(),body:body.slice(),contentType:request.content_type??null};
  const bytes=this.bytes-(this.writes.get(key)?.body.byteLength??0)+body.byteLength;
  if(bytes>32768||this.writes.size>=8&&!this.writes.has(key))throw Error("PublicationBatchExceeded");
  this.bytes=bytes;this.records.set(key,record);this.writes.set(key,record);return{status:0,json:{etag:record.etag}};
 }
 batch(){return{...(this.format===3?{format:3}:{}),...(this.writes.has(this.keys[1])?{wal_fold:true}:{}),lease:this.lease,expected:[...this.expected],writes:[...this.writes]};}
}
export async function applyPublication(tx,keys,batch,{clock=Date.now}={}){
 if(!batch||!Array.isArray(batch.expected)||batch.expected.length>16||!Array.isArray(batch.writes)||batch.writes.length>8)throw Error("InvalidPublicationBatch");
 const allowed=new Set(keys),permits=key=>allowed.has(key)||batch.format===3&&publicationKeyAllowed(keys,key),expected=new Map(batch.expected);
 if(expected.size!==batch.expected.length||keys.slice(0,5).filter(key=>!/\/wal-v1\//.test(key)).some(key=>!expected.has(key)))throw Error("InvalidPublicationReadSet");
 const values=await tx.getMany([...expected.keys()]);
 if(batch.wal_fold!=null&&batch.wal_fold!==true||batch.wal_fold===true&&(!expected.has(keys[1])||!batch.writes.some(([key])=>key===keys[1])))throw Error("InvalidPublicationBatch");
 const isWalFold=key=>batch.wal_fold===true&&key===keys[1];
 for(const[[key,etag],i]of [...expected].map((row,i)=>[row,i]))if(!permits(key)||!isWalFold(key)&&(values[i]?.etag??null)!==etag)throw Error("PublicationConflict");
 let bytes=0;const written=new Set();
 for(const [key,record]of batch.writes){
  if(!permits(key)||!expected.has(key)||written.has(key)||!(batch.format===3?publicationKeyAllowed(keys,key):/(?:\/HEAD|\/(?:READ_PINS|DOC_OFFSETS)\/[0-9]+)$/.test(key))||/\/wal-v1\//.test(key)&&!isWalFold(key)||!(record?.body instanceof Uint8Array)||record.body.byteLength>4096||typeof record.etag!=="string"||(record.etag.length<1||record.etag.length>64)||record.contentType!=null&&(typeof record.contentType!=="string"||record.contentType.length>4096))throw Error("InvalidPublicationBatch");
  if(/\/READ_PINS\//.test(key)){
   const value=new TextDecoder().decode(record.body).trim(),prior=values[[...expected.keys()].indexOf(key)];
   if(!/^(0|[1-9][0-9]{0,19})$/.test(value)||BigInt(value)>18446744073709551615n||prior&&BigInt(value)<BigInt(new TextDecoder().decode(prior.body).trim()))throw Error("InvalidPublicationPin");
  }
  bytes+=record.body.byteLength;if(bytes>32768)throw Error("PublicationBatchExceeded");written.add(key);
 }
 for(const[key,record]of batch.writes){
  // Folding does not change the logical prefix. A concurrent append keeps
  // its current WAL authority; publication can still consume its captured
  // immutable prefix. Abandoned fold uploads remain ordinary GC garbage.
  if(isWalFold(key)&&(values[[...expected.keys()].indexOf(key)]?.etag??null)!==expected.get(key))continue;
  tx.put(key,record);
 }
 // Establish read protection in the SAME authoritative transaction as HEAD.
 // Otherwise the first reader of every new generation must acquire a writable
 // admission and wait behind publication/maintenance despite an immutable read.
 await captureReadView(tx,{head:keys[0],wal:keys[1],floor:keys[2],progress:keys[0].slice(0,-4)},{},clock);
}
