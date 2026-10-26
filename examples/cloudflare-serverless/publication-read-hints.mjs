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

// Advisory immutable bytes travel with a freshly authenticated read view. They
// never replace mutable authority, generation pins or native integrity checks.
const text=new TextDecoder();
export const readHintBudget=8192;
const version=value=>typeof value==="string"&&/^(0|[1-9][0-9]{0,19})$/.test(value)&&BigInt(value)<=18446744073709551615n;
const manifestPrefix=namespace=>`antfly/manifests/${namespace}/manifests/`;
function kind(namespace,key,body){
 if(typeof key!=="string"||key.length>1024)return 0;
 const prefix=manifestPrefix(namespace);
 if(key.startsWith(prefix)&&/^[0-9]+\.bin$/.test(key.slice(prefix.length)))return 3;
 if(!/\/artifacts\/(?:sha256\/[a-f0-9]{2}\/[a-f0-9]{62}|graph\/[a-f0-9]{64}\/[a-f0-9]{32}\/[a-f0-9]{64})$/.test(key))return 0;
 return [512,576].includes(body.byteLength)&&/^AFDFACT[3-9A]$/.test(text.decode(body.subarray(0,8)))?2:1;
}
export function readHintCharge(key,record){return record.body.byteLength+key.length*2+record.etag.length*2+(record.contentType?.length??0)*2+256;}
export function normalizeReadHints(namespace,generation,input){
 if(!version(generation)||input?.version!==generation||!Array.isArray(input.records)||input.records.length>8)return null;
 let charge=128;const seen=new Set(),records=[];
 for(const row of input.records){
  if(!Array.isArray(row)||row.length!==2)return null;
  const[key,record]=row;
  if(!(record?.body instanceof Uint8Array)||record.body.byteLength>4096||typeof record.etag!=="string"||!record.etag.length||record.etag.length>128||record.contentType!=null&&typeof record.contentType!=="string"||seen.has(key)||!kind(namespace,key,record.body))return null;
  if(key.startsWith(manifestPrefix(namespace))&&key!==manifestPrefix(namespace)+generation+".bin")return null;
  seen.add(key);charge+=readHintCharge(key,record);if(charge>readHintBudget)return null;records.push([key,{body:record.body,etag:record.etag,contentType:record.contentType??null}]);
 }
 return {version:generation,records,charge};
}
export class PublicationReadHints {
 constructor(namespace){this.namespace=namespace;this.entries=new Map();this.bytes=128;this.sequence=0;}
 wrap(transport){return async(request,body)=>{
  const result=await transport(request,body);
  const payload=request.op==="put"&&request.if_none_match===true&&request.if_match_etag==null?body:request.op==="get"&&request.range==null?result.body:null;
  if(!(payload instanceof Uint8Array)||payload.byteLength>4096||typeof result.json?.etag!=="string"||request.if_match_etag!=null&&request.if_match_etag!==result.json.etag)return result;
  const record={body:payload,etag:result.json.etag,contentType:request.content_type??result.json.content_type??null},rank=kind(this.namespace,request.key,payload),charge=rank?readHintCharge(request.key,record):Infinity;
  if(!rank||!record.etag.length||record.etag.length>128||record.contentType!=null&&typeof record.contentType!=="string"||charge+128>readHintBudget)return result;
  // Prefer the newest manifest/facts root, then the latest small COW pages.
  for(const[key,item]of this.entries)if(key===request.key||rank>1&&item.rank===rank){this.entries.delete(key);this.bytes-=item.charge;}
  this.entries.set(request.key,{record:{...record,body:payload.slice()},rank,charge,sequence:this.sequence++});this.bytes+=charge;
  while(this.entries.size>8||this.bytes>readHintBudget){const[key,item]=[...this.entries].sort((a,b)=>a[1].rank-b[1].rank||a[1].sequence-b[1].sequence)[0];this.entries.delete(key);this.bytes-=item.charge;}
  return result;
 };}
 forHead(request,body){return request.op==="put"&&request.key===`antfly/progress/${this.namespace}/HEAD`?this.forVersion(text.decode(body).trim()):undefined;}
 forVersion(generation){
  if(!version(generation))return;
  const records=[...this.entries].filter(([key])=>!key.startsWith(manifestPrefix(this.namespace))||key===manifestPrefix(this.namespace)+generation+".bin").map(([key,{record}])=>[key,{...record,body:record.body.slice()}]);
  return {version:generation,records};
 }
}


// Persist compact bytes in the SAME metadata commit as HEAD. Base64 avoids the
// metadata adapter's general numeric-array encoding and stays below its 32 KiB
// value limit. These bytes remain advisory and independently digest-checked.
export function encodeReadHints(namespace,generation,input){
 const hints=normalizeReadHints(namespace,generation,input);if(!hints)return null;
 const stored={format:1,version:generation,records:hints.records.map(([key,record])=>[key,{etag:record.etag,contentType:record.contentType,body_base64:btoa(String.fromCharCode(...record.body))}])};
 return new TextEncoder().encode(JSON.stringify(stored)).byteLength<=16384?stored:null;
}
export function decodeReadHints(namespace,generation,stored){
 if(stored?.format!==1||stored.version!==generation||!Array.isArray(stored.records)||stored.records.length>8)return null;
 try{
  const records=stored.records.map(row=>{if(!Array.isArray(row)||row.length!==2)throw Error();const[key,value]=row,body=value?.body_base64;if(typeof body!=="string"||body.length>5464||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(body))throw Error();const bytes=Uint8Array.from(atob(body),c=>c.charCodeAt(0));if(btoa(String.fromCharCode(...bytes))!==body)throw Error();return[key,{body:bytes,etag:value.etag,contentType:value.contentType}];});
  return normalizeReadHints(namespace,generation,{version:generation,records});
 }catch{return null;}
}
export function writeReadHints(tx,namespace,generation,input){
 const key=`__antfly_read_hints/${namespace}`,stored=encodeReadHints(namespace,generation,input);
 if(stored)tx.put(key,stored);else tx.delete(key);
}
