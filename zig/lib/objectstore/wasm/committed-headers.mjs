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

// Owned immutable serving headers, installed with HEAD. They grant no authority;
// native manifest/domain/checksum validation and the caller's view still apply.
import{ObjectStoreError}from'./r2.mjs';
const text=new TextDecoder(),encoder=new TextEncoder();
export class CommittedHeaders {
 constructor(namespace,version,{routing=false}={}){this.routing=routing;this.namespace=namespace;this.version=version;this.records=[];}
 offer(request,result){
  if(request.op!=='get'||request.range!=null||request.bucket!=='database'||!(result.body instanceof Uint8Array)||(typeof result.json?.etag!=='string'||!result.json.etag.length||result.json.etag.length>128))return;
  const body=result.body,key=request.key,manifest=`antfly/manifests/${this.namespace}/manifests/${this.version}.bin`;
  if(key!==manifest&&!(/^antfly\/artifacts\/(sha256\/[a-f0-9]{2}\/[a-f0-9]{62}|graph\/[a-f0-9]{64}\/[a-f0-9]{32}\/[a-f0-9]{64})$/.test(key)&&(/^AFDFACT[3-9ABCDE]$/.test(text.decode(body.subarray(0,8)))||this.routing&&body.length<=4096)))return;
  if(this.records.length>=(this.routing?8:2)||body.byteLength>8192||this.records.some(r=>r.key===key))return;
  const row={key,etag:result.json.etag,body:btoa(String.fromCharCode(...body))};
  if(encoder.encode(JSON.stringify([...this.records,row])).byteLength>16384)return;
  this.records.push(row);
 }
 encode(){return this.records.some(r=>r.key===`antfly/manifests/${this.namespace}/manifests/${this.version}.bin`)?{format:this.routing?3:1,namespace:this.namespace,version:this.version,records:this.records}:null;}
 async encodeCompressed(){
  const value=this.encode();if(!value)return null;
  const source=encoder.encode(JSON.stringify(value)),stream=new Blob([source]).stream().pipeThrough(new CompressionStream('gzip')),bytes=new Uint8Array(await new Response(stream).arrayBuffer());
  const compressed={format:2,namespace:this.namespace,version:this.version,encoding:'gzip',payload:btoa(String.fromCharCode(...bytes))};
  return JSON.stringify(compressed).length<source.length?compressed:value;
 }
 static wrap(transport,value,namespace,version,{onStage}={}){
  if(value==null||value.version!==version)return transport;
  if(value.format===2){
   if(value.namespace!==namespace||value.encoding!=='gzip'||typeof value.payload!=='string'||value.payload.length>17000)throw Error('InvalidCommittedHeaders');
   let decoded;
   return async(request,body)=>{
    if(!decoded){
     const started=performance.now();try{
     const source=Uint8Array.from(atob(value.payload),c=>c.charCodeAt(0)),reader=new Blob([source]).stream().pipeThrough(new DecompressionStream('gzip')).getReader();let count=0;const parts=[];
     try{for(;;){const{done,value:part}=await reader.read();if(done)break;count+=part.length;if(count>17000)throw Error('InvalidCommittedHeaders');parts.push(part);}}finally{await reader.cancel();}
     const bytes=new Uint8Array(count);let offset=0;for(const part of parts){bytes.set(part,offset);offset+=part.length;}
     const plain=JSON.parse(text.decode(bytes));if(![1,3].includes(plain.format)||plain.version!==version)throw Error('InvalidCommittedHeaders');decoded=CommittedHeaders.wrap(transport,plain,namespace,version);
     }finally{onStage?.({name:"bootstrap_decode",duration_ms:performance.now()-started});}
    }
    return decoded(request,body);
   };
  }
  if(![1,3].includes(value.format)||value.namespace!==namespace||!Array.isArray(value.records)||value.records.length>(value.format===3?8:2)||encoder.encode(JSON.stringify(value)).byteLength>17000)throw Error('InvalidCommittedHeaders');
  const map=new Map();
  for(const row of value.records){
   if(typeof row.key!=='string'||typeof row.body!=='string'||typeof row.etag!=='string'||map.has(row.key))throw Error('InvalidCommittedHeaders');
   const body=Uint8Array.from(atob(row.body),c=>c.charCodeAt(0)),check=new CommittedHeaders(namespace,version,{routing:value.format===3});check.offer({op:'get',bucket:'database',key:row.key},{json:{etag:row.etag},body});if(check.records.length!==1)throw Error('InvalidCommittedHeaders');map.set(row.key,{body,etag:row.etag});
  }
  return async(request,body)=>{
   const row=request.bucket==='database'&&['get','head'].includes(request.op)&&map.get(request.key);
   if(!row||request.version_id!=null||Object.keys(request).some(k=>!['op','bucket','key','range','if_match_etag','max_response_bytes'].includes(k)))return transport(request,body);
   if(request.if_match_etag!=null&&request.if_match_etag!==row.etag)throw new ObjectStoreError(2,'committed header condition failed');
   let bytes=row.body;
   if(request.range){const{offset,length}=request.range;if(!Number.isSafeInteger(offset)||offset<0||offset>=bytes.length||length!=null&&(!Number.isSafeInteger(length)||length<=0))throw new ObjectStoreError(3,'invalid committed header range');bytes=bytes.subarray(offset,length==null?undefined:offset+length);}
   if(request.op==='get'&&bytes.byteLength>(request.max_response_bytes??Infinity))throw new ObjectStoreError(4,'committed header exceeds budget');
   return{json:{bucket:request.bucket,key:request.key,etag:row.etag,content_length:row.body.length,content_type:null},body:request.op==='get'?bytes.slice():undefined};
  };
 }
}
