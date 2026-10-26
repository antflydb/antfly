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

import {normalizeReadHints,decodeReadHints} from "./publication-read-hints.mjs";
import {unsupportedRpc} from "./performance.mjs";
import {ObjectStoreError} from "../../zig/lib/objectstore/wasm/r2.mjs";
const utf8=new TextEncoder(),text=new TextDecoder();
export async function metadataKeys(namespace){
 const hash=Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",utf8.encode(namespace))),b=>b.toString(16).padStart(2,"0")).join("");
 const progress=`antfly/progress/${namespace}/`;
 return {namespace,hints:`__antfly_read_hints/${namespace}`,progress,head:progress+"HEAD",wal:`antfly/wal-v1/${hash}/HEAD`,floor:progress+"MANIFEST_GC_FLOOR"};
}
export async function openReadView(store,namespace,{version,trace_id,immutable}={},now=Date.now){
 const keys=await metadataKeys(namespace);
 return store.transaction(tx=>captureReadView(tx,keys,{version,immutable},now),{traceId:trace_id});
}
export async function captureReadView(tx,keys,{version,immutable=true}={},now=Date.now){
  const values=await tx.getMany([keys.head,keys.wal,keys.floor,...(immutable&&keys.hints?[keys.hints]:[])]);
  const head=values[0]?text.decode(values[0].body).trim():"0";
  const selected=version??head;
  if(typeof selected!=="string"||!/^(0|[1-9][0-9]{0,19})$/.test(selected)||BigInt(selected)>18446744073709551615n)throw new Error("InvalidReadViewVersion");
  const floor=values[2]?BigInt(text.decode(values[2].body).trim()):0n;
  const entries=[[keys.head,values[0]],[keys.wal,values[1]],[keys.floor,values[2]]];
  if(BigInt(selected)>0n&&BigInt(selected)>=floor){
   const key=keys.progress+"READ_PINS/"+selected,prior=await tx.get(key),clock=BigInt(now())*1000000n;
   let pin=prior;
   if(!prior||BigInt(text.decode(prior.body).trim())-clock<300000000000n){
    pin={etag:crypto.randomUUID(),body:utf8.encode(String(clock+600000000000n)),contentType:null};tx.put(key,pin);
   }
   entries.push([key,pin]);
  }
  const hints=immutable?decodeReadHints(keys.namespace,selected,values[3]):null;
  return {format:1,version:selected,entries,...(hints?{immutable:hints.records}:{})};

}
// One operation owns this view. Its native deadline bounds use of captured WAL
// roots inside the collector's grace period. Never share it across requests.
export function withReadView(transport,coordinator,namespace,{onMetrics=()=>{},initial,admittedAt=performance.now()}={}){
 const mapView=view=>{if(view.format!==1)throw new Error("InvalidReadView");return {records:new Map(view.entries),immutable:new Map(normalizeReadHints(namespace,view.version,{version:view.version,records:view.immutable})?.records??[])};};
 let pending=initial?Promise.resolve(mapView(initial)):null,hint;const started=admittedAt;
 const progress=`antfly/progress/${namespace}/`,manifest=`antfly/manifests/${namespace}/manifests/`;
 return async(request,body)=>{
  if(request.op==="get"&&request.key?.startsWith(manifest)){
   const name=request.key.slice(manifest.length);if(/^[0-9]+\.bin$/.test(name))hint=name.slice(0,-4);
  }
  if((request.op==="get"||request.op==="head")&&(request.key?.startsWith(progress)||/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(request.key??""))){
   if(performance.now()-started>=30000)throw new ObjectStoreError(8,"read view deadline exceeded");
   pending??=coordinator.openReadView(hint==null?{}:{version:hint}).then(mapView).catch(error=>{if(unsupportedRpc(error,"openReadView"))return null;throw error;});
   const records=(await pending)?.records;
   if(records?.has(request.key)){
    const record=records.get(request.key);onMetrics({snapshot_reads:1});
    if(!record)throw new ObjectStoreError(1,"snapshot object not found");
    if(request.max_response_bytes!=null&&record.body.byteLength>request.max_response_bytes)throw new ObjectStoreError(4,"snapshot response exceeds budget");
    if(request.if_match_etag!=null&&request.if_match_etag!==record.etag)throw new ObjectStoreError(2,"snapshot condition failed");
    return {json:{bucket:request.bucket,key:request.key,etag:record.etag,content_length:record.body.byteLength,content_type:record.contentType},body:request.op==="get"?record.body.slice():undefined};
   }
  }
  if((request.op==="get"||request.op==="head")&&pending&&(request.bucket==null||request.bucket==="database")){
   const record=(await pending)?.immutable.get(request.key);
   if(record){
    if(performance.now()-started>=30000)throw new ObjectStoreError(8,"read view deadline exceeded");
    if(request.if_match_etag!=null&&request.if_match_etag!==record.etag)throw new ObjectStoreError(2,"immutable hint condition failed");
    let bytes=record.body;
    if(request.op==="get"&&request.range!=null){const {offset,length}=request.range;if(!Number.isSafeInteger(offset)||offset<0||length!=null&&(!Number.isSafeInteger(length)||length<=0)||offset>=bytes.byteLength)throw new ObjectStoreError(3,"invalid immutable hint range");bytes=bytes.subarray(offset,length==null?undefined:offset+length);}
    if(request.op==="get"&&bytes.byteLength>(request.max_response_bytes??Infinity))throw new ObjectStoreError(4,"immutable hint response exceeds budget");
    onMetrics({snapshot_immutable_reads:1});return{json:{bucket:request.bucket,key:request.key,etag:record.etag,content_length:record.body.byteLength,content_type:record.contentType},body:request.op==="get"?bytes.slice():undefined};
   }
  }
  return transport(request,body);
 };
}
