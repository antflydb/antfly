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

import {unsupportedRpc} from "./performance.mjs";
import {metadataKeys} from "./read-view.mjs";

export async function publicationKeys(namespace){const k=await metadataKeys(namespace);return [k.head,k.wal,k.floor,k.progress+"HEAD_FENCE",k.progress+"REINDEX"];}
export function publicationKeyAllowed(keys,key){return keys.includes(key)||typeof key==="string"&&key.startsWith(keys[0].slice(0,-4))&&/^(READ_PINS|DOC_OFFSETS)\/(0|[1-9][0-9]{0,19})$/.test(key.slice(keys[0].length-4));}
export async function publicationSnapshot(tx,keys){
 const values=await tx.getMany(keys),entries=keys.map((key,i)=>[key,values[i]]);
 const version=values[0]?new TextDecoder().decode(values[0].body).trim():"0";
 if(/^(0|[1-9][0-9]{0,19})$/.test(version)){
  const versions=[version];if(BigInt(version)<18446744073709551615n)versions.push(String(BigInt(version)+1n));
  const extra=versions.flatMap(value=>[keys[0].slice(0,-4)+"READ_PINS/"+value,keys[0].slice(0,-4)+"DOC_OFFSETS/"+value]),rows=await tx.getMany(extra);
  extra.forEach((key,i)=>entries.push([key,rows[i]]));
 }
 return entries;
}
export async function publicationCall(tx,keys,objectCall,request,body,expected=[]){
  if(!Array.isArray(expected)||expected.length>16)throw new Error("InvalidPublicationReadSet");
  for(const [key,etag] of expected){
    if(!publicationKeyAllowed(keys,key))throw new Error("InvalidPublicationReadSet");
    if(((await tx.get(key))?.etag??null)!==etag)return {result:{status:2},entries:[]};
  }
  const result=await objectCall(tx,request,body);
  // Capture these small records at the SAME revision as the conditional write.
  return {result,entries:await publicationSnapshot(tx,keys)};
}
// This operation-local view is optimistic, not a lock around the build. Every
// metadata PUT atomically validates all cached records used since the last PUT.
// Native ETag CAS, lease expiry and generation pins remain authoritative.
export function publicationCoordinator(coordinator,initial){
  let records=new Map(initial?.entries??[]),used=new Map(),legacy=false;
  // WAL roots are immutable prefix snapshots, not publication CAS dependencies.
  // Appending newer records must not invalidate an in-flight prefix build.
  const track=(key,record)=>{if(!/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(key))used.set(key,record?.etag??null);};
  return {async objectCall(request,body){
    if(legacy)return coordinator.objectCall(request,body);
    if((request.op==="get"||request.op==="head")&&records.has(request.key)){
      const record=records.get(request.key);track(request.key,record);
      if(!record)return {status:1};
      if(request.if_match_etag!=null&&request.if_match_etag!==record.etag)return {status:2};
      if(request.max_response_bytes!=null&&record.body.byteLength>request.max_response_bytes)return {status:4};
      return {status:0,json:{bucket:request.bucket,key:request.key,etag:record.etag,content_length:record.body.byteLength,content_type:record.contentType},body:request.op==="get"?record.body.slice():undefined};
    }
    let response;
    try{response=await coordinator.publicationCall(request,body,[...used]);}
    catch(error){if(!unsupportedRpc(error,"publicationCall")||!coordinator.objectCall)throw error;legacy=true;records.clear();used.clear();return coordinator.objectCall(request,body);}
    records=new Map(response.entries);if(request.op==="put")used=new Map();
    // An actual GET was fresh at this revision too. Track it if the returned
    // snapshot contains it, so subsequent writes validate exactly that value.
    if((request.op==="get"||request.op==="head")&&records.has(request.key))track(request.key,records.get(request.key));
    if(response.result.status!==0&&response.result.status!==1){records.clear();used.clear();}
    return response.result;
  }};
}
