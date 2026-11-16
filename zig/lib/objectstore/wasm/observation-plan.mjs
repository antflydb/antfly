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

// One request-owned immutable journal response, outside a native execution slot.
// A continuation is never cached. The native MAC binds source, epoch, owner,
// scope, purpose and the pre-I/O anchor; native resumption validates the frame.
import {ObjectStoreError} from './r2.mjs';
const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true});
export async function readObservationResponse(transport,key,{base,reserve,wait,check=()=>{},onDraining=()=>{}}){
 const prefix=base+'/epochs/';
 if(typeof base!=='string'||!base.length||typeof key!=='string'||!key.startsWith(prefix)||!/^[a-f0-9]{32}\/[0-9]{20}\.json$/.test(key.slice(prefix.length)))throw Error('InvalidImmutableLogObservationResponse');
 check();const request={op:'get',bucket:'database',key,max_response_bytes:16384};
 // Charge the full bounded response before issuing I/O, including unused input.
 reserve(1,encoder.encode(JSON.stringify(request)).byteLength+16384+4096);
 let settled=false;
 const task=Promise.resolve().then(()=>transport(request,new Uint8Array())).then(result=>{
  const body=result.body??new Uint8Array();
  if(body.byteLength>16384||encoder.encode(JSON.stringify(result.json??{})).byteLength>4096)throw Error('ObjectIoBudgetExceeded');
  return{key,body:decoder.decode(body)};
 },error=>{if(error instanceof ObjectStoreError&&error.status===1)return{key,body:null};throw error;}).then(value=>{settled=true;return value;},error=>{settled=true;throw error;});
 try{const value=await wait(task);check();return value;}
 catch(error){
  if(!settled){const ioSettled=task.then(()=>{},()=>{});Object.defineProperty(error,'ioSettled',{value:ioSettled});try{onDraining(ioSettled);}catch{}}
  throw error;
 }
}
