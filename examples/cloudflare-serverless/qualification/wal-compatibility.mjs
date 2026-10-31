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

// Exercise the actual writer/reader binaries before enabling a new WAL format.
// This probe uses only an in-memory object binding and no account credentials.
import{Bucket}from"../test/object-fixture.mjs";
import{metadataObjectCall}from"../metadata-object-call.mjs";
import{instantiateHost}from"../../../zig/lib/objectstore/wasm/asyncify.mjs";
import{createR2Transport}from"../../../zig/lib/objectstore/wasm/r2.mjs";
import{withCommitCoordinator}from"../../../zig/lib/objectstore/wasm/coordinated.mjs";

export async function verifyWalTailReaders(fullModule,readerModule,{enabled=true}={}){
 if(typeof enabled!=="boolean")throw Error("InvalidWalInlinePolicy");
 const values=new Map(),tx={get:async key=>structuredClone(values.get(key)),put:(key,value)=>values.set(key,structuredClone(value))};
 tx.getMany=async keys=>Promise.all(keys.map(key=>tx.get(key)));
 const transport=withCommitCoordinator(createR2Transport({database:new Bucket()}),{objectCall:(request,body)=>metadataObjectCall(tx,request,body)});
 const writer=await instantiateHost(fullModule,transport),reader=await instantiateHost(readerModule,transport),base={bucket:"database",prefix:"antfly",namespace:"qualification-wal-reader-probe"};
 const committed=await writer.request({...base,op:"mutate",wal_inline_tail:enabled,operation_id:"probe",mutations:[{kind:"upsert",doc_id:"one",body:'{"text":"capability"}'}]});
 const head=[...values].find(([key])=>/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(key))?.[1];
 const decoded=head?JSON.parse(new TextDecoder().decode(head.body)):null;
 if(decoded?.format!==(enabled?3:2)||enabled&&(typeof decoded.inline_tail!=="string"||!decoded.inline_tail.length)||!enabled&&decoded.inline_tail!=null)throw Error("WalInlineWriterUnsupported");
 const result=await reader.request({...base,op:"count",working_set_bytes:8*1024*1024,required_visibility:committed.visibility});
 if(result.count!=="1")throw Error("WalInlineReaderUnsupported");
 return{writer_format:enabled?3:2,reader_count:result.count};
}
