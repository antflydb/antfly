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

// Qualify all reader binaries before a writer emits format-three roots. This
// runs locally with an in-memory binding and no account credentials.
import{Bucket}from'../test/object-fixture.mjs';
import{R2CoordinatorStore,encode}from'../r2-coordinator-store.mjs';
import{instantiateHost}from'../../../zig/lib/objectstore/wasm/asyncify.mjs';
export async function verifyInlineMetadataReaders(fullModule,...readerModules){
 if(readerModules.length<2)throw Error('InlineMetadataReaderCoverageRequired');
 const hint='x'.repeat(5000),store=new R2CoordinatorStore(fullModule,new Bucket(),'metadata-format-probe',{inlineReadSet:{keys:['head','missing','wal','hint'],prefix:'pins/'}});
 await store.transaction(tx=>{tx.put('head',1);tx.put('pins/1',42);tx.put('hint',hint);});
 const record={etag:'probe-wal',body:new TextEncoder().encode('{"latest_lsn":1}'),contentType:null};
 const committed=await store.commitWal({wal_key:'wal',value:encode(record),publication_key:'publication',watermark:'1',now_ms:Date.now(),id:'probe'});
 if(committed.applied!==true||JSON.parse(store.state.root).format!==3||store.state.root.length<=4096)throw Error('InlineMetadataWriterUnsupported');
 const state=store.state,input={op:'coordinate',bucket:'database',prefix:'antfly',namespace:'metadata-format-probe',working_set_bytes:4*1024*1024,coordinate:{action:'read',owner:state.owner,state,commands:['head','missing','pins/1','hint','wal'].map(key=>({kind:'get',key}))}};
 for(const module of[fullModule,...readerModules]){
  const host=await instantiateHost(module,async()=>{throw Error('InlineMetadataUnexpectedPhysicalRead');});
  const {results}=await host.request(input);
  if(results[0]?.[0]?.value!=='1'||results[1]?.length!==0||results[2]?.[0]?.value!=='42'||results[3]?.[0]?.value!==JSON.stringify(hint)||results[4]?.[0]?.value!==encode(record))throw Error('InlineMetadataReaderUnsupported');
 }
 return{format:3,readers:readerModules.length+1,root_bytes:state.root.length,physical_row_reads:0};
}
