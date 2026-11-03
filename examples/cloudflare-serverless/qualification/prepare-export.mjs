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

import{walkOtlp}from'./otlp-chunks.mjs';
import{observerScopes,splitObserverEnvelope}from'./paired-observer.mjs';
import{sanitizeOtel}from'./otel-evidence.mjs';
import{prepareCanonicalRows,publishCanonicalRows}from'./canonical-windows.mjs';
const enc=new TextEncoder();
export async function prepareExport(text,signal,env,{receivedAt=Date.now(),digest}={}){
 const scopes=observerScopes(env),walk=walkOtlp(text,signal),buffers=new Map(),entries=[];let bytes=0,index=0,decodedChunks=0,bufferedBytes=0,peakBufferedBytes=0;
 async function flush(scope){const buffer=buffers.get(scope.source);if(!buffer?.rows.length)return;bufferedBytes-=buffer.bytes;const value={schema:1,qualification:false,signal,collector_version:env.WORKER_VERSION?.id??null,received_at:receivedAt,raw_batch_sha256:digest,source_batch_index:index++,rows:buffer.rows},prepared=await prepareCanonicalRows({...env,SOURCE_WORKER:scope.source,CAPTURE_ID:scope.capture_id},value);bytes+=prepared.bytes;if(bytes>16*1024*1024)throw Error('TelemetryDiagnosticWriteBudgetExceeded');entries.push({scope,prepared});buffers.delete(scope.source);}
 for(const chunk of walk.chunks){if(++decodedChunks>128)throw Error('ReceiverChunkBudgetExceeded');for(const part of splitObserverEnvelope(chunk,signal,scopes)){
  const scope=scopes.find(s=>s.source===part.source),rows=await sanitizeOtel(part.envelope,part.source,signal);
  for(const row of rows){const size=enc.encode(JSON.stringify(row)).length;if(size>4*1024*1024-4096)throw Error('TelemetrySanitizedBudgetExceeded');while(bufferedBytes+size>4*1024*1024){let largest;for(const [source,b]of buffers)if(!largest||b.bytes>largest[1].bytes)largest=[source,b];if(!largest)throw Error('TelemetrySanitizedBudgetExceeded');await flush(scopes.find(s=>s.source===largest[0]));}let buffer=buffers.get(part.source);if(buffer&&(buffer.rows.length>=1000||buffer.bytes+size>4*1024*1024-4096)){await flush(scope);buffer=null;}if(!buffer){buffer={rows:[],bytes:0};buffers.set(part.source,buffer);}buffer.rows.push(row);buffer.bytes+=size;bufferedBytes+=size;peakBufferedBytes=Math.max(peakBufferedBytes,bufferedBytes);}
 }}
 for(const scope of scopes)await flush(scope);
 return {entries,bytes,summary:{top_kind:walk.top_kind,envelopes:walk.envelopes,known_top_keys:walk.known_top_keys,resource_counts:walk.resource_counts,records:walk.records,resources:walk.resources,chunks:decodedChunks,prepared_batches:entries.length,prepared_bytes:bytes,peak_sanitized_buffer_bytes:peakBufferedBytes}};
}
export async function publishExport(env,prepared){
 let next=0;const failures=[];await Promise.all(Array.from({length:Math.min(4,prepared.entries.length)},async()=>{while(next<prepared.entries.length){const {scope,prepared:value}=prepared.entries[next++];try{await publishCanonicalRows({...env,SOURCE_WORKER:scope.source,CAPTURE_ID:scope.capture_id},value);}catch(error){failures.push(error);}}}));if(failures.length)throw failures[0];
}
