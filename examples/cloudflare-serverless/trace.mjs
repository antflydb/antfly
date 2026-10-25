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

// Bounded phase spans use a local monotonic clock. Related services share IDs,
// not clocks; offsets from different isolates must never be subtracted.
export class CriticalPath {
 constructor({id=crypto.randomUUID(),scope="operation",clock=()=>performance.now()}={}){this.id=/^[a-f0-9-]{36}$/.test(id)?id:crypto.randomUUID();this.scope=scope;this.clock=clock;this.started=clock();this.spans=[];this.dropped=0;}
 record(name,start,end=this.clock()){if(this.spans.length<128)this.spans.push({name,start_ms:start-this.started,duration_ms:Math.max(0,end-start)});else this.dropped++;}
 async span(name,run){const start=this.clock();try{return await run();}finally{this.record(name,start);}}
 stage(sample){const end=this.clock();this.record(sample.name,end-sample.duration_ms,end);}
 report(){return{event:"antfly.trace",trace_id:this.id,scope:this.scope,publication_id:this.publicationId??null,elapsed_ms:this.clock()-this.started,spans:this.spans,dropped_spans:this.dropped};}
 emit(enabled){if(enabled)try{console.log(JSON.stringify(this.report()));}catch{}}
}
