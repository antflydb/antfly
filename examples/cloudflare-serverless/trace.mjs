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

// Preserve platform flags without retaining arbitrary messages, headers or input.
// A remote exception alone does not establish a resource-limit failure.
export function platformErrorDetails(error){
 const details={};
 for(const key of ["retryable","overloaded","remote"])if(typeof error?.[key]==="boolean")details[key]=error[key];
 const message=typeof error?.message==="string"?error.message:"";
 const reference=/\breference\s*=\s*([a-zA-Z0-9_-]{1,128})\b/.exec(message)?.[1];if(reference)details.reference=reference;
 if(/\b(?:OutOfMemory|SQLITE_NOMEM)\b|memory limit exceeded|exceeded.*memory limit/i.test(message))details.resource_limit="memory";
 else if(/CPU.*limit|exceeded.*CPU/i.test(message))details.resource_limit="cpu";
 return details;
}

// Bounded phase spans use a local monotonic clock. Related services share IDs,
// not clocks; offsets from different isolates must never be subtracted.
export class CriticalPath {
 constructor({id=crypto.randomUUID(),scope="operation",clock=()=>performance.now()}={}){this.id=/^[a-f0-9-]{36}$/.test(id)?id:crypto.randomUUID();this.scope=scope;this.clock=clock;this.started=clock();this.spans=[];this.errors=[];this.dropped=0;}
 record(name,start,end=this.clock()){if(this.spans.length<128)this.spans.push({name,start_ms:start-this.started,duration_ms:Math.max(0,end-start)});else this.dropped++;}
 error(name,error){if(this.errors.length<8)try{this.errors.push({phase:name,...platformErrorDetails(error)});}catch{}}
 async span(name,run){const start=this.clock();try{return await run();}catch(error){this.error(name,error);throw error;}finally{this.record(name,start);}}
 stage(sample){const end=this.clock();this.record(sample.name,end-sample.duration_ms,end);}
 report(){return{event:"antfly.trace",trace_id:this.id,scope:this.scope,publication_id:this.publicationId??null,elapsed_ms:this.clock()-this.started,spans:this.spans,errors:this.errors,dropped_spans:this.dropped};}
 emit(enabled){if(enabled)try{console.log(JSON.stringify(this.report()));}catch{}}
}
