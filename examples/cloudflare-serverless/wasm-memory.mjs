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

// Rewrite only the declared memory maximum, preserving code and data. Zig's
// build has one internal wasm32 memory. Unsupported layouts fail the build.
export function limitWasmMemory(input,pages){
 const bytes=new Uint8Array(input);let at=8,found=false;const parts=[bytes.slice(0,8)];
 const read=()=>{let n=0,shift=0,b;do{b=bytes[at++];n+=(b&127)*2**shift;shift+=7;if(shift>35)throw new Error("InvalidWasmMemory");}while(b&128);return n;};
 const leb=n=>{const out=[];do{let b=n%128;n=Math.floor(n/128);if(n)b|=128;out.push(b);}while(n);return out;};
 while(at<bytes.length){const start=at,id=bytes[at++],length=read(),end=at+length;if(end>bytes.length)throw new Error("InvalidWasmMemory");if(id===5){const count=read(),flags=read(),minimum=read(),maximum=read();if(count!==1||flags!==1||at!==end||minimum>pages||maximum<pages)throw new Error("UnsupportedWasmMemory");const payload=[...leb(count),...leb(flags),...leb(minimum),...leb(pages)];parts.push(Uint8Array.from([5,...leb(payload.length),...payload]));found=true;}else parts.push(bytes.slice(start,end));at=end;}
 if(!found)throw new Error("MissingWasmMemory");const output=new Uint8Array(parts.reduce((n,p)=>n+p.length,0));let offset=0;for(const p of parts){output.set(p,offset);offset+=p.length;}if(!WebAssembly.validate(output))throw new Error("InvalidLimitedWasm");return output;
}
