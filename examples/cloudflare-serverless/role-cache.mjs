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

import{createImmutableReadCache}from'../../zig/lib/objectstore/wasm/cache.mjs';
// Protect reusable data pages from short-lived metadata/log churn, while
// retaining the existing aggregate byte and entry ceilings. Binding identities
// isolate equal namespace/object names in different R2 buckets.
export function createRoleCache({maxBytes=8*1024*1024,maxEntries=1024}={}){
 if(!Number.isSafeInteger(maxBytes)||maxBytes<0||!Number.isSafeInteger(maxEntries)||maxEntries<0)throw Error('InvalidRoleCacheBudget');
 const metadataBytes=Math.floor(maxBytes/4),metadataEntries=Math.floor(maxEntries/4),metadata=createImmutableReadCache({maxBytes:metadataBytes,maxEntries:metadataEntries,includeCommitLog:true,writeThrough:true}),data=createImmutableReadCache({maxBytes:maxBytes-metadataBytes,maxEntries:maxEntries-metadataEntries,writeThrough:true});
 const bindings=new WeakMap();let nextBinding=0;
 const prefix=binding=>{if(binding==null)return'unbound';if(typeof binding!=='object'&&typeof binding!=='function')throw Error('InvalidCacheBinding');if(!bindings.has(binding))bindings.set(binding,String(++nextBinding));return bindings.get(binding);};
 const clear=()=>{metadata.clear();data.clear();};
 const view=id=>({clear,retainedBytes:()=>metadata.retainedBytes()+data.retainedBytes(),wrap:(transport,scope)=>data.wrap(transport,id+'/'+scope),metadata:{clear,retainedBytes:metadata.retainedBytes,wrap:(transport,scope)=>metadata.wrap(transport,id+'/'+scope)}});
 return{clear,retainedBytes:()=>metadata.retainedBytes()+data.retainedBytes(),metadataBytes:metadata.retainedBytes,dataBytes:data.retainedBytes,forBucket:binding=>view(prefix(binding)),wrap:(transport,scope,binding)=>data.wrap(transport,prefix(binding)+'/'+scope)};
}
