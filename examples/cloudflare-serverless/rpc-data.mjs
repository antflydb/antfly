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

// These RPC interfaces return data, never remote capabilities or streams.
// Materialize request-owned bytes before releasing the returned RPC group.
// This also releases results while waitUntil work keeps the caller alive.
export async function takeRpcData(pending){
 let value;
 const disposer=object=>object!=null&&typeof Symbol.dispose==='symbol'&&typeof object[Symbol.dispose]==='function'?object[Symbol.dispose]:null;
 const pendingDispose=disposer(pending);
 try{
  value=await pending;
  if(!pendingDispose&&!disposer(value))return value;
  validateData(value);return structuredClone(value);
 }finally{
  try{disposer(value)?.call(value);}finally{if(pending!==value)pendingDispose?.call(pending);}
 }
}

function validateData(value){
 const pending=[value],seen=new WeakSet();
 while(pending.length){
  const item=pending.pop();if(item==null||!['object','function'].includes(typeof item))continue;
  if(typeof item==='function')throw TypeError('RpcDataContainsCapability');
  if(seen.has(item))continue;seen.add(item);
  if(ArrayBuffer.isView(item)||item instanceof ArrayBuffer||item instanceof Date)continue;
  const proto=Object.getPrototypeOf(item);
  if(proto===Map.prototype){for(const[k,v]of Map.prototype.entries.call(item))pending.push(k,v);continue;}
  if(proto===Set.prototype){for(const v of Set.prototype.values.call(item))pending.push(v);continue;}
  if(!Array.isArray(item)&&proto!==null&&proto!==Object.prototype)throw TypeError('RpcDataContainsCapability');
  for(const key of Object.keys(item)){const field=Object.getOwnPropertyDescriptor(item,key);if(!field||!Object.hasOwn(field,'value'))throw TypeError('RpcDataContainsCapability');pending.push(field.value);}
 }
}
