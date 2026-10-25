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

import full from "./dist/antfly-serverless.wasm";
import compact from "./dist/antfly-serverless-compact.wasm";
import {instantiateHost} from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import {createImmutableReadCache} from "../../zig/lib/objectstore/wasm/cache.mjs";
import {RequestPool} from "./request-pool.mjs";
export const sharedCache=createImmutableReadCache({includeCommitLog:true});
export const readerKernel=new RequestPool(()=>instantiateHost(compact,null),2);
export const metadataKernel=new RequestPool(()=>instantiateHost(compact,null),1);
export const buildKernel=new RequestPool(()=>instantiateHost(full,null),1,{recycleBytes:24*1024*1024});
const stores=new Map();
export function boundRows(store){stores.delete(store);stores.set(store,true);while(stores.size>8){const oldest=stores.keys().next().value;oldest.rows.clear();oldest.rowBytes=0;stores.delete(oldest);}}

export function kernelMetrics(){return {wasm_pool_linear_bytes:readerKernel.memoryBytes()+metadataKernel.memoryBytes()+buildKernel.memoryBytes(),wasm_pool_limit_bytes:80*1024*1024,shared_cache_bytes:sharedCache.retainedBytes(),decoded_row_charge_bytes:[...stores.keys()].reduce((n,store)=>n+store.rowBytes,0)};}
