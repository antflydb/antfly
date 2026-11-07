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

import module from './dist/antfly-serverless.wasm';
import {RequestPool} from './request-pool.mjs';
import {instantiateHost} from '../../zig/lib/objectstore/wasm/asyncify.mjs';
import {createR2Transport} from '../../zig/lib/objectstore/wasm/r2.mjs';
import {createImmutableReadCache} from '../../zig/lib/objectstore/wasm/cache.mjs';
import {sqlConfig,digestText} from './sql-control.mjs';
const pool=new RequestPool(()=>instantiateHost(module,null),1,{maxReservedBytes:32*1024*1024,reservationBytes:32*1024*1024,recycleBytes:24*1024*1024,maxQueued:8});
const deployments=new WeakMap();
const cache=createImmutableReadCache({includeSQL:true,maxBytes:8*1024*1024,maxEntries:128});
export function sqlDeployment(env){
 let deployment=deployments.get(env);
 if(!deployment){const{config,bindings}=sqlConfig(env);deployment={config,bindings,transport:cache.wrap(createR2Transport(bindings,{maxResponseBytes:8*1024*1024,maxPutBytes:8*1024*1024}),crypto.randomUUID())};deployments.set(env,deployment);}
 return deployment;
}
export async function runSQL(env,input){
 const deployment=sqlDeployment(env);
 return pool.withHost(host=>host.request({...input,config:deployment.config,working_set_bytes:20*1024*1024},{transport:deployment.transport,storageScope:env,deadlineMs:Date.now()+55000,maxOperations:900,maxTransferBytes:64*1024*1024}),{deadlineMs:Date.now()+55000,fairKey:input.tenant?digestText(input.tenant):'maintenance'});
}
