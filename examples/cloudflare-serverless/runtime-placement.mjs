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

// Compute identity is independent of the durable R2 collection/storage identity.
// Changing a generation creates fresh actors; ordinary native owner fencing and
// recovery still arbitrate access to the SAME authoritative R2 records.
const applied=Symbol("antfly.runtime-placement"),locations=new Set(["wnam","enam","sam","weur","eeur","apac","apac-ne","apac-se","oc","afr","me"]);
export function placementEnv(env){
 const hint=env.DO_LOCATION_HINT??env.R2_LOCATION_HINT,generation=env.RUNTIME_GENERATION;
 if(hint==null&&generation==null)return env;
 if(env.COORDINATOR_BACKEND!=="r2"||typeof generation!=="string"||!/^[a-zA-Z0-9_-]{1,64}$/.test(generation)||hint!=null&&!locations.has(hint))throw Error("InvalidRuntimePlacement");
 const policy=JSON.stringify([generation,hint??null]);
 if(env[applied]!=null){if(env[applied]!==policy)throw Error("RuntimePlacementAlreadyConfigured");return env;}
 const result={...env,[applied]:policy};
 for(const name of ["COMMITS","READERS","BUILDERS"]){
  const binding=env[name];if(!binding)continue;
  const encode=value=>JSON.stringify(["antfly-runtime-v1",generation,hint??null,value]);
  const options=value=>hint==null?value:{...value,locationHint:hint};
  result[name]=new Proxy(binding,{get(target,key){
   if(key==="idFromName")return value=>target.idFromName(encode(value));
   if(key==="getByName")return (value,settings)=>target.getByName(encode(value),options(settings));
   if(key==="get")return (id,settings)=>target.get(id,options(settings));
   const value=target[key];return typeof value==="function"?value.bind(target):value;
  }});
 }
 return result;
}
