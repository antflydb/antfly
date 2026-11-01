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

import{resourceFailureCode}from'../performance.mjs';
// Recovered failures still disqualify a window. Keep this identical for live,
// canonical and persisted evidence; a final outcome of "ok" cannot erase them.
export function resourceLogFailure(value){
 if(!value||typeof value!=='object')return null;
 for(const item of[value,...(Array.isArray(value.errors)?value.errors:[])]){
  for(const key of['failure_code','error','error_message','message']){const code=resourceFailureCode(item?.[key]);if(code)return code;}
  if(item?.resource_limit==='memory')return'MemoryLimitExceeded';if(item?.resource_limit==='cpu')return'CPULimitExceeded';
 }
 if(value.physical?.resource_failures>0||value.physical?.edge_cache_resource_errors>0)return'ResourceFailureObserved';return null;
}
