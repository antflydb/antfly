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

import{readCounterToken}from'./independent-counters.mjs';
export function platformCleanupApi(account,{authenticate=readCounterToken,fetch:send=fetch}={}){
 if(!/^[a-f0-9]{32}$/.test(account??''))throw Error('BaselineAccountInvalid');let token;
 return async route=>{if(!/^(?:workers\/scripts\/antfly-1278-floor-[a-z0-9]+-[a-f0-9]{4}(?:-private)?|r2\/buckets\/antfly-1278-floor-[a-z0-9]+-[a-f0-9]{4}|workers\/durable_objects\/namespaces(?:\/[a-f0-9]{32}|\?page=[1-9][0-9]?&per_page=100))$/.test(route))throw Error('BaselineReadRouteInvalid');token??=await authenticate();const response=await send('https://api.cloudflare.com/client/v4/accounts/'+account+'/'+route,{headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(30000)});if(!route.includes('?page=')){await response.body?.cancel();return{status:response.status,absent:response.status===404};}if(!response.ok){await response.body?.cancel();throw Error('BaselineNamespaceInventoryUnavailable');}const parts=[];let bytes=0;for await(const part of response.body){bytes+=part.byteLength;if(bytes>1024*1024)throw Error('BaselineNamespaceInventoryExceeded');parts.push(part);}return JSON.parse(Buffer.concat(parts));};
}
export async function ownPlatformNamespaces(runs,{api}={}){
 const account=runs[0]?.account,allowed=new Set(runs.map(r=>r.id+'-private'));if(runs.length!==3||runs.some(r=>r.account!==account||!/^antfly-1278-floor-[a-z0-9]+-[a-f0-9]{4}$/.test(r.id)))throw Error('BaselineNamespaceScopeInvalid');api??=platformCleanupApi(account);const own=[],seen=new Set();for(let page=1;page<100;page++){const value=await api('workers/durable_objects/namespaces?page='+page+'&per_page=100');if(!Array.isArray(value.result))throw Error('BaselineNamespaceInventoryInvalid');for(const n of value.result){if(seen.has(n.id))throw Error('BaselineNamespaceInventoryRepeated');seen.add(n.id);const script=n.script??n.script_name;if(allowed.has(script)){if(!/^[a-f0-9]{32}$/.test(n.id)||n.class!=='PlatformBaselineOwner'&&n.class_name!=='PlatformBaselineOwner')throw Error('BaselineNamespaceIdentityInvalid');own.push({script,id:n.id});}}if(value.result.length<100||value.result_info?.total_pages===page)break;if(page===99)throw Error('BaselineNamespaceInventoryExceeded');}if(own.length!==3||new Set(own.map(n=>n.script)).size!==3)throw Error('BaselineNamespaceIdentityMissing');return own;
}
export async function platformAbsence(runs,namespaces,{api}={}){if(namespaces.length!==3||namespaces.some(n=>!runs.some(r=>n.script===r.id+'-private')||!/^[a-f0-9]{32}$/.test(n.id)))throw Error('BaselineNamespaceScopeInvalid');api??=platformCleanupApi(runs[0].account);const checks=[];for(const run of runs){for(const name of[run.id,run.id+'-private'])checks.push({kind:'worker',name,...await api('workers/scripts/'+name)});checks.push({kind:'bucket',name:run.id,...await api('r2/buckets/'+run.id)});}for(const n of namespaces)checks.push({kind:'namespace',name:n.id,...await api('workers/durable_objects/namespaces/'+n.id)});return{qualification:false,checked_at:Date.now(),absent:checks.every(c=>c.absent===true),checks};}
