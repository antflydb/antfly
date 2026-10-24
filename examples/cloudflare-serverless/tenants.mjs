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

// Tenant routes are operator configuration. API_TOKEN remains an operator
// credential; end-user authorization belongs in the hosted application.
export function tenantEnv(env,id){
 if(id==null)return env;
 if(typeof id!=="string"||!/^[a-zA-Z0-9_-]{1,64}$/.test(id))throw new Error("UnknownTenant");
 const routes=JSON.parse(env.ANTFLY_TENANTS??"{}");
 if(Object.keys(routes).length>32)throw new Error("TenantRouteLimitExceeded");
 if(!Object.hasOwn(routes,id))throw new Error("UnknownTenant");
 const route=routes[id];
 if(!route||typeof route.incarnation!=="string"||!route.incarnation.length||route.incarnation.length>256||typeof route.storage_id!=="string"||!route.storage_id.length||route.storage_id.length>256)throw new Error("InvalidTenantRoute");
 const all=[{incarnation:env.ANTFLY_INCARNATION,storage_id:env.COORDINATOR_STORAGE_ID??env.ANTFLY_INCARNATION},...Object.values(routes)];
 if(new Set(all.map(r=>r.incarnation)).size!==all.length||new Set(all.map(r=>r.storage_id)).size!==all.length)throw new Error("TenantRouteCollision");
 return {...env,ANTFLY_INCARNATION:route.incarnation,COORDINATOR_STORAGE_ID:route.storage_id,ANTFLY_TENANT:id};
}
export function tenantEnvs(env){const routes=JSON.parse(env.ANTFLY_TENANTS??"{}");if(Object.keys(routes).length>32)throw new Error("TenantRouteLimitExceeded");return[env,...Object.keys(routes).map(id=>tenantEnv(env,id))];}
