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

import{test}from'node:test';import assert from'node:assert/strict';import{platformCleanupApi,ownPlatformNamespaces,platformAbsence}from'../qualification/platform-baseline-cleanup-proof.mjs';
const runs=['one','two','three'].map((s,i)=>({id:'antfly-1278-floor-'+s+'-'+['1234','abcd','abcd'][i],account:'a'.repeat(32)})),namespaces=runs.map((r,i)=>({id:String(i+1).repeat(32),script:r.id+'-private'}));
test('cleanup verification requires scoped worker, bucket and namespace absence; authorization failures are not absence',async()=>{
 let auth=0;const seen=[],api=platformCleanupApi('a'.repeat(32),{authenticate:async()=>{auth++;return'private-fixture-token';},fetch:async(url,init)=>{seen.push(url);assert.equal(init.headers.Authorization,'Bearer private-fixture-token');return new Response('{}',{status:seen.length===1?403:404});}}),proof=await platformAbsence(runs,namespaces,{api});assert.equal(auth,1);assert.equal(proof.checks.length,12);assert.equal(proof.absent,false);assert.equal(proof.checks[0].absent,false);await assert.rejects(api('workers/scripts/production'),/RouteInvalid/);assert.ok(!JSON.stringify(proof).includes('private-fixture-token'));assert.ok(seen.every(url=>url.startsWith('https://api.cloudflare.com/client/v4/accounts/'+'a'.repeat(32))));
});
test('namespace inventory retains only the three exact fixture identities and rejects missing or duplicate identities',async()=>{
 const api=async()=>({result:[...namespaces.map(n=>({...n,class:'PlatformBaselineOwner'})),{id:'f'.repeat(32),script:'production',class:'Other'}]}),own=await ownPlatformNamespaces(runs,{api});assert.deepEqual(own,namespaces);await assert.rejects(ownPlatformNamespaces(runs,{api:async()=>({result:[]})}),/IdentityMissing/);await assert.rejects(ownPlatformNamespaces(runs,{api:async()=>({result:[{...namespaces[0],class:'PlatformBaselineOwner'},{...namespaces[0],class:'PlatformBaselineOwner'}]})}),/Repeated/);
});
