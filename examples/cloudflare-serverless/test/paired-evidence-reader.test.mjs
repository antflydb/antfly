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

import{test}from'node:test';import assert from'node:assert/strict';import reader from'../qualification/paired-evidence-reader.mjs';import{Bucket}from'./object-fixture.mjs';
test('parallel evidence reader drains bounded gets, preserves native order and has no write or source-call path',async()=>{const pair='antfly-1278-g-fixture-1',scopes=['b','c'].flatMap(side=>['','-writer','-reader','-builder'].map(role=>({source:pair+'-'+side+role,capture_id:crypto.randomUUID()}))).map((s,i)=>({...s,binding:'PRODUCER_'+i})),bucket=new Bucket(),scope=scopes[0];for(let i=0;i<40;i++)await bucket.put('probe-v1/'+scope.capture_id+'/'+String(i).padStart(3,'0'),JSON.stringify({i}));let active=0,peak=0;const env={API_TOKEN:'local-test-only',SOURCE_SCOPES:JSON.stringify(scopes),LOGS:{list:o=>bucket.list(o),get:async key=>{active++;peak=Math.max(active,peak);await new Promise(ok=>setTimeout(ok,1));const value=await bucket.get(key);active--;return value;}}},call=(path,body={})=>reader.fetch(new Request('https://reader.invalid'+path,{method:'POST',headers:{Authorization:'Bearer local-test-only'},body:JSON.stringify(body)}),env);const page=await(await call('/source/'+scope.source+'/page')).json();assert.equal(page.objects.length,32);assert.ok(peak>1&&peak<=32);assert.deepEqual(page.objects.map(o=>o.value.i),Array.from({length:32},(_,i)=>i));assert.equal(bucket.writes,40);assert.equal((await call('/source/'+scope.source+'/counter-witness')).status,404);assert.equal((await call('/drain-all')).status,404);assert.equal((await(await call('/health')).json()).rejections_present,false);});
