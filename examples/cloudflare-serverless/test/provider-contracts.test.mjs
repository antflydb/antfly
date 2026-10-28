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

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,writeFile,readFile,rm,mkdir} from 'node:fs/promises';
import {resolve,join} from 'node:path';
const exec=promisify(execFile),runner=resolve('qualification/provider-contracts.mjs');

for(const [name,patch,expected] of [
 ['invalid resource name',{id:'production-bucket'},'InvalidDisposableResourceIdentity'],
 ['invalid ownership nonce',{resource_nonce:'bad'},'InvalidDisposableResourceIdentity'],
 ['unknown report schema',{schema:2},'InvalidDisposableResourceIdentity'],
 ['unowned S3 bucket',{provider:'s3'},'OwnershipNotEstablished'],
 ['unowned GCS bucket',{provider:'gcs'},'OwnershipNotEstablished'],
])test('provider cleanup rejects '+name+' before credentials or network',async()=>{
 await mkdir('dist/qualification/multiprovider-lab',{recursive:true});
 const dir=await mkdtemp(resolve('dist/qualification/multiprovider-lab/cleanup-guard-'));
 const file=join(dir,'report.json');
 const report={schema:1,id:'antfly-1278-multi-local-1234abcd',resource_nonce:'a'.repeat(32),provider:'gcs',region:'us-central1',owned:false,create_intent:false,cleanup_complete:false,...patch};
 try{
  await writeFile(file,JSON.stringify(report));
  // An empty PATH makes a credential-CLI call fail instead of reaching an
  // authenticated provider. Ownership must be rejected before that boundary.
  await assert.rejects(exec(process.execPath,[runner,'--cleanup-only='+file],{env:{...process.env,PATH:''}}),error=>{
   assert.equal(error.code,1);
   if(expected==='OwnershipNotEstablished')assert.match(error.stdout,/cleanup_complete.*false/);
   else assert.match(error.stderr,new RegExp(expected));
   return true;
  });
  const saved=JSON.parse(await readFile(file,'utf8'));
  assert.equal(saved.cleanup_complete,false);
  if(expected==='OwnershipNotEstablished')assert.equal(saved.cleanup_error,expected);
 }finally{await rm(dir,{recursive:true,force:true});}
});
