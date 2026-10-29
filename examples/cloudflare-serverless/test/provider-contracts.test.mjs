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
import {mkdtemp,writeFile,readFile,rm,mkdir,chmod} from 'node:fs/promises';
import {resolve,join} from 'node:path';
const exec=promisify(execFile),runner=resolve('qualification/provider-contracts.mjs');

for(const [name,patch,expected] of [
 ['invalid resource name',{id:'production-bucket'},'InvalidDisposableResourceIdentity'],
 ['invalid ownership nonce',{resource_nonce:'bad'},'InvalidDisposableResourceIdentity'],
 ['unknown report schema',{schema:2},'InvalidDisposableResourceIdentity'],
 ['unowned S3 bucket',{provider:'s3'},'OwnershipNotEstablished'],
 ['unowned GCS bucket',{provider:'gcs'},'OwnershipNotEstablished'],
])test('provider cleanup rejects '+name+' before credentials or network',async()=>{
 await mkdir('dist/qualification/provider-contracts-tests',{recursive:true});
 const dir=await mkdtemp(resolve('dist/qualification/provider-contracts-tests/cleanup-guard-'));
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

for(const failDelete of [false,true])test('versioned S3 cleanup '+(failDelete?'stops on per-version deletion failure':'removes exact versions and verifies absence'),async()=>{
 await mkdir('dist/qualification/provider-contracts-tests',{recursive:true});
 const dir=await mkdtemp(resolve('dist/qualification/provider-contracts-tests/version-cleanup-'));
 const file=join(dir,'report.json'),statePath=join(dir,'state.json'),binary=join(dir,'aws');
 const nonce='b'.repeat(32);
 const report={schema:1,id:'antfly-1278-multi-local-1234abcd',resource_nonce:nonce,provider:'s3',region:'us-east-1',owned:true,versioned:true,wal_gc:true,cleanup_complete:false};
 const state={failDelete,nonce,entries:[{Key:'wal/HEAD',VersionId:'old',Size:100},{Key:'wal/pages/old',VersionId:'null',Size:200}],markers:[{Key:'wal/HEAD',VersionId:'marker'}],calls:[]};
 try{
  await writeFile(file,JSON.stringify(report));await writeFile(statePath,JSON.stringify(state));
  await writeFile(binary,`#!${process.execPath}
import fs from 'node:fs';
const path=process.env.ANTFLY_TEST_AWS_STATE,s=JSON.parse(fs.readFileSync(path)),args=process.argv.slice(2),operation=args.slice(0,2).join(' ');
s.calls.push(operation);
let output={},code=0;
if(operation==='s3api get-bucket-tagging')output={TagSet:[{Key:'owner_nonce',Value:s.nonce}]};
else if(operation==='s3api list-object-versions')output={Versions:s.entries,DeleteMarkers:s.markers,IsTruncated:false};
else if(operation==='s3api delete-objects'){
 const batch=JSON.parse(args[args.indexOf('--delete')+1]).Objects;
 if(s.failDelete)output={Errors:[{Code:'AccessDenied'}]};
 else for(const item of batch){s.entries=s.entries.filter(v=>v.Key!==item.Key||v.VersionId!==item.VersionId);s.markers=s.markers.filter(v=>v.Key!==item.Key||v.VersionId!==item.VersionId);}
}else if(operation==='s3 rb'){if(s.entries.length||s.markers.length)code=2;else s.deleted=true;}
else if(operation==='s3api head-bucket'&&s.deleted){process.stderr.write('(404)');code=254;}
else code=3;
fs.writeFileSync(path,JSON.stringify(s));process.stdout.write(JSON.stringify(output));process.exitCode=code;
`);
  await chmod(binary,0o700);
  const run=exec(process.execPath,[runner,'--cleanup-only='+file],{env:{...process.env,PATH:dir,ANTFLY_TEST_AWS_STATE:statePath}});
  if(failDelete)await assert.rejects(run,error=>error.code===1);else await run;
  const saved=JSON.parse(await readFile(file,'utf8')),after=JSON.parse(await readFile(statePath,'utf8'));
  assert.equal(saved.cleanup_complete,!failDelete);
  if(failDelete){assert.equal(saved.cleanup_error,'VersionCleanupDeleteFailed');assert.equal(after.calls.includes('s3 rb'),false);}
  else {assert.equal(saved.cleanup_absence_verified,true);assert.equal(saved.retained_before_cleanup.versions,3);assert.equal(saved.retained_before_cleanup.bytes,300);assert.equal(after.deleted,true);assert.equal(after.entries.length+after.markers.length,0);}
 }finally{await rm(dir,{recursive:true,force:true});}
});
