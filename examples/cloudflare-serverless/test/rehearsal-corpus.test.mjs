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

import{test}from'node:test';import assert from'node:assert/strict';import{mkdtemp,mkdir,readFile,writeFile,readdir,rm}from'node:fs/promises';import{resolve}from'node:path';import{spawnSync}from'node:child_process';import{createHash}from'node:crypto';
const sha=v=>createHash('sha256').update(v).digest('hex');
async function fixture(runTest){
 const dir=await mkdtemp(resolve('dist','rehearsal-corpus-')),gate={schema:1,id:'gate-corpus',dir,account:'a'.repeat(32),trials:[{trial:1}]},corpusId=crypto.randomUUID(),execution=crypto.randomUUID(),report={schema:1,qualification:false,stage:'complete',finished_at:Date.now(),execution_id:execution,sides:[]};
 try{for(const kind of['baseline','candidate']){const id='antfly-1278-g-corpus-1-'+kind[0],folder=resolve(dir,id);await mkdir(folder);const corpus={id:corpusId,root:{incarnation:'root-'+kind,storage_id:'root-'+kind},tenants:Object.fromEntries(['a','b','c','d'].map(t=>[t,{incarnation:kind+'-'+t,storage_id:kind+'-'+t}]))},full=Buffer.from('test-only full'),compact=Buffer.from('test-only compact'),r={id,dir:folder,account:gate.account,bucket:id,worker:id,queues:[id,id+'-dead'],token_file:resolve(folder,'api-token'),paused:true,revision:'source-'+kind,version:crypto.randomUUID(),wasm_sha256:sha(full),reader_wasm_sha256:sha(compact),corpus,runtime_placement:null,bundle_hashes:{},services:[]};
  for(const role of['app','writer','reader','builder']){const name=id+(role==='app'?'':'-'+role),bundle=resolve(folder,'bundle-'+role);await mkdir(bundle);const files={'entry.js':Buffer.from('export default {};'),'test-antfly-serverless.wasm':full,'test-antfly-serverless-compact.wasm':compact};r.bundle_hashes[role]=Object.fromEntries(Object.entries(files).map(([n,b])=>[n,sha(b)]));for(const[n,b]of Object.entries(files))await writeFile(resolve(bundle,n),b);const config=resolve(folder,role+'.json');await writeFile(config,JSON.stringify({name,account_id:r.account,main:resolve(bundle,'entry.js'),r2_buckets:[{binding:'DATABASE',bucket_name:id}],vars:{ANTFLY_INCARNATION:corpus.root.incarnation,ANTFLY_TENANTS:JSON.stringify(corpus.tenants)}}));r.services.push({role,name,config});}
  const path=resolve(folder,'run.json');await writeFile(path,JSON.stringify(r));gate.trials[0][kind]=path;report.sides.push({run:id,source_revision:r.revision,version:r.version,wasm_sha256:r.wasm_sha256,reader_wasm_sha256:r.reader_wasm_sha256,corpus,bundle_hashes:r.bundle_hashes,runtime_placement:null,execution_id:execution,cold:[]});
 }
 const gatePath=resolve(dir,'gate.json'),reportPath=resolve(dir,'report.json');await writeFile(gatePath,JSON.stringify(gate));await writeFile(reportPath,JSON.stringify(report));await runTest({dir,gate,gatePath,report,reportPath,call:adopt=>spawnSync(process.execPath,['qualification/prepare-corpus.mjs',gatePath,'1',reportPath,...adopt?['--adopt-completed-rehearsal']:[]],{encoding:'utf8'})});
 }finally{await rm(dir,{recursive:true,force:true});}
}
test('legacy completed rehearsal adoption is explicit and archives original corpus and report bytes',async()=>fixture(async f=>{
 const before=await readFile(f.gate.trials[0].baseline),original=await readFile(f.reportPath);assert.notEqual(f.call(false).status,0);assert.deepEqual(await readFile(f.gate.trials[0].baseline),before);const result=f.call(true);assert.equal(result.status,0,result.stderr);const r=JSON.parse(await readFile(f.gate.trials[0].baseline));assert.equal(r.last_execution_id,f.report.execution_id);assert.equal(r.last_phase,'corpus-prepared');assert.notEqual(r.corpus.id,f.report.sides[0].corpus.id);assert.equal(r.adopted_completed_rehearsal_sha256,sha(original));const archive=(await readdir(f.dir)).find(n=>n.startsWith('corpus-'));assert.deepEqual(await readFile(resolve(f.dir,archive,r.id,'run.json')),before);assert.deepEqual(await readFile(f.reportPath),original);
}));
test('active or identity-mismatched completed rehearsals cannot claim a corpus',async()=>fixture(async f=>{
 const before=await readFile(f.gate.trials[0].baseline);f.report.stage='workload';await writeFile(f.reportPath,JSON.stringify(f.report));assert.notEqual(f.call(true).status,0);f.report.stage='complete';f.report.sides[0].source_revision='different';await writeFile(f.reportPath,JSON.stringify(f.report));assert.notEqual(f.call(true).status,0);assert.deepEqual(await readFile(f.gate.trials[0].baseline),before);
}));
