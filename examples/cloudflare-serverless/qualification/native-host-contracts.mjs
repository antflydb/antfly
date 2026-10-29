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

// Full native process lifecycle over owned local file storage. This verifies
// CLI/bootstrap/HTTP/reopen contracts; it is not a cloud latency qualification.
import {spawn} from 'node:child_process';
import {createReadStream} from 'node:fs';
import {createServer} from 'node:net';
import {mkdir,mkdtemp,readFile,writeFile,readdir,stat,rm} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const arg=name=>process.argv.find(value=>value.startsWith('--'+name+'='))?.slice(name.length+3);
const binary=resolve(arg('binary')??fileURLToPath(new URL('../../../zig/zig-out/bin/antfly',import.meta.url))),out=resolve(arg('out')??'dist/qualification/native-host');
await mkdir(out,{recursive:true});
const hash=createHash('sha256');for await(const bytes of createReadStream(binary))hash.update(bytes);
const binaryHash=hash.digest('hex');
const directory=await mkdtemp(join(out,'native-host-'));
const storage=join(directory,'storage');await mkdir(storage,{mode:0o700});
const report={qualification:false,kind:'native release process CLI/HTTP/file-storage lifecycle; not cloud latency or idempotent HTTP wire qualification',binary,sha256:binaryHash,passed:false,cleanup_complete:false,started_at:new Date().toISOString(),lifecycles:[],exits:[]};
let child,exited,spawnFailed=false,logs='';
async function port(){const server=createServer();await new Promise((done,fail)=>{server.once('error',fail);server.listen(0,'127.0.0.1',done);});const value=server.address().port;await new Promise((done,fail)=>server.close(error=>error?fail(error):done()));return value;}
const sleep=ms=>new Promise(done=>setTimeout(done,ms));
async function stop(){
 if(!child)return;
 const process=child;process.kill('SIGTERM');
 let timer;
 try{
  const outcome=await Promise.race([exited,new Promise((_,fail)=>{timer=setTimeout(()=>fail(Error('NativeShutdownTimedOut')),10000);})]);
  report.exits.push(outcome);
  assert.ok(outcome.code===0||outcome.signal==='SIGTERM','NativeShutdownFailed');
 }catch(error){process.kill('SIGKILL');await exited;throw error;}
 finally{clearTimeout(timer);if(process.exitCode!==null||process.signalCode!==null||spawnFailed)child=null;}
}
async function start(collection){
 const bind=await port();let health;do{health=await port();}while(health===bind);const args=['serverless','combined'];
 for(const kind of ['artifacts','manifests','wal','progress','catalog']){const path=join(storage,kind);await mkdir(path,{recursive:true});args.push('--'+kind+'-uri',pathToFileURL(path).href);}
 args.push('--indexed-wal','true','--wal-gc',String(collection),'--native-read-state','true','--wal-document-index','true','--query-cache-dir',join(storage,'cache'),'--query-cache-max-bytes','67108864','--host','127.0.0.1','--port',String(bind),'--health-port',String(health));
 for(const name of ['publish','compaction','prune','enrichment'])args.push('--'+name+'-enabled','false');
 const process=spawn(binary,args,{stdio:['ignore','pipe','pipe']});child=process;spawnFailed=false;
 exited=new Promise(done=>{process.once('error',error=>{spawnFailed=true;done({error:error.code});});process.once('exit',(code,signal)=>done({code,signal}));});
 const append=bytes=>{if(logs.length+bytes.length<=8*1024*1024)logs+=bytes;else{report.log_overflow=true;process.kill('SIGTERM');}};
 process.stdout.on('data',append);process.stderr.on('data',append);
 const origin='http://127.0.0.1:'+bind;
 const call=async(method,path,body,expected=200,timeout=10000)=>{
  const response=await fetch(origin+path,{method,headers:body===undefined?undefined:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(timeout)});
  const value=await response.json();assert.equal(response.status,expected,JSON.stringify(value));return value;
 };
 let ready=false;
 const deadline=performance.now()+30000;
 while(performance.now()<deadline){
  if(spawnFailed||process.exitCode!==null||process.signalCode!==null)throw Error('NativeProcessExitedBeforeReadiness');
  try{const status=await call('GET','/health',undefined,200,1000);if(status.live===true&&status.ready===true&&status.validated===true){ready=true;break;}}catch{}await sleep(100);
 }
 if(!ready)throw Error('NativeReadinessTimedOut');report.lifecycles.push({collection_enabled:collection,ready:true});return call;
}
async function inspect(path,latest){let marker=false,managedHeads=0;for(const entry of await readdir(path,{withFileTypes:true})){const p=join(path,entry.name);if(entry.isDirectory()){const nested=await inspect(p,latest);marker ||= nested.marker;managedHeads+=nested.managedHeads;}else if(entry.isFile()&&(await stat(p)).size<=4096){const value=await readFile(p,'utf8');if(value==='antfly-wal:indexed-managed:1')marker=true;try{const head=JSON.parse(value);if(head.root_lifetime_ms===30000&&head.latest_lsn===latest)managedHeads++;}catch{}}}return{marker,managedHeads};}
try{
 let call=await start(true);
 await call('PUT','/internal/v1/namespaces/docs',{created_at_ns:123,policy:{default_query_view:'published',keep_latest_versions:4,max_pending_records:4,compaction_trigger_version_count:2,vector_distance_metric:'inner_product'}},201);
 const ingested=await call('PUT','/internal/v1/namespaces/docs/ingest-batch',{timestamp_ns:456,mutations:[{kind:'upsert',doc_id:'doc-a',body:'alpha'},{kind:'upsert',doc_id:'doc-b',body:'beta'}]},202);
 assert.equal(ingested.start_lsn,1);assert.equal(ingested.end_lsn,2);
 const built=await call('POST','/internal/v1/namespaces/docs/build',undefined,202);assert.equal(built.published,true);
 const verify=async(latest,expected)=>{const result=await call('GET','/internal/v1/namespaces/docs/query/latest');assert.equal(result.latest_wal_lsn,latest);assert.equal(result.document_count,Object.keys(expected).length);assert.deepEqual(Object.fromEntries(result.documents.map(row=>[row.doc_id,row.body])),expected);};
 const initial={'doc-a':'alpha','doc-b':'beta'};
 await verify(2,initial);await stop();
 report.managed_storage=await inspect(join(storage,'wal'),2);assert.equal(report.managed_storage.marker,true);assert.ok(report.managed_storage.managedHeads>0);
 call=await start(false);await verify(2,initial);
 const fresh=await call('PUT','/internal/v1/namespaces/docs/ingest-batch',{timestamp_ns:457,mutations:[{kind:'upsert',doc_id:'doc-c',body:'gamma'}]},202);
 assert.equal(fresh.start_lsn,3);assert.equal(fresh.end_lsn,3);await verify(3,{...initial,'doc-c':'gamma'});await stop();
 report.managed_storage_after_reopen=await inspect(join(storage,'wal'),3);assert.equal(report.managed_storage_after_reopen.marker,true);assert.ok(report.managed_storage_after_reopen.managedHeads>0);assert.ok(!report.log_overflow);report.passed=true;
}catch(error){report.error=String(error.message).slice(0,1024);process.exitCode=1;}
finally{
 try{await stop();}catch(error){report.shutdown_error=error.message;process.exitCode=1;}
 await writeFile(join(directory,'native.log'),logs);
 if(!child){await rm(storage,{recursive:true,force:true});report.cleanup_complete=true;}
 report.finished_at=new Date().toISOString();await writeFile(join(directory,'report.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({report:join(directory,'report.json'),passed:report.passed,cleanup_complete:report.cleanup_complete,error:report.error??report.shutdown_error??null}));
}
