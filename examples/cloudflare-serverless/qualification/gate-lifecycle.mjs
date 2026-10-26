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

import{assertGateRun,assertGateConfig}from'./gate-scope.mjs';
import{verifySealedRun,assertUnusedRecovery}from'./gate-provenance.mjs';
import{verifyWalTailReaders}from'./wal-compatibility.mjs';
import {readFile,writeFile,appendFile,mkdir,readdir,unlink,rm} from 'node:fs/promises';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
const cwd=fileURLToPath(new URL('../',import.meta.url)),cli=resolve(cwd,'node_modules/wrangler/bin/wrangler.js');
const gate=JSON.parse(await readFile(process.argv[2])),action=process.argv[3],selection=process.argv[4];
if(gate.schema!==1||!/^gate-[a-z0-9]+$/.test(gate.id)||!['seal','deploy','enable-wal-inline','upload-token','pause','resume','cleanup'].includes(action))throw Error('invalid lifecycle manifest/action');
const paths=gate.trials.flatMap(t=>[t.baseline,t.candidate]).filter(path=>!selection||path.includes(selection));
const save=run=>writeFile(resolve(run.dir,'run.json'),JSON.stringify(run,null,2));
async function command(run,args,{input,allowExists=false,allowMissing=false}={}){
 const result=await new Promise((resolve,reject)=>{const child=spawn(process.execPath,[cli,...args],{cwd,env:{...process.env,WRANGLER_SEND_METRICS:'false'},stdio:['pipe','pipe','pipe']});let output='';child.stdout.on('data',v=>output+=v);child.stderr.on('data',v=>output+=v);child.on('error',reject);child.on('close',code=>resolve({code,output}));child.stdin.end(input);});
 await appendFile(resolve(run.dir,'lifecycle.log'),result.output);
 if(result.code!==0&&!(allowExists&&/already exists|Queue name '[^']+' is already taken/i.test(result.output))&&!(allowMissing&&/does not exist|not found|could not find|No consumer/i.test(result.output)))throw Error(action+' failed for '+run.id+'; inspect lifecycle.log');return result.output;
}
async function lifecycle(path){
 const run=JSON.parse(await readFile(path));
 assertGateRun(run,{account:gate.account,dir:gate.dir});
 for(const service of run.services)assertGateConfig(run,service,JSON.parse(await readFile(service.config)));
 console.log(action,run.id);
 if(run.deleted){if(action==='cleanup')return;throw Error('test deployment already deleted');}
 async function walReaders(){
  const errors=await verifySealedRun(run);if(errors.length)throw Error(errors.join('; '));
  const hashes=run.bundle_hashes.writer,dir=resolve(run.dir,'bundle-writer');
  const paths=['-antfly-serverless.wasm','-antfly-serverless-compact.wasm'].map(suffix=>resolve(dir,Object.keys(hashes).find(name=>name.endsWith(suffix))));
  const modules=await Promise.all(paths.map(async path=>WebAssembly.compile(await readFile(path))));
  return verifyWalTailReaders(...modules);
 }
 if(action==='enable-wal-inline'){
  if(run.kind!=='candidate'||!run.deployed&&!run.wal_inline_tail_activating||!['app','writer','reader','builder'].every(role=>run.versions?.[role]))throw Error('deploy every compatible role before WAL activation');
  assertUnusedRecovery(run);run.wal_inline_probe=await walReaders();
  run.deployed=false;run.wal_inline_tail_activating=true;await save(run);
  for(const service of run.services){
   const config=JSON.parse(await readFile(service.config));config.vars.WAL_INLINE_TAIL='true';await writeFile(service.config,JSON.stringify(config,null,2));
   const output=await command(run,['deploy','--no-bundle','--config',service.config]),version=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];
   if(!version)throw Error('missing activation deployment version');run.versions[service.role]=version;if(service.role==='app')run.version=version;await save(run);
  }
  run.wal_inline_tail_enabled=true;delete run.wal_inline_tail_activating;run.deployed=true;await save(run);return;
 }
 if(action==='resume'){assertUnusedRecovery(run);const builder=run.services.find(s=>s.role==='builder'),config=JSON.parse(await readFile(builder.config));config.triggers={crons:['* * * * *']};config.vars.AUTO_GC='true';await writeFile(builder.config,JSON.stringify(config,null,2));const output=await command(run,['deploy','--no-bundle','--config',builder.config]);run.versions.builder=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];await command(run,['queues','resume-delivery',run.id,'--config',builder.config]);run.paused=false;await save(run);return;}
 if(action==='seal'){
  run.deployed=false;await save(run);
  run.bundle_hashes={};for(const service of run.services){
   const config=JSON.parse(await readFile(service.config));config.main=service.entry;await writeFile(service.config,JSON.stringify(config,null,2));const output=resolve(run.dir,'bundle-'+service.role);await rm(output,{recursive:true,force:true});await mkdir(output,{recursive:true});
   await command(run,['deploy','--dry-run','--config',service.config,'--outdir',output]);const files=await readdir(output),entries=files.filter(f=>f.endsWith('.js'));if(entries.length!==1)throw Error('ambiguous sealed entry');
   config.main=resolve(output,entries[0]);await writeFile(service.config,JSON.stringify(config,null,2));const hashes={};for(const name of files.filter(f=>f.endsWith('.wasm')||f.endsWith('.js')))hashes[name]=createHash('sha256').update(await readFile(resolve(output,name))).digest('hex');
   if(!Object.entries(hashes).some(([name,hash])=>name.endsWith('-antfly-serverless.wasm')&&hash===run.wasm_sha256))throw Error('native artifact hash changed; regenerate the manifest');run.bundle_hashes[service.role]=hashes;
  }await save(run);return;
 }
 if(action==='deploy'){
  const errors=await verifySealedRun(run);if(errors.length)throw Error(errors.join('; '));
  if(run.wal_inline_tail_enabled||run.wal_inline_tail_activating)await walReaders();
  if(!run.bundle_hashes)throw Error('seal before deploying');for(const args of[['r2','bucket','create',run.id],['queues','create',run.id],['queues','create',run.id+'-dead']])await command(run,[...args,'--config',run.config],{allowExists:true});
  run.versions={};for(const service of run.services){const output=await command(run,['deploy','--no-bundle','--config',service.config]);const version=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];if(!version)throw Error('missing deployment version');run.versions[service.role]=version;if(service.role==='app'){run.origin=/https:\/\/[^\s]+\.workers\.dev/.exec(output)?.[0];if(!run.origin||!new URL(run.origin).hostname.startsWith(run.id+'.'))throw Error('origin mismatch');run.version=version;}await save(run);}run.deployed=true;await save(run);return;
 }
 if(action==='upload-token'){
  if(!run.deployed)throw Error('deploy before installing the disposable token');const token=(await readFile(run.token_file,'utf8')).trim();if(!/^[a-f0-9]{64}$/.test(token))throw Error('invalid disposable gate token');await command(run,['secret','put','API_TOKEN','--config',run.config],{input:token});let version;
  for(let i=0;i<120;i++){try{const response=await fetch(new URL('/_qualification/version',run.origin),{method:'POST',headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(10000)});if(response.ok){version=(await response.json()).version;if(version&&version!==run.version)break;}}catch{}await new Promise(r=>setTimeout(r,500));}
  if(!version||version===run.version)throw Error('secret version did not converge');run.version=version;run.versions.app=version;run.token_installed=true;await save(run);return;
 }
 const builder=run.services.find(s=>s.role==='builder');
 await command(run,['queues','pause-delivery',run.id,'--config',builder.config],{allowMissing:action==='cleanup'});
 if(action==='pause'){
  const config=JSON.parse(await readFile(builder.config));config.triggers={crons:[]};config.vars.AUTO_GC='false';await writeFile(builder.config,JSON.stringify(config,null,2));const output=await command(run,['deploy','--no-bundle','--config',builder.config]);run.versions.builder=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];run.paused=true;await save(run);return;
 }
 run.cleanup_steps??={};
 const step=async(name,perform)=>{if(run.cleanup_steps[name])return;await perform();run.cleanup_steps[name]=true;await save(run);};
 // Detach the public app from actor namespaces, stop all private writers, then
 // drain through its R2-only qualification endpoint. It never wakes an actor.
 await step('detach-app',async()=>{const config=JSON.parse(await readFile(run.config));config.durable_objects.bindings=[];await writeFile(run.config,JSON.stringify(config,null,2));await command(run,['deploy','--no-bundle','--config',run.config]);});
 await step('remove-consumer',()=>command(run,['queues','consumer','remove',run.id,run.id+'-builder','--config',builder.config],{allowMissing:true}));
 for(const role of['builder','reader','writer'])await step('delete-'+role,()=>command(run,['delete','--force','--config',run.services.find(s=>s.role===role).config],{allowMissing:true}));
 await step('quiescence',()=>new Promise(r=>setTimeout(r,31000)));
 await step('drain',async()=>{
  if(!run.token_installed)throw Error('approved disposable token installation required for authenticated drain');
  const token=(await readFile(run.token_file,'utf8')).trim(),drain={objects:0,bytes:0};let done=false;
  for(let i=0;i<10000;i++){const response=await fetch(new URL('/_qualification/drain',run.origin),{method:'POST',headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(35000)});if(!response.ok)throw Error('drain failed: '+response.status);const page=await response.json();drain.objects+=page.deleted??0;drain.bytes+=page.bytes??0;if(page.done){done=true;break;}}
  if(!done)throw Error('drain did not finish');await writeFile(resolve(run.dir,'drain-report.json'),JSON.stringify(drain,null,2));
 });
 await step('delete-app',()=>command(run,['delete','--force','--config',run.config],{allowMissing:true}));
 for(const queue of run.queues)await step('delete-'+queue,()=>command(run,['queues','delete',queue,'--config',run.config],{allowMissing:true}));
 await step('delete-bucket',()=>command(run,['r2','bucket','delete',run.id,'--config',run.config],{allowMissing:true}));
 await unlink(run.token_file).catch(error=>{if(error.code!=='ENOENT')throw error;});run.deleted=true;await save(run);
}
for(let i=0;i<paths.length;i+=2){const results=await Promise.allSettled(paths.slice(i,i+2).map(lifecycle));for(const result of results)if(result.status==='rejected')throw result.reason;}
