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

import{readFile,writeFile,mkdir,appendFile,readdir,unlink}from'node:fs/promises';import{resolve,join}from'node:path';import{randomBytes,createHash}from'node:crypto';import{spawn}from'node:child_process';import assert from'node:assert/strict';import{tailFrames}from'./metrics-tail.mjs';
const[action,path,candidatePath]=process.argv.slice(2),cli=resolve('node_modules/wrangler/bin/wrangler.js');
const json=async p=>JSON.parse(await readFile(p,'utf8')),save=r=>writeFile(join(r.dir,'pair.json'),JSON.stringify(r,null,2)),hash=b=>createHash('sha256').update(b).digest('hex');
if(action==='prepare'){
 const baseline=await json(path),candidate=await json(candidatePath);assert.equal(baseline.profile,'approved-control');assert.equal(baseline.source.revision,'c44896ac8c13898c549b7896ab633b1b8b25c04c');assert.equal(candidate.profile,'durable-candidate');assert.equal(baseline.account,candidate.account);
 const id='antfly-1278-driver-'+Date.now().toString(36)+'-'+randomBytes(2).toString('hex'),dir=resolve('dist/qualification',id);await mkdir(dir,{recursive:true});const r={schema:1,kind:'paired-http-diagnostic',qualification:false,id,dir,account:baseline.account,baseline:resolve(path),candidate:resolve(candidatePath),token_file:join(dir,'api-token'),driver_config:join(dir,'driver.json'),reports:[],limits:['Small four-tenant 64-document paired experiment; not full gate','Cloudflare-to-public-Worker HTTP wall latency; excludes external internet-client transit','I/O-advanced driver clocks; CPU profiling uses invocation telemetry','Tail is diagnostic, not exhaustive independent coverage','No automatic retries; failed and mixed-version populations retained']};await writeFile(r.token_file,randomBytes(32).toString('hex'),{mode:0o600});await save(r);console.log(join(dir,'pair.json'));process.exit(0);
}
const r=await json(path);assert.equal(r.schema,1);assert.equal(r.kind,'paired-http-diagnostic');assert.match(r.id,/^antfly-1278-driver-[a-z0-9]+-[a-f0-9]{4}$/);assert.equal(resolve(r.dir),resolve('dist/qualification',r.id));assert.equal(r.token_file,join(r.dir,'api-token'));assert.equal(r.driver_config,join(r.dir,'driver.json'));if(r.deleted&&action!=='cleanup')throw Error('TrialAlreadyDeleted');
const sides={baseline:await json(r.baseline),candidate:await json(r.candidate)};for(const s of Object.values(sides)){assert.equal(s.account,r.account);assert.match(s.id,/^antfly-1278-cold-[a-z0-9]+-[a-f0-9]{4}$/);assert.equal(s.token_file,join(s.dir,'api-token'));assert.equal(resolve(s.dir),resolve('dist/qualification',s.id));}
async function command(args,input){const result=await new Promise((ok,no)=>{const c=spawn(process.execPath,[cli,...args],{env:{...process.env,WRANGLER_SEND_METRICS:'false'},stdio:['pipe','pipe','pipe']});let output='';c.stdout.on('data',v=>output+=v);c.stderr.on('data',v=>output+=v);c.on('error',no);c.on('close',code=>ok({code,output}));c.stdin.end(input);});await appendFile(join(r.dir,'lifecycle.log'),result.output);if(result.code)throw Error('DriverCommandFailed:'+args[0]);return result.output;}
async function sideAction(side,op){await new Promise((ok,no)=>{const c=spawn(process.execPath,['qualification/cold-pilot.mjs',op,r[side]],{stdio:['ignore','pipe','pipe']});let output='';c.stdout.on('data',v=>output+=v);c.stderr.on('data',v=>output+=v);c.on('error',no);c.on('close',async code=>{await appendFile(join(r.dir,side+'-'+op+'.log'),output);code?no(Error('SideActionFailed:'+side+':'+op)):ok();});});sides[side]=await json(r[side]);}
async function driverCall(plan){await appendFile(join(r.dir,'driver-responses.jsonl'),JSON.stringify({event:'driver_attempt_started',at:new Date().toISOString(),side:plan.side,id:plan.id,version:plan.version,rounds:plan.rounds})+'\n');const started=performance.now();let result,status=null;try{const response=await fetch(new URL('/run',r.origin),{method:'POST',headers:{Authorization:'Bearer '+(await readFile(r.token_file,'utf8')).trim(),'Content-Type':'application/json'},body:JSON.stringify(plan),signal:AbortSignal.timeout(180000)});status=response.status;result=await response.json();if(response.status!==200)result={...result,passed:false,driver_status:response.status};}catch(error){result={passed:false,driver_status:status,error:error.name??'Error',ambiguous:true};}await appendFile(join(r.dir,'driver-responses.jsonl'),JSON.stringify({at:new Date().toISOString(),local_elapsed_ms:performance.now()-started,plan:{side:plan.side,id:plan.id,version:plan.version,rounds:plan.rounds,operations:plan.steps.map(s=>s.op)},result})+'\n');return result;}
async function state(side){const s=sides[side],response=await fetch(new URL('/_qualification/cold-state',s.origin),{method:'POST',headers:{Authorization:'Bearer '+(await readFile(s.token_file,'utf8')).trim()},body:'{}',signal:AbortSignal.timeout(60000)});assert.equal(response.status,200);return response.json();}
async function checkSeal(){const config=await readFile(r.driver_config);assert.equal(hash(config),r.driver_seal.config);for(const[name,expected]of Object.entries(r.driver_seal.files))assert.equal(hash(await readFile(join(r.dir,'bundle',name))),expected);}
async function installDriver(){
 assert.equal(r.reports.length,0,'DriverFrozenAfterFirstPhase');
 const config={name:r.id,account_id:r.account,main:resolve('qualification/http-trial-driver.mjs'),compatibility_date:'2026-10-23',compatibility_flags:['global_fetch_strictly_public'],workers_dev:true,vars:{BASELINE_ORIGIN:sides.baseline.origin,CANDIDATE_ORIGIN:sides.candidate.origin},limits:{cpu_ms:30000},version_metadata:{binding:'WORKER_VERSION'},observability:{enabled:true,head_sampling_rate:1}};await writeFile(r.driver_config,JSON.stringify(config,null,2));await command(['deploy','--dry-run','--config',r.driver_config,'--outdir',join(r.dir,'bundle')]);const files={};for(const name of await readdir(join(r.dir,'bundle')))if(name.endsWith('.js'))files[name]=hash(await readFile(join(r.dir,'bundle',name)));assert.equal(Object.keys(files).length,1);config.main=join(r.dir,'bundle',Object.keys(files)[0]);await writeFile(r.driver_config,JSON.stringify(config,null,2));r.driver_seal={config:hash(await readFile(r.driver_config)),files};await writeFile(join(r.dir,'sealed-driver.json'),JSON.stringify(config,null,2));const output=await command(['deploy','--no-bundle','--config',r.driver_config]);r.origin=/https:\/\/[^\s]+\.workers\.dev/.exec(output)?.[0];assert.ok(r.origin&&new URL(r.origin).hostname.startsWith(r.id+'.'));r.deployed=true;await save(r);
}
if(action==='deploy'){
 for(const side of['baseline','candidate'])await sideAction(side,'deploy');
 await installDriver();
}else if(action==='refresh-driver'){
 await installDriver();
}else if(action==='upload-tokens'){
 await checkSeal();for(const side of['baseline','candidate'])await sideAction(side,'upload-token');
 for(const[name,file]of[['API_TOKEN',r.token_file],['BASELINE_TOKEN',sides.baseline.token_file],['CANDIDATE_TOKEN',sides.candidate.token_file]]){const token=(await readFile(file,'utf8')).trim();assert.match(token,/^[a-f0-9]{64}$/);await command(['secret','put',name,'--config',r.driver_config],token);}r.tokens_installed=true;await save(r);
}else if(action==='seed'){
 for(const side of['baseline','candidate'])await sideAction(side,'seed');r.receipts=Object.fromEntries(Object.entries(sides).map(([side,s])=>[side,s.receipts]));r.latest={baseline:{},candidate:{}};await save(r);
}else if(action==='restart'){
 for(const side of['baseline','candidate'])await sideAction(side,'restart');r.restart_pending=true;await save(r);await new Promise(ok=>setTimeout(ok,10000));
}else if(action==='phase'){
 await checkSeal();const phase=candidatePath,reverse=process.argv.includes('--reverse'),order=reverse?['candidate','baseline']:['baseline','candidate'];assert.ok(['cold','warm','burst','mixed','verify','publish'].includes(phase));if(phase==='cold')assert.ok(r.restart_pending,'RestartRequired');const result={id:crypto.randomUUID(),phase,order,at:new Date().toISOString(),sides:{}};r.reports.push(result);await save(r);
 for(const side of order){
  const s=sides[side],rounds=['warm','burst','mixed'].includes(phase)?8:1;let steps=[];
  for(const tenant of['a','b','c','d']){
   const visibility=r.receipts[side][tenant];
   if(['cold','warm','burst','mixed'].includes(phase))for(let i=1;i<=(phase==='warm'?1:3);i++)steps.push({op:'lookup',tenant,id:i,visibility});
   if(phase==='mixed')steps.push({op:'mutate',tenant,sequence:r.reports.length*100});
   if(phase==='verify'){steps.push({op:'lookup',tenant,id:0,visibility,...(r.latest[side][tenant]!=null?{revision:r.latest[side][tenant]}:{})},{op:'count',tenant,visibility});}
   if(phase==='publish')steps.push({op:'publish',tenant});
  }
  const response=await driverCall({side,id:result.id,version:s.versions.app,rounds,steps});result.sides[side]=response;
  for(const sample of response.samples??[])if(sample.op==='mutate'&&sample.outcome==='ok'){r.receipts[side][sample.tenant]=sample.visibility;r.latest[side][sample.tenant]=sample.sequence;}
  if(phase==='cold'){
   const after=await state(side),before=s.before;response.actor_reconstruction=!!before&&after.states.every(a=>a.instance!==before.states.find(b=>b.tenant===a.tenant).instance);response.reader_reconstruction=!!before?.readers&&after.readers.every(a=>a.instance!==before.readers.find(b=>b.tenant===a.tenant&&b.slot===a.slot)?.instance);response.reader_versions_match=after.readers.every(a=>a.version===s.versions.reader);response.actor_versions_match=after.states.every(a=>a.version===s.versions.writer)&&after.version===s.versions.app;response.actor_state=after;response.passed=response.passed&&response.actor_reconstruction&&response.actor_versions_match&&response.reader_reconstruction&&response.reader_versions_match;
  }
  await save(r);
 }
 if(phase==='cold')r.restart_pending=false;result.passed=Object.values(result.sides).every(s=>s.passed);await save(r);console.log(JSON.stringify({phase,order,passed:result.passed,side_samples:Object.fromEntries(Object.entries(result.sides).map(([side,s])=>[side,s.samples?.length??0]))}));if(!result.passed)process.exitCode=1;
}else if(action==='tail'){
 const child=spawn(process.execPath,[cli,'tail',r.id,'--config',r.driver_config,'--format','json'],{env:{...process.env,WRANGLER_SEND_METRICS:'false'},stdio:['ignore','pipe','ignore']}),file=join(r.dir,'tail-driver.jsonl');let queue=Promise.resolve(),bytes=0;const parse=tailFrames(safe=>{const line=JSON.stringify({received_at:new Date().toISOString(),...safe})+'\n';bytes+=Buffer.byteLength(line);if(bytes>32*1024*1024){child.kill('SIGTERM');return;}queue=queue.then(()=>appendFile(file,line));});child.stdout.setEncoding('utf8');child.stdout.on('data',parse);await appendFile(file,JSON.stringify({connection_started:new Date().toISOString()})+'\n');for(const signal of['SIGINT','SIGTERM'])process.on(signal,()=>child.kill(signal));const exit_code=await new Promise((ok,no)=>{child.on('error',no);child.on('close',ok);});await queue;await appendFile(file,JSON.stringify({connection_ended:new Date().toISOString(),exit_code})+'\n');
}else if(action==='cleanup'){
 r.cleanup??={};if(!r.cleanup.driver){await command(['delete','--force','--config',r.driver_config]);r.cleanup.driver=true;await save(r);}for(const side of['baseline','candidate'])if(!r.cleanup[side]){await sideAction(side,'cleanup');r.cleanup[side]=true;await save(r);}await unlink(r.token_file).catch(e=>{if(e.code!=='ENOENT')throw e;});r.deleted=true;await save(r);
}else throw Error('UnknownPairedTrialAction');
