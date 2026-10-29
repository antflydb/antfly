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

// Owned diagnostic lifecycle. No raw provider logs or credentials reach disk.
import{readFile,writeFile,mkdir,readdir,appendFile,unlink}from'node:fs/promises';import{resolve,dirname,basename}from'node:path';import{fileURLToPath}from'node:url';import{spawn,execFileSync}from'node:child_process';import{createHash,randomBytes}from'node:crypto';import{reconcileOtel}from'./otel-evidence.mjs';import{readCounterToken,counterFailureDiagnostic}from'./independent-counters.mjs';
const cwd=fileURLToPath(new URL('../',import.meta.url)),cli=resolve(cwd,'node_modules/wrangler/bin/wrangler.js'),base='antfly-1278-telemetry-';
const json=async path=>JSON.parse(await readFile(path)),save=(path,value)=>writeFile(path,JSON.stringify(value,null,2),{mode:0o600});
async function prepare(gatePath,otel=false){
 const gate=await json(gatePath);if(!/^[a-f0-9]{32}$/.test(gate.account??''))throw Error('TelemetryAccountInvalid');
 if(execFileSync('git',['status','--porcelain'],{cwd,encoding:'utf8'}).trim())throw Error('TelemetryCleanSourceRequired');
 const sourceRevision=execFileSync('git',['rev-parse','HEAD'],{cwd,encoding:'utf8'}).trim();
 const tag=Date.now().toString(36)+randomBytes(2).toString('hex'),id=base+tag,dir=resolve(cwd,'dist/qualification/logpush-probe-'+tag);await mkdir(dir,{recursive:true});
 const run={schema:1,qualification:false,source_revision:sourceRevision,mode:otel?'otel':'logpush',source_authentication:true,id,account:gate.account,dir,source:id+'-source',collector:id+'-collector',bucket:id,capture_id:crypto.randomUUID(),token_file:resolve(dir,'api-token'),observability_token_file:resolve(cwd,'dist/qualification/gate-mvslyh876561/observability-token'),created_at:Date.now(),deployed:false,token_installed:false,deleted:false};
 await writeFile(run.token_file,randomBytes(32).toString('hex'),{mode:0o600});
 const common={account_id:run.account,compatibility_date:'2026-10-23'};
 await save(resolve(dir,'source.json'),{...common,name:run.source,main:resolve(cwd,'qualification/logpush-probe-source.mjs'),workers_dev:false,logpush:true,observability:{enabled:true,head_sampling_rate:1},durable_objects:{bindings:[{name:'PROBES',class_name:'Probe'}]},migrations:[{tag:'v1',new_sqlite_classes:['Probe']}]});
 if(otel){const c=await json(resolve(dir,'source.json'));c.observability={enabled:true,head_sampling_rate:1,logs:{enabled:true,head_sampling_rate:1,invocation_logs:true,persist:true},traces:{enabled:true,head_sampling_rate:1,persist:true}};await save(resolve(dir,'source.json'),c);}
 await save(resolve(dir,'collector.json'),{...common,name:run.collector,main:resolve(cwd,'qualification/logpush-probe-collector.mjs'),workers_dev:true,logpush:false,observability:{enabled:false},r2_buckets:[{binding:'LOGS',bucket_name:run.bucket}],services:[{binding:'FIXTURE',service:run.source}],vars:{SOURCE_WORKER:run.source,CAPTURE_ID:run.capture_id}});
 const proposal={name:id,dataset:'workers_trace_events',enabled:false,filter:JSON.stringify({where:{key:'ScriptName',operator:'eq',value:run.source}}),output_options:{field_names:['Event','EventTimestampMs','EventType','Exceptions','Logs','Outcome','ScriptName','ScriptVersion','Entrypoint','CPUTimeMs','WallTimeMs'],sample_rate:1,timestamp_format:'rfc3339'},max_upload_bytes:5000000,max_upload_records:1000,max_upload_interval_seconds:30};
 await save(resolve(dir,'job-proposal.json'),proposal);await save(resolve(dir,'run.json'),run);console.log(JSON.stringify({manifest:resolve(dir,'run.json'),source:run.source,collector:run.collector,bucket:run.bucket,qualification:false}));
}
async function openRun(path){
 const run=await json(path),tag=run.id?.slice(base.length);
 if(run.origin){let origin;try{origin=new URL(run.origin);}catch{throw Error('TelemetryOriginMismatch');}if(origin.protocol!=='https:'||!origin.hostname.startsWith(run.collector+'.')||!origin.hostname.endsWith('.workers.dev')||origin.username||origin.password||origin.port||origin.pathname!=='/'||origin.search||origin.hash)throw Error('TelemetryOriginMismatch');}

 if(run.job_id!=null&&(!Number.isSafeInteger(run.job_id)||run.job_id<1))throw Error('TelemetryJobIdentityInvalid');
 if(run.schema!==1||!/^[a-z0-9]+$/.test(tag??'')||run.id!==base+tag||run.source!==run.id+'-source'||run.collector!==run.id+'-collector'||run.bucket!==run.id||!/^[a-f0-9]{32}$/.test(run.account??'')||resolve(path)!==resolve(run.dir,'run.json')||basename(run.dir)!=='logpush-probe-'+tag||resolve(run.token_file)!==resolve(run.dir,'api-token')||!run.dir.startsWith(resolve(cwd,'dist/qualification')+'/'))throw Error('TelemetryScopeMismatch');
 for(const role of ['source','collector']){const c=await json(resolve(run.dir,role+'.json'));if(c.name!==run[role]||c.account_id!==run.account)throw Error('TelemetryConfigScopeMismatch');if(role==='collector'&&(c.r2_buckets?.length!==1||c.r2_buckets[0].bucket_name!==run.bucket||(c.vars?.RETIRE_MODE==='true'?c.services?.length!==0:c.services?.length!==1||c.services[0].service!==run.source)||c.logpush!==(run.parent_logpush_probe===true)))throw Error('TelemetryConfigScopeMismatch');}
 return run;
}
async function command(run,args,input,{allowExists=false,allowMissing=false}={}){
 let token='';try{token=(await readFile(run.token_file,'utf8')).trim();}catch{}
 const result=await new Promise((ok,bad)=>{const child=spawn(process.execPath,[cli,...args],{cwd,env:{...process.env,WRANGLER_SEND_METRICS:'false',WRANGLER_LOG:'log',WRANGLER_WRITE_LOGS:'false'},stdio:['pipe','pipe','pipe']});let output='';child.stdout.on('data',p=>output+=p);child.stderr.on('data',p=>output+=p);child.on('error',bad);child.on('close',code=>ok({code,output}));child.stdin.end(input);});
 let safe=result.output;if(token)safe=safe.replaceAll(token,'[REDACTED]');safe=safe.replace(/Bearer [A-Za-z0-9._-]+/g,'Bearer [REDACTED]');await appendFile(resolve(run.dir,'lifecycle.log'),safe);
 if(result.code!==0&&!(allowExists&&/already exists/i.test(safe))&&!(allowMissing&&/does not exist|not found|could not find/i.test(safe)))throw Error('TelemetryLifecycleCommandFailed');return safe;
}
async function api(run,path,method='GET',body){
 const token=(await readFile(run.observability_token_file,'utf8')).trim();if(!/^[A-Za-z0-9_-]{20,256}$/.test(token))throw Error('TelemetryAccountCredentialInvalid');
 const response=await fetch('https://api.cloudflare.com/client/v4/accounts/'+run.account+'/'+path,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});
 const value=await response.json();if(!response.ok||value.success!==true){await save(resolve(run.dir,'api-failure-'+Date.now()+'.json'),{path,method,status:response.status,error_codes:(value.errors??[]).map(e=>e.code).filter(Number.isSafeInteger)});throw Object.assign(Error('TelemetryApiStatus'+response.status),{status:response.status});}return value.result;
}
async function verify(run){
 if(!run.bundle_hashes)throw Error('TelemetrySealRequired');
 for(const role of ['source','collector']){const config=await json(resolve(run.dir,role+'.json')),dir=resolve(run.dir,run.bundle_dirs?.[role]??'bundle-'+role);if(dirname(config.main)!==dir||!run.bundle_hashes[role]?.[basename(config.main)])throw Error('TelemetryBundleSelectionChanged');for(const [name,sha]of Object.entries(run.bundle_hashes[role]))if(basename(name)!==name||createHash('sha256').update(await readFile(resolve(dir,name))).digest('hex')!==sha)throw Error('TelemetryBundleChanged');}
}
async function collector(run,path,body={}){
 const token=(await readFile(run.token_file,'utf8')).trim(),response=await fetch(new URL(path,run.origin),{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(60000)});const value=await response.json();if(!response.ok)throw Error('TelemetryCollectorStatus'+response.status);return value;
}
async function main(){
 const[action,path]=process.argv.slice(2);if(action==='prepare'||action==='prepare-otel')return prepare(path,action==='prepare-otel');if(!['seal','revise-collector','revise-source','expose-source','exercise-public','enable-parent-logpush','enable-parent-tracing','deploy','install-token','create-job','create-otel','exercise','read','status','status-otel','analyze-otel','cleanup'].includes(action))throw Error('TelemetryActionInvalid');
 const run=await openRun(path);if(run.deleted){if(action==='cleanup')return;throw Error('TelemetryAlreadyDeleted');}const config=role=>resolve(run.dir,role+'.json');
 if(action==='seal'){
  run.bundle_hashes={};for(const role of ['source','collector']){const out=resolve(run.dir,'bundle-'+role);await mkdir(out);await command(run,['deploy','--dry-run','--config',config(role),'--outdir',out]);const hashes={};for(const name of await readdir(out))if(name.endsWith('.js'))hashes[name]=createHash('sha256').update(await readFile(resolve(out,name))).digest('hex');if(Object.keys(hashes).length!==1)throw Error('TelemetryBundleAmbiguous');run.bundle_hashes[role]=hashes;const c=await json(config(role));c.main=resolve(out,Object.keys(hashes)[0]);await save(config(role),c);}await save(path,run);return;
 }
 await verify(run);
 if(action==='revise-collector'||action==='revise-source'){
  const role=action==='revise-source'?'source':'collector';
  if(execFileSync('git',['status','--porcelain'],{cwd,encoding:'utf8'}).trim())throw Error('TelemetryCleanSourceRequired');
  const revision=execFileSync('git',['rev-parse','HEAD'],{cwd,encoding:'utf8'}).trim(),selected='bundle-'+role+'-'+revision.slice(0,12),out=resolve(run.dir,selected),build=resolve(run.dir,role+'-build-'+revision.slice(0,12)+'.json'),c=await json(config(role));
  await mkdir(out);await save(build,{...c,main:resolve(cwd,'qualification/logpush-probe-'+role+'.mjs')});await command(run,['deploy','--dry-run','--config',build,'--outdir',out]);const hashes={};for(const name of await readdir(out))if(name.endsWith('.js'))hashes[name]=createHash('sha256').update(await readFile(resolve(out,name))).digest('hex');if(Object.keys(hashes).length!==1)throw Error('TelemetryBundleAmbiguous');
  run.bundle_history??=[];run.bundle_history.push({at:Date.now(),source_revision:run.source_revision,bundle_hashes:structuredClone(run.bundle_hashes),bundle_dirs:structuredClone(run.bundle_dirs??{source:'bundle-source',collector:'bundle-collector'}),versions:structuredClone(run.versions)});run.bundle_dirs??={source:'bundle-source',collector:'bundle-collector'};run.bundle_dirs[role]=selected;run.bundle_hashes[role]=hashes;run.source_revision=revision;c.main=resolve(out,Object.keys(hashes)[0]);if(role==='collector')c.version_metadata={binding:'WORKER_VERSION'};await save(config(role),c);await save(path,run);
  const output=await command(run,['deploy','--no-bundle','--config',config(role)]),version=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];if(!version)throw Error('TelemetryVersionMissing');run.versions[role]=version;if(role==='source')run.source_authentication=true;await save(path,run);return;
 }
 if(action==='deploy'){
  await command(run,['r2','bucket','create',run.bucket,'--config',config('collector')],undefined,{allowExists:true});run.versions={};for(const role of ['source','collector']){const output=await command(run,['deploy','--no-bundle','--config',config(role)]);const version=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];if(!version)throw Error('TelemetryVersionMissing');run.versions[role]=version;if(role==='collector'){run.origin=/https:\/\/[^\s]+\.workers\.dev/.exec(output)?.[0];if(!run.origin||!new URL(run.origin).hostname.startsWith(run.collector+'.'))throw Error('TelemetryOriginMismatch');}await save(path,run);}run.deployed=true;await save(path,run);return;
 }
 if(action==='install-token'){
  if(!run.deployed)throw Error('TelemetryDeployRequired');const token=(await readFile(run.token_file,'utf8')).trim();if(!/^[a-f0-9]{64}$/.test(token))throw Error('TelemetryDisposableTokenInvalid');await command(run,['secret','put','API_TOKEN','--config',config('source')],token);await command(run,['secret','put','API_TOKEN','--config',config('collector')],token);let ready=false;for(let n=0;n<120;n++){try{const response=await fetch(new URL('/report',run.origin),{method:'POST',headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(10000)});if(response.ok&&(await response.json()).capture_id===run.capture_id){ready=true;break;}}catch{}await new Promise(r=>setTimeout(r,500));}if(!ready)throw Error('TelemetryTokenDidNotConverge');run.token_installed=true;await save(path,run);return;
 }
 if(action==='create-job'){
  if(!run.token_installed||run.job_id)throw Error('TelemetryJobPrecondition');const proposal=await json(resolve(run.dir,'job-proposal.json'));
  if(proposal.dataset!=='workers_trace_events'||proposal.output_options?.sample_rate!==1||proposal.filter!==JSON.stringify({where:{key:'ScriptName',operator:'eq',value:run.source}}))throw Error('TelemetryJobScopeMismatch');
  const url=new URL('/ingest',run.origin);url.search='header_Authorization='+encodeURIComponent('Bearer '+(await readFile(run.token_file,'utf8')).trim());const result=await api(run,'logpush/jobs','POST',{...proposal,enabled:true,destination_conf:url.toString()});if(!Number.isSafeInteger(result.id))throw Error('TelemetryJobIdentityMissing');run.job_id=result.id;run.job_enabled=true;await save(path,run);console.log(JSON.stringify({job_id:run.job_id,qualification:false}));return;
 }
 if(action==='expose-source'){
  if(run.mode!=='otel'||!run.token_installed||run.source_origin||run.source_authentication!==true)throw Error('TelemetryPublicProbePrecondition');const token=(await readFile(run.token_file,'utf8')).trim();await command(run,['secret','put','API_TOKEN','--config',config('source')],token);const c=await json(config('source'));c.workers_dev=true;await save(config('source'),c);const output=await command(run,['deploy','--no-bundle','--config',config('source')]),version=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1],origin=/https:\/\/[^\s]+\.workers\.dev/.exec(output)?.[0];if(!version||!origin||!new URL(origin).hostname.startsWith(run.source+'.'))throw Error('TelemetrySourceEndpointInvalid');run.versions.source=version;run.source_origin=origin;run.public_source_at=Date.now();await save(path,run);return;
 }
 if(action==='exercise-public'){
  if(!run.source_origin)throw Error('TelemetryPublicProbePrecondition');const origin=new URL(run.source_origin);if(origin.protocol!=='https:'||!origin.hostname.startsWith(run.source+'.')||!origin.hostname.endsWith('.workers.dev')||origin.username||origin.password||origin.port||origin.pathname!=='/'||origin.search||origin.hash)throw Error('TelemetrySourceEndpointInvalid');
  const token=(await readFile(run.token_file,'utf8')).trim(),expected=[];for(let i=0;i<8;i++){const id=crypto.randomUUID(),url=new URL(origin);url.searchParams.set('nonce',id);const response=await fetch(url,{method:'POST',headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('TelemetryPublicProbeStatus'+response.status);const value=await response.json();if(value.id!==id||!/^[a-f0-9-]{36}$/.test(value.rpc??''))throw Error('TelemetryPublicProbeIdentityInvalid');expected.push(value);}await save(resolve(run.dir,'exercise-'+Date.now()+'.json'),{qualification:false,kind:'public',expected,source_version:run.versions.source});console.log(JSON.stringify({calls:expected.length,qualification:false}));return;
 }
 if(action==='create-otel'){
  if(run.mode!=='otel'||!run.token_installed)throw Error('TelemetryOtelPrecondition');run.destinations??={};
  for(const signal of ['traces','logs']){if(run.destinations[signal])continue;const name=run.id+'-'+signal,result=await api(run,'workers/observability/destinations','POST',{name,enabled:true,skipPreflightCheck:true,configuration:{type:'logpush',logpushDataset:'opentelemetry-'+signal,url:new URL('/v1/'+signal,run.origin).toString(),headers:{Authorization:'Bearer '+(await readFile(run.token_file,'utf8')).trim()}}});if(typeof result.slug!=='string'||!/^[-a-z0-9]+$/.test(result.slug))throw Error('TelemetryDestinationIdentityInvalid');run.destinations[signal]={slug:result.slug,name};await save(path,run);}
  const c=await json(config('source'));for(const signal of ['logs','traces'])c.observability[signal].destinations=[run.destinations[signal].slug];await save(config('source'),c);const output=await command(run,['deploy','--no-bundle','--config',config('source')]),version=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];if(!version)throw Error('TelemetryVersionMissing');run.versions.source=version;run.otel_enabled_at=Date.now();await save(path,run);return;
 }
 if(action==='enable-parent-tracing'){
  if(run.mode!=='otel'||run.parent_tracing)throw Error('TelemetryParentProbePrecondition');const c=await json(config('collector'));run.parent_tracing={at:Date.now(),previous_version:run.versions.collector};c.observability={enabled:true,head_sampling_rate:1,logs:{enabled:false},traces:{enabled:true,head_sampling_rate:1,persist:false}};await save(config('collector'),c);await save(path,run);const output=await command(run,['deploy','--no-bundle','--config',config('collector')]),version=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];if(!version)throw Error('TelemetryVersionMissing');run.versions.collector=version;await save(path,run);return;
 }
 if(action==='enable-parent-logpush'){
  if(!run.job_id||run.parent_logpush_probe)throw Error('TelemetryParentProbePrecondition');const job=await api(run,'logpush/jobs/'+run.job_id);if(job.name!==run.id||job.dataset!=='workers_trace_events'||job.filter!==JSON.stringify({where:{key:'ScriptName',operator:'eq',value:run.source}}))throw Error('TelemetryJobScopeMismatch');
  const c=await json(config('collector'));run.parent_logpush_history={at:Date.now(),previous_version:run.versions.collector,previous_logpush:c.logpush};run.parent_logpush_probe=true;c.logpush=true;await save(config('collector'),c);await save(path,run);const output=await command(run,['deploy','--no-bundle','--config',config('collector')]),version=/Current Version ID: ([a-f0-9-]{36})/.exec(output)?.[1];if(!version)throw Error('TelemetryVersionMissing');run.versions.collector=version;await save(path,run);return;
 }
 if(action==='status-otel'){
  if(run.mode!=='otel')throw Error('TelemetryOtelPrecondition');const all=await api(run,'workers/observability/destinations'),destinations=[];
  for(const [signal,expected]of Object.entries(run.destinations??{})){const d=all.find(d=>d.slug===expected.slug);if(!d||d.name!==expected.name||d.configuration?.logpushDataset!=='opentelemetry-'+signal)throw Error('TelemetryDestinationScopeMismatch');destinations.push({signal,slug:d.slug,name:d.name,enabled:d.enabled,scripts:d.scripts,last_complete:d.configuration?.jobStatus?.last_complete,last_error:d.configuration?.jobStatus?.last_error,has_error:!!d.configuration?.jobStatus?.error_message});}
  const report={at:Date.now(),qualification:false,destinations};await save(resolve(run.dir,'otel-status-'+report.at+'.json'),report);console.log(JSON.stringify(report));return;
 }
 if(action==='status'){
  if(!run.job_id)throw Error('TelemetryJobRequired');const job=await api(run,'logpush/jobs/'+run.job_id);if(job.name!==run.id||job.dataset!=='workers_trace_events')throw Error('TelemetryJobScopeMismatch');
  const status={at:Date.now(),qualification:false,job:{id:job.id,enabled:job.enabled,name:job.name,dataset:job.dataset,filter:job.filter,output_options:job.output_options,max_upload_interval_seconds:job.max_upload_interval_seconds,last_complete:job.last_complete,last_error:job.last_error,has_error_message:!!job.error_message}};
  const token=await readCounterToken(),from=new Date(run.created_at).toISOString(),to=new Date().toISOString(),query=`{viewer{accounts(filter:{accountTag:"${run.account}"}){logpushHealthAdaptiveGroups(limit:100,filter:{jobId:${run.job_id},datetime_geq:"${from}",datetime_leq:"${to}"}){count avg{sampleInterval} sum{records uploads bytes bytesCompressed} dimensions{jobId status success final}}}}}`;
  try{const response=await fetch('https://api.cloudflare.com/client/v4/graphql',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify({query}),signal:AbortSignal.timeout(30000)}),value=await response.json();status.health={from,to,...(!response.ok||value.errors?.length?{failure:counterFailureDiagnostic(value,response.status)}:{rows:value.data?.viewer?.accounts?.[0]?.logpushHealthAdaptiveGroups??null})};}catch{status.health={from,to,failure:{kind:'transport'}};}
  await save(resolve(run.dir,'status-'+status.at+'.json'),status);console.log(JSON.stringify(status));return;
 }
 if(action==='analyze-otel'){
  const files=(await readdir(run.dir)).filter(n=>/^capture-\d+\.json$/.test(n)).sort(),capture=await json(resolve(run.dir,files.at(-1))),rows=capture.objects.flatMap(o=>o.value.rows??[]),plans=[];
  for(const name of (await readdir(run.dir)).filter(n=>/^exercise-\d+\.json$/.test(n)).sort()){const plan=await json(resolve(run.dir,name)),fixtureIds=new Set(plan.expected.flatMap(p=>[p.id,p.rpc])),logs=rows.filter(r=>r.signal==='logs'&&r.probes.some(p=>fixtureIds.has(p.id))),ids=new Set(logs.map(l=>l.invocation_id_sha256).filter(Boolean)),spanKeys=new Set(logs.filter(l=>l.trace_id_sha256&&l.span_id_sha256).map(l=>l.trace_id_sha256+'/'+l.span_id_sha256)),selected=rows.filter(r=>logs.includes(r)||r.invocation_id_sha256&&ids.has(r.invocation_id_sha256)||spanKeys.has(r.trace_id_sha256+'/'+r.span_id_sha256));plans.push({file:name,...reconcileOtel(selected,plan.expected,{version:run.versions.source})});}
  const report={at:Date.now(),qualification:false,plans,signals:Object.fromEntries(['traces','logs'].map(s=>[s,rows.filter(r=>r.signal===s).length])),terminal_schemas:rows.filter(r=>r.terminal).slice(0,4)};await save(resolve(run.dir,'otel-analysis-'+report.at+'.json'),report);console.log(JSON.stringify({...report,terminal_schemas:report.terminal_schemas.map(r=>({handler:r.handler,version_id:r.version_id,provider_id:!!r.invocation_id_sha256,attributes:r.attribute_keys,outcome:r.outcome}))}));return;
 }
 if(action==='exercise'){const value=await collector(run,'/exercise',{count:8});await save(resolve(run.dir,'exercise-'+Date.now()+'.json'),value);console.log(JSON.stringify({calls:value.expected.length,qualification:false}));return;}
 if(action==='read'){const value=await collector(run,'/report');await save(resolve(run.dir,'capture-'+Date.now()+'.json'),value);const rows=value.objects.flatMap(o=>o.value.rows??[]);console.log(JSON.stringify({collector_version:value.collector_version,objects:value.objects.length,records:rows.length,event_types:rows.reduce((o,r)=>(o[r.event_type??r.handler]=(o[r.event_type??r.handler]??0)+1,o),{}),qualification:false}));return;}
 if(action==='cleanup'){
  run.cleanup_steps??={};const step=async(name,fn)=>{if(run.cleanup_steps[name])return;await fn();run.cleanup_steps[name]=true;await save(path,run);};
  if(run.job_id)await step('delete-job',async()=>{let job;try{job=await api(run,'logpush/jobs/'+run.job_id);}catch(error){if(error.status===404)return;throw error;}let filter;try{filter=typeof job.filter==='string'?JSON.parse(job.filter):job.filter;}catch{}if(job.name!==run.id||job.dataset!=='workers_trace_events'||filter?.where?.key!=='ScriptName'||filter.where.operator!=='eq'||filter.where.value!==run.source||Object.keys(filter).length!==1||Object.keys(filter.where).length!==3)throw Error('TelemetryCleanupJobScopeMismatch');try{await api(run,'logpush/jobs/'+run.job_id,'DELETE');}catch(error){if(error.status!==404)throw error;}});
  await step('retire-collector',async()=>{const c=await json(config('collector'));c.vars.RETIRE_MODE='true';c.services=[];await save(config('collector'),c);await command(run,['deploy','--no-bundle','--config',config('collector')]);});
  await step('delete-source',()=>command(run,['delete','--force','--config',config('source')],undefined,{allowMissing:true}));
  if(run.destinations)for(const [signal,destination]of Object.entries(run.destinations))await step('delete-destination-'+signal,async()=>{if(!['traces','logs'].includes(signal)||destination.name!==run.id+'-'+signal||!/^[-a-z0-9]+$/.test(destination.slug))throw Error('TelemetryDestinationScopeMismatch');const all=await api(run,'workers/observability/destinations'),d=all.find(d=>d.slug===destination.slug);if(!d)return;if(d.name!==destination.name||d.configuration?.logpushDataset!=='opentelemetry-'+signal||!Array.isArray(d.scripts)||d.scripts.some(s=>s!==run.source))throw Error('TelemetryDestinationScopeMismatch');await api(run,'workers/observability/destinations/'+destination.slug,'DELETE');});
  await step('quiescence',()=>new Promise(r=>setTimeout(r,31000)));
  await step('drain',async()=>{for(let n=0;n<1000;n++)if((await collector(run,'/drain')).done)return;throw Error('TelemetryDrainBudgetExceeded');});
  await step('delete-bucket',()=>command(run,['r2','bucket','delete',run.bucket,'--config',config('collector')],undefined,{allowMissing:true}));
  await step('delete-collector',()=>command(run,['delete','--force','--config',config('collector')],undefined,{allowMissing:true}));
  await unlink(run.token_file).catch(error=>{if(error.code!=='ENOENT')throw error;});run.deleted=true;await save(path,run);return;
 }
}
await main();
