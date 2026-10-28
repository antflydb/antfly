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

// Real-provider contracts. Credentials stay in captured process output and the
// child environment; reports contain resource identities and hashes only.
import {execFile,spawn} from 'node:child_process';
import {promisify} from 'node:util';
import {readFile,writeFile,mkdir,copyFile} from 'node:fs/promises';
import {homedir} from 'node:os';
import {resolve,join,dirname} from 'node:path';
import {randomBytes,createHash} from 'node:crypto';
const exec=promisify(execFile),arg=name=>process.argv.find(v=>v.startsWith('--'+name+'='))?.slice(name.length+3);
const resumePath=arg('cleanup-only'),previous=resumePath?JSON.parse(await readFile(resolve(resumePath),'utf8')):null;
const provider=previous?.provider??arg('provider'),region=previous?.region??arg('region')??(provider==='gcs'?'us-central1':null),profile=arg('profile')??previous?.credential_profile;
if(!['gcs','s3'].includes(provider)||!region||!/^[a-z0-9-]+$/.test(region))throw Error('usage: provider-contracts.mjs --provider=gcs|s3 --region=REGION [--project=PROJECT] [--profile=PROFILE] [--out=DIRECTORY]');
const id=previous?.id??'antfly-1278-multi-'+Date.now().toString(36)+'-'+randomBytes(4).toString('hex'),dir=previous?dirname(resolve(resumePath)):resolve(arg('out')??'dist/qualification/provider-contracts',id),repo=resolve('../..');
if(!/^antfly-1278-multi-[a-z0-9]+-[a-f0-9]{8}$/.test(id)||previous&&(previous.schema!==1||!/^([a-f0-9]{32})$/.test(previous.resource_nonce??'')))throw Error('InvalidDisposableResourceIdentity');await mkdir(dir,{recursive:true});
const report=previous??{schema:1,resource_nonce:randomBytes(16).toString('hex'),credential_profile:profile??null,qualification:false,kind:'real-provider native transport/runtime contracts; not latency qualification',id,provider,region,owned:false,cleanup_complete:false,started_at:new Date().toISOString()};
const save=()=>writeFile(join(dir,'report.json'),JSON.stringify(report,null,2)+'\n');
let token,project=previous?.project??arg('project');const awsArgs=profile?['--profile',profile]:[];
const aws=async args=>{const{stdout}=await exec('aws',[...args,...awsArgs,'--region',region,'--output','json'],{maxBuffer:8*1024*1024,timeout:60000});return stdout.trim()?JSON.parse(stdout):null;};
async function refresh(){const{stdout}=await exec('gcloud',['auth','application-default','print-access-token'],{timeout:45000});token=stdout.trim();if(!token)throw Error('MissingGoogleAccessToken');}
async function gcs(path,{method='GET',body,missing=false}={}){
 const response=await fetch('https://storage.googleapis.com/storage/v1/'+path,{method,headers:{Authorization:'Bearer '+token,'x-goog-user-project':project,...(body?{'Content-Type':'application/json'}:{})},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(30000)});
 if(missing&&response.status===404)return null;
 if(!response.ok){await response.body?.cancel();throw Error('GcsOperationFailed:'+method+':'+response.status);}
 return response.status===204?null:response.json();
}
async function cleanup(){
 if(report.id!==id||!(report.owned||provider==='gcs'&&report.create_intent))return;
 if(provider==='gcs'){
  await refresh();const bucket=await gcs('b/'+id,{missing:true});if(!bucket){report.cleanup_complete=true;return;}
  if(bucket.labels?.purpose!=='antfly-1278-qualification'||bucket.labels?.disposable!=='true'||!report.resource_nonce||bucket.labels?.owner_nonce!==report.resource_nonce)throw Error('DisposableBucketIdentityMismatch');
  let cursor,objects=0,bytes=0;
  do{
   const page=await gcs('b/'+id+'/o?versions=true&maxResults=1000'+(cursor?'&pageToken='+encodeURIComponent(cursor):''));
   for(const item of page.items??[]){objects++;bytes+=Number(item.size??0);await gcs('b/'+id+'/o/'+encodeURIComponent(item.name)+'?generation='+encodeURIComponent(item.generation)+'&ifGenerationMatch='+encodeURIComponent(item.generation),{method:'DELETE'});}
   cursor=page.nextPageToken;
  }while(cursor);
  report.retained_before_cleanup={objects,bytes};await gcs('b/'+id,{method:'DELETE'});report.cleanup_complete=(await gcs('b/'+id,{missing:true}))===null;
 }else{
  const tags=await aws(['s3api','get-bucket-tagging','--bucket',id]);if(!report.resource_nonce||!tags?.TagSet?.some(t=>t.Key==='owner_nonce'&&t.Value===report.resource_nonce))throw Error('DisposableBucketIdentityMismatch');
  await exec('aws',['s3','rb','s3://'+id,'--force',...awsArgs,'--region',region],{timeout:120000,maxBuffer:1024*1024});report.cleanup_complete=true;
 }
}
if(resumePath){
 try{await cleanup();}catch(error){report.cleanup_error=/^DisposableBucketIdentityMismatch/.test(error.message)?error.message:String(error.code??error.name);process.exitCode=1;}
 if(!report.cleanup_complete){process.exitCode=1;report.cleanup_error??='OwnershipNotEstablished';}
 await save();console.log(JSON.stringify({report:join(dir,'report.json'),cleanup_complete:report.cleanup_complete}));
}else try{
 const env={...process.env,ZIG_GLOBAL_CACHE_DIR:resolve(repo,'.zig-cache/global')};
 const changed=(await exec('git',['diff','--name-only','HEAD'],{cwd:repo})).stdout.trim().split('\n');
 const untracked=(await exec('git',['ls-files','--others','--exclude-standard'],{cwd:repo})).stdout.trim().split('\n');
 report.source_files=[];
 for(const path of new Set([...changed,...untracked])){
  if(!/^(zig\/|docs\/design\/|examples\/cloudflare-serverless\/).+\.(zig|mjs|md)$/.test(path))continue;
  const bytes=await readFile(resolve(repo,path)),target=join(dir,'source',path);await mkdir(dirname(target),{recursive:true});await copyFile(resolve(repo,path),target);
  report.source_files.push({path,sha256:createHash('sha256').update(bytes).digest('hex')});
 }

 if(provider==='gcs'){
  if(!project){const adc=JSON.parse(await readFile(join(homedir(),'.config/gcloud/application_default_credentials.json'),'utf8'));project=adc.quota_project_id;}
  if(!project)throw Error('MissingGoogleProject');report.project=project;await refresh();
  report.create_intent=true;await save();
  await gcs('b?project='+encodeURIComponent(project),{method:'POST',body:{name:id,location:region.toUpperCase(),storageClass:'STANDARD',iamConfiguration:{uniformBucketLevelAccess:{enabled:true}},softDeletePolicy:{retentionDurationSeconds:'0'},labels:{purpose:'antfly-1278-qualification',disposable:'true',owner_nonce:report.resource_nonce}}});
  report.owned=true;await save();Object.assign(env,{GCS_BEARER_TOKEN:token,GOOGLE_CLOUD_PROJECT:project,OBJECTSTORE_GCS_INTEGRATION:'1',OBJECTSTORE_GCS_TEST_BUCKET:id,ANTFLY_PROVIDER_LIVE:'1',ANTFLY_QUALIFY_STORAGE_URI:'gs://'+id+'/runtime'});
 }else{
  const create=['s3api','create-bucket','--bucket',id];if(region!=='us-east-1')create.push('--create-bucket-configuration',JSON.stringify({LocationConstraint:region}));report.create_intent=true;await save();await aws(create);report.owned=true;await save();
  await aws(['s3api','put-bucket-tagging','--bucket',id,'--tagging',JSON.stringify({TagSet:[{Key:'purpose',Value:'antfly-1278-qualification'},{Key:'owner_nonce',Value:report.resource_nonce}]})]);
  await aws(['s3api','put-public-access-block','--bucket',id,'--public-access-block-configuration',JSON.stringify({BlockPublicAcls:true,IgnorePublicAcls:true,BlockPublicPolicy:true,RestrictPublicBuckets:true})]);
  const{stdout}=await exec('aws',['configure','export-credentials',...awsArgs,'--format','process'],{timeout:45000});const credentials=JSON.parse(stdout);
  Object.assign(env,{AWS_ACCESS_KEY_ID:credentials.AccessKeyId,AWS_SECRET_ACCESS_KEY:credentials.SecretAccessKey,...(credentials.SessionToken?{AWS_SESSION_TOKEN:credentials.SessionToken}:{}),AWS_REGION:region,OBJECTSTORE_S3_INTEGRATION:'1',OBJECTSTORE_S3_TEST_BUCKET:id,ANTFLY_PROVIDER_LIVE:'1',ANTFLY_QUALIFY_STORAGE_URI:'s3://'+id+'/runtime'});
 }
 const args=['build','lib-objectstore-test','antfly-multiprovider-test','--summary','all'],zig=arg('zig')??'zig';
 report.source_revision=(await exec('git',['rev-parse','HEAD'],{cwd:repo})).stdout.trim();report.command={executable:zig,args};
 let log='';await new Promise((resolveRun,reject)=>{const child=spawn(zig,args,{cwd:resolve(repo,'zig'),env,stdio:['ignore','pipe','pipe']});child.stdout.on('data',v=>log+=v);child.stderr.on('data',v=>log+=v);const timer=setTimeout(()=>child.kill('SIGTERM'),15*60*1000);child.on('error',error=>{clearTimeout(timer);reject(error);});child.on('exit',async code=>{clearTimeout(timer);await writeFile(join(dir,'native.log'),log);report.exit_code=code;code===0?resolveRun():reject(Error('ProviderContractsFailed:'+code));});});
 report.log_sha256=createHash('sha256').update(log).digest('hex');
 report.native_binaries=[];
 for(const match of log.matchAll(/failed command: (\.\/\.zig-cache\/o\/[a-f0-9]+\/test(?:-local)?)(?: |\n)/g)){
  if(report.native_binaries.some(b=>b.path===match[1]))continue;
  const binary=await readFile(resolve(repo,'zig',match[1]));report.native_binaries.push({path:match[1],bytes:binary.length,sha256:createHash('sha256').update(binary).digest('hex')});
 }
 for(const source of report.source_files)if(createHash('sha256').update(await readFile(resolve(repo,source.path))).digest('hex')!==source.sha256)throw Error('QualificationSourceChanged');

 report.passed=/multiprovider live runtime[^\n]*\.\.\.OK/.test(log)&&!/[1-9][0-9]* failed;|FAIL|leaked[^.]/.test(log);
 if(!report.passed)throw Error('LiveRuntimeEvidenceMissingOrFailed');
}catch(error){report.error=(/^(QualificationSourceChanged|ProviderContractsFailed|LiveRuntimeEvidenceMissingOrFailed|GcsOperationFailed|DisposableBucketIdentityMismatch|MissingGoogle)/.test(error.message)?error.message.slice(0,300):String(error.code??error.name));process.exitCode=1;}
finally{
 try{await cleanup();}catch(error){report.cleanup_error=(/^(QualificationSourceChanged|ProviderContractsFailed|LiveRuntimeEvidenceMissingOrFailed|GcsOperationFailed|DisposableBucketIdentityMismatch|MissingGoogle)/.test(error.message)?error.message.slice(0,300):String(error.code??error.name));process.exitCode=1;}
 report.finished_at=new Date().toISOString();await save();console.log(JSON.stringify({report:join(dir,'report.json'),passed:report.passed??false,cleanup_complete:report.cleanup_complete,error:report.error??report.cleanup_error??null}));
}
