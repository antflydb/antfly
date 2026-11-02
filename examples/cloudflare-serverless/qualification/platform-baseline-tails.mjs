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

import{spawn}from'node:child_process';import{appendFile}from'node:fs/promises';import{resolve,join}from'node:path';import{tailFrames}from'./metrics-tail.mjs';import{resourceLogFailure}from'./resource-events.mjs';import{resourceFailureCode}from'../performance.mjs';
export async function baselineTails(run){
 if(!/^antfly-1278-floor-[a-z0-9]+-[a-f0-9]{4}$/.test(run.id)||run.services.length!==2||run.services.some(s=>![run.id,run.id+'-private'].includes(s.name)))throw Error('BaselineTailScopeInvalid');
 const sessions=[];for(const service of run.services){const file=join(run.dir,'tail-'+service.role+'.jsonl'),report={role:service.role,events:0,resource_failures:0,truncated:false,bytes:0,exit_code:null},child=spawn(process.execPath,[resolve('node_modules/wrangler/bin/wrangler.js'),'tail',service.name,'--config',service.config,'--format','json'],{env:{...process.env,CLOUDFLARE_ACCOUNT_ID:run.account,WRANGLER_SEND_METRICS:'false',WRANGLER_WRITE_LOGS:'false'},stdio:['ignore','pipe','pipe']});let writing=Promise.resolve(),failure=null;
 const parse=tailFrames(event=>{report.events++;if(event.truncated)report.truncated=true;if(['exceededMemory','exceededCpu'].includes(event.outcome))report.resource_failures++;for(const error of event.exceptions??[])if(resourceFailureCode(error.message))report.resource_failures++;for(const log of event.logs??[])for(const message of log.message??[])try{if(resourceLogFailure(JSON.parse(message)))report.resource_failures++;}catch{}const line=JSON.stringify({received_at:Date.now(),...event})+'\n';report.bytes+=Buffer.byteLength(line);if(report.bytes>32*1024*1024){report.truncated=true;child.kill('SIGTERM');return;}writing=writing.then(()=>appendFile(file,line)).catch(()=>{failure='BaselineTailJournalFailed';});});
 child.stdout.setEncoding('utf8');child.stdout.on('data',parse);child.stderr.resume();const ended=new Promise(ok=>{child.once('error',()=>{failure='BaselineTailProcessFailed';ok();});child.once('exit',code=>{report.exit_code=code;ok();});});sessions.push({child,ended,report,writing:()=>writing,failure:()=>failure});}
 await new Promise(r=>setTimeout(r,2000));return{async stop(){await new Promise(r=>setTimeout(r,2000));for(const session of sessions)session.child.kill('SIGTERM');await Promise.all(sessions.map(async s=>{const timer=setTimeout(()=>s.child.kill('SIGKILL'),5000);try{await s.ended;await s.writing();}finally{clearTimeout(timer);}}));return{exhaustive:false,qualification:false,sources:sessions.map(s=>({...s.report,failure:s.failure()})),resource_failures:sessions.reduce((n,s)=>n+s.report.resource_failures,0)};}};
}
