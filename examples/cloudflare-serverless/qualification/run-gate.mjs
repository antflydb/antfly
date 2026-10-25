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

import{readFile,writeFile,rename}from'node:fs/promises';
import{resolve}from'node:path';
import{spawn}from'node:child_process';
import{fileURLToPath}from'node:url';
import{GateClient}from'./gate-client.mjs';
import{evaluateTrial}from'./gate-policy.mjs';
const path=process.argv[2],trial=Number(process.argv[3]??1),seconds=Number(process.argv.find(v=>v.startsWith('--seconds='))?.slice(10)??3600),documents=512,period=30000;
if(!Number.isInteger(seconds)||seconds<60||seconds>7200)throw Error('seconds must be 60..7200');
const gate=JSON.parse(await readFile(path)),pair=gate.trials.find(t=>t.trial===trial);if(!pair)throw Error('trial missing');
const baseline=await GateClient.open(pair.baseline),candidate=await GateClient.open(pair.candidate),output=resolve(gate.dir,'trial-'+trial+(seconds<3600?'-smoke':'')+'.json');
const executionId=crypto.randomUUID();baseline.executionId=executionId;candidate.executionId=executionId;
let offered=0,fatal;
const report=()=>({schema:1,execution_id:executionId,trial,documents,offered_period_ms:period,soak_ms:offered,baseline:baseline.report(),candidate:candidate.report(),fatal,limitations:['Fresh physical actors do not prove complete platform isolate eviction.','Live R2 attempts and sampled memory are not billing or absolute memory peaks.']});
async function save(){const data=report();data.evaluation=evaluateTrial(data);await writeFile(output+'.tmp',JSON.stringify(data,null,2));await rename(output+'.tmp',output);}
async function lifecycle(action,id){await new Promise((resolve,reject)=>{const child=spawn(process.execPath,[fileURLToPath(new URL('./gate-lifecycle.mjs',import.meta.url)),path,action,id],{stdio:'inherit'});child.on('error',reject);child.on('exit',code=>code===0?resolve():reject(Error(action+' failed')));});}
try{
 for(const side of[baseline,candidate])if(side.run.paused)await lifecycle('resume',side.run.id);
 console.log('seeding',trial);await Promise.all([baseline.seed(documents),candidate.seed(documents)]);console.log('seeded',trial);
 const start=Date.now(),end=start+seconds*1000,rounds=new Set(),monitors=new Set();let index=0,minute=-1;
 while(Date.now()<end){
  const scheduled=start+index*period;if(Date.now()<scheduled){await new Promise(r=>setTimeout(r,Math.min(1000,scheduled-Date.now())));continue;}
  const order=(index+trial)%2?[candidate,baseline]:[baseline,candidate],round=index++;
  if(rounds.size>=4){for(const side of order)side.errors.push({phase:'client-admission',round,error:'fixed-arrival workload exceeded bounded pending rounds'});}
  else{const work=Promise.allSettled(order.map(side=>side.round(round,documents,scheduled))).then(results=>{for(const result of results)if(result.status==='rejected')console.error('round failed',trial,round,String(result.reason));}).finally(()=>rounds.delete(work));rounds.add(work);}
  const nextMinute=Math.floor((Date.now()-start)/60000);if(nextMinute!==minute){minute=nextMinute;const monitor=Promise.allSettled(order.map(side=>side.progress(minute))).finally(()=>monitors.delete(monitor));monitors.add(monitor);console.log('minute',trial,minute,'pending rounds',rounds.size);await save();}
 }
 offered=seconds*1000;await Promise.allSettled([...rounds,...monitors]);await Promise.all([baseline.converge(),candidate.converge()]);await Promise.all([baseline.verify(documents),candidate.verify(documents)]);await save();
 if(seconds>=3600||process.argv.includes('--cold')){
  await lifecycle('pause',candidate.run.id);await new Promise(r=>setTimeout(r,31000));
  for(let epoch=1;epoch<=100;epoch++){const tenant=['a','b','c','d'][(epoch-1)%4],{value,sample}=await candidate.call(tenant,{op:'count',required_visibility:candidate.receipts[tenant]},'cold',{epoch});candidate.cold.push({...sample,count:value.count});candidate.physicalEpochs??={};candidate.physicalEpochs[tenant]=epoch;if(epoch%10===0){console.log('cold',trial,epoch);await save();}}
  await candidate.verify(documents);
 }
}catch(error){fatal=String(error);console.error('gate trial failed',trial,fatal);}
await save();await writeFile(resolve(gate.dir,'trial-'+trial+'-'+executionId+'.json'),JSON.stringify(report(),null,2));console.log('saved',output,JSON.stringify(evaluateTrial(report())));if(fatal)process.exitCode=1;
