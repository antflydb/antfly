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

// Exploratory fixed-arrival staircase. Its output cannot qualify the soak gate.
import{readFile,writeFile,rename}from'node:fs/promises';
import{resolve}from'node:path';
import{GateClient}from'./gate-client.mjs';
import{runOfferedWorkload}from'./offered-workload.mjs';
import{summarizeWorkload}from'./workload-summary.mjs';
import{policy}from'./gate-policy.mjs';
import{assertUnusedRecovery}from'./gate-provenance.mjs';
const option=(name,fallback)=>process.argv.find(v=>v.startsWith('--'+name+'='))?.split('=')[1]??fallback;
const gate=JSON.parse(await readFile(process.argv[2])),trial=Number(process.argv[3]??1),seconds=Number(option('seconds',600)),documents=Number(option('documents',512)),periods=option('periods','30000,15000,7500,3750').split(',').map(Number);
if(!Number.isInteger(seconds)||seconds<60||seconds>7200||!Number.isInteger(documents)||documents<512||documents>100000||!periods.length||periods.length>12||periods.some(p=>!Number.isInteger(p)||p<1000||p>60000))throw Error('capacity limits: seconds 60..7200, documents 512..100000, 1..12 periods 1000..60000 ms');
const pair=gate.trials.find(t=>t.trial===trial);if(!pair)throw Error('trial missing');
const sides=await Promise.all([GateClient.open(pair.baseline),GateClient.open(pair.candidate)]);
for(const side of sides)assertUnusedRecovery(side.run);
if(sides.some(side=>side.run.paused))throw Error('resume both profiles before capacity exploration');
const executionId=crypto.randomUUID(),output=resolve(gate.dir,'capacity-'+trial+'-'+executionId+'.json'),result={schema:1,kind:'capacity-exploration',qualified:false,execution_id:executionId,trial,documents,stages:[],limitations:['Short staircase stages do not qualify the hour-long gate.','This is a single-client, four-tenant workload; fixed-arrival queueing and retries count toward latency.','Increasing the corpus reuses these deployments. Use a fresh deployment pair when reducing corpus size.']};
const save=async()=>{await writeFile(output+'.tmp',JSON.stringify(result,null,2));await rename(output+'.tmp',output);};
try{
 for(const side of sides)side.executionId=executionId;
 console.log('seeding capacity corpus',documents);await Promise.all(sides.map(side=>side.seed(documents)));
 for(const period of periods){
  const stageId=crypto.randomUUID();for(const side of sides){side.executionId=stageId;side.samples=[];side.errors=[];side.resourceErrors=[];side.lag=[];side.queue=[];side.converged=false;}
  const stage={execution_id:stageId,period_ms:period,offered_writes_per_second:4000/period,offered_reads_per_second:12000/period};result.stages.push(stage);await save();
  stage.workload=await runOfferedWorkload({sides,seconds,period,documents,onProgress:async state=>{console.log('capacity',period,'minute',state.minute,'pending',state.pending_rounds);await save();}});
  await Promise.all(sides.map(side=>side.converge()));await Promise.all(sides.map(side=>side.verify(documents)));
  stage.baseline=sides[0].report();stage.candidate=sides[1].report();stage.summaries={baseline:summarizeWorkload(stage.baseline),candidate:summarizeWorkload(stage.candidate)};
  const candidate=stage.candidate,phases=stage.summaries.candidate.phases;
  stage.within_exploration_targets=sides.every(side=>side.errors.length===0&&side.resourceErrors.length===0)&&candidate.verification?.passed===true&&candidate.converged===true&&phases.read?.p95_ms<=policy.read_p95_ms&&phases.write?.p95_ms<=policy.write_p95_ms&&candidate.lag.length>0&&candidate.lag.every(s=>Number.isFinite(s.lag)&&s.lag>=0&&s.lag<=policy.max_publication_lag)&&candidate.queue.length>0&&candidate.queue.every(s=>s.dead_letter_count===0&&Number.isFinite(s.backlog_count)&&s.backlog_count<=policy.max_backlog&&Number.isFinite(s.oldest_ms)&&s.oldest_ms<=policy.max_oldest_ms);
  await save();console.log('capacity result',period,JSON.stringify(stage.summaries.candidate.phases));
  if(!stage.within_exploration_targets){result.stopped_at_boundary=true;process.exitCode=2;break;}
 }
}catch(error){result.fatal=String(error);result.incomplete_profiles=sides.map(side=>side.report());process.exitCode=1;}
await save();console.log('saved',output);
