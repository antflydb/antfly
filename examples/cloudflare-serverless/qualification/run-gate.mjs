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
import{GateClient,resumeSeedExecution}from'./gate-client.mjs';
import{startLiveDiagnostics}from'./live-diagnostics.mjs';
import{evaluateTrial,evaluateWarmPrerequisites}from'./gate-policy.mjs';
import{validatedEvidence}from'./validated-evidence.mjs';
import{runOfferedWorkload}from'./offered-workload.mjs';
import{runPairedPhase}from'./paired-phase.mjs';
import{assertUnusedRecovery,verifyReportedRun,verifySealedRun}from'./gate-provenance.mjs';
const path=process.argv[2],trial=Number(process.argv[3]??1),seconds=Number(process.argv.find(v=>v.startsWith('--seconds='))?.slice(10)??3600),documents=512,period=30000;
if(!Number.isInteger(seconds)||seconds<60||seconds>7200)throw Error('seconds must be 60..7200');
const gate=JSON.parse(await readFile(path)),pair=gate.trials.find(t=>t.trial===trial);if(!pair)throw Error('trial missing');
const baseline=await GateClient.open(pair.baseline),candidate=await GateClient.open(pair.candidate),output=resolve(gate.dir,'trial-'+trial+(seconds<3600?'-smoke':'')+'.json');
for(const side of[baseline,candidate])assertUnusedRecovery(side.run);
const executionId=crypto.randomUUID();baseline.executionId=executionId;candidate.executionId=executionId;
const resumePath=process.argv.find(v=>v.startsWith('--resume-setup='))?.slice(15);
const resumedSeedExecutionId=resumePath?resumeSeedExecution(JSON.parse(await readFile(resumePath)),baseline.run,candidate.run):null;
let offered=0,fatal,workload,diagnostics,diagnosticReport,diagnosticGuard,evidenceSummary={};
for(const signal of['SIGTERM','SIGINT'])process.once(signal,()=>{baseline.stop(Error('QualificationInterrupted'));candidate.stop(Error('QualificationInterrupted'));});
const report=()=>({schema:1,execution_id:executionId,resumed_seed_execution_id:resumedSeedExecutionId,trial,documents,offered_period_ms:period,soak_ms:offered,workload,diagnostics:diagnosticReport??diagnostics?.report(),...evidenceSummary,baseline:baseline.report(),candidate:candidate.report(),fatal,limitations:['Fresh physical actors do not prove complete platform isolate eviction.','Live R2 attempts and sampled memory are not billing or absolute memory peaks.']});
async function save(){const data=report();data.evaluation=evaluateTrial(data);await writeFile(output+'.tmp',JSON.stringify(data,null,2));await rename(output+'.tmp',output);}
async function markPhase(phase){
 for(const side of[baseline,candidate]){
  const path=resolve(side.run.dir,'run.json'),current=JSON.parse(await readFile(path));
  const errors=[...verifyReportedRun(side.report(),current),...await verifySealedRun(current)];
  if(errors.length)throw Error('deployment changed before '+phase+': '+errors.join('; '));
  if(phase!=='setup'&&(current.last_execution_id!==executionId||current.last_phase!=='setup'))throw Error('qualification corpus execution changed');
  side.run={...current,last_execution_id:executionId,last_phase:phase};await writeFile(path,JSON.stringify(side.run,null,2));
 }
}
async function lifecycle(action,id){await new Promise((resolve,reject)=>{const child=spawn(process.execPath,[fileURLToPath(new URL('./gate-lifecycle.mjs',import.meta.url)),path,action,id],{stdio:'inherit'});child.on('error',reject);child.on('exit',code=>code===0?resolve():reject(Error(action+' failed')));});}
try{
 for(const side of[baseline,candidate])if(side.run.paused)await lifecycle('resume',side.run.id);
 diagnostics=await startLiveDiagnostics([baseline.run,candidate.run],executionId,gate.dir);
 diagnosticGuard=setInterval(()=>{try{diagnostics.assertClean();}catch(error){baseline.stop(error);candidate.stop(error);}},1000);
 await markPhase('setup');
 console.log('seeding',trial);await runPairedPhase([baseline,candidate],side=>side.seed(documents,{executionId:resumedSeedExecutionId??executionId}));console.log('seeded',trial);
 await markPhase('workload');
 workload=await runOfferedWorkload({sides:[baseline,candidate],seconds,period,documents,orderOffset:trial,onProgress:async state=>{console.log('minute',trial,state.minute,'pending rounds',state.pending_rounds);diagnostics.assertClean();await save();}});
 offered=seconds*1000;await runPairedPhase([baseline,candidate],side=>side.converge());await runPairedPhase([baseline,candidate],side=>side.verify(documents));await save();
 if(seconds>=3600||process.argv.includes('--cold')){
  let evidence;try{evidence=JSON.parse(await readFile(resolve(gate.dir,'evidence.json')));}catch{}
  evidenceSummary=await validatedEvidence(evidence,report(),baseline.run,candidate.run);
  diagnostics.assertClean();
  const prerequisites=evaluateWarmPrerequisites(report());
  if(!prerequisites.passed)throw Error('cold actors preserved; warm prerequisites failed: '+prerequisites.failures.join('; '));
  candidate.run.cold_execution_id=executionId;await writeFile(resolve(candidate.run.dir,'run.json'),JSON.stringify(candidate.run,null,2));
  await lifecycle('pause',candidate.run.id);await new Promise(r=>setTimeout(r,31000));
  for(let epoch=1;epoch<=100;epoch++){const tenant=['a','b','c','d'][(epoch-1)%4],{value,sample}=await candidate.call(tenant,{op:'count',required_visibility:candidate.receipts[tenant]},'cold',{epoch});candidate.cold.push({...sample,count:value.count});candidate.physicalEpochs??={};candidate.physicalEpochs[tenant]=epoch;if(epoch%10===0){console.log('cold',trial,epoch);await save();}}
  await candidate.verify(documents);
 }
}catch(error){fatal=String(error);console.error('gate trial failed',trial,fatal);}
clearInterval(diagnosticGuard);
if(diagnostics)try{diagnosticReport=await diagnostics.finish();if(!diagnosticReport.passed)fatal??='LiveResourceDiagnosticsFailed';}catch{fatal??='LiveResourceDiagnosticFinalizationFailed';}
await save();await writeFile(resolve(gate.dir,'trial-'+trial+'-'+executionId+'.json'),JSON.stringify(report(),null,2));console.log('saved',output,JSON.stringify(evaluateTrial(report())));if(fatal)process.exitCode=1;
