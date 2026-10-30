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

// A representative paired run, not a substitute for the full qualification gate.
import{readFile,writeFile,mkdir,rename}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';
import{startDestinationMonitor,evaluateDestinationHistory}from'./destination-history.mjs';
import{rehearsalSeedResume}from'./rehearsal-seed-resume.mjs';
import{GateClient}from'./gate-client.mjs';import{assertUnusedRecovery,verifyReportedRun,verifySealedRun}from'./gate-provenance.mjs';import{runPairedPhase}from'./paired-phase.mjs';import{runOfferedWorkload}from'./offered-workload.mjs';import{summarizeWorkload,rehearsalLatencyTargets}from'./workload-summary.mjs';import{observerCall,collectObserverSource,reconcileObserverSource,observerDiagnosticsComplete}from'./paired-observer-client.mjs';
const path=process.argv[2],seconds=Number(process.argv.find(a=>a.startsWith('--seconds='))?.slice(10)??180);if(!path||!Number.isInteger(seconds)||seconds<180||seconds>600)throw Error('UsagePairedRehearsalObserverAndSeconds180To600');
const observer=JSON.parse(await readFile(path));if(observer.deleted||!observer.attached_at||!observer.token_installed||observer.scopes?.length!==8)throw Error('ObserverReadyRequired');if(execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim())throw Error('RehearsalCleanSourceRequired');
const sides=await Promise.all(observer.paths.map(p=>GateClient.open(p)));for(const side of sides)assertUnusedRecovery(side.run);const execution=crypto.randomUUID(),dir=resolve(observer.dir,'rehearsal-'+execution);
const report={schema:1,qualification:false,execution_id:execution,source_revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),documents:512,seconds,stage:'witness-before',started_at:Date.now(),witnesses:{before:{},after:{}},workload:null,sides:[],captures:[],diagnostics:[],fatal:null,limitations:['Short paired rehearsal, not three hour-long trials or cold qualification.','Whole-run native terminal coverage is measured; provider isolate eviction and absolute peak memory are not proven.']};let saving=Promise.resolve();
const resumePath=process.argv.find(a=>a.startsWith('--resume-setup='))?.slice(15);let seedExecution=execution,resumed=null;if(resumePath){if(!resolve(resumePath).startsWith(observer.dir+'/rehearsal-')||!resolve(resumePath).endsWith('/report.json'))throw Error('RehearsalResumeScopeInvalid');const input=await readFile(resumePath);resumed=JSON.parse(input);seedExecution=rehearsalSeedResume(resumed,sides.map(s=>s.run));report.resumed_seed_execution_id=seedExecution;report.resumed_setup_report={path:resolve(resumePath),sha256:createHash('sha256').update(input).digest('hex'),execution_id:resumed.execution_id};report.limitations.push('Diagnostic-only setup replay uses the original seed operation IDs; measured workloads and cold epochs are never resumed.');}
await mkdir(dir,{mode:0o700});report.observer_resource_failures=observer.observed_resource_failures=[];
const save=()=>{report.sides=sides.map(s=>({...s.report(),summary:summarizeWorkload(s.report())}));const body=JSON.stringify(report,null,2);saving=saving.then(async()=>{await writeFile(resolve(dir,'report.tmp'),body,{mode:0o600});await rename(resolve(dir,'report.tmp'),resolve(dir,'report.json'));});return saving;};
async function markExecution(phase){
 const updates=[];for(const side of sides){const file=resolve(side.run.dir,'run.json'),current=JSON.parse(await readFile(file));if(verifyReportedRun(side.report(),current).length||(await verifySealedRun(current)).length)throw Error('RehearsalDeploymentChanged');if(phase==='started'&&current.last_execution_id&&current.last_phase!=='corpus-prepared'&&!(resumed&&current.last_execution_id===resumed.execution_id&&current.last_phase==='complete'))throw Error('RehearsalFreshCorpusRequired');if(phase!=='started'&&current.last_execution_id!==execution)throw Error('RehearsalExecutionChanged');updates.push({side,file,value:{...current,last_execution_id:execution,last_phase:phase==='started'?'rehearsal':'complete'}});}
 for(const u of updates){await writeFile(u.file,JSON.stringify(u.value,null,2));u.side.run=u.value;}
}
async function witnesses(phase){for(const scope of observer.scopes){const started=Date.now(),w=await observerCall(observer,'/source/'+scope.source+'/counter-witness',{id:crypto.randomUUID()});if(w.producer!==scope.source||![401,404].includes(w.status))throw Error('RehearsalWitnessInvalid');report.witnesses[phase][scope.source]={...w,driver_started_at:started,driver_finished_at:Date.now()};await save();}}
for(const side of sides){side.executionId=execution;side.onSeedProgress=async progress=>{console.log(JSON.stringify({stage:'seeding',side:side.run.kind,...progress}));await save();};}let claimed=false,interrupted=false,destinationMonitor=null;for(const signal of['SIGINT','SIGTERM'])process.once(signal,()=>{interrupted=true;for(const s of sides)s.stop(Error('RehearsalInterrupted'));});
try{
 await markExecution('started');claimed=true;report.destination_delivery_observations=[];destinationMonitor=await startDestinationMonitor(path,{samples:report.destination_delivery_observations,persist:save});await witnesses('before');report.stage='seeding';await save();const timer=setTimeout(()=>{for(const s of sides)s.stop(Error('RehearsalSetupDeadline'));},1800000);try{await runPairedPhase(sides,s=>s.seed(512,{executionId:seedExecution}));}finally{clearTimeout(timer);}
 report.stage='workload';await save();report.workload=await runOfferedWorkload({sides,seconds,documents:512,onProgress:async state=>{console.log(JSON.stringify({stage:'workload',...state}));await save();}});
 report.stage='verification';await save();await runPairedPhase(sides,async s=>{await s.converge();await s.verify(512);});
}catch(error){report.fatal=String(error);for(const s of sides)s.stop(error);console.log(JSON.stringify({stage:report.stage,fatal:report.fatal}));}
finally{report.measured_finished_at=Date.now();try{if(claimed)await witnesses('after');}catch{report.fatal??='RehearsalAfterWitnessFailed';}await save();}
try{if(!interrupted&&Object.keys(report.witnesses.after).length===8){
 report.stage='settlement';await save();const earliest=Date.now()+60000;while(Date.now()<earliest)await new Promise(ok=>setTimeout(ok,Math.min(1000,earliest-Date.now())));
 const deadline=Date.now()+600000;let previous=new Map(),pass=0;
 while(Date.now()<deadline&&!interrupted){
  const current=new Map();report.diagnostics=[];for(const scope of observer.scopes){try{const snapshot=await collectObserverSource(observer,scope,{witnesses:{before:report.witnesses.before[scope.source].attempt_id_sha256,after:report.witnesses.after[scope.source].attempt_id_sha256}}),file=resolve(dir,scope.source+'-snapshot-'+pass+'.json'),body=JSON.stringify(snapshot);await writeFile(file,body,{flag:'wx',mode:0o600});report.captures.push({source:scope.source,pass,path:file,bytes:Buffer.byteLength(body),sha256:createHash('sha256').update(body).digest('hex')});current.set(scope.source,snapshot);
    if(previous.has(scope.source)){const side=sides.find(s=>s.run.id===scope.run),proof=await reconcileObserverSource(previous.get(scope.source),snapshot,{source:scope.source,captureId:scope.capture_id,version:side.run.versions[scope.role],counterVersion:observer.version,witnesses:{before:report.witnesses.before[scope.source].attempt_id_sha256,after:report.witnesses.after[scope.source].attempt_id_sha256},...(scope.role==='app'?{ledger:side.invocations.report()}:{})});report.diagnostics.push({role:scope.role,...proof});}
   }catch(error){report.diagnostics.push({source:scope.source,complete:false,failures:[String(error)]});}await save();}
  const health=await observerCall(observer,'/health');report.observer_health=health;report.destination_health=evaluateDestinationHistory(report.destination_delivery_observations,{id:observer.id,captureId:observer.observation_capture_id,from:report.started_at,to:report.measured_finished_at});console.log(JSON.stringify({stage:'settlement',pass,complete:report.diagnostics.filter(d=>d.complete).length,total:8,failures:report.diagnostics.filter(d=>!d.complete).map(d=>({source:d.source,failures:d.failures}))}));
  if(observerDiagnosticsComplete(observer,health,report.diagnostics,report.destination_health))break;
  previous=current;pass++;await new Promise(ok=>setTimeout(ok,30000));
 }
}
}catch(error){report.fatal??=String(error);}finally{try{await destinationMonitor?.stop();}catch{report.fatal??='ExportDeliveryMonitorFailed';}}report.destination_health=evaluateDestinationHistory(report.destination_delivery_observations,{id:observer.id,captureId:observer.observation_capture_id,from:report.started_at,to:report.measured_finished_at});report.stage='complete';report.finished_at=Date.now();try{if(claimed)await markExecution('complete');}catch(error){report.fatal??=String(error);}report.database_checks_passed=!report.fatal&&sides.every(s=>s.verification?.passed&&s.verification.bodies===2052&&s.verification.receipts===8&&s.errors.length===0&&s.resourceErrors.length===0);report.diagnostics_complete=observerDiagnosticsComplete(observer,report.observer_health,report.diagnostics,report.destination_health);report.latency_evaluation=rehearsalLatencyTargets(sides[0].report(),sides[1].report());report.rehearsal_passed=report.database_checks_passed&&report.diagnostics_complete&&report.latency_evaluation.passed;await save();console.log(JSON.stringify({directory:dir,qualification:false,rehearsal_passed:report.rehearsal_passed,database_checks_passed:report.database_checks_passed,diagnostics_complete:report.diagnostics_complete,fatal:report.fatal,sides:report.sides.map(s=>({run:s.run,summary:s.summary}))}));if(!report.rehearsal_passed)process.exitCode=1;
