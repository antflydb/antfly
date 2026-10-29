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
import{GateClient}from'./gate-client.mjs';import{assertUnusedRecovery}from'./gate-provenance.mjs';import{runPairedPhase}from'./paired-phase.mjs';import{runOfferedWorkload}from'./offered-workload.mjs';import{summarizeWorkload}from'./workload-summary.mjs';import{observerCall,collectObserverSource,reconcileObserverSource}from'./paired-observer-client.mjs';
const path=process.argv[2],seconds=Number(process.argv.find(a=>a.startsWith('--seconds='))?.slice(10)??180);if(!path||!Number.isInteger(seconds)||seconds<180||seconds>600)throw Error('UsagePairedRehearsalObserverAndSeconds180To600');
const observer=JSON.parse(await readFile(path));if(observer.deleted||!observer.attached_at||!observer.token_installed||observer.scopes?.length!==8)throw Error('ObserverReadyRequired');if(execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim())throw Error('RehearsalCleanSourceRequired');
const sides=await Promise.all(observer.paths.map(p=>GateClient.open(p)));for(const side of sides)assertUnusedRecovery(side.run);const execution=crypto.randomUUID(),dir=resolve(observer.dir,'rehearsal-'+execution);await mkdir(dir,{mode:0o700});
const report={schema:1,qualification:false,execution_id:execution,source_revision:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),documents:512,seconds,stage:'witness-before',started_at:Date.now(),witnesses:{before:{},after:{}},workload:null,sides:[],captures:[],diagnostics:[],fatal:null,limitations:['Short paired rehearsal, not three hour-long trials or cold qualification.','Whole-run native terminal coverage is measured; provider isolate eviction and absolute peak memory are not proven.']};let saving=Promise.resolve();
const save=()=>{report.sides=sides.map(s=>({...s.report(),summary:summarizeWorkload(s.report())}));const body=JSON.stringify(report,null,2);saving=saving.then(async()=>{await writeFile(resolve(dir,'report.tmp'),body,{mode:0o600});await rename(resolve(dir,'report.tmp'),resolve(dir,'report.json'));});return saving;};
async function witnesses(phase){for(const scope of observer.scopes){const started=Date.now(),w=await observerCall(observer,'/source/'+scope.source+'/counter-witness',{id:crypto.randomUUID()});if(w.producer!==scope.source||![401,404].includes(w.status))throw Error('RehearsalWitnessInvalid');report.witnesses[phase][scope.source]={...w,driver_started_at:started,driver_finished_at:Date.now()};await save();}}
for(const side of sides){side.executionId=execution;side.onSeedProgress=async progress=>{console.log(JSON.stringify({stage:'seeding',side:side.run.kind,...progress}));await save();};}let interrupted=false;for(const signal of['SIGINT','SIGTERM'])process.once(signal,()=>{interrupted=true;for(const s of sides)s.stop(Error('RehearsalInterrupted'));});
try{
 await witnesses('before');report.stage='seeding';await save();const timer=setTimeout(()=>{for(const s of sides)s.stop(Error('RehearsalSetupDeadline'));},1800000);try{await runPairedPhase(sides,s=>s.seed(512));}finally{clearTimeout(timer);}
 report.stage='workload';await save();report.workload=await runOfferedWorkload({sides,seconds,documents:512,onProgress:async state=>{console.log(JSON.stringify({stage:'workload',...state}));await save();}});
 report.stage='verification';await save();await runPairedPhase(sides,async s=>{await s.converge();await s.verify(512);});
}catch(error){report.fatal=String(error);for(const s of sides)s.stop(error);console.log(JSON.stringify({stage:report.stage,fatal:report.fatal}));}
finally{report.measured_finished_at=Date.now();try{await witnesses('after');}catch{report.fatal??='RehearsalAfterWitnessFailed';}await save();}
if(!interrupted&&Object.keys(report.witnesses.after).length===8){
 report.stage='settlement';await save();const earliest=Date.now()+60000;while(Date.now()<earliest)await new Promise(ok=>setTimeout(ok,Math.min(1000,earliest-Date.now())));
 const deadline=Date.now()+600000;let previous=new Map(),pass=0;
 while(Date.now()<deadline&&!interrupted){
  const current=new Map();report.diagnostics=[];for(const scope of observer.scopes){try{const snapshot=await collectObserverSource(observer,scope),file=resolve(dir,scope.source+'-snapshot-'+pass+'.json'),body=JSON.stringify(snapshot);await writeFile(file,body,{flag:'wx',mode:0o600});report.captures.push({source:scope.source,pass,path:file,bytes:Buffer.byteLength(body),sha256:createHash('sha256').update(body).digest('hex')});current.set(scope.source,snapshot);
    if(previous.has(scope.source)){const side=sides.find(s=>s.run.id===scope.run),proof=await reconcileObserverSource(previous.get(scope.source),snapshot,{source:scope.source,captureId:scope.capture_id,version:side.run.versions[scope.role],counterVersion:observer.version,witnesses:{before:report.witnesses.before[scope.source].attempt_id_sha256,after:report.witnesses.after[scope.source].attempt_id_sha256},...(scope.role==='app'?{ledger:side.invocations.report()}:{})});report.diagnostics.push({role:scope.role,...proof});}
   }catch(error){report.diagnostics.push({source:scope.source,complete:false,failures:[String(error)]});}await save();}
  const health=await observerCall(observer,'/health');report.observer_health=health;console.log(JSON.stringify({stage:'settlement',pass,complete:report.diagnostics.filter(d=>d.complete).length,total:8,failures:report.diagnostics.filter(d=>!d.complete).map(d=>({source:d.source,failures:d.failures}))}));
  if(report.diagnostics.length===8&&report.diagnostics.every(d=>d.complete)&&health.rejections_present===false)break;
  previous=current;pass++;await new Promise(ok=>setTimeout(ok,30000));
 }
}
report.stage='complete';report.finished_at=Date.now();report.database_checks_passed=!report.fatal&&sides.every(s=>s.verification?.passed&&s.verification.bodies===2052&&s.verification.receipts===8&&s.errors.length===0&&s.resourceErrors.length===0);report.diagnostics_complete=report.diagnostics.length===8&&report.diagnostics.every(d=>d.complete)&&report.observer_health?.rejections_present===false;report.rehearsal_passed=report.database_checks_passed&&report.diagnostics_complete;await save();console.log(JSON.stringify({directory:dir,qualification:false,rehearsal_passed:report.rehearsal_passed,database_checks_passed:report.database_checks_passed,diagnostics_complete:report.diagnostics_complete,fatal:report.fatal,sides:report.sides.map(s=>({run:s.run,summary:s.summary}))}));if(!report.rehearsal_passed)process.exitCode=1;
