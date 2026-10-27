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

// Fresh fixed-arrival coverage experiment. Never consumes cold epochs or
// substitutes a partial/estimated diagnostic population into evaluate-gate.
import{readFile,writeFile,mkdir,rename}from'node:fs/promises';
import{resolve}from'node:path';import{spawn}from'node:child_process';
import{createHash}from'node:crypto';
import{GateClient}from'./gate-client.mjs';
import{assertUnusedRecovery,verifySealedRun}from'./gate-provenance.mjs';
import{assertGateRun}from'./gate-scope.mjs';
import{runOfferedWorkload}from'./offered-workload.mjs';
import{runPairedPhase}from'./paired-phase.mjs';
import{startLiveDiagnostics}from'./live-diagnostics.mjs';
import{readObservabilityToken}from'./persisted-diagnostic-probe.mjs';
import{collectPersistedWindow}from'./persisted-diagnostics.mjs';
import{reconcileInvocations}from'./invocation-accounting.mjs';

const[path,trialText]=process.argv.slice(2),trial=Number(trialText),seconds=Number(process.argv.find(x=>x.startsWith('--seconds='))?.slice(10)??600);
if(!path||!Number.isInteger(trial)||trial<1||trial>3||!Number.isInteger(seconds)||seconds<180||seconds>1200)throw Error('usage: run-coverage-probe GATE_JSON TRIAL [--seconds=180..1200] [--documents=64|128|512] [--seed]');
const documents=Number(process.argv.find(x=>x.startsWith('--documents='))?.slice(12)??512);
if(![64,128,512].includes(documents))throw Error('InvalidDiagnosticCorpusSize');
const gate=JSON.parse(await readFile(path)),pair=gate.trials[trial-1],paths=[pair.baseline,pair.candidate],initial=await Promise.all(paths.map(async p=>JSON.parse(await readFile(p))));
if(new Set(initial.map(r=>r.id)).size!==2||!initial[0].id.endsWith(`-${trial}-b`)||!initial[1].id.endsWith(`-${trial}-c`))throw Error('ProbePairScopeMismatch');
for(const run of initial){assertGateRun(run,{account:gate.account,dir:gate.dir});assertUnusedRecovery(run);if(!run.paused||!run.deployed||(await verifySealedRun(run)).length)throw Error('RequirePausedSealedReadyPair');}
const token=await readObservabilityToken(gate),execution=crypto.randomUUID(),directory=resolve(gate.dir,'coverage-probe-'+execution);await mkdir(directory,{mode:0o700});
const sources=[];await mkdir(resolve(directory,'sources'),{mode:0o700});
for(const name of ['run-coverage-probe.mjs','gate-client.mjs','invocation-accounting.mjs','persisted-diagnostics.mjs','gate-policy.mjs','offered-workload.mjs','live-diagnostics.mjs']){const bytes=await readFile(new URL('./'+name,import.meta.url));await writeFile(resolve(directory,'sources',name),bytes,{mode:0o600,flag:'wx'});sources.push({name,bytes:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex')});}
const report={schema:1,qualification:false,driver_sources:sources,execution_id:execution,trial,seconds,documents,scope:process.argv.includes('--seed')?'fresh-seeded diagnostic; one active minute audited twice; no cold epochs':'existing-corpus diagnostic; one active minute audited twice; no cold epochs',stage:'admission',started_at:Date.now(),workload_started_at:null,finished_at:null,window:null,diagnostics:null,sides:[],windows:[],window_failures:[],reconciliation:[],fatal:null};let sides=[],live,interrupted=false;
const safeError=e=>e?.message==='publication did not converge'?'PublicationDidNotConverge':/^[A-Z][A-Za-z0-9]+$/.test(e?.message??'')?e.message:'CoverageProbeFailed';
const save=async()=>{report.sides=sides.map(s=>s.report());await writeFile(resolve(directory,'report.tmp'),JSON.stringify(report,null,2),{mode:0o600});await rename(resolve(directory,'report.tmp'),resolve(directory,'report.json'));};
async function lifecycle(action,id){await new Promise((ok,bad)=>{const child=spawn(process.execPath,['qualification/gate-lifecycle.mjs',path,action,id],{stdio:['ignore','pipe','ignore']});child.stdout.on('data',()=>{});child.on('error',()=>bad(Error('ProbeLifecycleFailed')));child.on('exit',code=>code===0?ok():bad(Error('ProbeLifecycleFailed')));});}
const pause=()=>Promise.allSettled(initial.map(run=>lifecycle('pause',run.id)));
for(const signal of['SIGINT','SIGTERM'])process.once(signal,()=>{interrupted=true;for(const s of sides)s.stop(Error('CoverageProbeInterrupted'));});
try{
 for(const run of initial){if(interrupted)throw Error('CoverageProbeInterrupted');await lifecycle('resume',run.id);}
 if(interrupted)throw Error('CoverageProbeInterrupted');
 sides=await Promise.all(paths.map(p=>GateClient.open(p)));for(const s of sides){s.executionId=execution;s.run.last_execution_id=execution;s.run.last_phase='coverage-probe';await writeFile(resolve(s.run.dir,'run.json'),JSON.stringify(s.run,null,2));}
 live=await startLiveDiagnostics(sides.map(s=>s.run),execution,gate.dir);await save();
 if(process.argv.includes('--seed')){report.stage='seeding';await save();for(const s of sides)s.onSeedProgress=progress=>console.log(JSON.stringify({stage:'seeding',side:s.run.kind,...progress}));await runPairedPhase(sides,s=>s.seed(documents));}
 else await runPairedPhase(sides,async s=>{for(const tenant of['a','b','c','d']){s.receipts[tenant]=(await s.call(tenant,{op:'status'},'coverage-admission')).value.visibility;console.log(JSON.stringify({stage:'admission',side:s.run.kind,tenant}));}await s.converge(300000);});
 report.stage='workload';report.workload_started_at=Date.now();
 report.workload=await runOfferedWorkload({sides,seconds,documents,orderOffset:trial,onProgress:async state=>{console.log(JSON.stringify({stage:'workload',...state,tail_clean:live.report().passed}));await save();}});
 report.stage='verification';await runPairedPhase(sides,async s=>{await s.converge();for(const tenant of['a','b','c','d']){if((await s.call(tenant,{op:'count',required_visibility:s.receipts[tenant]},'coverage-verify')).value.count!==String(documents))throw Error('ProbeCountMismatch');}});
}catch(e){report.fatal=safeError(e);for(const s of sides)s.stop(e);await save();}
finally{const paused=await pause();if(paused.some(r=>r.status==='rejected'))report.fatal??='ProbePauseFailed';if(live)try{report.diagnostics=await live.finish();}catch{report.fatal??='ProbeTailFinalizationFailed';}}

if(interrupted)report.fatal??='CoverageProbeInterrupted';
if(report.workload_started_at&&sides.length===2&&!interrupted){
 report.stage='settlement';await save();
 // Choose an interior, whole minute, after the initial two minutes. The
 // experiment is ten minutes by default; this does NOT claim whole-run coverage.
 const from=Math.ceil((report.workload_started_at+60000)/60000)*60000,timeframe={from,to:from+59999};report.window=timeframe;
 const delay=timeframe.to+60000-Date.now();if(delay>0){if(delay>120000){report.fatal??='ProbeWindowNotReached';await save();process.exitCode=1;process.exit();}for(let left=delay;left>0;left-=30000)await new Promise(r=>setTimeout(r,Math.min(left,30000)));}
 const first=new Map(),complete=new Map();
 for(let pass=0;pass<2;pass++){
  if(pass)await new Promise(r=>setTimeout(r,30000));
  for(let side=0;side<sides.length;side++)for(const role of['app','writer','reader','builder']){
   if(interrupted){report.fatal??='CoverageProbeInterrupted';continue;}
   const s=sides[side],key=s.run.id+'/'+role;
   try{const window=await collectPersistedWindow(s.run,role,token,timeframe),bytes=Buffer.from(JSON.stringify(window)),file=resolve(directory,s.run.id+'-'+role+'-'+pass+'.json');
    await writeFile(file,bytes,{mode:0o600,flag:'wx'});report.windows.push({side,role,pass,path:file,bytes:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex'),events:window.events,invocations:window.invocations});
    if(!pass)first.set(key,window.rows_sha256);else if(first.get(key)!==window.rows_sha256)report.window_failures.push({side,role,pass,error:'DiagnosticIngestionUnsettled'});else{const set=complete.get(side)??[];set.push(window);complete.set(side,set);}
   }catch(e){report.window_failures.push({side,role,pass,error:safeError(e),...(e.diagnostic?{sampling:e.diagnostic}:{})});}
   await save();
  }
 }
 for(let side=0;side<sides.length;side++){
  const s=sides[side],ledger=s.invocations.report();ledger.attempts=ledger.attempts.filter(a=>a.started_at>=timeframe.from&&a.started_at<=timeframe.to);
  report.reconciliation.push({side,...reconcileInvocations(s.run,ledger,complete.get(side)??[],{timeframe})});
 }
}
report.stage='complete';report.finished_at=Date.now();await save();
console.log(JSON.stringify({directory,qualification:false,window_failures:report.window_failures,reconciliation:report.reconciliation.map(r=>({side:r.side,complete:r.complete,failures:r.failures})),fatal:report.fatal}));
if(report.fatal||report.window_failures.length||report.reconciliation.some(r=>!r.complete))process.exitCode=1;
