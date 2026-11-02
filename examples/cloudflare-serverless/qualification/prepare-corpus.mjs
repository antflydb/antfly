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

import{readFile,writeFile,mkdir,copyFile}from'node:fs/promises';
import{resolve,basename}from'node:path';
import{createHash}from'node:crypto';
import{planCorpus,corpusId}from'./corpus.mjs';
import{assertGateRun,assertGateConfig}from'./gate-scope.mjs';
import{verifySealedRun,verifyReportedRun}from'./gate-provenance.mjs';
const adopt=process.argv[5]==='--adopt-completed-rehearsal';if(process.argv.length!==5&&!(process.argv.length===6&&adopt))throw Error('usage: prepare-corpus GATE_JSON TRIAL COMPLETED_REPORT [--adopt-completed-rehearsal]');
const gate=JSON.parse(await readFile(process.argv[2])),trial=Number(process.argv[3]),source=resolve(process.argv[4]),pair=gate.trials.find(p=>p.trial===trial);
if(!pair)throw Error('TrialMissing');
const bytes=await readFile(source),report=JSON.parse(bytes),id=crypto.randomUUID(),plans=[];
// Validate the entire pair before archiving or changing either side.
for(const path of[pair.baseline,pair.candidate]){
 const run=JSON.parse(await readFile(path));assertGateRun(run,{account:gate.account,dir:gate.dir});
 const errors=await verifySealedRun(run);if(errors.length)throw Error(errors.join('; '));
 const configs=await Promise.all(run.services.map(async service=>{const c=JSON.parse(await readFile(service.config));assertGateConfig(run,service,c);return c;}));
 let current=run;if(adopt&&run.last_execution_id==null){const side=report.sides?.find(s=>s.run===run.id);if(report.qualification!==false||report.stage!=='complete'||!Number.isFinite(report.finished_at)||!corpusId(report.execution_id)||!side||side.execution_id!==report.execution_id||!Array.isArray(side.cold)||side.cold.length||verifyReportedRun(side,run).length)throw Error('CompletedRehearsalAdoptionInvalid');current={...run,last_execution_id:report.execution_id,last_phase:'complete',adopted_completed_rehearsal_sha256:createHash('sha256').update(bytes).digest('hex')};}plans.push({path,before:run,...planCorpus(current,configs,report,id)});
}
const archive=resolve(gate.dir,'corpus-'+id);await mkdir(archive);
await copyFile(source,resolve(archive,'previous-report.json'));
await writeFile(resolve(archive,'transition.json'),JSON.stringify({id,trial,previous_report:source,previous_report_sha256:createHash('sha256').update(bytes).digest('hex'),runs:plans.map(p=>({id:p.run.id,adopted_completed_rehearsal_sha256:p.run.adopted_completed_rehearsal_sha256??null,previous_corpus:p.before.corpus??null,next_corpus:p.run.corpus}))},null,2));
for(const p of plans){
 const dir=resolve(archive,p.run.id);await mkdir(dir);await copyFile(p.path,resolve(dir,'run.json'));
 for(const service of p.before.services)await copyFile(service.config,resolve(dir,basename(service.config)));
}
for(const p of plans){for(let i=0;i<p.run.services.length;i++)await writeFile(p.run.services[i].config,JSON.stringify(p.configs[i],null,2));await writeFile(p.path,JSON.stringify(p.run,null,2));}
console.log('Prepared paired corpus '+id+'; deploy the existing sealed bundles, resume delivery, then start a fresh trial.');
