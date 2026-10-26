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

import{readFile,writeFile}from'node:fs/promises';
import{resolve}from'node:path';
import{validatedEvidence}from'./validated-evidence.mjs';
import{evaluateGate}from'./gate-policy.mjs';
import{verifySealedRun,verifyReportedRun}from'./gate-provenance.mjs';
const gate=JSON.parse(await readFile(process.argv[2])),reports=[];let evidence;
try{evidence=JSON.parse(await readFile(resolve(gate.dir,'evidence.json')));}catch{}
for(const pair of gate.trials){
 let report;try{report=JSON.parse(await readFile(resolve(gate.dir,'trial-'+pair.trial+'.json')));}catch{report={schema:1,trial:pair.trial,fatal:'hour-long trial report unavailable'};}
 const base=JSON.parse(await readFile(pair.baseline)),candidate=JSON.parse(await readFile(pair.candidate));
 for(const[name,run]of[['baseline',base],['candidate',candidate]])if(report[name])report[name].provenance_errors=[...verifyReportedRun(report[name],run),...await verifySealedRun(run)];
 Object.assign(report,await validatedEvidence(evidence,report,base,candidate));
 reports.push(report);
}
const result={...evaluateGate(reports),evaluated_at:new Date().toISOString(),report_paths:gate.trials.map(t=>resolve(gate.dir,'trial-'+t.trial+'.json'))};await writeFile(resolve(gate.dir,'gate-report.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(result,null,2));if(!result.passed)process.exitCode=2;
