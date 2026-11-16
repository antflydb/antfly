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

import{readFile,writeFile}from'node:fs/promises';import{resolve,dirname}from'node:path';import{portableSnapshots}from'./independent-http-observation.mjs';import{closeIndependentEvidence}from'./independent-http-evidence.mjs';
const[reportPath,historyPath,output]=process.argv.slice(2);if(!reportPath||!historyPath||!output)throw Error('UsageIndependentReportDeliveryHistoryOutput');const bytes=await readFile(reportPath),report=JSON.parse(bytes),history=JSON.parse(await readFile(historyPath)),root=dirname(resolve(reportPath));if(!report.observation||!report.observer||!report.complete)throw Error('IndependentCaptureIncomplete');
const result=await closeIndependentEvidence({observer:report.observer,reports:report.sides,health:report.observation.health,history:Array.isArray(history)?history:history.samples,first:portableSnapshots(report.observation.first,root,{restore:true}),second:portableSnapshots(report.observation.second,root,{restore:true}),witnesses:report.observation.witnesses,started_at:report.observation.started_at,finished_at:report.observation.finished_at});await writeFile(resolve(output),JSON.stringify(result,null,2),{flag:'wx',mode:0o600});console.log(JSON.stringify({complete:result.complete,full_gate_passed:false,failures:result.failures}));if(!result.complete)process.exitCode=1;
