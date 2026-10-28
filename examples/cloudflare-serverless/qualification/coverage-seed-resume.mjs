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

import{verifyReportedRun}from'./gate-provenance.mjs';
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(value);
// Resume only an interrupted setup, never a measured workload or used cold
// population. The prior failed report remains immutable and is linked by hash.
export function resumeCoverageSeed(report,runs){
 if(report?.schema!==1||report.qualification!==false||report.stage!=='complete'||report.fatal!=='DiagnosticSetupDeadline'||report.documents!==512||report.workload_started_at!==null||report.workload||!uuid(report.execution_id)||!Array.isArray(report.sides)||report.sides.length!==2||runs.length!==2||!report.scope?.startsWith('fresh-seeded diagnostic;')||report.diagnostics?.passed!==true)throw Error('InvalidCoverageSeedResume');
 const seed=report.seed_execution_id??report.execution_id;if(!uuid(seed))throw Error('InvalidCoverageSeedResume');
 for(let i=0;i<2;i++){
  const run=runs[i],side=report.sides[i],samples=side.samples??[],seeds=samples.filter(sample=>sample.phase==='seed');
  if(run.kind!==(i?'candidate':'baseline')||!run.paused||run.deleted||run.cold_execution_id||run.last_execution_id!==report.execution_id||run.last_phase!=='coverage-probe'||verifyReportedRun(side,run).length||side.verification||(side.cold?.length??0)!==0||!Array.isArray(side.errors)||side.errors.some(error=>!['seed','converge'].includes(error.phase)||error.error!=='Error: DiagnosticSetupDeadline')||!Array.isArray(side.resource_errors)||side.resource_errors.length||seeds.length<1||seeds.length>32||samples.some(sample=>sample.epoch!==0||!['seed','converge'].includes(sample.phase)))throw Error('InvalidCoverageSeedResume');
  if(seeds.some((sample,index)=>sample.op!=='mutate'||sample.tenant!==['a','b','c','d'][Math.floor(index/8)]))throw Error('InvalidCoverageSeedResume');
 }
 return seed;
}
