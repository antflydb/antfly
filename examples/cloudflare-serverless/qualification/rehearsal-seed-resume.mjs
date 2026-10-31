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

import{verifyReportedRun,assertUnusedRecovery}from'./gate-provenance.mjs';
const uuid=v=>typeof v==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(v);
// Diagnostic setup only. Every keyed seed request is replayed and the entire
// corpus/receipt verification still runs after the measured workload. This
// cannot resume a measured run or consume/reuse physical recovery epochs.
export function rehearsalSeedResume(report,runs){
 if(report?.schema!==1||report.qualification!==false||report.stage!=='complete'||!report.fatal||!Number.isSafeInteger(report.finished_at)||report.workload||report.documents!==512||!uuid(report.execution_id)||runs.length!==2||report.sides?.length!==2)throw Error('RehearsalSetupResumeInvalid');
 for(const run of runs){assertUnusedRecovery(run);const side=report.sides.find(s=>s.run===run.id);if(run.last_execution_id!==report.execution_id||run.last_phase!=='complete'||!side||side.execution_id!==report.execution_id||verifyReportedRun(side,run).length||side.verification||!Array.isArray(side.cold)||side.cold.length||!Array.isArray(side.resource_errors)||side.resource_errors.length||!Array.isArray(side.samples)||side.samples.some(s=>!['seed','converge'].includes(s.phase)))throw Error('RehearsalSetupResumeIdentityInvalid');const seeds=side.samples.filter(s=>s.phase==='seed');if(seeds.length>32||seeds.some(s=>s.op!=='mutate'||!['a','b','c','d'].includes(s.tenant))||['a','b','c','d'].some(t=>seeds.filter(s=>s.tenant===t).length>8))throw Error('RehearsalSetupResumePopulationInvalid');}
 const id=report.resumed_seed_execution_id??report.execution_id;if(!uuid(id))throw Error('RehearsalSetupResumeInvalid');return id;
}
