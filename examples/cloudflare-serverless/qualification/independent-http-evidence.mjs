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

import{observerScopes}from'./paired-observer.mjs';import{reconcileObserverSource,observerDiagnosticsComplete}from'./paired-observer-client.mjs';import{evaluateDestinationHistory}from'./destination-history.mjs';
// Recompute canonical/native proofs for every role. Complete client responses
// or a successful ingestion acknowledgement never stand in for service coverage.
export async function closeIndependentEvidence({observer,reports,health,history,first,second,witnesses,started_at,finished_at}){
 const scopes=observerScopes({SOURCE_SCOPES:JSON.stringify(observer.scopes),SOURCE_LAYOUT:observer.source_layout}),failures=[],diagnostics=[];
 if(!Array.isArray(reports)||reports.length!==2||new Set(reports.map(r=>r.side)).size!==2||!reports.every(r=>['baseline','candidate'].includes(r.side)))throw Error('IndependentEvidenceReportsInvalid');
 const ledgers=new Map();for(const report of reports){if(!report.passed||!report.completed)failures.push('IndependentDriverFailed');if(!report.run||!report.source_versions)throw Error('IndependentEvidenceScopeInvalid');ledgers.set(report.run,report);}
 for(const scope of scopes){const report=ledgers.get(scope.run);if(!report||!report.source_versions[scope.role])throw Error('IndependentEvidenceScopeInvalid');
  try{const proof=await reconcileObserverSource(first[scope.source],second[scope.source],{source:scope.source,captureId:scope.capture_id,version:report.source_versions[scope.role],counterVersion:observer.version,witnesses:witnesses[scope.source],...(scope.role==='app'?{ledger:report.invocation_ledger}:{})});diagnostics.push({role:scope.role,...proof});}catch{diagnostics.push({source:scope.source,role:scope.role,complete:false,failures:['IndependentSourceEvidenceMissing']});}
 }
 // The local monitor starts before remote readiness. Require its entire earlier
 // window too, rather than discarding pre-workload errors or relaxing cadence.
 const deliveryFrom=Number.isSafeInteger(history?.[0]?.requested_at)?Math.min(started_at,history[0].requested_at):started_at;
 const destinations=evaluateDestinationHistory(history,{id:observer.id,captureId:observer.observation_capture_id,from:deliveryFrom,to:finished_at});
 const complete=observerDiagnosticsComplete(observer,health,diagnostics,destinations);if(!complete)failures.push('IndependentServiceEvidenceIncomplete');
 return{schema:1,qualification:false,full_gate_passed:false,complete:complete&&failures.length===0,failures:[...new Set(failures)],diagnostics,destinations,driver_checks_passed:reports.every(r=>r.passed)};
}
