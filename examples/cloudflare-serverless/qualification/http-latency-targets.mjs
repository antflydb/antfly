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

import{policy,percentile}from'./gate-policy.mjs';
// Numeric readiness only. Stretch goals never change the full gate's policy.
export function httpLatencyTargets(baseline,candidate,phase){
 if(!['cold','warm','burst','mixed'].includes(phase))throw Error('HttpLatencyPhaseInvalid');const rows=[];
 for(const op of phase==='mixed'?['lookup','mutate']:['lookup']){const a=(baseline?.samples??[]).filter(s=>s.op===op),b=(candidate?.samples??[]).filter(s=>s.op===op),valid=a.length>0&&b.length>0&&[...a,...b].every(s=>s.outcome==='ok'&&Number.isFinite(s.ms)&&s.ms>=0),base=percentile(a.map(s=>s.ms),.95),next=percentile(b.map(s=>s.ms),.95),absolute=phase==='cold'?policy.cold_p95_ms:op==='mutate'?policy.write_p95_ms:policy.read_p95_ms,factor=phase==='cold'?10:2;
  rows.push({op,baseline_samples:a.length,candidate_samples:b.length,baseline_p95_ms:base,candidate_p95_ms:next,gate_limit_ms:absolute,gate_absolute_passed:valid&&next<=absolute,gate_no_regression_passed:valid&&next<=base,design_goal_factor:factor,design_goal_limit_ms:base==null?null:base/factor,design_goal_passed:valid&&next<=base/factor});
 }return{qualification:false,full_gate_passed:false,phase,rows,gate_numeric_passed:rows.every(r=>r.gate_absolute_passed&&r.gate_no_regression_passed),design_goals_passed:rows.every(r=>r.design_goal_passed)};
}
