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

import{test}from'node:test';import assert from'node:assert/strict';import{httpLatencyTargets}from'../qualification/http-latency-targets.mjs';
const side=(r,w)=>({samples:[{op:'lookup',ms:r,outcome:'ok'},...(w==null?[]:[{op:'mutate',ms:w,outcome:'ok'}])]});
test('cold five-second policy is independent of the tenfold design goal',()=>{const r=httpLatencyTargets(side(8231),side(1721),'cold');assert.equal(r.rows[0].gate_limit_ms,5000);assert.equal(r.gate_numeric_passed,true);assert.equal(r.design_goals_passed,false);assert.equal(r.full_gate_passed,false);});
test('warm regression and mixed absolute miss are distinct from twofold goals',()=>{const warm=httpLatencyTargets(side(483),side(510),'warm');assert.equal(warm.rows[0].gate_absolute_passed,true);assert.equal(warm.rows[0].gate_no_regression_passed,false);const mixed=httpLatencyTargets(side(4840,6351),side(1594,2326),'mixed');assert.equal(mixed.rows[0].gate_absolute_passed,false);assert.equal(mixed.rows[1].gate_absolute_passed,true);assert.equal(mixed.design_goals_passed,true);});
test('empty and failed populations cannot satisfy numeric readiness',()=>{assert.equal(httpLatencyTargets({samples:[]},side(1),'warm').gate_numeric_passed,false);const broken=side(1);broken.samples[0].outcome='error';assert.equal(httpLatencyTargets(side(100),broken,'burst').design_goals_passed,false);});
