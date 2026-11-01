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

import{readFile,writeFile}from'node:fs/promises';import{attributeRequestPhases}from'./phase-attribution.mjs';
const[input,out]=process.argv.slice(2);if(!input||!out)throw Error('UsageSourceReportOutput');const report=JSON.parse(await readFile(input)),rows=report.cases?.flatMap(c=>c.samples??[])??report.sides?.flatMap(s=>s.samples??[])??report.samples;if(!Array.isArray(rows)||!rows.length)throw Error('PhaseSamplesRequired');const result=attributeRequestPhases(rows);await writeFile(out,JSON.stringify(result,null,2),{flag:'wx',mode:0o600});console.log(JSON.stringify({complete:result.complete,coverage:result.coverage}));if(!result.complete)process.exitCode=1;
