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

import{readFile}from'node:fs/promises';
import{createHash}from'node:crypto';
// Content hashes bind completed local evidence to the measured native pair.
export async function validatedEvidence(evidence,report,baseline,candidate){
 if(evidence?.candidate_wasm_sha256!==candidate.wasm_sha256||evidence?.baseline_wasm_sha256!==baseline.wasm_sha256||report.candidate?.wasm_sha256!==candidate.wasm_sha256||report.baseline?.wasm_sha256!==baseline.wasm_sha256||!Array.isArray(evidence.artifacts)||evidence.artifacts.length<3)return {};
 for(const artifact of evidence.artifacts){
  if(typeof artifact.path!=="string"||!/^[a-f0-9]{64}$/.test(artifact.sha256??""))return {};
  try{if(createHash('sha256').update(await readFile(artifact.path)).digest('hex')!==artifact.sha256)return {};}catch{return {};}
 }
 if(evidence.faults?.artifact_sha256!==createHash('sha256').update(JSON.stringify(evidence.artifacts.slice(0,3))).digest('hex'))return {};
 return {faults:evidence.faults,ann:evidence.ann};
}
