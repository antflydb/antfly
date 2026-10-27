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

// Eight-role backend access/ingestion probe, never a gate pass. No saved query,
// deployment, workload, platform storage or recovery epoch is changed.
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {assertGateRun} from './gate-scope.mjs';
import {readObservabilityToken} from './persisted-diagnostic-probe.mjs';
import {collectPersistedWindow} from './persisted-diagnostics.mjs';
import {verifyPersistedProbe} from './persisted-diagnostic-evidence.mjs';

const [path,trialText,fromText,toText]=process.argv.slice(2),trial=Number(trialText),timeframe={from:Number(fromText),to:Number(toText)};
if(process.argv.length!==6||!Number.isInteger(trial)||trial<1||trial>3)throw Error('usage: collect-persisted-diagnostics GATE_JSON TRIAL FROM_MS TO_MS');
if(!Number.isSafeInteger(timeframe.from)||!Number.isSafeInteger(timeframe.to)||timeframe.from<0||timeframe.from>=timeframe.to||timeframe.to-timeframe.from>60000||timeframe.to>Date.now()-60000)throw Error('RequireSettledDiagnosticWindow');
const gate=JSON.parse(await readFile(path)),pair=gate.trials[trial-1],runs=await Promise.all([pair.baseline,pair.candidate].map(async path=>JSON.parse(await readFile(path))));
for(const run of runs)assertGateRun(run,{account:gate.account,dir:gate.dir});
if(runs[0].id===runs[1].id)throw Error('DiagnosticScopeMismatch');
const token=await readObservabilityToken(gate),directory=resolve(gate.dir,'persisted-probe-'+crypto.randomUUID());
await mkdir(directory,{mode:0o700});
const report={schema:1,qualification:false,timeframe,trial,stable:false,resource_failures:0,invocation_errors:0,gaps:0,artifacts:[],error:null},snapshots=new Map();let totalBytes=0;
try{
 for(let pass=0;pass<2;pass++){
  if(pass)await new Promise(resolve=>setTimeout(resolve,30000));
  for(const run of runs)for(const role of ['app','writer','reader','builder']){
   const result=await collectPersistedWindow(run,role,token,timeframe),key=run.id+'-'+role;
   if(pass&&snapshots.get(key)!==result.rows_sha256)throw Error('DiagnosticIngestionUnsettled');
   snapshots.set(key,result.rows_sha256);report.resource_failures+=result.resource_failures;report.invocation_errors+=result.invocation_errors;report.gaps+=Number(result.gap);
   const bytes=Buffer.from(JSON.stringify(result));totalBytes+=bytes.byteLength;if(bytes.byteLength>64*1024*1024||totalBytes>256*1024*1024)throw Error('DiagnosticArtifactExceeded');
   const file=resolve(directory,key+'-'+pass+'.json');await writeFile(file,bytes,{mode:0o600,flag:'wx'});
   report.artifacts.push({worker:result.worker,role,pass,path:file,bytes:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex'),events:result.events,invocations:result.invocations});
  }
 }
 report.stable=true;
}catch(error){report.error=/^Diagnostic[A-Za-z0-9]+$/.test(error.message)?error.message:'DiagnosticProbeFailed';process.exitCode=1;}
await writeFile(resolve(directory,'audit.json'),JSON.stringify(report,null,2),{mode:0o600,flag:'wx'});
const integrity_verified=await verifyPersistedProbe(directory,runs);if(!integrity_verified||report.resource_failures||report.invocation_errors||report.gaps)process.exitCode=1;
console.log(JSON.stringify({directory,qualification:false,stable:report.stable,integrity_verified,resource_failures:report.resource_failures,invocation_errors:report.invocation_errors,gaps:report.gaps,artifacts:report.artifacts.length,error:report.error}));
