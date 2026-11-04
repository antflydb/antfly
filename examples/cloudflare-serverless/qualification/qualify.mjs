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

import{spawn}from'node:child_process';
import{resolve}from'node:path';
import{fileURLToPath,pathToFileURL}from'node:url';
const directory=resolve(fileURLToPath(new URL('..',import.meta.url)));
const hosted={stack:['cold-pilot.mjs'],observer:['paired-observer-lifecycle.mjs'],pair:['prepare-independent-http.mjs','pair'],plan:['prepare-independent-http.mjs','plan'],run:['independent-http-trial.mjs'],reconcile:['reconcile-independent-http.mjs'],delivery:['monitor-independent-delivery.mjs'],'gate-prepare':['prepare-gate.mjs'],'gate-run':['run-gate.mjs'],'gate-lifecycle':['gate-lifecycle.mjs'],'gate-evaluate':['evaluate-gate.mjs'],evidence:['gate-evidence.mjs'],corpus:['prepare-corpus.mjs'],placement:['probe-placement.mjs']};
const contracts={engine:['benchmark.mjs'],embeddings:['model-fixture.mjs'],native:['native-host-contracts.mjs'],providers:['provider-contracts.mjs'],scale:['r2-scale.mjs']};
export const help=`Usage: node qualification/qualify.mjs COMMAND [arguments]

  local [options]                       Local workerd latency/workload runner
  hosted stack ACTION ...               Prepare/seal/deploy/seed/cleanup a paired stack
  hosted observer ACTION ...            Scoped observation/export lifecycle
  hosted pair BASELINE CANDIDATE OUT     Prepare the public pair
  hosted plan BASELINE CANDIDATE OUT ... Prepare a fresh public execution plan
  hosted run --plan=... --out=... ...    Run the independent HTTP client
  hosted delivery OBSERVER OUT          Monitor provider delivery through settlement
  hosted reconcile REPORT HISTORY OUT  Reconcile independent evidence offline
  hosted gate-prepare ACCOUNT CONTROL   Prepare the original full gate
  hosted gate-lifecycle GATE ACTION ... Deploy, seal, pause, resume or clean a gate
  hosted gate-run GATE TRIAL ...         Run one original gate trial
  hosted gate-evaluate GATE              Evaluate the original gate
  hosted evidence GATE ...               Bind completed fault and ANN evidence
  hosted corpus GATE TRIAL REPORT ...   Prepare a fresh paired corpus
  hosted placement ...                  Inspect placement using separate identities
  analyze REPORT OUT                    Attribute same-request phase profiles
  analyze --native REPORT CAPTURES OUT  Analyze received native records offline
  contracts engine|embeddings|native|providers|scale ...
                                        Native/provider/ANN correctness and quality
  artifacts [options]                   Dry-run-first local artifact retention

Paths and outputs are relative to examples/cloudflare-serverless.
Hosted lifecycle and real-provider contracts can create paid resources or delete scoped test data.
Nothing is deployed by help, local, analyze or route selection. Qualification is manual;
no CI integration is required. See docs/design/cloudflare-independent-http.md.
`;
export function qualificationCommand(argv){
 const args=[...argv],command=args.shift();let target;
 if(command==='local')target=['read-state-latency.mjs'];
 else if(command==='hosted'){const action=args.shift();if(Object.hasOwn(hosted,action))target=hosted[action];}
 else if(command==='contracts'){const action=args.shift();if(Object.hasOwn(contracts,action))target=contracts[action];}
 else if(command==='analyze')target=['analyze-request-phases.mjs'];
 else if(command==='artifacts')target=['prune-local-artifacts.py'];
 if(!target)throw Error('UnknownQualificationCommand');
 return{program:target[0].endsWith('.py')?'python3':process.execPath,args:[resolve(directory,'qualification',target[0]),...target.slice(1),...args],cwd:directory};
}
const usage={
 local:`Local options: --twofold --only=serving-durable,serving-resident --documents=512
  --delay=20 --setup-delay=0 --rounds=8 --offered-reads=128 --offered-rps=16
  --offered-write-every=16 --reverse --out=FRESH_DIRECTORY
  Cold: --cold-restart [--cold-startup]. Bursts: --model=fixed-bursty.
  Keep each execution order in a fresh directory. These are local diagnostics.`,
 artifacts:`Retention defaults to a dry-run inventory. --apply performs guarded pruning.
  It preserves exact approved control modules and refuses protected credential paths.
  No cloud cleanup is performed.`,
 'hosted run':`Client options: --plan=PUBLIC_PLAN --out=FRESH_DIRECTORY
  --claims=PERSISTENT_CLAIM_DIRECTORY --dedicated-client (or --local-diagnostic)
  Tokens are supplied through the environment, never through the public plan.`,
 'contracts providers':`Provider options: --provider=s3|gcs --region=REGION [--project=PROJECT]
  [--profile=PROFILE] [--out=DIRECTORY] [--versioned=true] [--wal-gc=true]
  This creates owned disposable resources. --cleanup-only=REPORT resumes cleanup.`,
};
export async function runQualification(argv,{start=spawn}={}){
 if(!argv.length||argv.includes('--help')||argv[0]==='help'){const key=argv[0]==='local'||argv[0]==='artifacts'?argv[0]:argv.slice(0,2).join(' ');process.stdout.write(help+(Object.hasOwn(usage,key)?'\n'+usage[key]+'\n':''));return 0;}
 const command=qualificationCommand(argv);
 return new Promise((done,fail)=>{
  const child=start(command.program,command.args,{cwd:command.cwd,stdio:'inherit'}),signals=['SIGINT','SIGTERM'];
  const forward=signal=>child.kill(signal),handlers=signals.map(signal=>{const handler=()=>forward(signal);process.on(signal,handler);return handler;});
  const cleanup=()=>signals.forEach((signal,i)=>process.off(signal,handlers[i]));
  child.once('error',error=>{cleanup();fail(error);});
  child.once('exit',(code,signal)=>{cleanup();done(code??(signal==='SIGINT'?130:143));});
 });
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){try{process.exitCode=await runQualification(process.argv.slice(2));}catch(error){console.error(error.message);process.exitCode=1;}}
