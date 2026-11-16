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

import{test}from'node:test';
import assert from'node:assert/strict';
import{EventEmitter}from'node:events';
import{spawnSync}from'node:child_process';
import{resolve,basename}from'node:path';
import{qualificationCommand,runQualification}from'../qualification/qualify.mjs';

test('qualification CLI exposes one local and hosted path without accepting arbitrary programs',()=>{
 for(const[args,file,prefix]of[[['local'],'read-state-latency.mjs',[]],[['hosted','stack'],'cold-pilot.mjs',[]],[['hosted','pair'],'prepare-independent-http.mjs',['pair']],[['hosted','plan'],'prepare-independent-http.mjs',['plan']],[['hosted','run'],'independent-http-trial.mjs',[]],[['hosted','gate-run'],'run-gate.mjs',[]],[['contracts','providers'],'provider-contracts.mjs',[]],[['artifacts'],'prune-local-artifacts.py',[]]]){
  const value=qualificationCommand([...args,'literal argument; $(no-shell)']);assert.equal(basename(value.args[0]),file);assert.deepEqual(value.args.slice(1),[...prefix,'literal argument; $(no-shell)']);assert.equal(value.cwd,resolve('.'));
 }
 for(const args of[['unknown'],['hosted','__proto__'],['hosted','toString'],['contracts','constructor']])assert.throws(()=>qualificationCommand(args),/UnknownQualificationCommand/);
});

test('qualification CLI help has no child or cloud side effects',()=>{
 const result=spawnSync(process.execPath,[resolve('qualification/qualify.mjs'),'hosted','stack','--help'],{encoding:'utf8'});assert.equal(result.status,0,result.stderr);assert.match(result.stdout,/hosted gate-run/);assert.match(result.stdout,/Qualification is manual/);assert.equal(result.stderr,'');
 const retention=spawnSync(process.execPath,[resolve('qualification/qualify.mjs'),'artifacts','--help'],{encoding:'utf8'});assert.equal(retention.status,0);assert.match(retention.stdout,/--apply/);assert.match(retention.stdout,/No cloud cleanup/);
 const unknown=spawnSync(process.execPath,[resolve('qualification/qualify.mjs'),'hosted','not-a-route'],{encoding:'utf8'});assert.equal(unknown.status,1);assert.match(unknown.stderr,/UnknownQualificationCommand/);
});

test('qualification CLI preserves child failures and releases signal forwarding',async()=>{
 const before=['SIGINT','SIGTERM'].map(s=>process.listenerCount(s));let killed;
 const code=await runQualification(['hosted','run','--plan=owned plan.json'],{start(program,args,options){assert.equal(program,process.execPath);assert.deepEqual(args.slice(1),['--plan=owned plan.json']);assert.equal(options.shell,undefined);assert.equal(options.stdio,'inherit');const child=new EventEmitter();child.kill=signal=>{killed=signal;};queueMicrotask(()=>{process.emit('SIGTERM');child.emit('exit',7,null);});return child;}});
 assert.equal(code,7);assert.equal(killed,'SIGTERM');assert.deepEqual(['SIGINT','SIGTERM'].map(s=>process.listenerCount(s)),before);
});
