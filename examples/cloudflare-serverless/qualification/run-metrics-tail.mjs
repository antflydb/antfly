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

import{spawn}from'node:child_process';import{readFile}from'node:fs/promises';import{resolve}from'node:path';import{tailFrames}from'./metrics-tail.mjs';
const[manifest,role]=process.argv.slice(2),run=JSON.parse(await readFile(manifest));if(!/^antfly-1278-g-mvnf29dvfe41-[1-3]-[bc]$/.test(run.id))throw Error('Tail scope mismatch');const service=run.services.find(s=>s.role===role);if(!service)throw Error('Unknown tail role');
const child=spawn(process.execPath,[resolve('node_modules/wrangler/bin/wrangler.js'),'tail',service.name,'--config',service.config,'--format','json'],{stdio:['ignore','pipe','ignore'],env:{...process.env,WRANGLER_SEND_METRICS:'false'}});
const parse=tailFrames(safe=>process.stdout.write(JSON.stringify(safe)+'\n'));child.stdout.setEncoding('utf8');child.stdout.on('data',parse);child.on('error',()=>{process.exitCode=1;});child.on('exit',code=>{process.exitCode=code??1;});
for(const signal of['SIGTERM','SIGINT'])process.on(signal,()=>child.kill(signal));
