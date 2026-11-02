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

// Qualification binds a committed source revision to preserved executable
// bytes. A cached success line alone is not an executable provenance record.
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {resolve,relative,sep,join} from 'node:path';
import {mkdir,writeFile,rm} from 'node:fs/promises';
import {randomUUID} from 'node:crypto';
const exec=promisify(execFile);
export async function sourceSnapshot(repo){
 const git=async args=>(await exec('git',args,{cwd:repo,maxBuffer:8*1024*1024})).stdout;
 const before=(await git(['rev-parse','HEAD'])).trim();
 const status=await git(['status','--porcelain=v1','--untracked-files=all']);
 const revision=(await git(['rev-parse','HEAD'])).trim();
 if(before!==revision)throw Error('QualificationSourceChanged');
 return {revision,status};
}
export function assertCommittedSource(snapshot){
 if(!snapshot.revision||snapshot.status!=='')throw Error('QualificationSourceNotCommitted');
}
export function assertSameSource(before,after){
 assertCommittedSource(before);
 if(before.revision!==after.revision||after.status!=='')throw Error('QualificationSourceChanged');
}
export function executedNativeBinaries(log,cacheDir,cwd){
 const backslash=String.fromCharCode(92);
 const escape=value=>[...value].map(char=>'.*+?^${}()|[]'.includes(char)||char===backslash?backslash+char:char).join('');
 const absolute=resolve(cacheDir),local=relative(cwd,absolute).split(sep).join('/');
 const prefixes=[absolute,local,'./'+local];
 const found=new Map();
 for(const prefix of new Set(prefixes)){
  const pattern=new RegExp('^failed command: ("?)('+escape(prefix)+'/o/[a-f0-9]+/test(?:-local)?)\\1(?=\\s|$)','gm');
  for(const match of log.matchAll(pattern)){const path=resolve(cwd,match[2]);found.set(path,{path,command_path:match[2]});}
 }
 return [...found.values()];
}


export async function withCacheGuard(roots,run){
 const markers=[];let heartbeat,pending=Promise.resolve();
 try{
  for(const root of new Set(roots)){await mkdir(root,{recursive:true});const marker=join(root,'.qualification-active-'+randomUUID());await writeFile(marker,'active\n');markers.push(marker);}
  heartbeat=setInterval(()=>{pending=pending.then(()=>Promise.all(markers.map(marker=>writeFile(marker,'active\n')))).catch(()=>{});},10000);
  return await run();
 }finally{clearInterval(heartbeat);await pending;await Promise.all(markers.map(marker=>rm(marker,{force:true})));}
}
