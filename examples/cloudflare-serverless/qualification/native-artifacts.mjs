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

// Controlled actual-workerd/WASM experiment. Injected delays characterize
// dependency depth, including preparation; they do not measure cloud SLOs.
import{mkdir,writeFile,readFile,link,lstat}from'node:fs/promises';
import{join,basename}from'node:path';import{createHash}from'node:crypto';
// Copy each immutable module once, then hard-link case-local evidence names.
// Never link the mutable build output: a later rebuild must not alter a trial.
export async function retainNativeModules(modules,folder,cache){
 await mkdir(cache,{recursive:true});const hashes={};
 for(const module of modules){
  const name=basename(module.path),hash=createHash('sha256').update(module.contents).digest('hex'),artifact=join(cache,hash+'.wasm');
  try{await writeFile(artifact,module.contents,{flag:'wx',mode:0o444});}catch(error){if(error.code!=='EEXIST')throw error;const stat=await lstat(artifact);if(!stat.isFile()||stat.isSymbolicLink()||createHash('sha256').update(await readFile(artifact)).digest('hex')!==hash)throw Error('NativeArtifactHashMismatch');}
  await link(artifact,join(folder,name));hashes[name]=hash;
 }
 return hashes;
}
