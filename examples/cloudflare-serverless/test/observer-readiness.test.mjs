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

import{test}from'node:test';import assert from'node:assert/strict';import{mkdtemp,rm,readFile}from'node:fs/promises';import{resolve}from'node:path';import{observerReadiness}from'../qualification/observer-readiness.mjs';
async function fixture(run){const dir=await mkdtemp(resolve('dist','readiness-test-'));try{let now=0,serial=0;const scopes=Array.from({length:8},(_,i)=>({source:'source-'+i,capture_id:'capture-'+i})),observer={scopes,version:'observer',observed_resource_failures:[]},ids=new Map(),versions=Object.fromEntries(scopes.map(s=>[s.source,'version-'+s.source]));const options={dir,versions,clock:()=>now,sleep:async ms=>{now+=ms;},call:async(_observer,path)=>{if(path==='/health')return{rejections_present:false};const source=path.split('/')[2],id=(++serial).toString(16).padStart(64,'0');if(!ids.has(source))ids.set(source,[]);ids.get(source).push(id);return{producer:source,status:401,attempt_id_sha256:id};},collect:async(_observer,scope)=>{const id=ids.get(scope.source).at(-2);return{source:scope.source,objects:[{key:'native',value:{records:[{attempt_id_sha256:id}]}},{key:'canonical',value:{rows:[{terminal:true,attempt_id_sha256:id,invocation_id_sha256:'invocation'}]}}]};},reconcile:async(_first,_second,scope)=>{assert.equal(scope.version,versions[scope.source]);assert.equal(scope.counterVersion,'observer');return{complete:true,failures:[],resource_failures:0};}};await run({dir,observer,options,ids});}finally{await rm(dir,{recursive:true,force:true});}}
test('readiness requires positive native and canonical evidence in two independent observations for every source',()=>fixture(async({observer,options,dir})=>{const report=await observerReadiness(observer,options);assert.equal(report.passed,true);assert.equal(report.attempts.length,1);assert.equal(report.attempts[0].captures.length,16);assert.equal(report.attempts[0].diagnostics.length,8);assert.equal(JSON.parse(await readFile(resolve(dir,'report.json'))).passed,true);}));
test('empty but reconciled sources cannot satisfy readiness and both bounded attempts remain recorded',()=>fixture(async({observer,options})=>{options.collect=async()=>({objects:[]});const report=await observerReadiness(observer,options);assert.equal(report.passed,false);assert.equal(report.attempts.length,2);assert.ok(report.attempts.every(a=>a.failure==='ObserverReadinessUnproven'));}));
test('startup population gap can recover in one fresh preflight without overwriting the first failure',()=>fixture(async({observer,options,ids})=>{const collect=options.collect;options.collect=async(o,s)=>ids.get(s.source).length===3?{objects:[]}:collect(o,s);const report=await observerReadiness(observer,options);assert.equal(report.passed,true);assert.equal(report.attempts.length,2);assert.equal(report.attempts[0].passed,false);assert.equal(report.attempts[1].passed,true);}));
test('recovered resource failures terminate readiness rather than retrying into a clean report',()=>fixture(async({observer,options})=>{options.reconcile=async()=>({complete:false,failures:['ObserverResourceFailure'],resource_failures:1});const report=await observerReadiness(observer,options);assert.equal(report.passed,false);assert.equal(report.attempts.length,1);assert.match(report.attempts[0].failure,/ResourceFailure/);}));
test('a canceled preflight preserves its failure and does not issue source probes',()=>fixture(async({observer,options,ids})=>{options.signal=AbortSignal.abort(Error('Canceled'));const report=await observerReadiness(observer,options);assert.equal(report.passed,false);assert.equal(report.attempts.length,1);assert.equal(ids.size,0);assert.match(report.attempts[0].failure,/Canceled/);}));
