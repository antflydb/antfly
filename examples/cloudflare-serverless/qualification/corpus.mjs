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

import{isDeepStrictEqual}from'node:util';
import{assertUnusedRecovery}from'./gate-provenance.mjs';
export const corpusId=id=>typeof id==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(id);
// A retry starts two new logical databases. Old immutable data and reports are
// retained. Only table incarnation/storage identities change, never algorithms,
// resource limits, offered load or consumed recovery markers.
export function planCorpus(run,configs,report,id){
 assertUnusedRecovery(run);
 const sides=report?.sides??[report?.baseline,report?.candidate],side=sides.find(s=>s?.run===run.id);
 const verifiedShort=Number.isFinite(report?.workload?.finished_at)&&report.soak_ms>0&&report.soak_ms<3600000&&report.baseline?.verification?.passed===true&&report.candidate?.verification?.passed===true;
 const closed=report?.sides?['complete','failed'].includes(report.stage)&&Number.isFinite(report.finished_at):Boolean(report?.fatal)||verifiedShort;
 if(!closed||!side||side.execution_id!==run.last_execution_id||report.execution_id!==run.last_execution_id)throw Error('CorpusExecutionStillActiveOrStale');
 if(!run.paused||configs.length!==4)throw Error('PauseCorpusDeliveryFirst');
 const routes=configs.map(c=>JSON.parse(c.vars?.ANTFLY_TENANTS??'{}'));
 if(!corpusId(id)||!routes.every(r=>isDeepStrictEqual(r,routes[0]))||Object.keys(routes[0]).sort().join(',')!=='a,b,c,d')throw Error('InvalidCorpusConfiguration');
 if(run.corpus&&!routes.every(r=>isDeepStrictEqual(r,run.corpus.tenants)))throw Error('CorpusProvenanceChanged');
 const roots=configs.map(c=>({incarnation:c.vars?.ANTFLY_INCARNATION,storage_id:c.vars?.COORDINATOR_STORAGE_ID??c.vars?.ANTFLY_INCARNATION}));
 if(!roots.every(r=>isDeepStrictEqual(r,roots[0]))||run.corpus?.root&&!roots.every(r=>isDeepStrictEqual(r,run.corpus.root)))throw Error('CorpusProvenanceChanged');
 const originalRoot=run.initial_corpus_root??roots[0];
 if(typeof originalRoot.incarnation!=='string'||!originalRoot.incarnation.length||typeof originalRoot.storage_id!=='string'||!originalRoot.storage_id.length)throw Error('InvalidCorpusConfiguration');
 const root={incarnation:originalRoot.incarnation+'-'+id,storage_id:originalRoot.storage_id+'-'+id};
 const base=run.initial_corpus_routes??routes[0];
 if(Object.keys(base).sort().join(',')!=='a,b,c,d'||Object.values(base).some(r=>!r||typeof r.incarnation!=='string'||!r.incarnation.length||typeof r.storage_id!=='string'||!r.storage_id.length))throw Error('InvalidCorpusConfiguration');
 const tenants=Object.fromEntries(Object.entries(base).map(([tenant,r])=>[tenant,{incarnation:r.incarnation+'-'+id,storage_id:r.storage_id+'-'+id}]));
 if([root,...Object.values(tenants)].some(r=>r.incarnation.length>256||r.storage_id.length>256))throw Error('CorpusIdentityTooLong');
 return{run:{...structuredClone(run),initial_corpus_root:structuredClone(originalRoot),initial_corpus_routes:structuredClone(base),corpus:{id,root,tenants},deployed:false,last_phase:'corpus-prepared'},configs:configs.map(c=>({...structuredClone(c),vars:{...c.vars,ANTFLY_INCARNATION:root.incarnation,COORDINATOR_STORAGE_ID:root.storage_id,ANTFLY_TENANTS:JSON.stringify(tenants)}}))};
}
