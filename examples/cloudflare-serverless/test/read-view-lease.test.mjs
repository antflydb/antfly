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

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {FencedReadLeases,issueReadLease,bindReadLease,readLeaseRemaining,readLeasePolicy,visibilityFence} from '../../../zig/lib/objectstore/wasm/read-view-lease.mjs';
import {prepareReadAdmission} from '../read-admission.mjs';
import {CriticalPath} from '../trace.mjs';
const scope='a'.repeat(64),fence=n=>'01'+scope+BigInt(n).toString(16).padStart(16,'0'),utf8=new TextEncoder();
function fixture(){let wall=100000,mono=0;const cache=new FencedReadLeases({scope,clock:()=>wall,monotonic:()=>mono});return{cache,wall:()=>wall,mono:()=>mono,advance(ms){wall+=ms;mono+=ms;},set(w,m){wall=w;mono=m;},view(lsn=7,version='0',extras=[]){return issueReadLease({format:1,version,entries:[['wal',{etag:'wal',body:utf8.encode('{"format":4,"latest_lsn":'+lsn+'}')}],...extras]},{scope,walKey:'wal',pinPrefix:'pin/',policy:readLeasePolicy,observedAt:wall,now:wall});}};}
test('fenced leases require native certification and preserve exact u64 namespace fences',async()=>{
 const f=fixture(),{cache}=f,lsn=18446744073709551615n,view=await cache.run(fence(lsn),'native',()=>f.view(lsn));
 assert.equal(view.lease.visible_lsn,String(lsn));assert.equal(cache.get(fence(0)),null);
 assert.equal(cache.confirm(view,'01'+'b'.repeat(64)+'ffffffffffffffff'),false);
 assert.equal(cache.confirm(view,fence(lsn-1n)),false);assert.equal(cache.confirm(view,fence(lsn)),true);
 const copy=cache.get(fence(lsn));copy.entries[0][1].body[0]=0;assert.notEqual(cache.get(fence(lsn)).entries[0][1].body[0],0);
 assert.equal(visibilityFence(fence(lsn),scope),lsn);assert.equal(visibilityFence(fence(lsn),scope.toUpperCase()),null);
 assert.equal(cache.pending.size,0);assert.ok(cache.bytes<=32768);
});
test('read leases expire at the original deadline, including after delayed native validation and reuse',async()=>{
 const f=fixture(),{cache}=f;const view=await cache.run(fence(7),'native',()=>f.view());f.advance(20000);assert.equal(cache.confirm(view,fence(7)),true);
 f.advance(9999);assert.ok(cache.get(fence(7)));f.advance(1);assert.equal(cache.get(fence(7)),null);assert.equal(cache.bytes,0);
});
test('clock regression permanently invalidates a bound read lease and cannot resurrect it',()=>{
 for(const field of['wall','monotonic']){const f=fixture(),view=f.view();assert.equal(bindReadLease(view,{wall:100000,monotonic:0}),true);assert.equal(readLeaseRemaining(view,{wall:100100,monotonic:100}),29900);assert.equal(readLeaseRemaining(view,{wall:field==='wall'?100099:100101,monotonic:field==='monotonic'?99:101}),0);assert.equal(bindReadLease(view,{wall:100200,monotonic:200}),false);assert.equal(readLeaseRemaining(view,{wall:100200,monotonic:200}),0);}
});
test('leases require persisted GC policy, valid WAL and a protecting manifest pin',()=>{
 const f=fixture(),view=f.view(7,'3',[['pin/3',{etag:'pin',body:utf8.encode('110000000000')}]]);assert.equal(view.lease.expires_at_ms,110000);
 assert.equal(f.view(7,'3').lease,undefined);
 const raw={version:'0',entries:[['wal',{body:utf8.encode('{"format":99,"latest_lsn":7}')}]]};assert.equal(issueReadLease(raw,{scope,walKey:'wal',policy:readLeasePolicy,observedAt:100000,now:100000}),raw);
 assert.equal(issueReadLease(raw,{scope,walKey:'wal',observedAt:100000,now:100000}),raw);
 assert.equal(f.view(18446744073709551616n).lease,undefined);
});
test('coalesced fenced pin admissions share protected bytes but never retain a completed promise',async()=>{
 const f=fixture(),{cache}=f;let release,calls=0;const held=new Promise(r=>release=r),load=async()=>{calls++;await held;return f.view();};
 const owners=Array.from({length:32},()=>cache.run(fence(7),'rpc',load));release();const values=await Promise.all(owners);assert.equal(calls,1);assert.equal(cache.joins,31);assert.equal(cache.pending.size,0);assert.equal(new Set(values).size,32);assert.equal(cache.get(fence(7)),null);
 assert.equal(cache.confirm(values[0],fence(7)),true);assert.ok(cache.get(fence(7)));assert.equal(cache.get(fence(8)),null);
 const unprotected=new FencedReadLeases({scope});calls=0;await Promise.all(Array.from({length:8},()=>unprotected.run(fence(7),'rpc',async()=>{calls++;return{format:1,version:'0',entries:[]};})));assert.equal(calls,8);assert.equal(unprotected.pending.size,0);
});
test('late native confirmations cannot restore invalidated views or rewind newer retained state',async()=>{
 const f=fixture(),{cache}=f,older=await cache.run(fence(7),'native',()=>f.view(7)),newer=await cache.run(fence(8),'native',()=>f.view(8));
 assert.equal(cache.confirm(newer,fence(8)),true);assert.equal(cache.confirm(older,fence(7)),false);cache.clear();assert.equal(cache.confirm(newer,fence(8)),false);
 await assert.rejects(cache.run(fence(7),'native',()=>{throw Error('ImmutableLogFenced');}),/ImmutableLogFenced/);assert.equal(cache.pending.size,0);
 const large=await cache.run(fence(7),'native',()=>({...f.view(),immutable:[['large',{etag:'large',body:new Uint8Array(32768)}]]}));assert.equal(cache.confirm(large,fence(7)),false);assert.equal(cache.bytes,0);
});
test('latest, explicit fresh, cursors and exact ANN generations bypass fenced view reuse',async()=>{
 const calls=[],env={COORDINATOR_BACKEND:'r2'},views={open:async options=>{calls.push(options);return{format:1,entries:[]};}};
 for(const input of[{op:'lookup',required_visibility:fence(7)},{op:'latest',required_visibility:fence(7)},{op:'lookup',fresh:true,required_visibility:fence(7)},{op:'search',exact_state:{generation:'3'},required_visibility:fence(7)},{op:'count'},{op:'status'}])await prepareReadAdmission(env,input,new CriticalPath(),views);
 assert.deepEqual(calls.map(c=>c.allow_cached),[true,false,false,false,false,false]);assert.equal(await prepareReadAdmission(env,{op:'lookup',cursor:'cursor'},new CriticalPath(),views),undefined);
 env.FENCED_READ_LEASES='false';await prepareReadAdmission(env,{op:'lookup',required_visibility:fence(7)},new CriticalPath(),views);assert.equal(calls.at(-1).allow_cached,false);
});

test('RPC pin fallback coalesces fenced callers and charges only the physical owner',async()=>{
 const cache=new FencedReadLeases({scope});let rpc=0,fail=true;const view=()=>{const now=Date.now();return issueReadLease({format:1,version:'0',entries:[['wal',{etag:'wal',body:utf8.encode('{"format":4,"latest_lsn":7}')}]]},{scope,walKey:'wal',policy:readLeasePolicy,observedAt:now,now});};
 const views={open:o=>cache.run(o.required_visibility,'native',()=>{if(fail)throw Error('ReadSnapshotNeedsPin');return view();},o.onLease),admit:(o,load)=>cache.run(o.required_visibility,'rpc',load,o.onLease),confirm:(v,t)=>cache.confirm(v,t)},env={COORDINATOR_BACKEND:'r2',ANTFLY_INCARNATION:'scope',COMMITS:{getByName:()=>({openReadView:async()=>{rpc++;await new Promise(r=>setTimeout(r,5));return view();}})}};
 const admissions=await Promise.all(Array.from({length:16},()=>prepareReadAdmission(env,{op:'lookup',required_visibility:fence(7)},new CriticalPath(),views)));
 assert.equal(rpc,1);assert.equal(admissions.reduce((n,a)=>n+a.physical.rpc_calls,0),1);assert.equal(admissions.filter(a=>a.physical.read_view_lease_join===1).length,15);assert.ok(admissions.every(a=>a.physical.read_admission_fallback_reason==='ReadSnapshotNeedsPin'));
 admissions[0].confirm(fence(7));const cached=await prepareReadAdmission(env,{op:'lookup',required_visibility:fence(7)},new CriticalPath(),views);assert.equal(cached.physical.read_view_lease_hit,1);assert.equal(cached.physical.rpc_calls,0);assert.equal(rpc,1);assert.equal(cache.pending.size,0);
});

test('retained leases project protocol bytes and reject unchargeable metadata',async()=>{
 const f=fixture(),view=await f.cache.run(fence(7),'native',()=>({...f.view(),ctx:{promise:Promise.resolve()},extra:new Uint8Array(1024*1024)}));view.lease.ctx={promise:Promise.resolve()};view.entries[0][1].extra=new Uint8Array(1024*1024);assert.equal(f.cache.confirm(view,fence(7)),true);const hit=f.cache.get(fence(7));assert.equal(hit.ctx,undefined);assert.equal(hit.extra,undefined);assert.equal(hit.lease.ctx,undefined);assert.equal(hit.entries[0][1].extra,undefined);assert.ok(f.cache.bytes<32768);
 f.cache.clear();for(const change of[v=>v.entries[0][1].contentType={length:NaN},v=>v.version='invalid',v=>v.format=99]){const candidate=await f.cache.run(fence(7),'native',()=>f.view());change(candidate);assert.equal(f.cache.confirm(candidate,fence(7)),false);assert.equal(f.cache.bytes,0);}
});


test('conditional current-head cuts use bounded GC grace while historical reads persist pins',async()=>{
 const {captureReadView}=await import('../read-view.mjs');
 const {conditionalReadLeasePolicy}=await import('../../../zig/lib/objectstore/wasm/read-view-lease.mjs');
 const keys={head:'head',wal:'antfly/wal-v1/'+scope+'/HEAD',floor:'floor',progress:'progress/'};
 const row=value=>({etag:'e',body:utf8.encode(value),contentType:null});
 const values=[row('3'),row('{"format":4,"latest_lsn":7}'),row('0')];let gets=0,puts=0,wall=100000;
 const tx={getMany:async()=>values,get:async()=>{gets++;return undefined;},put(){puts++;}};
 const options={read_lease_policy:conditionalReadLeasePolicy,allow_ephemeral_pin:true,lease_observed_at_ms:100000};
 const current=await captureReadView(tx,keys,options,()=>wall);
 assert.equal(gets,0);assert.equal(puts,0);assert.equal(current.lease.expires_at_ms,130000);
 wall+=20000;const delayed=await captureReadView(tx,keys,options,()=>wall);assert.equal(delayed.lease.expires_at_ms,130000);
 await captureReadView(tx,keys,{...options,version:'2'},()=>wall);assert.equal(gets,1);assert.equal(puts,1);
 await captureReadView(tx,keys,{...options,read_lease_policy:readLeasePolicy},()=>wall);assert.equal(gets,2);assert.equal(puts,2);
 await captureReadView(tx,keys,{...options,durable_pin:true},()=>wall);assert.equal(gets,3);assert.equal(puts,3);
 wall=130000;await assert.rejects(captureReadView(tx,keys,options,()=>wall),/ReadSnapshotNeedsAdmission/);
 wall=99999;await assert.rejects(captureReadView(tx,keys,options,()=>wall),/ReadSnapshotNeedsAdmission/);
});

test('conditional cursor-producing and exact-generation reads request durable protection',async()=>{
 const options=[],views={open:async o=>{options.push(o);return{format:1,entries:[]};}};
 for(const op of['lookup','count','list','search'])await prepareReadAdmission({COORDINATOR_BACKEND:'r2',METADATA_ROOT_BACKEND:'conditional'},{op,required_visibility:fence(7)},new CriticalPath(),views);
 assert.deepEqual(options.map(o=>o.durable_pin),[false,false,true,true]);assert.deepEqual(options.map(o=>o.allow_cached),[true,true,false,false]);
});

for(const format of[5,6])test(`native certification can retain packed/deferred WAL format ${format} without extending its lease`,async()=>{
 const f=fixture(),load=()=>issueReadLease({format:1,version:'0',entries:[['wal',{etag:'packed',body:utf8.encode(JSON.stringify({format,latest_lsn:7,packed_pages:true,...(format===6?{deferred_indexes:true}:{} )}))}]]},{scope,walKey:'wal',pinPrefix:'pin/',policy:readLeasePolicy,observedAt:f.wall(),now:f.wall()});
 const view=await f.cache.run(fence(7),'native',load);assert.equal(f.cache.get(fence(7)),null);assert.equal(f.cache.confirm(view,fence(7)),true);assert.ok(f.cache.get(fence(7)));assert.equal(f.cache.get(fence(8)),null);f.advance(30000);assert.equal(f.cache.get(fence(7)),null);
});
