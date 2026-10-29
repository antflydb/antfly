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

// Provider-neutral host contract for native, visibility-fenced read snapshots.
// No transport, request context or completed promise is retained in a lease.
export const readLeasePolicy=Object.freeze({format:1,root_lifetime_ms:30000,gc_grace_ms:60000});
const text=new TextDecoder(),bindings=new WeakMap();
const u64=value=>{try{const n=typeof value==='bigint'?value:typeof value==='string'&&/^(0|[1-9][0-9]{0,19})$/.test(value)?BigInt(value):Number.isSafeInteger(value)&&value>=0?BigInt(value):null;return n!==null&&n<=18446744073709551615n?n:null;}catch{return null;}};
export const validReadLeasePolicy=p=>p?.format===1&&p.root_lifetime_ms===30000&&p.gc_grace_ms===60000;
export function visibilityFence(value,scope){if(typeof value!=='string'||!/^01[a-f0-9]{80}$/i.test(value)||value.slice(2,66).toLowerCase()!==scope)return null;return BigInt('0x'+value.slice(66));}
export function issueReadLease(view,{scope,walKey,pinPrefix,policy,observedAt,now}){
 if(!validReadLeasePolicy(policy)||!Number.isSafeInteger(observedAt)||observedAt<=0||!Number.isSafeInteger(now)||now<observedAt||!Array.isArray(view?.entries))return view;
 try{const rows=new Map(view.entries),record=rows.get(walKey);if(!(record?.body instanceof Uint8Array)||record.body.byteLength>4096)return view;
  const head=JSON.parse(text.decode(record.body),(key,value,context)=>key==='latest_lsn'&&context?.source&&/^[0-9]+$/.test(context.source)?BigInt(context.source):value),lsn=u64(head.latest_lsn);if(lsn===null||![1,2,3,4].includes(head.format)||!/^[a-f0-9]{64}$/.test(scope))return view;
  let expires=observedAt+policy.root_lifetime_ms;
  if(view.version!=='0'){const pin=rows.get(pinPrefix+view.version);if(!(pin?.body instanceof Uint8Array))return view;const ns=u64(text.decode(pin.body).trim());if(ns===null)return view;expires=Math.min(expires,Number(ns/1000000n));}
  if(expires<=now)return view;
  return{...view,lease:{format:1,scope,visible_lsn:String(lsn),observed_at_ms:observedAt,expires_at_ms:expires,max_age_ms:policy.root_lifetime_ms}};
 }catch{return view;}
}
export function bindReadLease(view,{wall=Date.now(),monotonic=performance.now(),started=monotonic}={}){
 const prior=bindings.get(view);if(prior?.invalid||![wall,monotonic,started].every(Number.isFinite))return false;
 const l=view?.lease,lsn=u64(l?.visible_lsn);if(l?.format!==1||lsn===null||!/^[a-f0-9]{64}$/.test(l.scope??'')||!Number.isSafeInteger(l.observed_at_ms)||!Number.isSafeInteger(l.expires_at_ms)||l.expires_at_ms<=l.observed_at_ms||l.expires_at_ms-l.observed_at_ms>30000||l.max_age_ms!==30000||wall>=l.expires_at_ms)return false;
 const remaining=Math.min(l.expires_at_ms-wall,30000-(monotonic-started));if(remaining<=0)return false;
 bindings.set(view,{deadline:Math.min(prior?.deadline??Infinity,monotonic+remaining),wall:prior?.wall??wall,monotonic:prior?.monotonic??monotonic});return true;
}
export function readLeaseRemaining(view,{wall=Date.now(),monotonic=performance.now()}={}){const l=view?.lease,b=bindings.get(view);if(!l||!b||b.invalid)return 0;if(!Number.isFinite(wall)||!Number.isFinite(monotonic)||wall<b.wall||monotonic<b.monotonic){b.invalid=true;return 0;}b.wall=wall;b.monotonic=monotonic;const remaining=Math.max(0,Math.min(l.expires_at_ms-wall,b.deadline-monotonic));if(!remaining)b.invalid=true;return remaining;}
function charge(view){let n=512;for(const list of[view.entries??[],view.immutable??[]]){if(!Array.isArray(list)||list.length>16)return Infinity;for(const row of list){if(!Array.isArray(row)||row.length!==2||typeof row[0]!=='string')return Infinity;const[k,r]=row;n+=k.length*2+128;if(r){if(!(r.body instanceof Uint8Array)||typeof r.etag!=='string')return Infinity;n+=r.body.byteLength+r.etag.length*2+(r.contentType?.length??0)*2+128;}}}return n;}
export class FencedReadLeases{
 constructor({scope,clock=Date.now,monotonic=()=>performance.now(),maxBytes=32768}={}){if(!/^[a-f0-9]{64}$/.test(scope??'')||!Number.isSafeInteger(maxBytes)||maxBytes<0||maxBytes>32768)throw Error('InvalidReadLeaseCache');this.scope=scope;this.clock=clock;this.monotonic=monotonic;this.maxBytes=maxBytes;this.pending=new Map();this.candidates=new WeakMap();this.generation=0;this.bytes=0;this.hits=0;this.joins=0;}
 clear(){this.current=null;this.bytes=0;this.generation++;}
 attach(view,anchor,generation){if(!bindReadLease(view,{wall:this.clock(),monotonic:this.monotonic(),started:anchor})||view.lease.scope!==this.scope)return false;this.candidates.set(view,{anchor,generation});return true;}
 get(fence){const wanted=visibilityFence(fence,this.scope),current=this.current;if(wanted===null||!current)return null;const remaining=readLeaseRemaining(current.view,{wall:this.clock(),monotonic:this.monotonic()});if(!remaining){this.clear();return null;}if(wanted>BigInt(current.view.lease.visible_lsn))return null;const view=structuredClone(current.view);if(!this.attach(view,current.anchor,this.generation))return null;this.hits++;return view;}
 async run(fence,kind,loader,onLease){const wanted=visibilityFence(fence,this.scope);if(wanted===null)return loader();const hit=this.get(fence);if(hit){onLease?.({read_view_lease_hit:1});return hit;}const key=kind+'/'+fence.toLowerCase();let entry=this.pending.get(key),owner=!entry;if(entry&&entry.members>=64||!entry&&this.pending.size>=8)throw Error('CoordinatorCommitBusy');if(!entry){entry={members:0,anchor:this.monotonic(),generation:this.generation};entry.promise=Promise.resolve().then(loader);this.pending.set(key,entry);}entry.members++;
  try{const value=await entry.promise,view=owner?value:structuredClone(value);if(this.attach(view,entry.anchor,entry.generation)){if(!owner){this.joins++;onLease?.({read_view_lease_join:1});}return view;}return owner?view:await loader();}finally{entry.members--;if(owner&&this.pending.get(key)===entry)this.pending.delete(key);}
 }
 confirm(view,token){const c=this.candidates.get(view);if(!c||c.generation!==this.generation||visibilityFence(token,this.scope)!==u64(view.lease?.visible_lsn)||!readLeaseRemaining(view,{wall:this.clock(),monotonic:this.monotonic()}))return false;const bytes=charge(view);if(bytes>this.maxBytes)return false;if(this.current&&BigInt(this.current.view.lease.visible_lsn)>BigInt(view.lease.visible_lsn))return false;const copy=structuredClone(view);if(!this.attach(copy,c.anchor,this.generation))return false;this.current={view:copy,anchor:c.anchor};this.bytes=bytes;return true;}
}
