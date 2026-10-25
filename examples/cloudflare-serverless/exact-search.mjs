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

// The only public entry to native exact-scan state. MAC verification happens
// before any read RPC; partial heaps never appear as completed search results.
const utf8=new TextEncoder(),text=new TextDecoder(),ttl=240000,lifetime=3600000,maxPayload=128*1024;
const keys=new Map();
function base64(bytes){let s='';for(let i=0;i<bytes.length;i+=8192)s+=String.fromCharCode(...bytes.subarray(i,i+8192));return btoa(s).replaceAll('+','-').replaceAll('/','_').replace(/=+$/,'');}
function unbase64(value){if(!/^[A-Za-z0-9_-]+$/.test(value))throw Error('InvalidExactScanCursor');const raw=atob(value.replaceAll('-','+').replaceAll('_','/'));return Uint8Array.from(raw,c=>c.charCodeAt(0));}
async function key(env){
 const secret=env.EXACT_SCAN_CURSOR_KEY??env.API_TOKEN;if(typeof secret!=='string'||!secret.length)throw Error('ExactScanCursorKeyRequired');
 if(!keys.has(secret)){if(keys.size>=8)keys.delete(keys.keys().next().value);keys.set(secret,crypto.subtle.importKey('raw',utf8.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign','verify']));}
 return keys.get(secret);
}
async function identity(env,input){
 const query={namespace:env.ANTFLY_INCARNATION,mode:input.mode,vector:input.vector,limit:input.limit??10,fresh:input.fresh??false,required_visibility:input.required_visibility??null};
 return base64(new Uint8Array(await crypto.subtle.digest('SHA-256',utf8.encode(JSON.stringify(query)))));
}
async function seal(env,payload){
 const bytes=utf8.encode(JSON.stringify(payload));if(bytes.length>maxPayload)throw Error('ExactScanStateTooLarge');
 const signature=new Uint8Array(await crypto.subtle.sign('HMAC',await key(env),bytes));return base64(bytes)+'.'+base64(signature);
}
async function open(env,cursor,query,now){
 if(typeof cursor!=='string'||cursor.length>Math.ceil(maxPayload*4/3)+64)throw Error('InvalidExactScanCursor');
 const parts=cursor.split('.');if(parts.length!==2)throw Error('InvalidExactScanCursor');let bytes,signature;
 try{bytes=unbase64(parts[0]);signature=unbase64(parts[1]);}catch{throw Error('InvalidExactScanCursor');}
 if(bytes.length>maxPayload||signature.length!==32||!await crypto.subtle.verify('HMAC',await key(env),signature,bytes))throw Error('InvalidExactScanCursor');
 let payload;try{payload=JSON.parse(text.decode(bytes));}catch{throw Error('InvalidExactScanCursor');}
 if(payload.protocol!=='antfly-exact-v1'||payload.namespace!==env.ANTFLY_INCARNATION||payload.query!==query||!payload.state||payload.state.format!==1)throw Error('ExactScanBindingMismatch');
 if(!Number.isSafeInteger(payload.started)||!Number.isSafeInteger(payload.deadline)||payload.deadline-payload.started!==lifetime||payload.started>now+30000||!Number.isSafeInteger(payload.expires)||payload.expires<=now||payload.expires>payload.deadline||payload.expires>now+ttl+30000||now>=payload.deadline)throw Error('ExactScanExpired');
 return payload;
}
export function isExactSearch(env,input){return env.COORDINATOR_BACKEND==='r2'&&env.PAGED_PROJECTIONS==='true'&&input.op==='search'&&input.mode==='vector'&&input.search_effort===1;}
export function rejectPrivateScanInput(input){for(const name of ['exact_scan','exact_state','exact_records'])if(Object.hasOwn(input,name))throw Error('PrivateQueryStateUnsupported');}
export async function exactSearch(env,input,execute,{signal,now=Date.now}={}){
 rejectPrivateScanInput(input);
 if(input.filters?.length)throw Error('FreshQueryCapabilityUnsupported');
 if(!Array.isArray(input.vector)||input.vector.length<1||input.vector.length>16384||input.vector.some(v=>!Number.isFinite(v)))throw Error('InvalidVectorQuery');
 const limit=input.limit??10;if(!Number.isInteger(limit)||limit<1||limit>100)throw Error('QueryCandidateBudgetExceeded');
 const maxSteps=Number(env.EXACT_SCAN_INLINE_STEPS??4),inlineMs=Number(env.EXACT_SCAN_INLINE_MS??1000),maxRecords=Number(env.EXACT_SCAN_RECORDS??512);
 if(!Number.isInteger(maxRecords)||maxRecords<1||maxRecords>512||!Number.isInteger(maxSteps)||maxSteps<1||maxSteps>8||!Number.isInteger(inlineMs)||inlineMs<0||inlineMs>2000)throw Error('InvalidExactScanBudget');
 const started=now(),query=await identity(env,input);
 let payload=input.cursor?await open(env,input.cursor,query,started):{protocol:'antfly-exact-v1',namespace:env.ANTFLY_INCARNATION,query,started,deadline:started+lifetime,expires:started+ttl,state:null};
 let result,steps=0,records=0;
 do{
  if(signal?.aborted)throw Error('Canceled');if(now()>=payload.expires)throw Error('ExactScanExpired');
  const before=payload.state;
  result=await execute({...input,cursor:null,fresh:before?false:input.fresh,exact_scan:true,exact_state:before,exact_records:maxRecords});
  steps++;records+=result.step_records??0;
  if(typeof result.complete!=='boolean'||!Array.isArray(result.hits)||!result.complete&&(!result.exact_state||result.hits.length))throw Error('InvalidExactScanResponse');
  if(result.complete)break;
  if(before&&(before.generation!==result.exact_state.generation||before.cutoff!==result.exact_state.cutoff||JSON.stringify(before.query)!==JSON.stringify(result.exact_state.query)||BigInt(result.exact_state.scanned)<=BigInt(before.scanned)))throw Error('InvalidExactScanProgress');
  payload={...payload,state:result.exact_state,expires:Math.min(now()+ttl,payload.deadline)};
 }while(steps<maxSteps&&now()-started<inlineMs);
 if(signal?.aborted)throw Error('Canceled');if(now()>=payload.expires)throw Error('ExactScanExpired');
 const cursor=result.complete?null:await seal(env,payload);
 const{exact_state,...publicResult}=result;
 return{...publicResult,cursor,steps,step_records:records};
}
