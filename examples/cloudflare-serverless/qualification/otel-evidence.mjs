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

// OTLP/JSON boundary. Preserve provider identities, never arbitrary attributes,
// headers, URLs, exception messages, or log bodies. Unknown data cannot qualify.
import{resourceLogFailure}from'./resource-events.mjs';
import{parseDiagnosticJson}from'./diagnostic-memory.mjs';
import{requestAttemptUuid,portableAttemptIdentity}from'./attempt-token.mjs';
import {resourceFailureCode} from '../performance.mjs';
import {sanitizeTailEvent} from './metrics-tail.mjs';
const enc = new TextEncoder();
const reject = code => { throw Error(code); };
const uuid = value => typeof value === 'string' && /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(value);
const hex = (value, length) => typeof value === 'string' && new RegExp('^[a-f0-9]{'+length+'}$','i').test(value) && !/^0+$/.test(value) ? value.toLowerCase() : null;
async function hash(value) {
 if (typeof value !== 'string' || !value.length || value.length > 512) return null;
 return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',enc.encode(value))),b=>b.toString(16).padStart(2,'0')).join('');
}
const attributeDiagnostics=new WeakMap();
function attrs(values = []) {
 if (!Array.isArray(values) || values.length > 256) reject('TelemetryAttributeBudgetExceeded');
 const out = Object.create(null),encoded=new Map();let duplicates=0;const invalid=[];
 for (const pair of values) {
  if(typeof pair?.key!=='string'){invalid.push({kind:pair===null?'null':Array.isArray(pair)?'array':typeof pair,known_keys:pair&&typeof pair==='object'?Object.keys(pair).filter(k=>['key','value','Key','Value'].includes(k)):[],key_type:typeof pair?.key});continue;}
  const serialized=JSON.stringify(pair.value);if(encoded.has(pair.key)){if(encoded.get(pair.key)!==serialized)throw Object.assign(Error('TelemetryAttributeConflict'),{attribute_key:['service.name','faas.name','faas.version','faas.invocation_id','cloudflare.handler_type','cloudflare.outcome','cloudflare.cpu_time_ms','cloudflare.wall_time_ms','cloudflare.invocation.sequence.number','cloudflare.execution_model'].includes(pair.key)?pair.key:'other'});duplicates++;continue;}encoded.set(pair.key,serialized);
  const v=pair.value;
  out[pair.key]=typeof v?.stringValue==='string'?v.stringValue:typeof v?.boolValue==='boolean'?v.boolValue:v?.intValue!=null&&/^-?\d+$/.test(String(v.intValue))?Number(v.intValue):typeof v?.doubleValue==='number'?v.doubleValue:null;
 }
 attributeDiagnostics.set(out,{duplicates,invalid});return out;
}
function timestamp(value) {
 if (!/^\d{1,20}$/.test(String(value??''))) return null;
 const n=Number(BigInt(value)/1000000n);return Number.isSafeInteger(n)?n:null;
}
function probes(body) {
 const result=[];
 let value;try { value=parseDiagnosticJson(body); } catch { return result; }
 if (value?.event==='antfly.qualification_probe'&&uuid(value.id)&&['fetch','rpc'].includes(value.kind)&&['start','late','complete'].includes(value.phase)) result.push({id:value.id,kind:value.kind,phase:value.phase});
 return result;
}
const outcomes=new Set(['ok','exception','canceled','exceededCpu','exceededMemory']);
const handlers=new Set(['fetch','scheduled','alarm','queue','email','rpc','jsrpc','worker_rpc','tail']);
export async function sanitizeOtel(envelope, source, signal) {
 if(!['traces','logs'].includes(signal))reject('TelemetryOtelSignalInvalid');
 const groups=envelope?.[signal==='traces'?'resourceSpans':'resourceLogs'];
 if (!Array.isArray(groups)||groups.length>128) reject('TelemetryOtelEnvelopeInvalid');
 const rows=[];
 for(const group of groups) {
  const resource=attrs(group.resource?.attributes),scopes=group[signal==='traces'?'scopeSpans':'scopeLogs'];
  if(!Array.isArray(scopes)||scopes.length>128) reject('TelemetryOtelScopeInvalid');
  for(const scope of scopes) {
   const items=scope[signal==='traces'?'spans':'logRecords'];
   if(!Array.isArray(items)||items.length>1000||rows.length+items.length>1000) reject('TelemetryRecordBudgetExceeded');
   for(const item of items) {
    const own=attrs(item.attributes),a={...resource,...own};
    for(const key of ['service.name','faas.name','faas.invocation_id'])if(resource[key]!=null&&own[key]!=null&&resource[key]!==own[key])reject('TelemetryAttributeConflict');
    if((a['service.name']??a['faas.name'])!==source || a['faas.name']!=null&&a['faas.name']!==source)reject('TelemetrySourceMismatch');
    for(const [primary,alias]of [['cloudflare.cpu_time_ms','cpu_time_ms'],['cloudflare.wall_time_ms','wall_time_ms']])if(a[primary]!=null&&a[alias]!=null&&a[primary]!==a[alias])reject('TelemetryAttributeConflict');
    const trace=hex(item.traceId,32),span=hex(item.spanId,16),outcome=outcomes.has(a['cloudflare.outcome'])?a['cloudflare.outcome']:null;
    const exceptions=[];if(Array.isArray(item.events)){if(item.events.length>256)reject('TelemetryRecordBudgetExceeded');for(const event of item.events){const ea=attrs(event.attributes);if(typeof ea['exception.message']==='string')exceptions.push({name:'Error',message:ea['exception.message']});}}if(typeof item.status?.message==='string'&&resourceFailureCode(item.status.message))exceptions.push({name:'Error',message:item.status.message});
    const body=typeof item.body?.stringValue==='string'?item.body.stringValue:'';
    const sequence=a['cloudflare.invocation.sequence.number'];
    const row={signal,script:source,attempt_id_sha256:await portableAttemptIdentity(requestAttemptUuid(a['url.full'])),trace_id_sha256:await hash(trace),span_id_sha256:await hash(span),parent_span_id_sha256:await hash(hex(item.parentSpanId,16)),invocation_id_sha256:await hash(a['faas.invocation_id']),version_id:uuid(a['faas.version'])?a['faas.version'].toLowerCase():null,handler:handlers.has(a['cloudflare.handler_type'])?a['cloudflare.handler_type']:'unknown',execution_model:['stateless','stateful','durableObject'].includes(a['cloudflare.execution_model'])?a['cloudflare.execution_model']:null,start:timestamp(item.startTimeUnixNano??item.timeUnixNano),end:timestamp(item.endTimeUnixNano),sequence:Number.isSafeInteger(sequence)&&sequence>=0?sequence:null,terminal:signal==='traces'&&Object.hasOwn(a,'cloudflare.outcome'),outcome,probes:signal==='logs'?probes(body):[],event:sanitizeTailEvent({outcome,cpuTime:a['cloudflare.cpu_time_ms']??a.cpu_time_ms,wallTime:a['cloudflare.wall_time_ms']??a.wall_time_ms,logs:body?[{message:[body]}]:[],exceptions})??{},duplicate_attributes:attributeDiagnostics.get(resource).duplicates+attributeDiagnostics.get(own).duplicates,attribute_keys:Object.keys(a).filter(k=>['faas.version','faas.invocation_id','cloudflare.handler_type','cloudflare.outcome','cloudflare.cpu_time_ms','cloudflare.wall_time_ms','cloudflare.invocation.sequence.number','cloudflare.execution_model'].includes(k)).sort(),attribute_schema:Object.keys(a).filter(k=>/^(cloudflare|faas|service|http|rpc|telemetry)\.[a-z0-9_.]{1,96}$/.test(k)).sort(),body_schema:{kind:Object.keys(item.body??{}).filter(k=>['stringValue','kvlistValue','arrayValue','intValue','doubleValue','boolValue'].includes(k)),known_json_keys:(()=>{try{const parsed=parseDiagnosticJson(body);return parsed&&typeof parsed==='object'?Object.keys(parsed).filter(k=>['outcome','cpuTime','cpuTimeMs','wallTime','wallTimeMs','requestId','request','response','event','exceptions','logs'].includes(k)):[];}catch{return [];}})()},unknown_attribute_count:Object.keys(a).filter(k=>!['service.name','faas.name','faas.version','faas.invocation_id','cloudflare.handler_type','cloudflare.outcome','cloudflare.cpu_time_ms','cloudflare.wall_time_ms','cloudflare.invocation.sequence.number','cloudflare.execution_model'].includes(k)).length};
    const invalid=[...attributeDiagnostics.get(resource).invalid,...attributeDiagnostics.get(own).invalid];if(invalid.length)row.invalid_attributes=invalid;
    if(item.droppedAttributesCount||item.droppedEventsCount||item.droppedLinksCount||invalid.length)row.dropped=true;
    if(a['cloudflare.warning.type']!=null){row.warning=true;row.warning_type=a['cloudflare.warning.type']==='span_not_ended'?'span_not_ended':'unknown';}
    rows.push(row);
   }
  }
 }
 return rows;
}
// A console log may belong to a callee method span nested below the RPC
// invocation span. Resolve only explicit provider parent links, never clocks.
export function attributeOtelInvocations(rows){
 const nodes=new Map(),roots=new Map(),failures=[];
 const key=r=>r.trace_id_sha256&&r.span_id_sha256?r.trace_id_sha256+'/'+r.span_id_sha256:null;
 for(const r of rows)if(r.signal==='traces'&&key(r)){
  const k=key(r),old=nodes.get(k);if(old&&['script','version_id','parent_span_id_sha256','invocation_id_sha256','start','end','handler','terminal','outcome'].some(f=>old[f]!==r[f]))failures.push('ProviderSpanConflict');else nodes.set(k,r);
  if(r.terminal&&r.invocation_id_sha256)roots.set(r.invocation_id_sha256,r);
 }
 if(nodes.size>20000)return{rows:[],failures:['ProviderSpanBudgetExceeded']};
 return{rows:rows.map(r=>{
  if(r.signal!=='logs'||r.invocation_id_sha256)return r;
  let at=key(r),root;const seen=new Set();
  for(let depth=0;at&&depth<64;depth++){
   if(seen.has(at)){failures.push('ProviderSpanCycle');break;}seen.add(at);
   const node=nodes.get(at);if(!node)break;
   if(node.script!==r.script||node.version_id!==r.version_id){failures.push('ProviderSpanScopeMismatch');break;}
   if(node.terminal&&node.invocation_id_sha256){root=node;break;}
   at=node.parent_span_id_sha256?node.trace_id_sha256+'/'+node.parent_span_id_sha256:null;
  }
  if(!root)return r;
  if(root.script!==r.script||root.version_id!==r.version_id){failures.push('ProviderSpanScopeMismatch');return r;}
  return{...r,invocation_id_sha256:root.invocation_id_sha256,identity_source:'provider-span-parentage'};
 }),failures:[...new Set(failures)]};
}

// This reports readiness only. Gate adoption additionally requires independent
// platform accounting, immutable evidence and a validated settlement contract.
export function reconcileOtel(rows, expected, {version} = {}) {
 const attributed=attributeOtelInvocations(rows);rows=attributed.rows;const failures=[...attributed.failures],terminals=rows.filter(r=>r.terminal),ids=new Map();
 for(const row of terminals){if(!row.invocation_id_sha256||!row.trace_id_sha256||!row.span_id_sha256)failures.push('ProviderIdentityMissing');else {const prior=ids.get(row.invocation_id_sha256);if(prior&&JSON.stringify(prior)!==JSON.stringify(row))failures.push('ProviderIdentityConflict');ids.set(row.invocation_id_sha256,row);}if(!row.version_id)failures.push('ProviderVersionMissing');if(row.handler==='unknown'||!row.outcome||row.start==null||row.end==null||row.end<row.start||row.dropped||row.warning||!Number.isFinite(row.event?.cpuTime)||row.event.cpuTime<0||!Number.isFinite(row.event?.wallTime)||row.event.wallTime<0)failures.push('TerminalMetadataIncomplete');if(row.outcome&&row.outcome!=='ok')failures.push('InvocationFailed');}
 for(const row of rows){for(const log of row.event?.logs??[])for(const message of log.message??[]){let value;try{value=JSON.parse(message);}catch{value=null;}if(resourceLogFailure(value))failures.push('ResourceFailureObserved');}for(const exception of row.event?.exceptions??[])if(resourceFailureCode(exception.message))failures.push('ResourceFailureObserved');if(row.dropped||row.warning&&(row.terminal||row.warning_type!=='span_not_ended'))failures.push('TelemetryDroppedOrWarning');}
 const unique=[...ids.values()];
 if(version&&unique.some(row=>row.version_id!==version))failures.push('ProviderVersionMismatch');
 for(const pair of expected)for(const[kind,id,phase]of[['fetch',pair.id,'late'],['rpc',pair.rpc,'complete']]){
  const logs=rows.filter(r=>r.signal==='logs'&&r.probes.some(p=>p.id===id&&p.kind===kind&&p.phase===phase));
  if(!logs.length)failures.push('FixtureLogMissing');
  const matches=unique.filter(t=>logs.some(l=>l.trace_id_sha256===t.trace_id_sha256&&(l.invocation_id_sha256!=null&&l.invocation_id_sha256===t.invocation_id_sha256||l.span_id_sha256!=null&&l.span_id_sha256===t.span_id_sha256)));if(matches.length!==1)failures.push('FixtureTerminalMismatch');else {if(kind==='fetch'?matches[0].handler!=='fetch':!['rpc','jsrpc','worker_rpc'].includes(matches[0].handler))failures.push('FixtureHandlerMismatch');if(logs.some(l=>l.start==null||l.start<matches[0].start||l.start>matches[0].end))failures.push('LateLogOutsideTerminalLifetime');}
 }
 if(unique.length!==expected.length*2)failures.push('TerminalCountMismatch');
 return{qualification:false,ready:failures.length===0,failures:[...new Set(failures)],terminals:unique.length,expected:expected.length*2};
}

// CPU/wall attributes are absent from the current live OTLP spans. Join the
// exact independently stored terminal record by provider invocation ID; never
// infer identity from clocks, payload fingerprints, or fixture UUIDs.
export async function reconcileCompositeOtel(rows, expected, terminalWindow, {version} = {}) {
 const failures=[],w=terminalWindow;
 if(w?.schema!==3||w.qualification!==false||w.reconciled!==true||!Array.isArray(w.rows)||w.rows.length>20000||w.events!==w.rows.length||!Number.isSafeInteger(w.timeframe?.from)||!Number.isSafeInteger(w.timeframe?.to)||w.timeframe.from>=w.timeframe.to||w.timeframe.to-w.timeframe.from>60000||!Number.isSafeInteger(w.collected_started_at)||!Number.isSafeInteger(w.collected_finished_at)||w.collected_started_at<w.timeframe.to+60000||w.collected_finished_at<w.collected_started_at)return{qualification:false,ready:false,failures:['TerminalWindowInvalid']};
 const encoded=enc.encode(JSON.stringify(w.rows));if(encoded.length>64*1024*1024)return{qualification:false,ready:false,failures:['TerminalWindowBudgetExceeded']};
 const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',encoded)),b=>b.toString(16).padStart(2,'0')).join('');if(digest!==w.rows_sha256)failures.push('TerminalWindowHashMismatch');
 if(w.gap||w.resource_failures!==0||w.invocation_errors!==0)failures.push('TerminalWindowFailure');
 const stored=w.rows.filter(r=>r.invocation),byId=new Map();for(const r of stored){if(!/^[a-f0-9]{64}$/.test(r.invocation_id_sha256??'')||byId.has(r.invocation_id_sha256)||!Number.isSafeInteger(r.timestamp)||r.timestamp<w.timeframe.from||r.timestamp>w.timeframe.to||JSON.stringify(sanitizeTailEvent(r.event)??{})!==JSON.stringify(r.event))failures.push('StoredTerminalInvalid');byId.set(r.invocation_id_sha256,r);}
 if(stored.length!==w.invocations)failures.push('StoredTerminalCountMismatch');
 const selected=rows.filter(r=>r.start>=w.timeframe.from&&r.start<=w.timeframe.to),matched=new Set();
 const enriched=selected.map(r=>{
  if(!r.terminal)return r;
  const terminal=byId.get(r.invocation_id_sha256);if(!terminal){failures.push('StoredTerminalMissing');return r;}matched.add(r.invocation_id_sha256);
  if(r.script!==w.worker||terminal.version_id!==r.version_id||terminal.event_type!==r.handler||terminal.event.outcome!==r.outcome||terminal.timestamp<r.start||terminal.timestamp>r.end)failures.push('StoredTerminalMetadataMismatch');
  for(const key of ['cpuTime','wallTime'])if(r.event[key]!=null&&r.event[key]!==terminal.event[key])failures.push('TerminalMetricConflict');
  return{...r,event:{...r.event,cpuTime:terminal.event.cpuTime,wallTime:terminal.event.wallTime}};
 });
 if(matched.size!==stored.length)failures.push('StoredTerminalUnmatched');
 const result=reconcileOtel([...enriched,...w.rows.map(r=>({signal:'stored',event:r.event}))],expected,{version});
 return{...result,ready:result.ready&&failures.length===0,failures:[...new Set([...failures,...result.failures])],matched_provider_terminals:matched.size,stored_rows_sha256:digest};
}
