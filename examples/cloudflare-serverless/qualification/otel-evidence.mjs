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
function attrs(values = []) {
 if (!Array.isArray(values) || values.length > 256) reject('TelemetryAttributeBudgetExceeded');
 const out = Object.create(null);
 for (const pair of values) {
  if (typeof pair?.key !== 'string' || Object.hasOwn(out,pair.key)) reject('TelemetryAttributeInvalid');
  const v=pair.value;
  out[pair.key]=typeof v?.stringValue==='string'?v.stringValue:typeof v?.boolValue==='boolean'?v.boolValue:v?.intValue!=null&&/^-?\d+$/.test(String(v.intValue))?Number(v.intValue):typeof v?.doubleValue==='number'?v.doubleValue:null;
 }
 return out;
}
function timestamp(value) {
 if (!/^\d{1,20}$/.test(String(value??''))) return null;
 const n=Number(BigInt(value)/1000000n);return Number.isSafeInteger(n)?n:null;
}
function probes(body) {
 const result=[];
 let value;try { value=JSON.parse(body); } catch { return result; }
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
    const trace=hex(item.traceId,32),span=hex(item.spanId,16),outcome=outcomes.has(a['cloudflare.outcome'])?a['cloudflare.outcome']:null;
    const body=typeof item.body?.stringValue==='string'?item.body.stringValue:'';
    const sequence=a['cloudflare.invocation.sequence.number'];
    const row={signal,script:source,trace_id_sha256:await hash(trace),span_id_sha256:await hash(span),parent_span_id_sha256:await hash(hex(item.parentSpanId,16)),invocation_id_sha256:await hash(a['faas.invocation_id']),version_id:uuid(a['faas.version'])?a['faas.version'].toLowerCase():null,handler:handlers.has(a['cloudflare.handler_type'])?a['cloudflare.handler_type']:'unknown',execution_model:['stateless','stateful','durableObject'].includes(a['cloudflare.execution_model'])?a['cloudflare.execution_model']:null,start:timestamp(item.startTimeUnixNano??item.timeUnixNano),end:timestamp(item.endTimeUnixNano),sequence:Number.isSafeInteger(sequence)&&sequence>=0?sequence:null,terminal:signal==='traces'&&Object.hasOwn(a,'cloudflare.outcome'),outcome,probes:signal==='logs'?probes(body):[],event:sanitizeTailEvent({outcome,cpuTime:a['cloudflare.cpu_time_ms'],wallTime:a['cloudflare.wall_time_ms'],logs:body?[{message:[body]}]:[],exceptions:[]})??{},attribute_keys:Object.keys(a).filter(k=>['faas.version','faas.invocation_id','cloudflare.handler_type','cloudflare.outcome','cloudflare.cpu_time_ms','cloudflare.wall_time_ms','cloudflare.invocation.sequence.number','cloudflare.execution_model'].includes(k)).sort(),unknown_attribute_count:Object.keys(a).filter(k=>!['service.name','faas.name','faas.version','faas.invocation_id','cloudflare.handler_type','cloudflare.outcome','cloudflare.cpu_time_ms','cloudflare.wall_time_ms','cloudflare.invocation.sequence.number','cloudflare.execution_model'].includes(k)).length};
    if(item.droppedAttributesCount||item.droppedEventsCount||item.droppedLinksCount)row.dropped=true;
    if(a['cloudflare.warning.type']!=null)row.warning=true;
    rows.push(row);
   }
  }
 }
 return rows;
}
// This reports readiness only. Gate adoption additionally requires independent
// platform accounting, immutable evidence and a validated settlement contract.
export function reconcileOtel(rows, expected, {version} = {}) {
 const failures=[],terminals=rows.filter(r=>r.terminal),ids=new Map();
 for(const row of terminals){if(!row.invocation_id_sha256||!row.trace_id_sha256||!row.span_id_sha256)failures.push('ProviderIdentityMissing');else {const prior=ids.get(row.invocation_id_sha256);if(prior&&JSON.stringify(prior)!==JSON.stringify(row))failures.push('ProviderIdentityConflict');ids.set(row.invocation_id_sha256,row);}if(!row.version_id)failures.push('ProviderVersionMissing');if(row.handler==='unknown'||!row.outcome||row.start==null||row.end==null||row.end<row.start||row.dropped||row.warning||!Number.isFinite(row.event?.cpuTime)||row.event.cpuTime<0||!Number.isFinite(row.event?.wallTime)||row.event.wallTime<0)failures.push('TerminalMetadataIncomplete');if(row.outcome&&row.outcome!=='ok')failures.push('InvocationFailed');}
 for(const row of rows){if(resourceFailureCode(JSON.stringify(row.event)))failures.push('ResourceFailureObserved');if(row.dropped||row.warning)failures.push('TelemetryDroppedOrWarning');}
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
