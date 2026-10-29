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
import {sanitizeOtel,reconcileOtel} from '../qualification/otel-evidence.mjs';
const source='antfly-1278-telemetry-fixture-source',version=crypto.randomUUID(),pair={id:crypto.randomUUID(),rpc:crypto.randomUUID()};
const attr=(key,value)=>({key,value:typeof value==='number'?{doubleValue:value}:{stringValue:value}});
const attributes=(kind,id)=>[attr('faas.invocation_id',id),attr('faas.version',version),attr('cloudflare.handler_type',kind),attr('cloudflare.outcome','ok'),attr('cloudflare.cpu_time_ms',1),attr('cloudflare.wall_time_ms',20)];
const span=(kind,id,n)=>({traceId:'a'.repeat(32),spanId:String(n).repeat(16),startTimeUnixNano:'1000000000',endTimeUnixNano:'1020000000',attributes:attributes(kind,id)});
const envelope=(signal,items)=>({[signal==='traces'?'resourceSpans':'resourceLogs']:[{resource:{attributes:[attr('service.name',source)]},[signal==='traces'?'scopeSpans':'scopeLogs']:[{[signal==='traces'?'spans':'logRecords']:items}]}]});
async function fixture(){const spans=[span('fetch','provider-fetch',1),span('rpc','provider-rpc',2)];const logs=spans.map((s,i)=>({...s,timeUnixNano:s.endTimeUnixNano,body:{stringValue:JSON.stringify({event:'antfly.qualification_probe',id:i?pair.rpc:pair.id,kind:i?'rpc':'fetch',phase:i?'complete':'late'})}}));return [...await sanitizeOtel(envelope('traces',spans),source,'traces'),...await sanitizeOtel(envelope('logs',logs),source,'logs')];}
test('OTLP readiness joins distinct provider invocation IDs and late logs to terminal metadata',async()=>{const rows=await fixture();assert.deepEqual(reconcileOtel(rows,[pair]),{qualification:false,ready:true,failures:[],terminals:2,expected:2});assert.equal(rows[0].version_id,version);assert.equal(rows[0].end,1020);assert.ok(!JSON.stringify(rows).includes('provider-fetch'));});
test('OTLP fixture UUIDs cannot replace provider IDs, versions, or complete terminal metadata',async()=>{for(const field of ['invocation_id_sha256','span_id_sha256','version_id','end']){const rows=await fixture();rows[0][field]=null;assert.equal(reconcileOtel(rows,[pair]).ready,false,field);}const rows=await fixture();rows[0].dropped=true;assert.equal(reconcileOtel(rows,[pair]).ready,false);rows[0].warning=true;assert.equal(reconcileOtel(rows,[pair]).ready,false);});
test('OTLP duplicates require identical provider identity content and exact population counts',async()=>{const rows=await fixture();assert.equal(reconcileOtel([...rows,structuredClone(rows[0])],[pair]).ready,true);assert.ok(reconcileOtel([...rows,{...rows[0],end:9999}],[pair]).failures.includes('ProviderIdentityConflict'));assert.equal(reconcileOtel(rows,[]).ready,false);});
test('OTLP redacts arbitrary attributes, URLs, error text and bodies while retaining resource failures',async()=>{const s=span('fetch','secret-provider-id',1);s.attributes.push(attr('url.full','https://private.invalid/secret'),attr('private-secret','private-value'));s.body={stringValue:'SQLITE_NOMEM private-secret'};const [row]=await sanitizeOtel(envelope('logs',[s]),source,'logs');const text=JSON.stringify(row);assert.ok(text.includes('SQLITE_NOMEM'));for(const secret of ['private.invalid','private-secret','private-value','secret-provider-id'])assert.ok(!text.includes(secret));});
test('OTLP rejects foreign sources, conflicting resource identity, duplicate attributes and oversized envelopes',async()=>{const s=span('rpc','provider',1),e=envelope('traces',[s]);await assert.rejects(sanitizeOtel(e,'other','traces'),/TelemetrySourceMismatch/);s.attributes.push(attr('service.name','other'));await assert.rejects(sanitizeOtel(e,source,'traces'),/TelemetryAttributeConflict/);s.attributes.pop();s.attributes.push(s.attributes[0]);await assert.rejects(sanitizeOtel(e,source,'traces'),/TelemetryAttributeInvalid/);await assert.rejects(sanitizeOtel({resourceSpans:Array(129).fill({})},source,'traces'),/TelemetryOtelEnvelopeInvalid/);});
test('OTLP recovered resource outcomes remain failures and child spans never inflate terminal totals',async()=>{const rows=await fixture();rows[0].outcome='exceededMemory';assert.ok(reconcileOtel(rows,[pair]).failures.includes('InvocationFailed'));rows[0].outcome='ok';assert.equal(reconcileOtel([...rows,{...rows[0],terminal:false}],[pair]).ready,true);});

test('OTLP readiness rejects recovered resource logs, swapped handlers and unexpected source versions',async()=>{const rows=await fixture();assert.equal(reconcileOtel(rows,[pair],{version:crypto.randomUUID()}).ready,false);rows[0].handler='rpc';assert.ok(reconcileOtel(rows,[pair]).failures.includes('FixtureHandlerMismatch'));rows[0].handler='fetch';rows[2].event={logs:[{message:['SQLITE_NOMEM']} ]};assert.ok(reconcileOtel(rows,[pair]).failures.includes('ResourceFailureObserved'));});

test('OTLP logs without invocation attributes require the exact provider trace and root span pair',async()=>{const rows=await fixture();for(const r of rows.filter(r=>r.signal==='logs'))r.invocation_id_sha256=null;assert.equal(reconcileOtel(rows,[pair]).ready,true);rows[2].span_id_sha256=rows[1].span_id_sha256;assert.equal(reconcileOtel(rows,[pair]).ready,false);});

test('OTLP readiness requires root CPU/wall metrics and late events inside the terminal lifetime',async()=>{const rows=await fixture();rows[2].start=rows[0].end+1;assert.ok(reconcileOtel(rows,[pair]).failures.includes('LateLogOutsideTerminalLifetime'));rows[2].start=rows[0].end;delete rows[0].event.cpuTime;assert.ok(reconcileOtel(rows,[pair]).failures.includes('TerminalMetadataIncomplete'));});
