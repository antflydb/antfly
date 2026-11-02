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

import {test} from 'node:test';import assert from 'node:assert/strict';
import {walkOtlp} from '../qualification/otlp-chunks.mjs';
import {checkDiagnosticJson} from '../qualification/diagnostic-memory.mjs';
const attr=(key,value)=>({key,value:{stringValue:value}}),span=i=>({traceId:'a'.repeat(32),spanId:'b'.repeat(16),attributes:Array.from({length:16},(_,j)=>attr('key'+j,'value'+i)),startTimeUnixNano:'1000000000',endTimeUnixNano:'1010000000'}),group=spans=>({resource:{attributes:[attr('service.name','source')]},scopeSpans:[{spans}]}),rows=chunk=>chunk.resourceSpans.flatMap(g=>g.scopeSpans.flatMap(s=>s.spans));

test('dense valid exports bypass whole-tree materialization while retaining the original record and chunk bounds',()=>{
 const input=JSON.stringify({resourceSpans:[group(Array.from({length:6000},(_,i)=>span(i)))]});assert.ok(Buffer.byteLength(input)<6*1024*1024);assert.throws(()=>checkDiagnosticJson(input),/ComplexityExceeded/);
 const parsed=walkOtlp(input,'traces'),chunks=[...parsed.chunks];assert.equal(parsed.records,6000);assert.ok(chunks.length>1&&chunks.length<=128);assert.equal(chunks.flatMap(rows).length,6000);for(const chunk of chunks){assert.ok(rows(chunk).length<=1000);checkDiagnosticJson(JSON.stringify(chunk));}
});
test('many resource groups, multiple scopes, bare groups, arrays and NDJSON preserve every record',()=>{
 const inputs=[{resourceSpans:Array.from({length:256},()=>group([span(1)]))},[ {resourceSpans:[group([span(2)])]}, {resourceSpans:[group([span(3)])]} ],group([span(4)])];
 for(const value of inputs){const parsed=walkOtlp(JSON.stringify(value),'traces'),chunks=[...parsed.chunks];assert.equal(chunks.flatMap(rows).length,parsed.records);assert.ok(chunks.every(c=>c.resourceSpans.length<=128));}
 const a=JSON.stringify({resourceSpans:[group([span(1)])]}),b=JSON.stringify({resourceSpans:[group([span(2)])]});assert.equal(walkOtlp(a+'\n'+b,'traces').records,2);assert.throws(()=>walkOtlp(a+b,'traces'),/EnvelopeInvalid/);
 const g=group([span(1)]);g.scopeSpans.push({spans:[span(2),span(3)]});assert.equal([...walkOtlp(JSON.stringify({resourceSpans:[g]}),'traces').chunks].flatMap(rows).length,3);
});
test('complete syntax validation precedes publishing, including unknown fields and escaped property names',()=>{
 for(const bad of ['{"resourceSpans":[],"extra":[1,]}','{"resourceSpans":[],"extra":"bad\\x"}','{"resourceSpans":[],"extra":01}','{"resourceSpans":[],"extra":truex}','{"resourceSpans":[],"extra":1e}','{"resourceSpans":[],"extra":{"x":1,}}','{"resourceSpans":[],"resourceSpans":[]}'])assert.throws(()=>walkOtlp(bad,'traces'));
 assert.equal(walkOtlp('{"\\u0072esourceSpans":[]}','traces').records,0);
 assert.equal(walkOtlp('{"resourceSpans":[],"extra":[-1.2e+3,true,false,null,"\\u0022"]}','traces').records,0);
});
test('huge top-level/resource arrays, deep metadata and oversized populations reject before materialization',()=>{
 assert.throws(()=>walkOtlp('['+Array(257).fill('{"resourceSpans":[]}').join(',')+']','traces'),/BudgetExceeded/);
 assert.throws(()=>walkOtlp('{"resourceSpans":['+Array(4097).fill('{"scopeSpans":[]}').join(',')+']}','traces'),/BudgetExceeded/);
 assert.throws(()=>walkOtlp('{"resourceSpans":[],"extra":'+'['.repeat(33)+'0'+']'.repeat(33)+'}','traces'),/ComplexityExceeded/);
 const input=JSON.stringify({resourceSpans:[group(Array.from({length:10001},()=>({}))) ]});assert.throws(()=>walkOtlp(input,'traces'),/RecordBudgetExceeded/);
});
test('log records stream through the same bounded resource/scoping shape',()=>{
 const value={resourceLogs:[{resource:{attributes:[attr('service.name','source')]},scopeLogs:[{logRecords:[{timeUnixNano:'1000000000',body:{stringValue:'message'}}]}]}]},parsed=walkOtlp(JSON.stringify(value),'logs'),chunks=[...parsed.chunks];assert.equal(parsed.records,1);assert.equal(chunks[0].resourceLogs[0].scopeLogs[0].logRecords[0].body.stringValue,'message');
});
