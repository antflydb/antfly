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

import {parseDiagnosticJson} from './diagnostic-memory.mjs';
const invalid=()=>{throw Error('ReceiverEnvelopeInvalid');};
const white=c=>c===' '||c==='\n'||c==='\r'||c==='\t';
// A non-materializing JSON syntax walk. Only the depth-bounded call stack is
// retained; individual object/array nodes never become a JavaScript tree.
function syntax(text){
 let nodes=0;
 const ws=i=>{while(i<text.length&&white(text[i]))i++;return i;};
 function string(i){for(i++;i<text.length;i++){const c=text[i];if(c==='"')return i+1;if(text.charCodeAt(i)<32)invalid();if(c==='\\'){const escape=text[++i];if(escape==='u'){for(let j=0;j<4;j++)if(!/[a-f0-9]/i.test(text[++i]??''))invalid();}else if(! '"\\/bfnrt'.includes(escape??'!'))invalid();}}invalid();}
 function value(i,depth=0){
  if(depth>32)throw Error('TelemetryJsonComplexityExceeded');i=ws(i);nodes++;const c=text[i];
  if(c==='"')return string(i);
  if(c==='{'||c==='['){const end=c==='{'?'}':']';i=ws(i+1);if(text[i]===end)return i+1;
   for(;;){if(c==='{'){if(text[i]!=='"')invalid();nodes++;i=ws(string(i));if(text[i]!==':')invalid();i=ws(i+1);}i=ws(value(i,depth+1));if(text[i]===end)return i+1;if(text[i]!==',')invalid();i=ws(i+1);}
  }
  if(text.startsWith('true',i))return i+4;if(text.startsWith('false',i))return i+5;if(text.startsWith('null',i))return i+4;
  const start=i;if(text[i]==='-')i++;if(text[i]==='0')i++;else{if(!/[1-9]/.test(text[i]??''))invalid();while(/[0-9]/.test(text[i]??''))i++;}
  if(text[i]==='.'){i++;if(!/[0-9]/.test(text[i]??''))invalid();while(/[0-9]/.test(text[i]??''))i++;}
  if(text[i]==='e'||text[i]==='E'){i++;if(text[i]==='+'||text[i]==='-')i++;if(!/[0-9]/.test(text[i]??''))invalid();while(/[0-9]/.test(text[i]??''))i++;}
  if(i===start)invalid();return i;
 }
 function measured(i,depth=0){const before=nodes,end=value(i,depth);return {start:ws(i),end,nodes:nodes-before};}
 function* array(range,depth=0){let i=ws(range.start);if(text[i]!=='[')invalid();i=ws(i+1);if(text[i]===']')return;for(;;){const item=measured(i,depth+1);yield item;i=ws(item.end);if(text[i]===']')return;if(text[i]!==',')invalid();i=ws(i+1);}}
 function members(range,depth=0){let i=ws(range.start);if(text[i]!=='{')invalid();i=ws(i+1);const out=new Map();if(text[i]==='}')return out;for(;;){if(text[i]!=='"')invalid();const end=string(i);if(end-i>1024)throw Error('ReceiverEnvelopeBudgetExceeded');const key=JSON.parse(text.slice(i,end));i=ws(end);if(text[i]!==':')invalid();const item=measured(i+1,depth+1);if(out.has(key))invalid();if(out.size>=256)throw Error('ReceiverEnvelopeBudgetExceeded');out.set(key,item);i=ws(item.end);if(text[i]==='}')return out;if(text[i]!==',')invalid();i=ws(i+1);if(out.size>256)throw Error('ReceiverEnvelopeBudgetExceeded');}}
 return {ws,measured,array,members};
}

// Materialize only bounded record slices plus their small resource metadata.
// Validation scans the entire payload before any chunk can be published.
export function walkOtlp(text,signal){
 if(typeof text!=='string'||text.length>6*1024*1024)throw Error('TelemetryBodyExceeded');
 if(!['traces','logs'].includes(signal))throw Error('ReceiverEnvelopeInvalid');
 const s=syntax(text),documents=[];let at=s.ws(0);
 while(at<text.length){const range=s.measured(at);documents.push(range);if(documents.length>256)throw Error('ReceiverEnvelopeBudgetExceeded');const next=s.ws(range.end);if(next<text.length&&!text.slice(range.end,next).includes('\n'))invalid();at=next;}
 if(!documents.length)invalid();const topArray=text[documents[0].start]==='[';
 const bounded=(range,limit)=>{const items=[];for(const item of s.array(range)){if(items.length>=limit)throw Error('ReceiverEnvelopeBudgetExceeded');items.push(item);}return items;};
 const envelopes=topArray?(()=>{if(documents.length!==1)invalid();return bounded(documents[0],256);})():documents;
 if(envelopes.length>256)throw Error('ReceiverEnvelopeBudgetExceeded');
 const groupKey=signal==='traces'?'resourceSpans':'resourceLogs',scopeKey=signal==='traces'?'scopeSpans':'scopeLogs',itemKey=signal==='traces'?'spans':'logRecords';
 const groups=[],resource_counts=[],known_top_keys=[];let records=0;
 for(const envelope of envelopes){const m=s.members(envelope);if(known_top_keys.length<8)known_top_keys.push(['resourceSpans','resourceLogs','resource','scopeSpans','scopeLogs','resource_spans','resource_logs'].filter(k=>m.has(k)));const resourceGroups=m.has(groupKey)?bounded(m.get(groupKey),4096-groups.length):m.has('resource')&&m.has(scopeKey)?[envelope]:null;if(!resourceGroups)invalid();resource_counts.push(m.has(groupKey)?resourceGroups.length:null);if(groups.length+resourceGroups.length>4096)throw Error('ReceiverEnvelopeBudgetExceeded');
  for(const range of resourceGroups){const group=s.members(range),resource=group.get('resource'),scopes=group.get(scopeKey);if(!scopes)throw Error('ReceiverScopeInvalid');const parts=[];let count=0;for(const scope of s.array(scopes)){if(++count>128)throw Error('ReceiverScopeInvalid');const items=s.members(scope).get(itemKey);if(!items)throw Error('ReceiverScopeInvalid');const rows=[];for(const item of s.array(items)){if(++records>10000)throw Error('ReceiverRecordBudgetExceeded');if(item.nodes>240000)throw Error('TelemetryJsonComplexityExceeded');rows.push(item);}parts.push(rows);}groups.push({resource,parts});}
 }
 function* chunks(){
  let selected=[],selectedRecords=0,selectedNodes=0;
  const emit=()=>({[groupKey]:selected.map(part=>({...(part.resource?{resource:part.resource}:{}),[scopeKey]:[{[itemKey]:part.rows.map(r=>parseDiagnosticJson(text.slice(r.start,r.end)))}]}))});
  for(const group of groups){if(group.resource?.nodes>16000)throw Error('TelemetryJsonComplexityExceeded');const resource=group.resource?parseDiagnosticJson(text.slice(group.resource.start,group.resource.end)):undefined;
   for(const rows of group.parts){let pending=[],nodes=group.resource?.nodes??0;
    for(let i=0;i<rows.length;){const row=rows[i];if(pending.length&&(pending.length===1000||nodes+row.nodes>250000)){const part={resource,rows:pending,nodes};if(selected.length&&(selected.length===128||selectedRecords+pending.length>1000||selectedNodes+nodes>250000)){yield emit();selected=[];selectedRecords=0;selectedNodes=0;}selected.push(part);selectedRecords+=pending.length;selectedNodes+=nodes;pending=[];nodes=group.resource?.nodes??0;continue;}pending.push(row);nodes+=row.nodes;i++;}
    if(pending.length){if(selected.length&&(selected.length===128||selectedRecords+pending.length>1000||selectedNodes+nodes>250000)){yield emit();selected=[];selectedRecords=0;selectedNodes=0;}selected.push({resource,rows:pending,nodes});selectedRecords+=pending.length;selectedNodes+=nodes;}
   }
  }
  if(selected.length)yield emit();
 }
 return {chunks:chunks(),resources:groups.length,records,envelopes:envelopes.length,top_kind:topArray?'array':'object',known_top_keys,resource_counts};
}
