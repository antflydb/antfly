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

import{readFile,writeFile}from'node:fs/promises';
import{homedir}from'node:os';
import{join}from'node:path';
import{assertGateRun}from'./gate-scope.mjs';
import{providerEmbeddings}from'./provider-embeddings.mjs';
import{GateClient}from'./gate-client.mjs';
const accountApi=process.argv.includes('--account-api'),output=process.argv[3];if(!output)throw Error('usage: model-fixture RUN_JSON OUTPUT [--account-api]');
let client,run,token;
if(accountApi){
 run=JSON.parse(await readFile(process.argv[2]));assertGateRun(run);
 token=process.env.CLOUDFLARE_API_TOKEN;
 if(!token){const auth=await readFile(process.env.WRANGLER_AUTH_FILE??join(homedir(),'Library/Preferences/.wrangler/config/default.toml'),'utf8');token=/^oauth_token\s*=\s*"([^"]+)"/m.exec(auth)?.[1];}
 if(!token)throw Error('existing Cloudflare account authentication unavailable');
}else{client=await GateClient.open(process.argv[2]);run=client.run;}

const topics=['database transactions and recovery','ocean currents and coral reefs','medieval architecture and stone bridges','space telescopes and distant galaxies','vegetable gardens and soil nutrients','railway timetables and freight terminals','symphony orchestras and woodwind instruments','computer networks and TLS certificates','bread fermentation and sourdough baking','renewable electricity and battery storage','forest ecosystems and bird migration','robot navigation and visual sensors','public libraries and book preservation','photography and lens exposure','marathon training and endurance recovery','volcano geology and seismic measurements','language translation and linguistic history','manufacturing quality and supply chains','urban cycling and road planning','weather forecasting and cloud formation'];
const documents=Array.from({length:1000},(_,i)=>'Document '+i+' examines '+topics[i%topics.length]+'. The '+['annual','regional','comparative','experimental','historical','practical','statistical'][Math.floor(i/topics.length)%7]+' report discusses '+['measurement accuracy','long-term changes','equipment maintenance','unexpected failures','seasonal differences','planning decisions','resource requirements','field observations','technical constraints','community participation'][Math.floor(i/7)%10]+', with evidence from study '+(i*7919%10007)+'.');
const queries=topics.slice(0,10).map(topic=>'Find evidence and practical explanations about '+topic+' and the causes of changes over time.');
const vectors=[];for(let start=0;start<documents.length+queries.length;start+=64){const all=[...documents,...queries],batch=all.slice(start,start+64),value=accountApi?await providerEmbeddings(run.account,batch,token):(await client.call('a',{text:batch},'embedding',{path:'/ai'})).value;if(!Array.isArray(value.data)||value.data.length!==batch.length||value.data.some(v=>!Array.isArray(v)||v.length!==384||v.some(n=>!Number.isFinite(n))))throw Error('invalid provider vectors');vectors.push(...value.data);console.log('embedded',vectors.length);}
await writeFile(output,JSON.stringify({model:'@cf/baai/bge-small-en-v1.5',gateway:'default',source:'generated topic documents; not a production corpus',transport:accountApi?'cloudflare-account-rest':'test-worker-binding',served_version:accountApi?null:run.version,document_texts:documents,query_texts:queries,documents:vectors.slice(0,documents.length),queries:vectors.slice(documents.length)}));console.log('saved',output);
