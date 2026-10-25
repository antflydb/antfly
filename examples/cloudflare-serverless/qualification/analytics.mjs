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

import {readFile,writeFile} from "node:fs/promises";
import {homedir} from "node:os";
import {join} from "node:path";
const run=JSON.parse(await readFile(process.argv[2],"utf8"));if(!run.id.startsWith("antfly-1278-")||run.bucket!==run.id)throw new Error("qualification identity mismatch");
let token=process.env.CLOUDFLARE_API_TOKEN;
if(!token){const config=await readFile(process.env.WRANGLER_AUTH_FILE??join(homedir(),"Library/Preferences/.wrangler/config/default.toml"),"utf8");token=/^oauth_token\s*=\s*"([^"]+)"/m.exec(config)?.[1];}
if(!token)throw new Error("Cloudflare analytics authentication unavailable");
const headers={Authorization:`Bearer ${token}`,"Content-Type":"application/json"};
async function api(path){const response=await fetch("https://api.cloudflare.com/client/v4"+path,{headers});const result=await response.json();if(!response.ok||!result.success)throw new Error("Cloudflare analytics lookup failed: "+response.status);return result.result;}
const namespaces=(await api(`/accounts/${run.account}/workers/durable_objects/namespaces`)).filter(n=>n.script===run.id),bucket=await api(`/accounts/${run.account}/r2/buckets/${run.bucket}`);
const since=process.argv[3]??bucket.creation_date,until=new Date().toISOString();if(!since||!namespaces.length)throw new Error("qualification analytics scope unavailable");
const query=`{viewer{accounts(filter:{accountTag:${JSON.stringify(run.account)}}){memory:durableObjectsPeriodicGroups(limit:100,filter:{datetime_geq:${JSON.stringify(since)},datetime_leq:${JSON.stringify(until)},namespaceId_in:${JSON.stringify(namespaces.flatMap(n=>[n.id,n.id.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/,"$1-$2-$3-$4-$5")]))}}){dimensions{namespaceId} count quantiles{memoryUsageBytesP50 memoryUsageBytesP95 memoryUsageBytesP99 memoryUsageBytesP999}} r2:r2OperationsAdaptiveGroups(limit:100,filter:{datetime_geq:${JSON.stringify(since)},datetime_leq:${JSON.stringify(until)},bucketName:${JSON.stringify(run.bucket)}}){dimensions{actionType actionStatus} sum{requests responseBytes responseObjectSize}}}}}`;
const response=await fetch("https://api.cloudflare.com/client/v4/graphql",{method:"POST",headers,body:JSON.stringify({query})}),result=await response.json();
const report={since,until,namespaces:namespaces.map(n=>({id:n.id,class:n.class})),data:result.data?.viewer?.accounts?.[0]??null,errors:result.errors??null,billed_cost:null,limitations:["Memory percentiles describe whole hosting isolates, potentially including colocated objects; periodic samples are not an absolute peak.","R2 analytics are adaptive samples and usage attribution, not a finalized bill; recent data can lag."]};
await writeFile(run.dir+"/analytics-report.json",JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));if(result.errors)process.exitCode=1;
