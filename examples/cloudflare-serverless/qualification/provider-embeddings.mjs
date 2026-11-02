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

// Official account API authentication stays local; no Worker secret is installed.
// https://developers.cloudflare.com/ai-gateway/usage/rest-api/#call-a-workers-ai-model
export async function providerEmbeddings(account,text,token,{fetcher=fetch}={}){
 if(!/^[a-f0-9]{32}$/.test(account)||!token||!Array.isArray(text)||text.length<1||text.length>64||text.some(value=>typeof value!=='string'||value.length>2048))throw Error('invalid bounded embedding request');
 const response=await fetcher('https://api.cloudflare.com/client/v4/accounts/'+account+'/ai/run/@cf/baai/bge-small-en-v1.5',{method:'POST',redirect:'error',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json','cf-aig-gateway-id':'default'},body:JSON.stringify({text}),signal:AbortSignal.timeout(60000)});
 if(!response.ok)throw Error('Cloudflare model API failed: HTTP '+response.status);
 const envelope=await response.json();if(envelope.success===false)throw Error('Cloudflare model API returned a failed inference');return envelope.result;
}
