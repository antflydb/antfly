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

import{readCounterToken}from'./independent-counters.mjs';
// Account authorization stays transient and local. No credential file fallback.
export function providerAccountApi(account,{authenticate=readCounterToken,fetch:request=fetch}={}){
 if(!/^[a-f0-9]{32}$/.test(account??''))throw Error('ProviderAccountInvalid');let credential;
 return async(route,method='GET',body)=>{
  if(!/^workers\/observability\/destinations(?:\/[a-zA-Z0-9_-]+)?$/.test(route)||!['GET','POST','DELETE'].includes(method))throw Error('ProviderRouteInvalid');
  if(!credential){try{credential=await authenticate();}catch{throw Error('ProviderCredentialLookupFailed');}if(typeof credential!=='string'||credential.length<20||credential.length>4096||/\s/.test(credential))throw Error('ProviderCredentialInvalid');}
  const response=await request('https://api.cloudflare.com/client/v4/accounts/'+account+'/'+route,{method,headers:{Authorization:'Bearer '+credential,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});let bytes=0;const parts=[];try{for await(const part of response.body){bytes+=part.length;if(bytes>1024*1024)throw Error('ProviderResponseExceeded');parts.push(part);}}catch{throw Error('ProviderResponseInvalid');}let value;try{value=JSON.parse(Buffer.concat(parts));}catch{throw Error('ProviderResponseInvalid');}if(!response.ok||value.success!==true)throw Error('ObserverApiStatus'+response.status);return value.result;
 };
}
