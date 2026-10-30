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

import {resourceFailureCode} from '../performance.mjs';
const identities=new WeakMap();
export const diagnosticBucketIdentity=bucket=>identities.get(bucket)??bucket;
const components=new Set(['export-receiver','tail-observer','evidence-reader']);
export function diagnosticStorage(env,component,{emit=value=>console.error(JSON.stringify(value))}={}){
 if(!components.has(component))throw Error('TelemetryDiagnosticComponentInvalid');
 const wrapped={...env},aliases=new Map();
 for(const [binding,storage]of[['LOGS','proof'],['ARCHIVE','archive']]){
  const bucket=env[binding];if(!bucket)continue;
  if(aliases.has(bucket)){wrapped[binding]=aliases.get(bucket);continue;}
  wrapped[binding]=Object.fromEntries(['get','put','head','list','delete'].map(op=>[op,async(...args)=>{try{return await bucket[op](...args);}catch(error){const failure_code=resourceFailureCode(error);if(failure_code)try{emit({event:'antfly.resource_failure',failure_code,boundary:'r2-binding',component,storage,op});}catch{}throw error;}}]));aliases.set(bucket,wrapped[binding]);identities.set(wrapped[binding],diagnosticBucketIdentity(bucket));
 }
 return wrapped;
}
export function diagnosticFailureResponse(error,fallback){
 const failure_code=resourceFailureCode(error),code=/^(?:Observer|Telemetry)[A-Za-z]+$/.test(error?.message??'')?error.message:fallback;
 return Response.json({error:code,...(failure_code?{failure_code}:{})},{status:503,...(failure_code?{headers:{'X-Antfly-Resource-Failure':failure_code}}:{})});
}
