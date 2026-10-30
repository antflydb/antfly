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

import{spawn}from'node:child_process';
import{resolve}from'node:path';
export function evaluateDestinationHealth(status,{id,captureId,from,to}={}){
 const failures=[],valid=Number.isSafeInteger(from)&&Number.isSafeInteger(to)&&from<=to&&Number.isSafeInteger(status?.at)&&status.at>=to&&status.id===id&&(status.observation_capture_id??null)===(captureId??null)&&status.destinations?.length===2;
 if(!valid)return {qualification:false,complete:false,failures:['ExportDeliveryScopeInvalid']};
 for(const signal of['logs','traces']){const rows=status.destinations.filter(d=>d.signal===signal);if(rows.length!==1){failures.push('ExportDeliveryScopeInvalid');continue;}const d=rows[0],complete=Date.parse(d.last_complete??''),error=d.last_error==null||d.last_error===''?null:Date.parse(d.last_error);if(d.enabled!==true||d.dataset!=='opentelemetry-'+signal)failures.push('ExportDestinationDisabledOrChanged');if(!Number.isFinite(complete)||complete<to||complete>status.at+1000)failures.push('ExportDeliveryNotSettled');if(error!==null&&(!Number.isFinite(error)||error>=from))failures.push('ExportDeliveryFailureObserved');}
 return {schema:1,qualification:false,complete:failures.length===0,failures:[...new Set(failures)],from,to,observed_at:status.at,status};
}
export async function readDestinationStatus(path){
 const lifecycle=resolve(import.meta.dirname,'paired-observer-lifecycle.mjs'),status=await new Promise((ok,bad)=>{const child=spawn(process.execPath,[lifecycle,'destination-status',path],{stdio:['ignore','pipe','pipe']}),parts=[];let size=0;const timer=setTimeout(()=>child.kill(),60000);child.stdout.on('data',p=>{size+=p.length;if(size>65536)child.kill();else parts.push(p);});child.stderr.resume();child.on('error',()=>{clearTimeout(timer);bad(Error('ExportDeliveryStatusUnavailable'));});child.on('exit',code=>{clearTimeout(timer);if(code||size>65536)return bad(Error('ExportDeliveryStatusUnavailable'));try{ok(JSON.parse(Buffer.concat(parts)));}catch{bad(Error('ExportDeliveryStatusInvalid'));}});});
 return status;
}

export async function destinationHealth(path,observer,timeframe){return evaluateDestinationHealth(await readDestinationStatus(path),{id:observer.id,captureId:observer.observation_capture_id,...timeframe});}
