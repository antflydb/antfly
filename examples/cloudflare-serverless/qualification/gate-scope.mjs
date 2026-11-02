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

import{resolve,sep}from'node:path';
export function assertGateRun(run,{account,dir}={}){
 if(!/^antfly-1278-g-[a-z0-9]+-[1-3]-[bc]$/.test(run.id??'')||!/^[a-f0-9]{32}$/.test(run.account??'')||account&&run.account!==account||run.bucket!==run.id||run.worker!==run.id||JSON.stringify(run.queues)!==JSON.stringify([run.id,run.id+'-dead'])||!Array.isArray(run.services)||run.services.length!==4||new Set(run.services.map(s=>s.role)).size!==4||!run.services.every(s=>['app','writer','reader','builder'].includes(s.role)&&s.name===run.id+(s.role==='app'?'':'-'+s.role))||resolve(run.token_file??'')!==resolve(run.dir??'','api-token')||dir&&!resolve(run.dir).startsWith(resolve(dir)+sep))throw Error('GateResourceScopeMismatch');
}
export function assertGateConfig(run,service,config){if(config.name!==service.name||config.account_id!==run.account||config.r2_buckets?.length!==1||config.r2_buckets[0].bucket_name!==run.bucket)throw Error('GateConfigurationScopeMismatch');}
