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

import {mkdir,rm} from 'node:fs/promises';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const example=fileURLToPath(new URL('../',import.meta.url));
const zigRoot=fileURLToPath(new URL('../../../zig/',import.meta.url));
const env={...process.env,WRANGLER_SEND_METRICS:'false',ZIG_GLOBAL_CACHE_DIR:zigRoot+'.zig-cache/global'};
function run(command,args,cwd=example,extra={}){
 const result=spawnSync(command,args,{cwd,env:{...env,...extra},stdio:'inherit'});
 if(result.error)throw result.error;if(result.status!==0)throw Error(command+' failed: '+result.status);
}
await mkdir(new URL('../dist/',import.meta.url),{recursive:true});
run(env.ANTFLY_ZIG??'zig',['build','distributed-sql-test','-j2'],zigRoot,{ANTFLY_SQL_HOSTED_FIXTURE:fileURLToPath(new URL('../dist/sql-fixture.json',import.meta.url)),ANTFLY_SQL_HOSTED_BUCKET_FIXTURE:fileURLToPath(new URL('../dist/sql-bucket-fixture.json',import.meta.url)),ANTFLY_SQL_HOSTED_SKEW_FIXTURE:fileURLToPath(new URL('../dist/sql-skew-fixture.json',import.meta.url)),ANTFLY_SQL_HOSTED_RANGE_FIXTURE:fileURLToPath(new URL('../dist/sql-range-fixture.json',import.meta.url))});
run(process.execPath,['build.mjs']);
run(process.execPath,['--test','test/sql-control.test.mjs','test/sql-hosted.test.mjs']);
if(process.argv.includes('--workerd')){
 await rm(new URL('../dist/sql-worker/',import.meta.url),{recursive:true,force:true});
 run(process.execPath,['node_modules/wrangler/bin/wrangler.js','deploy','--dry-run','--config','wrangler.sql.jsonc','--outdir','dist/sql-worker']);
 run(process.execPath,['--test','test/sql-workerd.test.mjs']);
}
