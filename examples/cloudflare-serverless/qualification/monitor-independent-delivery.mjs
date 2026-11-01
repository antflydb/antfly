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

import{open}from'node:fs/promises';import{resolve}from'node:path';import{startDestinationMonitor}from'./destination-history.mjs';
// Run locally beside the remote client. Only sanitized account delivery status
// is persisted; account authorization remains in Wrangler's local login flow.
const[manifest,output]=process.argv.slice(2);if(!manifest||!output)throw Error('UsageObserverManifestDeliveryHistoryOutput');const path=resolve(output),samples=[];let handle=await open(path,'wx',0o600),stopping;
const stopped=new Promise(r=>stopping=r),stop=()=>stopping();process.once('SIGINT',stop);process.once('SIGTERM',stop);const timer=setTimeout(stop,90*60*1000);
// Positional writes avoid a growing offset after truncate; readers consume the
// file only after this monitor stops and the final poll has been fsynced.
async function save(){const bytes=Buffer.from(JSON.stringify({schema:1,samples},null,2));await handle.write(bytes,0,bytes.length,0);await handle.truncate(bytes.length);await handle.sync();}
try{const monitor=await startDestinationMonitor(resolve(manifest),{samples,persist:save});console.log(JSON.stringify({monitoring:true,output:path,credential_exported:false}));await stopped;await monitor.stop();}finally{clearTimeout(timer);process.removeListener('SIGINT',stop);process.removeListener('SIGTERM',stop);await handle.close();}
