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

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {Bucket} from './object-fixture.mjs';
import {instantiateHost} from '../../../zig/lib/objectstore/wasm/asyncify.mjs';
import {createR2Transport} from '../../../zig/lib/objectstore/wasm/r2.mjs';

// Generate with ANTFLY_SQL_HOSTED_FIXTURE=../examples/cloudflare-serverless/dist/sql-fixture.json
// zig build distributed-sql-test, then instrument the WASM through npm run build.
const module = await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless.wasm', import.meta.url)));
for (const mode of ['shuffle','bucket','skew','range']) {
const bucketed=mode==='bucket';
const fixture = JSON.parse(await readFile(new URL(mode==='range'?'../dist/sql-range-fixture.json':mode==='skew'?'../dist/sql-skew-fixture.json':bucketed?'../dist/sql-bucket-fixture.json':'../dist/sql-fixture.json', import.meta.url)));
test(mode==='skew'?'WASM skew kernel finalizes accepted native map tiles with global NULL ownership':`WASM ${mode} joins native retained Parquet cuts with WAL overlays and recovers accepted tasks`, async () => {
  const buckets = Object.fromEntries(['source','execution'].map(name => [name,new Bucket()]));
  for (const object of fixture.objects) await buckets[object.bucket].put(object.key, new Uint8Array(Buffer.from(object.body_base64,'base64')));
  const transport = createR2Transport(buckets,{maxPutBytes:8*1024*1024});
  const host = await instantiateHost(module,transport);
  const request = {config:fixture.config,tenant:fixture.tenant,owner:'wasm-coordinator'};
  const submitted = fixture.query?{query:fixture.query}:await host.request({...request,op:'sql_submit',raw_plan:fixture.raw_plan});
  request.query = submitted.query;
  assert.equal(await host.request({...request,op:'sql_result'}),null);
  let complete=false,accepted=false,lost=false;
  for (let turn=0;turn<64;turn++) {
    const result = await host.request({...request,op:'sql_turn'});
    if (result.complete) { complete=true;break; }
    for (const ticket of result.tickets) {
      const task={...request,op:'sql_execute',owner:'wasm-worker',ticket};
      // Lose an accepted task's final write response. A new instance resolves
      // the durable claim/output instead of rerunning and duplicating rows.
      const fault=async (call,body) => {
        const result=await transport(call,body);
        if (!lost&&call.op==='put'&&call.key?.includes(bucketed?'/stage/0/':'/stage/1/')&&new TextDecoder().decode(body).includes('"status":"succeeded"')) {
          lost=true;throw Error('lost completion response');
        }
        return result;
      };
      let outcome;
      try { outcome=await host.request(task,{transport:fault}); }
      catch(error) {
        if (!lost) throw error;
        const replacement=await instantiateHost(module,transport);
        outcome=await replacement.request(task);
      }
      assert.equal(outcome.status,'succeeded');
      accepted=true;
    }
  }
  assert.ok(complete);assert.ok(accepted);assert.ok(lost);
  const result=await host.request({...request,op:'sql_result'});
  assert.equal(result.rows,fixture.expected_rows??2);if(mode!=='range') assert.equal(result.pages.length,fixture.expected_pages??(bucketed?4:1));
  assert.ok(result.pages[0].index_bytes>0);
  const other=await instantiateHost(module,transport);
  assert.deepEqual(await other.request({...request,op:'sql_result'}),result);
  await assert.rejects(other.request({...request,op:'sql_submit',tenant:Array(64).fill(98),raw_plan:fixture.raw_plan}),/SqlExecutionUnauthorized/);
  const sourceObjects=buckets.source.objects.size;
  const expiry=JSON.parse(fixture.raw_plan).expires_ms;
  const collector=await instantiateHost(module,transport,{clock:()=>expiry+31000});
  let deleted=0;
  for(let turn=0;turn<16&&buckets.execution.objects.size;turn++){
    const collected=await collector.request({...request,op:'sql_collect'});
    deleted+=collected.deleted;
  }
  assert.ok(deleted>0);
  assert.equal(buckets.execution.objects.size,0,'R2 expiry cleanup left execution records or exchanges');
  assert.equal(buckets.source.objects.size,sourceObjects,'execution GC crossed into source retention');
});

}
