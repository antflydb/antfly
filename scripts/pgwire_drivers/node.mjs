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

import assert from 'node:assert/strict';
import pg from 'pg';
const pool = new pg.Pool({connectionString: process.env.ANTFLY_PGWIRE_URL});
const client = await pool.connect();
try {
  assert.equal((await client.query('SELECT 1 AS n')).rows[0].n, 1);
  assert.equal((await client.query({name: 'bound', text: 'SELECT CAST($1 AS BIGINT) AS n', values: [42]})).rows[0].n, '42');
  for (const [name, value] of [['DateStyle', 'ISO, MDY'], ['TimeZone', 'UTC'], ['extra_float_digits', '3']]) {
    assert.equal(Object.values((await client.query('SHOW ' + name)).rows[0])[0], value);
  }
  await client.query('BEGIN');
  await client.query('SELECT 1');
  await client.query('COMMIT');
} finally { client.release(); await pool.end(); }
console.log('node-postgres: PASS');
