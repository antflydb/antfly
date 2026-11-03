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

import { DatabaseSync } from "node:sqlite";
import { CollectionJournal } from "../collection-journal.mjs";
export function fixture() {
  const db = new DatabaseSync(":memory:"), values = new Map(); let now = 1000000;
  const storage = { sql: {exec(sql, ...args) {
    const statement = db.prepare(sql), rows = /^SELECT/.test(sql) ? statement.all(...args) : (statement.run(...args), []);
    return {toArray: () => rows};
  }}, kv: {get: key => structuredClone(values.get(key)), put: (key, value) => values.set(key, structuredClone(value)), delete: key => values.delete(key),
    list: ({prefix, startAfter = "", limit}) => ([...values].filter(([key]) => key.startsWith(prefix) && key > startAfter).sort(([a], [b]) => a.localeCompare(b)).slice(0, limit)).values()},
  transactionSync(fn) { db.exec("BEGIN"); const before = structuredClone(values); try { const result = fn(); db.exec("COMMIT"); return result; } catch (error) { db.exec("ROLLBACK"); values.clear(); for (const entry of before) values.set(...entry); throw error; } }};
  return {db, storage, now: () => now, tick: ms => now += ms, journal: () => new CollectionJournal(storage, () => now)};
}
