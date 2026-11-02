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

import {TASKS, PENDING, MARKS} from "./r2-collection-journal.mjs";
export const MIGRATION = "__antfly_r2_import";
export async function migrateCoordinator(store, source, namespace, now = Date.now) {
  const frozen = await source.freezeForR2(namespace);
  const current = await store.transaction(async tx => {
    let state = await tx.get(MIGRATION);
    if (!state) {
      if ((await tx.list({limit: 1})).size) throw new Error("CoordinatorMigrationTargetNotEmpty");
      state = {token: frozen.token, namespace, phase: "freeze", cursor: "", counts: {kv: 0, tasks: 0, marks: 0}};
      tx.put(MIGRATION, state);
    }
    if (state.token !== frozen.token || state.namespace !== namespace) throw new Error("CoordinatorMigrationSourceMismatch");
    return state;
  });
  if (now() < frozen.ready_at) return {has_more: true, delay_seconds: Math.ceil((frozen.ready_at-now())/1000), phase: "freeze"};
  if (current.phase === "freeze") return store.transaction(async tx => {const state=await tx.get(MIGRATION); if(state.phase === "freeze") {state.phase="kv";tx.put(MIGRATION,state);} return {has_more:true,phase:state.phase};});
  if (current.phase === "active") {await source.completeR2Migration(current.token); return {done: true, counts: current.counts};}
  const page = await source.exportFrozenR2({token: current.token, kind: current.phase, after: current.cursor});
  return store.transaction(async tx => {
    const state = await tx.get(MIGRATION);
    if (state.token !== current.token || state.phase !== current.phase || state.cursor !== current.cursor) return {has_more: true, phase: state.phase};
    for (const [key, value] of page.rows) {
      if (state.phase === "kv") {
        if (key.startsWith("__antfly_gc/") || key.startsWith("__antfly_coord_gc/") || key === MIGRATION) throw new Error("CoordinatorMigrationReservedKey");
        tx.put(key, value);
      } else if (state.phase === "tasks") {
        tx.put(TASKS+key, {done: value.done});
        if (!value.done) tx.put(PENDING+key, value.task);
      } else if (state.phase === "marks") tx.put(MARKS+key, true);
      else throw new Error("CoordinatorMigrationPhaseInvalid");
    }
    state.counts[state.phase] += page.rows.length;
    state.cursor = page.next;
    if (page.complete) {state.phase = {kv: "tasks", tasks: "marks", marks: "active"}[state.phase]; state.cursor = "";}
    tx.put(MIGRATION,state);
    // An extra delivery completes the source-side marker after target commit.
    return {has_more: true, phase: state.phase, counts: state.counts};
  });
}
