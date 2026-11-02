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

import type { AuthKeyValueSession, AuthKeyValueStore } from "../src/workers.js";
/** Transactional model for adapter contract tests, never a production backend. */
export class MemoryAuthStore implements AuthKeyValueStore {
  data = new Map<string, unknown>();
  pending = Promise.resolve();
  failCommit = false;
  calls = 0;
  reads = 0;
  lists = 0;
  transaction<T>(callback: (tx: AuthKeyValueSession) => Promise<T>): Promise<T> {
    const task = this.pending.then(async () => {
      this.calls++;
      const copy = structuredClone(this.data);
      const tx: AuthKeyValueSession = {
        get: async (key) => {
          this.reads++;
          return structuredClone(copy.get(key));
        },
        put: (key, value) => {
          copy.set(key, structuredClone(value));
        },
        delete: (key) => {
          copy.delete(key);
        },
        list: async ({ prefix, startAfter = "", limit }) => {
          this.lists++;
          return new Map(
            [...copy]
              .filter(([key]) => key.startsWith(prefix) && key > startAfter)
              .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
              .slice(0, limit)
          );
        },
      };
      const result = await callback(tx);
      if (this.failCommit) {
        this.failCommit = false;
        throw Error("test-only uncertain commit");
      }
      this.data = copy;
      return result;
    });
    this.pending = task.then(
      () => {},
      () => {}
    );
    return task;
  }
}
