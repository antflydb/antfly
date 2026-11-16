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

import { mkdir, realpath, stat } from "node:fs/promises";
import { basename, dirname, resolve } from "node:path";
import { Connection, create, type Database, open } from "@antfly/embedded";
import type { AuthDatabase } from "./transport.js";

export { antflyAdapter } from "./adapter.js";
export { createUserContent, prepareContentSchema } from "./content.js";
export { authDefaults, consumerAuthOptions } from "./options.js";
export { checkAuthSchema, prepareAuthSchema } from "./schema.js";

const registryKey = Symbol.for("antfly.better-auth.databases.v1");
const globalRegistry = globalThis as typeof globalThis & {
  [registryKey]?: Map<string, Promise<EmbeddedAuthDatabase>>;
};
globalRegistry[registryKey] ??= new Map();
const registry = globalRegistry[registryKey];
export interface EmbeddedAuthDatabase extends AuthDatabase {
  close(): Promise<void>;
}
/** Wrap a caller-owned database. The caller retains responsibility for closing it. */
export function embeddedDatabase(db: Database): AuthDatabase {
  const tableCall = async <T>(name: string, fn: (table: Database) => Promise<T>): Promise<T> => {
    const table = await db.openTable(name);
    try {
      return await fn(table);
    } finally {
      await table.close();
    }
  };
  return {
    connect: async () => {
      const connection = await Connection.open(db);
      return {
        async execute(statement, parameters) {
          const result = await connection.execute(statement, parameters);
          if (!("mutationOutcome" in result) || !("ddlReceipt" in result)) {
            throw new Error(
              "This embedded binding omits SQL write status. Upgrade @antfly/embedded alongside @antfly/better-auth before enabling login."
            );
          }
          return result;
        },
        close: () => connection.close(),
      };
    },
    listTables: () => db.listTables(),
    schema: (name) => tableCall(name, (table) => table.getSchema()),
    indexes: (name) => tableCall(name, (table) => table.listIndexes()),
    removeIndex: async (name, index) => {
      await tableCall(name, (table) => table.deleteIndex(index));
    },
    createContentTable: async (name, schema) => {
      await db.createTable(name, schema);
    },
    search: (name, query) =>
      tableCall(name, async (table) => {
        await table.runUntilIdle();
        return table.search(query);
      }),
  };
}
/** Shared initialization across hot reloads. Close once, when the application stops. */
export async function getDatabase(path: string): Promise<EmbeddedAuthDatabase> {
  if (
    Number(process.versions.node.split(".")[0]) < 24 ||
    !(
      (process.platform === "darwin" && process.arch === "arm64") ||
      (process.platform === "linux" && ["arm64", "x64"].includes(process.arch))
    )
  ) {
    throw new Error(
      "Embedded auth requires Node 24+ on macOS arm64 or glibc Linux arm64/x64. Use node:24-slim or remote Antfly."
    );
  }
  const absolute = resolve(path);
  await mkdir(dirname(absolute), { recursive: true });
  const key = await realpath(absolute).catch(async (error) => {
    if (error.code !== "ENOENT") throw error;
    return resolve(await realpath(dirname(absolute)), basename(absolute));
  });
  let task = registry.get(key);
  if (!task) {
    task = (async () => {
      const exists = await stat(key).then(
        () => true,
        (error) => {
          if (error.code === "ENOENT") return false;
          throw error;
        }
      );
      const native = await (exists ? open(key) : create(key));
      return {
        ...embeddedDatabase(native),
        async close() {
          await native.close();
          if (registry.get(key) === task) registry.delete(key);
        },
      };
    })();
    registry.set(key, task);
    task.catch(() => {
      if (registry.get(key) === task) registry.delete(key);
    });
  }
  return task;
}
