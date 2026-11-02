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

import type { CustomAdapter } from "@better-auth/core/db/adapter";
import { type BetterAuthOptions, betterAuth } from "better-auth";
import { createAdapterFactory } from "better-auth/adapters";
import { APIError } from "better-auth/api";
import {
  type AuthKeyValueSession,
  type AuthKeyValueStore,
  type Row,
  WorkersAuthRows,
} from "./workers-store.js";

export { authDefaults, consumerAuthOptions } from "./options.js";
export {
  type AuthKeyValueSession,
  type AuthKeyValueStore,
  prepareWorkersAuth,
} from "./workers-store.js";
/** Workers-native adapter over Antfly's serializable metadata store. No SQL
 * emulation, filesystem, native Node addon, or application callback retries. */
export function workersAdapter(store: AuthKeyValueStore) {
  return (options: BetterAuthOptions) => {
    const rows = new WorkersAuthRows(options);
    async function run<T>(fn: (tx: AuthKeyValueSession) => Promise<T>): Promise<T> {
      let applicationError: unknown;
      try {
        return await store.transaction(async (tx) => {
          await rows.ready(tx);
          try {
            return await fn(tx);
          } catch (error) {
            applicationError = error;
            throw error;
          }
        });
      } catch (error) {
        // Preserve application errors only after the store confirms rollback by
        // rethrowing the callback error. A different commit/rollback error is
        // still an uncertain storage failure.
        if (error === applicationError && error instanceof APIError) throw error;
        throw new APIError("INTERNAL_SERVER_ERROR", {
          code: "ANTFLY_AUTH_DATABASE_ERROR",
          message: "Authentication storage is unavailable. Do not replay an uncertain write.",
        });
      }
    }
    const build = (
      active?: AuthKeyValueSession
    ): ReturnType<ReturnType<typeof createAdapterFactory>> =>
      createAdapterFactory({
        config: {
          adapterId: "antfly-workers",
          adapterName: "Antfly Workers",
          supportsDates: false,
          supportsJSON: false,
          supportsArrays: false,
          supportsBooleans: true,
          supportsNumericIds: false,
          transaction: async (callback) => {
            if (active) throw Error("Nested auth transaction");
            return run((tx) => callback(build(tx)));
          },
        },
        adapter: ({ getFieldName }) => {
          const op = <T>(fn: (tx: AuthKeyValueSession) => Promise<T>) =>
            active ? fn(active) : run(fn);
          const select = (model: string, row: Row, fields?: string[]) =>
            fields?.length
              ? Object.fromEntries(
                  fields.map((f) => {
                    const name = getFieldName({ model, field: f });
                    return [name, row[name]];
                  })
                )
              : row;
          const adapter: CustomAdapter = {
            async create({ model, data, select: fields }) {
              return op(async (tx) =>
                select(model, await rows.save(tx, model, data), fields)
              ) as Promise<typeof data>;
            },
            async findOne({ model, where, select: fields, join }) {
              if (join) throw Error("Native joins unsupported");
              return op(async (tx) => {
                const row = (await rows.find(tx, model, where))[0];
                return row ? (select(model, row, fields) as never) : null;
              });
            },
            async findMany({
              model,
              where,
              select: fields,
              limit = 100,
              offset = 0,
              sortBy,
              join,
            }) {
              if (
                join ||
                !Number.isSafeInteger(limit) ||
                limit < 0 ||
                limit > 4096 ||
                !Number.isSafeInteger(offset) ||
                offset < 0
              )
                throw Error("Invalid auth query");
              return op(async (tx) => {
                const found = await rows.find(tx, model, where);
                if (sortBy) {
                  const field = getFieldName({ model, field: sortBy.field });
                  found.sort((a, b) =>
                    a[field] === b[field] ? 0 : (a[field] as string) < (b[field] as string) ? -1 : 1
                  );
                  if (sortBy.direction === "desc") found.reverse();
                }
                return found
                  .slice(offset, offset + limit)
                  .map((r) => select(model, r, fields)) as never;
              });
            },
            async count({ model, where }) {
              return op(async (tx) => (await rows.find(tx, model, where)).length);
            },
            async update({ model, where, update }) {
              return op(async (tx) => {
                const row = (await rows.find(tx, model, where))[0];
                return row
                  ? ((await rows.save(tx, model, { ...row, ...update }, row)) as never)
                  : null;
              });
            },
            async updateMany({ model, where, update }) {
              return op(async (tx) => {
                const found = await rows.find(tx, model, where);
                for (const row of found) await rows.save(tx, model, { ...row, ...update }, row);
                return found.length;
              });
            },
            async delete({ model, where }) {
              await op(async (tx) => {
                const row = (await rows.find(tx, model, where))[0];
                if (row) await rows.remove(tx, model, row);
              });
            },
            async deleteMany({ model, where }) {
              return op(async (tx) => {
                const found = await rows.find(tx, model, where);
                for (const row of found) await rows.remove(tx, model, row);
                return found.length;
              });
            },
            async incrementOne({ model, where, increment, set }) {
              return op(async (tx) => {
                const row = (await rows.find(tx, model, where))[0];
                if (!row) return null;
                const data = { ...row, ...set };
                for (const [field, delta] of Object.entries(increment)) {
                  if (
                    field in (set ?? {}) ||
                    typeof delta !== "number" ||
                    !Number.isFinite(delta) ||
                    (row[field] != null && typeof row[field] !== "number")
                  )
                    throw Error("Invalid auth increment");
                  data[field] = Number(row[field] ?? 0) + delta;
                }
                return (await rows.save(tx, model, data, row)) as never;
              });
            },
          };
          return adapter;
        },
      })(options);
    return build();
  };
}

/** Construct auth with the same Better Auth peer instance as its adapter hooks. */
export function createWorkersAuth(store: AuthKeyValueStore, options: BetterAuthOptions) {
  return betterAuth({ ...options, database: workersAdapter(store) });
}
