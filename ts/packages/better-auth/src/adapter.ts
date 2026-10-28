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

import type { CleanedWhere, CustomAdapter } from "@better-auth/core/db/adapter";
import type { BetterAuthOptions } from "better-auth";
import { createAdapterFactory } from "better-auth/adapters";
import { APIError } from "better-auth/api";
import {
  type AuthDatabase,
  assertReady,
  identifier as q,
  type Row,
  type Session,
} from "./transport.js";

function storageError() {
  return new APIError("INTERNAL_SERVER_ERROR", {
    code: "ANTFLY_AUTH_DATABASE_ERROR",
    message:
      "Authentication storage is unavailable. Check database health and run antfly-auth check; do not repeat an uncertain write.",
  });
}
async function connect(db: AuthDatabase): Promise<Session> {
  let session: Session;
  try {
    session = await db.connect();
  } catch {
    throw storageError();
  }
  return {
    async execute(statement, parameters) {
      try {
        return assertReady(await session.execute(statement, parameters));
      } catch {
        throw storageError();
      }
    },
    async close() {
      try {
        await session.close();
      } catch {
        throw storageError();
      }
    },
  };
}
function safeNumber(value: unknown): unknown {
  if (typeof value !== "bigint") return value;
  const number = Number(value);
  if (!Number.isSafeInteger(number))
    throw new RangeError("Auth number exceeds the JavaScript safe integer range");
  return number;
}
export function predicate(where: CleanedWhere[] | undefined, args: unknown[]): string {
  const bind = (value: unknown) => {
    args.push(value);
    return `$${args.length}`;
  };
  let result = "";
  for (const clause of where ?? []) {
    const insensitive = clause.mode === "insensitive";
    const field = insensitive ? `LOWER(${q(clause.field)})` : q(clause.field);
    const normalize = (value: unknown) =>
      insensitive && typeof value === "string" ? value.toLowerCase() : value;
    let sql: string;
    if (clause.value === null) {
      if (!["eq", "ne"].includes(clause.operator)) throw new Error("Unsupported null comparison");
      sql = `${field} IS ${clause.operator === "ne" ? "NOT " : ""}NULL`;
    } else if (clause.operator === "in" || clause.operator === "not_in") {
      if (!Array.isArray(clause.value)) throw new Error("Membership requires an array");
      sql = clause.value.length
        ? `${field} ${clause.operator === "not_in" ? "NOT " : ""}IN (${clause.value.map((value) => bind(normalize(value))).join(",")})`
        : clause.operator === "in"
          ? "FALSE"
          : "TRUE";
    } else if (["contains", "starts_with", "ends_with"].includes(clause.operator)) {
      const escaped = String(normalize(clause.value)).replace(/[\\%_]/g, "\\$&");
      sql = `${field} LIKE ${bind(`${clause.operator !== "starts_with" ? "%" : ""}${escaped}${clause.operator !== "ends_with" ? "%" : ""}`)} ESCAPE '\\'`;
    } else {
      const operator = (
        { eq: "=", ne: "<>", lt: "<", lte: "<=", gt: ">", gte: ">=" } as Record<string, string>
      )[clause.operator];
      if (!operator) throw new Error("Unsupported auth predicate");
      sql = `${field} ${operator} ${bind(normalize(clause.value))}`;
    }
    result = result ? `(${result} ${clause.connector === "OR" ? "OR" : "AND"} ${sql})` : sql;
  }
  return result || "TRUE";
}
function limit(value: number, max = 4096): number {
  if (!Number.isInteger(value) || value < 0 || value > max)
    throw new RangeError(`Limit must be between 0 and ${max}`);
  return value;
}
/** Custom Better Auth adapter; transactions never replay application callbacks. */
export function antflyAdapter(db: AuthDatabase) {
  return (options: BetterAuthOptions) => {
    const build = (transaction?: Session): ReturnType<ReturnType<typeof createAdapterFactory>> =>
      createAdapterFactory({
        config: {
          adapterId: "antfly",
          adapterName: "Antfly",
          supportsDates: false,
          supportsJSON: false,
          supportsArrays: false,
          supportsBooleans: true,
          supportsNumericIds: false,
          customTransformOutput: ({ data, fieldAttributes }) =>
            fieldAttributes.type === "number" ? safeNumber(data) : data,
          transaction: async (callback) => {
            if (transaction) throw new Error("Nested auth transactions are not supported");
            const session = await connect(db);
            try {
              assertReady(await session.execute("BEGIN ISOLATION LEVEL READ COMMITTED"));
              const value = await callback(build(session));
              assertReady(await session.execute("COMMIT"));
              return value;
            } catch (error) {
              try {
                await session.execute("ROLLBACK");
              } catch {
                /* Never replay a failed or uncertain commit. */
              }
              throw error;
            } finally {
              await session.close();
            }
          },
        },
        adapter: ({ getFieldName }) => {
          const run = async (sql: string, args: unknown[] = []) => {
            const session = transaction ?? (await connect(db));
            try {
              return assertReady(await session.execute(sql, args));
            } catch {
              throw storageError();
            } finally {
              if (!transaction) await session.close();
            }
          };
          const find = async (
            model: string,
            where: CleanedWhere[] | undefined,
            select?: string[],
            count = 1,
            offset = 0,
            sort?: { field: string; direction: "asc" | "desc" }
          ) => {
            const args: unknown[] = [];
            const order = sort
              ? ` ORDER BY ${q(getFieldName({ model, field: sort.field }))} ${sort.direction === "desc" ? "DESC" : "ASC"}`
              : "";
            return (
              await run(
                `SELECT ${select?.length ? select.map((field) => q(getFieldName({ model, field }))).join(",") : "*"} FROM ${q(model)} WHERE ${predicate(where, args)}${order} LIMIT ${limit(count)} OFFSET ${limit(offset, Number.MAX_SAFE_INTEGER)}`,
                args
              )
            ).rows;
          };
          const update = async (
            model: string,
            where: CleanedWhere[],
            data: Row,
            increment: Record<string, number> = {},
            returning = true
          ) => {
            const args: unknown[] = [];
            const fields = Object.entries(data)
              .filter(([, value]) => value !== undefined)
              .map(([field, value]) => {
                args.push(value);
                return `${q(field)}=$${args.length}`;
              });
            for (const [field, delta] of Object.entries(increment)) {
              if (!Number.isFinite(delta) || field in data)
                throw new Error("Invalid counter increment");
              args.push(delta);
              fields.push(`${q(field)}=COALESCE(${q(field)},0)+$${args.length}`);
            }
            if (!fields.length)
              return { rows: await find(model, where, undefined, 1), rowsAffected: 0n };
            return run(
              `UPDATE ${q(model)} SET ${fields.join(",")} WHERE ${predicate(where, args)}${returning ? " RETURNING *" : ""}`,
              args
            );
          };
          const guarded = async (model: string, where: CleanedWhere[]) => {
            if (!where.length) return null;
            const first = (await find(model, where))[0];
            if (!first) return null;
            // Append the ID outside the original grouped predicate, preserving OR semantics.
            return [
              ...where,
              { field: "id", operator: "eq", value: first.id, connector: "AND", mode: "sensitive" },
            ] as CleanedWhere[];
          };
          const adapter: CustomAdapter = {
            async create({ model, data, select }) {
              const entries = Object.entries(data).filter(([, value]) => value !== undefined);
              const result = await run(
                `INSERT INTO ${q(model)} (${entries.map(([key]) => q(key)).join(",")}) VALUES (${entries.map((_, i) => `$${i + 1}`).join(",")}) RETURNING ${select?.length ? select.map((field) => q(getFieldName({ model, field }))).join(",") : "*"}`,
                entries.map(([, value]) => value)
              );
              return result.rows[0] as typeof data;
            },
            async findOne<T>({
              model,
              where,
              select,
              join,
            }: Parameters<CustomAdapter["findOne"]>[0]) {
              if (join) throw new Error("Native joins are unsupported");
              return ((await find(model, where, select))[0] as T) ?? null;
            },
            async findMany<T>({
              model,
              where,
              select,
              limit: count,
              offset,
              sortBy,
              join,
            }: Parameters<CustomAdapter["findMany"]>[0]) {
              if (join) throw new Error("Native joins are unsupported");
              return (await find(model, where, select, count, offset, sortBy)) as T[];
            },
            async count({ model, where }) {
              const args: unknown[] = [];
              return Number(
                (
                  await run(
                    `SELECT COUNT(*) AS n FROM ${q(model)} WHERE ${predicate(where, args)}`,
                    args
                  )
                ).rows[0]?.n ?? 0
              );
            },
            async update<T>({
              model,
              where,
              update: data,
            }: Parameters<CustomAdapter["update"]>[0]) {
              const guard = await guarded(model, where);
              return guard
                ? (((await update(model, guard, data as Row)).rows[0] as T) ?? null)
                : null;
            },
            async updateMany({ model, where, update: data }) {
              return Number((await update(model, where, data, {}, false)).rowsAffected);
            },
            async delete({ model, where }) {
              const guard = await guarded(model, where);
              if (guard) await adapter.deleteMany({ model, where: guard });
            },
            async deleteMany({ model, where }) {
              const args: unknown[] = [];
              return Number(
                (await run(`DELETE FROM ${q(model)} WHERE ${predicate(where, args)}`, args))
                  .rowsAffected
              );
            },
            async incrementOne<T>({
              model,
              where,
              increment,
              set,
            }: Parameters<NonNullable<CustomAdapter["incrementOne"]>>[0]) {
              const guard = await guarded(model, where);
              return guard
                ? (((await update(model, guard, set ?? {}, increment)).rows[0] as T) ?? null)
                : null;
            },
            // Better Auth's consumeOne fallback guards the complete row snapshot and exact delete count.
          };
          return adapter;
        },
      })(options);
    return build();
  };
}
