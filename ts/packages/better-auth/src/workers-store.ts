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

import type { CleanedWhere } from "@better-auth/core/db/adapter";
import type { BetterAuthOptions } from "better-auth";
import { getAuthTables, getSchema } from "better-auth/db";
export type Row = Record<string, unknown>;
type Where = Omit<CleanedWhere, "value"> & { value: unknown };
type Fields = Record<
  string,
  {
    type: string;
    logicalType: string;
    required: boolean;
    unique: boolean;
    references?: { model: string; field: string; onDelete: string };
  }
>;
export interface AuthKeyValueSession {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): void | Promise<void>;
  delete(key: string): void | Promise<void>;
  list(options: {
    prefix: string;
    startAfter?: string;
    limit: number;
  }): Promise<Map<string, unknown>>;
}
/** Private native Antfly capability: fenced, serializable and durable. Callbacks
 * run once, failed callbacks roll back, and uncertain commits are never replayed.
 * Eventually consistent KV and per-operation atomicity do not satisfy this API. */
export interface AuthKeyValueStore {
  transaction<T>(callback: (tx: AuthKeyValueSession) => Promise<T>): Promise<T>;
}
export const authPrefix = "better-auth/v1/";
const maxRows = 4096;
const identifier = (s: string) => {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(s)) throw Error("Invalid auth identifier");
  return s;
};
async function hash(value: unknown): Promise<string> {
  return Array.from(
    new Uint8Array(
      await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(value)))
    ),
    (b) => b.toString(16).padStart(2, "0")
  ).join("");
}
export function workersSchema(options: BetterAuthOptions): Record<string, Fields> {
  if (options.advanced?.database?.generateId === "serial") throw Error("String IDs required");
  return Object.fromEntries(
    Object.entries(getSchema(options)).map(([model, table]) => {
      identifier(model);
      if (table.disableMigrations || table.indexes?.length)
        throw Error("Unsupported Workers auth schema extension");
      const fields = { id: { type: "string", required: true, unique: true }, ...table.fields };
      return [
        model,
        Object.fromEntries(
          Object.entries(fields).map(([field, value]) => {
            identifier(field);
            if (
              !["string", "date", "number", "boolean", "json", "string[]", "number[]"].includes(
                String(value.type)
              )
            )
              throw Error("Unsupported Workers auth field");
            const ref = ("references" in value ? value.references : null) as {
              model: string;
              field?: string;
              onDelete?: string;
            } | null;
            const target = ref
              ? Object.values(getAuthTables(options)).find((t) => t.modelName === ref.model)
              : null;
            return [
              field,
              {
                logicalType: String(value.type),
                type: ["json", "string[]", "number[]"].includes(String(value.type))
                  ? "string"
                  : String(value.type),
                required: value.required !== false,
                unique: value.unique === true,
                ...(ref
                  ? {
                      references: {
                        model: ref.model,
                        field: target?.fields[ref.field ?? "id"]?.fieldName ?? ref.field ?? "id",
                        onDelete: ref.onDelete ?? "cascade",
                      },
                    }
                  : {}),
              },
            ];
          })
        ),
      ];
    })
  );
}
export async function prepareWorkersAuth(
  store: AuthKeyValueStore,
  options: BetterAuthOptions
): Promise<void> {
  const fingerprint = await hash(workersSchema(options));
  await store.transaction(async (tx) => {
    const old = await tx.get(`${authPrefix}schema`);
    if (old != null && old !== fingerprint)
      throw Error("Auth schema changed; explicit migration required");
    if (old == null) await tx.put(`${authPrefix}schema`, fingerprint);
  });
}
function match(row: Row, where: Where[] = []): boolean {
  let result: boolean | undefined;
  for (const w of where) {
    let a = row[w.field] ?? null,
      b = w.value;
    if (w.mode === "insensitive") {
      if (typeof a === "string") a = a.toLowerCase();
      if (typeof b === "string") b = b.toLowerCase();
      if (Array.isArray(b)) b = b.map((v) => (typeof v === "string" ? v.toLowerCase() : v));
    }
    let yes: boolean;
    switch (w.operator) {
      case "eq":
        yes = a === b;
        break;
      case "ne":
        yes = a !== b;
        break;
      case "in":
      case "not_in":
        if (!Array.isArray(b)) throw Error("Invalid membership");
        yes = b.includes(a);
        if (w.operator === "not_in") yes = !yes;
        break;
      case "contains":
        yes = typeof a === "string" && a.includes(String(b));
        break;
      case "starts_with":
        yes = typeof a === "string" && a.startsWith(String(b));
        break;
      case "ends_with":
        yes = typeof a === "string" && a.endsWith(String(b));
        break;
      case "lt":
        yes = a != null && b != null && (a as number) < (b as number);
        break;
      case "lte":
        yes = a != null && b != null && (a as number) <= (b as number);
        break;
      case "gt":
        yes = a != null && b != null && (a as number) > (b as number);
        break;
      case "gte":
        yes = a != null && b != null && (a as number) >= (b as number);
        break;
      default:
        throw Error("Unsupported auth predicate");
    }
    result = result === undefined ? yes : w.connector === "OR" ? result || yes : result && yes;
  }
  return result ?? true;
}
async function list(tx: AuthKeyValueSession, prefix: string): Promise<[string, unknown][]> {
  const rows: [string, unknown][] = [];
  let after = "";
  for (;;) {
    const page = await tx.list({ prefix, startAfter: after, limit: 128 });
    if (!(page instanceof Map) || page.size > 128) throw Error("Invalid auth page");
    for (const [key, value] of page) {
      if (!key.startsWith(prefix) || key <= after || rows.length >= maxRows)
        throw Error("Auth query budget or cursor invalid");
      after = key;
      rows.push([key, value]);
    }
    if (page.size < 128) return rows;
  }
}
export class WorkersAuthRows {
  readonly schema;
  readonly fingerprint;
  constructor(options: BetterAuthOptions) {
    this.schema = workersSchema(options);
    this.fingerprint = hash(this.schema);
  }
  private fields(model: string) {
    const fields = Object.hasOwn(this.schema, model) ? this.schema[model] : undefined;
    if (!fields) throw Error("Unknown auth model");
    return fields;
  }
  private prefix(model: string) {
    return `${authPrefix}row/${identifier(model)}/`;
  }
  private async key(model: string, id: unknown) {
    return `${this.prefix(model)}${await hash(id)}`;
  }
  private async index(model: string, field: string, value: unknown, insensitive = false) {
    return (
      authPrefix +
      "index/" +
      identifier(model) +
      "/" +
      identifier(field) +
      "/" +
      (insensitive ? "i/" : "s/") +
      (await hash(insensitive && typeof value === "string" ? value.toLowerCase() : value)) +
      "/"
    );
  }
  async ready(tx: AuthKeyValueSession) {
    if ((await tx.get(`${authPrefix}schema`)) !== (await this.fingerprint))
      throw Error("Auth schema not prepared");
  }
  async find(tx: AuthKeyValueSession, model: string, where: Where[] = []): Promise<Row[]> {
    const fields = this.fields(model);
    for (const w of where) if (!Object.hasOwn(fields, w.field)) throw Error("Unknown auth field");
    // Every scalar has a value index. Only an all-AND predicate can safely use
    // one equality as a candidate superset; mixed OR falls back to a bounded scan.
    const indexed = where.some((w) => w.connector === "OR")
      ? undefined
      : where.find((w) => w.operator === "eq" && w.value != null);
    let rows: Row[];
    if (indexed?.field === "id" && indexed.mode !== "insensitive") {
      const row = await tx.get(await this.key(model, indexed.value));
      rows = row == null ? [] : [row as Row];
    } else if (indexed) {
      const keys = await list(
        tx,
        await this.index(model, indexed.field, indexed.value, indexed.mode === "insensitive")
      );
      rows = [];
      for (const [, key] of keys) {
        if (typeof key !== "string" || !key.startsWith(this.prefix(model)))
          throw Error("Invalid auth index");
        const row = await tx.get(key);
        if (!row) throw Error("Missing indexed auth row");
        rows.push(row as Row);
      }
    } else rows = (await list(tx, this.prefix(model))).map(([, row]) => row as Row);
    return rows.filter((row) => match(row, where));
  }
  private async indexes(tx: AuthKeyValueSession, model: string, row: Row, remove: boolean) {
    const suffix = await hash(row.id),
      key = await this.key(model, row.id);
    for (const [name, value] of Object.entries(row))
      if (value != null)
        for (const insensitive of typeof value === "string" ? [false, true] : [false]) {
          const index = (await this.index(model, name, value, insensitive)) + suffix;
          if (remove) await tx.delete(index);
          else await tx.put(index, key);
        }
  }
  async save(tx: AuthKeyValueSession, model: string, input: Row, old?: Row): Promise<Row> {
    const fields = this.fields(model),
      row = Object.fromEntries(
        Object.entries(input)
          .filter(([, v]) => v !== undefined)
          .map(([k, v]) => {
            if (typeof v === "bigint") {
              const n = Number(v);
              if (!Number.isSafeInteger(n)) throw Error("Unsafe auth integer");
              return [k, n];
            }
            return [k, v];
          })
      );
    if (
      typeof row.id !== "string" ||
      !row.id ||
      row.id.length > 256 ||
      (old && row.id !== old.id) ||
      new TextEncoder().encode(JSON.stringify(row)).byteLength > 65536
    )
      throw Error("Invalid auth row");
    for (const name of Object.keys(row))
      if (!Object.hasOwn(fields, name)) throw Error("Unknown auth field");
    for (const [name, field] of Object.entries(fields)) {
      const value = row[name];
      if (value == null) {
        if (field.required) throw Error("Missing auth field");
        continue;
      }
      if (
        typeof value !== (field.type === "date" ? "string" : field.type) ||
        (typeof value === "number" && !Number.isFinite(value))
      )
        throw Error("Invalid auth field type");
      if (
        field.unique &&
        (
          await this.find(tx, model, [
            { field: name, operator: "eq", value, connector: "AND", mode: "sensitive" },
          ])
        ).some((r) => r.id !== row.id)
      )
        throw Error("Auth uniqueness violation");
      if (
        field.references &&
        !(
          await this.find(tx, field.references.model, [
            {
              field: field.references.field,
              operator: "eq",
              value,
              connector: "AND",
              mode: "sensitive",
            },
          ])
        ).length
      )
        throw Error("Missing auth reference");
    }
    if (old)
      for (const [child, declarations] of Object.entries(this.schema))
        for (const [name, declaration] of Object.entries(declarations)) {
          const ref = declaration.references;
          if (
            ref?.model === model &&
            old[ref.field] !== row[ref.field] &&
            (
              await this.find(tx, child, [
                {
                  field: name,
                  operator: "eq",
                  value: old[ref.field],
                  connector: "AND",
                  mode: "sensitive",
                },
              ])
            ).length
          )
            throw Error("Referenced auth key update unsupported");
        }
    const key = await this.key(model, row.id);
    if (!old && (await tx.get(key)) != null) throw Error("Auth ID exists");
    if (old) await this.indexes(tx, model, old, true);
    await tx.put(key, row);
    await this.indexes(tx, model, row, false);
    return structuredClone(row);
  }
  async remove(tx: AuthKeyValueSession, model: string, row: Row, depth = 0): Promise<void> {
    if (depth > 8) throw Error("Auth cascade budget exceeded");
    for (const [child, fields] of Object.entries(this.schema))
      for (const [name, field] of Object.entries(fields))
        if (field.references?.model === model) {
          const refs = await this.find(tx, child, [
            {
              field: name,
              operator: "eq",
              value: row[field.references.field],
              connector: "AND",
              mode: "sensitive",
            },
          ]);
          for (const ref of refs)
            if (field.references.onDelete === "cascade")
              await this.remove(tx, child, ref, depth + 1);
            else if (field.references.onDelete === "set null")
              await this.save(tx, child, { ...ref, [name]: null }, ref);
            else throw Error("Auth row referenced");
        }
    await this.indexes(tx, model, row, true);
    await tx.delete(await this.key(model, row.id));
  }
}
