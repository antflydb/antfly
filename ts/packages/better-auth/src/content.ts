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

import { randomUUID } from "node:crypto";
import type { BetterAuthOptions } from "better-auth";
import { APIError } from "better-auth/api";
import { getSchema } from "better-auth/db";
import { type AuthDatabase, execute, identifier as q, type Row } from "./transport.js";
export interface ContentDocument {
  _id: string;
  [field: string]: string | null;
}
function document(row: Row | undefined): ContentDocument {
  if (!row || typeof row._id !== "string") throw new Error("Invalid content response");
  for (const value of Object.values(row))
    if (value !== null && typeof value !== "string") throw new Error("Invalid content field");
  return row as ContentDocument;
}
export type ContentTables = Record<string, { fields: readonly string[] }>;
interface SessionAuth {
  options: BetterAuthOptions;
  api: { getSession(input: { headers: Headers }): Promise<{ user: { id: string } } | null> };
}
function validateTables(tables: ContentTables, options: BetterAuthOptions) {
  const authTables = new Set(Object.keys(getSchema(options)));
  for (const [name, { fields }] of Object.entries(tables)) {
    q(name);
    if (authTables.has(name)) throw new Error("Auth tables cannot be exposed as content");
    if (!fields.length || new Set(fields).size !== fields.length)
      throw new Error("Content fields must be nonempty and unique");
    for (const field of fields) {
      q(field);
      if (field.startsWith("_") || ["id", "owner_id"].includes(field))
        throw new Error("Reserved content field");
    }
  }
}
export async function prepareContentSchema(
  db: AuthDatabase,
  tables: ContentTables,
  options: BetterAuthOptions
): Promise<void> {
  validateTables(tables, options);
  const existing = new Set(await db.listTables());
  for (const [name, { fields }] of Object.entries(tables)) {
    if (!existing.has(name))
      await db.createContentTable(name, {
        version: 1,
        default_type: "doc",
        document_schemas: {
          doc: {
            schema: {
              type: "object",
              properties: {
                owner_id: { type: "keyword" },
                ...Object.fromEntries(fields.map((field) => [field, { type: "string" }])),
              },
              required: ["owner_id"],
              additionalProperties: false,
            },
          },
        },
      });
    const schema = (await db.schema(name)) as {
      default_type?: string;
      document_schemas?: Record<
        string,
        { schema: { properties?: Record<string, { type?: string }> } }
      >;
    };
    const properties = schema.document_schemas?.[schema.default_type ?? "doc"]?.schema.properties;
    if (
      properties?.owner_id?.type !== "keyword" ||
      fields.some((field) => properties?.[field]?.type !== "string")
    )
      throw new Error(
        `Content schema ${name} requires keyword owner_id and declared string fields`
      );
  }
}
function dataFields(data: Row, fields: readonly string[]): [string, string][] {
  if (!data || typeof data !== "object" || Array.isArray(data))
    throw new Error("Content must be an object");
  return Object.entries(data).map(([field, value]) => {
    if (!fields.includes(field) || typeof value !== "string")
      throw new Error("Only declared string content fields may be written");
    return [field, value];
  });
}
function pageLimit(value = 20): number {
  if (!Number.isInteger(value) || value < 1 || value > 100)
    throw new Error("Content limit must be 1..100");
  return value;
}
export function createUserContent<T extends ContentTables>({
  auth,
  db,
  tables,
}: {
  auth: SessionAuth;
  db: AuthDatabase;
  tables: T;
}) {
  validateTables(tables, auth.options);
  return async function requireUser(request: Request | { headers: Headers }) {
    const session = await auth.api.getSession({ headers: request.headers });
    if (!session?.user.id)
      throw new APIError("UNAUTHORIZED", { message: "Sign in to access your content" });
    const owner = session.user.id;
    const result: Record<string, ReturnType<typeof scoped>> = {};
    function scoped(name: string, fields: readonly string[]) {
      const projection = ["_id", ...fields].map(q).join(",");
      return {
        async create(data: Row): Promise<ContentDocument> {
          const entries = dataFields(data, fields);
          const id = randomUUID();
          return document(
            (
              await execute(
                db,
                `INSERT INTO ${q(name)} (${["_id", "owner_id", ...entries.map(([field]) => field)].map(q).join(",")}) VALUES (${[id, owner, ...entries.map(([, value]) => value)].map((_, i) => `$${i + 1}`).join(",")}) RETURNING ${projection}`,
                [id, owner, ...entries.map(([, value]) => value)]
              )
            ).rows[0]
          );
        },
        async get(id: string) {
          const row =
            (
              await execute(
                db,
                `SELECT ${projection} FROM ${q(name)} WHERE owner_id=$1 AND _id=$2`,
                [owner, id]
              )
            ).rows[0] ?? null;
          return row ? document(row) : null;
        },
        async list(options: { limit?: number } = {}) {
          if (Object.keys(options).some((key) => key !== "limit"))
            throw new Error("Unsupported content query");
          return (
            await execute(
              db,
              `SELECT ${projection} FROM ${q(name)} WHERE owner_id=$1 ORDER BY _id LIMIT ${pageLimit(options.limit)}`,
              [owner]
            )
          ).rows.map(document);
        },
        async update(id: string, data: Row) {
          const entries = dataFields(data, fields);
          if (!entries.length) throw new Error("Content update must not be empty");
          const row =
            (
              await execute(
                db,
                `UPDATE ${q(name)} SET ${entries.map(([field], i) => `${q(field)}=$${i + 3}`).join(",")} WHERE owner_id=$1 AND _id=$2 RETURNING ${projection}`,
                [owner, id, ...entries.map(([, value]) => value)]
              )
            ).rows[0] ?? null;
          return row ? document(row) : null;
        },
        async delete(id: string) {
          return (
            (await execute(db, `DELETE FROM ${q(name)} WHERE owner_id=$1 AND _id=$2`, [owner, id]))
              .rowsAffected === 1n
          );
        },
        async searchText(options: { query: string; limit?: number }) {
          if (
            Object.keys(options).some((key) => !["query", "limit"].includes(key)) ||
            typeof options.query !== "string" ||
            !options.query.trim()
          )
            throw new Error("Unsupported content search");
          const response = (await db.search(name, {
            full_text_search: {
              disjuncts: fields.map((field) => ({ match: { field, text: options.query } })),
            },
            filter_query: { term: { owner_id: owner } },
            fields: ["owner_id", ...fields],
            limit: pageLimit(options.limit),
          })) as {
            responses?: { status?: number; hits?: { hits?: { _id: string; _source?: Row }[] } }[];
            hits?: { hits?: { _id: string; _source?: Row }[] };
          };
          const part = response.responses?.[0] ?? response;
          if ("status" in part && part.status !== 200)
            throw new Error("Content search unavailable");
          if (!part.hits?.hits) throw new Error("Unrecognized search response");
          // Defense in depth; no totals, snippets, or raw query response escape the capability.
          return part.hits.hits.map((hit) => {
            if (hit._source?.owner_id !== owner)
              throw new Error("Content ownership invariant violated");
            return document({
              _id: hit._id,
              ...Object.fromEntries(fields.map((field) => [field, hit._source?.[field] ?? null])),
            });
          });
        },
      };
    }
    for (const [name, { fields }] of Object.entries(tables)) result[name] = scoped(name, fields);
    return result as { [K in keyof T]: ReturnType<typeof scoped> };
  };
}
