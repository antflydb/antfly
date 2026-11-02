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

import type { BetterAuthOptions } from "better-auth";
import { getAuthTables, getSchema } from "better-auth/db";
import { type AuthDatabase, assertReady, identifier as q } from "./transport.js";
export type SchemaOptions = { command?: string; apply?: boolean };
interface NativeSchema {
  storage_mode?: string;
  default_type?: string;
  document_schemas?: Record<
    string,
    {
      schema: {
        properties?: Record<string, { type?: string; nullable?: boolean }>;
        required?: string[];
      };
    }
  >;
  unique_constraints?: { columns: string[]; primary?: boolean; state?: string }[];
  foreign_keys?: {
    child_columns: string[];
    parent_table: string;
    parent_columns: string[];
    on_delete: string;
  }[];
}
export function indexNames(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((item) => String(item.name));
  if (value && typeof value === "object") {
    const object = value as Record<string, unknown>;
    if (Array.isArray(object.indexes)) return indexNames(object.indexes);
    return Object.keys(object);
  }
  throw new Error("Unrecognized index status response");
}
function referenceField(options: BetterAuthOptions, model: string, field: string): string {
  const target = Object.values(getAuthTables(options)).find((table) => table.modelName === model);
  return target?.fields[field]?.fieldName ?? field;
}
function tables(options: BetterAuthOptions) {
  if (options.advanced?.database?.generateId === "serial")
    throw new Error("Antfly auth requires string IDs");
  const schema = getSchema(options);
  for (const [table, definition] of Object.entries(schema)) {
    q(table);
    if (definition.disableMigrations)
      throw new Error(`External schema management is unsupported for ${table}`);
  }
  return Object.entries(schema).sort(([, a], [, b]) => a.order - b.order);
}
export async function checkAuthSchema(
  db: AuthDatabase,
  options: BetterAuthOptions
): Promise<string[]> {
  const issues: string[] = [];
  const existing = new Set(await db.listTables());
  for (const [name, table] of tables(options)) {
    if (!existing.has(name)) {
      issues.push(`Missing table ${name}`);
      continue;
    }
    try {
      await db.assertConstraints?.(name);
    } catch {
      issues.push(`${name} constraints are not ready`);
    }
    const actual = (await db.schema(name)) as NativeSchema;
    const fields = actual.document_schemas?.[actual.default_type ?? "row"]?.schema.properties ?? {};
    if (actual.storage_mode !== "relational") issues.push(`${name} must use relational storage`);
    const declarations: typeof table.fields = {
      id: { type: "string", required: true, unique: true },
      ...table.fields,
    };
    for (const [field, value] of Object.entries(fields)) {
      if (!(field in declarations) && value.nullable === false)
        issues.push(`${name}.${field} is an unexpected required column`);
    }
    for (const [field, attribute] of Object.entries(declarations)) {
      const type =
        attribute.type === "number"
          ? "number"
          : attribute.type === "boolean"
            ? "boolean"
            : "keyword";
      if (fields[field]?.type !== type)
        issues.push(`${name}.${field} has missing or incompatible type`);
      if (attribute.required !== false && fields[field]?.nullable !== false)
        issues.push(`${name}.${field} must be required`);
      if (
        "references" in attribute &&
        attribute.references &&
        !actual.foreign_keys?.some(
          (key) =>
            key.child_columns.length === 1 &&
            key.child_columns[0] === field &&
            key.parent_table === attribute.references!.model &&
            key.parent_columns[0] ===
              referenceField(options, attribute.references!.model, attribute.references!.field) &&
            key.on_delete === (attribute.references!.onDelete ?? "cascade").replaceAll(" ", "_")
        )
      )
        issues.push(`${name}.${field} requires the declared foreign key`);
      if (
        attribute.unique &&
        !actual.unique_constraints?.some(
          (constraint) =>
            constraint.columns.length === 1 &&
            constraint.columns[0] === field &&
            (!constraint.state || constraint.state === "ready" || constraint.state === "active")
        )
      )
        issues.push(`${name}.${field} requires an active unique constraint`);
    }
    for (const index of table.indexes ?? []) {
      if (
        index.unique &&
        !actual.unique_constraints?.some(
          (constraint) => JSON.stringify(constraint.columns) === JSON.stringify(index.columns)
        )
      )
        issues.push(`${name} requires unique constraint ${index.name}`);
    }
    if (indexNames(await db.indexes(name)).length)
      issues.push(`${name} must not have document retrieval indexes`);
  }
  return issues;
}
async function executeSchema(db: AuthDatabase, sql: string) {
  const session = await db.connect();
  try {
    const result = await session.execute(sql);
    if (
      result.mutationOutcome === "committed_pending" &&
      result.ddlReceipt?.state === "pending" &&
      db.waitForSchema
    ) {
      await db.waitForSchema(result.ddlReceipt);
      return;
    }
    assertReady(result);
  } finally {
    await session.close();
  }
}

/** Fresh tables only at startup. Existing schema drift requires explicit maintenance. */
export async function prepareAuthSchema(
  db: AuthDatabase,
  options: BetterAuthOptions,
  settings: SchemaOptions = {}
): Promise<void> {
  const command =
    settings.command ?? "antfly-auth apply --config ./auth.config.ts --database ./data/app.aflite";
  const existing = new Set(await db.listTables());
  const plan = tables(options);
  const existingAuth = plan.filter(([name]) => existing.has(name));
  if (existingAuth.length && !settings.apply) {
    const problems = await checkAuthSchema(db, options);
    if (problems.length)
      throw new Error(`${problems.join("; ")}. Stop the app and run: ${command}`);
    return;
  }
  for (const [name, table] of plan) {
    if (existing.has(name)) {
      if (settings.apply) {
        const actual = (await db.schema(name)) as NativeSchema;
        const fields =
          actual.document_schemas?.[actual.default_type ?? "row"]?.schema.properties ?? {};
        for (const [field, attribute] of Object.entries(table.fields)) {
          if (fields[field]) continue;
          if (attribute.required !== false || attribute.unique || attribute.references) {
            throw new Error(
              `Adding constrained field ${name}.${field} requires an explicit data migration`
            );
          }
          const type =
            attribute.type === "number"
              ? "DOUBLE PRECISION"
              : attribute.type === "boolean"
                ? "BOOLEAN"
                : "TEXT";
          await executeSchema(db, `ALTER TABLE ${q(name)} ADD COLUMN ${q(field)} ${type}`);
        }
      }
      continue;
    }
    const columns = [`${q("id")} TEXT PRIMARY KEY`];
    for (const [field, attribute] of Object.entries(table.fields)) {
      const type =
        attribute.type === "number"
          ? "DOUBLE PRECISION"
          : attribute.type === "boolean"
            ? "BOOLEAN"
            : "TEXT";
      let column = `${q(field)} ${type}${attribute.required !== false ? " NOT NULL" : ""}${attribute.unique ? " UNIQUE" : ""}`;
      if (attribute.references) {
        const reference = attribute.references;
        const targetField = referenceField(options, reference.model, reference.field);
        const deletion = reference.onDelete ?? "cascade";
        if (!["cascade", "restrict", "no action", "set null", "set default"].includes(deletion))
          throw new Error("Unsupported reference action");
        column += ` REFERENCES ${q(reference.model)} (${q(targetField)}) ON DELETE ${deletion.toUpperCase()}`;
      }
      columns.push(column);
    }
    for (const index of table.indexes ?? []) {
      if (index.unique)
        columns.push(`CONSTRAINT ${q(index.name)} UNIQUE (${index.columns.map(q).join(",")})`);
    }
    await executeSchema(db, `CREATE TABLE ${q(name)} (${columns.join(",")})`);
    for (const index of indexNames(await db.indexes(name))) await db.removeIndex(name, index);
  }
  // Explicit apply may repair retrieval-index drift, but never changes populated column types.
  if (settings.apply)
    for (const [name] of existingAuth)
      for (const index of indexNames(await db.indexes(name))) await db.removeIndex(name, index);
  const issues = await checkAuthSchema(db, options);
  if (issues.length)
    throw new Error(
      `${issues.join("; ")}. Schema requires manual review; no destructive changes were applied. Run antfly-auth check with the same config and database.`
    );
}
