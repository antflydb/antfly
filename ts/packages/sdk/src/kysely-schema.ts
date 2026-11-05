// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import type { ColumnMetadata, DatabaseIntrospector, TableMetadata } from "kysely";
import { DEFAULT_MIGRATION_LOCK_TABLE, DEFAULT_MIGRATION_TABLE } from "kysely/migration";
import type { AntflyClient } from "./client.js";
import { MIGRATION_CLAIMS_TABLE } from "./kysely-internals.js";
import type { Table } from "./types.js";

export interface AntflyColumnMetadata extends ColumnMetadata {
  /** Output-only generated column, unlike a default that callers can override. */
  isGeneratedAlways?: boolean;
}

export type AntflyKyselyClient = Pick<AntflyClient, "tables" | "executeSQL"> &
  Partial<Pick<AntflyClient, "api">>;

export interface AntflyIntrospectorOptions {
  database?: string;
  /** Omit to enumerate every visible namespace in the database. */
  namespaces?: readonly string[];
}

/** Read scoped HTTP catalog metadata and bounded, non-mutating SQL descriptions. */
export function createAntflyIntrospector(
  client: AntflyKyselyClient,
  options: AntflyIntrospectorOptions = {}
): DatabaseIntrospector {
  const database = options.database ?? "default";
  const selected = options.namespaces ? [...options.namespaces] : undefined;
  if (!client.api && (database !== "default" || selected?.some((name) => name !== "public"))) {
    throw new TypeError("Scoped introspection requires the Antfly client's catalog API");
  }
  const schemas = async () => {
    if (!client.api) return [{ name: "public" }];
    const { data, response } = await client.api.GET("/db/v1/databases/{databaseName}/namespaces", {
      params: { path: { databaseName: database } },
    });
    if (!response.ok || !data)
      throw new Error(`Failed to list namespaces (HTTP ${response.status})`);
    return data.map((entry) => ({ name: entry.name }));
  };
  return {
    getSchemas: schemas,
    async getTables(metadataOptions) {
      const result: TableMetadata[] = [];
      const namespaces = selected ?? (await schemas()).map((schema) => schema.name);
      for (const namespace of namespaces) {
        const tables: { name: string }[] = [];
        if (!client.api) tables.push(...(await client.tables.list()));
        else {
          let cursor: string | undefined;
          const cursors = new Set<string>();
          do {
            const { data, response } = await client.api.GET(
              "/db/v1/databases/{databaseName}/namespaces/{namespaceName}/tables",
              {
                params: {
                  path: { databaseName: database, namespaceName: namespace },
                  query: { limit: 1000, cursor },
                },
              }
            );
            if (!response.ok || !data)
              throw new Error(`Failed to list namespace tables (HTTP ${response.status})`);
            tables.push(...data);
            cursor = response.headers.get("X-Antfly-Next-Cursor") ?? undefined;
            if (cursor && cursors.has(cursor))
              throw new Error("Repeated namespace table pagination cursor");
            if (cursor) cursors.add(cursor);
          } while (cursor);
        }
        for (const entry of tables) {
          if (
            !metadataOptions?.withInternalKyselyTables &&
            (entry.name === DEFAULT_MIGRATION_TABLE ||
              entry.name === DEFAULT_MIGRATION_LOCK_TABLE ||
              entry.name === MIGRATION_CLAIMS_TABLE)
          )
            continue;
          let table: Table | undefined;
          if (!client.api) table = await client.tables.get(entry.name);
          else {
            const { data, response } = await client.api.GET(
              "/db/v1/databases/{databaseName}/namespaces/{namespaceName}/tables/{tableName}",
              {
                params: {
                  path: { databaseName: database, namespaceName: namespace, tableName: entry.name },
                },
              }
            );
            if (!response.ok)
              throw new Error(`Failed to get namespace table (HTTP ${response.status})`);
            table = data;
          }
          if (!table) throw new Error(`Missing Antfly metadata for ${JSON.stringify(entry.name)}`);
          const schema = table.migration?.read_schema ?? table.schema;
          const documents = Object.values(schema?.document_schemas ?? {}).map(
            (document) =>
              document.schema as
                | {
                    properties?: Record<string, { type?: string | string[]; nullable?: boolean }>;
                    required?: string[];
                  }
                | undefined
          );
          const described = await client.executeSQL({
            statement: `SELECT * FROM "${entry.name.replace(/"/g, '""')}" LIMIT 0`,
            ...(client.api || database !== "default" || namespace !== "public"
              ? { database, namespace }
              : {}),
          });
          const columns: AntflyColumnMetadata[] = described.columns.map((column) => {
            return {
              name: column.name,
              dataType:
                column.type === "array"
                  ? `${column.element_type ?? "unknown"}[]`
                  : (column.element_type ?? column.type),
              isNullable:
                column.name !== "_id" &&
                (documents.length === 0 ||
                  documents.some((document) => {
                    const property = document?.properties?.[column.name];
                    // Missing fields become SQL NULL, even if explicit JSON null
                    // is forbidden. A column must be required in every row type.
                    return (
                      !property ||
                      !document?.required?.includes(column.name) ||
                      property.nullable === true ||
                      property?.type === "null" ||
                      (Array.isArray(property?.type) && property.type.includes("null"))
                    );
                  })),
              isAutoIncrementing: false,
              hasDefaultValue:
                schema?.column_defaults?.some((value) => value.column === column.name) ?? false,
              isGeneratedAlways:
                schema?.generated_columns?.some((value) => value.column === column.name) ?? false,
            };
          });
          result.push({
            name: entry.name,
            schema: namespace,
            isView: false,
            isForeign: false,
            columns,
          });
        }
      }
      return result;
    },
  };
}

export interface GenerateKyselyTypesOptions {
  /** Match the actual driver/parser configuration. No runtime conversion is performed. */
  integerMode: "bigint" | "string";
  /** Emit qualified keys plus unqualified aliases following this ordered path. */
  searchPath?: readonly string[];
  /** pg defaults to numbers for NUMERIC[]; use string with createAntflyPostgresTypes. */
  numericArrayMode?: "number" | "string";
}

function scalarType(type: string, options: GenerateKyselyTypesOptions): string {
  if (type.endsWith("[]")) {
    // PostgreSQL's [] type identity does not constrain rank. pg returns nested
    // arrays for multidimensional values, with null at SQL NULL elements.
    let element = type;
    while (element.endsWith("[]")) element = element.slice(0, -2);
    const elementType =
      element.toLowerCase() === "numeric"
        ? (options.numericArrayMode ?? "number")
        : scalarType(element, options);
    // An unmanaged codec can change the whole value, not only its elements.
    // Preserve unknown at the root rather than promising an array shape.
    return elementType === "unknown" ? "unknown" : `SQLArray<${elementType}>`;
  }
  switch (type.toLowerCase()) {
    case "integer":
    case "int64":
    case "int8":
    case "bigint":
      return options.integerMode;
    case "int16":
    case "int32":
    case "int2":
    case "int4":
    case "float32":
    case "float64":
    case "float4":
    case "float8":
    case "number":
      return "number";
    case "numeric":
      return "string";
    case "string":
    case "text":
    case "uuid":
      return "string";
    case "boolean":
    case "bool":
      return "boolean";
    default:
      // JSON, dates, arrays of unknown elements and new engine types need an
      // explicit application type. Never silently generate `any` or parse data.
      return "unknown";
  }
}

/** Generate deterministic source from Kysely introspection, for embedded or server. */
export function generateKyselyTypes(
  tables: readonly TableMetadata[],
  options: GenerateKyselyTypesOptions
): string {
  if (options.integerMode !== "bigint" && options.integerMode !== "string") {
    throw new TypeError("integerMode must match the driver: bigint or string");
  }
  if (
    options.numericArrayMode !== undefined &&
    options.numericArrayMode !== "number" &&
    options.numericArrayMode !== "string"
  ) {
    throw new TypeError("numericArrayMode must match the driver: number or string");
  }
  const lines = [
    "// Generated from Antfly metadata. Review unknown types and driver mappings.",
    "export interface Database {",
  ];
  const imports = new Set<string>();
  let hasArrays = false;
  const names = new Set<string>();
  const entries: { table: TableMetadata; name: string }[] = [];
  const ordered = [...tables].sort((a, b) =>
    `${a.schema ?? "public"}.${a.name}`.localeCompare(`${b.schema ?? "public"}.${b.name}`, "en")
  );
  for (const table of ordered) {
    const schema = table.schema ?? "public";
    entries.push({
      table,
      name: options.searchPath || schema !== "public" ? `${schema}.${table.name}` : table.name,
    });
  }
  if (options.searchPath) {
    const aliases = new Set<string>();
    for (const schema of options.searchPath) {
      for (const table of ordered.filter((table) => (table.schema ?? "public") === schema)) {
        if (!aliases.has(table.name)) {
          aliases.add(table.name);
          entries.push({ table, name: table.name });
        }
      }
    }
  }
  for (const { table, name } of entries) {
    if (names.has(name)) throw new Error(`Duplicate table ${JSON.stringify(name)}`);
    names.add(name);
    lines.push(`  ${JSON.stringify(name)}: {`);
    const columns = new Set<string>();
    for (const column of table.columns as AntflyColumnMetadata[]) {
      if (columns.has(column.name))
        throw new Error(`Duplicate column ${JSON.stringify(column.name)}`);
      columns.add(column.name);
      let type = scalarType(column.dataType, options);
      hasArrays ||= type.startsWith("SQLArray<");
      if (column.isNullable && type !== "unknown") type += " | null";
      if (column.isGeneratedAlways) {
        imports.add("GeneratedAlways");
        type = `GeneratedAlways<${type}>`;
      } else if (column.isAutoIncrementing || column.hasDefaultValue) {
        imports.add("Generated");
        type = `Generated<${type}>`;
      }
      lines.push(`    ${JSON.stringify(column.name)}: ${type};`);
    }
    lines.push("  };");
  }
  const declarations: string[] = [];
  if (imports.size)
    declarations.push(`import type { ${[...imports].sort().join(", ")} } from "kysely";`, "");
  if (hasArrays) declarations.push("export type SQLArray<T> = Array<T | SQLArray<T> | null>;", "");
  lines.splice(1, 0, ...declarations);
  return `${[...lines, "}", ""].join("\n")}`;
}
