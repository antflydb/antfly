// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import type { ColumnMetadata, DatabaseIntrospector, TableMetadata } from "kysely";
import { DEFAULT_MIGRATION_LOCK_TABLE, DEFAULT_MIGRATION_TABLE } from "kysely/migration";
import type { AntflyClient } from "./client.js";

export interface AntflyColumnMetadata extends ColumnMetadata {
  /** Output-only generated column, unlike a default that callers can override. */
  isGeneratedAlways?: boolean;
}

/**
 * Introspect the HTTP API's default database/public namespace. Use the same
 * principal and database as the pg pool. This reads metadata, not table rows,
 * and does not create durable prepared-statement resources.
 */
export function createAntflyIntrospector(
  client: Pick<AntflyClient, "tables" | "executeSQL">
): DatabaseIntrospector {
  return {
    async getSchemas() {
      return [{ name: "public" }];
    },
    async getTables(options) {
      const tables = await client.tables.list();
      const result: TableMetadata[] = [];
      for (const entry of tables) {
        if (
          !options?.withInternalKyselyTables &&
          (entry.name === DEFAULT_MIGRATION_TABLE || entry.name === DEFAULT_MIGRATION_LOCK_TABLE)
        )
          continue;
        const table = await client.tables.get(entry.name);
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
          schema: "public",
          isView: false,
          isForeign: false,
          columns,
        });
      }
      return result;
    },
  };
}

export interface GenerateKyselyTypesOptions {
  /** Match the actual driver/parser configuration. No runtime conversion is performed. */
  integerMode: "bigint" | "string";
}

function scalarType(type: string, integerMode: "bigint" | "string"): string {
  if (type.endsWith("[]")) {
    // PostgreSQL's [] type identity does not constrain rank. pg returns nested
    // arrays for multidimensional values, with null at SQL NULL elements.
    let element = type;
    while (element.endsWith("[]")) element = element.slice(0, -2);
    return `SQLArray<${scalarType(element, integerMode)}>`;
  }
  switch (type.toLowerCase()) {
    case "integer":
    case "int64":
    case "int8":
    case "bigint":
      return integerMode;
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
  const lines = [
    "// Generated from Antfly metadata. Review unknown types and driver mappings.",
    "export interface Database {",
  ];
  const imports = new Set<string>();
  let hasArrays = false;
  const names = new Set<string>();
  for (const table of [...tables].sort((a, b) => a.name.localeCompare(b.name, "en"))) {
    const name =
      table.schema && table.schema !== "public" ? `${table.schema}.${table.name}` : table.name;
    if (names.has(name)) throw new Error(`Duplicate table ${JSON.stringify(name)}`);
    names.add(name);
    lines.push(`  ${JSON.stringify(name)}: {`);
    const columns = new Set<string>();
    for (const column of table.columns as AntflyColumnMetadata[]) {
      if (columns.has(column.name))
        throw new Error(`Duplicate column ${JSON.stringify(column.name)}`);
      columns.add(column.name);
      hasArrays ||= column.dataType.endsWith("[]");
      let type = scalarType(column.dataType, options.integerMode);
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
