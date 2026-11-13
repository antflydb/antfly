// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import {
  sql,
  type DatabaseIntrospector,
  type Kysely,
  type MigrationLockOptions,
  type TableMetadata,
} from "kysely";
import {
  DEFAULT_MIGRATION_LOCK_TABLE,
  DEFAULT_MIGRATION_TABLE,
  MIGRATION_LOCK_ID,
  type MigratorProps,
} from "kysely/migration";

/** Share these options directly with Kysely's Migrator. */
export type PrepareKyselyMigrationTablesOptions = Pick<
  MigratorProps,
  "migrationTableName" | "migrationLockTableName" | "migrationTableSchema"
>;

/**
 * Create Kysely's bookkeeping tables using native TEXT instead of VARCHAR(255).
 * Call before Migrator on either Antfly dialect. Runs under the dialect's
 * migration lock, outside transactions, on a reserved connection. Application
 * migrations still use Kysely's normal Migrator and lock lifecycle.
 */
export async function prepareKyselyMigrationTables<DB>(
  db: Kysely<DB>,
  options: PrepareKyselyMigrationTablesOptions = {}
): Promise<void> {
  const migrationTable = options.migrationTableName ?? DEFAULT_MIGRATION_TABLE;
  const lockTable = options.migrationLockTableName ?? DEFAULT_MIGRATION_LOCK_TABLE;
  if (!migrationTable || !lockTable || migrationTable === lockTable) {
    throw new TypeError("Migration and lock table names must be nonempty and distinct");
  }
  const lockOptions: MigrationLockOptions = {
    lockTable,
    lockRowId: MIGRATION_LOCK_ID,
    ...(options.migrationTableSchema ? { lockTableSchema: options.migrationTableSchema } : {}),
  };
  await db.connection().execute(async (connection) => {
    const adapter = connection.getExecutor().adapter;
    await adapter.acquireMigrationLock(connection, lockOptions);
    try {
      const schema = options.migrationTableSchema;
      if (
        schema &&
        !(await connection.introspection.getSchemas()).some((entry) => entry.name === schema)
      ) {
        await connection.schema.createSchema(schema).ifNotExists().execute();
      }
      const builder = schema ? connection.schema.withSchema(schema) : connection.schema;
      await builder
        .createTable(migrationTable)
        .ifNotExists()
        .addColumn("name", "text", (column) => column.notNull().primaryKey())
        .addColumn("timestamp", "text", (column) => column.notNull())
        .execute();
      await builder
        .createTable(lockTable)
        .ifNotExists()
        .addColumn("id", "text", (column) => column.notNull().primaryKey())
        .addColumn("is_locked", "integer", (column) => column.notNull().defaultTo(0))
        .execute();
    } finally {
      await adapter.releaseMigrationLock(connection, lockOptions);
    }
  });
}

/** Private bootstrap view: Kysely needs only its own table names before loading migrations. */
export function createMigrationBootstrap<DB>(
  full: DatabaseIntrospector,
  connection: Kysely<DB>,
  options: PrepareKyselyMigrationTablesOptions & Pick<MigratorProps, "provider">
): { introspector: DatabaseIntrospector; provider: MigratorProps["provider"] } {
  let preflight = true;
  const namespace = options.migrationTableSchema ?? "public";
  const names = [
    options.migrationTableName ?? DEFAULT_MIGRATION_TABLE,
    options.migrationLockTableName ?? DEFAULT_MIGRATION_LOCK_TABLE,
  ];
  return {
    introspector: {
      getSchemas: () => full.getSchemas(),
      async getTables(metadataOptions) {
        if (!preflight) return full.getTables(metadataOptions);
        const tables: TableMetadata[] = [];
        for (const name of names) {
          try {
            // Use the same reserved connection and SQL authorization/scope as
            // DDL. LIMIT 0 admits no documents and never scans the application
            // catalog or describes unrelated tables over HTTP.
            await sql`SELECT 1 FROM ${sql.id(namespace, name)} LIMIT 0`.execute(connection);
            tables.push({ name, schema: namespace, isView: false, isForeign: false, columns: [] });
          } catch (error) {
            const code =
              error && typeof error === "object" && "code" in error ? error.code : undefined;
            if (code !== "42P01") throw error;
          }
        }
        return tables;
      },
    },
    provider: {
      async getMigrations() {
        // Restore the complete, fresh introspector before ANY application
        // provider/up/down code runs. No application gets the bootstrap view.
        preflight = false;
        return options.provider.getMigrations();
      },
    },
  };
}
