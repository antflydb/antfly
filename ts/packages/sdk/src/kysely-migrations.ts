// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import type { Kysely, MigrationLockOptions } from "kysely";
import {
  DEFAULT_MIGRATION_LOCK_TABLE,
  DEFAULT_MIGRATION_TABLE,
  MIGRATION_LOCK_ID,
} from "kysely/migration";

export interface PrepareKyselyMigrationTablesOptions {
  /** Pass the same name to Migrator. */
  migrationTable?: string;
  /** Pass the same name to Migrator. */
  migrationLockTable?: string;
}

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
  const migrationTable = options.migrationTable ?? DEFAULT_MIGRATION_TABLE;
  const lockTable = options.migrationLockTable ?? DEFAULT_MIGRATION_LOCK_TABLE;
  if (!migrationTable || !lockTable || migrationTable === lockTable) {
    throw new TypeError("Migration and lock table names must be nonempty and distinct");
  }
  const lockOptions: MigrationLockOptions = { lockTable, lockRowId: MIGRATION_LOCK_ID };
  await db.connection().execute(async (connection) => {
    const adapter = connection.getExecutor().adapter;
    await adapter.acquireMigrationLock(connection, lockOptions);
    try {
      await connection.schema
        .createTable(migrationTable)
        .ifNotExists()
        .addColumn("name", "text", (column) => column.notNull().primaryKey())
        .addColumn("timestamp", "text", (column) => column.notNull())
        .execute();
      await connection.schema
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
