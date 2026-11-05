// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import {
  type DatabaseIntrospector,
  type Kysely,
  type MigrationLockOptions,
  PostgresAdapter,
  PostgresDialect,
  type PostgresDialectConfig,
  type RawBuilder,
  sql,
} from "kysely";
import type { TableQueryRequest } from "./types.js";

export {
  type PrepareKyselyMigrationTablesOptions,
  prepareKyselyMigrationTables,
} from "./kysely-migrations.js";
export {
  type AntflyPostgresTypes,
  createAntflyPostgresCursor,
  createAntflyPostgresTypes,
} from "./kysely-postgres-types.js";

export {
  type AntflyColumnMetadata,
  createAntflyIntrospector,
  type GenerateKyselyTypesOptions,
  generateKyselyTypes,
} from "./kysely-schema.js";

/** Standard columns added to the document columns by antfly_search. */
export interface AntflySearchColumns {
  _id: string;
  score: number;
  _highlights: unknown;
}

export interface AntflySearchOptions {
  /** Candidates admitted before outer SQL filters/joins. Must be a positive safe integer. */
  candidateLimit?: number;
}

/**
 * A SQL search relation, composable through `.as(alias)` in FROM and JOIN.
 * Row is a schema assertion, not runtime validation. Search support is a backend
 * capability: currently the embedded backend implements this relation.
 */
export function antflySearch<Row extends object>(
  table: string,
  request: TableQueryRequest | string,
  options: AntflySearchOptions = {}
): RawBuilder<Omit<Row, keyof AntflySearchColumns> & AntflySearchColumns> {
  if (!table || table.includes("\\") || [...table].some((char) => char.charCodeAt(0) < 32)) {
    throw new TypeError(
      "Search table must be nonempty and contain no control characters or backslashes"
    );
  }
  const limit = options.candidateLimit;
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit <= 0)) {
    throw new RangeError("candidateLimit must be a positive safe integer");
  }
  // The compiler requires a literal table name, not a parameter or identifier.
  // Quote it as a standard SQL string literal; all other values remain bound.
  const name = sql.raw(`'${table.replace(/'/g, "''")}'`);
  const encoded = typeof request === "string" ? request : JSON.stringify(request);
  return limit === undefined
    ? sql`antfly_search(${name}, ${encoded})`
    : sql`antfly_search(${name}, ${encoded}, ${limit})`;
}

/** Must exclude every migration runner for the same database, including other processes. */
export interface AntflyMigrationLock {
  acquire(db: Kysely<unknown>, options: MigrationLockOptions): Promise<void>;
  release(db: Kysely<unknown>, options: MigrationLockOptions): Promise<void>;
}

export interface AntflyPostgresDialectConfig extends PostgresDialectConfig {
  /** Antfly metadata provider; PostgreSQL system catalogs are not available. */
  introspector?: DatabaseIntrospector;
  /** Required for Migrator; no implicit PostgreSQL advisory locks or process-local substitute. */
  migrationLock?: AntflyMigrationLock;
}

class AntflyPostgresAdapter extends PostgresAdapter {
  constructor(private readonly lock?: AntflyMigrationLock) {
    super();
  }

  override get supportsTransactionalDdl(): boolean {
    return false;
  }

  override async acquireMigrationLock(db: Kysely<unknown>, options: MigrationLockOptions) {
    if (!this.lock) {
      throw new Error(
        "Antfly migrations require a migrationLock that excludes all migration runners"
      );
    }
    await this.lock.acquire(db, options);
  }

  override async releaseMigrationLock(db: Kysely<unknown>, options: MigrationLockOptions) {
    if (!this.lock) throw new Error("Antfly migrationLock is not configured");
    await this.lock.release(db, options);
  }
}

/**
 * Reuses Kysely's pg driver/compiler, adapting DDL and migration capabilities.
 * Supply an Antfly introspector for schema tooling. Pool ownership follows
 * PostgresDialect: db.destroy() ends the supplied pool.
 */
export class AntflyPostgresDialect extends PostgresDialect {
  constructor(private readonly antflyConfig: AntflyPostgresDialectConfig) {
    super(antflyConfig);
  }

  override createAdapter(): PostgresAdapter {
    return new AntflyPostgresAdapter(this.antflyConfig.migrationLock);
  }

  override createIntrospector(): DatabaseIntrospector {
    const introspector = this.antflyConfig.introspector;
    if (introspector) return introspector;
    const unsupported = async (): Promise<never> => {
      throw new Error("Antfly has no PostgreSQL system catalogs; configure an Antfly introspector");
    };
    return { getSchemas: unsupported, getTables: unsupported };
  }
}
