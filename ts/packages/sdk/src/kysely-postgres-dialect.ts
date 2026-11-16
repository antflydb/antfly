// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import {
  type DatabaseIntrospector,
  type Kysely,
  type MigrationLockOptions,
  PostgresAdapter,
  PostgresDialect,
  type PostgresDialectConfig,
} from "kysely";

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
