// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import { Kysely, type PostgresPool } from "kysely";
import { type MigrationResultSet, Migrator, type MigratorProps } from "kysely/migration";
import type { AntflyClient } from "./client.js";
import {
  type AntflyMigrationLockSettings,
  AntflyPersistentMigrationLock,
} from "./kysely-migration-lock.js";
import { prepareKyselyMigrationTables } from "./kysely-migrations.js";
import { type AntflyMigrationLock, AntflyPostgresDialect } from "./kysely-postgres-dialect.js";
import {
  type AntflyConfigurablePostgresCursor,
  type AntflyPostgresTypes,
  createAntflyPostgresCursor,
  createAntflyPostgresTypes,
} from "./kysely-postgres-types.js";
import { createAntflyIntrospector, generateKyselyTypes } from "./kysely-schema.js";
export interface AntflyPostgresOptions {
  pg: {
    Pool: new (options: {
      connectionString: string;
      types: AntflyPostgresTypes;
      max?: number;
      idleTimeoutMillis?: number;
      connectionTimeoutMillis?: number;
    }) => PostgresPool;
    types: AntflyPostgresTypes;
  };
  pool: {
    connectionString: string;
    max?: number;
    idleTimeoutMillis?: number;
    connectionTimeoutMillis?: number;
  };
  cursor?: AntflyConfigurablePostgresCursor;
  client: Pick<AntflyClient, "tables" | "executeSQL">;
  values?: "exact" | "pg-default";
}
export type AntflyPostgresMigrationOptions = Pick<
  MigratorProps,
  | "provider"
  | "migrationTableName"
  | "migrationLockTableName"
  | "allowUnorderedMigrations"
  | "nameComparator"
> & { lock?: AntflyMigrationLockSettings };
export interface AntflyPostgresRuntime<DB> {
  db: Kysely<DB>;
  generateTypes(): Promise<string>;
  migrateToLatest(options: AntflyPostgresMigrationOptions): Promise<MigrationResultSet>;
  inspectMigrationLock(): ReturnType<AntflyPersistentMigrationLock["inspect"]>;
  recoverMigrationLock(options: {
    ownerToken: string;
    previousRunnerStopped: true;
  }): Promise<boolean>;
  close(): Promise<void>;
}
/** Owns the pg pool, parser policy, schema generation, and migration lifecycle. */
export function createAntflyPostgres<DB>(
  options: AntflyPostgresOptions
): AntflyPostgresRuntime<DB> {
  const values = options.values ?? "exact";
  if (values !== "exact" && values !== "pg-default")
    throw new TypeError("Unknown Antfly value policy");
  const url = new URL(options.pool.connectionString);
  if (
    !["postgres:", "postgresql:"].includes(url.protocol) ||
    !["/default"].includes(url.pathname) ||
    ["options", "database", "dbname"].some((key) => url.searchParams.has(key))
  )
    throw new TypeError(
      "Antfly HTTP introspection requires the default database and public namespace"
    );
  const base = options.pg.types;
  const textArray = base.getTypeParser(1009, "text") as (value: string) => unknown;
  const numericArray = (value: unknown): unknown =>
    Array.isArray(value) ? value.map(numericArray) : value === null ? null : Number(value);
  const policy: AntflyPostgresTypes = {
    getTypeParser(oid, format = "text") {
      if (format === "text" && (oid === 20 || oid === 1700)) return (value: string) => value;
      if (format === "text" && (oid === 1016 || oid === 1231)) {
        return (value: string) =>
          oid === 1231 && values === "pg-default"
            ? numericArray(textArray(value))
            : textArray(value);
      }
      return base.getTypeParser(oid, format);
    },
  };
  const types = values === "exact" ? createAntflyPostgresTypes(policy) : policy;
  const pool = new options.pg.Pool({
    connectionString: options.pool.connectionString,
    max: options.pool.max,
    idleTimeoutMillis: options.pool.idleTimeoutMillis,
    connectionTimeoutMillis: options.pool.connectionTimeoutMillis,
    types,
  });
  const borrowed: PostgresPool = {
    connect: () => pool.connect(),
    end: async () => {},
    options: pool.options,
  };
  const introspector = createAntflyIntrospector(options.client);
  const makeDb = (lock: AntflyMigrationLock = new AntflyPersistentMigrationLock()) =>
    new Kysely<DB>({
      dialect: new AntflyPostgresDialect({
        pool: borrowed,
        introspector,
        migrationLock: lock,
        ...(options.cursor ? { cursor: createAntflyPostgresCursor(options.cursor, types) } : {}),
      }),
    });
  const db = makeDb();
  const lock = new AntflyPersistentMigrationLock();
  const active = new Set<Promise<unknown>>();
  let closing: Promise<void> | undefined;
  const checkOpen = () => {
    if (closing) throw new Error("Antfly runtime is closing");
  };
  const track = <T>(work: () => Promise<T>): Promise<T> => {
    checkOpen();
    const pending = Promise.resolve().then(work);
    active.add(pending);
    pending.then(
      () => active.delete(pending),
      () => active.delete(pending)
    );
    return pending;
  };
  return {
    db,
    generateTypes: () =>
      track(async () =>
        generateKyselyTypes(await introspector.getTables(), {
          integerMode: "string",
          numericArrayMode: values === "exact" ? "string" : "number",
        })
      ),
    migrateToLatest: (settings) =>
      track(async () => {
        const { lock: lockSettings, ...props } = settings;
        const runLock = new AntflyPersistentMigrationLock(lockSettings);
        // The outer claim covers bootstrap and Kysely's pre-lock bookkeeping too.
        // This private adapter is used only inside the already-locked connection.
        const migrationDb = makeDb({ acquire: async () => {}, release: async () => {} });
        try {
          return await migrationDb.connection().execute(async (connection) => {
            await runLock.acquire(connection as Kysely<unknown>);
            let result: MigrationResultSet;
            try {
              await prepareKyselyMigrationTables(connection, props);
              result = await new Migrator({ db: connection, ...props }).migrateToLatest();
            } catch (error) {
              result = { error };
            }
            try {
              await runLock.release(connection as Kysely<unknown>);
            } catch (releaseError) {
              return {
                ...result,
                error: result.error
                  ? Object.assign(new Error("Migration failed and claim release also failed"), {
                      cause: result.error,
                      releaseError,
                    })
                  : releaseError,
              };
            }
            return result;
          });
        } catch (error) {
          return { error };
        } finally {
          await migrationDb.destroy();
        }
      }),
    inspectMigrationLock: () => track(() => lock.inspect(db as Kysely<unknown>)),
    recoverMigrationLock: (settings) => {
      checkOpen();
      if (active.size)
        throw new Error("Wait for active runtime operations before recovering a migration claim");
      return track(() => lock.recover(db as Kysely<unknown>, settings));
    },
    close: () => {
      if (!closing)
        closing = (async () => {
          await Promise.allSettled([...active]);
          try {
            await db.destroy();
          } finally {
            await pool.end();
          }
        })();
      return closing;
    },
  };
}
