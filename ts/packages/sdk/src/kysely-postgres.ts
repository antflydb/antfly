// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import process from "node:process";
import { CompiledQuery, type DatabaseIntrospector, Kysely, type PostgresPool } from "kysely";
import { type MigrationResultSet, Migrator, type MigratorProps } from "kysely/migration";
import {
  type AntflyMigrationLockSettings,
  AntflyPersistentMigrationLock,
} from "./kysely-migration-lock.js";
import { createMigrationBootstrap, prepareKyselyMigrationTables } from "./kysely-migrations.js";
import { type AntflyMigrationLock, AntflyPostgresDialect } from "./kysely-postgres-dialect.js";
import { createManagedPostgresTypes } from "./kysely-postgres-policy.js";
import {
  type AntflyConfigurablePostgresCursor,
  type AntflyPostgresTypes,
  createAntflyPostgresCursor,
} from "./kysely-postgres-types.js";
import {
  type AntflyKyselyClient,
  createAntflyIntrospector,
  generateKyselyTypes,
} from "./kysely-schema.js";
/** Owned pg pool, including its asynchronous idle-client error channel. */
export interface AntflyPostgresPool extends PostgresPool {
  on(event: "error", listener: (error: Error) => void): unknown;
}

export interface AntflyPostgresOptions {
  pg: {
    Pool: new (options: {
      connectionString: string;
      types: AntflyPostgresTypes;
      max?: number;
      idleTimeoutMillis?: number;
      connectionTimeoutMillis?: number;
    }) => AntflyPostgresPool;
    types: AntflyPostgresTypes;
  };
  pool: {
    connectionString: string;
    max?: number;
    idleTimeoutMillis?: number;
    connectionTimeoutMillis?: number;
  };
  cursor?: AntflyConfigurablePostgresCursor;
  client: AntflyKyselyClient;
  /** Ordered namespace lookup, pinned whenever a pooled connection is reserved. */
  searchPath?: readonly string[];
  /** Shared claim storage for every migration runner in this database. Defaults to public. */
  migrationLockNamespace?: string;
  values?: "exact" | "pg-default";
  /** Background idle-client errors; defaults to a Node warning. Query errors still reject their query. */
  onPoolError?: (error: Error) => void | Promise<void>;
}
export type AntflyPostgresMigrationOptions = Pick<
  MigratorProps,
  | "provider"
  | "migrationTableSchema"
  | "migrationTableName"
  | "migrationLockTableName"
  | "allowUnorderedMigrations"
  | "nameComparator"
> & { lock?: Omit<AntflyMigrationLockSettings, "namespace"> };
export interface AntflyPostgresRuntime<DB> {
  db: Kysely<DB>;
  generateTypes(): Promise<string>;
  migrateToLatest(options: AntflyPostgresMigrationOptions): Promise<MigrationResultSet>;
  migrateUp(options: AntflyPostgresMigrationOptions): Promise<MigrationResultSet>;
  migrateDown(options: AntflyPostgresMigrationOptions): Promise<MigrationResultSet>;
  migrateTo(
    target: Parameters<Migrator["migrateTo"]>[0],
    options: AntflyPostgresMigrationOptions
  ): Promise<MigrationResultSet>;
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
    ["options", "database", "dbname"].some((key) => url.searchParams.has(key))
  ) {
    throw new TypeError(
      "Select the database in the connection URL path and namespaces through searchPath"
    );
  }
  const database = decodeURIComponent(url.pathname.slice(1)) || "default";
  if (
    Buffer.byteLength(database) > 128 ||
    [...database].some(
      (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127
    )
  )
    throw new TypeError("Invalid Antfly database name");
  url.pathname = `/${encodeURIComponent(database)}`;
  const searchPath = [...(options.searchPath ?? ["public"])];
  if (!searchPath.length || searchPath.length > 8)
    throw new RangeError("searchPath requires 1 to 8 namespaces");
  const lockNamespace = options.migrationLockNamespace ?? "public";
  for (const name of [...searchPath, lockNamespace]) {
    if (!/^[_a-zA-Z][_a-zA-Z0-9-]{0,127}$/.test(name))
      throw new TypeError("Invalid Antfly search-path namespace");
  }
  const introspector = createAntflyIntrospector(options.client, { database });
  if (!options.client.api && searchPath.some((name) => name !== "public"))
    throw new TypeError("Scoped introspection requires the Antfly client's catalog API");
  const pathStatement = CompiledQuery.raw(
    `SET search_path TO ${searchPath.map((name) => `"${name}"`).join(", ")}`
  );
  const types = createManagedPostgresTypes(options.pg.types, values);
  const pool = new options.pg.Pool({
    connectionString: url.toString(),
    max: options.pool.max,
    idleTimeoutMillis: options.pool.idleTimeoutMillis,
    connectionTimeoutMillis: options.pool.connectionTimeoutMillis,
    types,
  });
  // Keep this listener installed through pool.end(), including shutdown errors.
  const onPoolError = options.onPoolError;
  pool.on("error", (error) => {
    if (!onPoolError) {
      process.emitWarning(error);
      return;
    }
    const reportFailure = (failure: unknown) => {
      process.emitWarning(error);
      process.emitWarning(failure instanceof Error ? failure : new Error(String(failure)));
    };
    try {
      Promise.resolve(onPoolError(error)).catch(reportFailure);
    } catch (failure) {
      reportFailure(failure);
    }
  });
  const borrowed: PostgresPool = {
    connect: async () => {
      const connection = await pool.connect();
      try {
        await connection.query(pathStatement.sql, []);
        return connection;
      } catch (error) {
        connection.release();
        throw error;
      }
    },
    Client: pool.Client,
    end: async () => {},
    options: pool.options,
  };
  const makeDb = (
    lock: AntflyMigrationLock = new AntflyPersistentMigrationLock({ namespace: lockNamespace }),
    metadata: DatabaseIntrospector = introspector
  ) =>
    new Kysely<DB>({
      dialect: new AntflyPostgresDialect({
        pool: borrowed,
        introspector: metadata,
        migrationLock: lock,
        ...(options.cursor ? { cursor: createAntflyPostgresCursor(options.cursor, types) } : {}),
      }),
    });
  const db = makeDb();
  const lock = new AntflyPersistentMigrationLock({ namespace: lockNamespace });
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
  const migrate = (
    settings: AntflyPostgresMigrationOptions,
    action: (migrator: Migrator) => Promise<MigrationResultSet>
  ) =>
    track(async () => {
      const { lock: lockSettings, ...provided } = settings;
      const props = {
        ...provided,
        migrationTableSchema: provided.migrationTableSchema ?? searchPath[0],
      };
      const runLock = new AntflyPersistentMigrationLock({
        ...lockSettings,
        namespace: lockNamespace,
      });
      // The outer claim covers bootstrap and Kysely's pre-lock bookkeeping too.
      // This private adapter is used only inside the already-locked connection.
      let migrationMetadata = introspector;
      const migrationDb = makeDb(
        { acquire: async () => {}, release: async () => {} },
        {
          getSchemas: () => migrationMetadata.getSchemas(),
          getTables: (options) => migrationMetadata.getTables(options),
        }
      );
      try {
        return await migrationDb.connection().execute(async (connection) => {
          await runLock.acquire(connection as Kysely<unknown>);
          let result: MigrationResultSet;
          try {
            await prepareKyselyMigrationTables(connection, props);
            const bootstrap = createMigrationBootstrap(introspector, connection, props);
            migrationMetadata = bootstrap.introspector;
            result = await action(
              new Migrator({ db: connection, ...props, provider: bootstrap.provider })
            );
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
    });
  return {
    db,
    generateTypes: () =>
      track(async () =>
        generateKyselyTypes(await introspector.getTables(), {
          integerMode: "string",
          searchPath,
          numericArrayMode: values === "exact" ? "string" : "number",
        })
      ),
    migrateToLatest: (settings) => migrate(settings, (migrator) => migrator.migrateToLatest()),
    migrateUp: (settings) => migrate(settings, (migrator) => migrator.migrateUp()),
    migrateDown: (settings) => migrate(settings, (migrator) => migrator.migrateDown()),
    migrateTo: (target, settings) => migrate(settings, (migrator) => migrator.migrateTo(target)),
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
