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

import {
  type AbortableOperationOptions,
  type CompiledQuery,
  type DatabaseConnection,
  type DatabaseIntrospector,
  type DatabaseMetadataOptions,
  type Dialect,
  type Driver,
  type Kysely,
  type MigrationLockOptions,
  PostgresAdapter,
  PostgresQueryCompiler,
  type QueryResult,
  type TransactionSettings,
} from "kysely";
import type { Database } from "./database.js";
import { Connection } from "./sql.js";
import type { WireResult } from "./sql-types.js";

class KyselyConnection implements DatabaseConnection {
  constructor(readonly sql: Connection) {}
  async executeQuery<R>(
    query: CompiledQuery,
    options?: AbortableOperationOptions
  ): Promise<QueryResult<R>> {
    options?.signal?.throwIfAborted();
    const result = await this.sql.query<R>(query.sql, query.parameters);
    return { rows: result.rows, numAffectedRows: result.rowsAffected };
  }
  async *streamQuery<R>(
    query: CompiledQuery,
    chunkSize: number,
    options?: AbortableOperationOptions
  ): AsyncIterableIterator<QueryResult<R>> {
    options?.signal?.throwIfAborted();
    for await (const page of this.sql.stream<R>(query.sql, query.parameters, chunkSize)) {
      options?.signal?.throwIfAborted();
      yield { rows: page.rows, numAffectedRows: page.rowsAffected };
    }
  }
}
class AntflyDriver implements Driver {
  #connections = new Set<KyselyConnection>();
  #destroyed = false;
  constructor(readonly database: Database) {}
  async init(): Promise<void> {}
  async acquireConnection(options?: AbortableOperationOptions): Promise<DatabaseConnection> {
    options?.signal?.throwIfAborted();
    if (this.#destroyed) throw new Error("Antfly driver is destroyed");
    const connection = new KyselyConnection(await Connection.open(this.database));
    this.#connections.add(connection);
    return connection;
  }
  async beginTransaction(
    connection: DatabaseConnection,
    settings: TransactionSettings
  ): Promise<void> {
    if (settings.isolationLevel && settings.isolationLevel !== "read committed")
      throw new Error("Antfly supports READ COMMITTED isolation");
    await (connection as KyselyConnection).sql.execute(
      `BEGIN ISOLATION LEVEL READ COMMITTED${settings.accessMode === "read only" ? " READ ONLY" : ""}`
    );
  }
  async commitTransaction(connection: DatabaseConnection): Promise<void> {
    await (connection as KyselyConnection).sql.execute("COMMIT");
  }
  async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
    await (connection as KyselyConnection).sql.execute("ROLLBACK");
  }
  async savepoint(connection: DatabaseConnection, name: string): Promise<void> {
    await (connection as KyselyConnection).sql.execute(`SAVEPOINT "${name.replaceAll('"', '""')}"`);
  }
  async rollbackToSavepoint(connection: DatabaseConnection, name: string): Promise<void> {
    await (connection as KyselyConnection).sql.execute(
      `ROLLBACK TO SAVEPOINT "${name.replaceAll('"', '""')}"`
    );
  }
  async releaseSavepoint(connection: DatabaseConnection, name: string): Promise<void> {
    await (connection as KyselyConnection).sql.execute(
      `RELEASE SAVEPOINT "${name.replaceAll('"', '""')}"`
    );
  }
  async releaseConnection(connection: DatabaseConnection): Promise<void> {
    const c = connection as KyselyConnection;
    this.#connections.delete(c);
    await c.sql.close();
  }
  async destroy(): Promise<void> {
    this.#destroyed = true;
    await Promise.all([...this.#connections].map((c) => this.releaseConnection(c)));
  }
}
// All dialects using the same database share the migration queue.
const migrationLocks = new WeakMap<Database, Promise<void>>();
class AntflyAdapter extends PostgresAdapter {
  #release?: () => void;
  constructor(readonly database: Database) {
    super();
  }
  override async acquireMigrationLock(
    _db: Kysely<unknown>,
    _options: MigrationLockOptions
  ): Promise<void> {
    const prior = migrationLocks.get(this.database) ?? Promise.resolve();
    let release: () => void = () => {};
    const mine = new Promise<void>((resolve) => {
      release = resolve;
    });
    migrationLocks.set(
      this.database,
      prior.then(() => mine)
    );
    await prior;
    this.#release = release;
  }
  override async releaseMigrationLock(
    _db: Kysely<unknown>,
    _options: MigrationLockOptions
  ): Promise<void> {
    this.#release?.();
    this.#release = undefined;
  }
  override get supportsTransactionalDdl(): boolean {
    return false;
  }
}
/** PostgreSQL-style SQL compilation with an in-process Antfly session driver.
 * The caller retains ownership of the supplied Database. */
export class AntflyDialect implements Dialect {
  constructor(readonly config: { database: Database }) {}
  createDriver(): Driver {
    return new AntflyDriver(this.config.database);
  }
  createQueryCompiler(): PostgresQueryCompiler {
    return new PostgresQueryCompiler();
  }
  createAdapter(): AntflyAdapter {
    return new AntflyAdapter(this.config.database);
  }
  createIntrospector(): DatabaseIntrospector {
    const database = this.config.database;
    return {
      async getSchemas() {
        return [{ name: "public" }];
      },
      async getTables(options?: DatabaseMetadataOptions) {
        const names = (await database.listTables()).filter(
          (name) =>
            name !== "default" && (options?.withInternalKyselyTables || !name.startsWith("kysely_"))
        );
        return Promise.all(
          names.map(async (name) => {
            const table = `"${name.replaceAll('"', '""')}"`;
            const described = (await database.sqlJson({
              statement: `SELECT * FROM ${table} LIMIT 0`,
            })) as WireResult;
            const handle = await database.openTable(name);
            let schema: {
              default_type?: string;
              column_defaults?: { column: string }[];
              document_schemas?: Record<
                string,
                {
                  schema?: {
                    properties?: Record<string, { nullable?: boolean }>;
                    required?: string[];
                  };
                }
              >;
            };
            try {
              schema = (await handle.getSchema()) as typeof schema;
            } finally {
              await handle.close();
            }
            const document = schema.document_schemas?.[schema.default_type ?? "row"]?.schema;
            return {
              name,
              schema: "public",
              isView: false,
              isForeign: false,
              columns: described.columns.map((column) => ({
                name: column.name,
                dataType: column.type,
                isNullable:
                  column.name !== "_id" &&
                  !document?.required?.includes(column.name) &&
                  document?.properties?.[column.name]?.nullable !== false,
                isAutoIncrementing: false,
                hasDefaultValue:
                  schema.column_defaults?.some((entry) => entry.column === column.name) ?? false,
              })),
            };
          })
        );
      },
    };
  }
}
