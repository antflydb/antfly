// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { type RawBuilder, sql } from "kysely";
import type { TableQueryRequest } from "./types.js";

export {
  type AntflyMigrationClaim,
  AntflyMigrationLockError,
  type AntflyMigrationLockSettings,
  AntflyPersistentMigrationLock,
} from "./kysely-migration-lock.js";
export {
  type PrepareKyselyMigrationTablesOptions,
  prepareKyselyMigrationTables,
} from "./kysely-migrations.js";
export {
  type AntflyPostgresMigrationOptions,
  type AntflyPostgresOptions,
  type AntflyPostgresPool,
  type AntflyPostgresRuntime,
  createAntflyPostgres,
} from "./kysely-postgres.js";
export {
  type AntflyMigrationLock,
  AntflyPostgresDialect,
  type AntflyPostgresDialectConfig,
} from "./kysely-postgres-dialect.js";
export {
  type AntflyPostgresTypes,
  createAntflyPostgresCursor,
  createAntflyPostgresTypes,
} from "./kysely-postgres-types.js";

export {
  type AntflyColumnMetadata,
  type AntflyIntrospectorOptions,
  type AntflyKyselyClient,
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
