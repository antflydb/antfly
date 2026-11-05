# Type-safe SQL with Kysely

Kysely builds typed SQL; Antfly executes it. Use the embedded dialect for a local
database, or the PostgreSQL driver for a server. Optional helpers live in
`@antfly/sdk/kysely`; importing the main SDK does not load Kysely. These APIs
target Kysely 0.29.6 and later compatible 0.29 releases, and Node 24+.

## Managed PostgreSQL connection

Use the factory for an Antfly server. It owns the pool and shares one value
policy between queries, cursor streams, and generated types.

```ts
import pg from "pg";
import Cursor from "pg-cursor";
import { writeFile } from "node:fs/promises";
import { AntflyClient } from "@antfly/sdk";
import { createAntflyPostgres } from "@antfly/sdk/kysely";

const antfly = createAntflyPostgres<Database>({
  pg,
  cursor: Cursor,
  pool: { connectionString: process.env.ANTFLY_PGWIRE_URL! },
  client: new AntflyClient({ baseUrl: process.env.ANTFLY_HTTP_URL! }),
  values: "exact",
  onPoolError(error) {
    console.error("Antfly background pool error", error);
  },
});
const db = antfly.db;
try {
  const result = await antfly.migrateToLatest({
    provider, // any Kysely MigrationProvider
    lock: { waitTimeoutMs: 30_000, owner: "deploy-job" },
  });
  if (result.error) throw result.error;
  await writeFile("database.ts", await antfly.generateTypes());
} finally {
  await antfly.close();
}
```

Install `pg`, `kysely`, and optionally `pg-cursor` alongside the SDK. Supply the
same credentials to HTTP and pgwire. The factory derives the database from the
connection URL path (for example `/tenant_ops`); an omitted database means
`default`. Introspection uses explicit HTTP catalog and SQL scopes for that same
database. `exact` (the default) preserves BIGINT and NUMERIC
values, including nested arrays, as strings. `pg-default` keeps numeric arrays
as numbers. Neither preset changes global pg parsers. Managed text codecs for
booleans,
numbers, exact integers/decimals, strings/UUIDs, and their arrays are independent
of global parser changes before or after construction. Binary results for these
types are rejected; the managed runtime uses text results. Other OIDs, such as
JSON and date/time types, retain the supplied registry's behavior and generate as
`unknown`. Use the low-level dialect and explicit generation options if you need
custom mappings. Generate types through the
runtime so the output uses its parser policy. Keep generated files current as
the schema changes. `Database` is the interface exported by that generated file.

`close()` waits for runtime migration and metadata operations, then ends the
owned pool, including when no queries ran. Background errors on idle pooled
connections go to `onPoolError`; the default emits a Node warning instead of an
unhandled pool error. This listener remains installed during shutdown. Query
errors still reject the corresponding query. The callback may be synchronous or
async. If it throws or rejects, the runtime emits warnings for both the original
pool error and the handler failure, preventing an unhandled rejection. Finish
direct `db` queries before
closing; use `antfly.close()` for lifecycle management.

### Databases, namespaces, and qualified SQL

Antfly namespaces are PostgreSQL schemas. Configure the ordered lookup path for
unqualified tables; use qualified table keys such as `"analytics.events"` for
explicit namespace references. Every pool checkout restores the configured path, so a previous
borrower's session setting cannot leak into the next operation. A transaction or
reserved connection retains its own session settings until released.

```ts
const antfly = createAntflyPostgres<Database>({
  pg,
  client,
  pool: { connectionString: process.env.ANTFLY_PGWIRE_URL! }, // URL path: /tenant_ops
  searchPath: ["app", "analytics", "public"],
  migrationLockNamespace: "public",
});
await antfly.db.selectFrom("orders").selectAll().execute();
await antfly.db.selectFrom("analytics.events").selectAll().execute();
const result = await antfly.migrateToLatest({
  provider,
  migrationTableSchema: "deployment_history",
});
if (result.error) throw result.error;
await writeFile("database.ts", await antfly.generateTypes());
```

The database and lookup namespaces must exist and be visible to the principal.
The migration history schema is created if needed under the migration claim.
Use SQL `CREATE DATABASE`, Kysely's schema builder for `CREATE SCHEMA`, or the
SDK's typed `client.api` catalog endpoints to provision them. The native pgwire
search path accepts up to eight existing namespaces, each at most 128 bytes.
Pass it through `searchPath` rather than unchecked startup `options`.

HTTP introspection discovers every visible namespace in the selected database,
including tables outside the lookup path, and follows catalog pagination even
when an authorization-filtered page is empty. Generated types contain qualified
keys such as `"app.orders"` plus unqualified aliases for the first matching table
on the lookup path. A shadowed table remains available through its qualified key. Use qualified keys
for queries against a namespace outside the lookup path or one shadowed by an
earlier namespace: Kysely's `.withSchema()` changes SQL qualification, not the
TypeScript table mapping. Use the same path when compiling code against that
generated interface.

Low-level tools can use `createAntflyIntrospector(client, { database: "tenant_ops",
namespaces: ["app", "analytics"] })` to select a subset, and
`generateKyselyTypes(metadata, { integerMode: "string", searchPath: ["app", "public"] })`
to select aliases. Omitting `namespaces` discovers all visible namespaces.

The integration forwards Antfly's supported SQL shapes through Kysely's compiler;
it does not reduce them to a subset of CRUD. Native table/schema/database and
tablespace DDL, indexes, defaults, generated columns, transactions, savepoints,
and supported expressions remain available. Use `sql` for native syntax without
a Kysely builder. For a native cross-database relation, construct its components
separately rather than passing a three-part string to Kysely's table parser:

```ts
import { sql } from "kysely";

const source = sql<{ id: string }>`
  ${sql.id("other_database", "analytics", "events")}
`.as("source");
await antfly.db.selectFrom(source).select("source.id").execute();
```

Admission, authorization, provider capabilities, and SQL errors remain native.
In particular, the native runtime rejects DDL inside an explicit transaction;
the dialect reports that capability accurately and runs schema migrations
outside transactions. It does not emulate rollback for committed schema changes.

### Persistent migration ownership

`migrateToLatest` bootstraps Kysely's tables and holds a catalog-wide persistent
claim throughout bootstrap and migration execution. The same guarded path backs
`migrateUp`, `migrateDown`, and `migrateTo(target, options)` as well. Migration
history defaults to the first namespace on `searchPath`; set
`migrationTableSchema` to store it elsewhere. Different custom history names and
schemas share the same claim in `migrationLockNamespace` (default `public`),
independent of lookup path. All runners in one database must use the same claim
namespace, which must already exist and permit claim-table access. Different
databases have independent claims. All deployment runners must use this lock
protocol; unrelated SQL writers are not fenced by it. DDL is nontransactional,
so a failed migration can leave partial changes.

Claims **never expire**. The acquisition timeout only limits how long a runner
waits; a crashed runner leaves a claim that requires explicit recovery:

```ts
const claim = await antfly.inspectMigrationLock();
// Stop the previous runner and reconcile any partially applied migration first.
if (claim) {
  const removed = await antfly.recoverMigrationLock({
    ownerToken: claim.ownerToken,
    previousRunnerStopped: true,
  });
  if (!removed) throw new Error("Claim changed; inspect it again");
}
```

Recovery conditionally removes only the observed token. The stopped-runner
assertion is an operator responsibility; automatic lease takeover requires
server-side fencing and is unavailable. Do not recover a live runner. Ownership
errors include `code` and `claim`; an uncertain claim or release must be reconciled
before retrying application migrations. The SDK never replays an uncertain
migration write. The private `_antfly_kysely_migration_claims` table is hidden
from default introspection. Low-level dialect users can configure
`AntflyPersistentMigrationLock` directly, while retaining responsibility for
bootstrap and pool lifecycle.

## Embedded connection

```sh
npm install @antfly/embedded @antfly/sdk kysely@^0.29.6
```

```ts
import { create } from "@antfly/embedded";
import { AntflyDialect } from "@antfly/embedded/kysely";
import { Kysely } from "kysely";

interface Tables {
  articles: { id: string; title: string; body: string; published: boolean };
}

const database = await create("app.aflite");
const db = new Kysely<Tables>({
  dialect: new AntflyDialect({ database }),
});

try {
  await db.schema.createTable("articles").ifNotExists()
    .addColumn("id", "text", (col) => col.primaryKey())
    .addColumn("title", "text", (col) => col.notNull())
    .addColumn("body", "text", (col) => col.notNull())
    .addColumn("published", "boolean", (col) => col.notNull())
    .execute();

  await db.insertInto("articles").values({
    id: "article-1", title: "Getting started", body: "Search and SQL with Antfly",
    published: true,
  }).onConflict((conflict) => conflict.column("id").doNothing()).execute();

  const articles = await db.selectFrom("articles")
    .select(["id", "title"]).where("published", "=", true).execute();
  console.log(articles);
} finally {
  await db.destroy(); // closes SQL sessions
  await database.close(); // caller owns the native database
}
```

See the [runnable embedded example](../../embedded/examples/kysely.mjs) for search
setup, querying, and type generation in one process.

## Server connection

Enable the optional pgwire listener on a server with authentication configured:

```json
{"enable_auth":true,"pgwire":{"enabled":true,"bind_host":"127.0.0.1","bind_port":5432}}
```

Install the PostgreSQL driver and, for streaming, its cursor implementation:

```sh
npm install @antfly/sdk kysely@^0.29.6 pg@8.16.3 pg-cursor@2.15.3
npm install -D @types/pg @types/pg-cursor
```

Set `ANTFLY_PGWIRE_URL` to your authenticated PostgreSQL URL, for example
`postgres://USER:PASSWORD@127.0.0.1:5432/default`, and `ANTFLY_HTTP_URL` to the
matching HTTP server origin. Obtain credentials through your deployment's secret
configuration. Remote pgwire requires an externally protected transport; see
the [pgwire contract](https://github.com/antflydb/antfly/blob/main/zig/pkg/antfly/src/pgwire/README.md).

```ts
import { AntflyClient } from "@antfly/sdk";
import {
  AntflyPostgresDialect, createAntflyIntrospector,
  createAntflyPostgresTypes, createAntflyPostgresCursor,
} from "@antfly/sdk/kysely";
import { Kysely } from "kysely";
import { Pool, types } from "pg";
import Cursor from "pg-cursor";

const credentials = new URL(process.env.ANTFLY_PGWIRE_URL!);
const client = new AntflyClient({
  baseUrl: process.env.ANTFLY_HTTP_URL!,
  auth: {
    username: decodeURIComponent(credentials.username),
    password: decodeURIComponent(credentials.password),
  },
});

const numericTypes = createAntflyPostgresTypes(types);
const db = new Kysely<Tables>({
  dialect: new AntflyPostgresDialect({
    pool: new Pool({ connectionString: process.env.ANTFLY_PGWIRE_URL, types: numericTypes }),
    cursor: createAntflyPostgresCursor(Cursor, numericTypes),
    introspector: createAntflyIntrospector(client),
  }),
});

try {
  // Reuse the schema builders and ordinary queries from the embedded example.
  console.log(await db.selectFrom("articles").selectAll().execute());
} finally {
  await db.destroy(); // ends the supplied pool
}
```

`Tables` is the interface from the embedded example. Both transports compile
PostgreSQL-style SQL. The server dialect reuses Kysely's standard pg driver and
compiler. Its overrides disable transactional DDL, replace PostgreSQL catalog
introspection, and require explicit migration locking. Standard `PostgresDialect`
also works for ordinary SQL queries, but its PostgreSQL introspection and advisory
locks are not supported by Antfly.

For a low-level pool, use `createAntflyIntrospector(client, { database })` with
the same database, principal, and deployment. Its catalog calls enumerate visible
namespaces in that database, and every SQL description carries an explicit
database/namespace scope. The managed factory wires this automatically.

## Search as a SQL relation

The embedded backend exposes `antfly_search` as a relation. A search helper
produces that SQL relation, so SQL joins and filters execute inside Antfly.
Server SQL currently rejects this relation with SQLSTATE `0A000`; the helper
does not fall back to fetching and joining documents in JavaScript.

On an embedded database with an indexed `articles` table:

```ts
import { antflySearch } from "@antfly/sdk/kysely";

const hits = await db
  .selectFrom(antflySearch<Tables["articles"]>("articles", {
    full_text_search: { query: "body:search" },
  }, { candidateLimit: 100 }).as("hits"))
  .select(["hits._id", "hits.title", "hits.score"])
  .where("hits.published", "=", true)
  .orderBy("hits.score", "desc")
  .orderBy("hits._id")
  .limit(10)
  .execute();
```

The relation adds `_id`, `score`, and `_highlights` to document columns. The row
generic asserts your schema; it does not validate runtime data. Aliases work in
`innerJoin` and other ordinary Kysely composition. Pass a query string such as
`"body:search"`, or an SDK `TableQueryRequest`. Values are bound parameters;
the compiler-required table literal is escaped separately.

`candidateLimit: 100` admits up to 100 candidates **before** the outer SQL filter.
An outer `LIMIT 10` can return fewer than ten surviving hits. Put eligible filters
in the search request's `filter_query` when they should constrain search candidates.
There is no promise of exhaustive top-K or automatic SQL filter pushdown.
Add `_id` after score ordering for deterministic ties within a captured result.
The engine currently bounds search candidates to 10,000.

The helper accepts existing full-text, vector, and hybrid request types; index
availability and provider support still apply. Search materializes a bounded
candidate relation; using `.stream()` does not make candidate discovery unbounded.
Native statement capture governs consistency. Embedded search over a table with
uncommitted writes in the same transaction is rejected rather than merging staged
writes into index results. Query authorization remains a backend responsibility;
the helper adds no authorization or snapshot guarantees.

## Generate database interfaces

Both dialects expose Kysely introspection. Generate a source file during schema
development, review it, and commit it alongside application queries:

```ts
import { writeFile } from "node:fs/promises";
import { generateKyselyTypes } from "@antfly/sdk/kysely";

const tables = await db.introspection.getTables();
await writeFile("database.ts", generateKyselyTypes(tables, {
  integerMode: "bigint", // embedded; use "string" for pg's default BIGINT parser
}));
```

The generator emits quoted table/column names, nullability, `Generated<T>` for
defaults, and `GeneratedAlways<T>` for output-only generated columns. Unknown
types, JSON, and timestamps remain `unknown` for application-specific refinement.
Only imports that the generated source uses are included. Run this against a
stable schema; metadata reads are not an atomic catalog export.

For the server connection configured above, generate with
`{ integerMode: "string", numericArrayMode: "string" }`.

Nullable unions such as `type: ["string", "null"]` retain `null` even when the
field is required. Optional fields also retain SQL NULL when absent, even if
explicit JSON null is forbidden with `nullable: false`. Columns must be required
in every document type to be non-nullable. Default introspection excludes
`kysely_migration`, `kysely_migration_lock`, and the private
`_antfly_kysely_migration_claims` table; other `kysely_` names and custom
bookkeeping table names remain visible, matching Kysely's standard introspector.
PostgreSQL array columns use a generated recursive
`SQLArray<T>` type: `pg` can return one-dimensional or nested arrays for the same
SQL array type, with nullable elements. Array rank is not fixed by the column's
type identity. Embedded SQL arrays retain their native envelope and generate as
`unknown` for application-specific refinement.

Integer mode describes driver behavior; it does not convert results. Embedded
SQL returns integer columns as `bigint`; pg returns BIGINT as strings and smaller
integer widths as numbers. NUMERIC scalars return decimal text. Unmodified `pg`
parses NUMERIC array elements as JavaScript numbers, which can lose precision;
type generation therefore defaults to `numericArrayMode: "number"`.

The server recipe uses `createAntflyPostgresTypes(types)` to preserve NUMERIC
scalar and array elements as exact decimal strings, including scale and special
values. Pass its registry to both the pool and `createAntflyPostgresCursor`:
pg-cursor does not inherit pool-local parsers. These helpers do not change global
`pg.types` settings, BIGINT parsing, or other OIDs. With this configuration, set
`numericArrayMode: "string"` when generating types. Embedded SQL arrays keep their
native envelopes and are unaffected by this option.

Do not coerce BIGINT to `number` unless you enforce JavaScript's safe integer
range. If you customize parsers, match the generated scalar and array types to
the actual parser configuration.

## Transactions and migrations

Use `db.transaction().setIsolationLevel("read committed").execute(...)` for portable
transactions. Embedded supports READ COMMITTED; server stronger isolation depends
on capable providers. Savepoints are supported. Schema DDL runs outside explicit
transactions, so a failed migration can leave previously committed DDL in place.
Inspect results and repair the migration; do not blindly retry uncertain writes.

Import `Migrator` from `kysely/migration` with Kysely 0.29. The embedded dialect
queues migrators that share the same native `Database` object. That queue is not
a distributed lock or a guarantee across independent handles.

With the managed PostgreSQL factory, use `antfly.migrateToLatest` above.
For embedded or low-level dialect usage, first call `prepareKyselyMigrationTables(db)`. Kysely's automatic bookkeeping
DDL uses `varchar(255)`, which Antfly does not accept. This helper creates only
the bookkeeping tables with native `TEXT` columns, under the dialect's migration
lock. It does not rewrite application DDL. If you customize migration table names,
pass the same `migrationTableName` and `migrationLockTableName` options to the helper
and to `Migrator`. The helper also shares `migrationTableSchema`, creates that
schema if needed, and supplies its value as `lockTableSchema` to custom locks.

```ts
import { prepareKyselyMigrationTables } from "@antfly/sdk/kysely";
import { Migrator } from "kysely/migration";

const migrationTables = {
  migrationTableSchema: "deployment_history",
  migrationTableName: "app_migrations",
  migrationLockTableName: "app_migration_lock",
};
await prepareKyselyMigrationTables(db, migrationTables);
const migrator = new Migrator({
  db,
  ...migrationTables,
  provider: { async getMigrations() { return {
    "001_create_notes": { async up(migrationDb) {
      await migrationDb.schema.createTable("notes")
        .addColumn("id", "text", (col) => col.primaryKey())
        .addColumn("body", "text").execute();
    } },
  }; } },
});
const result = await migrator.migrateToLatest();
if (result.error) throw result.error;
```

For low-level server migrations, supply `migrationLock` with asynchronous `acquire(db,
options)` and `release(db, options)` methods. Its implementation must exclude
**all** runners for the database, including other processes. A deployment lock
or exclusive migration job can provide that contract. The adapter throws when a
lock is missing; it never assumes PostgreSQL advisory locks exist. Kysely can
bootstrap its metadata tables before asking the adapter for the migration lock.
Do not supply no-op callbacks unless deployment orchestration already guarantees
exclusive execution throughout the run. Lock release failures must be surfaced.

Unsupported schema operations and query shapes retain Antfly's SQL errors.
Do not infer PostgreSQL extension or catalog compatibility from the wire protocol.

## Tested capabilities

| Capability | Embedded | Server through pgwire |
| --- | --- | --- |
| Typed CRUD, parameters, joins | Native Kysely driver | Kysely pg driver |
| SQL NULL and exact integers | Integers as bigint | BIGINT strings with default pg parsers |
| Commit, rollback, savepoints | READ COMMITTED | Native provider isolation contracts |
| Result streaming beyond a page | Native cursors | Supply pg-cursor |
| Schema introspection and type generation | Native container catalog | Scoped HTTP catalog, all visible namespaces, ordered lookup aliases |
| Kysely migrations | Queue shared by Database object | Factory includes persistent ownership and bootstrap |
| Transactional schema DDL | Unsupported | Unsupported |
| Search relation with SQL composition | Supported | Explicitly unsupported |

Native Kysely tests run with the embedded SDK suite. The pgwire driver conformance
suite runs real Kysely CRUD, transaction, streaming, introspection, and migration
queries against the candidate server. Compile-time tests verify inferred search
selections and invalid field/request rejection. Neither transport needs a new
HTTP SQL execution driver.
