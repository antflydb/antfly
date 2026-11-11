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

// Run through test_pgwire_drivers.py after building @antfly/sdk.
import { fork } from "node:child_process";
import { once } from "node:events";
import assert from "node:assert/strict";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";
import Cursor from "pg-cursor";
import { AntflyClient } from "../../ts/packages/sdk/dist/index.js";
import {
  AntflyPostgresDialect,
  createAntflyPostgres,
  AntflyPersistentMigrationLock,
  antflySearch,
  createAntflyIntrospector,
  createAntflyPostgresCursor,
  createAntflyPostgresTypes,
  generateKyselyTypes,
} from "../../ts/packages/sdk/dist/kysely.js";

const workers = new Set();
const connectionString = process.env.ANTFLY_PGWIRE_URL;
const url = new URL(connectionString);
const client = new AntflyClient({
  baseUrl: process.env.ANTFLY_HTTP_URL,
  auth: { username: decodeURIComponent(url.username), password: decodeURIComponent(url.password) },
});
const introspector = createAntflyIntrospector(client);
const numericTypes = createAntflyPostgresTypes(pg.types);
const ExactCursor = createAntflyPostgresCursor(Cursor, numericTypes);
const makeDatabase = () =>
  new Kysely({
    dialect: new AntflyPostgresDialect({
      pool: new pg.Pool({ connectionString, types: numericTypes }),
      cursor: ExactCursor,
      introspector,
      migrationLock: new AntflyPersistentMigrationLock(),
    }),
  });
const db = makeDatabase();
const second = makeDatabase();
const standard = new Kysely({
  dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString }) }),
});
try {
  // The standard dialect is sufficient for SQL execution.
  assert.deepEqual((await sql`SELECT 1 AS n`.execute(standard)).rows, [{ n: 1 }]);
  await assert.rejects(standard.introspection.getTables());
  await db.schema
    .createTable("kysely_people")
    .addColumn("id", "text", (col) => col.primaryKey())
    .addColumn("name", "text")
    .addColumn("n", "bigint")
    .addColumn("payload", "jsonb")
    .execute();
  const hostile = "Ada'); DROP TABLE kysely_people; --";
  await db
    .insertInto("kysely_people")
    .values({
      id: "a",
      name: hostile,
      n: "9007199254740993",
      payload: { nested: [true, null] },
    })
    .execute();
  assert.deepEqual(await db.selectFrom("kysely_people").selectAll().execute(), [
    {
      id: "a",
      name: hostile,
      n: "9007199254740993",
      payload: { nested: [true, null] },
    },
  ]);
  const updated = await db
    .updateTable("kysely_people")
    .set({ name: null })
    .where("id", "=", "a")
    .returning(["id", "name"])
    .execute();
  assert.deepEqual(updated, [{ id: "a", name: null }]);
  await db
    .transaction()
    .setIsolationLevel("read committed")
    .execute(async (tx) => {
      await tx.insertInto("kysely_people").values({ id: "b", n: "-9223372036854775808" }).execute();
      await sql`SAVEPOINT test_savepoint`.execute(tx);
      await tx.insertInto("kysely_people").values({ id: "discard" }).execute();
      await sql`ROLLBACK TO SAVEPOINT test_savepoint`.execute(tx);
      await sql`RELEASE SAVEPOINT test_savepoint`.execute(tx);
    });
  await assert.rejects(
    db.transaction().execute(async (tx) => {
      await tx.insertInto("kysely_people").values({ id: "rollback" }).execute();
      throw new Error("rollback sentinel");
    }),
    /rollback sentinel/
  );
  assert.deepEqual(
    (await db.selectFrom("kysely_people").select("id").orderBy("id").execute()).map((r) => r.id),
    ["a", "b"]
  );
  await db
    .transaction()
    .setIsolationLevel("serializable")
    .execute(async (tx) => {
      assert.equal((await tx.selectFrom("kysely_people").select("id").execute()).length, 2);
    });
  await assert.rejects(
    db
      .transaction()
      .setIsolationLevel("read uncommitted")
      .execute(async () => {})
  );

  await db.schema.createTable("kysely_numbers").addColumn("n", "bigint").execute();
  // Keep writes within native per-statement bounds; the read still crosses pages.
  for (let offset = 0; offset < 300; offset += 100) {
    await db
      .insertInto("kysely_numbers")
      .values(Array.from({ length: 100 }, (_, n) => ({ n: n + offset })))
      .execute();
  }
  let count = 0;
  for await (const row of db.selectFrom("kysely_numbers").select("n").stream(17)) {
    assert.equal(typeof row.n, "string");
    count++;
  }
  assert.equal(count, 300);
  // Early termination must close the portal and release the pooled connection.
  for await (const row of db.selectFrom("kysely_numbers").select("n").stream(17)) {
    assert.ok(row);
    break;
  }
  assert.equal((await db.selectFrom("kysely_numbers").selectAll().execute()).length, 300);
  const joined = await db
    .selectFrom("kysely_people as p")
    .innerJoin("kysely_people as q", "p.id", "q.id")
    .select("p.id")
    .orderBy("p.id")
    .execute();
  assert.deepEqual(joined, [{ id: "a" }, { id: "b" }]);
  assert.equal(
    (await db.deleteFrom("kysely_people").where("id", "=", "b").executeTakeFirst()).numDeletedRows,
    1n
  );

  await client.tables.create("kysely_shapes", {
    schema: {
      storage_mode: "relational",
      default_type: "row",
      document_schemas: {
        row: {
          schema: {
            type: "object",
            required: ["body", "matrix"],
            properties: {
              body: { type: ["string", "null"] },
              optional_nonnull: { type: "string", nullable: false },
              matrix: { type: "sql_array", "x-antfly-sql-type": "int32", nullable: false },
              amount: { type: "number", "x-antfly-sql-type": "numeric", nullable: true },
              amounts: { type: "sql_array", "x-antfly-sql-type": "numeric", nullable: true },
            },
          },
        },
      },
    },
  });
  await db
    .insertInto("kysely_shapes")
    .values({
      _id: "shape",
      body: null,
      matrix: [
        [1, null],
        [2, 3],
      ],
    })
    .execute();
  assert.deepEqual(
    await db.selectFrom("kysely_shapes").select(["body", "matrix", "optional_nonnull"]).execute(),
    [
      {
        body: null,
        optional_nonnull: null,
        matrix: [
          [1, null],
          [2, 3],
        ],
      },
    ]
  );
  const decimals = [
    ["9007199254740993.1200", null, "1.2300"],
    ["-0.000001", "NaN", "Infinity"],
  ];
  await db
    .updateTable("kysely_shapes")
    .set({ amount: "9007199254740993.1200", amounts: decimals })
    .where("_id", "=", "shape")
    .execute();
  const decimalQuery = db
    .selectFrom("kysely_shapes")
    .select(["amount", "amounts"])
    .where("_id", "=", "shape");
  const expectedDecimal = { amount: "9007199254740993.1200", amounts: decimals };
  assert.deepEqual(await decimalQuery.execute(), [expectedDecimal]);
  const streamedDecimals = [];
  for await (const row of decimalQuery.stream(1)) streamedDecimals.push(row);
  assert.deepEqual(streamedDecimals, [expectedDecimal]);
  // A separate, unmodified pool keeps pg's default behavior. The exact
  // registry must not change process-global parsers or claim their types.
  const defaultDecimals = await standard
    .selectFrom("kysely_shapes")
    .select(["amount", "amounts"])
    .where("_id", "=", "shape")
    .executeTakeFirstOrThrow();
  assert.equal(defaultDecimals.amount, expectedDecimal.amount);
  assert.equal(typeof defaultDecimals.amounts[0][0], "number");
  await db
    .insertInto("kysely_shapes")
    .values([
      { _id: "empty", body: null, matrix: [[0]], amounts: [] },
      { _id: "null", body: null, matrix: [[0]], amounts: null },
    ])
    .execute();
  assert.deepEqual(
    await db
      .selectFrom("kysely_shapes")
      .select(["_id", "amounts"])
      .where("_id", "in", ["empty", "null"])
      .orderBy("_id")
      .execute(),
    [
      { _id: "empty", amounts: [] },
      { _id: "null", amounts: null },
    ]
  );

  const metadata = await introspector.getTables({ withInternalKyselyTables: true });
  assert.ok(metadata.find((table) => table.name === "kysely_people"));
  assert.match(generateKyselyTypes(metadata, { integerMode: "string" }), /"n": string \| null;/);
  const generated = generateKyselyTypes(metadata, {
    integerMode: "string",
    numericArrayMode: "string",
  });
  assert.match(generated, /"body": string \| null;/);
  assert.match(generated, /"matrix": SQLArray<number>;/);
  assert.match(generated, /"optional_nonnull": string \| null;/);
  assert.match(generated, /"amounts": SQLArray<string> \| null;/);
  assert.match(
    generateKyselyTypes(metadata, { integerMode: "string" }),
    /"amounts": SQLArray<number> \| null;/
  );

  // Server search support is an explicit capability boundary, never an
  // application-side search plus join fallback.
  await assert.rejects(
    db
      .selectFrom(antflySearch("kysely_people", "name:Ada").as("hits"))
      .select("hits._id")
      .execute(),
    (error) => error.code === "0A000"
  );

  const overriddenOids = [20, 23, 1007, 1009, 1016, 1231];
  const previousParsers = overriddenOids.map((oid) => [oid, pg.types.getTypeParser(oid, "text")]);
  for (const oid of overriddenOids) pg.types.setTypeParser(oid, () => "wrong before construction");
  const runtime = createAntflyPostgres({
    pg,
    client,
    cursor: Cursor,
    pool: { connectionString, max: 1 },
  });
  try {
    for (const oid of overriddenOids) pg.types.setTypeParser(oid, () => "wrong after construction");
    const query = runtime.db.selectFrom("kysely_shapes").select(["amount", "amounts"]).where("_id", "=", "shape");
    assert.deepEqual(await query.execute(), [expectedDecimal]);
    const streamed = [];
    for await (const row of query.stream(1)) streamed.push(row);
    assert.deepEqual(streamed, [expectedDecimal]);
    const spawnWorker = (mode) => {
      const child = fork(
        new URL("./kysely-lock-worker.mjs", import.meta.url),
        [mode],
        { stdio: ["ignore", "inherit", "inherit", "ipc"] },
      );
      workers.add(child);
      // A worker can exit before reporting (including an unsettled top-level
      // await). Surface that outcome immediately instead of masking it as an
      // unrelated 60-second AbortError. Keep the deadline for a live stall.
      const message = new Promise((resolve, reject) => {
        const cleanup = () => {
          clearTimeout(timer);
          child.off("message", reported);
          child.off("exit", stopped);
          child.off("error", failed);
        };
        const reported = (value) => { cleanup(); resolve(value); };
        const failed = (error) => { cleanup(); reject(error); };
        const stopped = (code, signal) => failed(new Error(
          `Kysely ${mode} worker exited before reporting: code=${code} signal=${signal}`,
        ));
        const timer = setTimeout(() => failed(new Error(
          `Kysely ${mode} worker did not report within 60 seconds (pid=${child.pid})`,
        )), 60_000);
        child.once("message", reported);
        child.once("exit", stopped);
        child.once("error", failed);
      });
      const exited = once(child, "exit").then((value) => {
        workers.delete(child);
        return value;
      });
      return { child, message, exited };
    };
    const stoppedEarly = spawnWorker("exit-before-report");
    await assert.rejects(stoppedEarly.message, /exited before reporting: code=17/);
    assert.deepEqual(await stoppedEarly.exited, [17, null]);
    const runners = [spawnWorker("migrate"), spawnWorker("migrate")];
    const outcomes = await Promise.all(runners.map((runner) => runner.message));
    await Promise.all(runners.map((runner) => runner.exited));
    assert.deepEqual(outcomes.map((outcome) => outcome.error), [null, null],
      `Concurrent migration outcomes: ${JSON.stringify(outcomes)}`);
    assert.equal(
      outcomes.reduce((total, outcome) => total + outcome.applied, 0),
      1,
    );
    const crashed = spawnWorker("claim");
    const abandoned = await crashed.message;
    assert.ok(abandoned.claimed?.ownerToken, abandoned.error);
    const liveContender = await runtime.migrateToLatest({
      provider: {
        async getMigrations() {
          throw new Error("must not enter provider while locked");
        },
      },
      lock: { waitTimeoutMs: 0 },
    });
    assert.equal(liveContender.error?.code, "ANTFLY_MIGRATION_LOCK_BUSY");
    crashed.child.kill("SIGKILL");
    await crashed.exited;
    assert.equal(
      (await runtime.inspectMigrationLock()).ownerToken,
      abandoned.claimed.ownerToken,
    );
    assert.equal(
      await runtime.recoverMigrationLock({
        ownerToken: abandoned.claimed.ownerToken,
        previousRunnerStopped: true,
      }),
      true,
    );
    const owner = new AntflyPersistentMigrationLock({ waitTimeoutMs: 0 });
    const contender = new AntflyPersistentMigrationLock({ waitTimeoutMs: 0 });
    await owner.acquire(runtime.db);
    const claim = await runtime.inspectMigrationLock();
    assert.ok(claim?.ownerToken);
    await assert.rejects(contender.acquire(second), {
      code: "ANTFLY_MIGRATION_LOCK_BUSY",
    });
    assert.equal(
      await contender.recover(second, {
        ownerToken: "00000000-0000-0000-0000-000000000000",
        previousRunnerStopped: true,
      }),
      false,
    );
    await owner.release(runtime.db);
    assert.equal(await runtime.inspectMigrationLock(), null);
    const result = await runtime.migrateToLatest({
      migrationTableName: "runtime_migrations",
      migrationLockTableName: "runtime_lock",
      provider: {
        async getMigrations() {
          return {
            "001": {
              async up(tx) {
                await tx.schema
                  .createTable("runtime_created")
                  .addColumn("id", "text")
                  .execute();
              },
            },
          };
        },
      },
    });
    assert.ifError(result.error);
    assert.ok(
      !(await runtime.generateTypes()).includes(
        '"_antfly_kysely_migration_claims"',
      ),
    );
  } finally {
    try { await runtime.close(); }
    finally { for (const [oid, parser] of previousParsers) pg.types.setTypeParser(oid, parser); }
  }
  for (const { migrationTables, table } of [
    { migrationTables: {}, table: "kysely_migrated" },
    {
      migrationTables: {
        migrationTableName: "app_migrations",
        migrationLockTableName: "app_migration_lock",
      },
      table: "kysely_custom_migrated",
    },
  ]) {
    const provider = {
      async getMigrations() {
        return {
          "001_create": {
            async up(tx) {
              await tx.schema
                .createTable(table)
                .addColumn("id", "text", (col) => col.primaryKey())
                .execute();
            },
          },
          "002_index": {
            async up(tx) {
              await tx.schema.createIndex(`${table}_id`).on(table).column("id").execute();
            },
          },
        };
      },
    };
    const runtimes = [0, 1].map(() => createAntflyPostgres({ pg, client, pool: { connectionString, max: 1 } }));
    let results;
    try {
      results = await Promise.all(runtimes.map((runtime) => runtime.migrateToLatest({ provider, ...migrationTables })));
    } finally { await Promise.all(runtimes.map((runtime) => runtime.close())); }
    for (const result of results) assert.ifError(result.error);
    assert.equal(
      results.flatMap((r) => r.results ?? []).filter((r) => r.status === "Success").length,
      2
    );
    assert.equal(
      (
        await db
          .selectFrom(migrationTables.migrationTableName ?? "kysely_migration")
          .selectAll()
          .execute()
      ).length,
      2
    );
  }
  await sql`CREATE DATABASE kysely_tenant`.execute(db);
  const tenantUrl = new URL(connectionString);
  tenantUrl.pathname = "/kysely_tenant";
  const setup = createAntflyPostgres({ pg, client, pool: { connectionString: tenantUrl.toString() } });
  try {
    await setup.db.schema.createSchema("app").execute();
    await setup.db.schema.withSchema("app").createTable("shadow").addColumn("value", "text", (column) => column.notNull()).execute();
    await setup.db.schema.withSchema("public").createTable("shadow").addColumn("value", "double precision", (column) => column.notNull()).execute();
    await setup.db.insertInto("app.shadow").values({ value: "app" }).execute();
    await setup.db.insertInto("public.shadow").values({ value: 7 }).execute();
  } finally { await setup.close(); }
  const tenant = createAntflyPostgres({ pg, client, searchPath: ["app", "public"], pool: { connectionString: tenantUrl.toString(), max: 1 } });
  const tenantPublic = createAntflyPostgres({ pg, client, pool: { connectionString: tenantUrl.toString(), max: 1 } });
  try {
    assert.deepEqual(await tenant.db.selectFrom("shadow").selectAll().execute(), [{ value: "app" }]);
    assert.deepEqual(await tenant.db.selectFrom("public.shadow").selectAll().execute(), [{ value: 7 }]);
    const source = await tenant.generateTypes();
    assert.match(source, /"app.shadow": \{\s+"value": string;/);
    assert.match(source, /"public.shadow": \{\s+"value": number;/);
    assert.match(source, /"shadow": \{\s+"value": string;/);
    assert.ok(!source.includes('"kysely_people"'));
    const provider = { async getMigrations() { return {
      "001": {
        async up(tx) { await tx.schema.withSchema("app").createTable("scoped_migration").addColumn("value", "text").execute(); },
        async down(tx) { await tx.schema.withSchema("app").dropTable("scoped_migration").execute(); },
      },
    }; } };
    const settings = { provider, migrationTableSchema: "history" };
    // A claim in another database must not block this database's migrations.
    const rootLock = new AntflyPersistentMigrationLock({ waitTimeoutMs: 0 });
    await rootLock.acquire(db);
    try { assert.ifError((await tenant.migrateToLatest(settings)).error); }
    finally { await rootLock.release(db); }
    assert.ok((await tenant.db.introspection.getSchemas()).some((schema) => schema.name === "history"));
    assert.ifError((await tenant.migrateDown(settings)).error);
    assert.ifError((await tenant.migrateUp(settings)).error);
    assert.ifError((await tenant.migrateTo("001", settings)).error);
    const shared = new AntflyPersistentMigrationLock({ waitTimeoutMs: 0 });
    await shared.acquire(tenant.db);
    try {
      const blocked = await tenantPublic.migrateToLatest({ ...settings, lock: { waitTimeoutMs: 0 } });
      assert.equal(blocked.error?.code, "ANTFLY_MIGRATION_LOCK_BUSY");
    } finally { await shared.release(tenant.db); }
    const localClaims = createAntflyPostgres({ pg, client, searchPath: ["app", "public"], migrationLockNamespace: "app", pool: { connectionString: tenantUrl.toString(), max: 1 } });
    const appLock = new AntflyPersistentMigrationLock({ namespace: "app" });
    try {
      await appLock.acquire(localClaims.db);
      assert.ok((await localClaims.inspectMigrationLock())?.ownerToken);
      await appLock.release(localClaims.db);
      assert.ifError((await localClaims.migrateToLatest(settings)).error);
      assert.equal(await localClaims.inspectMigrationLock(), null);
    } finally { await localClaims.close(); }
    await assert.rejects(tenant.db.transaction().execute(async (tx) => {
      await tx.schema.createTable("transactional_ddl").addColumn("n", "integer").execute();
    }));
    assert.ok(!(await tenant.db.introspection.getTables()).some((table) => table.name === "transactional_ddl"));
  } finally { await Promise.all([tenant.close(), tenantPublic.close()]); }
  const visible = (await introspector.getTables()).map((table) => table.name);
  assert.ok(visible.includes("kysely_people"));
  assert.ok(!visible.includes("kysely_migration"));
  assert.ok(!visible.includes("kysely_migration_lock"));
  const allTables = (await introspector.getTables({ withInternalKyselyTables: true })).map(
    (table) => table.name
  );
  assert.ok(allTables.includes("kysely_migration"));
  assert.ok(allTables.includes("kysely_migration_lock"));
  console.log(
    "Kysely pgwire: CRUD, streaming, scoped databases/search paths, introspection, migration directions and lock isolation: PASS"
  );
} finally {
  for (const worker of workers) worker.kill("SIGKILL");
  await Promise.all([db.destroy(), second.destroy(), standard.destroy()]);
}
