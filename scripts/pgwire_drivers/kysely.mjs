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
import assert from "node:assert/strict";
import { Kysely, PostgresDialect, sql } from "kysely";
import { Migrator } from "kysely/migration";
import pg from "pg";
import Cursor from "pg-cursor";
import { AntflyClient } from "../../ts/packages/sdk/dist/index.js";
import {
  AntflyPostgresDialect,
  antflySearch,
  createAntflyIntrospector,
  generateKyselyTypes,
  prepareKyselyMigrationTables,
} from "../../ts/packages/sdk/dist/kysely.js";

const connectionString = process.env.ANTFLY_PGWIRE_URL;
const url = new URL(connectionString);
const client = new AntflyClient({
  baseUrl: process.env.ANTFLY_HTTP_URL,
  auth: { username: decodeURIComponent(url.username), password: decodeURIComponent(url.password) },
});
const introspector = createAntflyIntrospector(client);
// A deployment lock must be supplied for migrations. This single-process test
// deliberately queues all its runners in memory; it is not a distributed lock.
let tail = Promise.resolve();
const migrationLock = () => {
  let release;
  return {
    async acquire() {
      const prior = tail;
      tail = new Promise((resolve) => {
        release = resolve;
      });
      await prior;
    },
    async release() {
      release();
    },
  };
};
const makeDatabase = () =>
  new Kysely({
    dialect: new AntflyPostgresDialect({
      pool: new pg.Pool({ connectionString }),
      cursor: Cursor,
      introspector,
      migrationLock: migrationLock(),
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
  const metadata = await introspector.getTables({ withInternalKyselyTables: true });
  assert.ok(metadata.find((table) => table.name === "kysely_people"));
  assert.match(generateKyselyTypes(metadata, { integerMode: "string" }), /"n": string \| null;/);
  const generated = generateKyselyTypes(metadata, { integerMode: "string" });
  assert.match(generated, /"body": string \| null;/);
  assert.match(generated, /"matrix": SQLArray<number>;/);
  assert.match(generated, /"optional_nonnull": string \| null;/);

  // Server search support is an explicit capability boundary, never an
  // application-side search plus join fallback.
  await assert.rejects(
    db
      .selectFrom(antflySearch("kysely_people", "name:Ada").as("hits"))
      .select("hits._id")
      .execute(),
    (error) => error.code === "0A000"
  );

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
    const results = await Promise.all(
      [db, second].map(async (database) => {
        await prepareKyselyMigrationTables(database, migrationTables);
        return new Migrator({ db: database, provider, ...migrationTables }).migrateToLatest();
      })
    );
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
    "Kysely pgwire: CRUD, joins, exact integers, transactions, streaming, introspection, migrations: PASS"
  );
} finally {
  await Promise.all([db.destroy(), second.destroy(), standard.destroy()]);
}
