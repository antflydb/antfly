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

import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Kysely } from "kysely";
import { expect, it } from "vitest";
import { createWithOptions } from "../src/database.js";
import { AntflyDialect } from "../src/kysely.js";
import { Connection } from "../src/sql.js";
import { describeWithLibrary } from "./helpers.js";

describeWithLibrary("SQL", () => {
  it("aborts execute transactions after syntax errors", async () => {
    const directory = await mkdtemp(join(tmpdir(), "antfly-syntax-"));
    const db = await createWithOptions(join(directory, "db.aflite"), { noSync: true });
    const connection = await Connection.open(db);
    try {
      await connection.execute("CREATE TABLE numbers (n BIGINT)");
      await connection.execute("BEGIN");
      await connection.execute("INSERT INTO numbers (_id,n) VALUES ('discarded',1)");
      await expect(connection.execute("INSERT INTO")).rejects.toMatchObject({ sqlstate: "42601" });
      await expect(connection.execute("COMMIT")).rejects.toMatchObject({ sqlstate: "25P02" });
      await connection.execute("ROLLBACK");
      expect((await connection.query("SELECT n FROM numbers")).rows).toEqual([]);
    } finally {
      await connection.close();
      await db.close();
      await rm(directory, { recursive: true, force: true });
    }
  });
  it("runs the shared type and SQLSTATE cases", async () => {
    const directory = await mkdtemp(join(tmpdir(), "antfly-sql-"));
    const db = await createWithOptions(join(directory, "db.aflite"), { noSync: true });
    const fixture = JSON.parse(
      await readFile(
        new URL(
          "../../../../zig/pkg/antfly-embedded/capi-conformance/sql/search-fixture.json",
          import.meta.url
        ),
        "utf8"
      )
    );
    await db.createTable(fixture.table, fixture.schema);
    await db.createTable("history_items", fixture.history);
    const table = await db.openTable(fixture.table);
    try {
      for (const index of fixture.indexes) await table.addIndex(index);
      await table.batchJson(fixture.batch);
      await table.runUntilIdle();
    } finally {
      await table.close();
    }
    const connection = await Connection.open(db);
    try {
      const raw = await readFile(
        new URL(
          "../../../../zig/pkg/antfly-embedded/capi-conformance/sql/cases.json",
          import.meta.url
        ),
        "utf8"
      );
      const cases = JSON.parse(raw.replace("9007199254740993,", '"9007199254740993",')) as {
        statement: string;
        parameters?: unknown[];
        rows?: unknown[][];
        sqlstate?: string;
      }[];
      for (const c of cases) {
        if (c.sqlstate) {
          await expect(connection.query(c.statement, c.parameters)).rejects.toMatchObject({
            sqlstate: c.sqlstate,
          });
          continue;
        }
        const r = await connection.query(c.statement, c.parameters);
        if (c.rows)
          expect(
            r.rows.map((row) =>
              Object.values(row).map((v) => (typeof v === "bigint" ? String(v) : v))
            )
          ).toEqual(c.rows);
      }
    } finally {
      await connection.close();
      await db.close();
      await rm(directory, { recursive: true, force: true });
    }
  });
  it("uses Kysely transactions and streams beyond one page", async () => {
    const directory = await mkdtemp(join(tmpdir(), "antfly-kysely-"));
    const db = await createWithOptions(join(directory, "db.aflite"), { noSync: true });
    const sql = new Kysely<{ numbers: { n: bigint } }>({
      dialect: new AntflyDialect({ database: db }),
    });
    try {
      await sql.schema.createTable("numbers").addColumn("n", "bigint").execute();
      await sql.schema
        .createTable("defaults")
        .addColumn("n", "bigint", (column) => column.notNull().defaultTo(1))
        .execute();
      const metadata = await sql.introspection.getTables();
      const defaults = metadata.find((table) => table.name === "defaults");
      expect(defaults?.columns.find((column) => column.name === "n")).toMatchObject({
        isNullable: false,
        hasDefaultValue: true,
      });
      await sql.transaction().execute(async (tx) => {
        for (let i = 0; i < 300; i++)
          await tx
            .insertInto("numbers")
            .values({ n: BigInt(i) })
            .execute();
      });
      expect((await sql.selectFrom("numbers").selectAll().execute()).length).toBe(300);
      let count = 0;
      for await (const row of sql.selectFrom("numbers").selectAll().stream(17)) {
        expect(typeof row.n).toBe("bigint");
        count++;
      }
      expect(count).toBe(300);
      await expect(
        sql.transaction().execute(async (tx) => {
          await tx.insertInto("numbers").values({ n: 9000n }).execute();
          throw new Error("rollback");
        })
      ).rejects.toThrow("rollback");
      expect((await sql.selectFrom("numbers").selectAll().execute()).length).toBe(300);
    } finally {
      await sql.destroy();
      await db.close();
      await rm(directory, { recursive: true, force: true });
    }
  });
});
