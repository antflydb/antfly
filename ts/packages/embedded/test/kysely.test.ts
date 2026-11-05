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
import {
  antflySearch,
  generateKyselyTypes,
  prepareKyselyMigrationTables,
} from "@antfly/sdk/kysely";
import { Kysely } from "kysely";
import { Migrator } from "kysely/migration";
import { expect, it } from "vitest";
import { createWithOptions } from "../src/database.js";
import { AntflyDialect } from "../src/kysely.js";
import { describeWithLibrary } from "./helpers.js";

describeWithLibrary("Kysely search composition", () => {
  it("preserves required nullable unions when generating schema types", async () => {
    const directory = await mkdtemp(join(tmpdir(), "antfly-kysely-nullable-"));
    const database = await createWithOptions(join(directory, "db.aflite"), { noSync: true });
    const db = new Kysely<{
      nullable_fields: { body: string | null; note: string | null; title: string };
    }>({ dialect: new AntflyDialect({ database }) });
    try {
      await database.createTable("nullable_fields", {
        default_type: "row",
        document_schemas: {
          row: {
            schema: {
              type: "object",
              required: ["body", "note", "title"],
              properties: {
                body: { type: ["string", "null"] },
                note: { type: "string", nullable: true },
                title: { type: "string" },
                optional: { type: "string" },
              },
            },
          },
        },
      });
      const table = await database.openTable("nullable_fields");
      try {
        await table.batchJson({
          inserts: { a: { body: null, note: null, title: "present" } },
          sync_level: "write",
        });
      } finally {
        await table.close();
      }
      expect(
        await db.selectFrom("nullable_fields").select(["body", "note", "title"]).execute()
      ).toEqual([{ body: null, note: null, title: "present" }]);
      const tables = await db.introspection.getTables();
      expect(tables[0]?.columns.find((column) => column.name === "body")?.isNullable).toBe(true);
      expect(tables[0]?.columns.find((column) => column.name === "note")?.isNullable).toBe(true);
      expect(tables[0]?.columns.find((column) => column.name === "title")?.isNullable).toBe(false);
      expect(tables[0]?.columns.find((column) => column.name === "optional")?.isNullable).toBe(
        true
      );
      const source = generateKyselyTypes(tables, { integerMode: "bigint" });
      expect(source).toContain('"body": string | null;');
      expect(source).toContain('"note": string | null;');
      expect(source).toContain('"title": string;');
    } finally {
      await db.destroy();
      await database.close();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("joins ranked hits with SQL, bounds candidates, and streams typed results", async () => {
    const directory = await mkdtemp(join(tmpdir(), "antfly-kysely-search-"));
    const database = await createWithOptions(join(directory, "db.aflite"), { noSync: true });
    const db = new Kysely<{ threads: { id: string; published: boolean } }>({
      dialect: new AntflyDialect({ database }),
    });
    try {
      const fixture = JSON.parse(
        await readFile(
          new URL(
            "../../../../zig/pkg/antfly-embedded/capi-conformance/sql/search-fixture.json",
            import.meta.url
          ),
          "utf8"
        )
      );
      await database.createTable("history_items", fixture.history);
      const history = await database.openTable("history_items");
      try {
        await history.batchJson({
          inserts: {
            h1: { thread_id: "t1", body: "alpha memory" },
            h2: { thread_id: "t2", body: "alpha history" },
          },
          sync_level: "write",
        });
        await history.runUntilIdle();
      } finally {
        await history.close();
      }
      await db.schema
        .createTable("threads")
        .addColumn("id", "text", (col) => col.primaryKey())
        .addColumn("published", "boolean", (col) => col.notNull())
        .execute();
      await db
        .insertInto("threads")
        .values([
          { id: "t1", published: true },
          { id: "t2", published: false },
        ])
        .execute();
      const request = { full_text_search: { query: "body:alpha" } };
      const query = db
        .selectFrom(
          antflySearch<{ thread_id: string }>("history_items", request, {
            candidateLimit: 50,
          }).as("hits")
        )
        .innerJoin("threads", "threads.id", "hits.thread_id")
        .select(["hits._id", "hits.score", "threads.id"])
        .where("threads.published", "=", true)
        .orderBy("hits.score", "desc")
        .orderBy("hits._id")
        .limit(10);
      const rows = await query.execute();
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ _id: "h1", id: "t1" });
      expect(rows[0]?.score).toBeGreaterThan(0);
      const matched = await db
        .selectFrom(
          antflySearch("history_items", {
            full_text_search: { match: "alpha", field: "body" },
          }).as("hits")
        )
        .select("hits._id")
        .orderBy("hits._id")
        .execute();
      expect(matched).toEqual([{ _id: "h1" }, { _id: "h2" }]);
      const streamed = [];
      for await (const row of query.stream(1)) streamed.push(row);
      expect(streamed).toEqual(rows);

      // A post-search SQL filter cannot refill a bounded candidate set.
      const first = await db
        .selectFrom(antflySearch("history_items", request, { candidateLimit: 1 }).as("hits"))
        .select("hits._id")
        .executeTakeFirstOrThrow();
      const filtered = await db
        .selectFrom(antflySearch("history_items", request, { candidateLimit: 1 }).as("hits"))
        .select("hits._id")
        .where("hits._id", "!=", first._id)
        .limit(10)
        .execute();
      expect(filtered).toEqual([]);

      const source = generateKyselyTypes(await db.introspection.getTables(), {
        integerMode: "bigint",
      });
      expect(source).toContain('"published": boolean;');
      expect(source).toContain('"thread_id": string | null;');
    } finally {
      await db.destroy();
      await database.close();
      await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);

  it("serializes migrators sharing one database and reports default column types", async () => {
    const directory = await mkdtemp(join(tmpdir(), "antfly-kysely-migrate-"));
    const database = await createWithOptions(join(directory, "db.aflite"), { noSync: true });
    const makeDb = () => new Kysely({ dialect: new AntflyDialect({ database }) });
    const first = makeDb();
    const second = makeDb();
    let applied = 0;
    const provider = {
      async getMigrations() {
        return {
          "001_create": {
            async up(db: Kysely<unknown>) {
              await db.schema
                .createTable("defaults")
                .addColumn("n", "bigint", (col) => col.notNull().defaultTo(1))
                .execute();
              applied++;
            },
          },
        };
      },
    };
    try {
      const results = await Promise.all(
        [first, second].map(async (db) => {
          await prepareKyselyMigrationTables(db);
          return new Migrator({ db, provider }).migrateToLatest();
        })
      );
      for (const result of results) expect(result.error).toBeUndefined();
      expect(applied).toBe(1);
      const source = generateKyselyTypes(await first.introspection.getTables(), {
        integerMode: "bigint",
      });
      expect(source).toContain('"n": Generated<bigint>;');
      expect(source).not.toContain('"kysely_migration"');
    } finally {
      await Promise.all([first.destroy(), second.destroy()]);
      await database.close();
      await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);
});
