// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { fileURLToPath } from "node:url";
import { DummyDriver, Kysely, PostgresDialect } from "kysely";
import ts from "typescript";
import { describe, expect, expectTypeOf, it, vi } from "vitest";
import type { AntflyClient } from "../src/client.js";
import {
  type AntflyColumnMetadata,
  AntflyPostgresDialect,
  antflySearch,
  createAntflyIntrospector,
  generateKyselyTypes,
} from "../src/kysely.js";

function compiler() {
  const dialect = new PostgresDialect({
    pool: async () => {
      throw new Error("No network");
    },
  });
  return new Kysely<{ threads: { id: string; published: boolean } }>({
    dialect: {
      createDriver: () => new DummyDriver(),
      createAdapter: () => dialect.createAdapter(),
      createIntrospector: (db) => dialect.createIntrospector(db),
      createQueryCompiler: () => dialect.createQueryCompiler(),
    },
  });
}

describe("antflySearch", () => {
  it("composes typed search with joins, filters, ordering and bound parameters", () => {
    const db = compiler();
    const request = { full_text_search: { match: "'); DROP TABLE threads; --", field: "body" } };
    const query = db
      .selectFrom(
        antflySearch<{ thread_id: string }>("history", request, {
          candidateLimit: 100,
        }).as("hits")
      )
      .innerJoin("threads", "threads.id", "hits.thread_id")
      .select(["hits._id", "hits.score", "threads.id"])
      .where("threads.published", "=", true)
      .orderBy("hits.score", "desc")
      .orderBy("hits._id")
      .limit(10);
    expectTypeOf<Awaited<ReturnType<typeof query.execute>>>().toEqualTypeOf<
      {
        _id: string;
        score: number;
        id: string;
      }[]
    >();
    const compiled = query.compile();
    expect(compiled.sql).toBe(
      'select "hits"."_id", "hits"."score", "threads"."id" from antfly_search(\'history\', $1, $2) as "hits" inner join "threads" on "threads"."id" = "hits"."thread_id" where "threads"."published" = $3 order by "hits"."score" desc, "hits"."_id" limit $4'
    );
    expect(compiled.parameters).toEqual([JSON.stringify(request), 100, true, 10]);
  });

  it("escapes table literals and binds query strings without changing them", () => {
    const query = compiler()
      .selectFrom(antflySearch("x'); SELECT 1; --", "body:alpha").as("s"))
      .select("s._id")
      .compile();
    expect(query.sql).toContain("antfly_search('x''); SELECT 1; --', $1)");
    expect(query.parameters).toEqual(["body:alpha"]);
  });

  it.each([
    0,
    -1,
    1.5,
    Number.NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
  ])("rejects invalid candidate limit %s", (candidateLimit) => {
    expect(() => antflySearch("items", "alpha", { candidateLimit })).toThrow(RangeError);
  });

  it.each(["", "a\0b", "a\nb", "a\\b"])("rejects ambiguous table literal %j", (name) => {
    expect(() => antflySearch(name, "alpha")).toThrow(TypeError);
  });
});

describe("server capabilities", () => {
  it("reuses pg while rejecting implicit catalogs and migration locks", async () => {
    const pool = vi.fn();
    const dialect = new AntflyPostgresDialect({ pool });
    expect(dialect.createAdapter().supportsTransactionalDdl).toBe(false);
    const db = new Kysely({ dialect });
    await expect(db.introspection.getTables()).rejects.toThrow("no PostgreSQL system catalogs");
    await expect(
      dialect.createAdapter().acquireMigrationLock(db, { lockTable: "lock", lockRowId: "1" })
    ).rejects.toThrow("require a migrationLock");
    expect(pool).not.toHaveBeenCalled();
    await db.destroy();
  });

  it("delegates lock acquisition/release to the configured deployment lock", async () => {
    const lock = {
      acquire: vi.fn().mockResolvedValue(undefined),
      release: vi.fn().mockResolvedValue(undefined),
    };
    const dialect = new AntflyPostgresDialect({ pool: vi.fn(), migrationLock: lock });
    const db = new Kysely({ dialect });
    const options = { lockTable: "lock", lockRowId: "1" };
    const adapter = dialect.createAdapter();
    await adapter.acquireMigrationLock(db, options);
    await adapter.releaseMigrationLock(db, options);
    expect(lock.acquire).toHaveBeenCalledWith(db, options);
    expect(lock.release).toHaveBeenCalledWith(db, options);
    await db.destroy();
  });
});

describe("schema tooling", () => {
  it("generates valid TypeScript with required, defaulted and output-only columns", () => {
    const column = (
      name: string,
      dataType: string,
      extra: Partial<AntflyColumnMetadata> = {}
    ): AntflyColumnMetadata => ({
      name,
      dataType,
      isNullable: false,
      isAutoIncrementing: false,
      hasDefaultValue: false,
      ...extra,
    });
    const source = generateKyselyTypes(
      [
        {
          name: "items",
          schema: "public",
          isView: false,
          isForeign: false,
          columns: [
            column("id", "string"),
            column("quantity", "integer", { hasDefaultValue: true }),
            column("computed", "string", { isGeneratedAlways: true }),
            column('quote"; injected: never; //', "unknown", { isNullable: true }),
          ],
        },
      ],
      { integerMode: "bigint" }
    );
    const fileName = fileURLToPath(new URL("./generated-kysely.ts", import.meta.url));
    const input = `${source}
import type { Insertable, Updateable } from "kysely";
export const valid: Insertable<Database["items"]> = { id: "1" };
export const update: Updateable<Database["items"]> = { quantity: 2n };
// @ts-expect-error A required column cannot be omitted.
export const missing: Insertable<Database["items"]> = {};
// @ts-expect-error A generated column cannot be inserted.
export const invalid: Insertable<Database["items"]> = { id: "1", computed: "x" };
`;
    const options: ts.CompilerOptions = {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.NodeNext,
      strict: true,
      noEmit: true,
      noUnusedLocals: true,
      skipLibCheck: true,
      types: [],
    };
    const host = ts.createCompilerHost(options);
    const getSourceFile = host.getSourceFile.bind(host);
    host.getSourceFile = (name, languageVersion, onError, shouldCreate) =>
      name === fileName
        ? ts.createSourceFile(name, input, languageVersion)
        : getSourceFile(name, languageVersion, onError, shouldCreate);
    const diagnostics = ts.getPreEmitDiagnostics(ts.createProgram([fileName], options, host));
    expect(
      diagnostics.map((diagnostic) => ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n"))
    ).toEqual([]);
  });

  it("uses Antfly metadata and bounded descriptions, including internal migration tables on request", async () => {
    const client = {
      tables: {
        list: vi.fn().mockResolvedValue([{ name: 'items"quoted' }, { name: "kysely_migration" }]),
        get: vi.fn().mockResolvedValue({
          schema: {
            default_type: "row",
            document_schemas: {
              row: { schema: { required: ["id"], properties: { n: { nullable: false } } } },
            },
            column_defaults: [{ column: "n" }],
            generated_columns: [{ column: "computed" }],
          },
        }),
      },
      executeSQL: vi.fn().mockResolvedValue({
        columns: [
          { name: "id", type: "string" },
          { name: "n", type: "integer", element_type: "int64" },
          { name: "computed", type: "integer" },
          { name: "values", type: "array", element_type: "int32" },
        ],
      }),
    };
    const introspector = createAntflyIntrospector(client as unknown as AntflyClient);
    expect(await introspector.getSchemas()).toEqual([{ name: "public" }]);
    const tables = await introspector.getTables();
    expect(tables).toHaveLength(1);
    expect(client.executeSQL).toHaveBeenCalledWith({
      statement: 'SELECT * FROM "items""quoted" LIMIT 0',
    });
    expect(tables[0]?.columns[1]).toMatchObject({
      dataType: "int64",
      isNullable: false,
      hasDefaultValue: true,
    });
    expect(tables[0]?.columns[2]).toMatchObject({ isGeneratedAlways: true });
    expect(await introspector.getTables({ withInternalKyselyTables: true })).toHaveLength(2);
    const source = generateKyselyTypes(tables, { integerMode: "string" });
    expect(source).toContain('"n": Generated<string>;');
    expect(source).toContain('"computed": GeneratedAlways<string | null>;');
    expect(source).toContain('"values": Array<number | null> | null;');
    expect(generateKyselyTypes(tables, { integerMode: "bigint" })).toContain(
      '"n": Generated<bigint>;'
    );
  });
});
