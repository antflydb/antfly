// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { fileURLToPath } from "node:url";
import { DummyDriver, Kysely, PostgresDialect } from "kysely";
import pg from "pg";
import ts from "typescript";
import { describe, expect, expectTypeOf, it, vi } from "vitest";
import type { AntflyClient } from "../src/client.js";
import {
  type AntflyColumnMetadata,
  AntflyPostgresDialect,
  antflySearch,
  createAntflyIntrospector,
  createAntflyPostgresTypes,
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
  it.each([
    "number",
    "string",
  ] as const)("generates valid TypeScript with %s NUMERIC arrays", (numericArrayMode) => {
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
            column("matrix", "int32[]", { hasDefaultValue: true }),
            column("exact", "int64[]", { hasDefaultValue: true }),
            column("amounts", "numeric[]", { hasDefaultValue: true }),
            column("amount", "numeric", { hasDefaultValue: true }),
            column('quote"; injected: never; //', "unknown", { isNullable: true }),
          ],
        },
      ],
      { integerMode: "bigint", numericArrayMode }
    );
    const fileName = fileURLToPath(new URL("./generated-kysely.ts", import.meta.url));
    const input = `${source}
import type { Insertable, Updateable } from "kysely";
export const valid: Insertable<Database["items"]> = { id: "1" };
export const update: Updateable<Database["items"]> = { quantity: 2n };
export const vector: Updateable<Database["items"]> = { matrix: [1, null, 2] };
export const matrix: Updateable<Database["items"]> = { matrix: [[1, null], [2, 3]] };
export const cube: Updateable<Database["items"]> = { matrix: [[[1]], [[2]]] };
export const empty: Updateable<Database["items"]> = { matrix: [] };
export const exact: Updateable<Database["items"]> = { exact: [[9007199254740993n, null]] };
export const amounts: Updateable<Database["items"]> = { amounts: [[${numericArrayMode === "string" ? '"9007199254740993.1200"' : "1.25"}, null]] };
export const amount: Updateable<Database["items"]> = { amount: "9007199254740993.1200" };
// @ts-expect-error Array types must match the selected NUMERIC parser mode.
export const wrongNumeric: Updateable<Database["items"]> = { amounts: [[${numericArrayMode === "string" ? "1.25" : '"1.25"'}]] };
// @ts-expect-error Array elements must retain their scalar type at every rank.
export const wrongElement: Updateable<Database["items"]> = { matrix: [["x"]] };
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
        list: vi
          .fn()
          .mockResolvedValue([
            { name: 'items"quoted' },
            { name: "kysely_people" },
            { name: "kysely_migration" },
            { name: "kysely_migration_lock" },
          ]),
        get: vi.fn().mockResolvedValue({
          schema: {
            default_type: "row",
            document_schemas: {
              row: {
                schema: {
                  required: ["id", "n", "nullable_union"],
                  properties: {
                    n: { nullable: false },
                    nullable_union: { type: ["string", "null"] },
                    optional_nonnull: { type: "string", nullable: false },
                  },
                },
              },
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
          { name: "nullable_union", type: "string" },
          { name: "optional_nonnull", type: "string" },
        ],
      }),
    };
    const introspector = createAntflyIntrospector(client as unknown as AntflyClient);
    expect(await introspector.getSchemas()).toEqual([{ name: "public" }]);
    const tables = await introspector.getTables();
    expect(tables.map((table) => table.name)).toEqual(['items"quoted', "kysely_people"]);
    expect(client.executeSQL).toHaveBeenCalledWith({
      statement: 'SELECT * FROM "items""quoted" LIMIT 0',
    });
    expect(tables[0]?.columns[1]).toMatchObject({
      dataType: "int64",
      isNullable: false,
      hasDefaultValue: true,
    });
    expect(tables[0]?.columns[2]).toMatchObject({ isGeneratedAlways: true });
    expect(tables[0]?.columns[4]).toMatchObject({ isNullable: true });
    expect(tables[0]?.columns[5]).toMatchObject({ isNullable: true });
    expect(await introspector.getTables({ withInternalKyselyTables: true })).toHaveLength(4);
    const source = generateKyselyTypes(tables, { integerMode: "string" });
    expect(source).toContain('"n": Generated<string>;');
    expect(source).toContain('"computed": GeneratedAlways<string | null>;');
    expect(source).toContain('"values": SQLArray<number> | null;');
    expect(source).toContain('"nullable_union": string | null;');
    expect(source).toContain('"optional_nonnull": string | null;');
    expect(generateKyselyTypes(tables, { integerMode: "bigint" })).toContain(
      '"n": Generated<bigint>;'
    );
  });
});

describe("PostgreSQL NUMERIC parsing", () => {
  it("preserves decimal spelling, nested arrays, SQL NULLs and special values locally", () => {
    const defaultNumericArray = pg.types.getTypeParser(1231);
    const types = createAntflyPostgresTypes(pg.types);
    const parse = types.getTypeParser(1231) as (value: string) => unknown;
    expect(parse("{{9007199254740993.1200,NULL,1.2300},{-0.000001,NaN,Infinity}}")).toEqual([
      ["9007199254740993.1200", null, "1.2300"],
      ["-0.000001", "NaN", "Infinity"],
    ]);
    expect(parse("{}")).toEqual([]);
    expect(parse("[0:1]={1.2300,-Infinity}")).toEqual(["1.2300", "-Infinity"]);
    expect((types.getTypeParser(1700) as (value: string) => unknown)("9007199254740993.1200")).toBe(
      "9007199254740993.1200"
    );
    expect(types.getTypeParser(23)).toBe(pg.types.getTypeParser(23));
    expect(types.getTypeParser(1231, "binary")).toBe(pg.types.getTypeParser(1231, "binary"));
    expect(pg.types.getTypeParser(1231)).toBe(defaultNumericArray);
    expect(defaultNumericArray("{1.2300}")).toEqual([1.23]);
    const tables = [
      {
        name: "items",
        isView: false,
        isForeign: false,
        columns: [
          {
            name: "amounts",
            dataType: "numeric[]",
            isNullable: true,
            isAutoIncrementing: false,
            hasDefaultValue: false,
          },
        ],
      },
    ];
    expect(generateKyselyTypes(tables, { integerMode: "string" })).toContain(
      '"amounts": SQLArray<number> | null;'
    );
    expect(
      generateKyselyTypes(tables, { integerMode: "string", numericArrayMode: "string" })
    ).toContain('"amounts": SQLArray<string> | null;');
  });
});
