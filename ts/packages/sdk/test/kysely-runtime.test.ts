// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import pg from "pg";
import Cursor from "pg-cursor";
import { expect, it, vi } from "vitest";
import type { AntflyClient } from "../src/client.js";
import { type AntflyPostgresOptions, createAntflyPostgres } from "../src/kysely.js";

it("accepts pg and pg-cursor without an adapter", () => {
  const check = (client: AntflyClient): AntflyPostgresOptions => ({
    pg,
    cursor: Cursor,
    client,
    pool: { connectionString: "postgres://localhost/default" },
  });
  expect(check).toBeTypeOf("function");
});

it.each([
  "exact",
  "pg-default",
] as const)("binds %s generation and parsers and closes an unused owned pool once", async (values) => {
  const end = vi.fn(async () => {});
  let parsers: AntflyPostgresOptions["pg"]["types"] | undefined;
  class Pool {
    options = {};
    end = end;
    constructor(config: { types: AntflyPostgresOptions["pg"]["types"] }) {
      parsers = config.types;
    }
    async connect(): Promise<never> {
      throw new Error("No SQL needed for metadata");
    }
  }
  const client = {
    tables: {
      list: async () => [{ name: "prices" }],
      get: async () => ({ schema: { document_schemas: {} } }),
    },
    executeSQL: async () => ({
      columns: [{ name: "amounts", type: "array", element_type: "numeric" }],
    }),
  } as unknown as AntflyClient;
  const runtime = createAntflyPostgres({
    pg: { Pool, types: pg.types },
    client,
    pool: { connectionString: "postgres://localhost/default" },
    values,
  });
  expect((parsers?.getTypeParser(20) as (value: string) => unknown)("9007199254740993")).toBe(
    "9007199254740993"
  );
  expect((parsers?.getTypeParser(1231) as (value: string) => unknown)("{1.25}")).toEqual(
    values === "exact" ? ["1.25"] : [1.25]
  );
  expect(await runtime.generateTypes()).toContain(
    `SQLArray<${values === "exact" ? "string" : "number"}>`
  );
  await Promise.all([runtime.close(), runtime.close()]);
  expect(end).toHaveBeenCalledTimes(1);
  expect(() => runtime.generateTypes()).toThrow("closing");
});

it("waits for active metadata work before closing its pool", async () => {
  let resolve!: (value: never[]) => void;
  const metadata = new Promise<never[]>((done) => {
    resolve = done;
  });
  const end = vi.fn(async () => {});
  class Pool {
    options = {};
    end = end;
    async connect(): Promise<never> {
      throw new Error("unused");
    }
  }
  const client = { tables: { list: () => metadata } } as unknown as AntflyClient;
  const runtime = createAntflyPostgres({
    pg: { Pool, types: pg.types },
    client,
    pool: { connectionString: "postgres://localhost/default" },
  });
  const generating = runtime.generateTypes();
  const closing = runtime.close();
  await Promise.resolve();
  expect(end).not.toHaveBeenCalled();
  resolve([]);
  await Promise.all([generating, closing]);
  expect(end).toHaveBeenCalledTimes(1);
});

it.each([
  "postgres://localhost/other",
  "postgres://localhost/default?options=-csearch_path%3Dother",
  "postgres://localhost/default?database=other",
])("rejects unsupported catalog selection: %s", (connectionString) => {
  expect(() =>
    createAntflyPostgres({ pg, client: {} as AntflyClient, pool: { connectionString } })
  ).toThrow("default database");
});
