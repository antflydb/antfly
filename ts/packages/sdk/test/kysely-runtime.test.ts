// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import { EventEmitter } from "node:events";
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
  class Pool extends EventEmitter {
    options = {};
    end = end;
    constructor(config: { types: AntflyPostgresOptions["pg"]["types"] }) {
      super();
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
  class Pool extends EventEmitter {
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
  "postgres://localhost/default?options=-csearch_path%3Dother",
  "postgres://localhost/default?database=other",
])("rejects unsupported catalog selection: %s", (connectionString) => {
  expect(() =>
    createAntflyPostgres({ pg, client: {} as AntflyClient, pool: { connectionString } })
  ).toThrow("connection URL path");
});

it.each([
  false,
  true,
])("handles idle-pool errors, including shutdown (custom handler: %s)", async (custom) => {
  let pool!: Pool;
  const failure = new Error("idle connection lost");
  const handler = vi.fn();
  const warning = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
  class Pool extends EventEmitter {
    options = {};
    constructor() {
      super();
      pool = this;
    }
    async connect(): Promise<never> {
      throw new Error("unused");
    }
    async end() {
      this.emit("error", failure);
    }
  }
  const runtime = createAntflyPostgres({
    pg: { Pool, types: pg.types },
    client: {} as AntflyClient,
    pool: { connectionString: "postgres://localhost/default" },
    ...(custom ? { onPoolError: handler } : {}),
  });
  try {
    expect(() => pool.emit("error", failure)).not.toThrow();
    await runtime.close();
    expect(custom ? handler : warning).toHaveBeenCalledTimes(2);
    expect(custom ? handler : warning).toHaveBeenCalledWith(failure);
    expect(custom ? warning : handler).not.toHaveBeenCalled();
  } finally {
    warning.mockRestore();
  }
});

it.each([
  false,
  true,
])("reports failed pool-error handlers without unhandled exceptions (async: %s)", async (async) => {
  let pool!: Pool;
  const original = new Error("idle connection lost");
  const failure = new Error("telemetry unavailable");
  const warning = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
  class Pool extends EventEmitter {
    options = {};
    constructor() {
      super();
      pool = this;
    }
    async connect(): Promise<never> {
      throw new Error("unused");
    }
    async end() {}
  }
  const runtime = createAntflyPostgres({
    pg: { Pool, types: pg.types },
    client: {} as AntflyClient,
    pool: { connectionString: "postgres://localhost/default" },
    onPoolError: () => {
      if (async) return Promise.reject(failure);
      throw failure;
    },
  });
  try {
    expect(() => pool.emit("error", original)).not.toThrow();
    await Promise.resolve();
    expect(warning).toHaveBeenCalledWith(original);
    expect(warning).toHaveBeenCalledWith(failure);
    await runtime.close();
  } finally {
    warning.mockRestore();
  }
});

it("releases a checked-out client if namespace initialization fails", async () => {
  const release = vi.fn();
  const query = vi.fn(async () => {
    throw new Error("namespace permission denied");
  });
  class Pool extends EventEmitter {
    options = {};
    async connect() {
      return { query, release };
    }
    async end() {}
  }
  const runtime = createAntflyPostgres({
    pg: { Pool, types: pg.types },
    client: {} as AntflyClient,
    pool: { connectionString: "postgres://localhost/default" },
  });
  try {
    await expect(
      runtime.db.selectNoFrom((expression) => expression.val(1).as("n")).execute()
    ).rejects.toThrow("namespace permission denied");
    expect(release).toHaveBeenCalledTimes(1);
    expect(query).toHaveBeenCalledWith('SET search_path TO "public"', []);
  } finally {
    await runtime.close();
  }
});

it("pins an ordered namespace path on every checkout in a named database", async () => {
  const statements: string[] = [];
  let connectionString = "";
  const query = vi.fn(async (statement: string) => {
    statements.push(statement);
    return { rows: [], command: "SELECT", rowCount: 0 };
  });
  const release = vi.fn();
  class Pool extends EventEmitter {
    options = {};
    constructor(options: { connectionString: string }) {
      super();
      connectionString = options.connectionString;
    }
    async connect() {
      return { query, release };
    }
    async end() {}
  }
  const runtime = createAntflyPostgres({
    pg: { Pool, types: pg.types },
    client: { api: {} } as AntflyClient,
    searchPath: ["app", "public"],
    pool: { connectionString: "postgres://localhost/tenant" },
  });
  try {
    await runtime.db.selectNoFrom((expression) => expression.val(1).as("n")).execute();
    await runtime.db.selectNoFrom((expression) => expression.val(2).as("n")).execute();
    expect(new URL(connectionString).pathname).toBe("/tenant");
    expect(statements.filter((statement) => statement.startsWith("SET search_path"))).toEqual([
      'SET search_path TO "app", "public"',
      'SET search_path TO "app", "public"',
    ]);
    expect(release).toHaveBeenCalledTimes(2);
  } finally {
    await runtime.close();
  }
});

it.each([
  "exact",
  "pg-default",
] as const)("keeps custom JSONB[] results and generated %s types aligned", async (values) => {
  let types!: AntflyPostgresOptions["pg"]["types"];
  const decode = vi.fn((value: string) => `custom:${value}`);
  const registry = {
    getTypeParser: (oid: number, format?: "text" | "binary") =>
      oid === 3807 ? decode : pg.types.getTypeParser(oid, format),
  };
  class Pool extends EventEmitter {
    options = {};
    constructor(config: { types: AntflyPostgresOptions["pg"]["types"] }) {
      super();
      types = config.types;
    }
    async connect(): Promise<never> {
      throw new Error("No SQL needed for metadata");
    }
    async end() {}
  }
  const client = {
    tables: {
      list: async () => [{ name: "events" }],
      get: async () => ({ schema: { document_schemas: {} } }),
    },
    executeSQL: async () => ({
      columns: [{ name: "payloads", type: "array", element_type: "jsonb" }],
    }),
  } as unknown as AntflyClient;
  const runtime = createAntflyPostgres({
    pg: { Pool, types: registry },
    client,
    pool: { connectionString: "postgres://localhost/default" },
    values,
  });
  try {
    expect(types.getTypeParser(3807, "text")).toBe(decode);
    expect((types.getTypeParser(3807, "text") as typeof decode)('{"null",NULL}')).toBe(
      'custom:{"null",NULL}'
    );
    const source = await runtime.generateTypes();
    expect(source).toContain('"payloads": unknown;');
    expect(source).not.toContain("SQLArray");
  } finally {
    await runtime.close();
  }
});
