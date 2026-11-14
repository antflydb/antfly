// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { type DatabaseIntrospector, Kysely, PostgresDialect } from "kysely";
import { expect, it, vi } from "vitest";
import { createMigrationBootstrap } from "../src/kysely-migrations.js";

function fixture() {
  const query = vi.fn(async (_sql: string, _parameters: readonly unknown[]) => ({
    rows: [],
    rowCount: 0,
    command: "SELECT",
  }));
  const db = new Kysely<unknown>({
    dialect: new PostgresDialect({
      pool: {
        connect: async () => ({ query, release() {} }),
        end: async () => {},
      },
    }),
  });
  const full: DatabaseIntrospector = {
    getSchemas: vi.fn(async () => [{ name: "app" }]),
    getTables: vi.fn(async () => [
      { name: "application", schema: "app", isView: false, isForeign: false, columns: [] },
    ]),
  };
  return { query, db, full };
}

it("probes only the two scoped bookkeeping tables before loading application code", async () => {
  const { db, query, full } = fixture();
  try {
    const bootstrap = createMigrationBootstrap(full, db, {
      migrationTableSchema: 'app"scope',
      migrationTableName: 'history"rows',
      migrationLockTableName: "locks",
      provider: { getMigrations: async () => ({}) },
    });
    const tables = await bootstrap.introspector.getTables({ withInternalKyselyTables: true });
    expect(tables.map(({ name, schema }) => ({ name, schema }))).toEqual([
      { name: 'history"rows', schema: 'app"scope' },
      { name: "locks", schema: 'app"scope' },
    ]);
    expect(query.mock.calls.map(([sql]) => sql)).toEqual([
      'SELECT 1 FROM "app""scope"."history""rows" LIMIT 0',
      'SELECT 1 FROM "app""scope"."locks" LIMIT 0',
    ]);
    expect(full.getTables).not.toHaveBeenCalled();
    expect(await bootstrap.introspector.getSchemas()).toEqual([{ name: "app" }]);
  } finally {
    await db.destroy();
  }
});

it("treats missing tables as absent but propagates authorization and uncertain failures", async () => {
  const { db, query, full } = fixture();
  try {
    const bootstrap = createMigrationBootstrap(full, db, {
      provider: { getMigrations: async () => ({}) },
    });
    query.mockRejectedValueOnce(Object.assign(new Error("missing"), { code: "42P01" }));
    expect((await bootstrap.introspector.getTables()).map((table) => table.name)).toEqual([
      "kysely_migration_lock",
    ]);
    for (const code of ["42501", "40001", "08006"]) {
      const failure = Object.assign(new Error("probe failed"), { code });
      query.mockRejectedValueOnce(failure);
      await expect(bootstrap.introspector.getTables()).rejects.toBe(failure);
    }
  } finally {
    await db.destroy();
  }
});

it("restores full fresh introspection before provider, up and down callbacks", async () => {
  const { db, query, full } = fixture();
  try {
    const options = { withInternalKyselyTables: true };
    const assertFull = async () => {
      expect((await bootstrap.introspector.getTables(options)).map((table) => table.name)).toEqual([
        "application",
      ]);
    };
    const provider = {
      getMigrations: vi.fn(async function () {
        expect(this).toBe(provider);
        await assertFull();
        return { "001": { up: assertFull, down: assertFull } };
      }),
    };
    const bootstrap = createMigrationBootstrap(full, db, { provider });
    await bootstrap.introspector.getTables(options);
    query.mockClear();
    const migrations = await bootstrap.provider.getMigrations();
    await migrations["001"].up(db);
    await migrations["001"].down?.(db);
    expect(full.getTables).toHaveBeenCalledTimes(3);
    expect(full.getTables).toHaveBeenLastCalledWith(options);
    expect(query).not.toHaveBeenCalled();
  } finally {
    await db.destroy();
  }
});
