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

/**
 * Coverage beyond the shared conformance cases for the storage_kind /
 * cross-storage-restore surface added in the capi/naming-cleanup ABI
 * cleanup: restoring a .aflite backup into directory storage, and reopening
 * that directory afterwards (see zig/pkg/antfly-embedded/capi-conformance/cases/
 * directory_storage.json and backup_across_storage.json for the shared
 * cases this complements).
 */
// Database.sqlJson: statements run against the embedded table, and a failed
// statement throws an AntflyError whose `.body` carries the SQL diagnostics.
// Mirrors rs/crates/embedded/tests/sql.rs.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { createWithOptions } from "../src/database.js";
import { AntflyError, NotFoundError } from "../src/errors.js";
import { describeWithLibrary } from "./helpers.js";

function schema(enforced: boolean) {
  return {
    version: 1,
    ...(enforced ? { enforce_types: true } : {}),
    default_type: "row",
    document_schemas: {
      row: {
        schema: {
          type: "object",
          properties: { n: enforced ? { type: "integer", minimum: 0 } : { type: "integer" } },
          ...(enforced ? { required: ["n"] } : {}),
          additionalProperties: true,
        },
      },
    },
  };
}

describeWithLibrary("sqlJson", () => {
  it("reads and mutates documents", async () => {
    const dir = mkdtempSync(join(tmpdir(), "antfly-embedded-sql-"));
    const db = await createWithOptions(join(dir, "rw.aflite"), { noSync: true });
    try {
      await db.setSchema(schema(false));
      await db.batchJson({ inserts: { a: { n: 1, extra: true } } });

      const selected = (await db.sqlJson("items", {
        statement: "SELECT n FROM items WHERE _id='a'",
      })) as { rows: unknown[][] };
      expect(selected.rows).toEqual([["1"]]);

      await db.sqlJson("items", {
        statement: "INSERT INTO items (_id,n) VALUES ('b',2) RETURNING n",
      });
      await db.sqlJson("items", {
        statement: "UPDATE items SET n=n+10 WHERE _id='a' RETURNING n",
      });

      expect(await db.lookup("a")).toEqual({ n: 11, extra: true });
      expect(await db.lookup("b")).toEqual({ n: 2 });
      const count = await db.sqlJsonRaw("items", { statement: "SELECT COUNT(*) FROM items" });
      expect(Buffer.isBuffer(count)).toBe(true);
      expect(JSON.parse(count.toString("utf8")).rows).toEqual([["2"]]);
    } finally {
      await db.close();
    }
  });

  it("carries SQL diagnostics on failure", async () => {
    const dir = mkdtempSync(join(tmpdir(), "antfly-embedded-sql-"));
    const db = await createWithOptions(join(dir, "err.aflite"), { noSync: true });
    try {
      await db.setSchema(schema(true));

      const rejected = await db
        .sqlJson("items", {
          statement: "INSERT INTO items (_id,n) VALUES ('valid',1),('invalid',-1)",
        })
        .then(
          () => undefined,
          (e: unknown) => e
        );
      expect(rejected).toBeInstanceOf(AntflyError);
      const body = (rejected as AntflyError).body as { error: { code: string; message: string } };
      expect(body.error.code).toMatch(/^[0-9A-Z]{5}$/);
      expect((rejected as AntflyError).message).toContain(body.error.code);
      const count = (await db.sqlJson("items", { statement: "SELECT COUNT(*) FROM items" })) as {
        rows: unknown[][];
      };
      expect(count.rows).toEqual([["0"]]);

      const ddl = await db.sqlJson("items", { statement: "CREATE TABLE other (id INT)" }).then(
        () => undefined,
        (e: unknown) => e
      );
      expect(ddl).toBeInstanceOf(AntflyError);
      expect(ddl).not.toBeInstanceOf(NotFoundError);
    } finally {
      await db.close();
    }

    const closed = await db.sqlJson("items", { statement: "SELECT 1" }).then(
      () => undefined,
      (e: unknown) => e
    );
    expect(closed).toBeInstanceOf(AntflyError);
    expect((closed as AntflyError).body).toBeUndefined();
  });
});
