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

import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { createHosted } from "../src/database.js";
import { describeWithLibrary } from "./helpers.js";

describeWithLibrary("Cypher", () => {
  it("preserves exact parameters, atomic writes, diagnostics, and receipts", async () => {
    const directory = await mkdtemp(join(tmpdir(), "antfly-cypher-"));
    const db = await createHosted(join(directory, "db.aflite"));
    try {
      await db.sqlJson({ statement: "CREATE TABLE docs (id TEXT)" });
      await db.graphCatalogJson({
        action: "put",
        name: "knowledge",
        definition: { labels: [{ name: "Document", table: "docs" }] },
      });
      await expect(
        db.cypherJson("knowledge", {
          query: "RETURN $minimum,$maximum",
          typed_parameters: {
            type: "map",
            value: {
              minimum: { type: "integer", value: "-9223372036854775808" },
              maximum: { type: "integer", value: "9223372036854775807" },
            },
          },
        })
      ).resolves.toMatchObject({
        rows: [
          [
            { type: "integer", value: "-9223372036854775808" },
            { type: "integer", value: "9223372036854775807" },
          ],
        ],
      });
      const id = randomUUID().replaceAll("-", "");
      const write = {
        query: "CREATE (n:Document {id:'created'}) RETURN n.id",
        write_request_id: id,
      };
      await expect(db.cypherJson("knowledge", write)).resolves.toMatchObject({
        commit: { status: "committed", transaction_id: id },
      });
      await expect(db.cypherJson("knowledge", write)).rejects.toMatchObject({
        diagnostic: {
          code: "CypherWriteRequestAlreadyUsed",
          write_outcome: "unknown",
          transaction_id: id,
        },
      });
      await expect(
        db.cypherJson("knowledge", { query: "CREATE (n:Document {id:'aborted'}) RETURN 1/0" })
      ).rejects.toMatchObject({
        diagnostic: { code: "CypherDivisionByZero", write_outcome: "not_started" },
      });
      await expect(
        db.cypherJson("knowledge", { query: "MATCH (n:Document {id:'aborted'}) RETURN n" })
      ).resolves.toMatchObject({ rows: [] });
      await db.dropTable("docs");
      await expect(db.graphWriteStatus(id)).resolves.toMatchObject({
        disposition: "committed",
        durable: true,
      });
    } finally {
      await db.close();
      await rm(directory, { recursive: true, force: true });
    }
  });
});
