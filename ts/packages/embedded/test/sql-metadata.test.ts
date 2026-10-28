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

import { expect, it } from "vitest";
import type { Database } from "../src/database.js";
import { Connection } from "../src/sql.js";

it("preserves write disposition and DDL readiness without changing row decoding", async () => {
  const receipt = { state: "pending", table: "users", schema_version: 2 };
  const database = {
    openSqlSession: async () => 1,
    closeSqlSession: async () => {},
    sqlJson: async () => ({
      columns: [{ name: "n", type: "integer" }],
      rows: [["12"]],
      rows_affected: 1,
      command_tag: "UPDATE",
      mutation_outcome: "committed_pending",
      ddl_receipt: receipt,
    }),
  } as unknown as Database;
  const connection = await Connection.open(database);
  try {
    expect(await connection.execute("UPDATE users")).toMatchObject({
      rows: [{ n: 12n }],
      rowsAffected: 1n,
      mutationOutcome: "committed_pending",
      ddlReceipt: receipt,
    });
  } finally {
    await connection.close();
  }
});
