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

import type { AntflyClient } from "@antfly/sdk";
import type { AuthDatabase, Result } from "./transport.js";

export { antflyAdapter } from "./adapter.js";
export { createUserContent, prepareContentSchema } from "./content.js";
export { authDefaults, consumerAuthOptions } from "./options.js";
export { checkAuthSchema, prepareAuthSchema } from "./schema.js";
/** Supply a client pointing at one stable API owner, not a round-robin endpoint. */
export function remoteDatabase(client: AntflyClient): AuthDatabase {
  return {
    async connect() {
      const connection = await client.openSQLConnection({});
      let sessionId: string | undefined;
      let closed = false;
      let pending: Promise<unknown> = Promise.resolve();
      return {
        execute(statement, parameters = []): Promise<Result> {
          if (closed) return Promise.reject(new Error("SQL connection is closed"));
          const work = pending.then(async (): Promise<Result> => {
            const result = await client.executeSQL({
              statement,
              parameters: [...parameters],
              connection_id: connection.connection_id,
              session_id: sessionId,
              limit: 4096,
            });
            sessionId =
              result.transaction_status === "idle" ? undefined : (result.session_id ?? sessionId);
            let mutationOutcome = result.mutation_outcome;
            // Reconcile only an explicitly committed write. Never resubmit the statement.
            if (
              result.mutation_outcome === "committed_pending" &&
              !result.ddl_receipt &&
              result.transaction_id
            ) {
              for (let attempt = 0; attempt < 200; attempt++) {
                const outcome = await client.getSQLTransactionOutcome(result.transaction_id);
                if (outcome.disposition === "committed") {
                  mutationOutcome = "committed";
                  break;
                }
                if (outcome.disposition !== "committed_pending")
                  throw new Error("Committed auth transaction requires operator reconciliation");
                await new Promise((resolve) => setTimeout(resolve, 50));
              }
            }
            return {
              rowsAffected: BigInt(result.rows_affected),
              mutationOutcome,
              ddlReceipt: result.ddl_receipt,
              rows: result.rows.map((row, i) =>
                Object.fromEntries(
                  result.columns.map((column, j) => [
                    column.name,
                    result.sql_nulls?.[i]?.[j]
                      ? null
                      : column.type === "integer" && row[j] !== null
                        ? BigInt(row[j] as string)
                        : row[j],
                  ])
                )
              ),
            };
          });
          pending = work.catch(() => undefined);
          return work;
        },
        async close() {
          if (!closed) {
            closed = true;
            await pending;
            await client.closeSQLConnection(connection.connection_id);
          }
        },
      };
    },
    listTables: async () => (await client.tables.list()).map((table) => table.name),
    schema: async (name) => (await client.tables.get(name))?.schema,
    assertConstraints: async (name) => {
      if ((await client.tables.constraints.status(name)).state !== "enforced")
        throw new Error(`Auth constraints for ${name} are not enforced`);
    },
    async waitForSchema(receipt) {
      if (
        typeof receipt.table !== "string" ||
        typeof receipt.table_id !== "string" ||
        !Number.isInteger(receipt.schema_version) ||
        receipt.restore_job_id ||
        receipt.database !== "default" ||
        receipt.namespace !== "public"
      )
        throw new Error("Schema receipt requires operator reconciliation; do not replay DDL");
      for (let attempt = 0; attempt < 200; attempt++) {
        const table = (await client.tables.list()).find((table) => table.name === receipt.table);
        if (table) {
          if (
            String(table.table_id) !== receipt.table_id ||
            table.schema?.version !== receipt.schema_version
          )
            throw new Error("Schema generation changed while waiting for activation");
          const status = await client.tables.constraints.status(receipt.table);
          if (
            status.schema_version !== receipt.schema_version ||
            status.retirement ||
            status.state === "invalid"
          )
            throw new Error("Schema activation requires operator review");
          if (status.state === "enforced") return;
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      throw new Error(
        "Schema activation is still pending; inspect constraint status without replaying DDL"
      );
    },
    indexes: (name) => client.indexes.list(name),
    removeIndex: async (name, index) => {
      await client.indexes.drop(name, index);
    },
    createContentTable: async (name, schema) => {
      const { version: _version, ...declaration } = schema;
      await client.tables.create(name, { schema: declaration });
    },
    search: (name, query) => client.tables.query(name, query),
  };
}
