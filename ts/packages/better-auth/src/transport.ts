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

export type Row = Record<string, unknown>;
export interface Result {
  rows: Row[];
  rowsAffected: bigint;
  mutationOutcome?: string | null;
  ddlReceipt?: Readonly<Record<string, unknown>> | null;
}
export interface Session {
  execute(statement: string, parameters?: readonly unknown[]): Promise<Result>;
  close(): Promise<void>;
}
/** Server-only capability. Never expose this interface to browser clients. */
export interface AuthDatabase {
  connect(): Promise<Session>;
  listTables(): Promise<string[]>;
  schema(table: string): Promise<unknown>;
  assertConstraints?(table: string): Promise<void>;
  waitForSchema?(receipt: Readonly<Record<string, unknown>>): Promise<void>;
  indexes(table: string): Promise<unknown>;
  removeIndex(table: string, index: string): Promise<void>;
  createContentTable(table: string, schema: Record<string, unknown>): Promise<void>;
  search(table: string, query: Record<string, unknown>): Promise<unknown>;
}
export function identifier(value: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) throw new Error("Invalid schema identifier");
  return `"${value}"`;
}
export function assertReady(result: Result): Result {
  if (result.mutationOutcome && result.mutationOutcome !== "committed") {
    throw new Error(
      "Write committed but is not ready. Inspect database status; do not repeat the write."
    );
  }
  if (result.ddlReceipt && result.ddlReceipt.state !== "ready") {
    throw new Error(
      "Schema is not ready. Inspect table constraint status before starting authentication."
    );
  }
  return result;
}
export async function execute(
  db: AuthDatabase,
  sql: string,
  args: readonly unknown[] = []
): Promise<Result> {
  const session = await db.connect();
  try {
    return assertReady(await session.execute(sql, args));
  } finally {
    await session.close();
  }
}
