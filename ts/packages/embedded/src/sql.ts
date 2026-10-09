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

import type { Database } from "./database.js";
import type { Uint64Like } from "./marshal.js";
import { SqlError, type SqlResult, type WireResult } from "./sql-types.js";

function request(
  statement: string,
  parameters: readonly unknown[],
  session: Uint64Like,
  streaming = false
): string {
  return JSON.stringify(
    { statement, parameters, session_id: session, limit: streaming ? undefined : 4096 },
    (_key, value) => {
      if (typeof value === "bigint") {
        if (value < -(1n << 63n) || value >= 1n << 63n)
          throw new RangeError("integer parameter is outside the signed 64-bit range");
        return value.toString();
      }
      if (
        typeof value === "number" &&
        (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value)))
      )
        throw new RangeError("use bigint for integer parameters outside JavaScript's safe range");
      return value;
    }
  );
}
function result<Row>(wire: WireResult): SqlResult<Row> {
  return {
    columns: wire.columns,
    commandTag: wire.command_tag,
    rowsAffected: BigInt(wire.rows_affected),
    rows: wire.rows.map(
      (row, rowIndex) =>
        Object.fromEntries(
          wire.columns.map((column, index) => [
            column.name,
            wire.sql_nulls?.[rowIndex]?.[index]
              ? null
              : column.type === "integer" && row[index] !== null
                ? BigInt(row[index] as string)
                : row[index],
          ])
        ) as Row
    ),
  };
}

/** One SQL session. Use a separate Connection for concurrently active transactions. */
export class Connection {
  #pending: Promise<unknown> = Promise.resolve();
  #closed = false;
  private constructor(
    readonly database: Database,
    readonly session: Uint64Like,
    private readonly direct = false
  ) {}
  static async open(database: Database): Promise<Connection> {
    return new Connection(database, await database.openSqlSession());
  }
  #run<T>(operation: () => Promise<T>): Promise<T> {
    if (this.#closed) return Promise.reject(new Error("SQL connection is closed"));
    if (this.direct) return operation();
    const work = this.#pending.then(operation);
    this.#pending = work.catch(() => undefined);
    return work;
  }
  execute<Row = Record<string, unknown>>(
    statement: string,
    parameters: readonly unknown[] = []
  ): Promise<SqlResult<Row>> {
    return this.#run(async () =>
      result<Row>(
        (await this.database.sqlJson(request(statement, parameters, this.session))) as WireResult
      )
    );
  }
  query<Row = Record<string, unknown>>(
    statement: string,
    parameters: readonly unknown[] = []
  ): Promise<SqlResult<Row>> {
    return this.#run(async () => {
      let cursor: Uint64Like;
      try {
        cursor = await this.database.openSqlCursor(
          request(statement, parameters, this.session, true)
        );
      } catch (error) {
        if (!(error instanceof SqlError) || error.sqlstate !== "0A000") throw error;
        return result<Row>(
          (await this.database.sqlJson(request(statement, parameters, this.session))) as WireResult
        );
      }
      let output: SqlResult<Row> | undefined;
      try {
        while (true) {
          const page = (await this.database.fetchSqlCursor(cursor)) as {
            result: WireResult;
            exhausted: boolean;
          };
          const decoded = result<Row>(page.result);
          if (output) output.rows.push(...decoded.rows);
          else output = decoded;
          if (page.exhausted) return output;
        }
      } finally {
        await this.database.closeSqlCursor(cursor);
      }
    });
  }
  async *stream<Row = Record<string, unknown>>(
    statement: string,
    parameters: readonly unknown[] = [],
    chunkSize = 128
  ): AsyncGenerator<SqlResult<Row>> {
    if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > 4096)
      throw new RangeError("chunkSize must be 1..4096");
    if (this.#closed) throw new Error("SQL connection is closed");
    const previous = this.#pending;
    let release: () => void = () => {};
    if (!this.direct)
      this.#pending = new Promise<void>((resolve) => {
        release = resolve;
      });
    await previous;
    let cursor: Uint64Like | undefined;
    try {
      cursor = await this.database.openSqlCursor(
        request(statement, parameters, this.session, true)
      );
      while (true) {
        const page = (await this.database.fetchSqlCursor(cursor, chunkSize)) as {
          result: WireResult;
          exhausted: boolean;
        };
        yield result<Row>(page.result);
        if (page.exhausted) break;
      }
    } finally {
      try {
        if (cursor !== undefined) await this.database.closeSqlCursor(cursor);
      } finally {
        release();
      }
    }
  }
  transaction<T>(operation: (transaction: Connection) => Promise<T>): Promise<T> {
    return this.#run(async () => {
      const transaction = new Connection(this.database, this.session, true);
      await transaction.execute("BEGIN ISOLATION LEVEL READ COMMITTED");
      try {
        const value = await operation(transaction);
        await transaction.execute("COMMIT");
        return value;
      } catch (error) {
        try {
          await transaction.execute("ROLLBACK");
        } catch {
          /* Preserve the original diagnostic, including unknown commit outcome. */
        }
        throw error;
      } finally {
        transaction.#closed = true;
      }
    });
  }
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#pending;
    await this.database.closeSqlSession(this.session);
  }
}
