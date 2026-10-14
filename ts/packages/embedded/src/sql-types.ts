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

export interface SqlDiagnostic {
  code: string;
  message: string;
  hint?: string;
  retryable?: boolean;
}
export class SqlError extends Error {
  readonly sqlstate: string;
  constructor(
    readonly diagnostic: SqlDiagnostic,
    readonly transactionId?: string
  ) {
    super(`${diagnostic.code}: ${diagnostic.message}`);
    this.name = "SqlError";
    this.sqlstate = diagnostic.code;
  }
}
export interface SqlColumn {
  name: string;
  type: string;
}
export interface SqlResult<Row = Record<string, unknown>> {
  columns: readonly SqlColumn[];
  rows: Row[];
  rowsAffected: bigint;
  commandTag: string;
}
export interface WireResult {
  columns: SqlColumn[];
  rows: unknown[][];
  sql_nulls: boolean[][] | null;
  rows_affected: number;
  command_tag: string;
}
