// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
