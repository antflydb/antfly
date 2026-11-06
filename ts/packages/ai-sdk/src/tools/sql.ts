import type { AntflyClient } from "@antfly/sdk";
import { type Tool, tool } from "ai";
import { z } from "zod";

export interface AntflySqlToolOptions {
  client: AntflyClient;
  database?: string;
  namespace?: string;
  /**
   * Default `true`. When true, only statements that look like reads
   * (SELECT/WITH/SHOW/EXPLAIN/VALUES/TABLE) are allowed.
   *
   * IMPORTANT: this is a client-side syntax check, not a security boundary.
   * A model (or anything else holding this tool) could rephrase a statement
   * to slip past a naive keyword check, and this package cannot enforce
   * anything the credential itself does not enforce. The real server-side
   * enforcement is per-table `read` vs `write` permission grants, checked
   * in `zig/pkg/antfly/src/api/sql_execution.zig` via
   * `http_server.permissionsAllow(identity.permissions, .table, ..., .write)`
   * (see `specs/openapi/auth/api.yaml`'s table-scoped grants). Construct
   * this tool with a credential that only has `read` grants on the tables
   * it should touch if you need that guarantee; this flag only gives a
   * fast, honest, client-side fail-safe on top of that.
   */
  readOnly?: boolean;
  /** Admission bound on result rows, not an implicit SQL LIMIT. Default 128. */
  maxRows?: number;
}

const READ_ONLY_LEADING_KEYWORDS = new Set([
  "select",
  "with",
  "show",
  "explain",
  "table",
  "values",
]);

const MUTATING_KEYWORDS = new Set([
  "insert",
  "update",
  "delete",
  "merge",
  "drop",
  "alter",
  "create",
  "truncate",
  "grant",
  "revoke",
  "copy",
  "call",
  "into",
]);

/** Removes comments and quoted text, so only SQL keywords and identifiers remain. */
function sqlCode(statement: string): string {
  return statement.replace(
    /\/\*[\s\S]*?\*\/|--[^\n]*|'(?:[^']|'')*'|"(?:[^"]|"")*"|\$([a-zA-Z_]*)\$[\s\S]*?\$\1\$/g,
    " "
  );
}

/**
 * Best-effort, NOT a security boundary -- see `AntflySqlToolOptions.readOnly`.
 * It errs towards refusing: `WITH x AS (DELETE ...)`, `SELECT ... INTO`, `EXPLAIN
 * ANALYZE DELETE` and a second statement after `;` are all rejected, as is an
 * unquoted identifier that happens to be one of these keywords.
 */
function looksReadOnly(statement: string): boolean {
  const code = sqlCode(statement).trim().replace(/;\s*$/, "");
  if (code.includes(";")) return false;
  const words = code.toLowerCase().match(/[a-z_][a-z0-9_]*/g) ?? [];
  const first = words[0];
  if (first === undefined || !READ_ONLY_LEADING_KEYWORDS.has(first)) return false;
  return !words.some((word) => MUTATING_KEYWORDS.has(word));
}

/**
 * A single SQL tool over `AntflyClient.executeSQL`, scoped to a fixed
 * database/namespace at construction time. Read-only by default (see
 * `AntflySqlToolOptions.readOnly` for exactly what that does and does not
 * guarantee); writing requires explicit opt-in by the code that
 * constructs this tool, never by a tool-call argument.
 */
export function createSqlTool(options: AntflySqlToolOptions): Record<string, Tool> {
  const readOnly = options.readOnly ?? true;
  const sql = tool({
    description: `Execute one ${readOnly ? "read-only (SELECT/WITH/SHOW/EXPLAIN)" : ""} SQL statement against the Antfly relational catalog${
      options.database ? ` (database '${options.database}'` : ""
    }${options.namespace ? `, namespace '${options.namespace}')` : options.database ? ")" : ""}. Bind values with positional parameters ($1, $2, ...); never interpolate values into the statement text.`,
    inputSchema: z
      .object({
        statement: z.string().min(1).max(100000),
        parameters: z.array(z.unknown()).max(1024).optional(),
      })
      .strict(),
    execute: async ({ statement, parameters }) => {
      if (readOnly && !looksReadOnly(statement)) {
        throw new Error(
          "This SQL tool is configured read-only and only accepts SELECT/WITH/SHOW/EXPLAIN/VALUES/TABLE statements. " +
            "(Client-side check only -- see AntflySqlToolOptions.readOnly for what actually enforces this.)"
        );
      }
      return await options.client.executeSQL({
        statement,
        parameters: parameters as never,
        database: options.database,
        namespace: options.namespace,
        limit: options.maxRows ?? 128,
      });
    },
  });
  return { antfly_sql: sql };
}
