import type { AntflyClient } from "@antfly/sdk";
import { type Tool, tool } from "ai";
import { z } from "zod";

export interface AntflySqlToolOptions {
  client: AntflyClient;
  database?: string;
  namespace?: string;
  /**
   * Default `true`: refuse statements that look like writes. This catches mistakes
   * when the credential is broader than it should be, or when a local server runs
   * with authentication off. It is not a security boundary. Antfly's permissions
   * are: give this tool an API key whose grants only allow `read`.
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

/** A monotonic lexer: malformed quoted text fails closed without rescanning suffixes. */
function sqlCode(statement: string): string | undefined {
  const chunks: string[] = [];
  let i = 0;
  let start = 0;
  while (i < statement.length) {
    const c = statement[i];
    let end: number | undefined;
    if (statement.startsWith("--", i)) {
      const newline = statement.indexOf("\n", i + 2);
      end = newline < 0 ? statement.length : newline + 1;
    } else if (statement.startsWith("/*", i)) {
      let depth = 1;
      let cursor = i + 2;
      while (cursor < statement.length && depth > 0) {
        if (statement.startsWith("/*", cursor)) {
          depth++;
          cursor += 2;
        } else if (statement.startsWith("*/", cursor)) {
          depth--;
          cursor += 2;
        } else cursor++;
      }
      if (depth !== 0) return undefined;
      end = cursor;
    } else if (c === "'" || c === '"') {
      let cursor = i + 1;
      let closed = false;
      while (cursor < statement.length) {
        if (statement[cursor++] !== c) continue;
        if (statement[cursor] === c) {
          cursor++;
          continue;
        }
        closed = true;
        break;
      }
      if (!closed) return undefined;
      end = cursor;
    } else if (c === "$" && (statement[i + 1] === "$" || /[a-z_]/i.test(statement[i + 1] ?? ""))) {
      let cursor = i + 1;
      while (cursor < statement.length && /[a-z0-9_]/i.test(statement[cursor])) cursor++;
      if (statement[cursor] === "$") {
        const delimiter = statement.slice(i, cursor + 1);
        let close = cursor + 1;
        while (close < statement.length) {
          if (statement[close] !== "$") {
            close++;
            continue;
          }
          let matched = 1;
          while (matched < delimiter.length && statement[close + matched] === delimiter[matched])
            matched++;
          if (matched === delimiter.length) {
            end = close + matched;
            break;
          }
          // Tags contain no internal dollar sign. Consume the matched prefix;
          // the mismatching character can itself begin the next candidate.
          close += matched;
        }
        if (end === undefined) return undefined;
      } else {
        // The entire candidate is an ordinary token; do not scan it again.
        i = cursor;
        continue;
      }
    }
    if (end !== undefined) {
      chunks.push(statement.slice(start, i), " ");
      i = end;
      start = i;
    } else i++;
  }
  chunks.push(statement.slice(start));
  return chunks.join("");
}

/**
 * Best-effort, NOT a security boundary -- see `AntflySqlToolOptions.readOnly`.
 * It errs towards refusing: `WITH x AS (DELETE ...)`, `SELECT ... INTO`, `EXPLAIN
 * ANALYZE DELETE` and a second statement after `;` are all rejected, as is an
 * unquoted identifier that happens to be one of these keywords.
 */
function looksReadOnly(statement: string): boolean {
  const scanned = sqlCode(statement);
  if (scanned === undefined) return false;
  const code = scanned.trim().replace(/;\s*$/, "");
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
    execute: async ({ statement, parameters }, { abortSignal }) => {
      abortSignal?.throwIfAborted();
      if (readOnly && !looksReadOnly(statement)) {
        throw new Error(
          "This SQL tool is configured read-only and only accepts SELECT/WITH/SHOW/EXPLAIN/VALUES/TABLE statements. " +
            "(Client-side check only -- see AntflySqlToolOptions.readOnly for what actually enforces this.)"
        );
      }
      return await options.client.executeSQL(
        {
          statement,
          parameters: parameters as never,
          database: options.database,
          namespace: options.namespace,
          limit: options.maxRows ?? 128,
        },
        { signal: abortSignal }
      );
    },
  });
  return { antfly_sql: sql };
}
