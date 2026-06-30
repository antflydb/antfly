import {
  Alert,
  AlertDescription,
  Badge,
  Button,
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  DashboardToolbar,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  Textarea,
} from "@antfly/design-system";
import type { SqlStatementRequest, SqlStatementResponse } from "@antfly/sdk";
import { Copy, Database, History, Play, RotateCcw, Table as TableIcon } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useApi, useApiConfig } from "@/hooks/use-api-config";
import { useTable } from "@/hooks/use-table";

type SqlCapableApi = {
  sql?: {
    execute: (request: SqlStatementRequest) => Promise<SqlStatementResponse>;
  };
};

type SqlHistoryEntry = {
  id: string;
  sql: string;
  kind: SqlStatementResponse["kind"] | "error";
  statementKind?: string | null;
  sessionId?: number;
  durationMs: number;
  createdAt: string;
  error?: string;
};

type ResultTable = {
  rows: Record<string, unknown>[];
  columns: string[];
};

const HISTORY_LIMIT = 30;
const HISTORY_STORAGE_KEY = "antfarm-sql-history";
const DEFAULT_SQL = "SELECT 1;";

function selectForTable(tableName: string) {
  return `SELECT * FROM ${quoteIdentifier(tableName)} LIMIT 100;`;
}

function quoteIdentifier(identifier: string) {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function stripLeadingComments(sql: string) {
  let remaining = sql.trimStart();
  let changed = true;
  while (changed) {
    changed = false;
    if (remaining.startsWith("--")) {
      const newline = remaining.indexOf("\n");
      remaining = newline === -1 ? "" : remaining.slice(newline + 1).trimStart();
      changed = true;
    } else if (remaining.startsWith("/*")) {
      const end = remaining.indexOf("*/");
      remaining = end === -1 ? "" : remaining.slice(end + 2).trimStart();
      changed = true;
    }
  }
  return remaining;
}

function firstKeyword(sql: string) {
  return (
    stripSqlCode(sql)
      .match(/^[a-zA-Z_]+/)?.[0]
      ?.toLowerCase() ?? ""
  );
}

function stripSqlCode(sql: string) {
  let out = "";
  for (let i = 0; i < sql.length; i += 1) {
    const current = sql[i];
    const next = sql[i + 1];

    if (current === "-" && next === "-") {
      i += 2;
      while (i < sql.length && sql[i] !== "\n") i += 1;
      out += " ";
      continue;
    }

    if (current === "/" && next === "*") {
      i += 2;
      while (i < sql.length && !(sql[i] === "*" && sql[i + 1] === "/")) i += 1;
      i += 1;
      out += " ";
      continue;
    }

    if (current === "'") {
      i += 1;
      while (i < sql.length) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          i += 2;
          continue;
        }
        if (sql[i] === "'") break;
        i += 1;
      }
      out += " ";
      continue;
    }

    if (current === '"') {
      i += 1;
      while (i < sql.length) {
        if (sql[i] === '"' && sql[i + 1] === '"') {
          i += 2;
          continue;
        }
        if (sql[i] === '"') break;
        i += 1;
      }
      out += " ";
      continue;
    }

    out += current;
  }
  return stripLeadingComments(out).trimStart();
}

function hasExtraStatement(sql: string) {
  return stripSqlCode(sql)
    .split(";")
    .slice(1)
    .some((part) => part.trim().length > 0);
}

const READ_ONLY_KEYWORDS = new Set([
  "select",
  "show",
  "describe",
  "desc",
  "explain",
  "with",
  "values",
  "use",
  "set",
  "reset",
]);

const MUTATING_KEYWORDS = new Set([
  "alter",
  "analyze",
  "call",
  "copy",
  "create",
  "delete",
  "drop",
  "execute",
  "grant",
  "insert",
  "merge",
  "replace",
  "revoke",
  "truncate",
  "update",
  "vacuum",
]);

function isReadOnlyAllowed(sql: string) {
  const code = stripSqlCode(sql);
  const keyword = firstKeyword(code);
  if (!READ_ONLY_KEYWORDS.has(keyword)) return false;
  if (hasExtraStatement(code)) return false;

  const tokens = code.match(/[a-zA-Z_][a-zA-Z0-9_]*/g) ?? [];
  return !tokens.some((token) => MUTATING_KEYWORDS.has(token.toLowerCase()));
}

function stringifyValue(value: unknown) {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return JSON.stringify(value);
}

function resultRowKey(row: Record<string, unknown>, index: number) {
  const identity = row.id ?? row.key ?? row._id ?? JSON.stringify(row);
  return `${stringifyValue(identity)}:${index}`;
}

function objectRows(value: unknown): Record<string, unknown>[] | null {
  if (!Array.isArray(value)) return null;
  if (value.length === 0) return [];
  if (value.every((row) => row && typeof row === "object" && !Array.isArray(row))) {
    return value as Record<string, unknown>[];
  }
  return null;
}

function rowsFromColumns(value: unknown): ResultTable | null {
  if (!value || typeof value !== "object") return null;
  const candidate = value as { columns?: unknown; values?: unknown; rows?: unknown };
  if (!Array.isArray(candidate.columns)) return null;
  const columns = candidate.columns.map((column) => String(column));
  const values = Array.isArray(candidate.values) ? candidate.values : candidate.rows;
  if (!Array.isArray(values)) return null;
  const rows = values.map((row) => {
    if (!Array.isArray(row)) return {};
    return Object.fromEntries(columns.map((column, index) => [column, row[index]]));
  });
  return { columns, rows };
}

function findRows(value: unknown): Record<string, unknown>[] | null {
  const directRows = objectRows(value);
  if (directRows) return directRows;
  if (!value || typeof value !== "object") return null;

  const object = value as Record<string, unknown>;
  for (const key of ["rows", "documents", "records", "items", "data"]) {
    const rows = objectRows(object[key]);
    if (rows) return rows;
  }

  for (const key of ["result_set", "resultSet", "results", "response"]) {
    const rows = findRows(object[key]);
    if (rows) return rows;
  }

  const resultSets = object.result_sets ?? object.resultSets ?? object.sets;
  if (Array.isArray(resultSets)) {
    for (const resultSet of resultSets) {
      const rows = findRows(resultSet);
      if (rows) return rows;
    }
  }

  const responses = object.responses;
  if (Array.isArray(responses)) {
    for (const response of responses) {
      const rows = findRows(response);
      if (rows) return rows;
    }
  }

  return null;
}

function resultTable(response: SqlStatementResponse | null): ResultTable | null {
  if (!response?.result) return null;
  const columnRows = rowsFromColumns(response.result);
  if (columnRows) return columnRows;
  const rows = findRows(response.result);
  if (!rows) return null;
  const columns = Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
  return { rows, columns };
}

function readHistory(storageKey: string): SqlHistoryEntry[] {
  if (typeof window === "undefined") return [];
  try {
    return JSON.parse(window.localStorage.getItem(storageKey) ?? "[]") as SqlHistoryEntry[];
  } catch {
    return [];
  }
}

function writeHistory(storageKey: string, entries: SqlHistoryEntry[]) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(storageKey, JSON.stringify(entries.slice(0, HISTORY_LIMIT)));
  } catch {
    // Local history is best-effort only.
  }
}

async function executeSql(
  api: unknown,
  apiUrl: string,
  request: SqlStatementRequest
): Promise<SqlStatementResponse> {
  const sqlApi = api as SqlCapableApi;
  if (sqlApi.sql?.execute) return sqlApi.sql.execute(request);

  const response = await fetch(`${apiUrl.replace(/\/$/, "")}/sql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });
  if (!response.ok) {
    let message = `SQL execution failed with HTTP ${response.status}`;
    try {
      const body = (await response.json()) as { error?: unknown; message?: unknown };
      if (typeof body.error === "string") message = body.error;
      else if (typeof body.message === "string") message = body.message;
    } catch {
      // Keep the HTTP status fallback.
    }
    throw new Error(message);
  }
  return (await response.json()) as SqlStatementResponse;
}

export default function SqlWorkbenchPage() {
  const api = useApi();
  const { apiUrl } = useApiConfig();
  const { tables, isLoadingTables, selectedTable, setSelectedTable } = useTable();
  const editorRef = useRef<HTMLTextAreaElement | null>(null);

  const [sql, setSql] = useState(() =>
    selectedTable ? selectForTable(selectedTable) : DEFAULT_SQL
  );
  const [database, setDatabase] = useState("");
  const [namespace, setNamespace] = useState("");
  const [sessionId, setSessionId] = useState<number | null>(null);
  const [readOnly, setReadOnly] = useState(true);
  const [isRunning, setIsRunning] = useState(false);
  const [response, setResponse] = useState<SqlStatementResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [lastDurationMs, setLastDurationMs] = useState<number | null>(null);
  const [history, setHistory] = useState<SqlHistoryEntry[]>(() => readHistory(HISTORY_STORAGE_KEY));

  useEffect(() => {
    if (selectedTable && sql === DEFAULT_SQL) {
      setSql(selectForTable(selectedTable));
    }
  }, [selectedTable, sql]);

  const tableResult = useMemo(() => resultTable(response), [response]);

  const addHistoryEntry = useCallback((entry: SqlHistoryEntry) => {
    setHistory((current) => {
      const next = [entry, ...current].slice(0, HISTORY_LIMIT);
      writeHistory(HISTORY_STORAGE_KEY, next);
      return next;
    });
  }, []);

  const statementToRun = useCallback(() => {
    const editor = editorRef.current;
    if (!editor || editor.selectionStart === editor.selectionEnd) return sql.trim();
    return sql.slice(editor.selectionStart, editor.selectionEnd).trim();
  }, [sql]);

  const runSql = useCallback(async () => {
    const statement = statementToRun();
    if (!statement) return;
    if (readOnly && !isReadOnlyAllowed(statement)) {
      setError(
        "Read-only mode blocks this statement. Disable read-only mode to run writes or DDL."
      );
      return;
    }

    const started = performance.now();
    setIsRunning(true);
    setError(null);
    try {
      const nextResponse = await executeSql(api, apiUrl, {
        sql: statement,
        session_id: sessionId,
        database: database.trim() || null,
        namespace: namespace.trim() || null,
        read_only: readOnly ? true : undefined,
      });
      const durationMs = Math.round(performance.now() - started);
      setResponse(nextResponse);
      setSessionId(nextResponse.session_id);
      setLastDurationMs(durationMs);
      addHistoryEntry({
        id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
        sql: statement,
        kind: nextResponse.kind,
        statementKind: nextResponse.statement_kind,
        sessionId: nextResponse.session_id,
        durationMs,
        createdAt: new Date().toISOString(),
      });
    } catch (caught) {
      const durationMs = Math.round(performance.now() - started);
      const message = caught instanceof Error ? caught.message : String(caught);
      setError(message);
      setLastDurationMs(durationMs);
      addHistoryEntry({
        id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
        sql: statement,
        kind: "error",
        durationMs,
        createdAt: new Date().toISOString(),
        error: message,
      });
    } finally {
      setIsRunning(false);
    }
  }, [addHistoryEntry, api, apiUrl, database, namespace, readOnly, sessionId, statementToRun]);

  const resetSession = useCallback(() => {
    setSessionId(null);
    setResponse(null);
    setError(null);
    setLastDurationMs(null);
  }, []);

  const copySql = useCallback(() => {
    navigator.clipboard.writeText(sql);
  }, [sql]);

  const copyResponse = useCallback(() => {
    navigator.clipboard.writeText(JSON.stringify(response ?? { error }, null, 2));
  }, [error, response]);

  return (
    <DashboardPage className="h-full space-y-3">
      <DashboardPageHeader>
        <div>
          <DashboardPageTitle className="font-aeonik">SQL Workbench</DashboardPageTitle>
          <DashboardPageDescription>
            Run Antfly SQL with dashboard session state and typed results.
          </DashboardPageDescription>
        </div>
        <DashboardPageActions>
          <Button variant="outline" onClick={resetSession}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset Session
          </Button>
          <Button onClick={runSql} disabled={isRunning || sql.trim().length === 0}>
            <Play className="h-4 w-4 mr-2" />
            {isRunning ? "Running" : "Run"}
          </Button>
        </DashboardPageActions>
      </DashboardPageHeader>

      <DashboardToolbar className="flex-row flex-wrap items-center gap-3 md:items-center">
        <Badge className="gap-1.5">
          <Database className="h-3.5 w-3.5" />
          Session {sessionId ?? "new"}
        </Badge>
        {response && <Badge>{response.kind}</Badge>}
        {response?.statement_kind && <Badge>{response.statement_kind}</Badge>}
        {lastDurationMs !== null && (
          <span className="text-xs text-muted-foreground">{lastDurationMs} ms</span>
        )}
        <div className="ml-auto flex items-center gap-2">
          <Label htmlFor="sql-read-only" className="text-xs text-muted-foreground">
            Read only
          </Label>
          <Switch id="sql-read-only" checked={readOnly} onCheckedChange={setReadOnly} />
        </div>
      </DashboardToolbar>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      <div className="grid min-h-[680px] grid-cols-1 gap-4 xl:grid-cols-[280px_minmax(0,1fr)]">
        <aside className="rounded-md border bg-background">
          <div className="border-b p-3">
            <div className="flex items-center gap-2 text-sm font-medium">
              <TableIcon className="h-4 w-4 text-muted-foreground" />
              Schema
            </div>
          </div>
          <div className="space-y-4 p-3">
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">Table</Label>
              <Select
                value={selectedTable || undefined}
                onValueChange={(value) => {
                  setSelectedTable(value);
                  setSql(selectForTable(value));
                }}
                disabled={isLoadingTables || tables.length === 0}
              >
                <SelectTrigger>
                  <SelectValue
                    placeholder={isLoadingTables ? "Loading tables..." : "Select table"}
                  />
                </SelectTrigger>
                <SelectContent>
                  {tables.map((table) => (
                    <SelectItem key={table.name} value={table.name}>
                      {table.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="grid grid-cols-1 gap-3">
              <div className="space-y-2">
                <Label htmlFor="sql-database" className="text-xs text-muted-foreground">
                  Database
                </Label>
                <Input
                  id="sql-database"
                  value={database}
                  onChange={(event) => setDatabase(event.target.value)}
                  placeholder="default"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="sql-namespace" className="text-xs text-muted-foreground">
                  Namespace
                </Label>
                <Input
                  id="sql-namespace"
                  value={namespace}
                  onChange={(event) => setNamespace(event.target.value)}
                  placeholder="public"
                />
              </div>
            </div>

            <Button
              variant="outline"
              className="w-full justify-start"
              disabled={!selectedTable}
              onClick={() => selectedTable && setSql(selectForTable(selectedTable))}
            >
              <TableIcon className="h-4 w-4 mr-2" />
              Select 100 rows
            </Button>

            <div className="space-y-2">
              <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
                <History className="h-3.5 w-3.5" />
                Recent
              </div>
              <div className="max-h-80 space-y-1 overflow-y-auto">
                {history.length === 0 ? (
                  <div className="rounded-md border border-dashed p-3 text-xs text-muted-foreground">
                    No SQL history yet.
                  </div>
                ) : (
                  history.slice(0, 10).map((entry) => (
                    <button
                      key={entry.id}
                      type="button"
                      className="w-full rounded-md border px-2 py-2 text-left text-xs hover:bg-muted"
                      onClick={() => setSql(entry.sql)}
                    >
                      <div className="flex items-center gap-2">
                        <Badge className="text-[10px]">{entry.kind}</Badge>
                        <span className="text-muted-foreground">{entry.durationMs} ms</span>
                      </div>
                      <div className="mt-1 line-clamp-2 font-mono">{entry.sql}</div>
                    </button>
                  ))
                )}
              </div>
            </div>
          </div>
        </aside>

        <main className="grid min-w-0 grid-rows-[minmax(260px,0.85fr)_minmax(320px,1fr)] gap-4">
          <section className="rounded-md border bg-background">
            <div className="flex items-center justify-between border-b px-3 py-2">
              <Label htmlFor="sql-editor" className="text-sm font-medium">
                Editor
              </Label>
              <div className="flex items-center gap-2">
                <Button variant="ghost" size="sm" onClick={copySql}>
                  <Copy className="h-4 w-4 mr-2" />
                  Copy
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={runSql}
                  disabled={isRunning || sql.trim().length === 0}
                >
                  <Play className="h-4 w-4 mr-2" />
                  Run
                </Button>
              </div>
            </div>
            <Textarea
              ref={editorRef}
              id="sql-editor"
              value={sql}
              onChange={(event) => setSql(event.target.value)}
              onKeyDown={(event) => {
                if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
                  event.preventDefault();
                  void runSql();
                }
              }}
              spellCheck={false}
              className="h-full min-h-[260px] resize-none rounded-none border-0 font-mono text-sm leading-6 shadow-none focus-visible:ring-0"
            />
          </section>

          <section className="min-h-0 rounded-md border bg-background">
            <Tabs defaultValue="results" className="flex h-full flex-col">
              <div className="flex items-center justify-between border-b px-3 py-2">
                <TabsList>
                  <TabsTrigger value="results">Results</TabsTrigger>
                  <TabsTrigger value="messages">Messages</TabsTrigger>
                  <TabsTrigger value="json">JSON</TabsTrigger>
                  <TabsTrigger value="history">History</TabsTrigger>
                </TabsList>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={copyResponse}
                  disabled={!response && !error}
                >
                  <Copy className="h-4 w-4 mr-2" />
                  Copy
                </Button>
              </div>

              <TabsContent value="results" className="min-h-0 flex-1 overflow-auto p-0">
                {tableResult && tableResult.columns.length > 0 ? (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        {tableResult.columns.map((column) => (
                          <TableHead key={column}>{column}</TableHead>
                        ))}
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {tableResult.rows.map((row, index) => (
                        <TableRow key={resultRowKey(row, index)}>
                          {tableResult.columns.map((column) => (
                            <TableCell key={column} className="max-w-80 truncate font-mono text-xs">
                              {stringifyValue(row[column])}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                ) : tableResult && tableResult.rows.length === 0 ? (
                  <div className="p-6 text-sm text-muted-foreground">
                    The statement returned no rows.
                  </div>
                ) : response?.applied ? (
                  <pre className="h-full overflow-auto p-4 text-xs">
                    {JSON.stringify(response.applied, null, 2)}
                  </pre>
                ) : (
                  <div className="p-6 text-sm text-muted-foreground">
                    Run a statement to see results.
                  </div>
                )}
              </TabsContent>

              <TabsContent value="messages" className="min-h-0 flex-1 overflow-auto p-4">
                <div className="space-y-2 text-sm">
                  {error && <div className="text-destructive">{error}</div>}
                  {response && (
                    <>
                      <div>Statement completed as {response.kind}.</div>
                      <div>Session id: {response.session_id}</div>
                      {response.statement_kind && (
                        <div>Statement kind: {response.statement_kind}</div>
                      )}
                      {response.noop !== undefined && <div>No-op: {String(response.noop)}</div>}
                      {lastDurationMs !== null && <div>Duration: {lastDurationMs} ms</div>}
                    </>
                  )}
                  {!error && !response && (
                    <div className="text-muted-foreground">No messages yet.</div>
                  )}
                </div>
              </TabsContent>

              <TabsContent value="json" className="min-h-0 flex-1 overflow-auto">
                <pre className="p-4 text-xs">
                  {JSON.stringify(response ?? (error ? { error } : null), null, 2)}
                </pre>
              </TabsContent>

              <TabsContent value="history" className="min-h-0 flex-1 overflow-auto p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Status</TableHead>
                      <TableHead>Duration</TableHead>
                      <TableHead>Session</TableHead>
                      <TableHead>Statement</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {history.map((entry) => (
                      <TableRow
                        key={entry.id}
                        className="cursor-pointer"
                        onClick={() => setSql(entry.sql)}
                      >
                        <TableCell>
                          <Badge>{entry.kind}</Badge>
                        </TableCell>
                        <TableCell>{entry.durationMs} ms</TableCell>
                        <TableCell>{entry.sessionId ?? ""}</TableCell>
                        <TableCell className="max-w-[42rem] truncate font-mono text-xs">
                          {entry.sql}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TabsContent>
            </Tabs>
          </section>
        </main>
      </div>
    </DashboardPage>
  );
}
