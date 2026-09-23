import {
  Button,
  DashboardPage,
  DashboardPageHeader,
  DashboardPageTitle,
  Input,
  Label,
  Textarea,
} from "@antfly/design-system";
import { SQLExecutionError, type SQLResponse } from "@antfly/sdk";
import { useEffect, useRef, useState } from "react";
import { useApi } from "@/hooks/use-api-config";
import { sqlCell, sqlParameters } from "./sql-workbench";

export default function SQLWorkbenchPage() {
  const client = useApi();
  const [statement, setStatement] = useState("SELECT 1");
  const [parameters, setParameters] = useState("[]");
  const [database, setDatabase] = useState("default");
  const [namespace, setNamespace] = useState("public");
  const [session, setSession] = useState<string>();
  const [result, setResult] = useState<SQLResponse>();
  const [diagnostic, setDiagnostic] = useState("");
  const [busy, setBusy] = useState(false);
  const [uncertain, setUncertain] = useState(false);
  const request = useRef<AbortController | null>(null);
  useEffect(
    () => () => {
      request.current?.abort();
      request.current = null;
    },
    []
  );
  // A session belongs to its authenticated connection, never a new endpoint.
  // biome-ignore lint/correctness/useExhaustiveDependencies: changing the authenticated client must clear its session.
  useEffect(() => {
    request.current?.abort();
    request.current = null;
    setBusy(false);
    setSession(undefined);
    setResult(undefined);
    setDiagnostic("");
    setUncertain(false);
  }, [client]);

  async function execute(sql: string, editorParameters = false) {
    if (request.current || uncertain) return;
    let values: unknown[];
    try {
      values = editorParameters ? sqlParameters(parameters) : [];
    } catch (error) {
      setDiagnostic(String(error));
      return;
    }
    const controller = new AbortController();
    request.current = controller;
    setBusy(true);
    setDiagnostic("");
    setResult(undefined);
    try {
      const response = await client.executeSQL(
        {
          statement: sql,
          parameters: values,
          database,
          namespace,
          session_id: session,
          limit: 128,
        },
        { signal: controller.signal }
      );
      if (request.current !== controller) return;
      setResult(response);
      setSession(response.session_id);
    } catch (error) {
      if (request.current !== controller) return;
      if (error instanceof SQLExecutionError) {
        if (error.diagnostic.transaction_status === "idle") setSession(undefined);
        setDiagnostic(
          `${error.diagnostic.code}: ${error.diagnostic.message}${error.diagnostic.transaction_id ? `\nTransaction receipt: ${error.diagnostic.transaction_id}` : ""}`
        );
        setUncertain(error.diagnostic.code === "40003");
      } else {
        setDiagnostic(
          `${String(error)}\nThe response was not confirmed. Reconcile any mutation or transaction before continuing; do not replay it.`
        );
        setUncertain(true);
      }
    } finally {
      if (request.current === controller) {
        request.current = null;
        setBusy(false);
      }
    }
  }

  return (
    <DashboardPage>
      <DashboardPageHeader>
        <DashboardPageTitle>SQL workbench</DashboardPageTitle>
      </DashboardPageHeader>
      <p className="text-sm text-muted-foreground">
        One statement per execution. Results are capped at 128 rows without silently truncating.
        Integers remain exact; NULL and JSON null are distinct. Nothing is retried automatically.
      </p>
      <div className="flex gap-4">
        <div>
          <Label htmlFor="sql-database">Database</Label>
          <Input
            id="sql-database"
            value={database}
            disabled={busy || !!session}
            onChange={(e) => setDatabase(e.target.value)}
          />
        </div>
        <div>
          <Label htmlFor="sql-namespace">Namespace</Label>
          <Input
            id="sql-namespace"
            value={namespace}
            disabled={busy || !!session}
            onChange={(e) => setNamespace(e.target.value)}
          />
        </div>
      </div>
      <Label htmlFor="sql-statement">Statement</Label>
      <Textarea
        id="sql-statement"
        className="min-h-40 font-mono"
        value={statement}
        onChange={(e) => setStatement(e.target.value)}
        spellCheck={false}
      />
      <Label htmlFor="sql-parameters">
        Positional parameters (JSON array; quote large integers)
      </Label>
      <Textarea
        id="sql-parameters"
        className="font-mono"
        value={parameters}
        onChange={(e) => setParameters(e.target.value)}
        spellCheck={false}
      />
      <div className="flex gap-2">
        <Button
          disabled={busy || uncertain || !statement.trim()}
          onClick={() => void execute(statement, true)}
        >
          Run statement
        </Button>
        {!session && (
          <Button
            variant="outline"
            disabled={busy || uncertain}
            onClick={() => void execute("BEGIN ISOLATION LEVEL READ COMMITTED")}
          >
            Begin transaction
          </Button>
        )}
        {session && (
          <Button
            variant="outline"
            disabled={busy || uncertain}
            onClick={() => void execute("COMMIT")}
          >
            Commit session
          </Button>
        )}
        {busy && (
          <Button variant="outline" onClick={() => request.current?.abort()}>
            Cancel request
          </Button>
        )}
        {session && (
          <Button
            variant="outline"
            disabled={busy || uncertain}
            onClick={() => void execute("ROLLBACK")}
          >
            Roll back session
          </Button>
        )}
      </div>
      {session && (
        <p className="text-sm">
          Session: <code>{session}</code>. Roll back before leaving; closing this page does not
          commit.
        </p>
      )}
      {diagnostic && (
        <pre role="alert" className="whitespace-pre-wrap rounded border p-3">
          {diagnostic}
        </pre>
      )}
      {uncertain && (
        <Button variant="outline" onClick={() => setUncertain(false)}>
          I have reconciled the outcome; enable new statements
        </Button>
      )}
      {result && (
        <>
          <p role="status">
            {result.command_tag} · {result.rows.length} rows · {result.rows_affected} affected ·{" "}
            {result.transaction_status ?? "idle"}
            {result.mutation_outcome ? ` · ${result.mutation_outcome}` : ""}
          </p>
          {result.transaction_id && (
            <p>
              Transaction receipt: <code>{result.transaction_id}</code>
            </p>
          )}
          {result.ddl_receipt && (
            <pre className="whitespace-pre-wrap">{JSON.stringify(result.ddl_receipt, null, 2)}</pre>
          )}
          <div className="overflow-auto">
            <table className="w-full text-left text-sm">
              <thead>
                <tr>
                  {result.columns.map((column, index) => (
                    // biome-ignore lint/suspicious/noArrayIndexKey: positional columns may have duplicate names and never reorder within a result.
                    <th className="border p-2" key={`${index}:${column.name}`}>
                      {column.name}
                      <span className="ml-2 text-muted-foreground">{column.type}</span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.rows.map((row, ri) => (
                  <tr key={JSON.stringify([ri, row])}>
                    {row.map((_, ci) => (
                      // biome-ignore lint/suspicious/noArrayIndexKey: immutable SQL cells have positional identity, not unique values.
                      <td className="border p-2 font-mono whitespace-pre-wrap" key={`${ri}:${ci}`}>
                        {sqlCell(result, ri, ci)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </DashboardPage>
  );
}
