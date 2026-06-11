import { Alert, AlertDescription, Button, Textarea } from "@antfly/design-system";
import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useApi } from "@/hooks/use-api-config";
import { useTable } from "@/hooks/use-table";
import ChatPlaygroundPage from "../ChatPlaygroundPage";
import EvalsPlaygroundPage from "../EvalsPlaygroundPage";
import RagPlaygroundPage from "../RagPlaygroundPage";
import { LabSection } from "./LabPage";

interface ProposedPlan {
  summary?: string;
  ops?: Array<{
    op: string;
    summary?: string;
    params?: Record<string, unknown>;
  }>;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

function AntyPlanLab() {
  const api = useApi();
  const navigate = useNavigate();
  const { refreshTables, selectedTable, setSelectedTable } = useTable();
  const [intent, setIntent] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [plan, setPlan] = useState<ProposedPlan | null>(null);
  const [applying, setApplying] = useState(false);

  const createPlan = useCallback(async () => {
    const trimmed = intent.trim();
    if (!trimmed) return;

    try {
      const response = await fetch("/db/v1/agents/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ intent: trimmed, table_context: selectedTable || undefined }),
      });
      if (!response.ok) {
        const body = await response.text();
        throw new Error(body || `Config agent returned HTTP ${response.status}`);
      }
      setPlan((await response.json()) as ProposedPlan);
      setMessage(null);
    } catch (err) {
      setPlan(null);
      setMessage(err instanceof Error ? err.message : "Anty is not available.");
    }
  }, [intent, selectedTable]);

  const applyPlan = useCallback(async () => {
    if (!plan?.ops?.length) return;

    setApplying(true);
    try {
      let lastTableName: string | null = null;

      for (const op of plan.ops) {
        const params = op.params ?? {};
        if (op.op === "create_table") {
          const tableName = typeof params.table_name === "string" ? params.table_name : "";
          if (!tableName || !isRecord(params.request)) {
            throw new Error("create_table operation is missing table_name or request.");
          }
          await api.tables.create(
            tableName,
            params.request as Parameters<typeof api.tables.create>[1]
          );
          lastTableName = tableName;
          continue;
        }

        if (op.op === "create_index") {
          const tableName = typeof params.table_name === "string" ? params.table_name : "";
          if (!tableName || !isRecord(params.config)) {
            throw new Error("create_index operation is missing table_name or config.");
          }
          await api.indexes.create(
            tableName,
            params.config as Parameters<typeof api.indexes.create>[1]
          );
          lastTableName = tableName;
          continue;
        }

        throw new Error(`Unsupported config operation: ${op.op}`);
      }

      await refreshTables();
      if (lastTableName) {
        setSelectedTable(lastTableName);
        navigate(`/tables/${lastTableName}`);
      }
      setMessage("Plan applied.");
    } catch (err) {
      setMessage(err instanceof Error ? err.message : "Failed to apply plan.");
    } finally {
      setApplying(false);
    }
  }, [api, navigate, plan, refreshTables, setSelectedTable]);

  return (
    <div className="space-y-4 rounded-md border p-4">
      <div>
        <h3 className="font-semibold text-base">Anty setup planner</h3>
        <p className="text-muted-foreground text-sm">
          Draft additive setup operations from an intent.
        </p>
      </div>
      <Textarea
        value={intent}
        onChange={(event) => setIntent(event.target.value)}
        placeholder="Create a table for support tickets"
        className="min-h-28 resize-none"
      />
      <Button onClick={createPlan} disabled={!intent.trim()}>
        Run
      </Button>
      {message && (
        <Alert>
          <AlertDescription>{message}</AlertDescription>
        </Alert>
      )}
      {plan && (
        <div className="space-y-3 rounded-md bg-muted/40 p-4">
          <h4 className="font-medium text-sm">{plan.summary || "Proposed plan"}</h4>
          {(plan.ops ?? []).map((op) => (
            <div key={`${op.op}-${op.summary || ""}-${JSON.stringify(op.params)}`}>
              <div className="text-sm">{op.summary || op.op}</div>
              <pre className="mt-2 max-h-36 overflow-auto text-xs">
                {JSON.stringify(op.params, null, 2)}
              </pre>
            </div>
          ))}
          <Button onClick={applyPlan} disabled={applying || !plan.ops?.length}>
            {applying ? "Applying..." : "Apply plan"}
          </Button>
        </div>
      )}
    </div>
  );
}

export default function AgentLabPage() {
  return (
    <LabSection
      title="Agent Lab"
      description="Inspect config plans, retrieval answers, chat runs, and eval workflows."
      promote={
        <p className="text-muted-foreground text-sm">
          Use Anty setup planner to draft additive changes, then apply supported table and index
          operations.
        </p>
      }
    >
      <AntyPlanLab />
      <RagPlaygroundPage />
      <ChatPlaygroundPage />
      <EvalsPlaygroundPage />
    </LabSection>
  );
}
