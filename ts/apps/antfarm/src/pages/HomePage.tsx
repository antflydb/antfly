import {
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  DashboardPage,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  GraphPaperBg,
} from "@antfly/design-system";
import type { IndexStatus, TableStatus } from "@antfly/sdk";
import { ArrowRight, Database, Gauge, Search, Sparkles, Waypoints } from "lucide-react";
import type React from "react";
import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useApi } from "@/hooks/use-api-config";

interface HomeIndexSummary {
  table: string;
  indexes: IndexStatus[];
}

const normalizeTablesResponse = (response: unknown): TableStatus[] => {
  if (Array.isArray(response)) return response as TableStatus[];
  if (
    response &&
    typeof response === "object" &&
    "tables" in response &&
    Array.isArray((response as { tables?: unknown }).tables)
  ) {
    return (response as { tables: TableStatus[] }).tables;
  }
  return [];
};

const HomePage: React.FC = () => {
  const api = useApi();
  const navigate = useNavigate();
  const [tables, setTables] = useState<TableStatus[]>([]);
  const [indexSummaries, setIndexSummaries] = useState<HomeIndexSummary[]>([]);
  const [status, setStatus] = useState<{ swarm_mode?: boolean; auth_enabled?: boolean } | null>(
    null
  );

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      const tableList = normalizeTablesResponse(await api.tables.list());
      if (cancelled) return;
      setTables(tableList);

      const summaries = await Promise.all(
        tableList.slice(0, 5).map(async (table) => {
          try {
            return {
              table: table.name,
              indexes: ((await api.indexes.list(table.name)) ?? []) as IndexStatus[],
            };
          } catch {
            return { table: table.name, indexes: [] };
          }
        })
      );
      if (!cancelled) setIndexSummaries(summaries);

      try {
        const response = await fetch("/db/v1/status");
        if (response.ok && !cancelled) setStatus(await response.json());
      } catch {
        if (!cancelled) setStatus(null);
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [api]);

  const vectorIndexCount = useMemo(
    () =>
      indexSummaries.reduce(
        (count, summary) =>
          count + summary.indexes.filter((index) => index.config.type === "embeddings").length,
        0
      ),
    [indexSummaries]
  );

  const emptyTables = tables.filter((table) => table.storage_status?.empty);
  const suggestions = [
    tables.length === 0 && {
      title: "Load a sample",
      description: "Start with the bundled sample dataset and try search immediately.",
      action: "Open tables",
      icon: Database,
      onClick: () => navigate("/tables"),
    },
    tables.length > 0 &&
      vectorIndexCount === 0 && {
        title: "Add semantic search",
        description: "None of your first tables have vector indexes yet.",
        action: "Review indexes",
        icon: Waypoints,
        onClick: () => navigate(`/tables/${tables[0].name}`),
      },
    emptyTables.length > 0 && {
      title: "Populate empty tables",
      description: `${emptyTables.length} table${emptyTables.length === 1 ? "" : "s"} have no stored documents yet.`,
      action: "Upload data",
      icon: Search,
      onClick: () => navigate(`/tables/${emptyTables[0].name}`),
    },
    {
      title: "Plan setup",
      description: "Draft additive instance changes in Agent Lab.",
      action: "Open Agent Lab",
      icon: Sparkles,
      onClick: () => navigate("/lab/agent"),
    },
  ].filter(Boolean) as Array<{
    title: string;
    description: string;
    action: string;
    icon: React.ComponentType<{ className?: string }>;
    onClick: () => void;
  }>;

  return (
    <DashboardPage>
      <div className="relative isolate">
        <GraphPaperBg className="absolute inset-0 -z-10 rounded-none" />
        <DashboardPageHeader>
          <div>
            <DashboardPageTitle className="font-aeonik">Home</DashboardPageTitle>
            <DashboardPageDescription>
              Cluster health, data coverage, and next useful actions.
            </DashboardPageDescription>
          </div>
        </DashboardPageHeader>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Database className="size-4" />
              Tables
            </CardTitle>
          </CardHeader>
          <CardContent className="text-3xl font-semibold">{tables.length}</CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Waypoints className="size-4" />
              Vector Indexes
            </CardTitle>
          </CardHeader>
          <CardContent className="text-3xl font-semibold">{vectorIndexCount}</CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Gauge className="size-4" />
              Cluster
            </CardTitle>
          </CardHeader>
          <CardContent className="flex gap-2">
            <Badge variant="outline">{status?.swarm_mode ? "Swarm" : "Connected"}</Badge>
            <Badge variant="outline">{status?.auth_enabled ? "Auth on" : "Auth off"}</Badge>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {suggestions.map((suggestion) => {
          const Icon = suggestion.icon;
          return (
            <Card key={suggestion.title}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Icon className="size-4" />
                  {suggestion.title}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <p className="text-muted-foreground text-sm">{suggestion.description}</p>
                <Button variant="outline" onClick={suggestion.onClick}>
                  {suggestion.action}
                  <ArrowRight className="ml-2 size-4" />
                </Button>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </DashboardPage>
  );
};

export default HomePage;
