import {
  Alert,
  AlertDescription,
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Checkbox,
  DashboardToolbar,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  StatCard,
  Tabs,
  TabsList,
  TabsTrigger,
} from "@antfly/design-system";
import { ForceGraph, type GraphData, type GraphEdge, type GraphNode } from "@antfly/graph";
import type {
  Edge,
  EdgeDirection,
  EdgeTypeConfig,
  GraphQuery,
  GraphQueryResult,
  IndexStatus,
  PathWeightMode,
  QueryRequest,
} from "@antfly/sdk";
import { GitBranch, Hash, Loader2, Network, RefreshCw, Route, Search } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useApi } from "@/hooks/use-api-config";
import JsonViewer from "./JsonViewer";

type GraphMode = "neighbors" | "traverse" | "shortest_path";

type ExplorerNodeMeta = {
  key: string;
  depth?: number;
  distance?: number;
  document?: Record<string, unknown>;
  path?: string[];
  resultCount?: number;
};

type ExplorerEdge = GraphEdge & {
  id: string;
  type?: string;
  metadata?: Record<string, unknown>;
  pathEdge?: boolean;
};

type ExplorerGraph = Omit<GraphData<ExplorerNodeMeta>, "edges"> & {
  edges: ExplorerEdge[];
};

type GraphIndexStatus = IndexStatus & {
  config: IndexStatus["config"] & {
    edge_types?: EdgeTypeConfig[];
    max_edges_per_document?: number;
    ttl_duration?: string;
    summarizer?: unknown;
    template?: string;
  };
  status?: IndexStatus["status"] & {
    total_edges?: number;
    edge_types?: Record<string, number>;
    backfill_progress?: number;
    rebuilding?: boolean;
    algebraic_graph?: {
      traversal?: {
        attempted?: number;
        proven?: number;
        rejected?: number;
        fallback?: number;
        result_nodes?: number;
      };
    };
  };
};

const MAX_LABEL_LENGTH = 28;

function displayKey(key: string) {
  return key.length > MAX_LABEL_LENGTH ? `${key.slice(0, 12)}...${key.slice(-10)}` : key;
}

function documentLabel(key: string, document?: Record<string, unknown>) {
  if (!document) return displayKey(key);
  for (const field of ["title", "name", "label", "text", "content"]) {
    const value = document[field];
    if (typeof value === "string" && value.trim()) {
      return value.length > MAX_LABEL_LENGTH ? `${value.slice(0, MAX_LABEL_LENGTH - 1)}...` : value;
    }
  }
  return displayKey(key);
}

function formatNumber(value: number | undefined, fallback = "-") {
  if (value === undefined || Number.isNaN(value)) return fallback;
  return Intl.NumberFormat(undefined, { maximumFractionDigits: 3 }).format(value);
}

function graphIndexesOnly(indexes: IndexStatus[]): GraphIndexStatus[] {
  return indexes.filter((index) => index.config.type === "graph") as GraphIndexStatus[];
}

function edgeTypesForIndex(index?: GraphIndexStatus) {
  return index?.config.edge_types?.map((edgeType) => edgeType.name).filter(Boolean) ?? [];
}

function controlId(prefix: string, value: string) {
  return `${prefix}-${value.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
}

function normalizeEdge(
  edge: Partial<Edge>,
  fallbackIndex: number,
  pathEdge = false
): ExplorerEdge | null {
  const source = edge.source;
  const target = edge.target;
  if (!source || !target) return null;
  return {
    id: `${source}->${target}:${edge.type ?? "edge"}:${fallbackIndex}`,
    source,
    target,
    weight: edge.weight ?? 1,
    type: edge.type,
    metadata: edge.metadata,
    pathEdge,
  };
}

function addNode(
  nodes: Map<string, GraphNode<ExplorerNodeMeta>>,
  key: string,
  patch?: Partial<ExplorerNodeMeta>
) {
  const existing = nodes.get(key);
  const metadata = { ...(existing?.metadata ?? { key }), key, ...patch };
  nodes.set(key, {
    id: key,
    label: documentLabel(key, metadata.document),
    type: metadata.depth === 0 ? "start" : "document",
    metric: metadata.resultCount ?? (metadata.depth !== undefined ? 1 / (metadata.depth + 1) : 1),
    metadata,
  });
}

function buildGraph(result: GraphQueryResult | null, startKey: string): ExplorerGraph {
  const nodes = new Map<string, GraphNode<ExplorerNodeMeta>>();
  const edges = new Map<string, ExplorerEdge>();

  if (startKey) addNode(nodes, startKey, { depth: 0 });

  for (const node of result?.nodes ?? []) {
    if (!node.key) continue;
    addNode(nodes, node.key, {
      depth: node.depth,
      distance: node.distance,
      document: node.document as Record<string, unknown> | undefined,
      path: node.path,
      resultCount: 1,
    });

    for (const edge of node.path_edges ?? []) {
      const normalized = normalizeEdge(edge, edges.size, true);
      if (normalized) {
        edges.set(normalized.id, normalized);
        addNode(nodes, normalized.source);
        addNode(nodes, normalized.target);
      }
    }

    for (const edge of node.edges ?? []) {
      const normalized = normalizeEdge(edge, edges.size);
      if (normalized) {
        edges.set(normalized.id, normalized);
        addNode(nodes, normalized.source);
        addNode(nodes, normalized.target);
      }
    }
  }

  for (const path of result?.paths ?? []) {
    for (const key of path.nodes ?? []) addNode(nodes, key, { resultCount: 1 });
    for (const edge of path.edges ?? []) {
      if (!edge.source || !edge.target) continue;
      const normalized: ExplorerEdge = {
        id: `${edge.source}->${edge.target}:${edge.type ?? "path"}:${edges.size}`,
        source: edge.source,
        target: edge.target,
        weight: edge.weight ?? 1,
        type: edge.type,
        metadata: edge.metadata,
        pathEdge: true,
      };
      edges.set(normalized.id, normalized);
      addNode(nodes, normalized.source);
      addNode(nodes, normalized.target);
    }
  }

  return {
    nodes: Array.from(nodes.values()),
    edges: Array.from(edges.values()),
  };
}

function nodeTypeColors() {
  return {
    start: { label: "Start", color: "var(--chart-1)" },
    document: { label: "Document", color: "var(--chart-6)" },
  };
}

function resultSummary(result: GraphQueryResult | null) {
  if (!result) return { total: 0, paths: 0 };
  return {
    total: result.nodes?.length ?? result.paths?.length ?? result.rows?.length ?? 0,
    paths: result.paths?.length ?? 0,
  };
}

function coerceInteger(value: string, fallback: number, min: number, max: number) {
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function coerceNumber(value: string, fallback: number, min: number) {
  const parsed = Number.parseFloat(value);
  if (Number.isNaN(parsed)) return fallback;
  return Math.max(min, parsed);
}

export function GraphIndexExplorer({
  tableName,
  indexes,
  onRefreshIndexes,
  initialMode = "traverse",
  initialStartKey = "",
  initialTargetKey = "",
  initialResult = null,
}: {
  tableName: string;
  indexes: IndexStatus[];
  onRefreshIndexes: () => void;
  initialMode?: GraphMode;
  initialStartKey?: string;
  initialTargetKey?: string;
  initialResult?: GraphQueryResult | null;
}) {
  const api = useApi();
  const graphIndexes = useMemo(() => graphIndexesOnly(indexes), [indexes]);
  const [selectedIndexName, setSelectedIndexName] = useState("");
  const selectedIndex = graphIndexes.find((index) => index.config.name === selectedIndexName);
  const availableEdgeTypes = useMemo(() => edgeTypesForIndex(selectedIndex), [selectedIndex]);
  const [selectedEdgeTypes, setSelectedEdgeTypes] = useState<string[]>([]);
  const [mode, setMode] = useState<GraphMode>(initialMode);
  const [direction, setDirection] = useState<EdgeDirection>("out");
  const [weightMode, setWeightMode] = useState<PathWeightMode>("min_hops");
  const [startKey, setStartKey] = useState(initialStartKey);
  const [targetKey, setTargetKey] = useState(initialTargetKey);
  const [maxDepth, setMaxDepth] = useState(2);
  const [maxResults, setMaxResults] = useState(100);
  const [minWeight, setMinWeight] = useState(0);
  const [includePaths, setIncludePaths] = useState(true);
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingKeys, setIsLoadingKeys] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<GraphQueryResult | null>(initialResult);
  const [sampleKeys, setSampleKeys] = useState<string[]>([]);
  const [selectedNode, setSelectedNode] = useState<GraphNode<ExplorerNodeMeta> | null>(null);

  useEffect(() => {
    if (
      graphIndexes.length > 0 &&
      !graphIndexes.some((index) => index.config.name === selectedIndexName)
    ) {
      setSelectedIndexName(graphIndexes[0].config.name);
    }
  }, [graphIndexes, selectedIndexName]);

  useEffect(() => {
    setSelectedEdgeTypes((current) =>
      current.filter((edgeType) => availableEdgeTypes.includes(edgeType))
    );
  }, [availableEdgeTypes]);

  const graph = useMemo(() => buildGraph(result, startKey.trim()), [result, startKey]);
  const summary = resultSummary(result);

  const loadSampleKeys = useCallback(async () => {
    if (!tableName) return;
    setIsLoadingKeys(true);
    setError(null);
    try {
      const response = await api.tables.query(tableName, {
        filter_query: { match_all: {} },
        limit: 20,
      } as QueryRequest);
      const keys =
        response?.responses?.[0]?.hits?.hits?.map((hit) => hit._id).filter(Boolean) ?? [];
      setSampleKeys(keys);
      if (!startKey && keys[0]) setStartKey(keys[0]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load sample keys");
    } finally {
      setIsLoadingKeys(false);
    }
  }, [api, startKey, tableName]);

  const runGraphQuery = useCallback(async () => {
    if (!selectedIndex || !startKey.trim()) {
      setError("Choose a graph index and start key.");
      return;
    }
    if (mode === "shortest_path" && !targetKey.trim()) {
      setError("Shortest path needs a target key.");
      return;
    }

    setIsLoading(true);
    setError(null);
    setSelectedNode(null);

    const graphQuery: GraphQuery =
      mode === "shortest_path"
        ? {
            index: selectedIndex.config.name,
            shortest_path: {
              from: { key: startKey.trim() },
              to: { key: targetKey.trim() },
              edge_types: selectedEdgeTypes.length > 0 ? selectedEdgeTypes : undefined,
              direction,
              max_depth: maxDepth,
              min_weight: minWeight > 0 ? minWeight : undefined,
              weight_mode: weightMode,
              include_documents: true,
            },
          }
        : {
            index: selectedIndex.config.name,
            traverse: {
              start: { keys: [startKey.trim()] },
              edge_types: selectedEdgeTypes.length > 0 ? selectedEdgeTypes : undefined,
              direction,
              max_depth: mode === "neighbors" ? 1 : maxDepth,
              limit: maxResults,
              min_weight: minWeight > 0 ? minWeight : undefined,
              include_paths: includePaths,
              include_documents: true,
              deduplicate_nodes: true,
            },
          };

    try {
      const response = await api.tables.query(tableName, {
        graph_queries: { explorer: graphQuery },
        limit: 10,
      } as QueryRequest);
      const graphResult = response?.responses?.[0]?.graph_results?.explorer ?? null;
      setResult(graphResult);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Graph query failed");
    } finally {
      setIsLoading(false);
    }
  }, [
    api,
    direction,
    includePaths,
    maxDepth,
    maxResults,
    minWeight,
    mode,
    selectedEdgeTypes,
    selectedIndex,
    startKey,
    tableName,
    targetKey,
    weightMode,
  ]);

  const toggleEdgeType = (edgeType: string, checked: boolean) => {
    setSelectedEdgeTypes((current) =>
      checked ? [...new Set([...current, edgeType])] : current.filter((item) => item !== edgeType)
    );
  };

  if (graphIndexes.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Graph Explorer</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          This table does not have a graph index yet.
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      <DashboardToolbar className="items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <Network className="size-4 text-muted-foreground" />
          <h2 className="font-display text-xl tracking-tight">Graph Explorer</h2>
          {selectedIndex?.status?.rebuilding && (
            <Badge className="af-status-badge-warning">Rebuilding</Badge>
          )}
        </div>
        <Button variant="outline" size="sm" onClick={onRefreshIndexes}>
          <RefreshCw className="size-3.5" />
          Refresh
        </Button>
      </DashboardToolbar>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      <div className="grid gap-3 md:grid-cols-4">
        <StatCard
          label="Total edges"
          value={formatNumber(selectedIndex?.status?.total_edges)}
          icon={<GitBranch className="size-4" />}
        />
        <StatCard
          label="Edge types"
          value={availableEdgeTypes.length}
          icon={<Network className="size-4" />}
        />
        <StatCard
          label="Result nodes"
          value={formatNumber(summary.total)}
          icon={<Hash className="size-4" />}
        />
        <StatCard
          label="Paths"
          value={formatNumber(summary.paths)}
          icon={<Route className="size-4" />}
        />
      </div>

      <div className="grid gap-6 xl:grid-cols-[320px_minmax(0,1fr)_320px]">
        <Card className="h-fit">
          <CardHeader>
            <CardTitle className="text-lg">Query</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label>Graph index</Label>
              <Select value={selectedIndexName} onValueChange={setSelectedIndexName}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {graphIndexes.map((index) => (
                    <SelectItem key={index.config.name} value={index.config.name}>
                      {index.config.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <Tabs value={mode} onValueChange={(value) => setMode(value as GraphMode)}>
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="neighbors">Neighbors</TabsTrigger>
                <TabsTrigger value="traverse">Traverse</TabsTrigger>
                <TabsTrigger value="shortest_path">Path</TabsTrigger>
              </TabsList>
            </Tabs>

            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2">
                <Label htmlFor="graph-start-key">Start key</Label>
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={loadSampleKeys}
                  disabled={isLoadingKeys}
                  className="shrink-0"
                >
                  {isLoadingKeys ? (
                    <Loader2 className="size-3.5 animate-spin" />
                  ) : (
                    <Search className="size-3.5" />
                  )}
                  Load keys
                </Button>
              </div>
              <Input
                id="graph-start-key"
                value={startKey}
                onChange={(event) => setStartKey(event.target.value)}
                placeholder="doc-key"
              />
              {sampleKeys.length > 0 && (
                <div className="flex flex-wrap gap-1">
                  {sampleKeys.slice(0, 5).map((key) => (
                    <Button
                      key={key}
                      variant="outline"
                      size="xs"
                      className="max-w-full"
                      onClick={() => setStartKey(key)}
                    >
                      {displayKey(key)}
                    </Button>
                  ))}
                </div>
              )}
            </div>

            {mode === "shortest_path" && (
              <div className="space-y-2">
                <Label htmlFor="graph-target-key">Target key</Label>
                <Input
                  id="graph-target-key"
                  value={targetKey}
                  onChange={(event) => setTargetKey(event.target.value)}
                  placeholder="target-key"
                />
              </div>
            )}

            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <Label>Direction</Label>
                <Select
                  value={direction}
                  onValueChange={(value) => setDirection(value as EdgeDirection)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="out">Out</SelectItem>
                    <SelectItem value="in">In</SelectItem>
                    <SelectItem value="both">Both</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="graph-max-depth">Depth</Label>
                <Input
                  id="graph-max-depth"
                  type="number"
                  min={1}
                  max={12}
                  value={maxDepth}
                  onChange={(event) => setMaxDepth(coerceInteger(event.target.value, 2, 1, 12))}
                />
              </div>
            </div>

            {mode === "shortest_path" ? (
              <div className="space-y-2">
                <Label>Path score</Label>
                <Select
                  value={weightMode}
                  onValueChange={(value) => setWeightMode(value as PathWeightMode)}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="min_hops">Min hops</SelectItem>
                    <SelectItem value="min_weight">Min weight</SelectItem>
                    <SelectItem value="max_weight">Max weight</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            ) : (
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label htmlFor="graph-max-results">Limit</Label>
                  <Input
                    id="graph-max-results"
                    type="number"
                    min={1}
                    max={500}
                    value={maxResults}
                    onChange={(event) =>
                      setMaxResults(coerceInteger(event.target.value, 100, 1, 500))
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="graph-min-weight">Min weight</Label>
                  <Input
                    id="graph-min-weight"
                    type="number"
                    min={0}
                    step={0.05}
                    value={minWeight}
                    onChange={(event) => setMinWeight(coerceNumber(event.target.value, 0, 0))}
                  />
                </div>
              </div>
            )}

            {availableEdgeTypes.length > 0 && (
              <div className="space-y-2">
                <Label>Edge types</Label>
                <div className="grid gap-2 rounded-none border-(length:--border-width) border-border-strong bg-muted/30 p-2">
                  {availableEdgeTypes.map((edgeType) => {
                    const checkboxId = controlId("graph-edge-type", edgeType);
                    return (
                      <div key={edgeType} className="flex items-center gap-2 text-sm">
                        <Checkbox
                          id={checkboxId}
                          checked={selectedEdgeTypes.includes(edgeType)}
                          onCheckedChange={(checked) => toggleEdgeType(edgeType, checked === true)}
                        />
                        <Label htmlFor={checkboxId} className="font-normal">
                          {edgeType}
                        </Label>
                        <Badge className="ml-auto">
                          {formatNumber(selectedIndex?.status?.edge_types?.[edgeType], "0")}
                        </Badge>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            <div className="flex items-center gap-2 text-sm">
              <Checkbox
                id="graph-include-paths"
                checked={includePaths}
                onCheckedChange={(checked) => setIncludePaths(checked === true)}
              />
              <Label htmlFor="graph-include-paths" className="font-normal">
                Include paths
              </Label>
            </div>

            <Button className="w-full" onClick={runGraphQuery} disabled={isLoading}>
              {isLoading ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                <GitBranch className="size-4" />
              )}
              Run graph query
            </Button>
          </CardContent>
        </Card>

        <Card className="min-h-[620px] overflow-hidden">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2 text-lg">
              <Route className="size-4" />
              {selectedIndexName || "Graph"}
            </CardTitle>
            <div className="flex gap-2">
              <Badge>{graph.nodes.length} nodes</Badge>
              <Badge>{graph.edges.length} edges</Badge>
            </div>
          </CardHeader>
          <CardContent className="h-[560px] p-0">
            <ForceGraph
              data={graph}
              colorConfig={nodeTypeColors()}
              minHeight={560}
              className="border-0"
              showLegend
              showMinimap
              showSearch
              onNodeClick={setSelectedNode}
              renderTooltip={(node) => (
                <div className="space-y-1">
                  <div className="font-medium">{node.label}</div>
                  <div className="text-xs text-muted-foreground">{node.metadata?.key}</div>
                  {node.metadata?.depth !== undefined && (
                    <div className="text-xs">Depth {node.metadata.depth}</div>
                  )}
                </div>
              )}
            />
          </CardContent>
        </Card>

        <Card className="h-fit">
          <CardHeader>
            <CardTitle className="text-lg">Inspector</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {selectedNode ? (
              <>
                <div>
                  <div className="text-xs text-muted-foreground">Key</div>
                  <div className="break-all font-mono text-sm">{selectedNode.metadata?.key}</div>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <div className="text-xs text-muted-foreground">Depth</div>
                    <div className="font-medium">{selectedNode.metadata?.depth ?? "-"}</div>
                  </div>
                  <div>
                    <div className="text-xs text-muted-foreground">Distance</div>
                    <div className="font-medium">
                      {formatNumber(selectedNode.metadata?.distance)}
                    </div>
                  </div>
                </div>
                {selectedNode.metadata?.path && selectedNode.metadata.path.length > 0 && (
                  <div>
                    <div className="mb-1 text-xs text-muted-foreground">Path</div>
                    <div className="space-y-1">
                      {selectedNode.metadata.path.map((key) => (
                        <Badge key={key} className="mr-1 max-w-full">
                          {displayKey(key)}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}
                {selectedNode.metadata?.document && (
                  <div>
                    <div className="mb-2 text-xs text-muted-foreground">Document</div>
                    <JsonViewer json={selectedNode.metadata.document} />
                  </div>
                )}
              </>
            ) : (
              <div className="text-sm text-muted-foreground">Select a node to inspect it.</div>
            )}

            <div>
              <div className="mb-2 text-xs text-muted-foreground">Edges</div>
              {graph.edges.length > 0 ? (
                <div className="max-h-80 space-y-2 overflow-auto pr-1">
                  {graph.edges.slice(0, 40).map((edge) => (
                    <div
                      key={edge.id}
                      className="rounded-none border-(length:--border-width) border-border-strong bg-muted/30 p-2 text-xs"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <Badge variant={edge.pathEdge ? "default" : "default"}>
                          {edge.type ?? "edge"}
                        </Badge>
                        <span className="text-muted-foreground">w {formatNumber(edge.weight)}</span>
                      </div>
                      <div className="mt-2 break-all font-mono">
                        {displayKey(edge.source)} {"->"} {displayKey(edge.target)}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-sm text-muted-foreground">No edges in the current view.</div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Index configuration</CardTitle>
        </CardHeader>
        <CardContent>
          {selectedIndex ? <JsonViewer json={selectedIndex.config} /> : null}
        </CardContent>
      </Card>
    </div>
  );
}
