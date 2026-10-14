import { Button, Card, CardContent, CardHeader, CardTitle, Label } from "@antfly/design-system";
import {
  GraphExecutionError,
  type GraphCatalogEntry,
  type GraphQueryRequest,
  type GraphQueryResult,
  type GraphValue,
} from "@antfly/sdk";
import { useContext, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useApi } from "@/hooks/use-api-config";
import { AuthContext } from "@/contexts/auth-context";

function byteOffsetToUtf16(text: string, offset: number): number {
  let bytes = 0;
  let index = 0;
  const encoder = new TextEncoder();
  for (const scalar of text) {
    const size = encoder.encode(scalar).length;
    if (bytes + size > offset) break;
    bytes += size;
    index += scalar.length;
  }
  return index;
}

const quote = (name: string) => `\`${name.replaceAll("`", "``")}\``;
const identity = (value: { table: string; key: string }) =>
  JSON.stringify([value.table, value.key]);
function plain(value: GraphValue): unknown {
  if (value.type === "list") return value.value.map(plain);
  if (value.type === "map")
    return Object.fromEntries(Object.entries(value.value).map(([key, item]) => [key, plain(item)]));
  return value.value;
}
function cell(value: GraphValue): string {
  if (value.type === "node")
    return `${value.value.labels.map((label) => `:${label}`).join("")} ${value.value.identity.key}`;
  if (value.type === "relationship") return `:${value.value.type}`;
  if (value.type === "path") return `Path (${value.value.relationships.length} relationships)`;
  if (value.type === "null") return "null";
  return typeof value.value === "string" ? value.value : JSON.stringify(plain(value));
}
type NodeValue = Extract<GraphValue, { type: "node" }>;
type EdgeValue = Extract<GraphValue, { type: "relationship" }>;
export function collectGraph(values: GraphValue[], definition?: GraphCatalogEntry["definition"]) {
  const nodes = new Map<string, NodeValue>();
  const edges = new Map<string, EdgeValue>();
  let clipped = false;
  function addNode(value: NodeValue) {
    const id = identity(value.value.identity);
    if (!nodes.has(id) && nodes.size >= 200) {
      clipped = true;
      return;
    }
    nodes.set(id, value);
  }
  function visit(value: GraphValue) {
    if (value.type === "node") addNode(value);
    else if (value.type === "relationship") {
      const id = JSON.stringify([
        value.value.identity.table,
        value.value.index,
        value.value.identity.key,
      ]);
      if (!edges.has(id) && edges.size >= 400) {
        clipped = true;
        return;
      }
      edges.set(id, value);
      const mapping = definition?.relationships?.find((entry) => entry.name === value.value.type);
      for (const [endpoint, label] of [
        [value.value.source, mapping?.source_label],
        [value.value.target, mapping?.target_label],
      ] as const) {
        if (!nodes.has(identity(endpoint)))
          addNode({
            type: "node",
            value: {
              identity: endpoint,
              labels: label ? [label] : [],
              properties: { type: "map", value: {} },
            },
          });
      }
    } else if (value.type === "path") {
      value.value.nodes.forEach(visit);
      value.value.relationships.forEach(visit);
    } else if (value.type === "list") value.value.forEach(visit);
    else if (value.type === "map") Object.values(value.value).forEach(visit);
  }
  values.forEach(visit);
  return { nodes: [...nodes.entries()], edges: [...edges.values()], clipped };
}

type WriteRecovery = { id: string; graph: string; disposition: string };
function restoreWriteRecovery(key: string): WriteRecovery | undefined {
  try {
    const value = JSON.parse(localStorage.getItem(key) ?? "null");
    if (
      value &&
      /^[0-9a-f]{32}$/.test(value.id) &&
      typeof value.graph === "string" &&
      typeof value.disposition === "string"
    )
      return value;
  } catch {
    /* Storage may be unavailable in a private browser session. */
  }
  return undefined;
}

export default function GraphQueryWorkbench({
  graphs,
  selected,
  onSelect,
}: {
  graphs: GraphCatalogEntry[];
  selected?: string;
  onSelect: (name: string) => void;
}) {
  const client = useApi();
  const authentication = useContext(AuthContext);
  const navigate = useNavigate();
  const editor = useRef<HTMLTextAreaElement>(null);
  const sequence = useRef(0);
  const schemaSequence = useRef(0);
  const active = useRef<AbortController | null>(null);
  const graph = graphs.find((entry) => entry.name === selected) ?? graphs[0];
  const [query, setQuery] = useState("MATCH (n) RETURN n LIMIT 25");
  const [parameters, setParameters] = useState("{}");
  const [typed, setTyped] = useState(false);
  const [result, setResult] = useState<GraphQueryResult>();
  const [propertySuggestions, setPropertySuggestions] = useState<string[]>([]);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const principalScope = authentication
    ? `:principal:${authentication.user?.username ?? "anonymous"}`
    : "";
  const recoveryKey = `antfly:graph-write:${client.baseUrl ?? window.location.origin}${principalScope}`;
  const recoveryScope = useRef(recoveryKey);
  const [writeRecovery, updateWriteRecovery] = useState<WriteRecovery | undefined>(() =>
    restoreWriteRecovery(recoveryKey)
  );
  const recoveryRef = useRef(writeRecovery);
  function setWriteRecovery(value: WriteRecovery | undefined) {
    recoveryRef.current = value;
    // Persist before sending the request so navigation or reload retains its ID.
    try {
      if (value) localStorage.setItem(recoveryKey, JSON.stringify(value));
      else localStorage.removeItem(recoveryKey);
    } catch {
      /* The visible receipt remains available when storage is disabled. */
    }
    updateWriteRecovery(value);
  }
  useEffect(() => {
    recoveryScope.current = recoveryKey;
    const restored = restoreWriteRecovery(recoveryKey);
    recoveryRef.current = restored;
    updateWriteRecovery(restored);
  }, [recoveryKey]);
  const unresolvedWrite =
    writeRecovery?.disposition === "active" || writeRecovery?.disposition === "outcome_unknown";

  const [extra, setExtra] = useState<GraphValue[]>([]);
  const [selectedNode, setSelectedNode] = useState<NodeValue>();
  const [selectedEntity, setSelectedEntity] = useState<GraphValue>();
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const drag = useRef<{
    x: number;
    y: number;
    panX: number;
    panY: number;
  } | null>(null);
  const [view, setView] = useState<"Table" | "Graph" | "JSON" | "Explain">("Table");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  useEffect(() => {
    sequence.current += 1;
    active.current?.abort();
    active.current = null;
    setBusy(false);
    setResult(undefined);
    setPropertySuggestions([]);
    setLoadingSchema(false);
    schemaSequence.current += 1;
    setExtra([]);
    setSelectedNode(undefined);
    setSelectedEntity(undefined);
    setMessage("");
    return () => {
      sequence.current += 1;
      active.current?.abort();
    };
  }, [client, graph?.name, graph?.revision, principalScope]);
  const visual = useMemo(
    () => collectGraph([...(result?.rows.flat() ?? []), ...extra], graph?.definition),
    [result, extra, graph?.definition]
  );
  const returnedNodes = useMemo(
    () =>
      new Set(collectGraph(result?.rows.flat() ?? [], graph?.definition).nodes.map(([id]) => id)),
    [result, graph?.definition]
  );
  const positions = useMemo(
    () =>
      new Map(
        visual.nodes.map(([id], index) => {
          const angle = (2 * Math.PI * index) / Math.max(1, visual.nodes.length);
          return [
            id,
            {
              x: visual.nodes.length === 1 ? 400 : 400 + 310 * Math.cos(angle),
              y: visual.nodes.length === 1 ? 210 : 210 + 155 * Math.sin(angle),
            },
          ];
        })
      ),
    [visual]
  );

  async function execute(mode: "run" | "explain" | "profile") {
    if (!graph || busy || !query.trim()) return;
    let request: GraphQueryRequest;
    try {
      const input: unknown = JSON.parse(parameters);
      if (typed) {
        if (!input || typeof input !== "object" || !("type" in input) || input.type !== "map")
          throw new Error("Typed parameters must be a map graph value");
        request = { query, typed_parameters: input as GraphValue };
      } else {
        if (!input || typeof input !== "object" || Array.isArray(input))
          throw new Error("Parameters must be a JSON object");
        request = {
          query,
          parameters: input as GraphQueryRequest["parameters"],
        };
      }
    } catch (error) {
      setMessage(String(error));
      return;
    }
    if (mode !== "explain" && unresolvedWrite) {
      setMessage("Inspect the previous write transaction before submitting another statement.");
      return;
    }
    const statement = query.replace(/^\s*(?:EXPLAIN|PROFILE)\s+/i, "");
    request.query = mode === "run" ? query : `${mode.toUpperCase()} ${statement}`;
    const token = ++sequence.current;
    const controller = new AbortController();
    active.current = controller;
    setBusy(true);
    setMessage("");
    setResult(undefined);
    setExtra([]);
    setSelectedNode(undefined);
    setSelectedEntity(undefined);
    let writeId: string | undefined;
    try {
      const text = statement.replace(
        /'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|`(?:[^`]|``)*`|\/\*[\s\S]*?\*\/|\/\/[^\n]*/g,
        " "
      );
      if (mode !== "explain" && /\b(?:CREATE|MERGE|SET|REMOVE|DELETE)\b/i.test(text)) {
        const preview = await client.graphs.execute(
          graph.name,
          { ...request, query: `EXPLAIN ${statement}` },
          { signal: controller.signal }
        );
        if (sequence.current !== token) return;
        if (preview.read_only !== true) {
          writeId = crypto.randomUUID().replaceAll("-", "");
          request.write_request_id = writeId;
          setWriteRecovery({
            id: writeId,
            graph: graph.name,
            disposition: "active",
          });
        }
      }
      const response = await client.graphs.execute(graph.name, request, {
        signal: controller.signal,
      });
      if (sequence.current !== token) return;
      setResult(response);
      setView(mode === "explain" ? "Explain" : "Table");
      if (response.commit) {
        setWriteRecovery({
          id: response.commit.transaction_id,
          graph: graph.name,
          disposition:
            response.commit.status === "unknown" ? "outcome_unknown" : response.commit.status,
        });
        if (response.commit.status === "unknown")
          setMessage(
            "The write outcome is unknown. Inspect this transaction before submitting another statement."
          );
      } else if (writeId) setWriteRecovery(undefined);
      if (!response.complete && !response.commit)
        setMessage("The server reported an incomplete result.");
    } catch (error) {
      if (sequence.current === token) {
        if (
          writeId &&
          error instanceof GraphExecutionError &&
          error.diagnostic.write_outcome === "committed"
        ) {
          setWriteRecovery({
            id: writeId,
            graph: graph.name,
            disposition: "committed",
          });
          setMessage(
            "The write committed, but its result could not be delivered. Inspect the transaction for its receipt."
          );
        } else if (
          writeId &&
          error instanceof GraphExecutionError &&
          (error.diagnostic.write_outcome === "not_started" ||
            error.diagnostic.write_outcome === "aborted" ||
            [
              "Forbidden",
              "RowPolicyDenied",
              "RowPolicyAuthenticationRequired",
              "CypherTransactionConflict",
            ].includes(error.diagnostic.code))
        ) {
          setWriteRecovery({
            id: writeId,
            graph: graph.name,
            disposition: "aborted",
          });
          setMessage(error.diagnostic.message);
        } else if (writeId) {
          setWriteRecovery({
            id: writeId,
            graph: graph.name,
            disposition: "outcome_unknown",
          });
          setMessage(
            "Write result delivery stopped. Inspect the transaction before submitting another statement."
          );
        } else if (controller.signal.aborted)
          setMessage("Request canceled; result delivery stopped.");
        else if (
          error instanceof GraphExecutionError &&
          error.diagnostic.span &&
          error.diagnostic.span.end > error.diagnostic.span.start
        ) {
          const encoder = new TextEncoder();
          const prefixDifference =
            mode === "run"
              ? 0
              : encoder.encode(request.query.slice(0, request.query.length - statement.length))
                  .length - encoder.encode(query.slice(0, query.length - statement.length)).length;
          const start = byteOffsetToUtf16(
            query,
            Math.max(0, error.diagnostic.span.start - prefixDifference)
          );
          const end = byteOffsetToUtf16(
            query,
            Math.max(0, error.diagnostic.span.end - prefixDifference)
          );
          const before = query.slice(0, start);
          const line = before.split("\n").length;
          const column = [...before.slice(before.lastIndexOf("\n") + 1)].length + 1;
          setMessage(`${error.diagnostic.message} at line ${line}, column ${column}`);
          editor.current?.focus();
          editor.current?.setSelectionRange(start, end);
        } else if (error instanceof GraphExecutionError) setMessage(error.diagnostic.message);
        else setMessage(String(error));
      }
    } finally {
      if (sequence.current === token) {
        setBusy(false);
        active.current = null;
      }
    }
  }
  async function inspectWrite() {
    if (!writeRecovery) return;
    const selected = writeRecovery;
    const selectedScope = recoveryKey;
    try {
      const status = await client.graphs.writeStatus(selected.id);
      if (recoveryScope.current !== selectedScope) return;
      if (recoveryRef.current?.id === selected.id)
        setWriteRecovery({ ...selected, disposition: status.disposition });
      setMessage(`Transaction ${selected.id}: ${status.disposition}.`);
    } catch (error) {
      if (recoveryScope.current !== selectedScope) return;
      setMessage(
        `The transaction receipt is not available yet. Keep its ID and inspect it again before replaying the statement. ${String(error)}`
      );
    }
  }
  async function expand() {
    if (!graph || !selectedNode || !result || busy || !selectedNode.value.labels.length) return;
    const token = ++sequence.current;
    const controller = new AbortController();
    active.current = controller;
    setBusy(true);
    setMessage("");
    try {
      const label = quote(selectedNode.value.labels[0]);
      const response = await client.graphs.execute(
        graph.name,
        {
          query: `MATCH (n:${label}) WHERE antfly.documentKey(n)=$key OPTIONAL MATCH (n)-[r]-(m) RETURN n,r,m LIMIT 50`,
          parameters: { key: selectedNode.value.identity.key },
          limits: { explored_edges: 10000, intermediate_states: 10000 },
        },
        { signal: controller.signal }
      );
      if (sequence.current !== token) return;
      if (
        response.graph_incarnation !== result.graph_incarnation ||
        response.graph_revision !== result.graph_revision
      )
        throw new Error("The graph definition changed. Run the query again before expanding.");
      setExtra((old) => [...old, ...response.rows.flat()].slice(-1000));
      const hydrated = collectGraph(response.rows.flat(), graph.definition).nodes.find(
        ([id]) => id === identity(selectedNode.value.identity)
      );
      if (hydrated) {
        setSelectedNode(hydrated[1]);
        setSelectedEntity(hydrated[1]);
      }
      setMessage("Added up to 50 neighbor rows from a fresh read of the graph.");
    } catch (error) {
      if (sequence.current === token) setMessage(String(error));
    } finally {
      if (sequence.current === token) {
        setBusy(false);
        active.current = null;
      }
    }
  }
  async function loadSchema() {
    if (!graph) return;
    const token = ++schemaSequence.current;
    setLoadingSchema(true);
    const names = new Set<string>();
    const results = await Promise.allSettled(
      graph.definition.labels.map(async (label) => {
        const target = label.table;
        const table =
          typeof target === "string"
            ? await client.tables.get(target)
            : await client.tables.get(target.table, {
                database: target.database ?? "default",
                namespace: target.namespace ?? "public",
              });
        for (const definition of Object.values(table?.schema?.document_schemas ?? {})) {
          for (const property of Object.keys(definition.schema?.properties ?? {}))
            names.add(`n.${quote(property)}`);
        }
      })
    );
    if (token !== schemaSequence.current) return;
    setPropertySuggestions([...names].sort());
    setLoadingSchema(false);
    if (results.some((result) => result.status === "rejected"))
      setMessage(
        "Some mapped table schemas could not be loaded. Property suggestions include accessible schemas."
      );
  }
  function insert(text: string) {
    const input = editor.current;
    const start = input?.selectionStart ?? query.length;
    const end = input?.selectionEnd ?? query.length;
    setQuery(query.slice(0, start) + text + query.slice(end));
    requestAnimationFrame(() => {
      input?.focus();
      input?.setSelectionRange(start + text.length, start + text.length);
    });
  }
  function changeLanguage(language: string) {
    if (language === "sql") {
      const name = graph?.name.replaceAll('"', '""');
      const statement = name
        ? `SELECT g.node FROM GRAPH_TABLE("${name}" MATCH (n) COLUMNS (n AS node)) AS g LIMIT 25`
        : "SELECT 1";
      navigate(`/sql?${new URLSearchParams({ statement })}`);
    } else if (language === "json") {
      const mapping = graph?.definition.relationships?.[0];
      const label =
        graph?.definition.labels.find((entry) => entry.name === mapping?.source_label) ??
        graph?.definition.labels[0];
      const table = typeof label?.table === "string" ? label.table : label?.table.table;
      navigate(table ? `/tables/${encodeURIComponent(table)}?section=graph` : "/retrieval/graph");
    }
  }
  return (
    <Card>
      <CardHeader>
        <CardTitle>Query workbench</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap items-end gap-3">
          <label className="grid gap-1 text-sm">
            Language
            <select
              aria-label="Query language"
              className="rounded border bg-background p-2"
              value="cypher"
              onChange={(event) => changeLanguage(event.target.value)}
            >
              <option value="cypher">Cypher</option>
              <option value="sql">SQL</option>
              <option value="json">JSON graph</option>
            </select>
          </label>
          <label className="grid gap-1 text-sm">
            Graph
            <select
              aria-label="Query graph"
              className="rounded border bg-background p-2"
              value={graph?.name ?? ""}
              onChange={(event) => onSelect(event.target.value)}
              disabled={busy || !graphs.length}
            >
              {!graphs.length && <option value="">Create a graph below</option>}
              {graphs.map((entry) => (
                <option key={entry.graph_id} value={entry.name}>
                  {entry.name}
                </option>
              ))}
            </select>
          </label>
          <select
            aria-label="Insert Cypher syntax"
            className="rounded border bg-background p-2 text-sm"
            value=""
            onChange={(event) => insert(event.target.value)}
          >
            <option value="">Insert syntax…</option>
            {[
              "MATCH ",
              "OPTIONAL MATCH ",
              "WITH ",
              "WHERE ",
              "RETURN ",
              "UNWIND ",
              "ORDER BY ",
              "LIMIT ",
              "elementId(n)",
              "antfly.documentKey(n)",
              "labels(n)",
              "properties(n)",
              "nodes(p)",
              "relationships(p)",
              "count(n)",
              "collect(n)",
              "coalesce(n.property, null)",
              ...propertySuggestions,
              ...(graph?.definition.labels[0]
                ? [
                    ...["text", "semantic", "hybrid"].map(
                      (mode) =>
                        `CALL antfly.search(${JSON.stringify(graph.definition.labels[0].table)}, {query:$query, mode:'${mode}', limit:10}) YIELD node, score `
                    ),
                  ]
                : []),
              ...(graph?.definition.labels ?? []).map((label) => `:${quote(label.name)}`),
              ...(graph?.definition.relationships ?? []).map(
                (edge) =>
                  `(:${quote(edge.source_label)})-[:${quote(edge.name)}]->(:${quote(edge.target_label)})`
              ),
            ].map((text) => (
              <option key={text} value={text}>
                {text}
              </option>
            ))}
          </select>
          <Button
            variant="outline"
            disabled={busy || loadingSchema || !graph}
            onClick={() => void loadSchema()}
          >
            {loadingSchema ? "Loading properties…" : "Load properties"}
          </Button>
        </div>
        <Label htmlFor="cypher-query">Cypher statement</Label>
        <textarea
          ref={editor}
          id="cypher-query"
          className="min-h-40 w-full rounded-md border bg-background p-3 font-mono text-sm"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
              event.preventDefault();
              void execute("run");
            }
          }}
          spellCheck={false}
        />
        <details>
          <summary className="cursor-pointer text-sm">Parameters</summary>
          <label className="my-2 flex gap-2 text-sm">
            <input
              type="checkbox"
              checked={typed}
              onChange={(event) => {
                setTyped(event.target.checked);
                setParameters(event.target.checked ? '{"type":"map","value":{}}' : "{}");
              }}
            />
            Lossless typed values
          </label>
          <textarea
            aria-label="Cypher parameters"
            className="min-h-24 w-full rounded border bg-background p-2 font-mono text-sm"
            value={parameters}
            onChange={(event) => setParameters(event.target.value)}
            spellCheck={false}
          />
        </details>
        <div className="flex flex-wrap gap-2">
          <Button
            onClick={() => void execute("run")}
            disabled={busy || unresolvedWrite || !graph || !query.trim()}
          >
            Run
          </Button>
          <Button
            variant="outline"
            onClick={() => void execute("explain")}
            disabled={busy || !graph}
          >
            Explain
          </Button>
          <Button
            variant="outline"
            onClick={() => void execute("profile")}
            disabled={busy || unresolvedWrite || !graph}
          >
            Profile
          </Button>
          {busy && (
            <Button variant="outline" onClick={() => active.current?.abort()}>
              Cancel request
            </Button>
          )}
        </div>
        {writeRecovery && (
          <div className="rounded border p-3 text-sm">
            <p>
              Write transaction in {writeRecovery.graph}: {writeRecovery.disposition}
            </p>
            <code className="break-all">{writeRecovery.id}</code>
            <Button
              className="ml-3"
              variant="outline"
              onClick={() => void inspectWrite()}
              disabled={busy}
            >
              Inspect transaction
            </Button>
          </div>
        )}
        {message && (
          <p role="status" className="whitespace-pre-wrap text-sm">
            {message}
          </p>
        )}
        {result && (
          <>
            <p className="text-sm text-muted-foreground">
              {result.rows.length} rows · {result.stats.explored_edges ?? 0} edges explored ·{" "}
              {result.stats.mutations ?? 0} changes
              {result.stats.search_calls
                ? ` · ${result.stats.search_candidates ?? 0} search seeds`
                : ""}
              {result.stats.search_approximate ? " · Approximate search ranking" : ""}
            </p>
            <div role="tablist" aria-label="Graph result views" className="flex gap-2">
              {(["Table", "Graph", "JSON", "Explain"] as const).map((tab) => (
                <Button
                  key={tab}
                  role="tab"
                  aria-selected={view === tab}
                  variant={view === tab ? "default" : "outline"}
                  onClick={() => setView(tab)}
                >
                  {tab}
                </Button>
              ))}
            </div>
            <div role="tabpanel" aria-label={`${view} results`}>
              {view === "Table" && (
                <div className="overflow-auto">
                  <table className="w-full text-left text-sm">
                    <thead>
                      <tr>
                        {result.columns.map((name) => (
                          <th key={name} className="border-b p-2">
                            {name}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.rows.slice(0, 1000).map((row, index) => (
                        <tr key={index}>
                          {row.map((value, column) => (
                            <td key={column} className="max-w-80 whitespace-pre-wrap border-b p-2">
                              {["node", "relationship", "path", "map", "list"].includes(
                                value.type
                              ) ? (
                                <button
                                  className="text-left underline decoration-dotted"
                                  onClick={() => {
                                    setSelectedEntity(value);
                                    setSelectedNode(value.type === "node" ? value : undefined);
                                  }}
                                >
                                  {cell(value)}
                                </button>
                              ) : (
                                cell(value)
                              )}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {result.rows.length > 1000 && (
                    <p>Showing the first 1,000 rows. The JSON view contains all returned rows.</p>
                  )}
                </div>
              )}
              {view === "JSON" && (
                <pre className="max-h-[32rem] overflow-auto rounded bg-muted p-3 text-xs">
                  {JSON.stringify(result, null, 2)}
                </pre>
              )}
              {view === "Explain" && (
                <pre className="max-h-[32rem] overflow-auto rounded bg-muted p-3 text-xs">
                  {JSON.stringify({ plan: result.plan, stats: result.stats }, null, 2)}
                </pre>
              )}
              {view === "Graph" && (
                <>
                  <p className="text-sm">
                    {result.rows.length} returned rows · {visual.nodes.length} displayed nodes ·{" "}
                    {visual.edges.length} displayed relationships
                  </p>
                  {extra.length > 0 && (
                    <p className="text-sm">
                      Green nodes were added by neighbor exploration. The original query rows are
                      unchanged.
                    </p>
                  )}
                  <label className="flex items-center gap-2 text-sm">
                    Zoom
                    <input
                      aria-label="Graph zoom"
                      type="range"
                      min="0.5"
                      max="4"
                      step="0.1"
                      value={zoom}
                      onChange={(event) => setZoom(Number(event.target.value))}
                    />
                  </label>
                  <div className="flex flex-wrap gap-2" aria-label="Graph navigation">
                    {[
                      ["Pan left", -80, 0],
                      ["Pan right", 80, 0],
                      ["Pan up", 0, -50],
                      ["Pan down", 0, 50],
                    ].map(([label, dx, dy]) => (
                      <Button
                        key={label}
                        variant="outline"
                        size="sm"
                        onClick={() =>
                          setPan((current) => ({
                            x: current.x + Number(dx) / zoom,
                            y: current.y + Number(dy) / zoom,
                          }))
                        }
                      >
                        {label}
                      </Button>
                    ))}
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        setPan({ x: 0, y: 0 });
                        setZoom(1);
                      }}
                    >
                      Reset view
                    </Button>
                  </div>
                  {visual.clipped && (
                    <p className="text-sm">
                      The visualization shows up to 200 nodes and 400 relationships. Query rows are
                      unchanged.
                    </p>
                  )}
                  {!visual.nodes.length ? (
                    <p className="p-4 text-sm">
                      Return node, relationship or path values to visualize the graph.
                    </p>
                  ) : (
                    <svg
                      viewBox={`${400 - 400 / zoom + pan.x} ${210 - 210 / zoom + pan.y} ${800 / zoom} ${420 / zoom}`}
                      onPointerDown={(event) => {
                        if (
                          event.button !== 0 ||
                          (event.target as Element).closest('[role="button"]')
                        )
                          return;
                        drag.current = {
                          x: event.clientX,
                          y: event.clientY,
                          panX: pan.x,
                          panY: pan.y,
                        };
                        event.currentTarget.setPointerCapture(event.pointerId);
                      }}
                      onPointerMove={(event) => {
                        const start = drag.current;
                        if (!start) return;
                        const bounds = event.currentTarget.getBoundingClientRect();
                        const scale = Math.min(
                          bounds.width / (800 / zoom),
                          bounds.height / (420 / zoom)
                        );
                        if (scale > 0)
                          setPan({
                            x: start.panX - (event.clientX - start.x) / scale,
                            y: start.panY - (event.clientY - start.y) / scale,
                          });
                      }}
                      onPointerUp={() => {
                        drag.current = null;
                      }}
                      onPointerCancel={() => {
                        drag.current = null;
                      }}
                      style={{ touchAction: "none" }}
                      role="img"
                      aria-label="Query graph"
                      className="w-full rounded border bg-muted/20"
                    >
                      <defs>
                        <marker
                          id="cypher-arrow"
                          markerWidth="8"
                          markerHeight="8"
                          refX="18"
                          refY="4"
                          orient="auto"
                        >
                          <path d="M0,0 L8,4 L0,8" fill="currentColor" />
                        </marker>
                      </defs>
                      {visual.edges.map((edge) => {
                        const a = positions.get(identity(edge.value.source));
                        const b = positions.get(identity(edge.value.target));
                        if (!a || !b) return null;
                        const key = JSON.stringify([edge.value.identity, edge.value.index]);
                        const parallels = visual.edges.filter(
                          (other) =>
                            (identity(other.value.source) === identity(edge.value.source) &&
                              identity(other.value.target) === identity(edge.value.target)) ||
                            (identity(other.value.target) === identity(edge.value.source) &&
                              identity(other.value.source) === identity(edge.value.target))
                        );
                        const lane = parallels.indexOf(edge);
                        const offset =
                          (lane - (parallels.length - 1) / 2) *
                          28 *
                          (identity(edge.value.source) < identity(edge.value.target) ? 1 : -1);
                        const loopRadius = 48 + lane * 18;
                        const dx = b.x - a.x;
                        const dy = b.y - a.y;
                        const length = Math.hypot(dx, dy) || 1;
                        const path =
                          identity(edge.value.source) === identity(edge.value.target)
                            ? `M ${a.x + 10} ${a.y - 14} C ${a.x + loopRadius} ${a.y - loopRadius - 20}, ${a.x - loopRadius} ${a.y - loopRadius - 20}, ${a.x - 10} ${a.y - 14}`
                            : `M ${a.x} ${a.y} Q ${(a.x + b.x) / 2 - (dy / length) * offset} ${(a.y + b.y) / 2 + (dx / length) * offset} ${b.x} ${b.y}`;
                        return (
                          <g
                            key={key}
                            role="button"
                            tabIndex={0}
                            aria-label={`Inspect relationship ${edge.value.identity.key}`}
                            onClick={() => {
                              setSelectedEntity(edge);
                              setSelectedNode(undefined);
                            }}
                            onKeyDown={(event) => {
                              if (event.key === "Enter" || event.key === " ") {
                                event.preventDefault();
                                setSelectedEntity(edge);
                                setSelectedNode(undefined);
                              }
                            }}
                            className="cursor-pointer"
                          >
                            <path
                              d={path}
                              fill="none"
                              stroke="currentColor"
                              strokeOpacity="0.4"
                              strokeWidth="3"
                              markerEnd="url(#cypher-arrow)"
                            />
                            <path d={path} fill="none" stroke="transparent" strokeWidth="14" />
                            <title>{edge.value.type}</title>
                          </g>
                        );
                      })}
                      {visual.nodes.map(([id, node]) => {
                        const p = positions.get(id)!;
                        return (
                          <g
                            key={id}
                            role="button"
                            tabIndex={0}
                            aria-label={`Inspect ${node.value.identity.key}`}
                            onClick={() => {
                              setSelectedNode(node);
                              setSelectedEntity(node);
                            }}
                            onKeyDown={(event) => {
                              if (event.key === "Enter" || event.key === " ") {
                                event.preventDefault();
                                setSelectedNode(node);
                                setSelectedEntity(node);
                              }
                            }}
                            className="cursor-pointer"
                          >
                            <circle
                              cx={p.x}
                              cy={p.y}
                              r={16}
                              fill={
                                selectedNode && identity(selectedNode.value.identity) === id
                                  ? "#eab308"
                                  : returnedNodes.has(id)
                                    ? "#3b82f6"
                                    : "#10b981"
                              }
                            />
                            <text
                              x={p.x}
                              y={p.y + 31}
                              textAnchor="middle"
                              fill="currentColor"
                              fontSize="12"
                            >
                              {node.value.identity.key.slice(0, 24)}
                            </text>
                            <title>{node.value.labels.join(", ")}</title>
                          </g>
                        );
                      })}
                    </svg>
                  )}
                </>
              )}
              {selectedEntity && (
                <div className="space-y-2 rounded border p-3">
                  <p className="font-medium">{cell(selectedEntity)}</p>
                  {selectedNode && (
                    <Button
                      variant="outline"
                      onClick={() => void expand()}
                      disabled={busy || !selectedNode.value.labels.length || visual.clipped}
                    >
                      Expand neighbors
                    </Button>
                  )}
                  <pre className="max-h-60 overflow-auto text-xs">
                    {JSON.stringify(
                      selectedEntity.type === "node" || selectedEntity.type === "relationship"
                        ? {
                            ...selectedEntity.value,
                            properties: plain(selectedEntity.value.properties),
                          }
                        : plain(selectedEntity),
                      null,
                      2
                    )}
                  </pre>
                </div>
              )}
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}
