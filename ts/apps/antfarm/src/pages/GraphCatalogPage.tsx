import {
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  DashboardPage,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  Input,
  Label,
} from "@antfly/design-system";
import type { GraphCatalogEntry, GraphDefinition } from "@antfly/sdk";
import { useCallback, useEffect, useRef, useState } from "react";
import GraphQueryWorkbench from "@/components/GraphQueryWorkbench";
import { useApi } from "@/hooks/use-api-config";

type NodeMapping = {
  name: string;
  table: string;
  predicate: string;
  database?: string;
  namespace?: string;
};
const mappingKey = (label: NodeMapping) =>
  JSON.stringify([label.database || "default", label.namespace || "public", label.table]);
const sourceMappingKey = (labels: NodeMapping[], name: string) => {
  const label = labels.find((entry) => entry.name === name);
  return label ? mappingKey(label) : "";
};
type EdgeMapping = {
  name: string;
  index: string;
  source_label: string;
  target_label: string;
  stored_type: string;
};
const quoted = (name: string) => `\`${name.replaceAll("`", "``")}\``;

export default function GraphCatalogPage() {
  const client = useApi();
  const current = useRef(client);
  current.current = client;
  const [graphs, setGraphs] = useState<GraphCatalogEntry[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [graphIndexes, setGraphIndexes] = useState<Record<string, string[]>>({});
  const [name, setName] = useState("");
  const [labels, setLabels] = useState<NodeMapping[]>([
    { name: "Document", table: "", predicate: "" },
  ]);
  const [relationships, setRelationships] = useState<EdgeMapping[]>([]);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [selected, setSelected] = useState<string>();
  const [editing, setEditing] = useState<GraphCatalogEntry>();
  const [readiness, setReadiness] = useState("");
  const [checkingReadiness, setCheckingReadiness] = useState(false);
  const readinessRequest = useRef<AbortController | null>(null);
  const readinessSequence = useRef(0);
  function edit(entry: GraphCatalogEntry) {
    setEditing(entry);
    setName(entry.name);
    setLabels(
      entry.definition.labels.map((label) => ({
        name: label.name,
        table: typeof label.table === "string" ? label.table : label.table.table,
        database: typeof label.table === "string" ? undefined : label.table.database,
        namespace: typeof label.table === "string" ? undefined : label.table.namespace,
        predicate: label.where ? JSON.stringify(label.where, null, 2) : "",
      }))
    );
    setRelationships((entry.definition.relationships ?? []).map((edge) => ({ ...edge })));
    setMessage("");
  }
  const refresh = useCallback(async () => {
    setBusy(true);
    try {
      const [catalog, inventory] = await Promise.allSettled([
        client.graphs.list(),
        client.tables.list(),
      ]);
      if (current.current !== client) return;
      if (inventory.status === "fulfilled") setTables(inventory.value.map((table) => table.name));
      if (catalog.status === "fulfilled") setGraphs(catalog.value);
      else setMessage(String(catalog.reason));
    } catch (error) {
      if (current.current === client) setMessage(String(error));
    } finally {
      if (current.current === client) setBusy(false);
    }
  }, [client]);
  useEffect(() => {
    setGraphs([]);
    setTables([]);
    setGraphIndexes({});
    setSelected(undefined);
    setEditing(undefined);
    setName("");
    setLabels([{ name: "Document", table: "", predicate: "" }]);
    setRelationships([]);
    setMessage("");
    void refresh();
  }, [refresh]);
  const activeGraph = graphs.find((entry) => entry.name === selected);
  useEffect(() => {
    readinessSequence.current += 1;
    readinessRequest.current?.abort();
    setReadiness("");
    setCheckingReadiness(false);
    return () => readinessRequest.current?.abort();
  }, [client, activeGraph?.graph_id, activeGraph?.revision]);
  async function checkReadiness(entry: GraphCatalogEntry) {
    readinessRequest.current?.abort();
    const controller = new AbortController();
    readinessRequest.current = controller;
    const sequence = ++readinessSequence.current;
    setCheckingReadiness(true);
    setReadiness("");
    try {
      // An untyped edge demands every mapped relationship projection. A
      // node-only probe would validate primary reads while skipping those
      // projections. LIMIT 0 retains capture checks without returning rows.
      const result = await client.graphs.execute(
        entry.name,
        {
          query: "MATCH ()-[r]->() RETURN r LIMIT 0",
        },
        { signal: controller.signal }
      );
      if (current.current !== client || sequence !== readinessSequence.current) return;
      setReadiness(
        result.complete
          ? `Ready at definition revision ${result.graph_revision}. Every query checks readiness again.`
          : "The readiness check did not complete. Try again."
      );
    } catch (error) {
      if (
        current.current === client &&
        sequence === readinessSequence.current &&
        !controller.signal.aborted
      )
        setReadiness(`Graph unavailable: ${String(error)}`);
    } finally {
      if (sequence === readinessSequence.current) setCheckingReadiness(false);
    }
  }

  const sourceTables = JSON.stringify(
    [
      ...new Set(
        labels
          .filter(
            (label) =>
              label.table.trim() &&
              (label.database || label.namespace || tables.includes(label.table))
          )
          .map(mappingKey)
      ),
    ].sort()
  );
  useEffect(() => {
    let active = true;
    const requested = JSON.parse(sourceTables) as string[];
    void Promise.allSettled(
      requested.map(async (key) => {
        const [database, namespace, table] = JSON.parse(key) as [string, string, string];
        const indexes =
          database === "default" && namespace === "public"
            ? await client.indexes.list(table)
            : await client.indexes.list(table, { database, namespace });
        if (!Array.isArray(indexes)) throw new Error("Index metadata was empty.");
        return [
          key,
          indexes
            .filter((index) => index.config.type === "graph")
            .map((index) => index.config.name),
        ] as const;
      })
    ).then((results) => {
      if (!active || current.current !== client) return;
      const available: Record<string, string[]> = {};
      for (const result of results)
        if (result.status === "fulfilled") available[result.value[0]] = result.value[1];
      setGraphIndexes(available);
    });
    return () => {
      active = false;
    };
  }, [client, sourceTables]);

  async function create() {
    if (busy) return;
    if (!name.trim() || labels.some((label) => !label.name.trim() || !label.table.trim())) {
      setMessage("Give the graph a name and choose a name and table for every label.");
      return;
    }
    const names = new Set(labels.map((label) => label.name));
    if (
      names.size !== labels.length ||
      new Set(relationships.map((edge) => edge.name)).size !== relationships.length
    ) {
      setMessage("Label names and relationship names must each be unique.");
      return;
    }
    if (
      relationships.some(
        (edge) =>
          !edge.name ||
          !edge.index ||
          !edge.stored_type ||
          !names.has(edge.source_label) ||
          !names.has(edge.target_label)
      )
    ) {
      setMessage("Complete every relationship mapping and select its source and target labels.");
      return;
    }
    let mappings: GraphDefinition["labels"];
    try {
      mappings = labels.map(({ predicate, database, namespace, ...label }) => {
        const mapped = {
          ...label,
          table:
            database || namespace
              ? {
                  table: label.table,
                  database: database || "default",
                  namespace: namespace || "public",
                }
              : label.table,
        };
        if (!predicate.trim()) return mapped;
        const where: unknown = JSON.parse(predicate);
        if (where === null || typeof where !== "object" || Array.isArray(where))
          throw new Error("A label filter must be a JSON object.");
        return { ...mapped, where: where as Record<string, unknown> };
      });
    } catch (error) {
      setMessage(`Check the label filters: ${String(error)}`);
      return;
    }
    const definition: GraphDefinition = { labels: mappings, relationships };
    setBusy(true);
    setMessage("");
    try {
      const result = editing
        ? await client.graphs.update(editing.name, editing.revision, definition)
        : await client.graphs.create(name.trim(), definition);
      if (current.current !== client) return;
      if ("graph_id" in result) {
        setSelected(result.name);
        setMessage(
          editing
            ? `Updated graph ${result.name} to revision ${result.revision}.`
            : `Created graph ${result.name}.`
        );
        setName("");
        setEditing(undefined);
      } else {
        setMessage("The graph was committed. Refresh the catalog to check its visibility.");
      }
      await refresh();
    } catch (error) {
      if (current.current === client) setMessage(String(error));
    } finally {
      if (current.current === client) setBusy(false);
    }
  }
  const graph = graphs.find((entry) => entry.name === selected);
  return (
    <DashboardPage>
      <DashboardPageHeader>
        <DashboardPageTitle>Graphs</DashboardPageTitle>
        <DashboardPageDescription>
          Map document tables and graph indexes to named labels and relationship types.
        </DashboardPageDescription>
      </DashboardPageHeader>
      {message && (
        <p role="status" className="whitespace-pre-wrap text-sm">
          {message}
        </p>
      )}
      <GraphQueryWorkbench graphs={graphs} selected={selected} onSelect={setSelected} />
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Graph catalog</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <Button variant="outline" onClick={() => void refresh()} disabled={busy}>
              Refresh
            </Button>
            {!graphs.length && !busy && (
              <p className="text-sm text-muted-foreground">
                Create a graph to give your indexes a shared vocabulary.
              </p>
            )}
            {graphs.map((entry) => (
              <Button
                key={entry.graph_id}
                variant={entry.name === selected ? "default" : "outline"}
                onClick={() => setSelected(entry.name)}
              >
                {entry.name}
              </Button>
            ))}
            {graph && (
              <>
                <p className="text-sm text-muted-foreground">
                  Definition revision {graph.revision}
                </p>
                <Button disabled={busy} variant="outline" onClick={() => edit(graph)}>
                  Edit definition
                </Button>
                <Button
                  disabled={checkingReadiness}
                  variant="outline"
                  onClick={() => void checkReadiness(graph)}
                >
                  {checkingReadiness ? "Checking readiness…" : "Check readiness"}
                </Button>
                {readiness && (
                  <p role="status" className="text-sm">
                    {readiness}
                  </p>
                )}
                <dl className="space-y-2 text-sm">
                  {graph.definition.labels.map((label) => (
                    <div key={label.name}>
                      <dt className="font-medium">:{label.name}</dt>
                      <dd className="text-muted-foreground">
                        Documents in{" "}
                        {typeof label.table === "string"
                          ? label.table
                          : `${label.table.database ?? "default"}.${label.table.namespace ?? "public"}.${label.table.table}`}
                      </dd>
                    </div>
                  ))}
                  {(graph.definition.relationships ?? []).map((edge) => (
                    <div key={edge.name}>
                      <dt className="font-medium">
                        {edge.source_label} → {edge.name} → {edge.target_label}
                      </dt>
                      <dd className="text-muted-foreground">
                        Index {edge.index}, stored type {edge.stored_type}
                      </dd>
                    </div>
                  ))}
                </dl>
                <Label>Cypher pattern</Label>
                <pre className="overflow-auto rounded-md bg-muted p-3 text-sm">{`MATCH (n:${quoted(graph.definition.labels[0]?.name ?? "Node")}) RETURN n LIMIT 25`}</pre>
                <details>
                  <summary className="cursor-pointer text-sm">Graph definition</summary>
                  <pre className="overflow-auto p-3 text-xs">
                    {JSON.stringify(graph.definition, null, 2)}
                  </pre>
                </details>
              </>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>{editing ? `Edit ${editing.name}` : "Create a graph"}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-5">
            <div className="space-y-2">
              <Label htmlFor="graph-name">Graph name</Label>
              <Input
                id="graph-name"
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="knowledge"
                disabled={busy || Boolean(editing)}
              />
            </div>
            <datalist id="graph-table-options">
              {tables.map((table) => (
                <option key={table} value={table} />
              ))}
            </datalist>
            <fieldset className="space-y-3" disabled={busy}>
              <legend className="mb-2 font-medium">Node labels</legend>
              {labels.map((label, i) => (
                <div key={i} className="grid grid-cols-2 gap-2 rounded-md border p-3">
                  <Label htmlFor={`label-name-${i}`}>Label</Label>
                  <Label htmlFor={`label-table-${i}`}>Document table</Label>
                  <Input
                    id={`label-name-${i}`}
                    value={label.name}
                    onChange={(event) =>
                      setLabels(
                        labels.map((item, at) =>
                          at === i ? { ...item, name: event.target.value } : item
                        )
                      )
                    }
                  />
                  <Input
                    list="graph-table-options"
                    id={`label-table-${i}`}
                    value={label.table}
                    placeholder="documents"
                    onChange={(event) =>
                      setLabels(
                        labels.map((item, at) =>
                          at === i ? { ...item, table: event.target.value } : item
                        )
                      )
                    }
                  />
                  <details className="col-span-2 space-y-2">
                    <summary className="cursor-pointer text-sm">Table scope</summary>
                    <Label htmlFor={`label-database-${i}`}>Database</Label>
                    <Input
                      id={`label-database-${i}`}
                      value={label.database ?? ""}
                      placeholder="default"
                      onChange={(event) =>
                        setLabels(
                          labels.map((item, at) =>
                            at === i ? { ...item, database: event.target.value } : item
                          )
                        )
                      }
                    />
                    <Label htmlFor={`label-namespace-${i}`}>Namespace</Label>
                    <Input
                      id={`label-namespace-${i}`}
                      value={label.namespace ?? ""}
                      placeholder="public"
                      onChange={(event) =>
                        setLabels(
                          labels.map((item, at) =>
                            at === i ? { ...item, namespace: event.target.value } : item
                          )
                        )
                      }
                    />
                  </details>
                  <details className="col-span-2 space-y-2">
                    <summary className="cursor-pointer text-sm">Label filter</summary>
                    <Label htmlFor={`label-filter-${i}`}>Property equality predicates (JSON)</Label>
                    <textarea
                      id={`label-filter-${i}`}
                      className="min-h-20 w-full rounded-md border bg-background p-2 font-mono text-sm"
                      value={label.predicate}
                      placeholder={'{"kind":"person"}'}
                      onChange={(event) =>
                        setLabels(
                          labels.map((item, at) =>
                            at === i ? { ...item, predicate: event.target.value } : item
                          )
                        )
                      }
                    />
                    <p className="text-xs text-muted-foreground">
                      Leave empty to include every document in the table.
                    </p>
                  </details>
                  {labels.length > 1 && (
                    <Button
                      variant="ghost"
                      onClick={() => setLabels(labels.filter((_, at) => at !== i))}
                    >
                      Remove label
                    </Button>
                  )}
                </div>
              ))}
              <Button
                variant="outline"
                disabled={labels.length >= 64}
                onClick={() => setLabels([...labels, { name: "", table: "", predicate: "" }])}
              >
                Add label
              </Button>
            </fieldset>
            <fieldset className="space-y-3" disabled={busy}>
              <legend className="mb-2 font-medium">Relationship types</legend>
              {relationships.map((edge, i) => (
                <div key={i} className="space-y-2 rounded-md border p-3">
                  {(["name", "index", "stored_type"] as const).map((field) => (
                    <div key={field} className="space-y-1">
                      <Label htmlFor={`edge-${field}-${i}`}>
                        {field === "name"
                          ? "Relationship type"
                          : field === "index"
                            ? "Source graph index"
                            : "Stored edge type"}
                      </Label>
                      <Input
                        id={`edge-${field}-${i}`}
                        list={field === "index" ? `graph-index-options-${i}` : undefined}
                        value={edge[field]}
                        onChange={(event) =>
                          setRelationships(
                            relationships.map((item, at) =>
                              at === i ? { ...item, [field]: event.target.value } : item
                            )
                          )
                        }
                      />
                    </div>
                  ))}
                  <datalist id={`graph-index-options-${i}`}>
                    {(graphIndexes[sourceMappingKey(labels, edge.source_label)] ?? []).map(
                      (index) => (
                        <option key={index} value={index} />
                      )
                    )}
                  </datalist>
                  {(["source_label", "target_label"] as const).map((field) => (
                    <div key={field} className="space-y-1">
                      <Label htmlFor={`edge-${field}-${i}`}>
                        {field === "source_label" ? "Source label" : "Target label"}
                      </Label>
                      <select
                        id={`edge-${field}-${i}`}
                        className="h-9 w-full rounded-md border bg-background px-3 text-sm"
                        value={edge[field]}
                        onChange={(event) =>
                          setRelationships(
                            relationships.map((item, at) =>
                              at === i ? { ...item, [field]: event.target.value } : item
                            )
                          )
                        }
                      >
                        <option value="">Select a label</option>
                        {labels
                          .filter((label) => label.name)
                          .map((label) => (
                            <option key={label.name} value={label.name}>
                              {label.name}
                            </option>
                          ))}
                      </select>
                    </div>
                  ))}
                  <Button
                    variant="ghost"
                    onClick={() => setRelationships(relationships.filter((_, at) => at !== i))}
                  >
                    Remove relationship
                  </Button>
                </div>
              ))}
              <Button
                variant="outline"
                disabled={relationships.length >= 64}
                onClick={() =>
                  setRelationships([
                    ...relationships,
                    {
                      name: "",
                      index: "",
                      source_label: "",
                      target_label: "",
                      stored_type: "",
                    },
                  ])
                }
              >
                Add relationship
              </Button>
            </fieldset>
            <Button onClick={() => void create()} disabled={busy}>
              {busy ? "Working…" : editing ? "Save definition" : "Create graph"}
            </Button>
            {editing && (
              <>
                <Button
                  variant="outline"
                  disabled={busy}
                  onClick={() => {
                    setEditing(undefined);
                    setName("");
                    setLabels([{ name: "Document", table: "", predicate: "" }]);
                    setRelationships([]);
                    setMessage("");
                  }}
                >
                  Cancel editing
                </Button>
                <Button
                  variant="outline"
                  disabled={busy}
                  onClick={async () => {
                    const requested = client;
                    try {
                      const latest = await client.graphs.get(editing.name);
                      if (current.current === requested) edit(latest);
                    } catch (error) {
                      if (current.current === requested) setMessage(String(error));
                    }
                  }}
                >
                  Reload definition
                </Button>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardPage>
  );
}
