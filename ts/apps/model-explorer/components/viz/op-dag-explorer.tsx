"use client";

import {
  Badge,
  Button,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  cn,
  Separator,
} from "@antfly/design-system";
import dagre from "@dagrejs/dagre";
import {
  Background,
  Controls,
  type Edge as FlowEdge,
  type Node as FlowNode,
  Handle,
  MarkerType,
  type NodeProps,
  Position,
  ReactFlow,
} from "@xyflow/react";
import { Search, X } from "lucide-react";
import Link from "next/link";
import { useTheme } from "next-themes";
import { parseAsString, parseAsStringLiteral, useQueryState } from "nuqs";
import { Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { CodeLink } from "@/components/code/code-link";
import {
  dtypeColorVar,
  EnvFlagChip,
  FusionChip,
  OpKindBadge,
  QuantChip,
  TensorShapeBadge,
} from "@/components/primitives/chips";
import { ChoiceGroup } from "@/components/primitives/choice-group";
import type { KernelRoute, ModelSpec, OpNode, PhaseGraph, Stage } from "@/lib/schema";

/* ------------------------------------------------------------------ */
/* Layout                                                              */
/* ------------------------------------------------------------------ */

const NODE_W = 190;
const NODE_H = 64;

const FAMILY_OF_OPKIND: Array<[RegExp, string]> = [
  [/attention/, "var(--kfam-attention)"],
  [/moe/, "var(--kfam-moe)"],
  [/(linear|matmul|dot_general)/, "var(--kfam-matvec)"],
  [/(rope|norm)/, "var(--kfam-fusion)"],
  [/(argmax|sample)/, "var(--kfam-sampling)"],
  [/embedding/, "var(--kfam-mmsg)"],
];

function opColor(node: OpNode, colorBy: string): string {
  if (colorBy === "backend") {
    return node.backend === "native" ? "var(--muted-foreground)" : "var(--kfam-attention)";
  }
  if (colorBy === "kernel") {
    for (const [re, color] of FAMILY_OF_OPKIND) if (re.test(node.opKind)) return color;
    return "var(--muted-foreground)";
  }
  // default: dtype of the primary output
  const out = node.shapes.out[0];
  return dtypeColorVar(out?.quant ?? out?.dtype);
}

function layoutGraph(graph: PhaseGraph): Map<string, { x: number; y: number }> {
  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: "LR", nodesep: 28, ranksep: 56, marginx: 24, marginy: 24 });
  g.setDefaultEdgeLabel(() => ({}));
  for (const node of graph.nodes) {
    g.setNode(node.id, { width: NODE_W, height: NODE_H });
  }
  for (const edge of graph.edges) {
    g.setEdge(edge.from, edge.to, { weight: edge.kind === "residual" ? 3 : 1 });
  }
  dagre.layout(g);
  const out = new Map<string, { x: number; y: number }>();
  for (const node of graph.nodes) {
    const pos = node.position ?? g.node(node.id);
    out.set(node.id, { x: pos.x, y: pos.y });
  }
  return out;
}

/* ------------------------------------------------------------------ */
/* Custom node                                                         */
/* ------------------------------------------------------------------ */

type OpFlowNode = FlowNode<{ op: OpNode; color: string; selected: boolean }, "op">;

function OpNodeView({ data }: NodeProps<OpFlowNode>) {
  const { op, color, selected } = data;
  const isAttention = /attention/.test(op.opKind);
  const isNorm = /norm/.test(op.opKind) && !op.fusedOps.length;
  return (
    <div
      className={cn(
        "flex flex-col justify-center border bg-card px-2.5 shadow-sm transition-shadow",
        isAttention
          ? "[clip-path:polygon(10%_0,90%_0,100%_50%,90%_100%,10%_100%,0_50%)] px-5"
          : "rounded-md",
        isNorm ? "h-8" : "h-16",
        selected && "shadow-[0_0_0_2px_var(--primary)]"
      )}
      style={{
        borderColor: color,
        width: NODE_W,
        borderStyle: op.backend === "native" ? "dashed" : "solid",
        borderBottomWidth: op.opKind === "parameter" ? 4 : undefined,
      }}
    >
      <Handle type="target" position={Position.Left} className="opacity-0!" />
      <div className="flex items-center gap-1.5 overflow-hidden">
        <span className="truncate text-xs font-medium">{op.label ?? op.opKind}</span>
        {op.fusedOps.length > 0 && (
          <span className="font-mono text-[9px] text-primary">⟨{op.fusedOps.length}⟩</span>
        )}
      </div>
      {!isNorm && (
        <div className="flex items-center gap-1 overflow-hidden font-mono text-[9px] text-muted-foreground">
          <span className="truncate">{op.opKind}</span>
          {op.shapes.out[0] && (
            <span className="truncate text-muted-foreground">
              [{op.shapes.out[0].dims.join(",")}]
            </span>
          )}
        </div>
      )}
      <Handle type="source" position={Position.Right} className="opacity-0!" />
    </div>
  );
}

const nodeTypes = { op: OpNodeView };

/* ------------------------------------------------------------------ */
/* Inspector                                                           */
/* ------------------------------------------------------------------ */

function Inspector({
  op,
  stage,
  routes,
  onClose,
}: {
  op: OpNode;
  stage: Stage | undefined;
  routes: KernelRoute[];
  onClose: () => void;
}) {
  return (
    <div className="absolute right-3 top-3 bottom-3 z-10 flex w-80 max-w-[calc(100%-1.5rem)] flex-col overflow-hidden rounded-lg border bg-background/95 shadow-lg backdrop-blur">
      <div className="flex items-center justify-between border-b px-3 py-2">
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold">{op.label ?? op.opKind}</div>
          <div className="font-mono text-[11px] text-muted-foreground">
            {stage?.title ?? op.stageId}
            {stage?.repeat && ` · ${stage.repeat.count}-layer stage`}
          </div>
        </div>
        <Button variant="ghost" size="icon" onClick={onClose} aria-label="Close inspector">
          <X className="size-4" />
        </Button>
      </div>
      <div className="min-h-0 flex-1 space-y-4 overflow-y-auto p-3 text-sm">
        {(stage?.summary || stage?.repeat?.note) && (
          <section className="space-y-1 text-xs text-muted-foreground">
            {stage.summary && <p>{stage.summary}</p>}
            {stage.repeat?.note && <p>{stage.repeat.note}</p>}
          </section>
        )}
        <section>
          <SectionTitle>Identity</SectionTitle>
          <div className="flex flex-wrap items-center gap-1.5">
            <OpKindBadge opKind={op.opKind} />
            {op.fusedOps.length > 0 && <FusionChip ops={op.fusedOps} />}
            <Badge className="text-[10px]">{op.backend}</Badge>
          </div>
        </section>

        {(op.shapes.in.length > 0 || op.shapes.out.length > 0) && (
          <section>
            <SectionTitle>Tensors</SectionTitle>
            <div className="space-y-1">
              {op.shapes.in.map((s, i) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: tensor input positions are stable slot identities
                <div key={`in-${i}`} className="flex items-center gap-2">
                  <span className="w-7 font-mono text-[10px] text-muted-foreground">in{i}</span>
                  <TensorShapeBadge shape={s} />
                </div>
              ))}
              {op.shapes.out.map((s, i) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: tensor output positions are stable slot identities
                <div key={`out-${i}`} className="flex items-center gap-2">
                  <span className="w-7 font-mono text-[10px] text-muted-foreground">out{i}</span>
                  <TensorShapeBadge shape={s} />
                </div>
              ))}
            </div>
          </section>
        )}

        {Object.keys(op.attrs).length > 0 && (
          <section>
            <SectionTitle>Model details</SectionTitle>
            <dl className="space-y-2 text-xs">
              {Object.entries(op.attrs).map(([key, value]) => (
                <div key={key}>
                  <dt className="font-mono text-muted-foreground">{key}</dt>
                  <dd className="break-words">
                    {typeof value === "object" ? JSON.stringify(value) : String(value)}
                  </dd>
                </div>
              ))}
            </dl>
          </section>
        )}

        {(op.kernels.length > 0 || routes.length > 0) && (
          <section>
            <SectionTitle>Lowering</SectionTitle>
            <div className="space-y-2">
              {op.kernels.map((k) => (
                <div key={k}>
                  <Link
                    href={`/systems/kernels?q=${encodeURIComponent(k)}`}
                    className="font-mono text-xs text-primary hover:underline"
                  >
                    {k}
                  </Link>
                </div>
              ))}
              {routes.map((r) => (
                <div key={r.id} className="rounded border bg-muted/30 p-2">
                  <div className="mb-1 flex items-center gap-1.5">
                    <QuantChip format={r.format} />
                    <span className="font-mono text-[10px] text-muted-foreground">
                      {r.rowBucket} · {r.epilogue}
                    </span>
                  </div>
                  <div className="font-mono text-[10px] text-muted-foreground">
                    tptg {r.schedule.threadsPerThreadgroup} · cols {r.schedule.colsPerThreadgroup}
                    {r.schedule.rowsPerThreadgroup !== undefined &&
                      ` · rows ${r.schedule.rowsPerThreadgroup}`}{" "}
                    · {r.schedule.reduction}
                  </div>
                  <div className="mt-1">
                    <CodeLink link={r.source} label="schedule row" />
                  </div>
                </div>
              ))}
            </div>
          </section>
        )}

        {(op.source || op.lowererSource) && (
          <section>
            <SectionTitle>Provenance</SectionTitle>
            <div className="flex flex-col items-start gap-1.5">
              {op.source && (
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  built <CodeLink link={op.source} />
                </div>
              )}
              {op.lowererSource && (
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  lowered <CodeLink link={op.lowererSource} />
                </div>
              )}
            </div>
          </section>
        )}

        {op.envFlagNames.length > 0 && (
          <section>
            <SectionTitle>Gates</SectionTitle>
            <div className="flex flex-col items-start gap-1">
              {op.envFlagNames.map((f) => (
                <EnvFlagChip key={f} name={f} />
              ))}
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <div className="mb-1.5 font-mono text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
      {children}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Explorer                                                            */
/* ------------------------------------------------------------------ */

export function OpDagExplorer(props: {
  spec: ModelSpec;
  routes: KernelRoute[];
  className?: string;
  height?: string;
}) {
  // nuqs reads useSearchParams(), which requires a Suspense boundary under
  // static export.
  return (
    <Suspense
      fallback={
        <div
          className={props.className}
          style={{ height: props.height ?? "calc(100vh - 3.5rem)" }}
        />
      }
    >
      <OpDagExplorerInner {...props} />
    </Suspense>
  );
}

function OpDagExplorerInner({
  spec,
  routes,
  className,
  height = "calc(100vh - 3.5rem)",
}: {
  spec: ModelSpec;
  routes: KernelRoute[];
  className?: string;
  height?: string;
}) {
  const [phase, setPhase] = useQueryState(
    "phase",
    parseAsStringLiteral(["decode", "prefill"] as const).withDefault("decode")
  );
  const { resolvedTheme } = useTheme();
  const activePhase = phase === "prefill" && spec.graphs.prefill ? "prefill" : "decode";
  const forwardOnly = spec.id === "gliner2" || spec.id === "gliner25" || spec.id === "qwen3-embedding";
  const [colorBy, setColorBy] = useQueryState(
    "colorBy",
    parseAsStringLiteral(["dtype", "backend", "kernel"] as const).withDefault("dtype")
  );
  const [selectedId, setSelectedId] = useQueryState("node", parseAsString);
  const [searchOpen, setSearchOpen] = useState(false);

  const graph = (
    phase === "prefill" && spec.graphs.prefill ? spec.graphs.prefill : spec.graphs.decode
  ) as PhaseGraph;
  const stageById = useMemo(() => new Map(spec.stages.map((s) => [s.id, s])), [spec.stages]);
  const routesById = useMemo(() => new Map(routes.map((r) => [r.id, r])), [routes]);

  const positions = useMemo(() => layoutGraph(graph), [graph]);

  const flowNodes = useMemo<OpFlowNode[]>(
    () =>
      graph.nodes.map((op) => {
        const pos = positions.get(op.id) ?? { x: 0, y: 0 };
        return {
          id: op.id,
          type: "op" as const,
          position: { x: pos.x - NODE_W / 2, y: pos.y - NODE_H / 2 },
          data: { op, color: opColor(op, colorBy), selected: op.id === selectedId },
          draggable: false,
        };
      }),
    [graph, positions, colorBy, selectedId]
  );

  const flowEdges = useMemo<FlowEdge[]>(
    () =>
      graph.edges.map((e) => ({
        id: e.id,
        source: e.from,
        target: e.to,
        type: "smoothstep",
        animated: false,
        style: {
          stroke:
            e.kind === "residual"
              ? "var(--primary)"
              : e.kind === "kv"
                ? "var(--kfam-kv)"
                : "var(--border)",
          strokeWidth:
            e.kind === "residual"
              ? 2.5
              : e.tensor?.bytes
                ? Math.min(4, 1 + Math.log10(e.tensor.bytes) / 3)
                : 1.5,
          strokeDasharray: e.kind === "kv" ? "5 4" : undefined,
        },
        markerEnd: { type: MarkerType.ArrowClosed, width: 14, height: 14 },
      })),
    [graph]
  );

  const selectedOp = graph.nodes.find((n) => n.id === selectedId);
  const selectedRoutes = (selectedOp?.kernelRouteIds ?? [])
    .map((id) => routesById.get(id))
    .filter((r): r is KernelRoute => !!r);

  const onNodeClick = useCallback(
    (_evt: unknown, node: FlowNode) => setSelectedId(node.id),
    [setSelectedId]
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setSearchOpen((v) => !v);
      }
      if (e.key === "Escape") setSelectedId(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [setSelectedId]);

  return (
    <div className={cn("relative", className)} style={{ height, minHeight: 480 }}>
      <div className="absolute inset-x-0 bottom-0 top-24 sm:top-16">
        <ReactFlow
          key={`${spec.id}:${activePhase}`}
          nodes={flowNodes}
          edges={flowEdges}
          nodeTypes={nodeTypes}
          onNodeClick={onNodeClick}
          onPaneClick={() => setSelectedId(null)}
          fitView
          minZoom={0.1}
          proOptions={{ hideAttribution: true }}
          colorMode={resolvedTheme === "dark" ? "dark" : "light"}
        >
          <Background gap={24} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>

      {/* Toolbar */}
      <div className="absolute left-3 right-3 top-3 z-10 flex flex-wrap items-center gap-2 rounded-lg border bg-background/95 p-1.5 shadow backdrop-blur">
        <ChoiceGroup
          label="Inference phase"
          value={activePhase}
          onValueChange={setPhase}
          options={[
            { value: "decode", label: forwardOnly ? "forward pass" : "decode" },
            { value: "prefill", label: "prefill", disabled: !spec.graphs.prefill },
          ]}
        />
        <Separator orientation="vertical" className="h-5" />
        <ChoiceGroup
          label="Color operations by"
          value={colorBy}
          onValueChange={setColorBy}
          options={[
            { value: "dtype", label: "dtype" },
            { value: "backend", label: "backend" },
            { value: "kernel", label: "kernel" },
          ]}
        />
        <Separator orientation="vertical" className="h-5" />
        <Button
          variant="ghost"
          size="sm"
          className="h-7 gap-1.5 text-xs"
          aria-label="Search operations"
          onClick={() => setSearchOpen(true)}
        >
          <Search className="size-3" /> ⌘K
        </Button>
      </div>

      {selectedOp && (
        <Inspector
          op={selectedOp}
          stage={stageById.get(selectedOp.stageId)}
          routes={selectedRoutes}
          onClose={() => setSelectedId(null)}
        />
      )}

      <CommandDialog open={searchOpen} onOpenChange={setSearchOpen}>
        <CommandInput placeholder="Search ops, kernels, files…" />
        <CommandList>
          <CommandEmpty>No results.</CommandEmpty>
          <CommandGroup heading="Ops">
            {graph.nodes.map((n) => (
              <CommandItem
                key={n.id}
                value={`${n.label ?? ""} ${n.opKind} ${n.id} ${n.kernels.join(" ")} ${n.source?.path ?? ""} ${n.lowererSource?.path ?? ""}`}
                onSelect={() => {
                  setSelectedId(n.id);
                  setSearchOpen(false);
                }}
              >
                <span className="truncate">{n.label ?? n.opKind}</span>
                <span className="ml-auto font-mono text-[10px] text-muted-foreground">
                  {n.opKind}
                </span>
              </CommandItem>
            ))}
          </CommandGroup>
        </CommandList>
      </CommandDialog>
    </div>
  );
}
