"use client";

import { cn } from "@antfly/design-system";
import {
  Background,
  BackgroundVariant,
  Controls,
  type Edge,
  MiniMap,
  type Node,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from "@xyflow/react";
import * as React from "react";
import { GraphLegend } from "./graph-legend";
import { GraphNodeComponent } from "./graph-node";
import { GraphSearch } from "./graph-search";
import { GraphStyle } from "./graph-style";
import { computeForceLayout } from "./layout";
import type { ForceGraphProps, GraphNode as GraphNodeType, InternalNodeData } from "./types";
import { useResizeObserver } from "./use-resize-observer";
import {
  assignTypeColors,
  buildRadiusFn,
  clampEdgeWidth,
  getFallbackColor,
  resolveTypeOrder,
} from "./utils";

const nodeTypes = { graphNode: GraphNodeComponent };

function ForceGraphInner<M = Record<string, unknown>>({
  data,
  colorConfig,
  layoutOptions,
  nodeSize,
  width: explicitWidth,
  height: explicitHeight,
  minHeight = 400,
  showMinimap = true,
  showControls = true,
  showSearch = true,
  showLegend = true,
  renderNode,
  renderTooltip,
  renderLegend,
  onNodeClick,
  className,
}: ForceGraphProps<M>) {
  const containerRef = React.useRef<HTMLDivElement>(null);
  const observed = useResizeObserver(containerRef);
  const reactFlow = useReactFlow();
  const uniqueId = React.useId();
  const graphId = `graph-${uniqueId.replace(/:/g, "")}`;

  const resolvedWidth = explicitWidth ?? observed.width;
  const resolvedHeight = explicitHeight ?? (observed.height > 0 ? observed.height : minHeight);

  const [selectedId, setSelectedId] = React.useState<string | null>(null);

  const onNodeClickRef = React.useRef(onNodeClick);
  onNodeClickRef.current = onNodeClick;

  const typeOrder = React.useMemo(
    () => resolveTypeOrder(data.nodes as GraphNodeType[]),
    [data.nodes]
  );
  const typeColorMap = React.useMemo(
    () => assignTypeColors(typeOrder, colorConfig),
    [typeOrder, colorConfig]
  );

  const typeIndexMap = React.useMemo(() => {
    const m = new Map<string, number>();
    typeOrder.forEach((t, i) => {
      m.set(t, i);
    });
    return m;
  }, [typeOrder]);

  const { rfNodes, rfEdges } = React.useMemo(() => {
    if (resolvedWidth <= 0 || resolvedHeight <= 0 || !data.nodes.length) {
      return { rfNodes: [] as Node<InternalNodeData>[], rfEdges: [] as Edge[] };
    }

    const positions = computeForceLayout(
      data as { nodes: GraphNodeType[]; edges: typeof data.edges },
      resolvedWidth,
      resolvedHeight,
      layoutOptions
    );
    const radiusFn = buildRadiusFn(data.nodes as GraphNodeType[], nodeSize);

    const nodes: Node<InternalNodeData>[] = (data.nodes as GraphNodeType[]).map((n) => {
      const pos = positions.get(n.id) ?? { x: 0, y: 0 };
      const colorVar = typeColorMap.get(n.type) ?? "var(--chart-1)";
      return {
        id: n.id,
        type: "graphNode",
        position: pos,
        data: {
          graphNode: n,
          radius: radiusFn(n.metric ?? 0),
          colorVar,
          selected: n.id === selectedId,
          renderNode: renderNode as ForceGraphProps["renderNode"],
          renderTooltip: renderTooltip as ForceGraphProps["renderTooltip"],
        },
      };
    });

    const nodeSet = new Set(data.nodes.map((n) => n.id));
    const edges: Edge[] = data.edges
      .filter((e) => nodeSet.has(e.source) && nodeSet.has(e.target))
      .map((e, i) => ({
        id: `e-${i}`,
        source: e.source,
        target: e.target,
        type: "straight",
        style: {
          stroke: "var(--graph-edge-stroke, var(--muted-foreground))",
          strokeWidth: clampEdgeWidth(e.weight),
          opacity: 0.4,
        },
      }));

    return { rfNodes: nodes, rfEdges: edges };
  }, [
    data,
    resolvedWidth,
    resolvedHeight,
    layoutOptions,
    nodeSize,
    typeColorMap,
    selectedId,
    renderNode,
    renderTooltip,
  ]);

  const [nodes, setNodes, onNodesChange] = useNodesState<Node<InternalNodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);

  React.useEffect(() => {
    setNodes(rfNodes);
    setEdges(rfEdges);
    if (rfNodes.length > 0) {
      setTimeout(() => {
        reactFlow.fitView({ padding: 0.15, duration: 300 });
      }, 50);
    }
  }, [rfNodes, rfEdges, setNodes, setEdges, reactFlow]);

  const handleNodeClick = React.useCallback((_: React.MouseEvent, node: Node) => {
    const nodeData = node.data as InternalNodeData;
    setSelectedId(node.id);
    onNodeClickRef.current?.(nodeData.graphNode as GraphNodeType<M>);
  }, []);

  const handleSearchSelect = React.useCallback(
    (nodeId: string) => {
      setSelectedId(nodeId);
      const node = rfNodes.find((n) => n.id === nodeId);
      if (node) {
        reactFlow.setCenter(node.position.x, node.position.y, { zoom: 1.5, duration: 400 });
      }
    },
    [rfNodes, reactFlow]
  );

  const minimapNodeColor = React.useCallback(
    (node: Node) => {
      const data = node.data as InternalNodeData | undefined;
      if (!data?.graphNode) return "#6b7280";
      const idx = typeIndexMap.get(data.graphNode.type) ?? 0;
      return getFallbackColor(idx);
    },
    [typeIndexMap]
  );

  if (!data.nodes.length) {
    return (
      <div
        ref={containerRef}
        className={cn(
          "antfly-graph flex items-center justify-center rounded-md bg-muted/30 text-sm text-muted-foreground",
          className
        )}
        style={{ minHeight }}
      >
        No graph data
      </div>
    );
  }

  return (
    <div
      data-graph={graphId}
      ref={containerRef}
      className={cn(
        "antfly-graph relative w-full rounded-md overflow-hidden bg-muted/30",
        !explicitHeight && "h-full",
        className
      )}
      style={{
        height: explicitHeight ?? undefined,
        minHeight,
      }}
    >
      <GraphStyle id={graphId} colorConfig={colorConfig} typeOrder={typeOrder} />
      {showSearch && (
        <GraphSearch nodes={data.nodes as GraphNodeType[]} onSelect={handleSearchSelect} />
      )}
      {showLegend &&
        (renderLegend ? (
          renderLegend(typeColorMap)
        ) : (
          <GraphLegend
            typeColors={typeColorMap}
            typeLabels={
              colorConfig
                ? Object.entries(colorConfig).reduce<Record<string, string>>((labels, [k, v]) => {
                    if (v.label) {
                      labels[k] = v.label;
                    }
                    return labels;
                  }, {})
                : undefined
            }
            className="absolute bottom-3 left-3 z-[5]"
          />
        ))}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={handleNodeClick}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.1}
        maxZoom={4}
        proOptions={{ hideAttribution: true }}
        nodesDraggable
        panOnDrag
        zoomOnScroll
        zoomOnPinch
        defaultEdgeOptions={{
          type: "straight",
          style: {
            stroke: "var(--graph-edge-stroke, var(--muted-foreground))",
            strokeWidth: 1.5,
            opacity: 0.4,
          },
        }}
      >
        <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
        {showControls && (
          <Controls
            position="bottom-right"
            showInteractive={false}
            style={{ transform: "scale(0.85)", transformOrigin: "bottom right" }}
          />
        )}
        {showMinimap && (
          <MiniMap
            position="top-right"
            nodeColor={minimapNodeColor}
            maskColor="rgba(0,0,0,0.08)"
            pannable
            zoomable
            style={{ width: 80, height: 56 }}
          />
        )}
      </ReactFlow>
    </div>
  );
}

export function ForceGraph<M = Record<string, unknown>>(props: ForceGraphProps<M>) {
  return (
    <ReactFlowProvider>
      <ForceGraphInner {...props} />
    </ReactFlowProvider>
  );
}
