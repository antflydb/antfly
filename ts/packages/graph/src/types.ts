import type * as React from "react";

export interface GraphNode<M = Record<string, unknown>> {
  id: string;
  label: string;
  type: string;
  metric?: number;
  metadata?: M;
}

export interface GraphEdge {
  source: string;
  target: string;
  weight?: number;
}

export interface GraphData<M = Record<string, unknown>> {
  nodes: GraphNode<M>[];
  edges: GraphEdge[];
}

export type GraphColorConfig = {
  [nodeType: string]: {
    label?: string;
  } & (
    | { color?: string; theme?: never }
    | { color?: never; theme: { light: string; dark: string } }
  );
};

export interface ForceLayoutOptions {
  ticks?: number;
  linkDistance?: number;
  linkStrength?: number;
  chargeStrength?: number;
  collideRadius?: number;
}

export interface NodeSizeConfig {
  minRadius?: number;
  maxRadius?: number;
}

export interface DefaultNodeProps {
  color: string;
  radius: number;
  label: string;
  selected: boolean;
}

export type RenderNodeFn<M = Record<string, unknown>> = (
  node: GraphNode<M>,
  defaults: DefaultNodeProps
) => React.ReactNode;

export type RenderTooltipFn<M = Record<string, unknown>> = (node: GraphNode<M>) => React.ReactNode;

export type RenderLegendFn = (typeColorMap: Map<string, string>) => React.ReactNode;

export interface ForceGraphProps<M = Record<string, unknown>> {
  data: GraphData<M>;
  colorConfig?: GraphColorConfig;
  layoutOptions?: ForceLayoutOptions;
  nodeSize?: NodeSizeConfig;
  width?: number;
  height?: number;
  minHeight?: number;
  showMinimap?: boolean;
  showControls?: boolean;
  showSearch?: boolean;
  showLegend?: boolean;
  renderNode?: RenderNodeFn<M>;
  renderTooltip?: RenderTooltipFn<M>;
  renderLegend?: RenderLegendFn;
  onNodeClick?: (node: GraphNode<M>) => void;
  onEdgeClick?: (edge: GraphEdge) => void;
  className?: string;
}

export interface GraphLegendProps {
  typeLabels?: Record<string, string>;
  typeColors: Map<string, string>;
  className?: string;
}

export interface GraphSearchProps {
  nodes: GraphNode[];
  onSelect: (nodeId: string) => void;
  className?: string;
}

export interface InternalNodeData {
  [key: string]: unknown;
  graphNode: GraphNode;
  radius: number;
  colorVar: string;
  selected: boolean;
  renderNode?: RenderNodeFn;
  renderTooltip?: RenderTooltipFn;
}
