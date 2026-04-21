import { ReloadIcon } from "@radix-ui/react-icons";
import { Clock, GitBranch, Hash, Network, Plus, RotateCcw, X, Zap } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { BackendInfoBar } from "@/components/playground/BackendInfoBar";
import { NoModelsGuide } from "@/components/playground/NoModelsGuide";
import type { SamplePreset } from "@/components/playground/SamplePresets";
import { SamplePresets } from "@/components/playground/SamplePresets";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { useApiConfig } from "@/hooks/use-api-config";
import { fetchWithRetry } from "@/lib/utils";

// RecognizeResponse types matching Termite /api/recognize
interface RecognizeEntity {
  text: string;
  label: string;
  start?: number;
  end?: number;
  score: number;
}

interface RecognizeRelation {
  head: RecognizeEntity;
  tail: RecognizeEntity;
  label: string;
  score: number;
}

interface RecognizeResponse {
  model: string;
  entities: RecognizeEntity[][];
  relations?: RecognizeRelation[][];
}

// Graph visualization types (derived from RecognizeResponse)
interface KGNode {
  id: string;
  canonical_name: string;
  type: string;
  confidence: number;
}

interface KGEdge {
  id: string;
  source_id: string;
  target_id: string;
  type: string;
  confidence: number;
}

interface KGResult {
  model: string;
  nodes: KGNode[];
  edges: KGEdge[];
}

interface ModelInfo {
  capabilities?: string[];
}

interface ModelsResponse {
  recognizers: Record<string, ModelInfo>;
  [key: string]: Record<string, ModelInfo>;
}

interface ResolverConfig {
  similarity_threshold: number;
  type_must_match: boolean;
  min_entity_confidence: number;
  min_relation_confidence: number;
  deduplicate_relations: boolean;
  track_provenance: boolean;
}

// Default labels
const DEFAULT_ENTITY_LABELS = ["person", "organization", "location", "date"];
const DEFAULT_RELATION_LABELS = ["founded", "works_at", "located_in", "ceo_of", "acquired"];

// Entity type colors
const ENTITY_TYPE_COLORS: Record<string, string> = {
  person: "#3b82f6", // blue
  organization: "#22c55e", // green
  location: "#a855f7", // purple
  date: "#f59e0b", // amber
  product: "#ec4899", // pink
  event: "#06b6d4", // cyan
  default: "#6b7280", // gray
};

const STORAGE_KEY = "antfarm-playground-knowledge-graph";

const SAMPLE_TEXTS = [
  "Elon Musk founded SpaceX in 2002. He is also the CEO of Tesla.",
  "SpaceX is headquartered in Hawthorne, California.",
  "Tesla acquired SolarCity in 2016 and is based in Austin, Texas.",
];

// Convert RecognizeResponse to graph nodes and edges for visualization.
// When resolver is used, entities[0] contains deduplicated entities and
// relations[0] contains resolved relations.
function buildGraphFromResponse(data: RecognizeResponse): KGResult {
  // Flatten all entities across text arrays.
  const allEntities: RecognizeEntity[] = [];
  for (const textEntities of data.entities) {
    allEntities.push(...textEntities);
  }

  // Deduplicate entities by (text, label) to build nodes.
  const nodeKey = (e: RecognizeEntity) => `${e.text.toLowerCase()}::${e.label.toLowerCase()}`;
  const nodeMap = new Map<string, KGNode>();
  let nodeIdx = 0;
  for (const e of allEntities) {
    const key = nodeKey(e);
    const existing = nodeMap.get(key);
    if (existing) {
      // Keep highest confidence.
      if (e.score > existing.confidence) {
        existing.confidence = e.score;
        existing.canonical_name = e.text;
      }
    } else {
      nodeMap.set(key, {
        id: `node-${nodeIdx++}`,
        canonical_name: e.text,
        type: e.label,
        confidence: e.score,
      });
    }
  }

  const nodes = Array.from(nodeMap.values());

  // Build edges from relations.
  const edges: KGEdge[] = [];
  if (data.relations) {
    let edgeIdx = 0;
    for (const textRelations of data.relations) {
      for (const rel of textRelations) {
        const sourceNode = nodeMap.get(nodeKey(rel.head));
        const targetNode = nodeMap.get(nodeKey(rel.tail));
        if (sourceNode && targetNode) {
          edges.push({
            id: `edge-${edgeIdx++}`,
            source_id: sourceNode.id,
            target_id: targetNode.id,
            type: rel.label,
            confidence: rel.score,
          });
        }
      }
    }
  }

  return { model: data.model, nodes, edges };
}

function getNodeColor(type: string): string {
  return ENTITY_TYPE_COLORS[type.toLowerCase()] || ENTITY_TYPE_COLORS.default;
}

// Get the actual model name (strip "rel:" prefix for REBEL models)
function getModelName(model: string): string {
  if (model.startsWith("rel:")) {
    return model.slice(4);
  }
  return model;
}

const KnowledgeGraphPlaygroundPage: React.FC = () => {
  const { termiteApiUrl } = useApiConfig();
  const [searchParams, setSearchParams] = useSearchParams();

  // Restore state from localStorage
  const [inputText, setInputText] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).inputText || "";
    } catch {
      /* ignore */
    }
    return "";
  });
  const [selectedModel, setSelectedModel] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).selectedModel || "";
    } catch {
      /* ignore */
    }
    return "";
  });
  const [entityLabels, setEntityLabels] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved).entityLabels;
        if (Array.isArray(parsed) && parsed.length > 0) return parsed;
      }
    } catch {
      /* ignore */
    }
    return DEFAULT_ENTITY_LABELS;
  });
  const [relationLabels, setRelationLabels] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved).relationLabels;
        if (Array.isArray(parsed) && parsed.length > 0) return parsed;
      }
    } catch {
      /* ignore */
    }
    return DEFAULT_RELATION_LABELS;
  });
  const [newEntityLabel, setNewEntityLabel] = useState("");
  const [newRelationLabel, setNewRelationLabel] = useState("");
  const [config, setConfig] = useState<ResolverConfig>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved).config;
        if (parsed && typeof parsed === "object") return parsed;
      }
    } catch {
      /* ignore */
    }
    return {
      similarity_threshold: 0.85,
      type_must_match: true,
      min_entity_confidence: 0.0,
      min_relation_confidence: 0.0,
      deduplicate_relations: true,
      track_provenance: true,
    };
  });
  const [result, setResult] = useState<KGResult | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [modelsLoaded, setModelsLoaded] = useState(false);
  const [selectedNode, setSelectedNode] = useState<KGNode | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<KGEdge | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  // Persist state to localStorage
  useEffect(() => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ inputText, selectedModel, entityLabels, relationLabels, config })
    );
  }, [inputText, selectedModel, entityLabels, relationLabels, config]);

  // Fetch available models on mount
  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        const response = await fetch(`${termiteApiUrl}/ml/v1/models`, {
          signal: controller.signal,
        });
        if (response.ok) {
          const data: ModelsResponse = await response.json();
          const recognizersMap = data.recognizers || {};

          // Filter for models with "relations" capability (REBEL, GLiNER multitask)
          const relationModels = Object.entries(recognizersMap)
            .filter(([, info]) => info.capabilities?.includes("relations"))
            .map(([name]) => name);

          // Mark REBEL models with prefix for special handling
          const models = relationModels.map((m) => {
            // REBEL models typically have "rebel" in the name
            if (m.toLowerCase().includes("rebel")) {
              return `rel:${m}`;
            }
            return m;
          });

          setAvailableModels(models);
          setSelectedModel((prev: string) =>
            prev && models.includes(prev) ? prev : models[0] || ""
          );
        }
      } catch {
        // Ignore fetch errors
      } finally {
        if (!controller.signal.aborted) {
          setModelsLoaded(true);
        }
      }
    })();
    return () => controller.abort();
  }, [termiteApiUrl]);

  // Handle ?model= URL param from Model Registry "Open in Playground"
  useEffect(() => {
    const modelParam = searchParams.get("model");
    if (modelParam && modelsLoaded && availableModels.includes(modelParam)) {
      setSelectedModel(modelParam);
      setSearchParams(
        (prev) => {
          prev.delete("model");
          return prev;
        },
        { replace: true }
      );
    }
  }, [searchParams, modelsLoaded, availableModels, setSearchParams]);

  // Check if the selected model is a REBEL model
  const isRebelModel = selectedModel.startsWith("rel:");

  const handleBuildGraph = useCallback(async () => {
    if (!inputText.trim()) {
      setError("Please enter some text to build a knowledge graph from");
      return;
    }

    if (!selectedModel) {
      setError("Please select a model");
      return;
    }

    // Only require entity labels for GLiNER models (REBEL doesn't need them)
    if (!isRebelModel && entityLabels.length === 0) {
      setError("Please add at least one entity label");
      return;
    }

    // Cancel any previous request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    abortControllerRef.current = new AbortController();
    setIsLoading(true);
    setError(null);
    setResult(null);
    setSelectedNode(null);
    setSelectedEdge(null);

    const startTime = performance.now();

    try {
      // Split input by double newlines to get multiple texts
      const texts = inputText
        .split(/\n\n+/)
        .map((t: string) => t.trim())
        .filter((t: string) => t.length > 0);

      // Build request body for /api/recognize with resolver config
      const requestBody: Record<string, unknown> = {
        model: getModelName(selectedModel),
        texts: texts,
        resolver: config,
      };

      // Only include labels for GLiNER models
      if (!isRebelModel) {
        requestBody.labels = entityLabels;
        if (relationLabels.length > 0) {
          requestBody.relation_labels = relationLabels;
        }
      }

      const response = await fetchWithRetry(`${termiteApiUrl}/ml/v1/recognize`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || `HTTP ${response.status}`);
      }

      const data: RecognizeResponse = await response.json();
      setResult(buildGraphFromResponse(data));
      setProcessingTime(performance.now() - startTime);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      setError(
        err instanceof Error
          ? err.message
          : "Failed to connect to Termite. Make sure Termite is running."
      );
    } finally {
      setIsLoading(false);
    }
  }, [inputText, selectedModel, isRebelModel, entityLabels, relationLabels, config, termiteApiUrl]);

  // Cmd+Enter shortcut
  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        handleBuildGraph();
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, [handleBuildGraph]);

  const handleReset = () => {
    setInputText("");
    setEntityLabels(DEFAULT_ENTITY_LABELS);
    setRelationLabels(DEFAULT_RELATION_LABELS);
    setNewEntityLabel("");
    setNewRelationLabel("");
    setConfig({
      similarity_threshold: 0.85,
      type_must_match: true,
      min_entity_confidence: 0.0,
      min_relation_confidence: 0.0,
      deduplicate_relations: true,
      track_provenance: true,
    });
    setResult(null);
    setError(null);
    setProcessingTime(null);
    setSelectedNode(null);
    setSelectedEdge(null);
    localStorage.removeItem(STORAGE_KEY);
  };

  const samplePresets: SamplePreset[] = [
    {
      name: "Tech Companies",
      description: "People, companies, and acquisitions",
      onLoad: () => setInputText(SAMPLE_TEXTS.join("\n\n")),
    },
  ];

  const handleAddEntityLabel = () => {
    const trimmed = newEntityLabel.trim().toLowerCase();
    if (trimmed && !entityLabels.includes(trimmed)) {
      setEntityLabels([...entityLabels, trimmed]);
      setNewEntityLabel("");
    }
  };

  const handleAddRelationLabel = () => {
    const trimmed = newRelationLabel.trim().toLowerCase();
    if (trimmed && !relationLabels.includes(trimmed)) {
      setRelationLabels([...relationLabels, trimmed]);
      setNewRelationLabel("");
    }
  };

  // Build a node map for quick lookup
  const nodeMap = useMemo(() => {
    if (!result) return new Map<string, KGNode>();
    return new Map(result.nodes.map((n) => [n.id, n]));
  }, [result]);

  // Simple graph visualization
  const graphVisualization = useMemo(() => {
    if (!result || result.nodes.length === 0) {
      return (
        <div className="h-80 flex items-center justify-center text-muted-foreground">
          <div className="text-center">
            <Network className="h-12 w-12 mx-auto mb-3 opacity-20" />
            <p>No graph to display</p>
          </div>
        </div>
      );
    }

    const width = 600;
    const height = 400;
    const nodeRadius = 30;
    const padding = 50;

    // Simple circular layout
    const nodePositions = new Map<string, { x: number; y: number }>();
    const nodeCount = result.nodes.length;
    result.nodes.forEach((node, i) => {
      const angle = (2 * Math.PI * i) / nodeCount - Math.PI / 2;
      const radiusX = (width - padding * 2) / 2 - nodeRadius;
      const radiusY = (height - padding * 2) / 2 - nodeRadius;
      nodePositions.set(node.id, {
        x: width / 2 + radiusX * Math.cos(angle),
        y: height / 2 + radiusY * Math.sin(angle),
      });
    });

    return (
      <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-80 bg-muted/30 rounded-lg">
        <title>Knowledge graph visualization</title>
        <defs>
          <marker
            id="arrowhead"
            markerWidth="10"
            markerHeight="7"
            refX="9"
            refY="3.5"
            orient="auto"
          >
            <polygon points="0 0, 10 3.5, 0 7" fill="#888" />
          </marker>
        </defs>

        {/* Edges */}
        {result.edges.map((edge) => {
          const source = nodePositions.get(edge.source_id);
          const target = nodePositions.get(edge.target_id);
          if (!source || !target) return null;

          // Calculate edge endpoints (stop at node boundary)
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const dist = Math.sqrt(dx * dx + dy * dy);
          const offsetX = (dx / dist) * nodeRadius;
          const offsetY = (dy / dist) * nodeRadius;

          const x1 = source.x + offsetX;
          const y1 = source.y + offsetY;
          const x2 = target.x - offsetX;
          const y2 = target.y - offsetY;

          // Midpoint for label
          const midX = (x1 + x2) / 2;
          const midY = (y1 + y2) / 2;

          const isSelected = selectedEdge?.id === edge.id;

          return (
            <g
              key={edge.id}
              className="cursor-pointer"
              role="button"
              tabIndex={0}
              onClick={() => setSelectedEdge(edge)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") setSelectedEdge(edge);
              }}
            >
              <line
                x1={x1}
                y1={y1}
                x2={x2}
                y2={y2}
                stroke={isSelected ? "#3b82f6" : "#888"}
                strokeWidth={isSelected ? 2 : 1}
                markerEnd="url(#arrowhead)"
              />
              <text
                x={midX}
                y={midY - 5}
                textAnchor="middle"
                className="text-[10px] fill-muted-foreground"
              >
                {edge.type}
              </text>
            </g>
          );
        })}

        {/* Nodes */}
        {result.nodes.map((node) => {
          const pos = nodePositions.get(node.id);
          if (!pos) return null;

          const isSelected = selectedNode?.id === node.id;
          const color = getNodeColor(node.type);

          return (
            <g
              key={node.id}
              className="cursor-pointer"
              role="button"
              tabIndex={0}
              onClick={() => setSelectedNode(node)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") setSelectedNode(node);
              }}
            >
              <circle
                cx={pos.x}
                cy={pos.y}
                r={nodeRadius}
                fill={color}
                fillOpacity={0.2}
                stroke={color}
                strokeWidth={isSelected ? 3 : 2}
              />
              <text
                x={pos.x}
                y={pos.y}
                textAnchor="middle"
                dominantBaseline="middle"
                className="text-xs font-medium fill-foreground pointer-events-none"
              >
                {node.canonical_name.length > 10
                  ? `${node.canonical_name.slice(0, 10)}...`
                  : node.canonical_name}
              </text>
              <text
                x={pos.x}
                y={pos.y + nodeRadius + 12}
                textAnchor="middle"
                className="text-[10px] fill-muted-foreground pointer-events-none"
              >
                {node.type}
              </text>
            </g>
          );
        })}
      </svg>
    );
  }, [result, selectedNode, selectedEdge]);

  return (
    <div className="h-full">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Knowledge Graph Playground</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Build knowledge graphs from text using REBEL or GLiNER models
          </p>
        </div>
        <div className="flex gap-2">
          <SamplePresets presets={samplePresets} />
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </div>
      </div>

      <BackendInfoBar />

      {modelsLoaded && availableModels.length === 0 && (
        <NoModelsGuide
          modelType="recognizer"
          requiredCapability="relations"
          typeName="relation extraction"
        />
      )}

      {/* Configuration Panel */}
      <Card className="mb-6">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Configuration</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Model and Build Button */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label htmlFor="model">Model</Label>
              <Select
                value={selectedModel}
                onValueChange={setSelectedModel}
                disabled={!modelsLoaded || availableModels.length === 0}
              >
                <SelectTrigger id="model">
                  <SelectValue
                    placeholder={
                      !modelsLoaded
                        ? "Loading models..."
                        : availableModels.length === 0
                          ? "No KG models available"
                          : "Select a model"
                    }
                  />
                </SelectTrigger>
                <SelectContent>
                  {availableModels.map((model) => (
                    <SelectItem key={model} value={model}>
                      {model.startsWith("rel:") ? `${model.slice(4)} (REBEL)` : `${model} (GLiNER)`}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {isRebelModel && (
                <p className="text-xs text-muted-foreground">
                  REBEL models extract 200+ relation types automatically
                </p>
              )}
            </div>

            <div className="space-y-2">
              <Label htmlFor="similarity">Similarity Threshold</Label>
              <Input
                id="similarity"
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={config.similarity_threshold}
                onChange={(e) =>
                  setConfig({
                    ...config,
                    similarity_threshold: Number.parseFloat(e.target.value) || 0.85,
                  })
                }
              />
            </div>

            <div className="space-y-2 flex items-end">
              <Button
                onClick={handleBuildGraph}
                disabled={isLoading || !inputText.trim() || !selectedModel}
                className="w-full"
              >
                {isLoading ? (
                  <>
                    <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                    Building
                  </>
                ) : (
                  <>
                    <Network className="h-4 w-4 mr-2" />
                    Build Graph
                  </>
                )}
              </Button>
            </div>
          </div>

          {/* Labels Configuration - only for GLiNER models */}
          {!isRebelModel && (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Entity Labels */}
              <div className="space-y-2">
                <Label>Entity Labels</Label>
                <div className="flex flex-wrap gap-2 mb-2">
                  {entityLabels.map((label) => (
                    <Badge
                      key={label}
                      variant="secondary"
                      style={{
                        backgroundColor: `${getNodeColor(label)}20`,
                        borderColor: getNodeColor(label),
                      }}
                      className="border gap-1"
                    >
                      {label}
                      <button
                        type="button"
                        onClick={() => setEntityLabels(entityLabels.filter((l) => l !== label))}
                        className="ml-1 hover:opacity-70"
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  ))}
                </div>
                <div className="flex gap-2">
                  <Input
                    placeholder="Add entity label..."
                    value={newEntityLabel}
                    onChange={(e) => setNewEntityLabel(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        handleAddEntityLabel();
                      }
                    }}
                    className="flex-1"
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleAddEntityLabel}
                    disabled={!newEntityLabel.trim()}
                  >
                    <Plus className="h-4 w-4" />
                  </Button>
                </div>
              </div>

              {/* Relation Labels */}
              <div className="space-y-2">
                <Label>Relation Labels</Label>
                <div className="flex flex-wrap gap-2 mb-2">
                  {relationLabels.map((label) => (
                    <Badge key={label} variant="outline" className="gap-1">
                      {label}
                      <button
                        type="button"
                        onClick={() => setRelationLabels(relationLabels.filter((l) => l !== label))}
                        className="ml-1 hover:opacity-70"
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  ))}
                </div>
                <div className="flex gap-2">
                  <Input
                    placeholder="Add relation label..."
                    value={newRelationLabel}
                    onChange={(e) => setNewRelationLabel(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        handleAddRelationLabel();
                      }
                    }}
                    className="flex-1"
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleAddRelationLabel}
                    disabled={!newRelationLabel.trim()}
                  >
                    <Plus className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </div>
          )}

          {/* Advanced Options */}
          <div className="flex flex-wrap gap-x-6 gap-y-2 text-sm">
            <label htmlFor="type-must-match" className="flex items-center gap-2">
              <Checkbox
                id="type-must-match"
                checked={config.type_must_match}
                onCheckedChange={(checked) =>
                  setConfig({ ...config, type_must_match: checked === true })
                }
              />
              Type must match for merge
            </label>
            <label htmlFor="deduplicate-relations" className="flex items-center gap-2">
              <Checkbox
                id="deduplicate-relations"
                checked={config.deduplicate_relations}
                onCheckedChange={(checked) =>
                  setConfig({ ...config, deduplicate_relations: checked === true })
                }
              />
              Deduplicate relations
            </label>
            <label htmlFor="track-provenance" className="flex items-center gap-2">
              <Checkbox
                id="track-provenance"
                checked={config.track_provenance}
                onCheckedChange={(checked) =>
                  setConfig({ ...config, track_provenance: checked === true })
                }
              />
              Track provenance
            </label>
          </div>
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <div className="mb-6 p-4 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive text-sm">
          {error}
        </div>
      )}

      {/* Results Stats Bar */}
      {result && (
        <div className="mb-6 flex flex-wrap items-center gap-3">
          <Badge variant="secondary" className="gap-1.5">
            <Hash className="h-3 w-3" />
            {result.nodes.length} nodes
          </Badge>
          <Badge variant="secondary" className="gap-1.5">
            <GitBranch className="h-3 w-3" />
            {result.edges.length} edges
          </Badge>
          <Badge variant="secondary" className="gap-1.5">
            <Zap className="h-3 w-3" />
            {result.model}
          </Badge>
          {processingTime && (
            <Badge variant="outline" className="gap-1.5">
              <Clock className="h-3 w-3" />
              {processingTime.toFixed(0)}ms
            </Badge>
          )}
        </div>
      )}

      {/* Main Content */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Input Text</CardTitle>
              {inputText && (
                <span className="text-xs text-muted-foreground">{inputText.length} characters</span>
              )}
            </div>
          </CardHeader>
          <CardContent className="flex-1">
            <Textarea
              placeholder="Enter text to build a knowledge graph from. Separate multiple documents with blank lines..."
              className="h-80 resize-y font-mono text-sm"
              value={inputText}
              onChange={(e) => setInputText(e.target.value)}
            />
            <p className="text-xs text-muted-foreground mt-2">
              Tip: Separate multiple documents with blank lines for multi-document extraction.
            </p>
          </CardContent>
        </Card>

        {/* Output Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">{result ? "Knowledge Graph" : "Preview"}</CardTitle>
          </CardHeader>
          <CardContent className="flex-1 overflow-hidden">
            {result ? (
              <Tabs defaultValue="visual" className="h-full">
                <TabsList className="mb-4">
                  <TabsTrigger value="visual">Visual</TabsTrigger>
                  <TabsTrigger value="nodes">Nodes ({result.nodes.length})</TabsTrigger>
                  <TabsTrigger value="edges">Edges ({result.edges.length})</TabsTrigger>
                </TabsList>

                <TabsContent value="visual" className="mt-0">
                  {graphVisualization}
                  {(selectedNode || selectedEdge) && (
                    <div className="mt-4 p-3 bg-muted/50 rounded-lg border text-sm">
                      {selectedNode && (
                        <div>
                          <div className="font-medium">{selectedNode.canonical_name}</div>
                          <div className="text-muted-foreground">Type: {selectedNode.type}</div>
                          <div className="text-muted-foreground">
                            Confidence: {(selectedNode.confidence * 100).toFixed(1)}%
                          </div>
                        </div>
                      )}
                      {selectedEdge && (
                        <div>
                          <div className="font-medium">
                            {nodeMap.get(selectedEdge.source_id)?.canonical_name ||
                              selectedEdge.source_id}
                            {" → "}
                            {nodeMap.get(selectedEdge.target_id)?.canonical_name ||
                              selectedEdge.target_id}
                          </div>
                          <div className="text-muted-foreground">Relation: {selectedEdge.type}</div>
                          <div className="text-muted-foreground">
                            Confidence: {(selectedEdge.confidence * 100).toFixed(1)}%
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </TabsContent>

                <TabsContent value="nodes" className="mt-0 h-80 overflow-y-auto">
                  <div className="rounded-lg border overflow-hidden">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50 sticky top-0">
                        <tr>
                          <th className="text-left px-3 py-2 font-medium">Name</th>
                          <th className="text-left px-3 py-2 font-medium">Type</th>
                          <th className="text-right px-3 py-2 font-medium">Confidence</th>
                        </tr>
                      </thead>
                      <tbody>
                        {result.nodes.map((node) => (
                          <tr key={node.id} className="border-t hover:bg-muted/30">
                            <td className="px-3 py-2">
                              <div>{node.canonical_name}</div>
                            </td>
                            <td className="px-3 py-2">
                              <Badge
                                variant="outline"
                                style={{
                                  backgroundColor: `${getNodeColor(node.type)}20`,
                                  borderColor: getNodeColor(node.type),
                                }}
                              >
                                {node.type}
                              </Badge>
                            </td>
                            <td className="px-3 py-2 text-right tabular-nums">
                              {(node.confidence * 100).toFixed(1)}%
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </TabsContent>

                <TabsContent value="edges" className="mt-0 h-80 overflow-y-auto">
                  <div className="rounded-lg border overflow-hidden">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50 sticky top-0">
                        <tr>
                          <th className="text-left px-3 py-2 font-medium">Source</th>
                          <th className="text-left px-3 py-2 font-medium">Relation</th>
                          <th className="text-left px-3 py-2 font-medium">Target</th>
                          <th className="text-right px-3 py-2 font-medium">Confidence</th>
                        </tr>
                      </thead>
                      <tbody>
                        {result.edges.map((edge) => (
                          <tr key={edge.id} className="border-t hover:bg-muted/30">
                            <td className="px-3 py-2">
                              {nodeMap.get(edge.source_id)?.canonical_name || edge.source_id}
                            </td>
                            <td className="px-3 py-2">
                              <Badge variant="outline">{edge.type}</Badge>
                            </td>
                            <td className="px-3 py-2">
                              {nodeMap.get(edge.target_id)?.canonical_name || edge.target_id}
                            </td>
                            <td className="px-3 py-2 text-right tabular-nums">
                              {(edge.confidence * 100).toFixed(1)}%
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </TabsContent>
              </Tabs>
            ) : (
              <div className="h-80 flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <Network className="h-12 w-12 mx-auto mb-3 opacity-20" />
                  <p>Enter text and click "Build Graph" to see results</p>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Help text */}
      <div className="mt-6 text-xs text-muted-foreground space-y-1">
        <p>
          <strong>Knowledge Graphs:</strong> Extracts entities and relationships from text, then
          resolves co-references (e.g., "Elon Musk" and "Musk" → single entity) using similarity
          matching.
        </p>
        <p>
          <strong>REBEL (Recommended):</strong> End-to-end relation extraction using seq2seq
          generation. Supports 200+ relation types automatically without needing predefined labels.
        </p>
        <p>
          <strong>GLiNER:</strong> Zero-shot entity and relation extraction with custom labels.
          Requires specifying entity and relation types to extract.
        </p>
      </div>
    </div>
  );
};

export default KnowledgeGraphPlaygroundPage;
