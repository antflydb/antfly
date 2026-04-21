import { type EmbedResponse, TermiteClient } from "@antfly/termite-sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import {
  ArrowDownUp,
  Clock,
  FileText,
  Hash,
  Maximize2,
  Minimize2,
  Plus,
  RotateCcw,
  Trash2,
  Zap,
} from "lucide-react";
import type React from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { BackendInfoBar } from "@/components/playground/BackendInfoBar";
import { NoModelsGuide } from "@/components/playground/NoModelsGuide";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { useApiConfig } from "@/hooks/use-api-config";
import { fetchWithRetry } from "@/lib/utils";

interface ModelInfo {
  capabilities?: string[];
}

interface ModelsResponse {
  embedders: Record<string, ModelInfo>;
  [key: string]: Record<string, ModelInfo>;
}

interface ScoredDocument {
  index: number;
  text: string;
  similarity: number;
  embedding: number[];
}

function cosineSimilarity(a: number[], b: number[]): number {
  let dot = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  const denom = Math.sqrt(normA) * Math.sqrt(normB);
  return denom === 0 ? 0 : dot / denom;
}

const SAMPLE_QUERY = "How does photosynthesis work in plants?";

const SAMPLE_DOCUMENTS = [
  "Photosynthesis is the process by which green plants and certain other organisms transform light energy into chemical energy. During photosynthesis, plants capture light energy and use it to convert water and carbon dioxide into oxygen and glucose.",
  "The water cycle describes how water evaporates from the surface of the earth, rises into the atmosphere, cools and condenses into clouds, and falls back to the surface as precipitation.",
  "Chloroplasts are the organelles responsible for photosynthesis in plant cells. They contain chlorophyll, the green pigment that absorbs light energy, primarily from the blue and red wavelengths.",
  "The French Revolution was a period of radical political and societal change in France that began with the Estates General of 1789 and ended with the formation of the French Consulate in November 1799.",
  "Plants use the Calvin cycle, also known as the light-independent reactions, to convert CO2 into organic molecules. This process takes place in the stroma of chloroplasts and uses ATP and NADPH produced during the light reactions.",
  "Machine learning is a subset of artificial intelligence that enables systems to learn and improve from experience without being explicitly programmed.",
];

/** Tiny sparkline for embedding vectors: renders the first N dimensions as vertical bars. */
function EmbeddingSparkline({
  values,
  width = 120,
  height = 24,
}: {
  values: number[];
  width?: number;
  height?: number;
}) {
  const dims = values.slice(0, 60);
  if (dims.length === 0) return null;

  const max = Math.max(...dims.map(Math.abs));
  const mid = height / 2;
  const barW = width / dims.length;

  return (
    <svg
      width={width}
      height={height}
      className="shrink-0 rounded"
      aria-label="Embedding vector preview"
    >
      <rect width={width} height={height} className="fill-muted/50" rx={2} />
      <line x1={0} y1={mid} x2={width} y2={mid} className="stroke-border" strokeWidth={0.5} />
      {dims.map((v, i) => {
        const norm = max === 0 ? 0 : v / max;
        const barH = Math.abs(norm) * mid;
        const y = norm >= 0 ? mid - barH : mid;
        return (
          <rect
            // biome-ignore lint/suspicious/noArrayIndexKey: SVG bars are positional
            key={i}
            x={i * barW}
            y={y}
            width={Math.max(barW - 0.3, 0.3)}
            height={barH}
            className={norm >= 0 ? "fill-info-500/70" : "fill-warning-500/70"}
          />
        );
      })}
    </svg>
  );
}

/** Continuous gradient bar mapping similarity [0,1] to a color ramp. */
function SimilarityBar({ score, maxScore }: { score: number; maxScore: number }) {
  const pct = maxScore > 0 ? Math.max(0, Math.min(100, (score / maxScore) * 100)) : 0;

  // Gradient from muted through amber to green
  const hue = Math.round(pct * 1.2); // 0=0° (red-ish) → 120° (green)
  const saturation = 70;
  const lightness = 45;

  return (
    <div className="w-full bg-muted rounded-full h-2 overflow-hidden">
      <div
        className="h-2 rounded-full transition-all duration-500"
        style={{
          width: `${pct}%`,
          backgroundColor: `hsl(${hue}, ${saturation}%, ${lightness}%)`,
        }}
      />
    </div>
  );
}

const EmbeddingPlaygroundPage: React.FC = () => {
  const { termiteApiUrl } = useApiConfig();
  const [query, setQuery] = useState("");
  const [documents, setDocuments] = useState<string[]>([""]);
  const [selectedModel, setSelectedModel] = useState("");
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [modelsLoaded, setModelsLoaded] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [results, setResults] = useState<ScoredDocument[] | null>(null);
  const [queryEmbedding, setQueryEmbedding] = useState<number[] | null>(null);
  const [dimensions, setDimensions] = useState<number | null>(null);
  const [showSparklines, setShowSparklines] = useState(true);
  const abortControllerRef = useRef<AbortController | null>(null);

  const termiteClient = useMemo(
    () => new TermiteClient({ baseUrl: termiteApiUrl }),
    [termiteApiUrl]
  );

  // Fetch available embedder models
  useEffect(() => {
    const fetchModels = async () => {
      try {
        const response = await fetchWithRetry(`${termiteApiUrl}/ml/v1/models`);
        if (response.ok) {
          const data: ModelsResponse = await response.json();
          const embedders = Object.keys(data.embedders || {});
          setAvailableModels(embedders);
          if (embedders.length > 0) {
            const builtin = embedders.find((m) => m.includes("bge-small"));
            setSelectedModel(builtin || embedders[0]);
          }
        }
      } catch {
        console.error("Failed to fetch models");
      } finally {
        setModelsLoaded(true);
      }
    };
    fetchModels();
  }, [termiteApiUrl]);

  const handleEmbed = async () => {
    const nonEmptyDocs = documents.filter((d) => d.trim());

    if (!query.trim()) {
      setError("Please enter a query");
      return;
    }
    if (nonEmptyDocs.length === 0) {
      setError("Please add at least one document");
      return;
    }
    if (!selectedModel) {
      setError("Please select a model");
      return;
    }

    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    abortControllerRef.current = new AbortController();
    setIsLoading(true);
    setError(null);
    setResults(null);
    setQueryEmbedding(null);
    setDimensions(null);

    const startTime = performance.now();

    try {
      // Embed query and all documents in one call
      const allTexts = [query, ...nonEmptyDocs];
      const response: EmbedResponse = await termiteClient.embed(selectedModel, allTexts);

      const embeddings = response.embeddings;
      if (!embeddings || embeddings.length < 2) {
        throw new Error("No embeddings returned");
      }

      const qEmb = embeddings[0];
      setQueryEmbedding(qEmb);
      setDimensions(qEmb.length);

      // Calculate similarities
      const scored: ScoredDocument[] = nonEmptyDocs.map((text, i) => ({
        index: i,
        text,
        similarity: cosineSimilarity(qEmb, embeddings[i + 1]),
        embedding: embeddings[i + 1],
      }));

      // Sort by similarity descending
      scored.sort((a, b) => b.similarity - a.similarity);

      setResults(scored);
      setProcessingTime(performance.now() - startTime);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      setError(
        err instanceof Error ? err.message : `Failed to connect to Termite at ${termiteApiUrl}`
      );
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = () => {
    setQuery("");
    setDocuments([""]);
    setResults(null);
    setQueryEmbedding(null);
    setDimensions(null);
    setError(null);
    setProcessingTime(null);
  };

  const loadSampleData = () => {
    setQuery(SAMPLE_QUERY);
    setDocuments(SAMPLE_DOCUMENTS);
  };

  const addDocument = () => {
    setDocuments([...documents, ""]);
  };

  const removeDocument = (index: number) => {
    if (documents.length <= 1) return;
    setDocuments(documents.filter((_, i) => i !== index));
  };

  const updateDocument = (index: number, text: string) => {
    const updated = [...documents];
    updated[index] = text;
    setDocuments(updated);
  };

  const maxSimilarity = results ? Math.max(...results.map((r) => r.similarity)) : 1;

  return (
    <div className="h-full">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Embedding Playground</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Explore vector similarity between a query and documents using embedding models
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={loadSampleData}>
            <FileText className="h-4 w-4 mr-2" />
            Load Sample
          </Button>
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </div>
      </div>

      <BackendInfoBar />

      {modelsLoaded && availableModels.length === 0 && (
        <NoModelsGuide modelType="embedder" typeName="embedder" />
      )}

      {/* Configuration Panel */}
      <Card className="mb-6">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Configuration</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {/* Model Selection */}
            <div className="space-y-2">
              <Label htmlFor="model">Embedder Model</Label>
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
                          ? "No models available"
                          : "Select a model"
                    }
                  />
                </SelectTrigger>
                <SelectContent>
                  {availableModels.map((model) => (
                    <SelectItem key={model} value={model}>
                      {model}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Query */}
            <div className="space-y-2">
              <Label htmlFor="query">Query</Label>
              <Input
                id="query"
                placeholder="Enter a query to compare against documents..."
                value={query}
                onChange={(e) => setQuery(e.target.value)}
              />
            </div>

            {/* Embed Button */}
            <div className="space-y-2 flex items-end">
              <Button
                onClick={handleEmbed}
                disabled={
                  isLoading ||
                  !query.trim() ||
                  !selectedModel ||
                  documents.filter((d) => d.trim()).length === 0
                }
                className="w-full"
              >
                {isLoading ? (
                  <>
                    <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                    Embedding
                  </>
                ) : (
                  <>
                    <ArrowDownUp className="h-4 w-4 mr-2" />
                    Embed &amp; Compare
                  </>
                )}
              </Button>
            </div>
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
      {results && (
        <div className="mb-6 flex flex-wrap items-center gap-3">
          <Badge variant="secondary" className="gap-1.5">
            <Hash className="h-3 w-3" />
            {results.length} documents
          </Badge>
          <Badge variant="secondary" className="gap-1.5">
            <Zap className="h-3 w-3" />
            {selectedModel}
          </Badge>
          {dimensions && (
            <Badge variant="secondary" className="gap-1.5">
              <Maximize2 className="h-3 w-3" />
              {dimensions}d vectors
            </Badge>
          )}
          {processingTime && (
            <Badge variant="outline" className="gap-1.5">
              <Clock className="h-3 w-3" />
              {processingTime.toFixed(0)}ms
            </Badge>
          )}
          <Button
            variant="ghost"
            size="sm"
            className="h-6 ml-auto gap-1 text-xs text-muted-foreground"
            onClick={() => setShowSparklines(!showSparklines)}
          >
            {showSparklines ? <Minimize2 className="h-3 w-3" /> : <Maximize2 className="h-3 w-3" />}
            {showSparklines ? "Hide" : "Show"} vectors
          </Button>
        </div>
      )}

      {/* Main Content - Side by Side */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Panel - Documents */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Documents</CardTitle>
              <Button variant="outline" size="sm" onClick={addDocument}>
                <Plus className="h-4 w-4 mr-1" />
                Add
              </Button>
            </div>
          </CardHeader>
          <CardContent className="flex-1 space-y-3 max-h-[600px] overflow-y-auto">
            {documents.map((doc, index) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: document inputs identified by position
              <div key={index} className="space-y-1">
                <div className="flex items-center justify-between">
                  <Label className="text-xs text-muted-foreground">Document {index + 1}</Label>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => removeDocument(index)}
                    disabled={documents.length <= 1}
                    className="h-6 w-6 p-0 text-muted-foreground hover:text-destructive"
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                </div>
                <Textarea
                  placeholder="Enter document text..."
                  className="resize-y font-mono text-sm min-h-16"
                  value={doc}
                  onChange={(e) => updateDocument(index, e.target.value)}
                />
              </div>
            ))}
          </CardContent>
        </Card>

        {/* Output Panel - Similarity Results */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">{results ? "Similarity Results" : "Preview"}</CardTitle>
          </CardHeader>
          <CardContent className="flex-1 overflow-hidden">
            {results ? (
              <div className="max-h-[600px] overflow-y-auto space-y-3">
                {/* Query embedding preview */}
                {queryEmbedding && showSparklines && (
                  <div className="p-3 bg-muted/30 rounded-lg border border-dashed space-y-2">
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-medium text-muted-foreground">
                        Query vector
                      </span>
                      <EmbeddingSparkline values={queryEmbedding} />
                    </div>
                  </div>
                )}

                {results.map((doc, rank) => (
                  <div key={doc.index} className="p-3 bg-muted/30 rounded-lg border space-y-2">
                    <div className="flex items-center gap-2">
                      <span className="flex items-center justify-center w-6 h-6 rounded-full bg-primary/10 text-primary text-xs font-bold shrink-0">
                        {rank + 1}
                      </span>
                      <span className="text-xs text-muted-foreground">
                        Document {doc.index + 1}
                      </span>
                      <span className="ml-auto text-sm font-mono font-medium tabular-nums">
                        {doc.similarity.toFixed(4)}
                      </span>
                    </div>
                    <SimilarityBar score={doc.similarity} maxScore={maxSimilarity} />
                    {showSparklines && (
                      <div className="flex items-center gap-2 pt-1">
                        <EmbeddingSparkline values={doc.embedding} />
                        <span className="text-[10px] text-muted-foreground/60 font-mono">
                          [
                          {doc.embedding
                            .slice(0, 3)
                            .map((v) => v.toFixed(3))
                            .join(", ")}
                          , ...]
                        </span>
                      </div>
                    )}
                    <p className="text-sm leading-relaxed">{doc.text}</p>
                  </div>
                ))}
              </div>
            ) : (
              <div className="h-80 flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <ArrowDownUp className="h-12 w-12 mx-auto mb-3 opacity-20" />
                  <p>Add documents and click "Embed &amp; Compare" to see similarity scores</p>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Help text */}
      <div className="mt-6 text-xs text-muted-foreground space-y-1">
        <p>
          <strong>Bi-Encoder Embeddings:</strong> Each text is independently embedded into a
          high-dimensional vector. Cosine similarity between vectors measures semantic relatedness.
          Faster than cross-encoder reranking but less precise for fine-grained relevance.
        </p>
        <p>
          <strong>Scores:</strong> Cosine similarity ranges from -1 (opposite) to 1 (identical).
          Values above 0.7 typically indicate strong semantic similarity. The sparkline shows the
          first 60 dimensions of each embedding vector.
        </p>
      </div>
    </div>
  );
};

export default EmbeddingPlaygroundPage;
