import { ReloadIcon } from "@radix-ui/react-icons";
import { ArrowUpDown, Clock, Hash, Plus, RotateCcw, Trash2, Zap } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { BackendInfoBar } from "@/components/playground/BackendInfoBar";
import { NoModelsGuide } from "@/components/playground/NoModelsGuide";
import type { SamplePreset } from "@/components/playground/SamplePresets";
import { SamplePresets } from "@/components/playground/SamplePresets";
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

interface RerankResponse {
  model: string;
  scores: number[];
}

interface ModelInfo {
  capabilities?: string[];
}

interface ModelsResponse {
  rerankers: Record<string, ModelInfo>;
  [key: string]: Record<string, ModelInfo>;
}

interface RankedDocument {
  index: number;
  text: string;
  score: number;
  rank: number;
}

const STORAGE_KEY = "antfarm-playground-reranking";

const SAMPLE_DATA = {
  photosynthesis: {
    name: "Photosynthesis",
    description: "Science documents with mixed relevance",
    query: "How does photosynthesis work in plants?",
    documents: [
      "Photosynthesis is the process by which green plants and certain other organisms transform light energy into chemical energy. During photosynthesis, plants capture light energy and use it to convert water and carbon dioxide into oxygen and glucose.",
      "The water cycle describes how water evaporates from the surface of the earth, rises into the atmosphere, cools and condenses into clouds, and falls back to the surface as precipitation.",
      "Chloroplasts are the organelles responsible for photosynthesis in plant cells. They contain chlorophyll, the green pigment that absorbs light energy, primarily from the blue and red wavelengths.",
      "The French Revolution was a period of radical political and societal change in France that began with the Estates General of 1789 and ended with the formation of the French Consulate in November 1799.",
      "Plants use the Calvin cycle, also known as the light-independent reactions, to convert CO2 into organic molecules. This process takes place in the stroma of chloroplasts and uses ATP and NADPH produced during the light reactions.",
      "Machine learning is a subset of artificial intelligence that enables systems to learn and improve from experience without being explicitly programmed.",
    ],
  },
};

const RerankingPlaygroundPage: React.FC = () => {
  const { termiteApiUrl } = useApiConfig();
  const [searchParams, setSearchParams] = useSearchParams();

  // Restore state from localStorage
  const [query, setQuery] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).query || "";
    } catch {
      /* ignore */
    }
    return "";
  });
  const [documents, setDocuments] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const docs = JSON.parse(saved).documents;
        if (Array.isArray(docs) && docs.length > 0) return docs;
      }
    } catch {
      /* ignore */
    }
    return [""];
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
  const [result, setResult] = useState<RerankResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [modelsLoaded, setModelsLoaded] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);
  const docKeyCounterRef = useRef(0);
  const [docKeys, setDocKeys] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const docs = JSON.parse(saved).documents;
        if (Array.isArray(docs) && docs.length > 0) {
          return docs.map((_: string, i: number) => `doc-init-${i}`);
        }
      }
    } catch {
      /* ignore */
    }
    return ["doc-init-0"];
  });

  // Persist state to localStorage
  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ query, documents, selectedModel }));
  }, [query, documents, selectedModel]);

  // Fetch available models on mount
  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        const response = await fetch(`${termiteApiUrl}/api/models`, {
          signal: controller.signal,
        });
        if (response.ok) {
          const data: ModelsResponse = await response.json();
          const rerankers = Object.keys(data.rerankers || {});
          setAvailableModels(rerankers);
          setSelectedModel((prev: string) => {
            if (prev && rerankers.includes(prev)) return prev;
            const builtin = rerankers.find((m) => m === "antfly-builtin-reranker");
            return builtin || rerankers[0] || "";
          });
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

  const handleRerank = useCallback(async () => {
    const nonEmptyDocs = documents.filter((d) => d.trim());

    if (!query.trim()) {
      setError("Please enter a search query");
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

    // Cancel any previous request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    abortControllerRef.current = new AbortController();
    setIsLoading(true);
    setError(null);
    setResult(null);

    const startTime = performance.now();

    try {
      const response = await fetchWithRetry(`${termiteApiUrl}/api/rerank`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: selectedModel,
          query: query,
          prompts: nonEmptyDocs,
        }),
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || `HTTP ${response.status}`);
      }

      const data: RerankResponse = await response.json();
      setResult(data);
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
  }, [query, documents, selectedModel, termiteApiUrl]);

  // Cmd+Enter shortcut
  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        handleRerank();
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, [handleRerank]);

  const nextDocKey = () => {
    docKeyCounterRef.current += 1;
    return `doc-${docKeyCounterRef.current}`;
  };

  const handleReset = () => {
    setQuery("");
    setDocuments([""]);
    setDocKeys([nextDocKey()]);
    setResult(null);
    setError(null);
    setProcessingTime(null);
    localStorage.removeItem(STORAGE_KEY);
  };

  const addDocument = () => {
    setDocuments([...documents, ""]);
    setDocKeys([...docKeys, nextDocKey()]);
    setResult(null);
  };

  const removeDocument = (index: number) => {
    if (documents.length <= 1) return;
    setDocuments(documents.filter((_, i) => i !== index));
    setDocKeys(docKeys.filter((_, i) => i !== index));
    setResult(null);
  };

  const updateDocument = (index: number, text: string) => {
    const updated = [...documents];
    updated[index] = text;
    setDocuments(updated);
    setResult(null);
  };

  // Get ranked documents sorted by score
  const getRankedDocuments = (): RankedDocument[] => {
    if (!result || !result.scores) return [];

    const nonEmptyDocs = documents.filter((d) => d.trim());
    const ranked = result.scores.map((score, index) => ({
      index,
      text: nonEmptyDocs[index] || "",
      score,
      rank: 0,
    }));

    // Sort by score descending
    ranked.sort((a, b) => b.score - a.score);

    // Assign ranks
    ranked.forEach((doc, i) => {
      doc.rank = i + 1;
    });

    return ranked;
  };

  // Get the max score for normalization
  const getMaxScore = (): number => {
    if (!result || !result.scores || result.scores.length === 0) return 1;
    return Math.max(...result.scores);
  };

  const getScoreColor = (score: number, maxScore: number) => {
    const normalized = maxScore > 0 ? score / maxScore : 0;
    if (normalized >= 0.7) return "bg-green-500";
    if (normalized >= 0.4) return "bg-yellow-500";
    return "bg-red-400";
  };

  const samplePresets: SamplePreset[] = Object.values(SAMPLE_DATA).map((sample) => ({
    name: sample.name,
    description: sample.description,
    onLoad: () => {
      setQuery(sample.query);
      setDocuments(sample.documents);
      setDocKeys(sample.documents.map(() => nextDocKey()));
    },
  }));

  return (
    <div className="h-full">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Reranking Playground</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Rerank documents by relevance to a query using cross-encoder models
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
        <NoModelsGuide modelType="reranker" typeName="reranker" />
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
                placeholder="Enter your search query..."
                value={query}
                onChange={(e) => setQuery(e.target.value)}
              />
            </div>

            {/* Rerank Button */}
            <div className="space-y-2 flex items-end">
              <Button
                onClick={handleRerank}
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
                    Reranking
                  </>
                ) : (
                  <>
                    <ArrowUpDown className="h-4 w-4 mr-2" />
                    Rerank
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
      {result && (
        <div className="mb-6 flex flex-wrap items-center gap-3">
          <Badge variant="secondary" className="gap-1.5">
            <Hash className="h-3 w-3" />
            {result.scores.length} documents
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

        {/* Output Panel - Ranked Results */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">{result ? "Ranked Results" : "Preview"}</CardTitle>
          </CardHeader>
          <CardContent className="flex-1 overflow-hidden">
            {result ? (
              <div className="max-h-[600px] overflow-y-auto space-y-3">
                {getRankedDocuments().map((doc) => {
                  const maxScore = getMaxScore();
                  const barWidth = maxScore > 0 ? Math.max(2, (doc.score / maxScore) * 100) : 0;

                  return (
                    <div key={doc.index} className="p-3 bg-muted/30 rounded-lg border space-y-2">
                      <div className="flex items-center gap-2">
                        <span className="flex items-center justify-center w-6 h-6 rounded-full bg-primary/10 text-primary text-xs font-bold shrink-0">
                          {doc.rank}
                        </span>
                        <span className="text-xs text-muted-foreground">
                          Original position: {doc.index + 1}
                        </span>
                        <span className="ml-auto text-sm font-mono font-medium tabular-nums">
                          {doc.score.toFixed(4)}
                        </span>
                      </div>
                      {/* Score bar */}
                      <div className="w-full bg-muted rounded-full h-1.5">
                        <div
                          className={`h-1.5 rounded-full transition-all ${getScoreColor(doc.score, maxScore)}`}
                          style={{ width: `${barWidth}%` }}
                        />
                      </div>
                      <p className="text-sm leading-relaxed">{doc.text}</p>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="h-80 flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <ArrowUpDown className="h-12 w-12 mx-auto mb-3 opacity-20" />
                  <p className="mb-3">
                    Add documents and press{" "}
                    <kbd className="px-1.5 py-0.5 text-xs border rounded bg-muted">Cmd+Enter</kbd>{" "}
                    to rerank
                  </p>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      setQuery(SAMPLE_DATA.photosynthesis.query);
                      setDocuments(SAMPLE_DATA.photosynthesis.documents);
                      setDocKeys(SAMPLE_DATA.photosynthesis.documents.map(() => nextDocKey()));
                    }}
                  >
                    Try a sample
                  </Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Help text */}
      <div className="mt-6 text-xs text-muted-foreground space-y-1">
        <p>
          <strong>Cross-Encoder Reranking:</strong> Uses a cross-encoder model to score each
          document against the query for fine-grained relevance ranking. More accurate than
          bi-encoder similarity but slower.
        </p>
        <p>
          <strong>Scores:</strong> Higher scores indicate greater relevance to the query. Documents
          are sorted by score in descending order.
        </p>
      </div>
    </div>
  );
};

export default RerankingPlaygroundPage;
