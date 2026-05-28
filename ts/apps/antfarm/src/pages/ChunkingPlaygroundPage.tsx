import {
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  DashboardToolbar,
  FormActions,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Separator,
  Skeleton,
  Textarea,
} from "@antfly/design-system";
import { type ChunkResponse, InferenceClient } from "@antfly/sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import { Clock, Database, Hash, RotateCcw, Scissors, Zap } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { PlaygroundEmptyState } from "@/components/branded-empty-state";
import { BackendInfoBar } from "@/components/playground/BackendInfoBar";
import { NoModelsGuide } from "@/components/playground/NoModelsGuide";
import type { SamplePreset } from "@/components/playground/SamplePresets";
import { SamplePresets } from "@/components/playground/SamplePresets";
import { useApiConfig } from "@/hooks/use-api-config";

// Configuration state (extends SDK config with UI-specific fields)
interface ChunkConfig {
  model: string;
  target_tokens: number;
  overlap_tokens: number;
  separator: string;
  max_chunks: number;
  threshold: number;
}

type ChunkResult = ChunkResponse["data"][number];
type TextChunkResult = ChunkResult & { text: string; start_char: number; end_char: number };

function isTextChunk(chunk: ChunkResult): chunk is TextChunkResult {
  return "text" in chunk;
}

const DEFAULT_CONFIG: ChunkConfig = {
  model: "fixed",
  target_tokens: 500,
  overlap_tokens: 50,
  separator: "\\n\\n",
  max_chunks: 50,
  threshold: 0.5,
};

const STORAGE_KEY = "antfarm-playground-chunking";

// Color palette for chunk visualization
const CHUNK_COLORS = [
  "af-chart-surface af-chart-surface-1",
  "af-chart-surface af-chart-surface-2",
  "af-chart-surface af-chart-surface-3",
  "af-chart-surface af-chart-surface-4",
  "af-chart-surface af-chart-surface-5",
  "af-chart-surface af-chart-surface-6",
];

const CHUNK_TEXT_COLORS = [
  "af-chart-text af-chart-text-1",
  "af-chart-text af-chart-text-2",
  "af-chart-text af-chart-text-3",
  "af-chart-text af-chart-text-4",
  "af-chart-text af-chart-text-5",
  "af-chart-text af-chart-text-6",
];

const SAMPLE_TEXTS = {
  technical: {
    name: "Technical Documentation",
    description: "Distributed systems architecture overview",
    text: `Distributed databases achieve horizontal scalability by partitioning data across multiple nodes. Each node is responsible for a subset of the data, determined by a partitioning strategy such as consistent hashing or range-based partitioning.

Replication ensures durability and availability. In a leader-follower model, writes go to the leader and are asynchronously replicated to followers. This provides eventual consistency but may result in stale reads from followers.

Consensus protocols like Raft and Paxos are used to maintain agreement across nodes. Raft simplifies the leader election process and log replication, making it more practical for production systems than classical Paxos.

Vector search adds a new dimension to distributed databases. By storing high-dimensional embeddings alongside traditional data, databases can perform semantic similarity searches. This requires specialized index structures like HNSW (Hierarchical Navigable Small World) graphs that must be distributed across nodes while maintaining search quality.

Query routing in distributed systems involves determining which nodes contain relevant data. For vector queries, this often means searching multiple shards and merging results, as the nearest neighbors may be distributed across partitions.`,
  },
  wikipedia: {
    name: "Wikipedia Excerpt",
    description: "History of computing article",
    text: `The history of computing hardware covers the developments from early simple devices to aid calculation to modern day computers. Before the 20th century, most calculations were done by humans. Early mechanical tools to help humans with digital calculations, like the abacus, were called "calculating machines" or "calculators".

The first aids to computation were purely mechanical devices which required the operator to set up the initial values of an elementary arithmetic operation, then manipulate the device to obtain the result. The abacus was early used for arithmetic tasks. The Roman abacus was used in Babylonia as early as 2400 BC.

Charles Babbage, an English mechanical engineer and polymath, originated the concept of a programmable computer. Considered the "father of the computer", he conceptualized and invented the first mechanical computer in the early 19th century. After working on his revolutionary difference engine, designed to aid in navigational calculations, in 1833 he realized that a much more general design, an Analytical Engine, was possible.

The era of modern computing began with a flurry of development before and during World War II. Most digital computers built in this period were electromechanical. Circuit-based computers and the introduction of vacuum tubes allowed for faster and more reliable machines. The development of transistors in the late 1940s at Bell Laboratories allowed a new generation of computers to be designed with greatly reduced power consumption.`,
  },
  legal: {
    name: "Legal Contract",
    description: "Software license agreement excerpt",
    text: `1. GRANT OF LICENSE. Subject to the terms of this Agreement, Licensor hereby grants to Licensee a non-exclusive, non-transferable, limited license to use the Software solely for Licensee's internal business purposes.

2. RESTRICTIONS. Licensee shall not: (a) sublicense, sell, resell, transfer, assign, or otherwise dispose of or make available to any third party the Software; (b) modify or make derivative works based upon the Software; (c) reverse engineer or access the Software in order to build a competitive product or service.

3. INTELLECTUAL PROPERTY. The Software and all copies thereof are proprietary to Licensor and title thereto remains in Licensor. All applicable rights to patents, copyrights, trademarks, and trade secrets in the Software are and shall remain in Licensor.

4. CONFIDENTIALITY. Each party agrees that all code, inventions, know-how, business, technical and financial information disclosed to such party constitute the confidential property of the disclosing party. Each party shall hold in confidence and not use or disclose the other party's confidential information.

5. WARRANTY DISCLAIMER. THE SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.`,
  },
};

const ChunkingPlaygroundPage: React.FC = () => {
  const { inferenceApiUrl } = useApiConfig();
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
  const [config, setConfig] = useState<ChunkConfig>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return { ...DEFAULT_CONFIG, ...JSON.parse(saved).config };
    } catch {
      /* ignore */
    }
    return DEFAULT_CONFIG;
  });
  const [result, setResult] = useState<ChunkResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [modelsLoaded, setModelsLoaded] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);

  const inferenceClient = useMemo(
    () => new InferenceClient({ baseUrl: inferenceApiUrl }),
    [inferenceApiUrl]
  );

  // Persist state to localStorage
  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ inputText, config }));
  }, [inputText, config]);

  // Fetch available chunker models
  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        const response = await fetch(`${inferenceApiUrl}/ai/v1/models`, {
          signal: controller.signal,
        });
        if (response.ok) {
          const data = await response.json();
          setAvailableModels(Object.keys(data.chunkers || {}));
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
  }, [inferenceApiUrl]);

  // Handle ?model= URL param from Model Directory "Open in Playground"
  useEffect(() => {
    const modelParam = searchParams.get("model");
    if (modelParam && modelsLoaded && availableModels.includes(modelParam)) {
      setConfig((prev) => ({ ...prev, model: modelParam }));
      setSearchParams(
        (prev) => {
          prev.delete("model");
          return prev;
        },
        { replace: true }
      );
    }
  }, [searchParams, modelsLoaded, availableModels, setSearchParams]);

  const handleChunk = useCallback(async () => {
    if (!inputText.trim()) {
      setError("Please enter some text to chunk");
      return;
    }

    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    abortControllerRef.current = new AbortController();
    setIsLoading(true);
    setError(null);
    setResult(null);

    const startTime = performance.now();

    try {
      const actualSeparator = config.separator.replace(/\\n/g, "\n").replace(/\\t/g, "\t");

      const data = await inferenceClient.chunk(
        inputText,
        {
          model: config.model,
          text: {
            target_tokens: config.target_tokens,
            overlap_tokens: config.overlap_tokens,
            separator: actualSeparator,
          },
          max_chunks: config.max_chunks,
          threshold: config.threshold,
        },
        { signal: abortControllerRef.current.signal }
      );

      setResult(data);
      setProcessingTime(performance.now() - startTime);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      setError(
        err instanceof Error
          ? err.message
          : "Failed to connect to Inference. Make sure Inference is running."
      );
    } finally {
      setIsLoading(false);
    }
  }, [inputText, config, inferenceClient]);

  // Cmd+Enter shortcut
  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        handleChunk();
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, [handleChunk]);

  const handleReset = () => {
    setConfig(DEFAULT_CONFIG);
    setInputText("");
    setResult(null);
    setError(null);
    setProcessingTime(null);
    localStorage.removeItem(STORAGE_KEY);
  };

  const estimateTokens = (text: string): number => {
    return Math.ceil(text.length / 4);
  };

  const samplePresets: SamplePreset[] = Object.values(SAMPLE_TEXTS).map((sample) => ({
    name: sample.name,
    description: sample.description,
    onLoad: () => setInputText(sample.text),
  }));

  // Render text with chunk boundaries highlighted
  const renderHighlightedText = () => {
    if (!result || result.data.length === 0) {
      return (
        <pre className="whitespace-pre-wrap text-sm text-muted-foreground font-mono">
          {inputText || "Enter text and click 'Chunk' to see results"}
        </pre>
      );
    }

    const elements: React.ReactNode[] = [];
    let lastEnd = 0;

    result.data.forEach((chunk, index) => {
      if (!isTextChunk(chunk)) return;

      // Add any text before this chunk (gaps)
      if (chunk.start_char > lastEnd) {
        elements.push(
          <span key={`gap-${chunk.id}`} className="text-muted-foreground/50">
            {inputText.slice(lastEnd, chunk.start_char)}
          </span>
        );
      }

      const colorIndex = index % CHUNK_COLORS.length;
      elements.push(
        <span
          key={`chunk-${chunk.id}`}
          className={`${CHUNK_COLORS[colorIndex]} rounded px-0.5 border`}
          title={`Chunk ${chunk.id}`}
        >
          {inputText.slice(chunk.start_char, chunk.end_char)}
        </span>
      );

      lastEnd = chunk.end_char;
    });

    if (lastEnd < inputText.length) {
      elements.push(
        <span key="end" className="text-muted-foreground/50">
          {inputText.slice(lastEnd)}
        </span>
      );
    }

    return <pre className="whitespace-pre-wrap text-sm font-mono leading-relaxed">{elements}</pre>;
  };

  return (
    <DashboardPage className="h-full space-y-3">
      <DashboardPageHeader>
        <div>
          <DashboardPageTitle className="font-aeonik">Chunking Playground</DashboardPageTitle>
          <DashboardPageDescription>
            Experiment with different chunking models and configurations
          </DashboardPageDescription>
        </div>
        <DashboardPageActions>
          <SamplePresets presets={samplePresets} />
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </DashboardPageActions>
      </DashboardPageHeader>

      <BackendInfoBar />

      {modelsLoaded && availableModels.length === 0 && (
        <NoModelsGuide modelType="chunker" typeName="semantic chunker" soft />
      )}

      {/* Configuration Panel */}
      <Card>
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
            <div className="space-y-2 col-span-2">
              <Label htmlFor="model">Model</Label>
              <Select
                value={config.model}
                onValueChange={(value) => setConfig({ ...config, model: value })}
                disabled={!modelsLoaded || availableModels.length === 0}
              >
                <SelectTrigger id="model">
                  <SelectValue placeholder={!modelsLoaded ? "Loading..." : "Select model"} />
                </SelectTrigger>
                <SelectContent>
                  {modelsLoaded && availableModels.length > 0 ? (
                    availableModels.map((model) => (
                      <SelectItem key={model} value={model}>
                        {model === "fixed" ? "Fixed Token Size" : model}
                      </SelectItem>
                    ))
                  ) : (
                    <SelectItem value="fixed">Fixed Token Size</SelectItem>
                  )}
                </SelectContent>
              </Select>
            </div>

            {/* Basic/Fixed model options */}
            {config.model === "fixed" && (
              <>
                <div className="space-y-2">
                  <Label htmlFor="target_tokens">Target Tokens</Label>
                  <Input
                    id="target_tokens"
                    type="number"
                    min={50}
                    max={2000}
                    value={config.target_tokens}
                    onChange={(e) =>
                      setConfig({
                        ...config,
                        target_tokens: parseInt(e.target.value, 10) || 500,
                      })
                    }
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="overlap_tokens">Overlap Tokens</Label>
                  <Input
                    id="overlap_tokens"
                    type="number"
                    min={0}
                    max={500}
                    value={config.overlap_tokens}
                    onChange={(e) =>
                      setConfig({
                        ...config,
                        overlap_tokens: parseInt(e.target.value, 10) || 0,
                      })
                    }
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="separator">Separator</Label>
                  <Select
                    value={config.separator}
                    onValueChange={(value) => setConfig({ ...config, separator: value })}
                  >
                    <SelectTrigger id="separator">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="\\n\\n">Paragraph (\\n\\n)</SelectItem>
                      <SelectItem value="\\n">Line (\\n)</SelectItem>
                      <SelectItem value=". ">Sentence (. )</SelectItem>
                      <SelectItem value=" ">Word ( )</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </>
            )}

            {/* Semantic (ONNX) model options */}
            {config.model !== "fixed" && (
              <>
                <div className="space-y-2">
                  <Label
                    htmlFor="target_tokens"
                    title="Target tokens per chunk. Small chunks are combined until reaching this size. Set to 0 to disable."
                  >
                    Target Tokens
                  </Label>
                  <Input
                    id="target_tokens"
                    type="number"
                    min={0}
                    max={2000}
                    value={config.target_tokens}
                    onChange={(e) =>
                      setConfig({
                        ...config,
                        target_tokens: parseInt(e.target.value, 10) || 0,
                      })
                    }
                  />
                </div>

                <div className="space-y-2">
                  <Label
                    htmlFor="threshold"
                    title="Minimum confidence score (0-1) for separator detection. Higher values = fewer, stronger boundaries."
                  >
                    Threshold
                  </Label>
                  <Input
                    id="threshold"
                    type="number"
                    min={0}
                    max={1}
                    step={0.05}
                    value={config.threshold}
                    onChange={(e) => {
                      const val = parseFloat(e.target.value);
                      if (!Number.isNaN(val) && val >= 0 && val <= 1) {
                        setConfig({ ...config, threshold: val });
                      }
                    }}
                  />
                </div>
              </>
            )}

            <div className="space-y-2">
              <Label htmlFor="max_chunks">Max Chunks</Label>
              <Input
                id="max_chunks"
                type="number"
                min={1}
                max={200}
                value={config.max_chunks}
                onChange={(e) =>
                  setConfig({
                    ...config,
                    max_chunks: parseInt(e.target.value, 10) || 50,
                  })
                }
              />
            </div>
          </div>
          <FormActions>
            <Button onClick={handleChunk} disabled={isLoading || !inputText.trim()}>
              {isLoading ? (
                <>
                  <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                  Processing
                </>
              ) : (
                <>
                  <Scissors className="h-4 w-4 mr-2" />
                  Chunk
                </>
              )}
            </Button>
          </FormActions>
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
          {error}
        </div>
      )}

      {/* Results Stats Bar */}
      {result && (
        <DashboardToolbar className="flex-row items-center gap-3 md:items-center">
          <Badge variant="secondary" className="gap-1.5">
            <Hash className="h-3 w-3" />
            {result.data.length} chunks
          </Badge>
          <Badge variant="secondary" className="gap-1.5">
            <Zap className="h-3 w-3" />
            {result.model}
          </Badge>
          {result.cache_hit && (
            <Badge variant="secondary" className="gap-1.5">
              <Database className="h-3 w-3" />
              Cache hit
            </Badge>
          )}
          {processingTime && (
            <Badge variant="outline" className="gap-1.5">
              <Clock className="h-3 w-3" />
              {processingTime.toFixed(0)}ms
            </Badge>
          )}
        </DashboardToolbar>
      )}

      {/* Main Content - Side by Side */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Input Text</CardTitle>
              {inputText && (
                <span className="text-xs text-muted-foreground">
                  {inputText.length} chars / ~{estimateTokens(inputText)} tokens
                </span>
              )}
            </div>
          </CardHeader>
          <CardContent className="flex-1">
            <Textarea
              placeholder="Paste or type your text here to experiment with chunking..."
              className="h-100 resize-y font-mono text-sm"
              value={inputText}
              onChange={(e) => setInputText(e.target.value)}
            />
          </CardContent>
        </Card>

        {/* Output Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">{result ? "Chunked Output" : "Preview"}</CardTitle>
          </CardHeader>
          <CardContent className="flex-1 overflow-hidden">
            {isLoading ? (
              <div className="h-100 space-y-3">
                <Skeleton className="h-24 w-full" />
                <Skeleton className="h-20 w-full" />
                <Skeleton className="h-16 w-full" />
              </div>
            ) : result ? (
              <div className="h-100 overflow-y-auto space-y-4">
                {/* Highlighted text view */}
                <div className="p-3 bg-muted/50 rounded-lg border max-h-37.5 overflow-y-auto">
                  {renderHighlightedText()}
                </div>

                <Separator />

                {/* Chunk list */}
                <div className="space-y-3">
                  {result.data.filter(isTextChunk).map((chunk, index) => {
                    const colorIndex = index % CHUNK_COLORS.length;
                    return (
                      <div
                        key={chunk.id}
                        className={`p-3 rounded-lg border ${CHUNK_COLORS[colorIndex]}`}
                      >
                        <div className="flex items-center justify-between mb-2">
                          <span
                            className={`font-semibold text-sm ${CHUNK_TEXT_COLORS[colorIndex]}`}
                          >
                            Chunk {chunk.id}
                          </span>
                          <div className="flex gap-2 text-xs text-muted-foreground">
                            <span>~{estimateTokens(chunk.text)} tokens</span>
                            <span>
                              {chunk.start_char}-{chunk.end_char}
                            </span>
                          </div>
                        </div>
                        <p className="text-sm whitespace-pre-wrap line-clamp-4 font-mono">
                          {chunk.text}
                        </p>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : (
              <div className="h-100 flex items-center justify-center">
                <PlaygroundEmptyState />
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Help text */}
      <div className="text-xs text-muted-foreground space-y-1">
        <p>
          <strong>Models:</strong> Fixed uses simple token-count splitting with BERT tokenization
          (configure with target tokens, overlap, separator). ONNX models use neural networks for
          intelligent boundary detection (requires models in chunker_models_dir).
        </p>
        {config.model !== "fixed" && (
          <p>
            <strong>Semantic Options:</strong> Target Tokens combines small chunks until reaching
            the target size while preserving strong semantic boundaries (0 = disabled, keeps
            original neural splits).
          </p>
        )}
      </div>
    </DashboardPage>
  );
};

export default ChunkingPlaygroundPage;
