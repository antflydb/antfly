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
  Textarea,
} from "@antfly/design-system";
import { type Chunk, type ChunkResponse, TermiteClient } from "@antfly/termite-sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import {
  ClipboardCopy,
  Clock,
  Database,
  Hash,
  RotateCcw,
  Scissors,
  Search,
  Zap,
} from "lucide-react";
import type React from "react";
import { useMemo, useRef, useState } from "react";
import { PlaygroundEmptyState } from "@/components/branded-empty-state";
import { useApiConfig } from "@/hooks/use-api-config";
import { useTable } from "@/hooks/use-table";

interface ChunkConfig {
  provider: string;
  strategy: string;
  model: string;
  target_tokens: number;
  overlap_tokens: number;
  separator: string;
  max_chunks: number;
  threshold: number;
}

function isTextChunk(
  chunk: Chunk
): chunk is Chunk & { text: string; start_char: number; end_char: number } {
  return "text" in chunk;
}

const DEFAULT_CONFIG: ChunkConfig = {
  provider: "termite",
  strategy: "fixed",
  model: "fixed",
  target_tokens: 500,
  overlap_tokens: 50,
  separator: "\\n\\n",
  max_chunks: 50,
  threshold: 0.5,
};

// Color palette for chunk visualization (matches Termite playground)
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

/** Extract text content from a document source object. */
function extractDocumentText(source: Record<string, unknown>): string {
  const textFields = ["text", "content", "body", "description", "summary", "title", "name"];
  const parts: string[] = [];

  for (const field of textFields) {
    const val = source[field];
    if (typeof val === "string" && val.length > 0) {
      parts.push(val);
    }
  }

  if (parts.length === 0) {
    return JSON.stringify(source, null, 2);
  }

  return parts.join("\n\n");
}

const AntflyChunkingPlaygroundPage: React.FC = () => {
  const { client, termiteApiUrl } = useApiConfig();
  const { selectedTable, selectedIndex } = useTable();

  const [config, setConfig] = useState<ChunkConfig>(DEFAULT_CONFIG);
  const [inputText, setInputText] = useState("");
  const [documentId, setDocumentId] = useState("");
  const [result, setResult] = useState<ChunkResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isFetchingDoc, setIsFetchingDoc] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [docSource, setDocSource] = useState<"manual" | "table">("table");
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<Array<{ id: string; preview: string }> | null>(
    null
  );
  const [isSearching, setIsSearching] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);

  const termiteClient = useMemo(
    () => new TermiteClient({ baseUrl: termiteApiUrl }),
    [termiteApiUrl]
  );

  const estimateTokens = (text: string): number => {
    return Math.ceil(text.length / 4);
  };

  /** Search table to find documents. */
  const handleSearchDocuments = async () => {
    if (!selectedTable || !searchQuery.trim()) return;

    setIsSearching(true);
    setError(null);

    try {
      const queryRequest = selectedIndex
        ? { semantic_search: searchQuery, indexes: [selectedIndex], limit: 5 }
        : { full_text_search: { query: searchQuery }, limit: 5 };

      const response = await client.tables.query(selectedTable, queryRequest);
      const hits = response?.responses?.[0]?.hits?.hits || [];

      setSearchResults(
        hits.map((hit) => {
          const source = (hit._source || {}) as Record<string, unknown>;
          const text = extractDocumentText(source);
          return {
            id: hit._id,
            preview: text.slice(0, 120) + (text.length > 120 ? "..." : ""),
          };
        })
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setIsSearching(false);
    }
  };

  /** Fetch a specific document by ID from the table. */
  const handleFetchDocument = async (docId: string) => {
    if (!selectedTable || !docId.trim()) return;

    setIsFetchingDoc(true);
    setError(null);

    try {
      const doc = await client.tables.lookup(selectedTable, docId);
      if (doc) {
        const source = doc as Record<string, unknown>;
        const text = extractDocumentText(source);
        setInputText(text);
        setDocumentId(docId);
        setResult(null);
        setSearchResults(null);
      } else {
        setError(`Document "${docId}" not found`);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch document");
    } finally {
      setIsFetchingDoc(false);
    }
  };

  const handleChunk = async () => {
    if (!inputText.trim()) {
      setError("Please enter or load text to chunk");
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

      const data = await termiteClient.chunk(
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
      if (err instanceof Error && err.name === "AbortError") return;
      setError(
        err instanceof Error ? err.message : `Failed to connect to Termite at ${termiteApiUrl}`
      );
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = () => {
    setConfig(DEFAULT_CONFIG);
    setInputText("");
    setDocumentId("");
    setResult(null);
    setError(null);
    setProcessingTime(null);
    setSearchResults(null);
    setSearchQuery("");
  };

  /** Build the chunker config JSON (for copy-paste into index creation). */
  const chunkerConfigJson = useMemo(() => {
    const cfg: Record<string, unknown> = {
      provider: config.provider,
      strategy: config.model === "fixed" ? "fixed" : "hugot",
      text: {
        target_tokens: config.target_tokens,
        overlap_tokens: config.overlap_tokens,
        separator: config.separator.replace(/\\n/g, "\n").replace(/\\t/g, "\t"),
      },
      max_chunks: config.max_chunks,
    };
    if (config.model !== "fixed") {
      cfg.threshold = config.threshold;
    }
    return JSON.stringify(cfg, null, 2);
  }, [config]);

  const handleCopyJson = () => {
    navigator.clipboard.writeText(chunkerConfigJson);
  };

  /** Render text with chunk boundaries highlighted. */
  const renderHighlightedText = () => {
    if (!result || result.chunks.length === 0) {
      return (
        <pre className="whitespace-pre-wrap text-sm text-muted-foreground font-mono">
          {inputText || "Load a document and click 'Chunk' to see results"}
        </pre>
      );
    }

    const elements: React.ReactNode[] = [];
    let lastEnd = 0;

    result.chunks.forEach((chunk, index) => {
      if (!isTextChunk(chunk)) return;

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
            Test chunking on documents from your table and build chunker configurations
          </DashboardPageDescription>
        </div>
        <DashboardPageActions>
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </DashboardPageActions>
      </DashboardPageHeader>

      {!selectedTable && (
        <DashboardToolbar className="items-center justify-center border-dashed text-center text-sm text-muted-foreground md:items-center">
          <Database className="h-8 w-8 mx-auto mb-2 opacity-30" />
          Select a table from the sidebar, or paste text directly to experiment with chunking
        </DashboardToolbar>
      )}

      {/* Configuration Panel */}
      <Card>
        <CardHeader className="pb-4">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg">Configuration</CardTitle>
            <Button variant="outline" size="sm" onClick={handleCopyJson} className="gap-1.5">
              <ClipboardCopy className="h-3 w-3" />
              Copy JSON
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
            <div className="space-y-2">
              <Label htmlFor="model">Model</Label>
              <Select
                value={config.model}
                onValueChange={(value) => setConfig({ ...config, model: value })}
              >
                <SelectTrigger id="model">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="fixed">Fixed Token Size</SelectItem>
                  <SelectItem value="chonky-mmbert-small-multilingual-1">
                    Chonky (ONNX Semantic)
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>

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
                      setConfig({ ...config, target_tokens: parseInt(e.target.value, 10) || 500 })
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
                      setConfig({ ...config, overlap_tokens: parseInt(e.target.value, 10) || 0 })
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

            {config.model !== "fixed" && (
              <>
                <div className="space-y-2">
                  <Label htmlFor="target_tokens">Target Tokens</Label>
                  <Input
                    id="target_tokens"
                    type="number"
                    min={0}
                    max={2000}
                    value={config.target_tokens}
                    onChange={(e) =>
                      setConfig({ ...config, target_tokens: parseInt(e.target.value, 10) || 0 })
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="threshold">Threshold</Label>
                  <Input
                    id="threshold"
                    type="text"
                    defaultValue={config.threshold}
                    onBlur={(e) => {
                      const val = parseFloat(e.target.value);
                      if (!Number.isNaN(val) && val >= 0 && val <= 1) {
                        setConfig({ ...config, threshold: val });
                      } else {
                        e.target.value = String(config.threshold);
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
                  setConfig({ ...config, max_chunks: parseInt(e.target.value, 10) || 50 })
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
            {result.chunks.length} chunks
          </Badge>
          <Badge variant="secondary" className="gap-1.5">
            <Zap className="h-3 w-3" />
            {result.model}
          </Badge>
          {documentId && (
            <Badge variant="outline" className="gap-1.5">
              <Database className="h-3 w-3" />
              {documentId}
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

      {/* Main Content */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Document Text</CardTitle>
              {inputText && (
                <span className="text-xs text-muted-foreground">
                  {inputText.length} chars / ~{estimateTokens(inputText)} tokens
                </span>
              )}
            </div>
          </CardHeader>
          <CardContent className="flex-1 space-y-3">
            {/* Document source toggle */}
            <div className="flex gap-2">
              <Button
                variant={docSource === "table" ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setDocSource("table")}
                disabled={!selectedTable}
              >
                <Database className="h-3 w-3 mr-1.5" />
                From Table
              </Button>
              <Button
                variant={docSource === "manual" ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setDocSource("manual")}
              >
                Paste Text
              </Button>
            </div>

            {/* Table document picker */}
            {docSource === "table" && selectedTable && (
              <div className="space-y-2 p-3 bg-muted/30 rounded-lg border">
                {/* Search for documents */}
                <div className="flex gap-2">
                  <Input
                    placeholder="Search for a document..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") handleSearchDocuments();
                    }}
                    className="text-sm"
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleSearchDocuments}
                    disabled={isSearching || !searchQuery.trim()}
                  >
                    {isSearching ? (
                      <ReloadIcon className="h-3 w-3 animate-spin" />
                    ) : (
                      <Search className="h-3 w-3" />
                    )}
                  </Button>
                </div>

                {/* Or fetch by ID */}
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span>or enter document ID directly:</span>
                </div>
                <div className="flex gap-2">
                  <Input
                    placeholder="Document ID..."
                    value={documentId}
                    onChange={(e) => setDocumentId(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") handleFetchDocument(documentId);
                    }}
                    className="text-sm font-mono"
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleFetchDocument(documentId)}
                    disabled={isFetchingDoc || !documentId.trim()}
                  >
                    {isFetchingDoc ? <ReloadIcon className="h-3 w-3 animate-spin" /> : "Load"}
                  </Button>
                </div>

                {/* Search results */}
                {searchResults && searchResults.length > 0 && (
                  <div className="space-y-1 pt-1">
                    {searchResults.map((sr) => (
                      <button
                        key={sr.id}
                        type="button"
                        className="w-full text-left p-2 rounded hover:bg-accent text-sm space-y-0.5 transition-colors"
                        onClick={() => handleFetchDocument(sr.id)}
                      >
                        <span className="font-mono text-xs text-muted-foreground">{sr.id}</span>
                        <p className="text-xs line-clamp-1">{sr.preview}</p>
                      </button>
                    ))}
                  </div>
                )}
                {searchResults && searchResults.length === 0 && (
                  <p className="text-xs text-muted-foreground">No documents found</p>
                )}
              </div>
            )}

            {/* Text area */}
            <Textarea
              placeholder={
                docSource === "table" && selectedTable
                  ? "Search or enter a document ID above to load text..."
                  : "Paste or type your text here to experiment with chunking..."
              }
              className="h-80 resize-y font-mono text-sm"
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
            {result ? (
              <div className="h-100 overflow-y-auto space-y-4">
                {/* Highlighted text view */}
                <div className="p-3 bg-muted/50 rounded-lg border max-h-37.5 overflow-y-auto">
                  {renderHighlightedText()}
                </div>

                <Separator />

                {/* Chunk list */}
                <div className="space-y-3">
                  {result.chunks.filter(isTextChunk).map((chunk, index) => {
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

      {/* Config JSON preview + help text */}
      <div className="text-xs text-muted-foreground space-y-1">
        <p>
          <strong>Copy JSON:</strong> Use the "Copy JSON" button to get the chunker configuration
          for use when creating an embedding index.
        </p>
        <p>
          <strong>Models:</strong> Fixed uses simple token-count splitting with BERT tokenization.
          ONNX models use neural networks for intelligent boundary detection (requires models in
          chunker_models_dir).
        </p>
      </div>
    </DashboardPage>
  );
};

export default AntflyChunkingPlaygroundPage;
