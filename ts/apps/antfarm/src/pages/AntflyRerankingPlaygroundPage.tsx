import { TermiteClient } from "@antfly/termite-sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import {
  ArrowDownUp,
  ArrowRight,
  Clock,
  Database,
  Hash,
  RotateCcw,
  Search,
  Zap,
} from "lucide-react";
import type React from "react";
import { useEffect, useMemo, useRef, useState } from "react";
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
import { useApiConfig } from "@/hooks/use-api-config";
import { useTable } from "@/hooks/use-table";
import { fetchWithRetry } from "@/lib/utils";

interface HitResult {
  id: string;
  score: number;
  source: Record<string, unknown>;
  text: string; // extracted text for reranking
}

interface RankedHit extends HitResult {
  originalRank: number;
  rerankScore: number;
  newRank: number;
  rankChange: number; // positive = moved up, negative = moved down
}

interface ModelsResponse {
  rerankers: Record<string, { capabilities?: string[] }>;
  [key: string]: Record<string, { capabilities?: string[] }>;
}

/** Extract a text representation from document source for reranking. */
function extractText(source: Record<string, unknown>): string {
  const textFields = ["text", "content", "body", "description", "summary", "title", "name"];
  const parts: string[] = [];

  for (const field of textFields) {
    const val = source[field];
    if (typeof val === "string" && val.length > 0) {
      parts.push(val);
    }
  }

  if (parts.length === 0) {
    // Fallback: stringify entire source
    return JSON.stringify(source).slice(0, 500);
  }

  return parts.join(" ").slice(0, 1000);
}

/** Render document preview from source. */
function DocumentPreview({ source }: { source: Record<string, unknown> }) {
  const displayFields = ["title", "name", "text", "content", "body", "description", "summary"];
  const titleField = displayFields.find((f) => typeof source[f] === "string");
  const title = titleField ? String(source[titleField]) : undefined;

  return (
    <div>
      {title ? (
        <p className="text-sm leading-relaxed line-clamp-2">{title}</p>
      ) : (
        <p className="text-xs text-muted-foreground font-mono line-clamp-2">
          {JSON.stringify(source, null, 0).slice(0, 200)}
        </p>
      )}
    </div>
  );
}

/** Score bar with configurable color. */
function ScoreBar({ score, maxScore, color }: { score: number; maxScore: number; color: string }) {
  const pct = maxScore > 0 ? Math.max(0, Math.min(100, (score / maxScore) * 100)) : 0;
  return (
    <div className="w-full bg-muted rounded-full h-1.5 overflow-hidden">
      <div
        className="h-1.5 rounded-full transition-all duration-500"
        style={{ width: `${pct}%`, backgroundColor: color }}
      />
    </div>
  );
}

/** Rank change indicator. */
function RankDelta({ change }: { change: number }) {
  if (change === 0) {
    return <span className="text-xs text-muted-foreground font-mono">=</span>;
  }
  if (change > 0) {
    return (
      <span className="text-xs text-success-600 dark:text-success-400 font-mono font-medium">
        +{change}
      </span>
    );
  }
  return <span className="text-xs text-danger-500 font-mono font-medium">{change}</span>;
}

const AntflyRerankingPlaygroundPage: React.FC = () => {
  const { client, termiteApiUrl } = useApiConfig();
  const { selectedTable, embeddingIndexes, selectedIndex, setSelectedIndex } = useTable();

  const [query, setQuery] = useState("");
  const [limit, setLimit] = useState(10);
  const [selectedModel, setSelectedModel] = useState("");
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [modelsLoaded, setModelsLoaded] = useState(false);

  // Search results (before reranking)
  const [searchResults, setSearchResults] = useState<HitResult[] | null>(null);
  const [searchTime, setSearchTime] = useState<number | null>(null);

  // Reranked results
  const [rerankedResults, setRerankedResults] = useState<RankedHit[] | null>(null);
  const [rerankTime, setRerankTime] = useState<number | null>(null);

  const [isSearching, setIsSearching] = useState(false);
  const [isReranking, setIsReranking] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const termiteClient = useMemo(
    () => new TermiteClient({ baseUrl: `${termiteApiUrl}/api` }),
    [termiteApiUrl]
  );

  // Fetch available reranker models
  useEffect(() => {
    const fetchModels = async () => {
      try {
        const response = await fetchWithRetry(`${termiteApiUrl}/api/models`);
        if (response.ok) {
          const data: ModelsResponse = await response.json();
          const rerankers = Object.keys(data.rerankers || {});
          setAvailableModels(rerankers);
          if (rerankers.length > 0) {
            const builtin = rerankers.find((m) => m === "antfly-builtin-reranker");
            setSelectedModel(builtin || rerankers[0]);
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

  const handleSearch = async () => {
    if (!query.trim()) {
      setError("Please enter a query");
      return;
    }
    if (!selectedTable) {
      setError("Please select a table");
      return;
    }

    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    abortControllerRef.current = new AbortController();
    setIsSearching(true);
    setError(null);
    setSearchResults(null);
    setRerankedResults(null);
    setSearchTime(null);
    setRerankTime(null);

    const startTime = performance.now();

    try {
      // Build query: use semantic search if an embedding index is selected, otherwise full-text
      const queryRequest = selectedIndex
        ? { semantic_search: query, indexes: [selectedIndex], limit }
        : { full_text_search: { query: query }, limit };

      const response = await client.tables.query(selectedTable, queryRequest);
      const hits = response?.responses?.[0]?.hits?.hits || [];

      const parsed: HitResult[] = hits.map((hit) => {
        const source = (hit._source || {}) as Record<string, unknown>;
        return {
          id: hit._id,
          score: hit._score,
          source,
          text: extractText(source),
        };
      });

      setSearchResults(parsed);
      setSearchTime(performance.now() - startTime);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") return;
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setIsSearching(false);
    }
  };

  const handleRerank = async () => {
    if (!searchResults || searchResults.length === 0) return;
    if (!selectedModel) {
      setError("Please select a reranker model");
      return;
    }

    setIsReranking(true);
    setError(null);
    setRerankedResults(null);

    const startTime = performance.now();

    try {
      const prompts = searchResults.map((r) => r.text);
      const response = await termiteClient.rerank(selectedModel, query, prompts);

      if (!response.scores || response.scores.length !== searchResults.length) {
        throw new Error("Reranker returned unexpected number of scores");
      }

      // Build ranked results
      const withScores: RankedHit[] = searchResults.map((hit, i) => ({
        ...hit,
        originalRank: i + 1,
        rerankScore: response.scores[i],
        newRank: 0,
        rankChange: 0,
      }));

      // Sort by rerank score descending
      withScores.sort((a, b) => b.rerankScore - a.rerankScore);

      // Assign new ranks and compute deltas
      withScores.forEach((hit, i) => {
        hit.newRank = i + 1;
        hit.rankChange = hit.originalRank - hit.newRank; // positive = improved
      });

      setRerankedResults(withScores);
      setRerankTime(performance.now() - startTime);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Reranking failed");
    } finally {
      setIsReranking(false);
    }
  };

  const handleReset = () => {
    setQuery("");
    setSearchResults(null);
    setRerankedResults(null);
    setError(null);
    setSearchTime(null);
    setRerankTime(null);
  };

  const maxSearchScore =
    searchResults && searchResults.length > 0 ? Math.max(...searchResults.map((r) => r.score)) : 1;

  const maxRerankScore =
    rerankedResults && rerankedResults.length > 0
      ? Math.max(...rerankedResults.map((r) => r.rerankScore))
      : 1;

  return (
    <div className="h-full">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Reranking Explorer</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Search your table then rerank results with a cross-encoder to compare rankings
          </p>
        </div>
        <Button variant="outline" onClick={handleReset}>
          <RotateCcw className="h-4 w-4 mr-2" />
          Reset
        </Button>
      </div>

      {!selectedTable && (
        <div className="mb-6 p-4 bg-muted/50 border border-dashed rounded-lg text-muted-foreground text-sm text-center">
          <Database className="h-8 w-8 mx-auto mb-2 opacity-30" />
          Select a table from the sidebar to start exploring reranking
        </div>
      )}

      {/* Configuration Panel */}
      <Card className="mb-6">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Configuration</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
            {/* Table */}
            <div className="space-y-2">
              <Label>Table</Label>
              <Input value={selectedTable || "No table selected"} disabled />
            </div>

            {/* Index */}
            <div className="space-y-2">
              <Label htmlFor="index">Search Index</Label>
              <Select
                value={selectedIndex || "__fulltext__"}
                onValueChange={(v) => setSelectedIndex(v === "__fulltext__" ? "" : v)}
              >
                <SelectTrigger id="index">
                  <SelectValue placeholder="Full-text search" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__fulltext__">Full-text (BM25)</SelectItem>
                  {embeddingIndexes.map((idx) => (
                    <SelectItem key={idx} value={idx}>
                      {idx} (vector)
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Reranker Model */}
            <div className="space-y-2">
              <Label htmlFor="reranker">Reranker Model</Label>
              <Select
                value={selectedModel}
                onValueChange={setSelectedModel}
                disabled={!modelsLoaded || availableModels.length === 0}
              >
                <SelectTrigger id="reranker">
                  <SelectValue
                    placeholder={
                      !modelsLoaded
                        ? "Loading..."
                        : availableModels.length === 0
                          ? "No models"
                          : "Select model"
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

            {/* Limit */}
            <div className="space-y-2">
              <Label htmlFor="limit">Limit</Label>
              <Input
                id="limit"
                type="number"
                min={1}
                max={50}
                value={limit}
                onChange={(e) => setLimit(parseInt(e.target.value, 10) || 10)}
              />
            </div>

            {/* Search Button */}
            <div className="space-y-2 flex items-end">
              <Button
                onClick={handleSearch}
                disabled={isSearching || !query.trim() || !selectedTable}
                className="w-full"
              >
                {isSearching ? (
                  <>
                    <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                    Searching
                  </>
                ) : (
                  <>
                    <Search className="h-4 w-4 mr-2" />
                    Search
                  </>
                )}
              </Button>
            </div>
          </div>

          {/* Query Input */}
          <div className="space-y-2">
            <Label htmlFor="query">Query</Label>
            <Input
              id="query"
              placeholder="Enter a search query..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  handleSearch();
                }
              }}
            />
          </div>
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <div className="mb-6 p-4 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive text-sm">
          {error}
        </div>
      )}

      {/* Results Stats + Rerank Button */}
      {searchResults && (
        <div className="mb-6 flex flex-wrap items-center gap-3">
          <Badge variant="secondary" className="gap-1.5">
            <Hash className="h-3 w-3" />
            {searchResults.length} results
          </Badge>
          {searchTime && (
            <Badge variant="outline" className="gap-1.5">
              <Clock className="h-3 w-3" />
              Search: {searchTime.toFixed(0)}ms
            </Badge>
          )}
          {rerankTime && (
            <Badge variant="outline" className="gap-1.5">
              <Clock className="h-3 w-3" />
              Rerank: {rerankTime.toFixed(0)}ms
            </Badge>
          )}
          {rerankedResults && (
            <Badge variant="secondary" className="gap-1.5">
              <Zap className="h-3 w-3" />
              {selectedModel}
            </Badge>
          )}

          {/* Rerank action */}
          {!rerankedResults && searchResults.length > 0 && (
            <Button
              size="sm"
              onClick={handleRerank}
              disabled={isReranking || !selectedModel}
              className="ml-auto"
            >
              {isReranking ? (
                <>
                  <ReloadIcon className="h-3 w-3 mr-1.5 animate-spin" />
                  Reranking
                </>
              ) : (
                <>
                  <ArrowDownUp className="h-3 w-3 mr-1.5" />
                  Rerank Results
                </>
              )}
            </Button>
          )}
        </div>
      )}

      {/* Side-by-Side Results */}
      {searchResults && searchResults.length > 0 && (
        <div
          className={`grid gap-6 ${rerankedResults ? "grid-cols-1 lg:grid-cols-2" : "grid-cols-1"}`}
        >
          {/* Original Search Results */}
          <Card className="flex flex-col">
            <CardHeader className="pb-3">
              <div className="flex items-center gap-2">
                <CardTitle className="text-lg">Search Results</CardTitle>
                <Badge variant="outline" className="text-xs">
                  {selectedIndex || "BM25"}
                </Badge>
              </div>
            </CardHeader>
            <CardContent className="flex-1 max-h-[600px] overflow-y-auto space-y-2">
              {searchResults.map((hit, rank) => (
                <div key={hit.id} className="p-3 bg-muted/30 rounded-lg border space-y-1.5">
                  <div className="flex items-center gap-2">
                    <span className="flex items-center justify-center w-5 h-5 rounded-full bg-primary/10 text-primary text-xs font-bold shrink-0">
                      {rank + 1}
                    </span>
                    <span className="text-xs font-mono text-muted-foreground truncate flex-1">
                      {hit.id}
                    </span>
                    <span className="text-xs font-mono font-medium tabular-nums">
                      {hit.score.toFixed(4)}
                    </span>
                  </div>
                  <ScoreBar
                    score={hit.score}
                    maxScore={maxSearchScore}
                    color="hsl(210, 70%, 50%)"
                  />
                  <DocumentPreview source={hit.source} />
                </div>
              ))}
            </CardContent>
          </Card>

          {/* Reranked Results */}
          {rerankedResults && (
            <Card className="flex flex-col">
              <CardHeader className="pb-3">
                <div className="flex items-center gap-2">
                  <CardTitle className="text-lg">Reranked Results</CardTitle>
                  <Badge variant="outline" className="text-xs">
                    {selectedModel}
                  </Badge>
                </div>
              </CardHeader>
              <CardContent className="flex-1 max-h-[600px] overflow-y-auto space-y-2">
                {rerankedResults.map((hit) => (
                  <div key={hit.id} className="p-3 bg-muted/30 rounded-lg border space-y-1.5">
                    <div className="flex items-center gap-2">
                      <span className="flex items-center justify-center w-5 h-5 rounded-full bg-primary/10 text-primary text-xs font-bold shrink-0">
                        {hit.newRank}
                      </span>
                      <div className="flex items-center gap-1 text-xs text-muted-foreground">
                        <span className="font-mono">#{hit.originalRank}</span>
                        <ArrowRight className="h-3 w-3" />
                        <span className="font-mono">#{hit.newRank}</span>
                        <RankDelta change={hit.rankChange} />
                      </div>
                      <span className="ml-auto text-xs font-mono font-medium tabular-nums">
                        {hit.rerankScore.toFixed(4)}
                      </span>
                    </div>
                    <ScoreBar
                      score={hit.rerankScore}
                      maxScore={maxRerankScore}
                      color="hsl(150, 60%, 40%)"
                    />
                    <DocumentPreview source={hit.source} />
                  </div>
                ))}
              </CardContent>
            </Card>
          )}
        </div>
      )}

      {/* Empty state */}
      {!searchResults && !error && selectedTable && (
        <div className="p-12 text-center text-muted-foreground">
          <ArrowDownUp className="h-12 w-12 mx-auto mb-3 opacity-20" />
          <p>
            Search your table, then rerank results to compare retrieval vs. cross-encoder ranking
          </p>
        </div>
      )}

      {/* Help text */}
      {rerankedResults && (
        <div className="mt-6 text-xs text-muted-foreground space-y-1">
          <p>
            <strong>Two-Stage Retrieval:</strong> Initial search uses fast bi-encoder similarity (or
            BM25) to retrieve candidates. Cross-encoder reranking then scores each document against
            the query for more accurate relevance ordering.
          </p>
          <p>
            <strong>Rank Change:</strong> Green values (+N) mean the document moved up in ranking
            after reranking, red values (-N) mean it moved down. Documents that move significantly
            may have been over- or under-scored by the initial retrieval.
          </p>
        </div>
      )}
    </div>
  );
};

export default AntflyRerankingPlaygroundPage;
