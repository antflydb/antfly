import { ReloadIcon } from "@radix-ui/react-icons";
import {
  ArrowDownUp,
  Clock,
  Database,
  Hash,
  Maximize2,
  Minimize2,
  RotateCcw,
  Search,
  Zap,
} from "lucide-react";
import type React from "react";
import { useRef, useState } from "react";
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

interface HitResult {
  id: string;
  score: number;
  source: Record<string, unknown>;
  indexScores?: Record<string, unknown>;
}

/** Continuous gradient bar mapping similarity score to a color ramp. */
function SimilarityBar({ score, maxScore }: { score: number; maxScore: number }) {
  const pct = maxScore > 0 ? Math.max(0, Math.min(100, (score / maxScore) * 100)) : 0;
  const hue = Math.round(pct * 1.2);

  return (
    <div className="w-full bg-muted rounded-full h-2 overflow-hidden">
      <div
        className="h-2 rounded-full transition-all duration-500"
        style={{
          width: `${pct}%`,
          backgroundColor: `hsl(${hue}, 70%, 45%)`,
        }}
      />
    </div>
  );
}

/** Render a document source as a compact preview, showing key fields. */
function DocumentPreview({ source }: { source: Record<string, unknown> }) {
  // Prioritize common content fields for display
  const displayFields = ["title", "name", "text", "content", "body", "description", "summary"];
  const titleField = displayFields.find((f) => typeof source[f] === "string");
  const title = titleField ? String(source[titleField]) : undefined;

  // Show other string fields (up to 2) as secondary info
  const otherFields = Object.entries(source)
    .filter(
      ([key, val]) =>
        typeof val === "string" &&
        key !== titleField &&
        !key.startsWith("_") &&
        val.length > 0 &&
        val.length < 500
    )
    .slice(0, 2);

  return (
    <div className="space-y-1">
      {title && <p className="text-sm leading-relaxed line-clamp-3">{title}</p>}
      {otherFields.map(([key, val]) => (
        <p key={key} className="text-xs text-muted-foreground line-clamp-2">
          <span className="font-medium">{key}:</span> {String(val)}
        </p>
      ))}
      {!title && otherFields.length === 0 && (
        <p className="text-xs text-muted-foreground font-mono">
          {JSON.stringify(source, null, 0).slice(0, 200)}
        </p>
      )}
    </div>
  );
}

const AntflyEmbeddingPlaygroundPage: React.FC = () => {
  const { client } = useApiConfig();
  const { selectedTable, embeddingIndexes, selectedIndex, setSelectedIndex } = useTable();

  const [query, setQuery] = useState("");
  const [limit, setLimit] = useState(10);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [results, setResults] = useState<HitResult[] | null>(null);
  const [showRawScores, setShowRawScores] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);

  const handleSearch = async () => {
    if (!query.trim()) {
      setError("Please enter a query");
      return;
    }
    if (!selectedTable) {
      setError("Please select a table");
      return;
    }
    if (!selectedIndex) {
      setError("Please select an embedding index");
      return;
    }

    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    abortControllerRef.current = new AbortController();
    setIsLoading(true);
    setError(null);
    setResults(null);

    const startTime = performance.now();

    try {
      const response = await client.tables.query(selectedTable, {
        semantic_search: query,
        indexes: [selectedIndex],
        limit,
      });

      const hits = response?.responses?.[0]?.hits?.hits || [];

      const parsed: HitResult[] = hits.map((hit) => ({
        id: hit._id,
        score: hit._score,
        source: (hit._source || {}) as Record<string, unknown>,
        indexScores: (hit._index_scores || undefined) as Record<string, unknown> | undefined,
      }));

      setResults(parsed);
      setProcessingTime(performance.now() - startTime);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      setError(err instanceof Error ? err.message : "Search failed");
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = () => {
    setQuery("");
    setResults(null);
    setError(null);
    setProcessingTime(null);
  };

  const maxScore = results && results.length > 0 ? Math.max(...results.map((r) => r.score)) : 1;

  return (
    <div className="h-full">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Embedding Explorer</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Search your table with vector similarity and understand how documents rank
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
          Select a table from the sidebar to start exploring embeddings
        </div>
      )}

      {/* Configuration Panel */}
      <Card className="mb-6">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Configuration</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Table (read-only, from context) */}
            <div className="space-y-2">
              <Label>Table</Label>
              <Input value={selectedTable || "No table selected"} disabled />
            </div>

            {/* Embedding Index Selection */}
            <div className="space-y-2">
              <Label htmlFor="index">Embedding Index</Label>
              <Select
                value={selectedIndex}
                onValueChange={setSelectedIndex}
                disabled={embeddingIndexes.length === 0}
              >
                <SelectTrigger id="index">
                  <SelectValue
                    placeholder={
                      embeddingIndexes.length === 0 ? "No embedding indexes" : "Select an index"
                    }
                  />
                </SelectTrigger>
                <SelectContent>
                  {embeddingIndexes.map((idx) => (
                    <SelectItem key={idx} value={idx}>
                      {idx}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Limit */}
            <div className="space-y-2">
              <Label htmlFor="limit">Result Limit</Label>
              <Input
                id="limit"
                type="number"
                min={1}
                max={100}
                value={limit}
                onChange={(e) => setLimit(parseInt(e.target.value, 10) || 10)}
              />
            </div>

            {/* Search Button */}
            <div className="space-y-2 flex items-end">
              <Button
                onClick={handleSearch}
                disabled={isLoading || !query.trim() || !selectedTable || !selectedIndex}
                className="w-full"
              >
                {isLoading ? (
                  <>
                    <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                    Searching
                  </>
                ) : (
                  <>
                    <Search className="h-4 w-4 mr-2" />
                    Search &amp; Analyze
                  </>
                )}
              </Button>
            </div>
          </div>

          {/* Query Input (full width) */}
          <div className="space-y-2">
            <Label htmlFor="query">Semantic Query</Label>
            <Input
              id="query"
              placeholder="Enter a natural language query to find similar documents..."
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

      {/* Results Stats Bar */}
      {results && (
        <div className="mb-6 flex flex-wrap items-center gap-3">
          <Badge variant="secondary" className="gap-1.5">
            <Hash className="h-3 w-3" />
            {results.length} hits
          </Badge>
          <Badge variant="secondary" className="gap-1.5">
            <Zap className="h-3 w-3" />
            {selectedIndex}
          </Badge>
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
            onClick={() => setShowRawScores(!showRawScores)}
          >
            {showRawScores ? <Minimize2 className="h-3 w-3" /> : <Maximize2 className="h-3 w-3" />}
            {showRawScores ? "Hide" : "Show"} raw scores
          </Button>
        </div>
      )}

      {/* Results */}
      {results ? (
        <div className="space-y-3">
          {results.length === 0 ? (
            <div className="p-8 text-center text-muted-foreground">
              <Search className="h-12 w-12 mx-auto mb-3 opacity-20" />
              <p>No results found for this query</p>
            </div>
          ) : (
            results.map((hit, rank) => (
              <Card key={hit.id} className="overflow-hidden">
                <CardContent className="p-4 space-y-2">
                  <div className="flex items-center gap-2">
                    <span className="flex items-center justify-center w-6 h-6 rounded-full bg-primary/10 text-primary text-xs font-bold shrink-0">
                      {rank + 1}
                    </span>
                    <span className="text-xs font-mono text-muted-foreground truncate">
                      {hit.id}
                    </span>
                    <span className="ml-auto text-sm font-mono font-medium tabular-nums">
                      {hit.score.toFixed(4)}
                    </span>
                  </div>
                  <SimilarityBar score={hit.score} maxScore={maxScore} />
                  <DocumentPreview source={hit.source} />
                  {showRawScores && hit.indexScores && (
                    <div className="pt-2 border-t">
                      <pre className="text-[10px] text-muted-foreground font-mono overflow-x-auto">
                        {JSON.stringify(hit.indexScores, null, 2)}
                      </pre>
                    </div>
                  )}
                </CardContent>
              </Card>
            ))
          )}
        </div>
      ) : (
        !error &&
        selectedTable && (
          <div className="p-12 text-center text-muted-foreground">
            <ArrowDownUp className="h-12 w-12 mx-auto mb-3 opacity-20" />
            <p>Enter a query and click "Search &amp; Analyze" to explore vector similarity</p>
          </div>
        )
      )}

      {/* Help text */}
      {results && (
        <div className="mt-6 text-xs text-muted-foreground space-y-1">
          <p>
            <strong>Similarity Scores:</strong> Documents are ranked by cosine similarity to your
            query's embedding vector. Higher scores indicate stronger semantic match. The score bar
            is normalized relative to the top result.
          </p>
          <p>
            <strong>Index:</strong> Results come from the "{selectedIndex}" embedding index.
            Different indexes may use different models and produce different rankings for the same
            query.
          </p>
        </div>
      )}
    </div>
  );
};

export default AntflyEmbeddingPlaygroundPage;
