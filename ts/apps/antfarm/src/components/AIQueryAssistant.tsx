import type { GeneratorConfig, QueryBuilderResult } from "@antfly/sdk";
import { GearIcon } from "@radix-ui/react-icons";
import { Sparkles } from "lucide-react";
import type React from "react";
import { useState } from "react";
import {
  formatGeneratorSummary,
  GENERATOR_DEFAULT_CONFIG,
  GeneratorSelector,
  getInheritedGeneratorLabels,
  QUERY_BUILDER_PROVIDERS,
} from "@/components/playground/GeneratorSelector";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Textarea } from "@/components/ui/textarea";
import { useApi } from "@/hooks/use-api-config";
import { useGeneratorPreference } from "@/hooks/use-generator-preference";
import { normalizeSimplifiedDSL, usesSimplifiedDSL } from "@/utils/normalizeQuery";
import { QueryDiffView } from "./QueryDiffView";

interface AIQueryAssistantProps {
  tableName?: string;
  schemaFields?: string[];
  currentQuery: object;
  onQueryApplied: (query: object) => void;
  onQueryAppliedAndRun: (query: object) => void;
}

const AIQueryAssistant: React.FC<AIQueryAssistantProps> = ({
  tableName,
  schemaFields,
  currentQuery,
  onQueryApplied,
  onQueryAppliedAndRun,
}) => {
  const client = useApi();
  const { dashboardGenerator } = useGeneratorPreference();
  const [intent, setIntent] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryBuilderResult | null>(null);
  const [normalizedQuery, setNormalizedQuery] = useState<object | null>(null);
  const [wasNormalized, setWasNormalized] = useState(false);

  // Generator configuration
  const [generatorOverride, setGeneratorOverride] = useState<GeneratorConfig | null>(null);
  const effectiveGenerator = generatorOverride ?? dashboardGenerator ?? null;
  const { label: inheritedGeneratorLabel, description: inheritedGeneratorDescription } =
    getInheritedGeneratorLabels(dashboardGenerator);

  const handleGenerate = async () => {
    if (!intent.trim()) {
      setError("Please describe what you want to search for");
      return;
    }

    setIsLoading(true);
    setError(null);
    setResult(null);
    setNormalizedQuery(null);
    setWasNormalized(false);

    try {
      const data = await client.queryBuilderAgent({
        intent: intent.trim(),
        ...(tableName && { table: tableName }),
        ...(schemaFields && schemaFields.length > 0 && { schema_fields: schemaFields }),
        ...(effectiveGenerator ? { generator: effectiveGenerator } : {}),
      });

      setResult(data);

      // Normalize if using simplified DSL
      const needsNormalization = usesSimplifiedDSL(data.query);
      const normalized = needsNormalization ? normalizeSimplifiedDSL(data.query) : data.query;
      setNormalizedQuery(normalized);
      setWasNormalized(needsNormalization);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate query");
    } finally {
      setIsLoading(false);
    }
  };

  const handleApply = () => {
    if (normalizedQuery) {
      onQueryApplied(normalizedQuery);
      clearProposal();
    }
  };

  const handleApplyAndRun = () => {
    if (normalizedQuery) {
      onQueryAppliedAndRun(normalizedQuery);
      clearProposal();
    }
  };

  const handleDiscard = () => {
    clearProposal();
  };

  const clearProposal = () => {
    setResult(null);
    setNormalizedQuery(null);
    setWasNormalized(false);
  };

  const getConfidenceColor = (confidence: number | undefined) => {
    if (confidence === undefined) return "secondary";
    if (confidence >= 0.8) return "default";
    if (confidence >= 0.5) return "secondary";
    return "destructive";
  };

  const getConfidenceLabel = (confidence: number | undefined) => {
    if (confidence === undefined) return "Unknown";
    if (confidence >= 0.8) return "High";
    if (confidence >= 0.5) return "Medium";
    return "Low";
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <Sparkles className="h-4 w-4" />
            AI Query Builder
            <Badge variant="outline" className="font-normal text-xs">
              Beta
            </Badge>
          </CardTitle>
          <Popover>
            <PopoverTrigger asChild>
              <Button variant="ghost" size="sm" className="h-7 gap-1.5 text-muted-foreground">
                <GearIcon className="h-3.5 w-3.5" />
                <span className="text-xs truncate max-w-[180px]">
                  {formatGeneratorSummary(effectiveGenerator)}
                </span>
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-[420px]" align="end">
              <div className="space-y-3">
                <h4 className="text-sm font-medium">Generator Settings</h4>
                <GeneratorSelector
                  value={generatorOverride}
                  onChange={setGeneratorOverride}
                  defaultConfig={GENERATOR_DEFAULT_CONFIG}
                  defaultLabel={inheritedGeneratorLabel}
                  defaultDescription={inheritedGeneratorDescription}
                  providers={QUERY_BUILDER_PROVIDERS}
                  showTemperature={false}
                />
              </div>
            </PopoverContent>
          </Popover>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex gap-2">
          <Textarea
            placeholder="Describe what you want to search for..."
            value={intent}
            onChange={(e) => setIntent(e.target.value)}
            onKeyDown={(e) => {
              if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
                e.preventDefault();
                handleGenerate();
              }
            }}
            rows={2}
            className="resize-none flex-1"
          />
          <Button
            onClick={handleGenerate}
            disabled={isLoading || !intent.trim()}
            className="self-end"
          >
            {isLoading ? "Generating..." : "Generate"}
          </Button>
        </div>

        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {result && normalizedQuery && (
          <div className="rounded-lg border p-3 space-y-3 bg-muted/30">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium truncate flex-1 mr-2">"{intent}"</span>
              <div className="flex items-center gap-2">
                {wasNormalized && (
                  <Badge variant="outline" className="text-[10px]">
                    Normalized
                  </Badge>
                )}
                {result.confidence !== undefined && (
                  <Badge variant={getConfidenceColor(result.confidence)}>
                    {getConfidenceLabel(result.confidence)} ({Math.round(result.confidence * 100)}%)
                  </Badge>
                )}
              </div>
            </div>

            {result.explanation && (
              <p className="text-xs text-muted-foreground">{result.explanation}</p>
            )}

            <QueryDiffView currentQuery={currentQuery} proposedQuery={normalizedQuery} />

            {result.warnings && result.warnings.length > 0 && (
              <Alert>
                <AlertDescription>
                  <ul className="list-disc list-inside space-y-0.5">
                    {result.warnings.map((warning) => (
                      <li key={warning} className="text-xs">
                        {warning}
                      </li>
                    ))}
                  </ul>
                </AlertDescription>
              </Alert>
            )}

            <div className="flex gap-2">
              <Button onClick={handleApplyAndRun} size="sm" className="flex-1">
                Apply & Run
              </Button>
              <Button onClick={handleApply} variant="secondary" size="sm" className="flex-1">
                Apply
              </Button>
              <Button onClick={handleDiscard} variant="ghost" size="sm">
                Discard
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
};

export default AIQueryAssistant;
