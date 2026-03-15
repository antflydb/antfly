import type { GeneratorProvider, QueryBuilderResult } from "@antfly/sdk";
import { GearIcon } from "@radix-ui/react-icons";
import { Sparkles } from "lucide-react";
import type React from "react";
import { useState } from "react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { useApi } from "@/hooks/use-api-config";
import { normalizeSimplifiedDSL, usesSimplifiedDSL } from "@/utils/normalizeQuery";
import { QueryDiffView } from "./QueryDiffView";

const PROVIDER_DEFAULTS: Partial<Record<GeneratorProvider, string>> = {
  gemini: "gemini-2.5-flash",
  vertex: "gemini-2.5-flash",
  ollama: "llama3.3:70b",
  openai: "gpt-4.1",
  bedrock: "anthropic.claude-sonnet-4-5-20250929-v1:0",
  anthropic: "claude-sonnet-4-5-20250929",
  cohere: "command-r-plus",
};

const PROVIDER_LABELS: Partial<Record<GeneratorProvider, string>> = {
  gemini: "Google AI (Gemini)",
  vertex: "Google Cloud Vertex AI",
  ollama: "Ollama (Local)",
  openai: "OpenAI",
  bedrock: "AWS Bedrock",
  anthropic: "Anthropic (Claude)",
  cohere: "Cohere",
};

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
  const [intent, setIntent] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryBuilderResult | null>(null);
  const [normalizedQuery, setNormalizedQuery] = useState<object | null>(null);
  const [wasNormalized, setWasNormalized] = useState(false);

  // Generator configuration
  const [provider, setProvider] = useState<GeneratorProvider | "">("");
  const [model, setModel] = useState("");

  const handleProviderChange = (value: GeneratorProvider) => {
    setProvider(value);
    setModel(PROVIDER_DEFAULTS[value] || "");
  };

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
        ...(provider &&
          model && {
            generator: { provider, model },
          }),
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
                {provider && (
                  <span className="text-xs">{PROVIDER_LABELS[provider] || provider}</span>
                )}
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-80" align="end">
              <div className="space-y-3">
                <h4 className="text-sm font-medium">Generator Settings</h4>
                <div className="space-y-2">
                  <Label htmlFor="ai-provider" className="text-xs">
                    Provider
                  </Label>
                  <Select
                    value={provider}
                    onValueChange={(v) => handleProviderChange(v as GeneratorProvider)}
                  >
                    <SelectTrigger id="ai-provider" className="h-8">
                      <SelectValue placeholder="Server default" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="gemini">Google AI (Gemini)</SelectItem>
                      <SelectItem value="vertex">Google Cloud Vertex</SelectItem>
                      <SelectItem value="openai">OpenAI</SelectItem>
                      <SelectItem value="anthropic">Anthropic (Claude)</SelectItem>
                      <SelectItem value="bedrock">AWS Bedrock</SelectItem>
                      <SelectItem value="ollama">Ollama (Local)</SelectItem>
                      <SelectItem value="cohere">Cohere</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="ai-model" className="text-xs">
                    Model
                  </Label>
                  <Input
                    id="ai-model"
                    placeholder={provider ? PROVIDER_DEFAULTS[provider] : "Default"}
                    value={model}
                    onChange={(e) => setModel(e.target.value)}
                    disabled={!provider}
                    className="h-8"
                  />
                </div>
                {!provider && (
                  <p className="text-xs text-muted-foreground">
                    Leave empty to use the server's default generator configuration.
                  </p>
                )}
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
