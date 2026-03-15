import type { GeneratorProvider, QueryBuilderRequest, QueryBuilderResult } from "@antfly/sdk";
import { ChevronDownIcon, ChevronUpIcon, GearIcon } from "@radix-ui/react-icons";
import type React from "react";
import { useState } from "react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
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
import { useApi } from "@/hooks/use-api-config";
import JsonViewer from "./JsonViewer";

const PROVIDER_DEFAULTS: Record<GeneratorProvider, string> = {
  gemini: "gemini-2.5-flash",
  vertex: "gemini-2.5-flash",
  ollama: "llama3.3:70b",
  openai: "gpt-4.1",
  openrouter: "openai/gpt-4.1",
  bedrock: "anthropic.claude-sonnet-4-5-20250929-v1:0",
  anthropic: "claude-sonnet-4-5-20250929",
  cohere: "command-r-plus",
  termite: "gemma-3-1b-it",
  mock: "mock",
};

const PROVIDER_LABELS: Record<GeneratorProvider, string> = {
  gemini: "Google AI (Gemini)",
  vertex: "Google Cloud Vertex AI",
  ollama: "Ollama (Local)",
  openai: "OpenAI",
  openrouter: "OpenRouter",
  bedrock: "AWS Bedrock",
  anthropic: "Anthropic (Claude)",
  cohere: "Cohere",
  termite: "Termite (Local)",
  mock: "Mock (Testing)",
};

interface QueryBuilderAgentProps {
  tableName?: string;
  schemaFields?: string[];
  onQueryGenerated?: (query: object) => void;
}

const QueryBuilderAgent: React.FC<QueryBuilderAgentProps> = ({
  tableName,
  schemaFields,
  onQueryGenerated,
}) => {
  const client = useApi();
  const [intent, setIntent] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryBuilderResult | null>(null);
  const [isExplanationOpen, setIsExplanationOpen] = useState(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);

  // Generator configuration
  const [provider, setProvider] = useState<GeneratorProvider | "">("");
  const [model, setModel] = useState("");

  const handleProviderChange = (value: GeneratorProvider) => {
    setProvider(value);
    setModel(PROVIDER_DEFAULTS[value] || "");
  };

  const handleGenerateQuery = async () => {
    if (!intent.trim()) {
      setError("Please describe what you want to search for");
      return;
    }

    setIsLoading(true);
    setError(null);
    setResult(null);

    try {
      const request: QueryBuilderRequest = {
        intent: intent.trim(),
        ...(tableName && { table: tableName }),
        ...(schemaFields && schemaFields.length > 0 && { schema_fields: schemaFields }),
        ...(provider &&
          model && {
            generator: {
              provider: provider,
              model: model,
            },
          }),
      };

      const data = await client.queryBuilderAgent(request);
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate query");
    } finally {
      setIsLoading(false);
    }
  };

  const handleUseQuery = () => {
    if (result?.query && onQueryGenerated) {
      onQueryGenerated(result.query);
    }
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
        <CardTitle className="text-base flex items-center gap-2">
          AI Query Builder
          <Badge variant="outline" className="font-normal text-xs">
            Beta
          </Badge>
        </CardTitle>
        <CardDescription>Describe what you want to search for in natural language</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-2">
          <Label htmlFor="intent">Search Intent</Label>
          <Textarea
            id="intent"
            placeholder="e.g., Find all published articles about machine learning from the last year"
            value={intent}
            onChange={(e) => setIntent(e.target.value)}
            onKeyDown={(e) => {
              if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
                e.preventDefault();
                handleGenerateQuery();
              }
            }}
            rows={3}
            className="resize-none"
          />
        </div>

        {/* Generator Settings */}
        <Collapsible open={isSettingsOpen} onOpenChange={setIsSettingsOpen}>
          <CollapsibleTrigger asChild>
            <Button variant="ghost" size="sm" className="w-full justify-between px-2">
              <span className="flex items-center gap-2 text-muted-foreground">
                <GearIcon className="h-4 w-4" />
                Generator Settings
                {provider && (
                  <Badge variant="secondary" className="font-normal text-xs">
                    {PROVIDER_LABELS[provider]}
                  </Badge>
                )}
              </span>
              {isSettingsOpen ? (
                <ChevronUpIcon className="h-4 w-4" />
              ) : (
                <ChevronDownIcon className="h-4 w-4" />
              )}
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent className="pt-3 space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <Label htmlFor="provider">Provider</Label>
                <Select
                  value={provider}
                  onValueChange={(v) => handleProviderChange(v as GeneratorProvider)}
                >
                  <SelectTrigger id="provider">
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
                <Label htmlFor="model">Model</Label>
                <Input
                  id="model"
                  placeholder={provider ? PROVIDER_DEFAULTS[provider] : "Default"}
                  value={model}
                  onChange={(e) => setModel(e.target.value)}
                  disabled={!provider}
                />
              </div>
            </div>
            {!provider && (
              <p className="text-xs text-muted-foreground">
                Leave empty to use the server's default generator configuration.
              </p>
            )}
          </CollapsibleContent>
        </Collapsible>

        <Button
          onClick={handleGenerateQuery}
          disabled={isLoading || !intent.trim()}
          className="w-full"
        >
          {isLoading ? "Generating..." : "Generate Query"}
        </Button>

        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {result && (
          <div className="space-y-3 pt-2">
            <div className="flex items-center justify-between">
              <Label>Generated Query</Label>
              {result.confidence !== undefined && (
                <Badge variant={getConfidenceColor(result.confidence)}>
                  {getConfidenceLabel(result.confidence)} confidence (
                  {Math.round(result.confidence * 100)}%)
                </Badge>
              )}
            </div>

            <div className="rounded-md border">
              <JsonViewer json={result.query} />
            </div>

            {result.explanation && (
              <Collapsible open={isExplanationOpen} onOpenChange={setIsExplanationOpen}>
                <CollapsibleTrigger asChild>
                  <Button variant="ghost" size="sm" className="w-full justify-between">
                    <span>Explanation</span>
                    {isExplanationOpen ? (
                      <ChevronUpIcon className="h-4 w-4" />
                    ) : (
                      <ChevronDownIcon className="h-4 w-4" />
                    )}
                  </Button>
                </CollapsibleTrigger>
                <CollapsibleContent>
                  <p className="text-sm text-muted-foreground p-3 bg-muted/30 rounded-md">
                    {result.explanation}
                  </p>
                </CollapsibleContent>
              </Collapsible>
            )}

            {result.warnings && result.warnings.length > 0 && (
              <Alert>
                <AlertDescription>
                  <ul className="list-disc list-inside space-y-1">
                    {result.warnings.map((warning) => (
                      <li key={warning} className="text-sm">
                        {warning}
                      </li>
                    ))}
                  </ul>
                </AlertDescription>
              </Alert>
            )}

            {onQueryGenerated && (
              <Button onClick={handleUseQuery} variant="secondary" className="w-full">
                Use This Query
              </Button>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
};

export default QueryBuilderAgent;
