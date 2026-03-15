import type { CustomInputProps } from "@antfly/components";
import { AnswerResults, Antfly, QueryBox } from "@antfly/components";
import type {
  ClassificationTransformationResult,
  GenerationConfidence,
  QueryHit,
} from "@antfly/sdk";
import { generatorProviders } from "@antfly/sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import {
  BookOpen,
  ChevronDown,
  ChevronRight,
  Clock,
  HelpCircle,
  Play,
  RotateCcw,
  Settings,
  Sparkles,
  Target,
  Zap,
} from "lucide-react";
import type React from "react";
import { useCallback, useMemo, useReducer, useRef, useState } from "react";
import { PipelineTrace } from "@/components/rag/PipelineTrace";
import {
  type ConfidenceStepData,
  type FollowupStepData,
  type GenerationStepData,
  initialPipelineState,
  type PipelineStepId,
  pipelineReducer,
  type SearchStepData,
} from "@/components/rag/pipeline-types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { useApiConfig } from "@/hooks/use-api-config";
import { useTable } from "@/hooks/use-table";
import { cn } from "@/lib/utils";

// Generator provider type from SDK
type GeneratorProvider = (typeof generatorProviders)[number];

interface GeneratorConfig {
  provider: GeneratorProvider;
  model: string;
  temperature?: number;
}

interface StepsConfig {
  classification: {
    enabled: boolean;
    with_reasoning: boolean;
  };
  generation: {
    enabled: boolean;
    system_prompt: string;
    generation_context: string;
  };
  followup: {
    enabled: boolean;
    count: number;
  };
  confidence: {
    enabled: boolean;
  };
}

const DEFAULT_GENERATOR: GeneratorConfig = {
  provider: "openai",
  model: "gpt-4o-mini",
  temperature: 0.7,
};

const DEFAULT_STEPS: StepsConfig = {
  classification: { enabled: false, with_reasoning: false },
  generation: { enabled: true, system_prompt: "", generation_context: "" },
  followup: { enabled: false, count: 3 },
  confidence: { enabled: false },
};

// Simple markdown-ish formatter for RAG answers
function formatAnswer(text: string): React.ReactNode {
  if (!text) return null;

  // Split into lines and process
  const lines = text.split("\n");
  const elements: React.ReactNode[] = [];

  for (let lineIndex = 0; lineIndex < lines.length; lineIndex++) {
    const line = lines[lineIndex];
    // Format citations: [resource_id ...] -> italicized
    let citationCount = 0;
    const formattedLine = line.split(/(\[resource_id [^\]]+\])/).map((part) => {
      if (part.match(/^\[resource_id [^\]]+\]$/)) {
        citationCount++;
        return (
          <span
            key={`cite-${lineIndex}-${citationCount}-${part}`}
            className="text-muted-foreground italic text-xs"
          >
            {part}
          </span>
        );
      }
      return part;
    });

    // Check if it's a bullet point
    const bulletMatch = line.match(/^(\s*)[-*]\s+(.*)$/);
    if (bulletMatch) {
      const [, indent] = bulletMatch;
      const indentLevel = Math.floor((indent?.length || 0) / 2);
      elements.push(
        <div
          key={`bullet-${lineIndex}-${line.trim().slice(0, 20)}`}
          className="flex gap-2"
          style={{ marginLeft: `${indentLevel * 1}rem` }}
        >
          <span className="text-muted-foreground">•</span>
          <span>{formattedLine}</span>
        </div>
      );
    } else if (line.trim() === "") {
      // Empty line = paragraph break
      elements.push(<div key={`empty-${lineIndex}`} className="h-3" />);
    } else {
      // Regular paragraph
      elements.push(
        <p key={`para-${lineIndex}-${line.trim().slice(0, 20)}`} className="leading-relaxed">
          {formattedLine}
        </p>
      );
    }
  }

  return <div className="space-y-1">{elements}</div>;
}

const RagPlaygroundPage: React.FC = () => {
  const { apiUrl } = useApiConfig();
  const { selectedTable, selectedIndex } = useTable();

  // Config state
  const [query, setQuery] = useState("");
  const [generator, setGenerator] = useState<GeneratorConfig>(DEFAULT_GENERATOR);
  const [limit, setLimit] = useState(10);
  const [steps, setSteps] = useState<StepsConfig>(DEFAULT_STEPS);
  const [settingsOpen, setSettingsOpen] = useState(true);

  // Pipeline state
  const [pipeline, dispatchPipeline] = useReducer(pipelineReducer, initialPipelineState);

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [contextOpen, setContextOpen] = useState(false);

  const startTimeRef = useRef<number>(0);

  // Accumulated state refs for pipeline dispatch callbacks
  const accumulatedHitsRef = useRef<QueryHit[]>([]);
  const accumulatedAnswerRef = useRef("");
  const accumulatedFollowupsRef = useRef<string[]>([]);
  const searchCompletedRef = useRef(false);

  const handleReset = () => {
    setQuery("");
    setError(null);
    setProcessingTime(null);
    setSteps(DEFAULT_STEPS);
    setGenerator(DEFAULT_GENERATOR);
    setLimit(10);
    setContextOpen(false);
    dispatchPipeline({ type: "RESET" });
  };

  // --- AnswerResults pipeline dispatch callbacks ---

  const handleStreamStart = useCallback(() => {
    // Reset accumulated state
    accumulatedHitsRef.current = [];
    accumulatedAnswerRef.current = "";
    accumulatedFollowupsRef.current = [];
    searchCompletedRef.current = false;

    setIsLoading(true);
    setError(null);
    setProcessingTime(null);
    setContextOpen(false);
    startTimeRef.current = performance.now();

    // Determine enabled pipeline steps
    const enabledSteps: PipelineStepId[] = [];
    if (steps.classification.enabled) enabledSteps.push("classification");
    enabledSteps.push("search");
    enabledSteps.push("generation");
    if (steps.confidence.enabled) enabledSteps.push("confidence");
    if (steps.followup.enabled) enabledSteps.push("followup");

    dispatchPipeline({ type: "START", enabledSteps });

    // Mark first step as running
    if (steps.classification.enabled) {
      dispatchPipeline({ type: "STEP_START", stepId: "classification" });
    } else {
      dispatchPipeline({ type: "STEP_START", stepId: "search" });
    }
  }, [steps.classification.enabled, steps.confidence.enabled, steps.followup.enabled]);

  const handleClassification = useCallback((c: ClassificationTransformationResult) => {
    dispatchPipeline({
      type: "STEP_COMPLETE",
      stepId: "classification",
      data: { classification: c },
    });
    dispatchPipeline({ type: "STEP_START", stepId: "search" });
  }, []);

  const handleHit = useCallback((hit: QueryHit) => {
    accumulatedHitsRef.current.push(hit);
    dispatchPipeline({
      type: "STEP_UPDATE",
      stepId: "search",
      data: { hits: [...accumulatedHitsRef.current] } as SearchStepData,
    });
  }, []);

  const handleAnswerChunk = useCallback(
    (chunk: string) => {
      // First answer chunk: complete search, start generation
      if (!searchCompletedRef.current) {
        searchCompletedRef.current = true;
        dispatchPipeline({
          type: "STEP_COMPLETE",
          stepId: "search",
          data: { hits: [...accumulatedHitsRef.current] } as SearchStepData,
        });
        dispatchPipeline({ type: "STEP_START", stepId: "generation" });
      }
      accumulatedAnswerRef.current += chunk;
      dispatchPipeline({
        type: "STEP_UPDATE",
        stepId: "generation",
        data: {
          answer: accumulatedAnswerRef.current,
          provider: generator.provider,
          model: generator.model,
        } as GenerationStepData,
      });
    },
    [generator.provider, generator.model]
  );

  const handleFollowUpQuestion = useCallback((q: string) => {
    accumulatedFollowupsRef.current.push(q);
    dispatchPipeline({ type: "STEP_START", stepId: "followup" });
    dispatchPipeline({
      type: "STEP_UPDATE",
      stepId: "followup",
      data: { questions: [...accumulatedFollowupsRef.current] } as FollowupStepData,
    });
  }, []);

  const handleConfidence = useCallback((c: GenerationConfidence) => {
    dispatchPipeline({
      type: "STEP_COMPLETE",
      stepId: "confidence",
      data: {
        generation: c.generation_confidence,
        context: c.context_relevance,
      } as ConfidenceStepData,
    });
  }, []);

  const handleStreamEnd = useCallback(() => {
    setIsLoading(false);
    setProcessingTime(performance.now() - startTimeRef.current);

    // Complete any running steps
    if (!searchCompletedRef.current) {
      searchCompletedRef.current = true;
      dispatchPipeline({
        type: "STEP_COMPLETE",
        stepId: "search",
        data: { hits: [...accumulatedHitsRef.current] } as SearchStepData,
      });
    }
    dispatchPipeline({
      type: "STEP_COMPLETE",
      stepId: "generation",
      data: {
        answer: accumulatedAnswerRef.current,
        provider: generator.provider,
        model: generator.model,
      } as GenerationStepData,
    });
    if (accumulatedFollowupsRef.current.length > 0) {
      dispatchPipeline({
        type: "STEP_COMPLETE",
        stepId: "followup",
        data: { questions: accumulatedFollowupsRef.current } as FollowupStepData,
      });
    }
    dispatchPipeline({ type: "COMPLETE" });
  }, [generator.provider, generator.model]);

  const handleError = useCallback((e: string) => {
    setError(e);
    setIsLoading(false);
    dispatchPipeline({ type: "ERROR", error: e });
  }, []);

  // --- Derived data from pipeline state for stats badges ---

  const generationData = useMemo(
    () => pipeline.steps.find((s) => s.id === "generation")?.data as GenerationStepData | undefined,
    [pipeline.steps]
  );
  const searchData = useMemo(
    () => pipeline.steps.find((s) => s.id === "search")?.data as SearchStepData | undefined,
    [pipeline.steps]
  );
  const confidenceData = useMemo(
    () => pipeline.steps.find((s) => s.id === "confidence")?.data as ConfidenceStepData | undefined,
    [pipeline.steps]
  );

  // --- Response tab content ---

  const responseContent = useMemo(
    () => (
      <div className="space-y-4">
        {/* Stats Bar */}
        {pipeline.overallStatus !== "idle" && (
          <div className="flex flex-wrap gap-2">
            {generationData?.provider && generationData?.model && (
              <Badge variant="secondary" className="gap-1.5">
                <Zap className="h-3 w-3" />
                {generationData.provider}/{generationData.model}
              </Badge>
            )}
            {(searchData?.hits?.length ?? 0) > 0 && (
              <Badge variant="outline" className="gap-1.5">
                <BookOpen className="h-3 w-3" />
                {searchData?.hits.length} docs
              </Badge>
            )}
            {confidenceData && (
              <Badge
                variant="outline"
                className={cn(
                  "gap-1.5",
                  confidenceData.generation > 0.7
                    ? "text-green-600"
                    : confidenceData.generation > 0.4
                      ? "text-yellow-600"
                      : "text-red-600"
                )}
              >
                <Target className="h-3 w-3" />
                {(confidenceData.generation * 100).toFixed(0)}% confidence
              </Badge>
            )}
          </div>
        )}

        {/* Streaming answer via AnswerResults */}
        <AnswerResults
          id="rag-answer"
          searchBoxId="rag-query"
          table={selectedTable}
          generator={{
            provider: generator.provider,
            model: generator.model,
            temperature: generator.temperature,
          }}
          systemPrompt={steps.generation.system_prompt || undefined}
          generationContext={steps.generation.generation_context || undefined}
          limit={limit}
          followUpCount={steps.followup.enabled ? steps.followup.count : undefined}
          semanticIndexes={selectedIndex ? [selectedIndex] : undefined}
          showClassification={steps.classification.enabled}
          showReasoning={steps.classification.with_reasoning}
          showFollowUpQuestions={steps.followup.enabled}
          showConfidence={steps.confidence.enabled}
          showHits
          renderLoading={() => (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <ReloadIcon className="h-4 w-4 animate-spin" />
              Searching and generating answer...
            </div>
          )}
          renderEmpty={() => (
            <div className="text-sm text-muted-foreground">
              Submit a question to get an AI-powered answer.
            </div>
          )}
          renderClassification={(data) => (
            <div className="p-3 rounded-lg bg-muted/50 space-y-2">
              <div className="flex items-center gap-2 text-sm font-medium">
                <Sparkles className="h-4 w-4" />
                Classification
              </div>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div>
                  <span className="text-muted-foreground">Strategy:</span>{" "}
                  <span className="font-medium">{data.strategy}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Mode:</span>{" "}
                  <span className="font-medium">{data.semantic_mode}</span>
                </div>
              </div>
              {data.semantic_query && (
                <div className="text-xs">
                  <span className="text-muted-foreground">Semantic Query:</span>{" "}
                  <span className="italic">{data.semantic_query}</span>
                </div>
              )}
              {data.reasoning && (
                <div className="text-xs text-muted-foreground mt-2 p-2 bg-background rounded">
                  {data.reasoning}
                </div>
              )}
            </div>
          )}
          renderAnswer={(answer, streaming) => (
            <div className="text-sm">
              {answer ? (
                formatAnswer(answer)
              ) : (
                <span className="text-muted-foreground italic">Generating...</span>
              )}
              {streaming && (
                <span className="inline-block w-2 h-4 bg-foreground/50 animate-pulse ml-0.5" />
              )}
            </div>
          )}
          renderFollowUpQuestions={(questions) => (
            <div className="space-y-2">
              <Separator />
              <div className="text-sm font-medium flex items-center gap-2">
                <HelpCircle className="h-4 w-4" />
                Follow-up Questions
              </div>
              <div className="space-y-1">
                {questions.map((q) => (
                  <Button
                    key={q}
                    variant="ghost"
                    size="sm"
                    className="w-full justify-start text-left h-auto py-2 text-sm"
                    onClick={() => setQuery(q)}
                  >
                    {q}
                  </Button>
                ))}
              </div>
            </div>
          )}
          renderHits={(hits) => (
            <Collapsible open={contextOpen} onOpenChange={setContextOpen}>
              <Separator />
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="w-full justify-between mt-2">
                  <span className="flex items-center gap-2">
                    <BookOpen className="h-4 w-4" />
                    Retrieved Context ({hits.length} documents)
                  </span>
                  {contextOpen ? (
                    <ChevronDown className="h-4 w-4" />
                  ) : (
                    <ChevronRight className="h-4 w-4" />
                  )}
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent className="space-y-2 mt-2">
                {hits.map((hit, i) => (
                  <div key={hit._id || i} className="p-3 rounded-lg border text-xs space-y-1">
                    <div className="flex items-center justify-between">
                      <span className="font-medium">{hit._id}</span>
                      <Badge variant="secondary" className="text-xs">
                        {hit._score?.toFixed(3)}
                      </Badge>
                    </div>
                    {hit._source && (
                      <pre className="text-muted-foreground overflow-x-auto whitespace-pre-wrap">
                        {JSON.stringify(hit._source, null, 2).slice(0, 500)}
                        {JSON.stringify(hit._source).length > 500 && "..."}
                      </pre>
                    )}
                  </div>
                ))}
              </CollapsibleContent>
            </Collapsible>
          )}
          onStreamStart={handleStreamStart}
          onStreamEnd={handleStreamEnd}
          onError={handleError}
          onClassification={handleClassification}
          onHit={handleHit}
          onGenerationChunk={handleAnswerChunk}
          onConfidence={handleConfidence}
          onFollowup={handleFollowUpQuestion}
        />
      </div>
    ),
    [
      pipeline.overallStatus,
      generationData,
      searchData,
      confidenceData,
      generator,
      steps,
      limit,
      selectedIndex,
      contextOpen,
      handleStreamStart,
      handleStreamEnd,
      handleError,
      handleClassification,
      handleHit,
      handleAnswerChunk,
      handleConfidence,
      handleFollowUpQuestion,
      selectedTable,
    ]
  );

  return (
    <Antfly url={apiUrl} table={selectedTable || ""}>
      <div className="h-full">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold">RAG Playground</h1>
            <p className="text-muted-foreground text-sm mt-1">
              Query your documents with AI-powered retrieval and generation
            </p>
          </div>
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </div>

        {/* Active Table/Index Indicator */}
        {selectedTable ? (
          <div className="mb-4 flex items-center gap-2 text-sm text-muted-foreground">
            <Badge variant="secondary">{selectedTable}</Badge>
            {selectedIndex && <Badge variant="outline">{selectedIndex}</Badge>}
          </div>
        ) : (
          <div className="mb-4 p-3 rounded-lg border border-dashed text-sm text-muted-foreground">
            Select a table from the sidebar to get started.
          </div>
        )}

        {/* Main Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Left Column - Query & Settings */}
          <div className="space-y-4">
            {/* Query Card */}
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-lg">Query</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <QueryBox
                  id="rag-query"
                  mode="submit"
                  initialValue={query}
                  renderInput={({ value, onChange, onSubmit }: CustomInputProps) => (
                    <>
                      <Textarea
                        placeholder="Enter your question..."
                        value={value}
                        onChange={(e) => onChange(e.target.value)}
                        className="min-h-[100px] resize-none"
                        onKeyDown={(e) => {
                          if (
                            e.key === "Enter" &&
                            (e.metaKey || e.ctrlKey) &&
                            value.trim() &&
                            selectedTable &&
                            selectedIndex &&
                            !isLoading
                          ) {
                            e.preventDefault();
                            onSubmit(value);
                          }
                        }}
                      />
                      <div className="flex items-center justify-end mt-4">
                        <Button
                          type="button"
                          onClick={() => onSubmit(value)}
                          disabled={!value.trim() || !selectedTable || !selectedIndex || isLoading}
                        >
                          {isLoading ? (
                            <>
                              <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                              Running...
                            </>
                          ) : (
                            <>
                              <Play className="h-4 w-4 mr-2" />
                              Run Query
                            </>
                          )}
                        </Button>
                      </div>
                    </>
                  )}
                />
              </CardContent>
            </Card>

            {/* Settings Card */}
            <Card>
              <Collapsible open={settingsOpen} onOpenChange={setSettingsOpen}>
                <CardHeader className="pb-3">
                  <CollapsibleTrigger asChild>
                    <div className="flex items-center justify-between cursor-pointer">
                      <CardTitle className="text-lg flex items-center gap-2">
                        <Settings className="h-4 w-4" />
                        Settings
                      </CardTitle>
                      {settingsOpen ? (
                        <ChevronDown className="h-4 w-4" />
                      ) : (
                        <ChevronRight className="h-4 w-4" />
                      )}
                    </div>
                  </CollapsibleTrigger>
                </CardHeader>
                <CollapsibleContent>
                  <CardContent className="space-y-6">
                    {/* Generator Config */}
                    <div className="space-y-4">
                      <Label className="text-sm font-medium">Generator</Label>
                      <div className="grid grid-cols-3 gap-4">
                        <div className="space-y-2">
                          <Label className="text-xs text-muted-foreground">Provider</Label>
                          <Select
                            value={generator.provider}
                            onValueChange={(v) =>
                              setGenerator((g) => ({ ...g, provider: v as GeneratorProvider }))
                            }
                          >
                            <SelectTrigger>
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {generatorProviders.map((p) => (
                                <SelectItem key={p} value={p}>
                                  {p}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="space-y-2">
                          <Label className="text-xs text-muted-foreground">Model</Label>
                          <Input
                            value={generator.model}
                            onChange={(e) => setGenerator((g) => ({ ...g, model: e.target.value }))}
                            placeholder="gpt-5-mini"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label className="text-xs text-muted-foreground">Temperature</Label>
                          <Input
                            type="number"
                            value={generator.temperature ?? 0.7}
                            onChange={(e) =>
                              setGenerator((g) => ({
                                ...g,
                                temperature: parseFloat(e.target.value) || 0.7,
                              }))
                            }
                            min={0}
                            max={2}
                            step={0.1}
                          />
                        </div>
                      </div>
                    </div>

                    {/* Query Options */}
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground">Result Limit</Label>
                      <Input
                        type="number"
                        value={limit}
                        onChange={(e) => setLimit(parseInt(e.target.value, 10) || 10)}
                        min={1}
                        max={50}
                        className="w-24"
                      />
                    </div>

                    <Separator />

                    {/* Pipeline Steps */}
                    <div className="space-y-4">
                      <Label className="text-sm font-medium">Pipeline Steps</Label>

                      {/* Classification */}
                      <div className="flex items-center justify-between p-3 rounded-lg border">
                        <div className="flex items-center gap-3">
                          <Sparkles className="h-4 w-4 text-muted-foreground" />
                          <div>
                            <p className="text-sm font-medium">Classification</p>
                            <p className="text-xs text-muted-foreground">Analyze query strategy</p>
                          </div>
                        </div>
                        <div className="flex items-center gap-3">
                          {steps.classification.enabled && (
                            <div className="flex items-center gap-2">
                              <Label className="text-xs">Reasoning</Label>
                              <Switch
                                checked={steps.classification.with_reasoning}
                                onCheckedChange={(v) =>
                                  setSteps((s) => ({
                                    ...s,
                                    classification: {
                                      ...s.classification,
                                      with_reasoning: v,
                                    },
                                  }))
                                }
                              />
                            </div>
                          )}
                          <Switch
                            checked={steps.classification.enabled}
                            onCheckedChange={(v) =>
                              setSteps((s) => ({
                                ...s,
                                classification: { ...s.classification, enabled: v },
                              }))
                            }
                          />
                        </div>
                      </div>

                      {/* Follow-up */}
                      <div className="flex items-center justify-between p-3 rounded-lg border">
                        <div className="flex items-center gap-3">
                          <HelpCircle className="h-4 w-4 text-muted-foreground" />
                          <div>
                            <p className="text-sm font-medium">Follow-up Questions</p>
                            <p className="text-xs text-muted-foreground">
                              Generate related questions
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center gap-3">
                          {steps.followup.enabled && (
                            <div className="flex items-center gap-2">
                              <Label className="text-xs">Count</Label>
                              <Input
                                type="number"
                                value={steps.followup.count}
                                onChange={(e) =>
                                  setSteps((s) => ({
                                    ...s,
                                    followup: {
                                      ...s.followup,
                                      count: parseInt(e.target.value, 10) || 3,
                                    },
                                  }))
                                }
                                className="w-14 h-7 text-xs"
                                min={1}
                                max={10}
                              />
                            </div>
                          )}
                          <Switch
                            checked={steps.followup.enabled}
                            onCheckedChange={(v) =>
                              setSteps((s) => ({
                                ...s,
                                followup: { ...s.followup, enabled: v },
                              }))
                            }
                          />
                        </div>
                      </div>

                      {/* Confidence */}
                      <div className="flex items-center justify-between p-3 rounded-lg border">
                        <div className="flex items-center gap-3">
                          <Target className="h-4 w-4 text-muted-foreground" />
                          <div>
                            <p className="text-sm font-medium">Confidence Scores</p>
                            <p className="text-xs text-muted-foreground">Rate answer confidence</p>
                          </div>
                        </div>
                        <Switch
                          checked={steps.confidence.enabled}
                          onCheckedChange={(v) =>
                            setSteps((s) => ({
                              ...s,
                              confidence: { enabled: v },
                            }))
                          }
                        />
                      </div>
                    </div>

                    {/* System Prompt */}
                    <div className="space-y-2">
                      <Label>System Prompt (optional)</Label>
                      <Textarea
                        placeholder="Custom instructions for the generator..."
                        value={steps.generation.system_prompt}
                        onChange={(e) =>
                          setSteps((s) => ({
                            ...s,
                            generation: { ...s.generation, system_prompt: e.target.value },
                          }))
                        }
                        className="min-h-[80px] resize-none text-sm"
                      />
                    </div>

                    {/* Generation Context */}
                    <div className="space-y-2">
                      <Label>Generation Context (optional)</Label>
                      <Textarea
                        placeholder="Guidance for tone, detail level, style... e.g. 'Be concise and technical. Include code examples.'"
                        value={steps.generation.generation_context}
                        onChange={(e) =>
                          setSteps((s) => ({
                            ...s,
                            generation: {
                              ...s.generation,
                              generation_context: e.target.value,
                            },
                          }))
                        }
                        className="min-h-[60px] resize-none text-sm"
                      />
                    </div>
                  </CardContent>
                </CollapsibleContent>
              </Collapsible>
            </Card>
          </div>

          {/* Right Column - Pipeline Trace */}
          <Card className="flex flex-col">
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="text-lg">Results</CardTitle>
                {processingTime && (
                  <Badge variant="outline" className="gap-1.5">
                    <Clock className="h-3 w-3" />
                    {(processingTime / 1000).toFixed(1)}s
                  </Badge>
                )}
              </div>
            </CardHeader>
            <CardContent className="flex-1 overflow-auto">
              {/* Error Display */}
              {error && (
                <div className="mb-4 p-4 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive text-sm">
                  {error}
                </div>
              )}

              <PipelineTrace
                pipeline={pipeline}
                onFollowupClick={(q) => setQuery(q)}
                formatAnswer={formatAnswer}
                responseContent={responseContent}
              />
            </CardContent>
          </Card>
        </div>

        {/* Help text */}
        <div className="mt-6 text-xs text-muted-foreground">
          <p>
            <strong>RAG Playground:</strong> Enter a natural language question to search your
            documents and generate an AI-powered answer with citations. Configure classification,
            follow-up questions, and confidence scoring in Settings.
          </p>
        </div>
      </div>
    </Antfly>
  );
};

export default RagPlaygroundPage;
