import type { ChatTurn } from "@antfly/components";
import { Antfly, ChatBar } from "@antfly/components";
import { createAIElementsRenderers, turnToStatus } from "@antfly/components/adapters";
import type { ChatToolName, GeneratorConfig } from "@antfly/sdk";
import { generatorProviders } from "@antfly/sdk";
import {
  Bot,
  ChevronDown,
  ChevronRight,
  HelpCircle,
  RotateCcw,
  Settings,
  Sparkles,
  Target,
} from "lucide-react";
import type React from "react";
import { useCallback, useState } from "react";
import { Message, MessageContent, MessageResponse } from "@/components/ai-elements/message";
import {
  PromptInput,
  PromptInputFooter,
  PromptInputSubmit,
  PromptInputTextarea,
} from "@/components/ai-elements/prompt-input";
import { Source, Sources, SourcesContent, SourcesTrigger } from "@/components/ai-elements/sources";
import { Suggestion, Suggestions } from "@/components/ai-elements/suggestion";
import { ReasoningChainCollapsible } from "@/components/playground/ReasoningChainCollapsible";
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

type GeneratorProvider = (typeof generatorProviders)[number];

interface StepsConfig {
  classification: { enabled: boolean };
  followup: { enabled: boolean; count: number };
  confidence: { enabled: boolean };
}

const DEFAULT_GENERATOR: GeneratorConfig = {
  provider: "openai",
  model: "gpt-4o-mini",
  temperature: 0.7,
};

const DEFAULT_STEPS: StepsConfig = {
  classification: { enabled: true },
  followup: { enabled: true, count: 3 },
  confidence: { enabled: false },
};

const AVAILABLE_TOOLS = [
  {
    name: "semantic_search" as ChatToolName,
    label: "Semantic Search",
    desc: "Vector similarity search",
  },
  {
    name: "full_text_search" as ChatToolName,
    label: "Full-Text Search",
    desc: "BM25 keyword search",
  },
  { name: "add_filter" as ChatToolName, label: "Add Filter", desc: "Field constraints" },
  {
    name: "ask_clarification" as ChatToolName,
    label: "Ask Clarification",
    desc: "Request user input",
  },
  { name: "websearch" as ChatToolName, label: "Web Search", desc: "Search the web" },
] as const;

const DEFAULT_ENABLED_TOOLS: ChatToolName[] = ["semantic_search", "full_text_search", "add_filter"];

const aiRenderers = createAIElementsRenderers({
  Message,
  MessageContent,
  MessageResponse,
  Sources,
  SourcesTrigger,
  SourcesContent,
  Source,
  Suggestions,
  Suggestion,
  PromptInput,
  PromptInputTextarea,
  PromptInputSubmit,
  PromptInputFooter,
});

const ChatPlaygroundPage: React.FC = () => {
  const { apiUrl } = useApiConfig();
  const { selectedTable, chatIndexes } = useTable();

  // Config state
  const [generator, setGenerator] = useState<GeneratorConfig>(DEFAULT_GENERATOR);
  const [limit, setLimit] = useState(10);
  const [steps, setSteps] = useState<StepsConfig>(DEFAULT_STEPS);
  const [systemPrompt, setSystemPrompt] = useState("");
  const [agentKnowledge, setAgentKnowledge] = useState("");
  const [agenticEnabled, setAgenticEnabled] = useState(false);
  const [maxIterations, setMaxIterations] = useState(5);
  const [enabledTools, setEnabledTools] = useState<ChatToolName[]>(DEFAULT_ENABLED_TOOLS);
  const [settingsOpen, setSettingsOpen] = useState(
    () => typeof window !== "undefined" && window.innerWidth >= 1024
  );

  // Chat bar key to force reset
  const [chatKey, setChatKey] = useState(0);

  const handleReset = useCallback(() => {
    setGenerator(DEFAULT_GENERATOR);
    setLimit(10);
    setSteps(DEFAULT_STEPS);
    setSystemPrompt("");
    setAgentKnowledge("");
    setAgenticEnabled(false);
    setMaxIterations(5);
    setEnabledTools(DEFAULT_ENABLED_TOOLS);
    setChatKey((k) => k + 1);
  }, []);

  return (
    <Antfly url={apiUrl} table={selectedTable || ""}>
      <div className="h-full">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold">Chat Playground</h1>
            <p className="text-muted-foreground text-sm mt-1">
              Multi-turn conversation with AI-powered retrieval
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
            {chatIndexes.length > 0 ? (
              <Badge variant="outline">
                {chatIndexes.length} index{chatIndexes.length !== 1 ? "es" : ""}
              </Badge>
            ) : (
              <span className="text-amber-600 dark:text-amber-400 text-xs">
                No searchable index found
              </span>
            )}
          </div>
        ) : (
          <div className="mb-4 p-3 rounded-lg border border-dashed text-sm text-muted-foreground">
            Select a table from the sidebar to get started.
          </div>
        )}

        {/* Main Grid — asymmetric: narrow settings, wide chat */}
        <div className="grid grid-cols-1 lg:grid-cols-[340px_1fr] gap-6">
          {/* Left Column - Settings */}
          <div className="space-y-4">
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
                            placeholder="gpt-4o-mini"
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
                        <Switch
                          checked={steps.classification.enabled}
                          onCheckedChange={(v) =>
                            setSteps((s) => ({
                              ...s,
                              classification: { enabled: v },
                            }))
                          }
                        />
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

                    <Separator />

                    {/* Agentic Mode */}
                    <div className="space-y-4">
                      <div className="flex items-center justify-between p-3 rounded-lg border">
                        <div className="flex items-center gap-3">
                          <Bot className="h-4 w-4 text-muted-foreground" />
                          <div>
                            <p className="text-sm font-medium">Agentic Mode</p>
                            <p className="text-xs text-muted-foreground">
                              LLM autonomously picks tools
                            </p>
                          </div>
                        </div>
                        <Switch checked={agenticEnabled} onCheckedChange={setAgenticEnabled} />
                      </div>

                      {agenticEnabled && (
                        <div className="space-y-3 pl-2">
                          <div className="space-y-2">
                            <Label className="text-xs text-muted-foreground">Max Iterations</Label>
                            <Input
                              type="number"
                              value={maxIterations}
                              onChange={(e) =>
                                setMaxIterations(
                                  Math.max(1, Math.min(20, parseInt(e.target.value, 10) || 5))
                                )
                              }
                              min={1}
                              max={20}
                              className="w-20 h-7 text-xs"
                            />
                          </div>
                          <div className="space-y-2">
                            <Label className="text-xs text-muted-foreground">Tools</Label>
                            <div className="space-y-1">
                              {AVAILABLE_TOOLS.map((tool) => (
                                <label
                                  key={tool.name}
                                  className="flex items-center gap-2 text-xs p-1.5 rounded hover:bg-muted cursor-pointer"
                                >
                                  <input
                                    type="checkbox"
                                    checked={enabledTools.includes(tool.name)}
                                    onChange={(e) => {
                                      if (e.target.checked) {
                                        setEnabledTools((t) => [...t, tool.name]);
                                      } else {
                                        setEnabledTools((t) => t.filter((n) => n !== tool.name));
                                      }
                                    }}
                                    className="rounded"
                                  />
                                  <span className="font-medium">{tool.label}</span>
                                  <span className="text-muted-foreground">— {tool.desc}</span>
                                </label>
                              ))}
                            </div>
                          </div>
                        </div>
                      )}
                    </div>

                    <Separator />

                    {/* System Prompt */}
                    <div className="space-y-2">
                      <Label>System Prompt (optional)</Label>
                      <Textarea
                        placeholder="Custom instructions for the generator..."
                        value={systemPrompt}
                        onChange={(e) => setSystemPrompt(e.target.value)}
                        className="min-h-[80px] resize-none text-sm"
                      />
                    </div>

                    {/* Agent Knowledge */}
                    <div className="space-y-2">
                      <Label>Agent Knowledge (optional)</Label>
                      <Textarea
                        placeholder="Domain-specific knowledge for the agent..."
                        value={agentKnowledge}
                        onChange={(e) => setAgentKnowledge(e.target.value)}
                        className="min-h-[60px] resize-none text-sm"
                      />
                    </div>
                  </CardContent>
                </CollapsibleContent>
              </Collapsible>
            </Card>
          </div>

          {/* Right Column - Chat */}
          <Card className="flex flex-col h-[60vh] lg:h-[calc(100vh-280px)] min-h-[400px]">
            <CardHeader className="pb-3 flex-none">
              <CardTitle className="text-lg">Chat</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 overflow-hidden flex flex-col p-0">
              <ChatBar
                key={chatKey}
                id="chat-playground"
                generator={{
                  provider: generator.provider,
                  model: generator.model,
                  temperature: generator.temperature,
                }}
                table={selectedTable}
                semanticIndexes={chatIndexes.length > 0 ? chatIndexes : undefined}
                agentKnowledge={agentKnowledge || undefined}
                systemPrompt={systemPrompt || undefined}
                maxIterations={agenticEnabled ? maxIterations : undefined}
                limit={limit}
                followUpCount={steps.followup.enabled ? steps.followup.count : undefined}
                showFollowUpQuestions={steps.followup.enabled}
                showConfidence={steps.confidence.enabled}
                showHits
                steps={{
                  generation: { enabled: true },
                  classification: { enabled: steps.classification.enabled },
                  followup: {
                    enabled: steps.followup.enabled,
                    count: steps.followup.count,
                  },
                  confidence: { enabled: steps.confidence.enabled },
                }}
                tools={
                  agenticEnabled
                    ? {
                        enabled_tools: enabledTools,
                        max_tool_iterations: maxIterations,
                      }
                    : undefined
                }
                placeholder="Ask a question..."
                {...aiRenderers}
                renderAssistantMessage={(message: string, isStreaming: boolean, turn: ChatTurn) => (
                  <>
                    {aiRenderers.renderAssistantMessage?.(message, isStreaming, turn)}
                    {(turn.reasoningChain.length > 0 ||
                      turn.activeSteps.length > 0 ||
                      turn.reasoningText) && (
                      <div className="mt-1 ml-1">
                        <ReasoningChainCollapsible
                          chain={turn.reasoningChain}
                          activeSteps={turn.activeSteps}
                          toolCallsMade={turn.toolCallsMade}
                          reasoningText={turn.reasoningText}
                          isStreaming={turn.isStreaming}
                        />
                      </div>
                    )}
                  </>
                )}
                renderInput={({
                  value,
                  onChange,
                  onSubmit,
                  isStreaming: streaming,
                  placeholder,
                  abort,
                }) => (
                  <PromptInput
                    onSubmit={(_msg, e) => {
                      e.preventDefault();
                      onSubmit();
                    }}
                  >
                    <PromptInputTextarea
                      value={value}
                      onChange={(e) => onChange(e.target.value)}
                      placeholder={placeholder}
                      disabled={streaming || !selectedTable || chatIndexes.length === 0}
                    />
                    <PromptInputFooter>
                      <PromptInputSubmit
                        status={turnToStatus(streaming, !!value)}
                        onClick={streaming ? () => abort() : undefined}
                      />
                    </PromptInputFooter>
                  </PromptInput>
                )}
                renderError={(error) => (
                  <div className="mb-3 mx-1 p-3 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive text-sm">
                    {error}
                  </div>
                )}
              />
            </CardContent>
          </Card>
        </div>
      </div>
    </Antfly>
  );
};

export default ChatPlaygroundPage;
