import type { ChatTurn } from "@antfly/components";
import { Antfly, ChatBar } from "@antfly/components";
import { createAIElementsRenderers, turnToStatus } from "@antfly/components/adapters";
import {
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
  DashboardPageActions,
  Field,
  FieldContent,
  FieldDescription,
  FieldLabel,
  FieldTitle,
  Input,
  Label,
  Message,
  MessageContent,
  Separator,
  Switch,
  Textarea,
} from "@antfly/design-system";
import type { ChatToolName, GeneratorConfig } from "@antfly/sdk";
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
import {
  Message as AIMessage,
  MessageContent as AIMessageContent,
  MessageResponse,
} from "@/components/ai-elements/message";
import {
  PromptInput,
  PromptInputFooter,
  PromptInputSubmit,
  PromptInputTextarea,
} from "@/components/ai-elements/prompt-input";
import { Source, Sources, SourcesContent, SourcesTrigger } from "@/components/ai-elements/sources";
import { Suggestion, Suggestions } from "@/components/ai-elements/suggestion";
import { AIWorkflowHeader, AIWorkflowPage } from "@/components/ai-workflow";
import { chatGPTErrorMessage, useChatGPT } from "@/components/chatgpt-provider";
import { getChatRequestGenerator } from "@/components/playground/chat-generator";
import {
  GENERATOR_DEFAULT_CONFIG,
  GeneratorSelector,
  getInheritedGeneratorLabels,
} from "@/components/playground/GeneratorSelector";
import { ReasoningChainCollapsible } from "@/components/playground/ReasoningChainCollapsible";
import { AskModeNavigation, RetrievalContextBar } from "@/components/retrieval-context";
import { useApiConfig } from "@/hooks/use-api-config";
import { useGeneratorPreference } from "@/hooks/use-generator-preference";
import { useTable } from "@/hooks/use-table";

interface StepsConfig {
  classification: { enabled: boolean };
  followup: { enabled: boolean; count: number };
  confidence: { enabled: boolean };
}

const DEFAULT_STEPS: StepsConfig = {
  classification: { enabled: false },
  followup: { enabled: false, count: 3 },
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
  { name: "web_search" as ChatToolName, label: "Web Search", desc: "Search the web" },
] as const;

const DEFAULT_ENABLED_TOOLS: ChatToolName[] = ["semantic_search", "full_text_search", "add_filter"];

const aiRenderers = createAIElementsRenderers({
  Message: AIMessage,
  MessageContent: AIMessageContent,
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
  const { dashboardGenerator } = useGeneratorPreference();
  const chatgpt = useChatGPT();
  const { selectedTable, selectedIndex, chatIndexes } = useTable();

  // Config state
  const [generatorOverride, setGeneratorOverride] = useState<GeneratorConfig | null>(null);
  const [limit, setLimit] = useState(10);
  const [steps, setSteps] = useState<StepsConfig>(DEFAULT_STEPS);
  const [systemPrompt, setSystemPrompt] = useState("");
  const [agentKnowledge, setAgentKnowledge] = useState("");
  const [agenticEnabled, setAgenticEnabled] = useState(false);
  const [maxInternalIterations, setMaxInternalIterations] = useState(5);
  const [enabledTools, setEnabledTools] = useState<ChatToolName[]>(DEFAULT_ENABLED_TOOLS);
  const [settingsOpen, setSettingsOpen] = useState(
    () => typeof window !== "undefined" && window.innerWidth >= 1024
  );
  const effectiveGenerator = generatorOverride ?? dashboardGenerator ?? null;
  const requestGenerator = getChatRequestGenerator(effectiveGenerator);
  const { label: inheritedGeneratorLabel, description: inheritedGeneratorDescription } =
    getInheritedGeneratorLabels(dashboardGenerator);

  // Chat bar key to force reset
  const [chatKey, setChatKey] = useState(0);

  const handleReset = useCallback(() => {
    setGeneratorOverride(null);
    setLimit(10);
    setSteps(DEFAULT_STEPS);
    setSystemPrompt("");
    setAgentKnowledge("");
    setAgenticEnabled(false);
    setMaxInternalIterations(5);
    setEnabledTools(DEFAULT_ENABLED_TOOLS);
    setChatKey((k) => k + 1);
  }, []);

  return (
    <Antfly url={apiUrl} table={selectedTable || ""}>
      <AIWorkflowPage className="h-full">
        <AIWorkflowHeader
          eyebrow="Retrieval"
          title="Ask your data"
          description="Explore your documents in a conversation, with sources and retrieval details."
          actions={
            <DashboardPageActions>
              <Button variant="outline" onClick={handleReset}>
                <RotateCcw className="h-4 w-4 mr-2" />
                Reset
              </Button>
            </DashboardPageActions>
          }
        />

        <AskModeNavigation />
        <RetrievalContextBar searchable />
        {/* Main Grid — asymmetric: narrow settings, wide chat */}
        <div className="grid grid-cols-1 lg:grid-cols-[minmax(0,300px)_minmax(0,1fr)] gap-6">
          {/* Left Column - Settings */}
          <div className="space-y-4">
            <Card>
              <Collapsible open={settingsOpen} onOpenChange={setSettingsOpen}>
                <CardHeader className="pb-3">
                  <CollapsibleTrigger asChild>
                    <button
                      type="button"
                      className="flex w-full items-center justify-between text-left"
                    >
                      <CardTitle className="text-lg flex items-center gap-2">
                        <Settings className="h-4 w-4" />
                        Settings
                      </CardTitle>
                      {settingsOpen ? (
                        <ChevronDown className="h-4 w-4" />
                      ) : (
                        <ChevronRight className="h-4 w-4" />
                      )}
                    </button>
                  </CollapsibleTrigger>
                </CardHeader>
                <CollapsibleContent>
                  <CardContent className="space-y-6">
                    {/* Generator Config */}
                    <div className="space-y-4">
                      <Label className="text-sm font-medium">Generator</Label>
                      <GeneratorSelector
                        allowPersonalConnections
                        value={generatorOverride}
                        onChange={setGeneratorOverride}
                        defaultConfig={GENERATOR_DEFAULT_CONFIG}
                        defaultLabel={inheritedGeneratorLabel}
                        defaultDescription={inheritedGeneratorDescription}
                      />
                    </div>

                    {/* Query Options */}
                    <Field className="gap-2">
                      <FieldLabel className="text-xs text-muted-foreground">
                        Result Limit
                      </FieldLabel>
                      <Input
                        type="number"
                        value={limit}
                        onChange={(e) => setLimit(parseInt(e.target.value, 10) || 10)}
                        min={1}
                        max={50}
                        className="w-24"
                      />
                    </Field>

                    <Separator />

                    {/* Pipeline Steps */}
                    <div className="space-y-4">
                      <Label className="text-sm font-medium">Pipeline Steps</Label>

                      {/* Classification */}
                      <Field orientation="horizontal" className="p-3 rounded-lg border">
                        <Sparkles className="h-4 w-4 text-muted-foreground shrink-0" />
                        <FieldContent>
                          <FieldTitle>Classification</FieldTitle>
                          <FieldDescription className="text-xs">
                            Analyze query strategy
                          </FieldDescription>
                        </FieldContent>
                        <Switch
                          checked={steps.classification.enabled}
                          onCheckedChange={(v) =>
                            setSteps((s) => ({
                              ...s,
                              classification: { enabled: v },
                            }))
                          }
                        />
                      </Field>

                      {/* Follow-up */}
                      <Field orientation="horizontal" className="p-3 rounded-lg border">
                        <HelpCircle className="h-4 w-4 text-muted-foreground shrink-0" />
                        <FieldContent>
                          <FieldTitle>Follow-up Questions</FieldTitle>
                          <FieldDescription className="text-xs">
                            Generate related questions
                          </FieldDescription>
                        </FieldContent>
                        <div className="flex items-center gap-3">
                          {steps.followup.enabled && (
                            <Field orientation="horizontal" className="w-auto gap-2">
                              <FieldLabel className="text-xs">Count</FieldLabel>
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
                            </Field>
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
                      </Field>

                      {/* Confidence */}
                      <Field orientation="horizontal" className="p-3 rounded-lg border">
                        <Target className="h-4 w-4 text-muted-foreground shrink-0" />
                        <FieldContent>
                          <FieldTitle>Confidence Scores</FieldTitle>
                          <FieldDescription className="text-xs">
                            Rate answer confidence
                          </FieldDescription>
                        </FieldContent>
                        <Switch
                          checked={steps.confidence.enabled}
                          onCheckedChange={(v) =>
                            setSteps((s) => ({
                              ...s,
                              confidence: { enabled: v },
                            }))
                          }
                        />
                      </Field>
                    </div>

                    <Separator />

                    {/* Agentic Mode */}
                    <div className="space-y-4">
                      <Field orientation="horizontal" className="p-3 rounded-lg border">
                        <Bot className="h-4 w-4 text-muted-foreground shrink-0" />
                        <FieldContent>
                          <FieldTitle>Agentic Mode</FieldTitle>
                          <FieldDescription className="text-xs">
                            LLM autonomously picks tools
                          </FieldDescription>
                        </FieldContent>
                        <Switch checked={agenticEnabled} onCheckedChange={setAgenticEnabled} />
                      </Field>

                      {agenticEnabled && (
                        <div className="space-y-3 pl-2">
                          <Field className="gap-2">
                            <FieldLabel className="text-xs text-muted-foreground">
                              Max Internal Iterations
                            </FieldLabel>
                            <Input
                              type="number"
                              value={maxInternalIterations}
                              onChange={(e) =>
                                setMaxInternalIterations(
                                  Math.max(1, Math.min(20, parseInt(e.target.value, 10) || 5))
                                )
                              }
                              min={1}
                              max={20}
                              className="w-20 h-7 text-xs"
                            />
                          </Field>
                          <div className="space-y-2">
                            <Label className="text-xs text-muted-foreground">Tools</Label>
                            <div className="space-y-1">
                              {AVAILABLE_TOOLS.map((tool) => (
                                <label
                                  key={tool.name}
                                  className="flex items-center gap-2 text-xs p-1.5 rounded-md hover:bg-muted cursor-pointer"
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
                                    className="rounded-md"
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
                    <Field className="gap-2">
                      <FieldLabel>System Prompt (optional)</FieldLabel>
                      <Textarea
                        placeholder="Custom instructions for the generator..."
                        value={systemPrompt}
                        onChange={(e) => setSystemPrompt(e.target.value)}
                        className="min-h-[80px] resize-none text-sm"
                      />
                    </Field>

                    {/* Agent Knowledge */}
                    <Field className="gap-2">
                      <FieldLabel>Agent Knowledge (optional)</FieldLabel>
                      <Textarea
                        placeholder="Domain-specific knowledge for the agent..."
                        value={agentKnowledge}
                        onChange={(e) => setAgentKnowledge(e.target.value)}
                        className="min-h-[60px] resize-none text-sm"
                      />
                    </Field>
                  </CardContent>
                </CollapsibleContent>
              </Collapsible>
            </Card>
          </div>

          {/* Right Column - Chat */}
          <Card className="flex flex-col min-h-[28rem] lg:h-[calc(100vh-280px)] min-h-[400px]">
            <CardHeader className="pb-3 flex-none">
              <CardTitle className="text-lg">Chat</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 overflow-hidden flex flex-col p-0">
              {requestGenerator?.provider === "chatgpt" && !chatgpt.supported ? (
                <p role="alert" className="p-6 text-sm text-muted-foreground">
                  {chatgpt.unavailableMessage}
                </p>
              ) : requestGenerator ? (
                <ChatBar
                  key={chatKey}
                  id="chat-playground"
                  generator={requestGenerator}
                  table={selectedTable}
                  semanticIndexes={selectedIndex ? [selectedIndex] : chatIndexes}
                  agentKnowledge={agentKnowledge || undefined}
                  systemPrompt={systemPrompt || undefined}
                  maxInternalIterations={agenticEnabled ? maxInternalIterations : undefined}
                  limit={limit}
                  followUpCount={steps.followup.enabled ? steps.followup.count : undefined}
                  showFollowUpQuestions={steps.followup.enabled}
                  showConfidence={steps.confidence.enabled}
                  showHits
                  steps={{
                    generation: {},
                    ...(steps.classification.enabled ? { classification: {} } : {}),
                    ...(steps.followup.enabled
                      ? { followup: { count: steps.followup.count } }
                      : {}),
                    ...(steps.confidence.enabled ? { confidence: {} } : {}),
                  }}
                  tools={
                    agenticEnabled
                      ? {
                          enabled_tools: enabledTools,
                          max_tool_iterations: maxInternalIterations,
                        }
                      : undefined
                  }
                  placeholder="Ask a question..."
                  {...aiRenderers}
                  renderUserMessage={(message: string) => (
                    <Message align="end">
                      <MessageContent className="w-fit max-w-[95%] rounded-md bg-secondary px-4 py-3 text-foreground">
                        <MessageResponse>{message}</MessageResponse>
                      </MessageContent>
                    </Message>
                  )}
                  renderAssistantMessage={(
                    message: string,
                    isStreaming: boolean,
                    turn: ChatTurn
                  ) => (
                    <Message>
                      <MessageContent className="max-w-[95%] text-foreground">
                        <MessageResponse parseIncompleteMarkdown={isStreaming}>
                          {message || " "}
                        </MessageResponse>
                        {(turn.steps.length > 0 ||
                          turn.activeSteps.length > 0 ||
                          turn.reasoningText) && (
                          <ReasoningChainCollapsible
                            chain={turn.steps}
                            activeSteps={turn.activeSteps}
                            toolCallsMade={turn.toolCallsMade}
                            reasoningText={turn.reasoningText}
                            isStreaming={turn.isStreaming}
                          />
                        )}
                      </MessageContent>
                    </Message>
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
                      {error.includes("ChatGPT") ? chatGPTErrorMessage(new Error(error)) : error}
                    </div>
                  )}
                />
              ) : (
                <div className="m-auto p-6 text-center text-sm text-muted-foreground">
                  Configure a generator to start a chat.
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </AIWorkflowPage>
    </Antfly>
  );
};

export default ChatPlaygroundPage;
