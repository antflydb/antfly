import { ReloadIcon } from "@radix-ui/react-icons";
import { Clock, HelpCircle, ListPlus, MessageCircle, Plus, RotateCcw, Zap } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { BackendInfoBar } from "@/components/playground/BackendInfoBar";
import { NoModelsGuide } from "@/components/playground/NoModelsGuide";
import type { SamplePreset } from "@/components/playground/SamplePresets";
import { SamplePresets } from "@/components/playground/SamplePresets";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import { useApiConfig } from "@/hooks/use-api-config";
import { useEvalSets } from "@/hooks/use-eval-sets";
import { fetchWithRetry } from "@/lib/utils";

// Rewrite response types matching Termite API
interface RewriteResponse {
  model: string;
  texts: string[][];
}

interface ModelInfo {
  capabilities?: string[];
}

interface ModelsResponse {
  rewriters: Record<string, ModelInfo>;
  [key: string]: Record<string, ModelInfo>;
}

const STORAGE_KEY = "antfarm-playground-question";

const SAMPLE_DATA = {
  eiffel: {
    name: "Eiffel Tower",
    description: "Historical fact about Paris landmark",
    context: `The Eiffel Tower is a wrought-iron lattice tower on the Champ de Mars in Paris, France. It is named after the engineer Gustave Eiffel, whose company designed and built the tower from 1887 to 1889 as the entrance arch for the 1889 World's Fair. The tower is 330 metres tall and was the tallest man-made structure in the world until the Chrysler Building in New York City was built in 1930.`,
    answer: "Gustave Eiffel",
  },
  dna: {
    name: "DNA Discovery",
    description: "Scientific breakthrough in biology",
    context: `The structure of DNA was discovered in 1953 by James Watson and Francis Crick at the University of Cambridge. They built on the X-ray crystallography work of Rosalind Franklin and Maurice Wilkins at King's College London. The double helix model they proposed explained how genetic information is stored and replicated, earning Watson, Crick, and Wilkins the Nobel Prize in Physiology or Medicine in 1962.`,
    answer: "James Watson and Francis Crick",
  },
  internet: {
    name: "Internet History",
    description: "Technology and computing origins",
    context: `The World Wide Web was invented by Tim Berners-Lee in 1989 while working at CERN in Geneva, Switzerland. He proposed an information management system that used hypertext to link documents across computer networks. The first website, info.cern.ch, went live on December 20, 1990. By 1993, CERN had made the World Wide Web software available on a royalty-free basis, enabling the explosive growth of the internet.`,
    answer: "Tim Berners-Lee",
  },
};

// Format input for LMQG question generation models
function formatInput(ctx: string, ans: string): string {
  // Highlight the answer in the context
  const highlightedContext = ctx.replace(
    new RegExp(`(${ans.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`, "i"),
    "<hl> $1 <hl>"
  );
  return `generate question: ${highlightedContext}`;
}

const RewritingPlaygroundPage: React.FC = () => {
  const { termiteApiUrl } = useApiConfig();
  const [searchParams, setSearchParams] = useSearchParams();

  // Restore state from localStorage
  const [context, setContext] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).context || "";
    } catch {
      /* ignore */
    }
    return "";
  });
  const [answer, setAnswer] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).answer || "";
    } catch {
      /* ignore */
    }
    return "";
  });
  const [selectedModel, setSelectedModel] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).selectedModel || "";
    } catch {
      /* ignore */
    }
    return "";
  });
  const [result, setResult] = useState<RewriteResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [modelsLoaded, setModelsLoaded] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);

  // Eval set integration
  const { evalSets, createEvalSet, addItem, getEvalSetNames } = useEvalSets();
  const [showEvalSetDialog, setShowEvalSetDialog] = useState(false);
  const [selectedEvalSet, setSelectedEvalSet] = useState("");
  const [newEvalSetName, setNewEvalSetName] = useState("");
  const [selectedQuestion, setSelectedQuestion] = useState("");
  const [evalSetSuccess, setEvalSetSuccess] = useState(false);

  // Persist state to localStorage
  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ context, answer, selectedModel }));
  }, [context, answer, selectedModel]);

  // Fetch available models on mount — use rewriters, not generators
  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        const response = await fetch(`${termiteApiUrl}/api/models`, {
          signal: controller.signal,
        });
        if (response.ok) {
          const data: ModelsResponse = await response.json();
          const rewriters = Object.keys(data.rewriters || {});
          setAvailableModels(rewriters);
          setSelectedModel((prev: string) =>
            prev && rewriters.includes(prev) ? prev : rewriters[0] || ""
          );
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
  }, [termiteApiUrl]);

  // Handle ?model= URL param from Model Registry "Open in Playground"
  useEffect(() => {
    const modelParam = searchParams.get("model");
    if (modelParam && modelsLoaded && availableModels.includes(modelParam)) {
      setSelectedModel(modelParam);
      setSearchParams(
        (prev) => {
          prev.delete("model");
          return prev;
        },
        { replace: true }
      );
    }
  }, [searchParams, modelsLoaded, availableModels, setSearchParams]);

  const handleGenerate = useCallback(async () => {
    if (!context.trim()) {
      setError("Please enter a context passage");
      return;
    }

    if (!answer.trim()) {
      setError("Please enter an answer to generate a question for");
      return;
    }

    if (!selectedModel) {
      setError("Please select a model");
      return;
    }

    // Check if answer appears in context
    if (!context.toLowerCase().includes(answer.toLowerCase())) {
      setError("The answer should appear in the context passage");
      return;
    }

    // Cancel any previous request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    abortControllerRef.current = new AbortController();
    setIsLoading(true);
    setError(null);
    setResult(null);

    const startTime = performance.now();

    try {
      const formattedInput = formatInput(context, answer);

      const response = await fetchWithRetry(`${termiteApiUrl}/api/rewrite`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          model: selectedModel,
          inputs: [formattedInput],
        }),
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || `HTTP ${response.status}`);
      }

      const data: RewriteResponse = await response.json();
      setResult(data);
      setProcessingTime(performance.now() - startTime);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      setError(
        err instanceof Error
          ? err.message
          : "Failed to connect to Termite. Make sure Termite is running."
      );
    } finally {
      setIsLoading(false);
    }
  }, [context, answer, selectedModel, termiteApiUrl]);

  // Cmd+Enter shortcut
  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        handleGenerate();
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, [handleGenerate]);

  const handleReset = () => {
    setContext("");
    setAnswer("");
    setResult(null);
    setError(null);
    setProcessingTime(null);
    localStorage.removeItem(STORAGE_KEY);
  };

  const openAddToEvalSetDialog = (question: string) => {
    setSelectedQuestion(question);
    setShowEvalSetDialog(true);
    setEvalSetSuccess(false);
    // Select first eval set by default if available
    const names = getEvalSetNames();
    if (names.length > 0 && !selectedEvalSet) {
      setSelectedEvalSet(names[0]);
    }
  };

  const handleAddToEvalSet = () => {
    let targetSetName = selectedEvalSet;

    // If creating a new set, create it first
    if (newEvalSetName.trim()) {
      const created = createEvalSet(newEvalSetName.trim());
      if (created) {
        targetSetName = created.name;
        setSelectedEvalSet(targetSetName);
      } else {
        return; // Set already exists or invalid name
      }
    }

    if (!targetSetName) return;

    // Add the Q+A pair to the eval set
    addItem(targetSetName, selectedQuestion, answer);
    setEvalSetSuccess(true);
    setNewEvalSetName("");

    // Close dialog after short delay to show success state
    setTimeout(() => {
      setShowEvalSetDialog(false);
      setEvalSetSuccess(false);
    }, 1000);
  };

  // Highlight the answer in the context for display
  const renderHighlightedContext = () => {
    if (!answer.trim()) {
      return <span>{context}</span>;
    }

    const regex = new RegExp(`(${answer.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`, "gi");
    const parts = context.split(regex);

    let charOffset = 0;
    return (
      <>
        {parts.map((part: string) => {
          const key = `part-${charOffset}`;
          charOffset += part.length;
          return part.toLowerCase() === answer.toLowerCase() ? (
            <span
              key={key}
              className="bg-yellow-200 dark:bg-yellow-900/50 text-yellow-900 dark:text-yellow-100 px-1 rounded"
            >
              {part}
            </span>
          ) : (
            <span key={key}>{part}</span>
          );
        })}
      </>
    );
  };

  const samplePresets: SamplePreset[] = Object.values(SAMPLE_DATA).map((sample) => ({
    name: sample.name,
    description: sample.description,
    onLoad: () => {
      setContext(sample.context);
      setAnswer(sample.answer);
    },
  }));

  return (
    <div className="h-full">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Rewriting Playground</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Transform text using Seq2Seq models (question generation, paraphrasing, etc.)
          </p>
        </div>
        <div className="flex gap-2">
          <SamplePresets presets={samplePresets} />
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </div>
      </div>

      <BackendInfoBar />

      {modelsLoaded && availableModels.length === 0 && (
        <NoModelsGuide modelType="rewriter" typeName="rewriting" />
      )}

      {/* Configuration Panel */}
      <Card className="mb-6">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {/* Model Selection */}
            <div className="space-y-2">
              <Label htmlFor="model">Model</Label>
              <Select
                value={selectedModel}
                onValueChange={setSelectedModel}
                disabled={!modelsLoaded || availableModels.length === 0}
              >
                <SelectTrigger id="model">
                  <SelectValue
                    placeholder={
                      !modelsLoaded
                        ? "Loading models..."
                        : availableModels.length === 0
                          ? "No models available"
                          : "Select a model"
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

            {/* Generate Button */}
            <div className="space-y-2 flex items-end lg:col-start-3">
              <Button
                onClick={handleGenerate}
                disabled={isLoading || !context.trim() || !answer.trim() || !selectedModel}
                className="w-full"
              >
                {isLoading ? (
                  <>
                    <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                    Generating
                  </>
                ) : (
                  <>
                    <HelpCircle className="h-4 w-4 mr-2" />
                    Generate Question
                  </>
                )}
              </Button>
            </div>
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
      {result && (
        <div className="mb-6 flex flex-wrap items-center gap-3">
          <Badge variant="secondary" className="gap-1.5">
            <Zap className="h-3 w-3" />
            {result.model}
          </Badge>
          {processingTime && (
            <Badge variant="outline" className="gap-1.5">
              <Clock className="h-3 w-3" />
              {processingTime.toFixed(0)}ms
            </Badge>
          )}
        </div>
      )}

      {/* Main Content */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">Input</CardTitle>
          </CardHeader>
          <CardContent className="flex-1 space-y-4">
            {/* Context */}
            <div className="space-y-2">
              <Label htmlFor="context">Context Passage</Label>
              <Textarea
                id="context"
                placeholder="Enter a passage of text containing the answer..."
                className="h-48 resize-y font-mono text-sm"
                value={context}
                onChange={(e) => setContext(e.target.value)}
              />
            </div>

            {/* Answer */}
            <div className="space-y-2">
              <Label htmlFor="answer">Answer</Label>
              <Textarea
                id="answer"
                placeholder="Enter the answer that the question should target..."
                className="h-20 resize-y font-mono text-sm"
                value={answer}
                onChange={(e) => setAnswer(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                The answer should appear in the context passage above.
              </p>
            </div>
          </CardContent>
        </Card>

        {/* Output Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">{result ? "Generated Question" : "Preview"}</CardTitle>
          </CardHeader>
          <CardContent className="flex-1 overflow-hidden">
            {isLoading ? (
              <div className="space-y-6">
                <Skeleton className="h-24 w-full" />
                <Skeleton className="h-32 w-full" />
                <Skeleton className="h-16 w-full" />
              </div>
            ) : result ? (
              <div className="space-y-6">
                {/* Generated Question(s) */}
                <div className="space-y-3">
                  {result.texts.map((textArray) =>
                    textArray.map((question) => (
                      <div
                        key={`q-${question}`}
                        className="p-4 bg-primary/5 border border-primary/20 rounded-lg"
                      >
                        <div className="flex items-start gap-3">
                          <MessageCircle className="h-5 w-5 text-primary mt-0.5 shrink-0" />
                          <div className="flex-1">
                            <p className="text-lg font-medium">{question}</p>
                            <Button
                              variant="outline"
                              size="sm"
                              className="mt-3"
                              onClick={() => openAddToEvalSetDialog(question)}
                            >
                              <ListPlus className="h-4 w-4 mr-1" />
                              Add to Eval Set
                            </Button>
                          </div>
                        </div>
                      </div>
                    ))
                  )}
                </div>

                {/* Context with highlighted answer */}
                <div className="space-y-2">
                  <Label className="text-sm text-muted-foreground">
                    Context (answer highlighted)
                  </Label>
                  <div className="p-3 bg-muted/50 rounded-lg border text-sm leading-relaxed">
                    {renderHighlightedContext()}
                  </div>
                </div>

                {/* Answer */}
                <div className="space-y-2">
                  <Label className="text-sm text-muted-foreground">Target Answer</Label>
                  <div className="p-3 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg border border-yellow-300 dark:border-yellow-700 text-sm font-medium">
                    {answer}
                  </div>
                </div>
              </div>
            ) : (
              <div className="h-80 flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <HelpCircle className="h-12 w-12 mx-auto mb-3 opacity-20" />
                  <p className="mb-3">
                    Enter context and answer, then press{" "}
                    <kbd className="px-1.5 py-0.5 text-xs border rounded bg-muted">Cmd+Enter</kbd>{" "}
                    to generate
                  </p>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      setContext(SAMPLE_DATA.eiffel.context);
                      setAnswer(SAMPLE_DATA.eiffel.answer);
                    }}
                  >
                    Try a sample
                  </Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Help text */}
      <div className="mt-6 text-xs text-muted-foreground space-y-1">
        <p>
          <strong>Question Generation:</strong> Uses Seq2Seq models (T5, FLAN-T5) trained for
          question generation. The model generates a question that would be answered by the
          specified answer within the given context.
        </p>
        <p>
          <strong>LMQG Models:</strong> Compatible with LMQG (Language Model for Question
          Generation) models. The input is automatically formatted with answer highlighting.
        </p>
      </div>

      {/* Add to Eval Set Dialog */}
      <Dialog open={showEvalSetDialog} onOpenChange={setShowEvalSetDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add to Eval Set</DialogTitle>
            <DialogDescription>Add this Q+A pair to an evaluation set.</DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            {/* Show the Q+A pair */}
            <div className="space-y-2 p-3 bg-muted/50 rounded-lg text-sm">
              <div>
                <span className="font-medium">Q:</span> {selectedQuestion}
              </div>
              <div>
                <span className="font-medium">A:</span> {answer}
              </div>
            </div>

            {/* Select existing eval set */}
            {getEvalSetNames().length > 0 && (
              <div className="space-y-2">
                <Label>Select Eval Set</Label>
                <Select value={selectedEvalSet} onValueChange={setSelectedEvalSet}>
                  <SelectTrigger>
                    <SelectValue placeholder="Choose an eval set..." />
                  </SelectTrigger>
                  <SelectContent>
                    {getEvalSetNames().map((name) => (
                      <SelectItem key={name} value={name}>
                        {name} ({evalSets[name]?.items.length || 0} items)
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            {/* Or create new */}
            <div className="space-y-2">
              <Label>Or Create New</Label>
              <div className="flex gap-2">
                <Input
                  value={newEvalSetName}
                  onChange={(e) => setNewEvalSetName(e.target.value)}
                  placeholder="New eval set name..."
                  className="flex-1"
                />
                <Button
                  variant="outline"
                  size="icon"
                  disabled={!newEvalSetName.trim()}
                  onClick={() => {
                    const created = createEvalSet(newEvalSetName.trim());
                    if (created) {
                      setSelectedEvalSet(created.name);
                      setNewEvalSetName("");
                    }
                  }}
                >
                  <Plus className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </div>
          <DialogFooter>
            {evalSetSuccess ? (
              <div className="text-green-600 font-medium">Added successfully!</div>
            ) : (
              <>
                <Button variant="outline" onClick={() => setShowEvalSetDialog(false)}>
                  Cancel
                </Button>
                <Button
                  onClick={handleAddToEvalSet}
                  disabled={!selectedEvalSet && !newEvalSetName.trim()}
                >
                  Add
                </Button>
              </>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default RewritingPlaygroundPage;
