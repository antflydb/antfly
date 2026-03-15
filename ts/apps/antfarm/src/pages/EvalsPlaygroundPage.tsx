import { generatorProviders } from "@antfly/sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import {
  Check,
  CheckCircle,
  ChevronDown,
  ChevronRight,
  Clock,
  Download,
  FileText,
  Percent,
  Play,
  Plus,
  RotateCcw,
  Settings,
  Trash2,
  Upload,
  X,
  XCircle,
} from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
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
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
import { useApi } from "@/hooks/use-api-config";
import { useEvalSets } from "@/hooks/use-eval-sets";
import { useTable } from "@/hooks/use-table";
import type { EvalItem, EvalItemResult, EvalRunResult } from "@/types/evals";

// Generator provider type from SDK
type GeneratorProvider = (typeof generatorProviders)[number];

interface JudgeConfig {
  provider: GeneratorProvider;
  model: string;
  temperature?: number;
}

const DEFAULT_JUDGE: JudgeConfig = {
  provider: "openai",
  model: "gpt-4o",
  temperature: 0, // Backend hardcodes temperature=0
};

// Sample eval set for demo
const SAMPLE_EVAL_SET = {
  name: "Sample Eval Set",
  items: [
    {
      question: "What is the capital of France?",
      referenceAnswer: "Paris is the capital of France.",
    },
    {
      question: "When was the Eiffel Tower built?",
      referenceAnswer:
        "The Eiffel Tower was built between 1887 and 1889 as the entrance arch for the 1889 World's Fair.",
    },
    {
      question: "Who designed the Eiffel Tower?",
      referenceAnswer: "The Eiffel Tower was designed by Gustave Eiffel's engineering company.",
    },
  ],
};

const EvalsPlaygroundPage: React.FC = () => {
  const apiClient = useApi();
  const {
    evalSets,
    createEvalSet,
    deleteEvalSet,
    addItem,
    removeItem,
    getEvalSet,
    getEvalSetNames,
    exportEvalSet,
    importEvalSet,
    importPromptfooSet,
  } = useEvalSets();

  const { selectedTable, selectedIndex } = useTable();

  // State
  const [selectedSetName, setSelectedSetName] = useState<string>("");
  const [judge, setJudge] = useState<JudgeConfig>(DEFAULT_JUDGE);
  const [showJudgeSettings, setShowJudgeSettings] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [progress, setProgress] = useState<{ current: number; total: number } | null>(null);
  const [results, setResults] = useState<EvalRunResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Dialog state
  const [showNewSetDialog, setShowNewSetDialog] = useState(false);
  const [showAddItemDialog, setShowAddItemDialog] = useState(false);
  const [showImportDialog, setShowImportDialog] = useState(false);
  const [newSetName, setNewSetName] = useState("");
  const [newItemQuestion, setNewItemQuestion] = useState("");
  const [newItemAnswer, setNewItemAnswer] = useState("");
  const [importJson, setImportJson] = useState("");
  const [importSetName, setImportSetName] = useState("");
  const [importFormat, setImportFormat] = useState<"native" | "promptfoo" | "unknown">("unknown");
  const [expandedResults, setExpandedResults] = useState<Set<string>>(new Set());

  const abortControllerRef = useRef<AbortController | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Set first eval set as default
  useEffect(() => {
    const names = getEvalSetNames();
    if (names.length > 0 && !selectedSetName) {
      setSelectedSetName(names[0]);
    }
  }, [getEvalSetNames, selectedSetName]);

  const selectedSet = selectedSetName ? getEvalSet(selectedSetName) : undefined;

  const handleCreateSet = () => {
    if (!newSetName.trim()) return;
    const created = createEvalSet(newSetName.trim());
    if (created) {
      setSelectedSetName(created.name);
      setNewSetName("");
      setShowNewSetDialog(false);
    }
  };

  const handleDeleteSet = () => {
    if (!selectedSetName) return;
    if (!confirm(`Delete eval set "${selectedSetName}"?`)) return;
    const deletedName = selectedSetName;
    deleteEvalSet(deletedName);
    // deleteEvalSet triggers async state update, so getEvalSetNames() would return stale data.
    // Reset selection and let the useEffect at line 137 pick the new first set.
    setSelectedSetName("");
    setResults(null);
  };

  const handleAddItem = () => {
    if (!selectedSetName || !newItemQuestion.trim() || !newItemAnswer.trim()) return;
    addItem(selectedSetName, newItemQuestion.trim(), newItemAnswer.trim());
    setNewItemQuestion("");
    setNewItemAnswer("");
    setShowAddItemDialog(false);
  };

  const handleRemoveItem = (itemId: string) => {
    if (!selectedSetName) return;
    removeItem(selectedSetName, itemId);
  };

  const handleExport = () => {
    if (!selectedSetName) return;
    const json = exportEvalSet(selectedSetName);
    if (!json) return;

    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${selectedSetName}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleImportFile = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const defaultName = file.name.replace(/\.json$/i, "");

    const reader = new FileReader();
    reader.onload = (event) => {
      const content = event.target?.result as string;
      setImportJson(content);

      // Auto-detect format
      try {
        const parsed = JSON.parse(content);
        if (Array.isArray(parsed)) {
          // Promptfoo format: array of {vars: {question, reference_answer}}
          setImportFormat("promptfoo");
          setImportSetName(defaultName);
        } else if (parsed.name && typeof parsed.name === "string") {
          // Native format: {name, items: [...]}
          setImportFormat("native");
          setImportSetName(parsed.name);
        } else {
          setImportFormat("unknown");
          setImportSetName(defaultName);
        }
      } catch {
        setImportFormat("unknown");
        setImportSetName(defaultName);
      }

      setShowImportDialog(true);
    };
    reader.readAsText(file);
    e.target.value = "";
  };

  const handleImport = () => {
    let result: { success: boolean; error?: string; name?: string };

    if (importFormat === "promptfoo") {
      result = importPromptfooSet(importJson, importSetName);
    } else if (importFormat === "native") {
      // For native format, update the name in the JSON if user changed it
      try {
        const parsed = JSON.parse(importJson);
        parsed.name = importSetName;
        result = importEvalSet(JSON.stringify(parsed));
      } catch {
        result = { success: false, error: "Invalid JSON" };
      }
    } else {
      // Try native first, then promptfoo
      result = importEvalSet(importJson);
      if (!result.success) {
        result = importPromptfooSet(importJson, importSetName);
      }
    }

    if (result.success && result.name) {
      setSelectedSetName(result.name);
      setImportJson("");
      setImportSetName("");
      setImportFormat("unknown");
      setShowImportDialog(false);
    } else {
      setError(result.error || "Failed to import");
    }
  };

  const loadSampleSet = () => {
    // Create sample set if it doesn't exist
    const set = getEvalSet(SAMPLE_EVAL_SET.name);
    if (!set) {
      createEvalSet(SAMPLE_EVAL_SET.name);
      for (const item of SAMPLE_EVAL_SET.items) {
        addItem(SAMPLE_EVAL_SET.name, item.question, item.referenceAnswer);
      }
    }
    setSelectedSetName(SAMPLE_EVAL_SET.name);
  };

  const handleReset = () => {
    setResults(null);
    setError(null);
  };

  const toggleResultExpanded = (itemId: string) => {
    setExpandedResults((prev) => {
      const next = new Set(prev);
      if (next.has(itemId)) {
        next.delete(itemId);
      } else {
        next.add(itemId);
      }
      return next;
    });
  };

  const runEvals = useCallback(async () => {
    if (!selectedSet || selectedSet.items.length === 0) {
      setError("No items in eval set");
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
    setIsLoading(true);
    setError(null);
    setResults(null);
    setProgress({ current: 0, total: selectedSet.items.length });

    const startTime = new Date();
    const itemResults: EvalItemResult[] = [];

    try {
      for (let i = 0; i < selectedSet.items.length; i++) {
        const item = selectedSet.items[i];
        setProgress({ current: i + 1, total: selectedSet.items.length });
        const itemStartTime = performance.now();

        try {
          // Call retrievalAgent with generation + inline eval config
          // Note: queries must include semantic_search (not auto-populated from query)
          // Note: steps.generation must have enabled: true (defaults to false)
          const answerResult = await apiClient.retrievalAgent(
            {
              query: item.question,
              queries: [
                {
                  table: selectedTable,
                  semantic_search: item.question,
                  indexes: selectedIndex ? [selectedIndex] : undefined,
                  limit: 10,
                },
              ],
              stream: false,
              generator: {
                provider: judge.provider,
                model: judge.model,
                temperature: judge.temperature,
              },
              steps: {
                generation: { enabled: true },
                eval: {
                  evaluators: ["correctness"],
                  judge: {
                    provider: judge.provider,
                    model: judge.model,
                    temperature: judge.temperature,
                  },
                  ground_truth: {
                    expectations: item.referenceAnswer,
                  },
                },
              },
            }
            // No callbacks - we want JSON response
          );

          if (answerResult instanceof AbortController) {
            throw new Error("Unexpected streaming response");
          }

          const ragAnswer = answerResult.generation || "";
          const correctnessScore = answerResult.eval_result?.scores?.generation?.correctness;
          const reason = correctnessScore?.reason ?? "";

          // Check if the reason indicates an eval error (not a failed eval)
          const isEvalError = reason.toLowerCase().startsWith("evaluation error:");

          itemResults.push({
            itemId: item.id,
            question: item.question,
            referenceAnswer: item.referenceAnswer,
            actualAnswer: ragAnswer,
            score: correctnessScore?.score ?? 0,
            pass: isEvalError ? false : (correctnessScore?.pass ?? false),
            reason: reason,
            durationMs: performance.now() - itemStartTime,
            error: isEvalError ? reason : undefined,
          });
        } catch (err) {
          if (err instanceof Error && err.name === "AbortError") {
            return;
          }
          itemResults.push({
            itemId: item.id,
            question: item.question,
            referenceAnswer: item.referenceAnswer,
            actualAnswer: "",
            score: 0,
            pass: false,
            reason: "",
            durationMs: performance.now() - itemStartTime,
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      }

      const passed = itemResults.filter((r) => r.pass && !r.error).length;
      const failed = itemResults.filter((r) => !r.pass && !r.error).length;
      const errors = itemResults.filter((r) => r.error).length;
      const totalScore = itemResults.reduce((sum, r) => sum + r.score, 0);

      setResults({
        setName: selectedSetName,
        tableName: selectedTable,
        startedAt: startTime.toISOString(),
        completedAt: new Date().toISOString(),
        results: itemResults,
        summary: {
          total: itemResults.length,
          passed,
          failed,
          errors,
          averageScore: itemResults.length > 0 ? totalScore / itemResults.length : 0,
          totalDurationMs: itemResults.reduce((sum, r) => sum + r.durationMs, 0),
        },
      });
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to run evals");
    } finally {
      setIsLoading(false);
      setProgress(null);
    }
  }, [selectedSet, selectedTable, selectedSetName, judge, apiClient, selectedIndex]);

  return (
    <div className="h-full">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Evals Playground</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Build eval sets and test RAG answers against reference answers
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={loadSampleSet}>
            <FileText className="h-4 w-4 mr-2" />
            Load Sample
          </Button>
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </div>
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

      {/* Configuration Panel */}
      <Card className="mb-6">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {/* Eval Set Selection */}
            <div className="space-y-2">
              <Label>Eval Set</Label>
              <Select value={selectedSetName} onValueChange={setSelectedSetName}>
                <SelectTrigger>
                  <SelectValue placeholder="Select eval set..." />
                </SelectTrigger>
                <SelectContent>
                  {getEvalSetNames().map((name) => (
                    <SelectItem key={name} value={name}>
                      {name} ({evalSets[name]?.items.length || 0})
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Judge Settings Button */}
            <div className="space-y-2">
              <Label>Judge</Label>
              <Button
                variant="outline"
                onClick={() => setShowJudgeSettings(true)}
                className="w-full justify-start"
              >
                <Settings className="h-4 w-4 mr-2" />
                {judge.provider}/{judge.model}
                {judge.temperature !== undefined && ` (t=${judge.temperature})`}
              </Button>
            </div>

            {/* Run Button */}
            <div className="space-y-2 flex flex-col items-stretch">
              {!selectedIndex && selectedTable && (
                <p className="text-sm text-amber-600">No embedding index found for this table</p>
              )}
              <Button
                onClick={runEvals}
                disabled={
                  isLoading ||
                  !selectedSet ||
                  selectedSet.items.length === 0 ||
                  !selectedTable ||
                  !selectedIndex
                }
                className="w-full"
              >
                {isLoading && progress ? (
                  <>
                    <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                    {progress.current} / {progress.total}
                  </>
                ) : (
                  <>
                    <Play className="h-4 w-4 mr-2" />
                    Run Evals
                  </>
                )}
              </Button>
            </div>
          </div>

          {/* Set Management Buttons */}
          <div className="flex flex-wrap gap-2 mt-4 pt-4 border-t">
            <Button variant="outline" size="sm" onClick={() => setShowNewSetDialog(true)}>
              <Plus className="h-4 w-4 mr-1" />
              New Set
            </Button>
            <Button variant="outline" size="sm" onClick={() => fileInputRef.current?.click()}>
              <Upload className="h-4 w-4 mr-1" />
              Import
            </Button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".json"
              onChange={handleImportFile}
              className="hidden"
            />
            <Button variant="outline" size="sm" onClick={handleExport} disabled={!selectedSetName}>
              <Download className="h-4 w-4 mr-1" />
              Export
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={handleDeleteSet}
              disabled={!selectedSetName}
              className="text-destructive hover:text-destructive"
            >
              <Trash2 className="h-4 w-4 mr-1" />
              Delete Set
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <div className="mb-6 p-4 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive text-sm">
          {error}
        </div>
      )}

      {/* Main Content Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Eval Set Items */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">
                Eval Set Items {selectedSet && `(${selectedSet.items.length})`}
              </CardTitle>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setShowAddItemDialog(true)}
                disabled={!selectedSetName}
              >
                <Plus className="h-4 w-4 mr-1" />
                Add Item
              </Button>
            </div>
          </CardHeader>
          <CardContent className="flex-1 overflow-auto">
            {!selectedSet || selectedSet.items.length === 0 ? (
              <div className="h-48 flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <FileText className="h-12 w-12 mx-auto mb-3 opacity-20" />
                  <p>No items in eval set</p>
                  <p className="text-xs mt-1">Add Q+A pairs to build your eval set</p>
                </div>
              </div>
            ) : (
              <div className="space-y-3">
                {selectedSet.items.map((item) => (
                  <EvalItemCard
                    key={item.id}
                    item={item}
                    onRemove={() => handleRemoveItem(item.id)}
                  />
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Results */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">Results</CardTitle>
          </CardHeader>
          <CardContent className="flex-1 overflow-auto">
            {!results ? (
              <div className="h-48 flex items-center justify-center text-muted-foreground">
                <div className="text-center">
                  <Play className="h-12 w-12 mx-auto mb-3 opacity-20" />
                  <p>Run evals to see results</p>
                </div>
              </div>
            ) : (
              <div className="space-y-4">
                {/* Summary */}
                <div className="flex flex-wrap gap-2">
                  <Badge variant="secondary" className="gap-1.5">
                    <Percent className="h-3 w-3" />
                    Avg: {(results.summary.averageScore * 100).toFixed(0)}%
                  </Badge>
                  <Badge variant="outline" className="gap-1.5 text-green-600">
                    <CheckCircle className="h-3 w-3" />
                    Passed: {results.summary.passed}
                  </Badge>
                  <Badge variant="outline" className="gap-1.5 text-red-600">
                    <XCircle className="h-3 w-3" />
                    Failed: {results.summary.failed}
                  </Badge>
                  {results.summary.errors > 0 && (
                    <Badge variant="outline" className="gap-1.5 text-yellow-600">
                      Errors: {results.summary.errors}
                    </Badge>
                  )}
                  <Badge variant="outline" className="gap-1.5">
                    <Clock className="h-3 w-3" />
                    {(results.summary.totalDurationMs / 1000).toFixed(1)}s
                  </Badge>
                </div>

                <Separator />

                {/* Individual Results */}
                <div className="space-y-2">
                  {results.results.map((result, index) => (
                    <Collapsible
                      key={result.itemId}
                      open={expandedResults.has(result.itemId)}
                      onOpenChange={() => toggleResultExpanded(result.itemId)}
                    >
                      <CollapsibleTrigger asChild>
                        <div className="flex items-center gap-2 p-2 rounded-lg border hover:bg-muted/50 cursor-pointer">
                          {expandedResults.has(result.itemId) ? (
                            <ChevronDown className="h-4 w-4" />
                          ) : (
                            <ChevronRight className="h-4 w-4" />
                          )}
                          <span className="text-muted-foreground text-sm w-6">#{index + 1}</span>
                          {result.error ? (
                            <span title={result.error}>❗</span>
                          ) : result.pass ? (
                            <Check className="h-4 w-4 text-green-600" />
                          ) : (
                            <X className="h-4 w-4 text-red-600" />
                          )}
                          <span className="flex-1 truncate text-sm">{result.question}</span>
                          <Badge variant="secondary" className="text-xs">
                            {(result.score * 100).toFixed(0)}%
                          </Badge>
                        </div>
                      </CollapsibleTrigger>
                      <CollapsibleContent>
                        <div className="ml-6 mt-2 p-3 rounded-lg bg-muted/30 space-y-3 text-sm">
                          {result.error ? (
                            <div className="text-yellow-600">
                              <span className="font-medium">Error:</span> {result.error}
                            </div>
                          ) : (
                            <>
                              <div>
                                <span className="font-medium text-muted-foreground">Question:</span>
                                <p className="mt-1">{result.question}</p>
                              </div>
                              <div>
                                <span className="font-medium text-muted-foreground">
                                  Reference:
                                </span>
                                <p className="mt-1">{result.referenceAnswer}</p>
                              </div>
                              <div>
                                <span className="font-medium text-muted-foreground">Actual:</span>
                                <p className="mt-1">{result.actualAnswer}</p>
                              </div>
                              {result.reason && (
                                <div>
                                  <span className="font-medium text-muted-foreground">Reason:</span>
                                  <p className="mt-1 text-muted-foreground">{result.reason}</p>
                                </div>
                              )}
                            </>
                          )}
                        </div>
                      </CollapsibleContent>
                    </Collapsible>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* New Set Dialog */}
      <Dialog open={showNewSetDialog} onOpenChange={setShowNewSetDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create New Eval Set</DialogTitle>
            <DialogDescription>Enter a name for your new evaluation set.</DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="setName">Name</Label>
              <Input
                id="setName"
                value={newSetName}
                onChange={(e) => setNewSetName(e.target.value)}
                placeholder="My Eval Set"
                onKeyDown={(e) => e.key === "Enter" && handleCreateSet()}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowNewSetDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreateSet} disabled={!newSetName.trim()}>
              Create
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Add Item Dialog */}
      <Dialog open={showAddItemDialog} onOpenChange={setShowAddItemDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Eval Item</DialogTitle>
            <DialogDescription>Add a question and reference answer pair.</DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="question">Question</Label>
              <Textarea
                id="question"
                value={newItemQuestion}
                onChange={(e) => setNewItemQuestion(e.target.value)}
                placeholder="What is the capital of France?"
                className="h-20"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="answer">Reference Answer</Label>
              <Textarea
                id="answer"
                value={newItemAnswer}
                onChange={(e) => setNewItemAnswer(e.target.value)}
                placeholder="Paris is the capital of France."
                className="h-20"
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowAddItemDialog(false)}>
              Cancel
            </Button>
            <Button
              onClick={handleAddItem}
              disabled={!newItemQuestion.trim() || !newItemAnswer.trim()}
            >
              Add
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Import Dialog */}
      <Dialog open={showImportDialog} onOpenChange={setShowImportDialog}>
        <DialogContent className="max-h-[80vh] flex flex-col">
          <DialogHeader>
            <DialogTitle>Import Eval Set</DialogTitle>
            <DialogDescription>
              {importFormat === "promptfoo"
                ? "Detected promptfoo format (array with vars.question/reference_answer)"
                : importFormat === "native"
                  ? "Detected native format (object with name and items)"
                  : "Auto-detecting format..."}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4 flex-1 min-h-0">
            <div className="space-y-2">
              <Label htmlFor="importSetName">Eval Set Name</Label>
              <Input
                id="importSetName"
                value={importSetName}
                onChange={(e) => setImportSetName(e.target.value)}
                placeholder="My Eval Set"
              />
            </div>
            <div className="space-y-2 flex-1 min-h-0">
              <Label>Preview</Label>
              <div className="p-3 bg-muted rounded-lg text-xs max-h-40 overflow-auto">
                {(() => {
                  try {
                    const parsed = JSON.parse(importJson);
                    if (Array.isArray(parsed)) {
                      // Promptfoo format
                      const validCount = parsed.filter(
                        (e) => e.vars?.question && e.vars?.reference_answer
                      ).length;
                      return (
                        <div className="space-y-2">
                          <p className="text-muted-foreground">
                            Found {validCount} valid items out of {parsed.length} entries
                          </p>
                          {parsed.slice(0, 3).map((entry, i) => (
                            <div
                              // biome-ignore lint/suspicious/noArrayIndexKey: preview items don't have stable IDs
                              key={i}
                              className="p-2 bg-background rounded border"
                            >
                              <p className="font-medium truncate">
                                Q: {entry.vars?.question || "—"}
                              </p>
                              <p className="text-muted-foreground truncate">
                                A: {entry.vars?.reference_answer || "—"}
                              </p>
                            </div>
                          ))}
                          {parsed.length > 3 && (
                            <p className="text-muted-foreground">...and {parsed.length - 3} more</p>
                          )}
                        </div>
                      );
                    } else if (parsed.items && Array.isArray(parsed.items)) {
                      // Native format
                      return (
                        <div className="space-y-2">
                          <p className="text-muted-foreground">Found {parsed.items.length} items</p>
                          {parsed.items
                            .slice(0, 3)
                            .map(
                              (
                                item: { question?: string; referenceAnswer?: string },
                                i: number
                              ) => (
                                <div
                                  // biome-ignore lint/suspicious/noArrayIndexKey: preview items don't have stable IDs
                                  key={i}
                                  className="p-2 bg-background rounded border"
                                >
                                  <p className="font-medium truncate">Q: {item.question || "—"}</p>
                                  <p className="text-muted-foreground truncate">
                                    A: {item.referenceAnswer || "—"}
                                  </p>
                                </div>
                              )
                            )}
                          {parsed.items.length > 3 && (
                            <p className="text-muted-foreground">
                              ...and {parsed.items.length - 3} more
                            </p>
                          )}
                        </div>
                      );
                    }
                    return <p className="text-yellow-600">Unknown format</p>;
                  } catch {
                    return <p className="text-red-600">Invalid JSON</p>;
                  }
                })()}
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowImportDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleImport} disabled={!importJson.trim() || !importSetName.trim()}>
              Import
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Judge Settings Dialog */}
      <Dialog open={showJudgeSettings} onOpenChange={setShowJudgeSettings}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Judge Settings</DialogTitle>
            <DialogDescription>Configure the LLM judge for evaluating answers.</DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label>Provider</Label>
              <Select
                value={judge.provider}
                onValueChange={(v) => setJudge((j) => ({ ...j, provider: v as GeneratorProvider }))}
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
              <Label>Model</Label>
              <Input
                value={judge.model}
                onChange={(e) => setJudge((j) => ({ ...j, model: e.target.value }))}
                placeholder="gpt-4o"
              />
            </div>
            <div className="space-y-2">
              <Label className="text-muted-foreground">Temperature</Label>
              <Input type="number" value={0} disabled className="bg-muted" />
              <p className="text-xs text-muted-foreground">
                Backend hardcodes temperature=0. Use models that support it (e.g., gpt-4o).
              </p>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowJudgeSettings(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Help text */}
      <div className="mt-6 text-xs text-muted-foreground space-y-1">
        <p>
          <strong>Evals Playground:</strong> Build evaluation sets with Q+A pairs, then run them
          against your RAG system. The correctness evaluator uses an LLM judge to compare the RAG
          answer against your reference answer.
        </p>
      </div>
    </div>
  );
};

// Sub-component for eval item display
function EvalItemCard({ item, onRemove }: { item: EvalItem; onRemove: () => void }) {
  return (
    <div className="p-3 border rounded-lg group relative">
      <Button
        variant="ghost"
        size="icon"
        className="absolute top-2 right-2 h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity"
        onClick={onRemove}
      >
        <X className="h-3 w-3" />
      </Button>
      <div className="space-y-2 pr-8">
        <div>
          <span className="text-xs text-muted-foreground font-medium">Q:</span>
          <p className="text-sm mt-0.5">{item.question}</p>
        </div>
        <div>
          <span className="text-xs text-muted-foreground font-medium">A:</span>
          <p className="text-sm mt-0.5 text-muted-foreground">{item.referenceAnswer}</p>
        </div>
      </div>
    </div>
  );
}

export default EvalsPlaygroundPage;
