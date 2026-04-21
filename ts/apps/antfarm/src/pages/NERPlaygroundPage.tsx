import { ReloadIcon } from "@radix-ui/react-icons";
import {
  Clock,
  Hash,
  Percent,
  Plus,
  RotateCcw,
  SlidersHorizontal,
  Tag,
  X,
  Zap,
} from "lucide-react";
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
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { useApiConfig } from "@/hooks/use-api-config";
import { fetchWithRetry } from "@/lib/utils";

// NER response types matching Termite API
interface NEREntity {
  text: string;
  label: string;
  start: number;
  end: number;
  score: number;
}

interface NERResponse {
  model: string;
  entities: NEREntity[][];
}

// Extract response types
interface ExtractFieldValue {
  value: string;
  score?: number;
  start?: number;
  end?: number;
}

interface ExtractResponse {
  model: string;
  results: Record<string, Record<string, unknown>[]>[];
}

interface ModelInfo {
  capabilities?: string[];
}

interface ModelsResponse {
  recognizers: Record<string, ModelInfo>;
  [key: string]: Record<string, ModelInfo>;
}

type PlaygroundMode = "recognize" | "extract";

// Schema field for extract mode
interface SchemaField {
  name: string;
  type: "str" | "list";
}

interface SchemaStructure {
  name: string;
  fields: SchemaField[];
}

// Default entity labels for GLiNER
const DEFAULT_LABELS = ["person", "organization", "location"];

const STORAGE_KEY = "antfarm-playground-ner";

// Default extract schema
const DEFAULT_SCHEMA: SchemaStructure[] = [
  {
    name: "person",
    fields: [
      { name: "name", type: "str" },
      { name: "age", type: "str" },
      { name: "company", type: "str" },
    ],
  },
];

// Standard entity type colors (for common NER labels)
const STANDARD_LABEL_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  per: {
    bg: "bg-blue-100 dark:bg-blue-900/30",
    text: "text-blue-700 dark:text-blue-300",
    border: "border-blue-300 dark:border-blue-700",
  },
  person: {
    bg: "bg-blue-100 dark:bg-blue-900/30",
    text: "text-blue-700 dark:text-blue-300",
    border: "border-blue-300 dark:border-blue-700",
  },
  org: {
    bg: "bg-green-100 dark:bg-green-900/30",
    text: "text-green-700 dark:text-green-300",
    border: "border-green-300 dark:border-green-700",
  },
  organization: {
    bg: "bg-green-100 dark:bg-green-900/30",
    text: "text-green-700 dark:text-green-300",
    border: "border-green-300 dark:border-green-700",
  },
  loc: {
    bg: "bg-purple-100 dark:bg-purple-900/30",
    text: "text-purple-700 dark:text-purple-300",
    border: "border-purple-300 dark:border-purple-700",
  },
  location: {
    bg: "bg-purple-100 dark:bg-purple-900/30",
    text: "text-purple-700 dark:text-purple-300",
    border: "border-purple-300 dark:border-purple-700",
  },
  misc: {
    bg: "bg-gray-100 dark:bg-gray-800/30",
    text: "text-gray-700 dark:text-gray-300",
    border: "border-gray-300 dark:border-gray-600",
  },
};

// Dynamic colors for custom labels (when not in standard set)
const DYNAMIC_COLORS = [
  {
    bg: "bg-orange-100 dark:bg-orange-900/30",
    text: "text-orange-700 dark:text-orange-300",
    border: "border-orange-300 dark:border-orange-700",
  },
  {
    bg: "bg-pink-100 dark:bg-pink-900/30",
    text: "text-pink-700 dark:text-pink-300",
    border: "border-pink-300 dark:border-pink-700",
  },
  {
    bg: "bg-cyan-100 dark:bg-cyan-900/30",
    text: "text-cyan-700 dark:text-cyan-300",
    border: "border-cyan-300 dark:border-cyan-700",
  },
  {
    bg: "bg-yellow-100 dark:bg-yellow-900/30",
    text: "text-yellow-700 dark:text-yellow-300",
    border: "border-yellow-300 dark:border-yellow-700",
  },
  {
    bg: "bg-indigo-100 dark:bg-indigo-900/30",
    text: "text-indigo-700 dark:text-indigo-300",
    border: "border-indigo-300 dark:border-indigo-700",
  },
  {
    bg: "bg-rose-100 dark:bg-rose-900/30",
    text: "text-rose-700 dark:text-rose-300",
    border: "border-rose-300 dark:border-rose-700",
  },
  {
    bg: "bg-teal-100 dark:bg-teal-900/30",
    text: "text-teal-700 dark:text-teal-300",
    border: "border-teal-300 dark:border-teal-700",
  },
  {
    bg: "bg-amber-100 dark:bg-amber-900/30",
    text: "text-amber-700 dark:text-amber-300",
    border: "border-amber-300 dark:border-amber-700",
  },
];

const SAMPLE_TEXTS = {
  tech: {
    name: "Tech News",
    description: "Companies, people, and products",
    text: `Apple Inc. announced that Tim Cook will be visiting the new headquarters in Cupertino, California next Monday. The company plans to unveil several new products, including the iPhone 16 and MacBook Pro. Meanwhile, Google's CEO Sundar Pichai confirmed that the search giant is expanding its AI research team in London. Microsoft and Amazon are also investing heavily in artificial intelligence, with Jeff Bezos recently stating that AWS will double its machine learning capabilities by 2025.`,
    labels: ["person", "organization", "location", "product", "date"],
  },
  scientific: {
    name: "Scientific Text",
    description: "Research entities and methods",
    text: `The CRISPR-Cas9 gene editing system, developed by Jennifer Doudna and Emmanuelle Charpentier at UC Berkeley and the Max Planck Institute, has revolutionized molecular biology. Their 2020 Nobel Prize in Chemistry recognized the potential of this technology for treating genetic diseases like sickle cell anemia and cystic fibrosis. Recent clinical trials at Massachusetts General Hospital have shown promising results using CRISPR to target the BCL11A gene in patients with beta-thalassemia.`,
    labels: ["person", "organization", "technology", "disease", "gene"],
  },
  product: {
    name: "Product Review",
    description: "Brands, features, and specifications",
    text: `The Sony WH-1000XM5 wireless headphones deliver exceptional noise cancellation powered by the V1 processor. Priced at $399, they compete directly with the Bose QuietComfort Ultra and Apple AirPods Max. The 30-hour battery life and multipoint Bluetooth 5.2 connectivity make them ideal for commuters. Sony's LDAC codec support enables high-resolution audio streaming up to 990 kbps, surpassing the standard SBC and AAC codecs used by most competitors.`,
    labels: ["product", "brand", "feature", "specification", "price"],
  },
};

const EXTRACT_SAMPLE_TEXT = `John Smith is a 35-year-old software engineer who works at Google in Mountain View. He graduated from Stanford University in 2012 with a degree in Computer Science. His colleague Jane Doe, age 29, is a product manager at the same company. She previously worked at Meta for three years.`;

const EXTRACT_SAMPLES = {
  people: {
    name: "People & Companies",
    description: "Extract personal and professional info",
    text: EXTRACT_SAMPLE_TEXT,
    schema: [
      {
        name: "person",
        fields: [
          { name: "name", type: "str" as const },
          { name: "age", type: "str" as const },
          { name: "company", type: "str" as const },
          { name: "role", type: "str" as const },
        ],
      },
    ],
  },
};

const RecognizePlaygroundPage: React.FC = () => {
  const { termiteApiUrl } = useApiConfig();
  const [searchParams, setSearchParams] = useSearchParams();

  // Restore state from localStorage
  const [mode, setMode] = useState<PlaygroundMode>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved).mode;
        if (parsed === "recognize" || parsed === "extract") return parsed;
      }
    } catch {
      /* ignore */
    }
    return "recognize";
  });
  const [inputText, setInputText] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).inputText || "";
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

  // Recognize mode state
  const [labels, setLabels] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved).labels;
        if (Array.isArray(parsed) && parsed.length > 0) return parsed;
      }
    } catch {
      /* ignore */
    }
    return DEFAULT_LABELS;
  });
  const [newLabel, setNewLabel] = useState("");
  const [confidenceThreshold, setConfidenceThreshold] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const val = JSON.parse(saved).confidenceThreshold;
        if (typeof val === "number") return val;
      }
    } catch {
      /* ignore */
    }
    return 0.5;
  });
  const [recognizeResult, setRecognizeResult] = useState<NERResponse | null>(null);

  // Extract mode state (restored from localStorage)
  const [schema, setSchema] = useState<SchemaStructure[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved).schema;
        if (Array.isArray(parsed) && parsed.length > 0) return parsed;
      }
    } catch {
      /* ignore */
    }
    return DEFAULT_SCHEMA;
  });
  const [extractThreshold, setExtractThreshold] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const val = JSON.parse(saved).extractThreshold;
        if (typeof val === "number") return val;
      }
    } catch {
      /* ignore */
    }
    return 0.3;
  });
  const [includeConfidence, setIncludeConfidence] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).includeConfidence === true;
    } catch {
      /* ignore */
    }
    return false;
  });
  const [includeSpans, setIncludeSpans] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) return JSON.parse(saved).includeSpans === true;
    } catch {
      /* ignore */
    }
    return false;
  });
  const [extractResult, setExtractResult] = useState<ExtractResponse | null>(null);

  // Shared state
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [recognizerModels, setRecognizerModels] = useState<string[]>([]);
  const [extractorModels, setExtractorModels] = useState<string[]>([]);
  const [modelsLoaded, setModelsLoaded] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);
  const labelColorMapRef = useRef<Map<string, number>>(new Map());

  // Persist state to localStorage
  useEffect(() => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        inputText,
        selectedModel,
        labels,
        confidenceThreshold,
        mode,
        schema,
        extractThreshold,
        includeConfidence,
        includeSpans,
      })
    );
  }, [
    inputText,
    selectedModel,
    labels,
    confidenceThreshold,
    mode,
    schema,
    extractThreshold,
    includeConfidence,
    includeSpans,
  ]);

  const availableModels = mode === "recognize" ? recognizerModels : extractorModels;

  // Fetch available models on mount
  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        const response = await fetch(`${termiteApiUrl}/ml/v1/models`, {
          signal: controller.signal,
        });
        if (response.ok) {
          const data: ModelsResponse = await response.json();
          const recognizers = Object.keys(data.recognizers || {});
          setRecognizerModels(recognizers);
          // Extractors are recognizers with "extraction" capability
          const extractors = Object.entries(data.recognizers || {})
            .filter(([, info]) => info.capabilities?.includes("extraction"))
            .map(([name]) => name);
          setExtractorModels(extractors);
          setSelectedModel((prev: string) =>
            prev && recognizers.includes(prev) ? prev : recognizers[0] || ""
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
  }, [termiteApiUrl]); // eslint-disable-line react-hooks/exhaustive-deps

  // Update selected model when mode changes
  useEffect(() => {
    const models = mode === "recognize" ? recognizerModels : extractorModels;
    setSelectedModel((prev: string) => (prev && models.includes(prev) ? prev : models[0] || ""));
  }, [mode, recognizerModels, extractorModels]);

  // Handle ?model= URL param from Model Registry "Open in Playground"
  useEffect(() => {
    const modelParam = searchParams.get("model");
    if (modelParam && modelsLoaded && recognizerModels.includes(modelParam)) {
      setSelectedModel(modelParam);
      setSearchParams(
        (prev) => {
          prev.delete("model");
          return prev;
        },
        { replace: true }
      );
    }
  }, [searchParams, modelsLoaded, recognizerModels, setSearchParams]);

  const getColorForLabel = (label: string) => {
    const normalizedLabel = label.toLowerCase();

    // Check standard colors first
    if (STANDARD_LABEL_COLORS[normalizedLabel]) {
      return STANDARD_LABEL_COLORS[normalizedLabel];
    }

    // Use dynamic color assignment for custom labels
    if (!labelColorMapRef.current.has(normalizedLabel)) {
      const nextIndex = labelColorMapRef.current.size % DYNAMIC_COLORS.length;
      labelColorMapRef.current.set(normalizedLabel, nextIndex);
    }

    const colorIndex = labelColorMapRef.current.get(normalizedLabel);
    return DYNAMIC_COLORS[colorIndex ?? 0];
  };

  const handleRecognize = useCallback(async () => {
    if (!inputText.trim()) {
      setError("Please enter some text to analyze");
      return;
    }

    if (!selectedModel) {
      setError("Please select a model");
      return;
    }

    if (mode === "recognize" && labels.length === 0) {
      setError("Please add at least one entity label");
      return;
    }

    if (mode === "extract" && schema.length === 0) {
      setError("Please add at least one structure to the schema");
      return;
    }

    // Cancel any previous request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    abortControllerRef.current = new AbortController();
    setIsLoading(true);
    setError(null);
    setRecognizeResult(null);
    setExtractResult(null);
    labelColorMapRef.current.clear();

    const startTime = performance.now();

    try {
      if (mode === "recognize") {
        const response = await fetchWithRetry(`${termiteApiUrl}/ml/v1/recognize`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model: selectedModel,
            texts: [inputText],
            labels: labels,
          }),
          signal: abortControllerRef.current.signal,
        });

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(errorText || `HTTP ${response.status}`);
        }

        const data: NERResponse = await response.json();
        setRecognizeResult(data);
      } else {
        // Build schema for extract API
        const apiSchema: Record<string, string[]> = {};
        for (const structure of schema) {
          apiSchema[structure.name] = structure.fields.map((f) => `${f.name}::${f.type}`);
        }

        const response = await fetchWithRetry(`${termiteApiUrl}/ml/v1/extract`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model: selectedModel,
            texts: [inputText],
            schema: apiSchema,
            threshold: extractThreshold,
            include_confidence: includeConfidence,
            include_spans: includeSpans,
          }),
          signal: abortControllerRef.current.signal,
        });

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(errorText || `HTTP ${response.status}`);
        }

        const data: ExtractResponse = await response.json();
        setExtractResult(data);
      }
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
  }, [
    inputText,
    selectedModel,
    labels,
    termiteApiUrl,
    mode,
    schema,
    extractThreshold,
    includeConfidence,
    includeSpans,
  ]);

  // Cmd+Enter shortcut
  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        handleRecognize();
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, [handleRecognize]);

  const handleReset = () => {
    setInputText("");
    setLabels(DEFAULT_LABELS);
    setNewLabel("");
    setConfidenceThreshold(0.5);
    setRecognizeResult(null);
    setExtractResult(null);
    setSchema(DEFAULT_SCHEMA);
    setExtractThreshold(0.3);
    setIncludeConfidence(false);
    setIncludeSpans(false);
    setError(null);
    setProcessingTime(null);
    labelColorMapRef.current.clear();
    localStorage.removeItem(STORAGE_KEY);
  };

  const handleAddLabel = () => {
    const trimmedLabel = newLabel.trim().toLowerCase();
    if (trimmedLabel && !labels.includes(trimmedLabel)) {
      setLabels([...labels, trimmedLabel]);
      setNewLabel("");
    }
  };

  const handleRemoveLabel = (labelToRemove: string) => {
    setLabels(labels.filter((l) => l !== labelToRemove));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleAddLabel();
    }
  };

  // Schema manipulation helpers
  const addStructure = () => {
    setSchema([...schema, { name: "", fields: [{ name: "", type: "str" }] }]);
  };

  const removeStructure = (index: number) => {
    setSchema(schema.filter((_, i) => i !== index));
  };

  const updateStructureName = (index: number, name: string) => {
    const updated = [...schema];
    updated[index] = { ...updated[index], name };
    setSchema(updated);
  };

  const addField = (structIndex: number) => {
    const updated = [...schema];
    updated[structIndex] = {
      ...updated[structIndex],
      fields: [...updated[structIndex].fields, { name: "", type: "str" }],
    };
    setSchema(updated);
  };

  const removeField = (structIndex: number, fieldIndex: number) => {
    const updated = [...schema];
    updated[structIndex] = {
      ...updated[structIndex],
      fields: updated[structIndex].fields.filter((_, i) => i !== fieldIndex),
    };
    setSchema(updated);
  };

  const updateField = (structIndex: number, fieldIndex: number, updates: Partial<SchemaField>) => {
    const updated = [...schema];
    updated[structIndex] = {
      ...updated[structIndex],
      fields: updated[structIndex].fields.map((f, i) =>
        i === fieldIndex ? { ...f, ...updates } : f
      ),
    };
    setSchema(updated);
  };

  // Get filtered entities based on confidence threshold
  const getFilteredEntities = (): NEREntity[] => {
    if (!recognizeResult || !recognizeResult.entities || recognizeResult.entities.length === 0) {
      return [];
    }
    return recognizeResult.entities[0].filter((entity) => entity.score >= confidenceThreshold);
  };

  // Get entity count per label
  const getEntityCountByLabel = (): Record<string, number> => {
    const entities = getFilteredEntities();
    const counts: Record<string, number> = {};
    for (const entity of entities) {
      counts[entity.label] = (counts[entity.label] || 0) + 1;
    }
    return counts;
  };

  // Render text with entity highlighting
  const renderHighlightedText = () => {
    const entities = getFilteredEntities();

    if (entities.length === 0) {
      if (recognizeResult) {
        return (
          <div className="text-sm text-muted-foreground">
            No entities found{" "}
            {confidenceThreshold > 0
              ? `above ${(confidenceThreshold * 100).toFixed(0)}% confidence`
              : ""}
          </div>
        );
      }
      return (
        <pre className="whitespace-pre-wrap text-sm text-muted-foreground font-mono">
          {inputText || 'Enter text and click "Run" to see results'}
        </pre>
      );
    }

    // Sort entities by start position
    const sortedEntities = [...entities].sort((a, b) => a.start - b.start);

    const elements: React.ReactNode[] = [];
    let lastEnd = 0;

    sortedEntities.forEach((entity, _index) => {
      // Add text before this entity
      if (entity.start > lastEnd) {
        elements.push(
          <span key={`text-${entity.start}-${entity.end}`}>
            {inputText.slice(lastEnd, entity.start)}
          </span>
        );
      }

      // Add the entity with highlighting
      const colors = getColorForLabel(entity.label);
      elements.push(
        <span
          key={`entity-${entity.label}-${entity.start}-${entity.end}`}
          className={`${colors.bg} ${colors.border} rounded px-1 py-0.5 border cursor-help`}
          title={`${entity.label}: ${(entity.score * 100).toFixed(1)}% confidence`}
        >
          {entity.text}
        </span>
      );

      lastEnd = entity.end;
    });

    // Add remaining text
    if (lastEnd < inputText.length) {
      elements.push(<span key="end">{inputText.slice(lastEnd)}</span>);
    }

    return <div className="whitespace-pre-wrap text-sm font-mono leading-relaxed">{elements}</div>;
  };

  // Get unique labels from results for legend
  const getUniqueLabels = (): string[] => {
    const entities = getFilteredEntities();
    return [...new Set(entities.map((e) => e.label))];
  };

  const samplePresets: SamplePreset[] =
    mode === "recognize"
      ? Object.values(SAMPLE_TEXTS).map((sample) => ({
          name: sample.name,
          description: sample.description,
          onLoad: () => {
            setInputText(sample.text);
            setLabels(sample.labels);
          },
        }))
      : Object.values(EXTRACT_SAMPLES).map((sample) => ({
          name: sample.name,
          description: sample.description,
          onLoad: () => {
            setInputText(sample.text);
            setSchema(sample.schema);
          },
        }));

  // Render extract results
  const renderExtractResults = () => {
    if (!extractResult || !extractResult.results || extractResult.results.length === 0) {
      return (
        <div className="h-100 flex items-center justify-center text-muted-foreground">
          <div className="text-center">
            <SlidersHorizontal className="h-12 w-12 mx-auto mb-3 opacity-20" />
            <p>Enter text and click "Run" to extract structured data</p>
          </div>
        </div>
      );
    }

    const textResult = extractResult.results[0];

    return (
      <div className="h-100 overflow-y-auto space-y-4">
        {Object.entries(textResult).map(([structName, instances]) => (
          <div key={structName}>
            <h4 className="text-sm font-semibold mb-2 capitalize">{structName}</h4>
            {(instances as Record<string, unknown>[]).length === 0 ? (
              <p className="text-sm text-muted-foreground">No instances found</p>
            ) : (
              <div className="space-y-2">
                {(instances as Record<string, unknown>[]).map((instance) => (
                  <div
                    key={`${structName}-${Object.keys(instance).join("-")}`}
                    className="p-3 bg-muted/50 rounded-lg border text-sm space-y-1"
                  >
                    {Object.entries(instance).map(([fieldName, fieldValue]) => {
                      // Handle both single ExtractFieldValue and arrays
                      const values = Array.isArray(fieldValue)
                        ? (fieldValue as ExtractFieldValue[])
                        : [fieldValue as ExtractFieldValue];

                      return (
                        <div key={fieldName} className="flex gap-2">
                          <span className="text-muted-foreground font-medium min-w-24">
                            {fieldName}:
                          </span>
                          <span className="font-mono">
                            {values.map((v, vi) => {
                              const valKey = `${fieldName}-${v.value}-${v.start ?? ""}-${v.end ?? ""}`;
                              return (
                                <span key={valKey}>
                                  {vi > 0 && ", "}
                                  {v.value}
                                  {includeConfidence && v.score != null && (
                                    <span className="text-muted-foreground ml-1">
                                      ({(v.score * 100).toFixed(0)}%)
                                    </span>
                                  )}
                                  {includeSpans && v.start != null && v.end != null && (
                                    <span className="text-muted-foreground ml-1">
                                      [{v.start}-{v.end}]
                                    </span>
                                  )}
                                </span>
                              );
                            })}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    );
  };

  const hasResult = mode === "recognize" ? recognizeResult !== null : extractResult !== null;
  const resultModel = mode === "recognize" ? recognizeResult?.model : extractResult?.model;

  return (
    <div className="h-full">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Recognize Playground</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Extract entities and structured data from text using GLiNER models
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
        <NoModelsGuide modelType="recognizer" typeName="NER recognizer" />
      )}

      {/* Configuration Panel */}
      <Card className="mb-6">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Configuration</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Mode Toggle */}
          <div className="flex gap-2">
            <Button
              variant={mode === "recognize" ? "default" : "outline"}
              size="sm"
              onClick={() => setMode("recognize")}
            >
              <Tag className="h-4 w-4 mr-1" />
              Recognize
            </Button>
            <Button
              variant={mode === "extract" ? "default" : "outline"}
              size="sm"
              onClick={() => setMode("extract")}
            >
              <SlidersHorizontal className="h-4 w-4 mr-1" />
              Extract
            </Button>
          </div>

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

            {/* Confidence Threshold (recognize mode) */}
            {mode === "recognize" && (
              <div className="space-y-2">
                <Label htmlFor="threshold">Confidence Threshold</Label>
                <div className="flex items-center gap-2">
                  <Input
                    id="threshold"
                    type="number"
                    min={0}
                    max={100}
                    step={5}
                    value={(confidenceThreshold * 100).toFixed(0)}
                    onChange={(e) => {
                      const val = Number.parseInt(e.target.value, 10);
                      if (!Number.isNaN(val) && val >= 0 && val <= 100) {
                        setConfidenceThreshold(val / 100);
                      }
                    }}
                    className="w-20"
                  />
                  <span className="text-sm text-muted-foreground">%</span>
                </div>
              </div>
            )}

            {/* Threshold (extract mode) */}
            {mode === "extract" && (
              <div className="space-y-2">
                <Label htmlFor="extract-threshold">Threshold</Label>
                <div className="flex items-center gap-2">
                  <Input
                    id="extract-threshold"
                    type="number"
                    min={0}
                    max={100}
                    step={5}
                    value={(extractThreshold * 100).toFixed(0)}
                    onChange={(e) => {
                      const val = Number.parseInt(e.target.value, 10);
                      if (!Number.isNaN(val) && val >= 0 && val <= 100) {
                        setExtractThreshold(val / 100);
                      }
                    }}
                    className="w-20"
                  />
                  <span className="text-sm text-muted-foreground">%</span>
                </div>
              </div>
            )}

            {/* Run Button */}
            <div className="space-y-2 flex items-end">
              <Button
                onClick={handleRecognize}
                disabled={isLoading || !inputText.trim() || !selectedModel}
                className="w-full"
              >
                {isLoading ? (
                  <>
                    <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                    Processing
                  </>
                ) : (
                  <>
                    {mode === "recognize" ? (
                      <Tag className="h-4 w-4 mr-2" />
                    ) : (
                      <SlidersHorizontal className="h-4 w-4 mr-2" />
                    )}
                    {mode === "recognize" ? "Extract Entities" : "Extract Data"}
                  </>
                )}
              </Button>
            </div>
          </div>

          {/* Extract mode options */}
          {mode === "extract" && (
            <div className="flex gap-6">
              <div className="flex items-center gap-2">
                <Switch
                  id="include-confidence"
                  checked={includeConfidence}
                  onCheckedChange={setIncludeConfidence}
                />
                <Label htmlFor="include-confidence" className="text-sm">
                  Include confidence
                </Label>
              </div>
              <div className="flex items-center gap-2">
                <Switch
                  id="include-spans"
                  checked={includeSpans}
                  onCheckedChange={setIncludeSpans}
                />
                <Label htmlFor="include-spans" className="text-sm">
                  Include spans
                </Label>
              </div>
            </div>
          )}

          {/* Entity Labels (recognize mode) */}
          {mode === "recognize" && (
            <div className="space-y-2">
              <Label>Entity Labels (GLiNER)</Label>
              <div className="flex flex-wrap gap-2 mb-2">
                {labels.map((label) => {
                  const colors = getColorForLabel(label);
                  return (
                    <Badge
                      key={label}
                      variant="secondary"
                      className={`${colors.bg} ${colors.text} ${colors.border} border gap-1`}
                    >
                      {label}
                      <button
                        type="button"
                        onClick={() => handleRemoveLabel(label)}
                        className="ml-1 hover:opacity-70"
                        aria-label={`Remove ${label}`}
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  );
                })}
              </div>
              <div className="flex gap-2">
                <Input
                  placeholder="Add custom label..."
                  value={newLabel}
                  onChange={(e) => setNewLabel(e.target.value)}
                  onKeyDown={handleKeyDown}
                  className="max-w-xs"
                />
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleAddLabel}
                  disabled={!newLabel.trim()}
                >
                  <Plus className="h-4 w-4" />
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                GLiNER models extract entities matching these labels. Press Enter or click + to add.
              </p>
            </div>
          )}

          {/* Schema Builder (extract mode) */}
          {mode === "extract" && (
            <div className="space-y-3">
              <Label>Extraction Schema</Label>
              {schema.map((structure, si) => (
                <div
                  key={`structure-${structure.name || `unnamed-${structure.fields.length}`}`}
                  className="p-3 bg-muted/30 rounded-lg border space-y-2"
                >
                  <div className="flex items-center gap-2">
                    <Input
                      placeholder="Structure name (e.g. person)"
                      value={structure.name}
                      onChange={(e) => updateStructureName(si, e.target.value)}
                      className="max-w-xs font-medium"
                    />
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removeStructure(si)}
                      className="text-muted-foreground hover:text-destructive"
                    >
                      <X className="h-4 w-4" />
                    </Button>
                  </div>
                  <div className="pl-4 space-y-1">
                    {structure.fields.map((field, fi) => (
                      <div
                        key={`${structure.name}-field-${field.name || "unnamed"}-${field.type}`}
                        className="flex items-center gap-2"
                      >
                        <Input
                          placeholder="Field name"
                          value={field.name}
                          onChange={(e) => updateField(si, fi, { name: e.target.value })}
                          className="max-w-48 text-sm"
                        />
                        <Select
                          value={field.type}
                          onValueChange={(val) =>
                            updateField(si, fi, { type: val as "str" | "list" })
                          }
                        >
                          <SelectTrigger className="w-20 text-sm">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="str">str</SelectItem>
                            <SelectItem value="list">list</SelectItem>
                          </SelectContent>
                        </Select>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => removeField(si, fi)}
                          disabled={structure.fields.length <= 1}
                          className="text-muted-foreground hover:text-destructive h-8 w-8 p-0"
                        >
                          <X className="h-3 w-3" />
                        </Button>
                      </div>
                    ))}
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => addField(si)}
                      className="text-xs"
                    >
                      <Plus className="h-3 w-3 mr-1" />
                      Add Field
                    </Button>
                  </div>
                </div>
              ))}
              <Button variant="outline" size="sm" onClick={addStructure}>
                <Plus className="h-4 w-4 mr-1" />
                Add Structure
              </Button>
              <p className="text-xs text-muted-foreground">
                Define structures with named fields. Each field can be "str" (single value) or
                "list" (multiple values).
              </p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <div className="mb-6 p-4 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive text-sm">
          {error}
        </div>
      )}

      {/* Results Stats Bar */}
      {hasResult && (
        <div className="mb-6 flex flex-wrap items-center gap-3">
          {mode === "recognize" && (
            <>
              <Badge variant="secondary" className="gap-1.5">
                <Hash className="h-3 w-3" />
                {getFilteredEntities().length} entities
              </Badge>
              <Badge variant="secondary" className="gap-1.5">
                <Percent className="h-3 w-3" />
                {(confidenceThreshold * 100).toFixed(0)}% threshold
              </Badge>
            </>
          )}
          {resultModel && (
            <Badge variant="secondary" className="gap-1.5">
              <Zap className="h-3 w-3" />
              {resultModel}
            </Badge>
          )}
          {processingTime && (
            <Badge variant="outline" className="gap-1.5">
              <Clock className="h-3 w-3" />
              {processingTime.toFixed(0)}ms
            </Badge>
          )}
        </div>
      )}

      {/* Main Content - Side by Side */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Input Text</CardTitle>
              {inputText && (
                <span className="text-xs text-muted-foreground">{inputText.length} characters</span>
              )}
            </div>
          </CardHeader>
          <CardContent className="flex-1">
            <Textarea
              placeholder={
                mode === "recognize"
                  ? "Paste or type your text here to extract named entities..."
                  : "Paste or type your text here to extract structured data..."
              }
              className="h-100 resize-y font-mono text-sm"
              value={inputText}
              onChange={(e) => setInputText(e.target.value)}
            />
          </CardContent>
        </Card>

        {/* Output Panel */}
        <Card className="flex flex-col">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">
              {hasResult
                ? mode === "recognize"
                  ? "Extracted Entities"
                  : "Extracted Data"
                : "Preview"}
            </CardTitle>
          </CardHeader>
          <CardContent className="flex-1 overflow-hidden">
            {isLoading ? (
              <div className="h-100 space-y-3">
                <div className="flex gap-2">
                  <Skeleton className="h-6 w-16" />
                  <Skeleton className="h-6 w-20" />
                  <Skeleton className="h-6 w-14" />
                </div>
                <Skeleton className="h-32 w-full" />
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-3/4" />
              </div>
            ) : mode === "recognize" ? (
              recognizeResult ? (
                <div className="h-100 overflow-y-auto space-y-4">
                  {/* Legend with entity counts */}
                  {getUniqueLabels().length > 0 && (
                    <div className="flex flex-wrap gap-2 pb-2">
                      {getUniqueLabels().map((label) => {
                        const colors = getColorForLabel(label);
                        const counts = getEntityCountByLabel();
                        return (
                          <Badge
                            key={label}
                            variant="outline"
                            className={`${colors.bg} ${colors.text} ${colors.border} text-xs`}
                          >
                            {label} ({counts[label]})
                          </Badge>
                        );
                      })}
                    </div>
                  )}

                  {/* Highlighted text view */}
                  <div className="p-3 bg-muted/50 rounded-lg border max-h-37.5 overflow-y-auto">
                    {renderHighlightedText()}
                  </div>

                  <Separator />

                  {/* Entity list */}
                  <div className="space-y-2">
                    {getFilteredEntities().length > 0 ? (
                      <div className="rounded-lg border overflow-hidden">
                        <table className="w-full text-sm">
                          <thead className="bg-muted/50">
                            <tr>
                              <th className="text-left px-3 py-2 font-medium">Entity</th>
                              <th className="text-left px-3 py-2 font-medium">Label</th>
                              <th className="text-right px-3 py-2 font-medium">Confidence</th>
                              <th className="text-right px-3 py-2 font-medium">Position</th>
                            </tr>
                          </thead>
                          <tbody>
                            {getFilteredEntities().map((entity, _index) => {
                              const colors = getColorForLabel(entity.label);
                              return (
                                <tr
                                  key={`${entity.text}-${entity.label}-${entity.start}`}
                                  className="border-t hover:bg-muted/30"
                                >
                                  <td className="px-3 py-2 font-mono">{entity.text}</td>
                                  <td className="px-3 py-2">
                                    <Badge
                                      variant="secondary"
                                      className={`${colors.bg} ${colors.text} ${colors.border} border text-xs`}
                                    >
                                      {entity.label}
                                    </Badge>
                                  </td>
                                  <td className="px-3 py-2 text-right tabular-nums">
                                    {(entity.score * 100).toFixed(1)}%
                                  </td>
                                  <td className="px-3 py-2 text-right text-muted-foreground tabular-nums">
                                    {entity.start}-{entity.end}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="text-center py-8 text-muted-foreground">
                        No entities found above the confidence threshold
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div className="h-100 flex items-center justify-center text-muted-foreground">
                  <div className="text-center">
                    <Tag className="h-12 w-12 mx-auto mb-3 opacity-20" />
                    <p className="mb-3">
                      Enter text and press{" "}
                      <kbd className="px-1.5 py-0.5 text-xs border rounded bg-muted">Cmd+Enter</kbd>{" "}
                      to extract entities
                    </p>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        setInputText(SAMPLE_TEXTS.tech.text);
                        setLabels(SAMPLE_TEXTS.tech.labels);
                      }}
                    >
                      Try a sample
                    </Button>
                  </div>
                </div>
              )
            ) : (
              renderExtractResults()
            )}
          </CardContent>
        </Card>
      </div>

      {/* Help text */}
      <div className="mt-6 text-xs text-muted-foreground space-y-1">
        {mode === "recognize" ? (
          <>
            <p>
              <strong>GLiNER Models:</strong> Zero-shot named entity recognition. Add custom labels
              to extract any entity types you need - no retraining required.
            </p>
            <p>
              <strong>Confidence Threshold:</strong> Adjust to filter out low-confidence
              predictions. Higher thresholds show fewer but more reliable entities.
            </p>
          </>
        ) : (
          <>
            <p>
              <strong>GLiNER2 Extraction:</strong> Extract structured data from text by defining a
              schema with structures and fields. The model maps text spans to your schema.
            </p>
            <p>
              <strong>Field Types:</strong> Use "str" for single-value fields and "list" for fields
              that can have multiple values.
            </p>
          </>
        )}
      </div>
    </div>
  );
};

export default RecognizePlaygroundPage;
