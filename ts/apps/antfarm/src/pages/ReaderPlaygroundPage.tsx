import {
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  DashboardToolbar,
  FormActions,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Switch,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@antfly/design-system";
import { Clock, Copy, FileText, Hash, RotateCcw, ScanLine, Upload, X, Zap } from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AIWorkflowContextBar,
  AIWorkflowPage,
  AIWorkflowSubmitButton,
} from "@/components/ai-workflow";
import { PlaygroundEmptyState } from "@/components/branded-empty-state";
import { BackendInfoBar } from "@/components/playground/BackendInfoBar";
import { NoModelsGuide } from "@/components/playground/NoModelsGuide";
import { useApiConfig } from "@/hooks/use-api-config";
import { APPLE_OCR_READER, useAppleOcr } from "@/hooks/use-apple-ocr";
import { useSelectedInferenceModels } from "@/hooks/use-connections";
import { useModelSelection } from "@/hooks/use-model-selection";
import { findIndexedResult, modelsAcceptingInput } from "@/lib/inference-playground-contracts";
import { beginAbortableRequest, fetchInferenceJson, finishAbortableRequest } from "@/lib/utils";

// --- Local type definitions ---

interface ReadResult {
  object: "read";
  index: number;
  text: string;
  fields?: Record<string, string>;
  regions?: TextRegion[];
}

interface TextRegion {
  text: string;
  bbox: number[];
  confidence?: number;
  label?: string;
}

interface ReadResponse {
  object: "list";
  model: string;
  data: ReadResult[];
  usage?: { completion_tokens: number };
}

interface UploadedImage {
  id: string;
  name: string;
  dataUri: string;
  width: number;
  height: number;
}

// --- Helpers ---

function generateSampleInvoiceImage(): Promise<UploadedImage> {
  return new Promise((resolve) => {
    const canvas = document.createElement("canvas");
    canvas.width = 600;
    canvas.height = 300;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
      resolve({
        id: crypto.randomUUID(),
        name: "sample-invoice.png",
        dataUri: "",
        width: 0,
        height: 0,
      });
      return;
    }

    // White background
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, 600, 300);

    // Header
    ctx.fillStyle = "#1a1a1a";
    ctx.font = "bold 28px sans-serif";
    ctx.fillText("INVOICE", 40, 50);

    // Invoice details
    ctx.font = "16px sans-serif";
    ctx.fillStyle = "#444444";
    ctx.fillText("Invoice #12345", 40, 90);
    ctx.fillText("Date: 2024-01-15", 40, 115);
    ctx.fillText("Bill To: Acme Corp", 40, 140);

    // Line items
    ctx.font = "14px monospace";
    ctx.fillStyle = "#333333";
    ctx.fillText("Item                   Qty    Price", 40, 180);
    ctx.fillText("─────────────────────────────────────", 40, 195);
    ctx.fillText("Widget A                 2   $25.00", 40, 215);
    ctx.fillText("Widget B                 1   $49.00", 40, 235);

    // Total
    ctx.font = "bold 18px sans-serif";
    ctx.fillStyle = "#1a1a1a";
    ctx.fillText("Total: $99.00", 40, 275);

    // Border
    ctx.strokeStyle = "#cccccc";
    ctx.lineWidth = 2;
    ctx.strokeRect(1, 1, 598, 298);

    const dataUri = canvas.toDataURL("image/png");
    resolve({
      id: crypto.randomUUID(),
      name: "sample-invoice.png",
      dataUri,
      width: 600,
      height: 300,
    });
  });
}

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text);
}

// --- Component ---

const ReaderPlaygroundPage: React.FC = () => {
  const { inferenceUrl, inferenceApiUrl, inferenceConnectionId } = useApiConfig();

  // Shared state
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  // Image OCR state
  const [images, setImages] = useState<UploadedImage[]>([]);
  const [prompt, setPrompt] = useState("");
  const [maxTokens, setMaxTokens] = useState<string>("");
  const [readResult, setReadResult] = useState<ReadResponse | null>(null);
  const [selectedImageIndex, setSelectedImageIndex] = useState(0);
  const [isDraggingImage, setIsDraggingImage] = useState(false);
  const [showBboxOverlay, setShowBboxOverlay] = useState(true);
  const [outputTab, setOutputTab] = useState("text");
  const [copiedId, setCopiedId] = useState<string | null>(null);

  // Refs
  const imageInputRef = useRef<HTMLInputElement>(null);
  const { models: connectedModels, loading: modelsLoading } = useSelectedInferenceModels("reader");
  const connectionModels = useMemo(
    () => modelsAcceptingInput(connectedModels, "image").map((model) => model.name),
    [connectedModels]
  );
  const appleOcr = useAppleOcr();
  const availableReaders = useMemo(
    () =>
      appleOcr.loading || modelsLoading
        ? []
        : appleOcr.available
          ? [...connectionModels, APPLE_OCR_READER]
          : connectionModels,
    [appleOcr.available, appleOcr.loading, connectionModels, modelsLoading]
  );
  const modelsLoaded = !modelsLoading && !appleOcr.loading;
  const [recognitionLevel, setRecognitionLevel] = useState<"accurate" | "fast">("accurate");
  const [recognitionLanguages, setRecognitionLanguages] = useState("en-US");
  const { model: selectedReaderModel, setModel: setSelectedReaderModel } = useModelSelection({
    models: availableReaders,
  });

  const isAppleOcr = selectedReaderModel === APPLE_OCR_READER;

  // biome-ignore lint/correctness/useExhaustiveDependencies: model/backend identity invalidates in-flight results
  useEffect(() => {
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
    setIsLoading(false);
    setReadResult(null);
    setError(null);
    setProcessingTime(null);
    setSelectedImageIndex(0);
    setOutputTab("text");
    return () => abortControllerRef.current?.abort();
  }, [selectedReaderModel, inferenceApiUrl, inferenceConnectionId]);

  // Auto-set prompt based on selected model (only when prompt is empty or a known auto-value)
  useEffect(() => {
    if (!selectedReaderModel) return;
    const autoValues = ["", "<s_cord-v2>", "<OCR>"];
    if (!autoValues.includes(prompt)) return;
    const lower = selectedReaderModel.toLowerCase();
    if (lower.includes("donut")) {
      setPrompt("<s_cord-v2>");
    } else if (lower.includes("florence")) {
      setPrompt("<OCR>");
    } else {
      setPrompt("");
    }
  }, [selectedReaderModel, prompt]);

  // --- Image handling ---

  const processImageFiles = useCallback(
    (files: FileList | File[]) => {
      const fileArray = Array.from(files).filter((f) => f.type.startsWith("image/"));
      if (
        isAppleOcr &&
        fileArray.some((file) => !["image/png", "image/jpeg"].includes(file.type))
      ) {
        setError(
          "Apple OCR supports PNG and JPEG images. Select a vision model for other formats."
        );
        return;
      }
      setError(null);
      for (const file of fileArray) {
        const reader = new FileReader();
        reader.onload = (e) => {
          const dataUri = e.target?.result as string;
          const img = new Image();
          img.onload = () => {
            setImages((prev) => [
              ...prev,
              {
                id: crypto.randomUUID(),
                name: file.name,
                dataUri,
                width: img.naturalWidth,
                height: img.naturalHeight,
              },
            ]);
          };
          img.src = dataUri;
        };
        reader.readAsDataURL(file);
      }
    },
    [isAppleOcr]
  );

  const handleImageDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDraggingImage(false);
      if (e.dataTransfer.files.length > 0) {
        processImageFiles(e.dataTransfer.files);
      }
    },
    [processImageFiles]
  );

  const removeImage = (id: string) => {
    const idx = images.findIndex((img) => img.id === id);
    const filtered = images.filter((img) => img.id !== id);
    setImages(filtered);
    if (selectedImageIndex >= filtered.length) {
      setSelectedImageIndex(Math.max(0, filtered.length - 1));
    } else if (idx < selectedImageIndex) {
      setSelectedImageIndex((prev) => Math.max(0, prev - 1));
    }
  };

  // --- API calls ---

  const handleReadImages = async () => {
    if (images.length === 0) {
      setError("Please upload at least one image");
      return;
    }
    if (!selectedReaderModel || !availableReaders.includes(selectedReaderModel)) {
      setError("Please select an available reader");
      return;
    }

    const controller = beginAbortableRequest(abortControllerRef);
    setIsLoading(true);
    setError(null);
    setReadResult(null);

    const startTime = performance.now();

    try {
      const body: Record<string, unknown> = {
        images: images.map((img) => ({ url: img.dataUri })),
      };
      if (isAppleOcr) {
        body.recognition_level = recognitionLevel;
        body.recognition_languages = recognitionLanguages
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean);
      } else {
        body.model = selectedReaderModel;
        if (prompt.trim()) body.prompt = prompt.trim();
        if (maxTokens && Number.parseInt(maxTokens, 10) > 0)
          body.max_tokens = Number.parseInt(maxTokens, 10);
      }

      const data = await fetchInferenceJson<ReadResponse>(
        inferenceUrl(isAppleOcr ? "_antfarm/readers/apple" : "read"),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
          signal: controller.signal,
        }
      );
      if (abortControllerRef.current !== controller || controller.signal.aborted) return;
      setReadResult(data);
      setProcessingTime(performance.now() - startTime);
      setSelectedImageIndex(0);
      setOutputTab("text");
    } catch (err) {
      if (
        abortControllerRef.current !== controller ||
        controller.signal.aborted ||
        (err instanceof Error && err.name === "AbortError")
      )
        return;
      setError(err instanceof Error ? err.message : "Failed to connect to inference");
    } finally {
      if (finishAbortableRequest(abortControllerRef, controller)) setIsLoading(false);
    }
  };

  // --- Actions ---

  const handleReset = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    abortControllerRef.current = null;
    setImages([]);
    setPrompt("");
    setMaxTokens("");
    setReadResult(null);
    setError(null);
    setProcessingTime(null);
    setSelectedImageIndex(0);
    setIsLoading(false);
    setCopiedId(null);
  };

  const loadSampleData = async () => {
    const sample = await generateSampleInvoiceImage();
    setImages([sample]);
    setSelectedImageIndex(0);
    setReadResult(null);
    setError(null);
  };

  const handleCopy = (text: string, id: string) => {
    copyToClipboard(text);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  // Current result for selected image
  const currentResult = readResult ? findIndexedResult(readResult.data, selectedImageIndex) : null;

  const hasFields = currentResult?.fields && Object.keys(currentResult.fields).length > 0;
  const hasRegions = currentResult?.regions && currentResult.regions.length > 0;

  // Determine available output tabs and reset to "text" if current tab no longer valid
  const outputTabs = ["text"];
  if (hasFields) outputTabs.push("fields");
  if (hasRegions) outputTabs.push("regions");
  outputTabs.push("json");

  useEffect(() => {
    const tabs = ["text"];
    if (hasFields) tabs.push("fields");
    if (hasRegions) tabs.push("regions");
    tabs.push("json");
    if (!tabs.includes(outputTab)) {
      setOutputTab("text");
    }
  }, [outputTab, hasFields, hasRegions]);

  return (
    <AIWorkflowPage className="h-full space-y-3">
      <DashboardPageHeader>
        <div>
          <DashboardPageTitle className="font-aeonik">Reader Playground</DashboardPageTitle>
          <DashboardPageDescription>
            Extract text, structured fields, and regions using vision models or native OCR
          </DashboardPageDescription>
        </div>
        <DashboardPageActions>
          <Button variant="outline" onClick={loadSampleData}>
            <FileText className="h-4 w-4 mr-2" />
            Load Sample
          </Button>
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </DashboardPageActions>
      </DashboardPageHeader>
      <AIWorkflowContextBar>
        <BackendInfoBar />
      </AIWorkflowContextBar>
      {modelsLoaded && availableReaders.length === 0 && (
        <NoModelsGuide modelType="reader" typeName="reader" />
      )}
      {/* Content */}
      <div className="space-y-6">
        {/* Config */}
        <Card>
          <CardHeader className="pb-4">
            <CardTitle className="text-lg">Configuration</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <div className="space-y-2">
                <Label htmlFor="reader-model">Reader</Label>
                <Select
                  value={selectedReaderModel}
                  onValueChange={setSelectedReaderModel}
                  disabled={!modelsLoaded || availableReaders.length === 0}
                >
                  <SelectTrigger id="reader-model">
                    <SelectValue
                      placeholder={
                        !modelsLoaded
                          ? "Loading models..."
                          : availableReaders.length === 0
                            ? "No reader models"
                            : "Select a model"
                      }
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {availableReaders.map((model) => (
                      <SelectItem key={model} value={model}>
                        {model === APPLE_OCR_READER ? "Apple Vision OCR" : model}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              {isAppleOcr ? (
                <>
                  <div className="space-y-2">
                    <Label htmlFor="ocr-recognition-level">Recognition</Label>
                    <Select
                      value={recognitionLevel}
                      onValueChange={(value) => setRecognitionLevel(value as "accurate" | "fast")}
                    >
                      <SelectTrigger id="ocr-recognition-level">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="accurate">Accurate</SelectItem>
                        <SelectItem value="fast">Fast</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="ocr-languages">Languages</Label>
                    <Input
                      id="ocr-languages"
                      value={recognitionLanguages}
                      onChange={(event) => setRecognitionLanguages(event.target.value)}
                      placeholder="en-US, fr-FR"
                    />
                  </div>
                  <p className="text-sm text-muted-foreground sm:col-span-3">
                    Extracts visible text from PNG and JPEG images on the connected Mac. For image
                    descriptions or questions, select a vision model such as Qwen3-VL. Enter
                    comma-separated language codes. The first request may take longer.
                  </p>
                </>
              ) : (
                <>
                  <div className="space-y-2">
                    <Label htmlFor="prompt">Prompt</Label>
                    <Input
                      id="prompt"
                      placeholder="Optional prompt..."
                      value={prompt}
                      onChange={(e) => setPrompt(e.target.value)}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="max-tokens">Max Tokens</Label>
                    <Input
                      id="max-tokens"
                      type="number"
                      placeholder="Default"
                      value={maxTokens}
                      onChange={(e) => setMaxTokens(e.target.value)}
                    />
                  </div>
                </>
              )}
            </div>
            <FormActions>
              <AIWorkflowSubmitButton
                onClick={handleReadImages}
                disabled={
                  isLoading ||
                  images.length === 0 ||
                  !availableReaders.includes(selectedReaderModel) ||
                  (isAppleOcr && !recognitionLanguages.trim())
                }
                running={isLoading}
                runningLabel="Reading…"
                onCancel={() => {
                  abortControllerRef.current?.abort();
                  abortControllerRef.current = null;
                  setIsLoading(false);
                }}
              >
                <ScanLine className="h-4 w-4 mr-2" />
                Read Images
              </AIWorkflowSubmitButton>
            </FormActions>
          </CardContent>
        </Card>
        {/* Error */}
        {error && (
          <div className="p-4 bg-destructive/10 border border-destructive/30 rounded-none text-destructive text-sm">
            {error}
          </div>
        )}
        {/* Stats */}
        {readResult && (
          <DashboardToolbar className="flex-row items-center gap-3 md:items-center">
            <Badge className="gap-1.5">
              <Hash className="h-3 w-3" />
              {images.length} image{images.length !== 1 ? "s" : ""}
            </Badge>
            <Badge className="gap-1.5">
              <Zap className="h-3 w-3" />
              {readResult.model}
            </Badge>
            {readResult.usage != null && (
              <Badge className="gap-1.5">
                <Hash className="h-3 w-3" />
                {readResult.usage.completion_tokens} output tokens
              </Badge>
            )}
            {processingTime != null && (
              <Badge className="gap-1.5">
                <Clock className="h-3 w-3" />
                {processingTime.toFixed(0)}ms
              </Badge>
            )}
          </DashboardToolbar>
        )}
        {/* Side-by-side panels */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Input: Upload & Previews */}
          <Card className="flex flex-col">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg">Input Images</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 space-y-4">
              {/* Drop zone */}
              <div
                className={`border-2 border-dashed rounded-none p-8 text-center cursor-pointer transition-colors ${
                  isDraggingImage
                    ? "border-primary bg-primary/5"
                    : "border-muted-foreground/25 hover:border-primary/50"
                }`}
                onClick={() => imageInputRef.current?.click()}
                onDragOver={(e) => {
                  e.preventDefault();
                  setIsDraggingImage(true);
                }}
                onDragLeave={() => setIsDraggingImage(false)}
                onDrop={handleImageDrop}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") imageInputRef.current?.click();
                }}
                role="button"
                tabIndex={0}
              >
                <Upload className="h-8 w-8 mx-auto mb-2 text-muted-foreground" />
                <p className="text-sm text-muted-foreground">Drop images here or click to browse</p>
                <p className="text-xs text-muted-foreground mt-1">
                  {isAppleOcr ? "Supports PNG and JPEG" : "Supports PNG, JPG, WebP, and more"}
                </p>
              </div>
              <input
                ref={imageInputRef}
                type="file"
                accept={isAppleOcr ? "image/png,image/jpeg" : "image/*"}
                multiple
                className="hidden"
                onChange={(e) => {
                  if (e.target.files) processImageFiles(e.target.files);
                  e.target.value = "";
                }}
              />

              {/* Thumbnails */}
              {images.length > 0 && (
                <div className="grid grid-cols-3 sm:grid-cols-4 gap-2">
                  {images.map((img, idx) => (
                    <div
                      key={img.id}
                      className={`relative group cursor-pointer rounded-none overflow-hidden border-2 transition-all ${
                        selectedImageIndex === idx
                          ? "border-primary ring-2 ring-primary/30"
                          : "border-transparent hover:border-muted-foreground/30"
                      }`}
                      onClick={() => setSelectedImageIndex(idx)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") setSelectedImageIndex(idx);
                      }}
                      role="button"
                      tabIndex={0}
                    >
                      <img src={img.dataUri} alt={img.name} className="w-full h-16 object-cover" />
                      <button
                        type="button"
                        className="absolute top-0.5 right-0.5 p-0.5 rounded-full bg-black/60 text-white opacity-0 group-hover:opacity-100 transition-opacity"
                        onClick={(e) => {
                          e.stopPropagation();
                          removeImage(img.id);
                        }}
                      >
                        <X className="h-3 w-3" />
                      </button>
                      <p className="text-[10px] text-muted-foreground truncate px-1 py-0.5">
                        {img.name}
                      </p>
                    </div>
                  ))}
                </div>
              )}

              {/* Selected image preview */}
              {images.length > 0 && images[selectedImageIndex] && (
                <div className="rounded-none overflow-hidden border bg-muted/20">
                  <img
                    src={images[selectedImageIndex].dataUri}
                    alt={images[selectedImageIndex].name}
                    className="w-full max-h-80 object-contain"
                  />
                </div>
              )}
            </CardContent>
          </Card>

          {/* Output */}
          <Card className="flex flex-col">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg">{readResult ? "Results" : "Output"}</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 overflow-hidden">
              {readResult && currentResult ? (
                <div className="space-y-4">
                  {/* Image selector if multiple results */}
                  {readResult.data.length > 1 && (
                    <div className="flex gap-1 flex-wrap">
                      {[...readResult.data]
                        .sort((a, b) => a.index - b.index)
                        .map((item) => {
                          const image = images[item.index];
                          if (!image) return null;
                          return (
                            <Button
                              key={`image-${image.id}`}
                              variant={selectedImageIndex === item.index ? "default" : "outline"}
                              size="sm"
                              onClick={() => setSelectedImageIndex(item.index)}
                            >
                              Image {item.index + 1}
                            </Button>
                          );
                        })}
                    </div>
                  )}

                  {/* Output tabs */}
                  <Tabs value={outputTab} onValueChange={setOutputTab}>
                    <TabsList>
                      <TabsTrigger value="text">Text</TabsTrigger>
                      {hasFields && <TabsTrigger value="fields">Fields</TabsTrigger>}
                      {hasRegions && <TabsTrigger value="regions">Regions</TabsTrigger>}
                      <TabsTrigger value="json">JSON</TabsTrigger>
                    </TabsList>

                    <TabsContent value="text" className="mt-3">
                      <div className="relative">
                        <Button
                          variant="ghost"
                          size="sm"
                          className="absolute top-2 right-2 h-7 gap-1.5 text-xs"
                          onClick={() => handleCopy(currentResult.text, "ocr-text")}
                        >
                          <Copy className="h-3 w-3" />
                          {copiedId === "ocr-text" ? "Copied!" : "Copy"}
                        </Button>
                        <pre className="p-4 pr-20 bg-muted/30 rounded-none border text-sm font-mono whitespace-pre-wrap max-h-[500px] overflow-y-auto">
                          {currentResult.text || "(no text extracted)"}
                        </pre>
                      </div>
                    </TabsContent>

                    {hasFields && (
                      <TabsContent value="fields" className="mt-3">
                        <div className="border rounded-none overflow-hidden">
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="border-b bg-muted/30">
                                <th className="text-left p-2 font-medium">Field</th>
                                <th className="text-left p-2 font-medium">Value</th>
                              </tr>
                            </thead>
                            <tbody>
                              {Object.entries(currentResult.fields ?? {}).map(([key, value]) => (
                                <tr key={key} className="border-b last:border-0">
                                  <td className="p-2 font-mono text-xs text-muted-foreground">
                                    {key}
                                  </td>
                                  <td className="p-2 font-mono text-xs">{value}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </TabsContent>
                    )}

                    {hasRegions && (
                      <TabsContent value="regions" className="mt-3 space-y-3">
                        {/* Bbox overlay on image */}
                        <div className="flex items-center gap-2 mb-2">
                          <Switch
                            id="bbox-toggle"
                            checked={showBboxOverlay}
                            onCheckedChange={setShowBboxOverlay}
                          />
                          <Label htmlFor="bbox-toggle" className="text-sm">
                            Show bounding boxes
                          </Label>
                        </div>
                        {images[selectedImageIndex] && (
                          <div className="relative rounded-none overflow-hidden border bg-muted/20 inline-block max-w-full">
                            <img
                              src={images[selectedImageIndex].dataUri}
                              alt="regions overlay"
                              className="block max-w-full max-h-72"
                            />
                            {showBboxOverlay &&
                              currentResult.regions?.map((region, rIdx) => {
                                const img = images[selectedImageIndex];
                                if (!region.bbox || region.bbox.length < 4) return null;
                                const [x, y, w, h] = region.bbox;
                                const left = (x / img.width) * 100;
                                const top = (y / img.height) * 100;
                                const width = (w / img.width) * 100;
                                const height = (h / img.height) * 100;

                                // Cycle the categorical chart palette so each
                                // region stays distinguishable without raw hues.
                                const regionColor = `var(--chart-series-${(rIdx % 6) + 1})`;

                                return (
                                  <div
                                    key={`region-${region.bbox.join("-")}-${region.text.slice(0, 20)}`}
                                    className="absolute border-2 pointer-events-none"
                                    style={{
                                      borderColor: regionColor,
                                      left: `${left}%`,
                                      top: `${top}%`,
                                      width: `${width}%`,
                                      height: `${height}%`,
                                    }}
                                    title={region.text}
                                  />
                                );
                              })}
                          </div>
                        )}

                        {/* Regions table */}
                        <div className="border rounded-none overflow-hidden max-h-60 overflow-y-auto">
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="border-b bg-muted/30">
                                <th className="text-left p-2 font-medium">#</th>
                                <th className="text-left p-2 font-medium">Text</th>
                                {currentResult.regions?.some((r) => r.label) && (
                                  <th className="text-left p-2 font-medium">Label</th>
                                )}
                                {currentResult.regions?.some((r) => r.confidence != null) && (
                                  <th className="text-left p-2 font-medium">Confidence</th>
                                )}
                              </tr>
                            </thead>
                            <tbody>
                              {currentResult.regions?.map((region, rIdx) => (
                                <tr
                                  // biome-ignore lint/suspicious/noArrayIndexKey: regions may have duplicate text
                                  key={rIdx}
                                  className="border-b last:border-0"
                                >
                                  <td className="p-2 text-muted-foreground">{rIdx + 1}</td>
                                  <td className="p-2 font-mono text-xs">{region.text}</td>
                                  {currentResult.regions?.some((r) => r.label) && (
                                    <td className="p-2 text-xs">{region.label || "-"}</td>
                                  )}
                                  {currentResult.regions?.some((r) => r.confidence != null) && (
                                    <td className="p-2 font-mono text-xs">
                                      {region.confidence != null
                                        ? `${(region.confidence * 100).toFixed(1)}%`
                                        : "-"}
                                    </td>
                                  )}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </TabsContent>
                    )}

                    <TabsContent value="json" className="mt-3">
                      <div className="relative">
                        <Button
                          variant="ghost"
                          size="sm"
                          className="absolute top-2 right-2 h-7 gap-1.5 text-xs"
                          onClick={() =>
                            handleCopy(JSON.stringify(currentResult, null, 2), "ocr-json")
                          }
                        >
                          <Copy className="h-3 w-3" />
                          {copiedId === "ocr-json" ? "Copied!" : "Copy"}
                        </Button>
                        <pre className="p-4 pr-20 bg-muted/30 rounded-none border text-xs font-mono whitespace-pre-wrap max-h-[500px] overflow-y-auto">
                          {JSON.stringify(currentResult, null, 2)}
                        </pre>
                      </div>
                    </TabsContent>
                  </Tabs>
                </div>
              ) : (
                <div className="h-80 flex items-center justify-center">
                  <PlaygroundEmptyState />
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
      {/* Help text */}
      <div className="text-xs text-muted-foreground space-y-1">
        <p>
          {isAppleOcr
            ? "Accurate and Fast control text recognition. Photos without readable text may return no text or false detections."
            : "Use a prompt to guide supported vision models: extract text, describe the image, or ask a question about it."}
        </p>
      </div>
    </AIWorkflowPage>
  );
};

export default ReaderPlaygroundPage;
