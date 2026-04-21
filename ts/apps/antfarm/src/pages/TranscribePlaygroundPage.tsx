import { TermiteClient } from "@antfly/termite-sdk";
import { ReloadIcon } from "@radix-ui/react-icons";
import {
  AudioLines,
  Check,
  Clock,
  Copy,
  Mic,
  RotateCcw,
  Sparkles,
  Square,
  Trash2,
  Upload,
  Zap,
} from "lucide-react";
import type React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { BackendInfoBar } from "@/components/playground/BackendInfoBar";
import { NoModelsGuide } from "@/components/playground/NoModelsGuide";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useApiConfig } from "@/hooks/use-api-config";
import {
  arrayBufferToBase64,
  downsampleBuffer,
  encodeWAV,
  formatDuration,
  formatFileSize,
  formatTime,
} from "@/lib/audio-utils";
import { fetchWithRetry } from "@/lib/utils";

interface UploadedAudio {
  name: string;
  base64: string;
  dataUri: string;
  mimeType: string;
  size: number;
}

interface VADChunk {
  data: string;
  start_time_ms?: number;
  end_time_ms?: number;
}

interface VADConfig {
  threshold: number;
  minSilenceDurationMs: number;
  minSpeechDurationMs: number;
  speechPadMs: number;
}

const DEFAULT_VAD_CONFIG: VADConfig = {
  threshold: 0.3,
  minSilenceDurationMs: 2000,
  minSpeechDurationMs: 500,
  speechPadMs: 200,
};

const LLM_CLEANUP_PROMPT =
  "You are a dictation cleanup assistant. Clean up the following transcribed speech. Remove filler words (um, uh, like, you know), fix grammar and punctuation, handle self-corrections (keep only the final version), and produce natural, well-formatted text. Output ONLY the cleaned text with no preamble or explanation.";

interface Segment {
  startMs: number;
  endMs: number;
  rawText: string | null;
  cleanedText: string | null;
  status: "pending" | "transcribing" | "cleaning" | "done";
  error?: string;
}

type InputMode = "file" | "mic";

function selectPlaceholder(loaded: boolean, count: number, typeName: string): string {
  if (!loaded) return "Loading...";
  if (count === 0) return `No ${typeName}`;
  return `Select ${typeName}`;
}

const TranscribePlaygroundPage: React.FC = () => {
  const { termiteApiUrl } = useApiConfig();

  const termiteClient = useMemo(
    () => new TermiteClient({ baseUrl: `${termiteApiUrl}/api` }),
    [termiteApiUrl]
  );

  // Models
  const [availableTranscribers, setAvailableTranscribers] = useState<string[]>([]);
  const [availableChunkers, setAvailableChunkers] = useState<string[]>([]);
  const [availableGenerators, setAvailableGenerators] = useState<string[]>([]);
  const [modelsLoaded, setModelsLoaded] = useState(false);

  // Config
  const [selectedTranscriberModel, setSelectedTranscriberModel] = useState("");
  const [selectedChunkerModel, setSelectedChunkerModel] = useState("");
  const [selectedGeneratorModel, setSelectedGeneratorModel] = useState("");
  const [language, setLanguage] = useState("");
  const [inputMode, setInputMode] = useState<InputMode>("mic");
  const [useVAD, setUseVAD] = useState(false);
  const [useLLMCleanup, setUseLLMCleanup] = useState(false);
  const [vadConfig, setVadConfig] = useState<VADConfig>(DEFAULT_VAD_CONFIG);

  // Audio state
  const [audioFile, setAudioFile] = useState<UploadedAudio | null>(null);
  const [audioDuration, setAudioDuration] = useState<number | null>(null);
  const [isDraggingAudio, setIsDraggingAudio] = useState(false);

  // Recording state
  const [isRecording, setIsRecording] = useState(false);
  const [recordingDuration, setRecordingDuration] = useState(0);
  const micStreamRef = useRef<MediaStream | null>(null);
  const audioCtxRef = useRef<AudioContext | null>(null);
  const processorRef = useRef<ScriptProcessorNode | null>(null);
  const sourceRef = useRef<MediaStreamAudioSourceNode | null>(null);
  const audioBuffersRef = useRef<Float32Array[]>([]);
  const recordingTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const blobUrlRef = useRef<string | null>(null);

  // Pipeline state
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingTime, setProcessingTime] = useState<number | null>(null);
  const [segments, setSegments] = useState<Segment[]>([]);
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  // Refs
  const audioInputRef = useRef<HTMLInputElement>(null);

  // Shared cleanup for recording resources
  const cleanupRecording = useCallback(() => {
    if (recordingTimerRef.current) {
      clearInterval(recordingTimerRef.current);
      recordingTimerRef.current = null;
    }
    if (processorRef.current) {
      processorRef.current.disconnect();
      processorRef.current = null;
    }
    if (sourceRef.current) {
      sourceRef.current.disconnect();
      sourceRef.current = null;
    }
    if (micStreamRef.current) {
      for (const t of micStreamRef.current.getTracks()) {
        t.stop();
      }
      micStreamRef.current = null;
    }
    if (audioCtxRef.current) {
      audioCtxRef.current.close();
      audioCtxRef.current = null;
    }
  }, []);

  // Revoke any existing blob URL
  const revokeBlobUrl = useCallback(() => {
    if (blobUrlRef.current) {
      URL.revokeObjectURL(blobUrlRef.current);
      blobUrlRef.current = null;
    }
  }, []);

  // Fetch models
  useEffect(() => {
    let cancelled = false;
    const fetchModels = async () => {
      try {
        const data = await termiteClient.listModels();
        if (cancelled) return;

        const transcribers = Object.keys(data.transcribers || {});
        setAvailableTranscribers(transcribers);
        if (transcribers.length > 0) setSelectedTranscriberModel(transcribers[0]);

        const chunkers = Object.keys(data.chunkers || {});
        setAvailableChunkers(chunkers);
        if (chunkers.length > 0) setSelectedChunkerModel(chunkers[0]);

        const generators = Object.keys(data.generators || {});
        setAvailableGenerators(generators);
        if (generators.length > 0) setSelectedGeneratorModel(generators[0]);
      } catch {
        console.error("Failed to fetch models");
      } finally {
        if (!cancelled) setModelsLoaded(true);
      }
    };
    fetchModels();
    return () => {
      cancelled = true;
    };
  }, [termiteClient]);

  // Audio file handling
  const processAudioFile = useCallback(
    (file: File) => {
      if (!file.type.startsWith("audio/")) {
        setError("Please select an audio file");
        return;
      }
      revokeBlobUrl();
      const reader = new FileReader();
      reader.onload = (e) => {
        const dataUri = e.target?.result as string;
        const base64 = dataUri.split(",")[1];
        setAudioFile({
          name: file.name,
          base64,
          dataUri,
          mimeType: file.type,
          size: file.size,
        });
        setAudioDuration(null);
      };
      reader.readAsDataURL(file);
    },
    [revokeBlobUrl]
  );

  const handleAudioDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDraggingAudio(false);
      if (e.dataTransfer.files.length > 0) {
        processAudioFile(e.dataTransfer.files[0]);
      }
    },
    [processAudioFile]
  );

  // Mic recording
  const startRecording = useCallback(async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true },
      });
      micStreamRef.current = stream;

      const ctx = new AudioContext();
      audioCtxRef.current = ctx;
      const src = ctx.createMediaStreamSource(stream);
      sourceRef.current = src;

      // ScriptProcessorNode is deprecated but used here for broad browser support.
      // Migrate to AudioWorkletProcessor if audio quality under load becomes an issue.
      const proc = ctx.createScriptProcessor(4096, 1, 1);
      processorRef.current = proc;
      audioBuffersRef.current = [];

      proc.onaudioprocess = (e) => {
        const data = e.inputBuffer.getChannelData(0);
        audioBuffersRef.current.push(new Float32Array(data));
      };

      src.connect(proc);
      proc.connect(ctx.destination);

      setIsRecording(true);
      setRecordingDuration(0);
      setAudioFile(null);
      setAudioDuration(null);
      setSegments([]);
      setError(null);

      const startTime = Date.now();
      recordingTimerRef.current = setInterval(() => {
        setRecordingDuration((Date.now() - startTime) / 1000);
      }, 100);
    } catch {
      setError("Microphone access denied");
    }
  }, []);

  const stopRecording = useCallback(async () => {
    // Capture sampleRate before cleanupRecording closes the AudioContext
    const sampleRate = audioCtxRef.current?.sampleRate ?? 48000;
    cleanupRecording();
    setIsRecording(false);

    const buffers = audioBuffersRef.current;
    audioBuffersRef.current = [];

    const totalLength = buffers.reduce((sum, b) => sum + b.length, 0);
    if (totalLength === 0) {
      setError("No audio captured");
      return;
    }

    const pcm = new Float32Array(totalLength);
    let offset = 0;
    for (const buf of buffers) {
      pcm.set(buf, offset);
      offset += buf.length;
    }

    // Downsample to 16kHz and encode WAV
    const samples16k = await downsampleBuffer(pcm, sampleRate, 16000);
    const wavBuffer = encodeWAV(samples16k, 16000);
    const base64 = arrayBufferToBase64(wavBuffer);

    // Create a blob URL for the audio player, revoking any previous one
    revokeBlobUrl();
    const blob = new Blob([wavBuffer], { type: "audio/wav" });
    const dataUri = URL.createObjectURL(blob);
    blobUrlRef.current = dataUri;

    setAudioFile({
      name: "recording.wav",
      base64,
      dataUri,
      mimeType: "audio/wav",
      size: wavBuffer.byteLength,
    });
    setAudioDuration(samples16k.length / 16000);
  }, [cleanupRecording, revokeBlobUrl]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      cleanupRecording();
      abortControllerRef.current?.abort();
      if (blobUrlRef.current) URL.revokeObjectURL(blobUrlRef.current);
    };
  }, [cleanupRecording]);

  // Update a single segment by index
  const updateSegment = useCallback((index: number, updates: Partial<Segment>) => {
    setSegments((prev) => prev.map((s, idx) => (idx === index ? { ...s, ...updates } : s)));
  }, []);

  // Process a single segment: transcribe, then optionally clean up with LLM
  const processSegment = useCallback(
    async (
      index: number,
      audioBase64: string,
      signal: AbortSignal,
      transcriberModel: string,
      lang: string,
      generatorModel: string | null
    ) => {
      // Transcribe
      updateSegment(index, { status: "transcribing" });

      if (signal.aborted) return;
      const langOpts: { model?: string; language?: string } = { model: transcriberModel };
      if (lang.trim()) langOpts.language = lang.trim();
      const result = await termiteClient.transcribe(audioBase64, langOpts);
      if (signal.aborted) return;

      const rawText = (result.text || "").trim();

      if (generatorModel && rawText) {
        updateSegment(index, { rawText, status: "cleaning" });

        try {
          const resp = await fetchWithRetry(`${termiteApiUrl}/ml/v1/generate`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              model: generatorModel,
              messages: [
                { role: "system", content: LLM_CLEANUP_PROMPT },
                { role: "user", content: rawText },
              ],
              temperature: 0.3,
              max_tokens: 1024,
            }),
            signal,
          });
          if (!resp.ok) {
            const text = await resp.text();
            throw new Error(text || `LLM cleanup failed: HTTP ${resp.status}`);
          }
          const genResult = await resp.json();
          const cleaned = (genResult.choices?.[0]?.message?.content || "").trim();
          updateSegment(index, { rawText, cleanedText: cleaned, status: "done" });
        } catch (e) {
          if (e instanceof Error && e.name === "AbortError") return;
          updateSegment(index, {
            rawText,
            status: "done",
            error: `LLM cleanup failed: ${e instanceof Error ? e.message : "unknown"}`,
          });
        }
      } else {
        updateSegment(index, { rawText, status: "done" });
      }
    },
    [termiteClient, termiteApiUrl, updateSegment]
  );

  // Main pipeline
  const handleTranscribe = useCallback(async () => {
    if (!audioFile) {
      setError("Please provide audio input");
      return;
    }
    if (!selectedTranscriberModel) {
      setError("Please select a transcriber model");
      return;
    }

    if (abortControllerRef.current) abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
    const { signal } = abortControllerRef.current;

    setIsLoading(true);
    setError(null);
    setSegments([]);
    setProcessingTime(null);

    const startTime = performance.now();
    const generatorModel = useLLMCleanup && selectedGeneratorModel ? selectedGeneratorModel : null;

    try {
      if (useVAD && selectedChunkerModel && audioDuration != null && audioDuration > 30) {
        // VAD pipeline: chunk -> transcribe each -> optionally clean up
        const resp = await fetchWithRetry(`${termiteApiUrl}/ml/v1/chunk`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            input: { type: "media", data: audioFile.base64, mime_type: audioFile.mimeType },
            config: {
              model: selectedChunkerModel,
              threshold: vadConfig.threshold,
              audio: {
                vad: {
                  min_silence_duration_ms: vadConfig.minSilenceDurationMs,
                  min_speech_duration_ms: vadConfig.minSpeechDurationMs,
                  speech_pad_ms: vadConfig.speechPadMs,
                },
              },
            },
          }),
          signal,
        });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(text || `VAD chunking failed: HTTP ${resp.status}`);
        }
        const chunkResult = await resp.json();
        const chunks: VADChunk[] = chunkResult.chunks || [];

        if (chunks.length === 0) {
          setError("No speech detected by VAD");
          return;
        }

        // Initialize segments
        setSegments(
          chunks.map((c) => ({
            startMs: c.start_time_ms ?? 0,
            endMs: c.end_time_ms ?? 0,
            rawText: null,
            cleanedText: null,
            status: "pending",
          }))
        );

        for (let i = 0; i < chunks.length; i++) {
          if (signal.aborted) return;
          await processSegment(
            i,
            chunks[i].data,
            signal,
            selectedTranscriberModel,
            language,
            generatorModel
          );
        }
      } else {
        // Direct pipeline: transcribe whole audio -> optionally clean up
        setSegments([
          {
            startMs: 0,
            endMs: (audioDuration ?? 0) * 1000,
            rawText: null,
            cleanedText: null,
            status: "pending",
          },
        ]);

        await processSegment(
          0,
          audioFile.base64,
          signal,
          selectedTranscriberModel,
          language,
          generatorModel
        );
      }

      setProcessingTime(performance.now() - startTime);
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") return;
      setError(
        err instanceof Error ? err.message : `Failed to connect to Termite at ${termiteApiUrl}`
      );
    } finally {
      setIsLoading(false);
    }
  }, [
    audioFile,
    audioDuration,
    selectedTranscriberModel,
    selectedChunkerModel,
    selectedGeneratorModel,
    language,
    useVAD,
    useLLMCleanup,
    vadConfig,
    termiteApiUrl,
    processSegment,
  ]);

  // Actions
  const handleReset = useCallback(() => {
    if (abortControllerRef.current) abortControllerRef.current.abort();
    if (isRecording) {
      cleanupRecording();
      setIsRecording(false);
    }
    revokeBlobUrl();
    setAudioFile(null);
    setAudioDuration(null);
    setLanguage("");
    setSegments([]);
    setError(null);
    setProcessingTime(null);
    setIsLoading(false);
    setCopiedId(null);
    setRecordingDuration(0);
    setVadConfig(DEFAULT_VAD_CONFIG);
  }, [isRecording, cleanupRecording, revokeBlobUrl]);

  const handleCopy = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const doneSegments = useMemo(() => segments.filter((s) => s.status === "done"), [segments]);
  const allText = useMemo(
    () => doneSegments.map((s) => s.cleanedText || s.rawText || "").join("\n"),
    [doneSegments]
  );

  return (
    <div className="h-full">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Transcribe Playground</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Record or upload audio, transcribe with Whisper, optionally chunk with VAD and clean up
            with an LLM
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={handleReset}>
            <RotateCcw className="h-4 w-4 mr-2" />
            Reset
          </Button>
        </div>
      </div>

      <BackendInfoBar />

      {modelsLoaded && availableTranscribers.length === 0 && (
        <NoModelsGuide modelType="transcriber" typeName="speech-to-text transcriber" soft />
      )}

      <div className="space-y-6">
        {/* Configuration */}
        <Card>
          <CardHeader className="pb-4">
            <CardTitle className="text-lg">Configuration</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Row 1: Core config */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="space-y-2">
                <Label htmlFor="transcriber-model">Transcriber Model</Label>
                <Select
                  value={selectedTranscriberModel}
                  onValueChange={setSelectedTranscriberModel}
                  disabled={!modelsLoaded || availableTranscribers.length === 0}
                >
                  <SelectTrigger id="transcriber-model">
                    <SelectValue
                      placeholder={selectPlaceholder(
                        modelsLoaded,
                        availableTranscribers.length,
                        "model"
                      )}
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {availableTranscribers.map((model) => (
                      <SelectItem key={model} value={model}>
                        {model}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label htmlFor="language">Language</Label>
                <Input
                  id="language"
                  placeholder="Auto-detect (or e.g. en, es)"
                  value={language}
                  onChange={(e) => setLanguage(e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <Label>Input Source</Label>
                <div className="flex rounded-md border overflow-hidden h-9">
                  <button
                    type="button"
                    className={`flex-1 flex items-center justify-center gap-1.5 text-sm transition-colors ${
                      inputMode === "mic" ? "bg-primary text-primary-foreground" : "hover:bg-muted"
                    }`}
                    onClick={() => setInputMode("mic")}
                  >
                    <Mic className="h-3.5 w-3.5" />
                    Mic
                  </button>
                  <button
                    type="button"
                    className={`flex-1 flex items-center justify-center gap-1.5 text-sm transition-colors border-l ${
                      inputMode === "file" ? "bg-primary text-primary-foreground" : "hover:bg-muted"
                    }`}
                    onClick={() => setInputMode("file")}
                  >
                    <Upload className="h-3.5 w-3.5" />
                    File
                  </button>
                </div>
              </div>

              <div className="space-y-2 flex items-end">
                <Button
                  onClick={handleTranscribe}
                  disabled={isLoading || !audioFile || !selectedTranscriberModel}
                  className="w-full"
                >
                  {isLoading ? (
                    <>
                      <ReloadIcon className="h-4 w-4 mr-2 animate-spin" />
                      Processing...
                    </>
                  ) : (
                    <>
                      <Zap className="h-4 w-4 mr-2" />
                      Transcribe
                    </>
                  )}
                </Button>
              </div>
            </div>

            {/* Row 2: Pipeline options */}
            <div className="space-y-3 pt-2 border-t">
              <div className="flex flex-wrap gap-x-8 gap-y-3">
                <div className="flex items-center gap-6">
                  <div className="flex items-center gap-2">
                    <Checkbox
                      id="use-vad"
                      checked={useVAD}
                      onCheckedChange={(v) => setUseVAD(v === true)}
                    />
                    <Label htmlFor="use-vad" className="text-sm font-normal cursor-pointer">
                      VAD chunking
                    </Label>
                  </div>
                  {useVAD && (
                    <Select
                      value={selectedChunkerModel}
                      onValueChange={setSelectedChunkerModel}
                      disabled={availableChunkers.length === 0}
                    >
                      <SelectTrigger className="w-56 h-8 text-xs">
                        <SelectValue
                          placeholder={selectPlaceholder(
                            true,
                            availableChunkers.length,
                            "VAD model"
                          )}
                        />
                      </SelectTrigger>
                      <SelectContent>
                        {availableChunkers.map((model) => (
                          <SelectItem key={model} value={model}>
                            {model}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  )}
                </div>

                <div className="flex items-center gap-6">
                  <div className="flex items-center gap-2">
                    <Checkbox
                      id="use-llm"
                      checked={useLLMCleanup}
                      onCheckedChange={(v) => setUseLLMCleanup(v === true)}
                    />
                    <Label htmlFor="use-llm" className="text-sm font-normal cursor-pointer">
                      LLM cleanup
                    </Label>
                  </div>
                  {useLLMCleanup && (
                    <Select
                      value={selectedGeneratorModel}
                      onValueChange={setSelectedGeneratorModel}
                      disabled={availableGenerators.length === 0}
                    >
                      <SelectTrigger className="w-56 h-8 text-xs">
                        <SelectValue
                          placeholder={selectPlaceholder(
                            true,
                            availableGenerators.length,
                            "generator"
                          )}
                        />
                      </SelectTrigger>
                      <SelectContent>
                        {availableGenerators.map((model) => (
                          <SelectItem key={model} value={model}>
                            {model}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  )}
                </div>
              </div>

              {/* VAD configuration options */}
              {useVAD && (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 pl-6">
                  <div className="space-y-1">
                    <Label
                      htmlFor="vad-threshold"
                      className="text-xs"
                      title="Speech probability threshold (0-1). Higher values require stronger speech signals."
                    >
                      Threshold
                    </Label>
                    <Input
                      id="vad-threshold"
                      type="number"
                      min={0}
                      max={1}
                      step={0.05}
                      className="h-8 text-xs"
                      value={vadConfig.threshold}
                      onChange={(e) => {
                        const val = parseFloat(e.target.value);
                        if (!Number.isNaN(val) && val >= 0 && val <= 1) {
                          setVadConfig({ ...vadConfig, threshold: val });
                        }
                      }}
                    />
                  </div>
                  <div className="space-y-1">
                    <Label
                      htmlFor="vad-min-silence"
                      className="text-xs"
                      title="Minimum silence duration (ms) to split segments. Longer values produce fewer, larger segments."
                    >
                      Min Silence (ms)
                    </Label>
                    <Input
                      id="vad-min-silence"
                      type="number"
                      min={100}
                      max={10000}
                      step={100}
                      className="h-8 text-xs"
                      value={vadConfig.minSilenceDurationMs}
                      onChange={(e) =>
                        setVadConfig({
                          ...vadConfig,
                          minSilenceDurationMs: parseInt(e.target.value, 10) || 2000,
                        })
                      }
                    />
                  </div>
                  <div className="space-y-1">
                    <Label
                      htmlFor="vad-min-speech"
                      className="text-xs"
                      title="Minimum speech duration (ms). Segments shorter than this are discarded."
                    >
                      Min Speech (ms)
                    </Label>
                    <Input
                      id="vad-min-speech"
                      type="number"
                      min={50}
                      max={5000}
                      step={50}
                      className="h-8 text-xs"
                      value={vadConfig.minSpeechDurationMs}
                      onChange={(e) =>
                        setVadConfig({
                          ...vadConfig,
                          minSpeechDurationMs: parseInt(e.target.value, 10) || 500,
                        })
                      }
                    />
                  </div>
                  <div className="space-y-1">
                    <Label
                      htmlFor="vad-speech-pad"
                      className="text-xs"
                      title="Padding (ms) added before and after each speech segment."
                    >
                      Speech Pad (ms)
                    </Label>
                    <Input
                      id="vad-speech-pad"
                      type="number"
                      min={0}
                      max={2000}
                      step={50}
                      className="h-8 text-xs"
                      value={vadConfig.speechPadMs}
                      onChange={(e) =>
                        setVadConfig({
                          ...vadConfig,
                          speechPadMs: parseInt(e.target.value, 10) || 200,
                        })
                      }
                    />
                  </div>
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Error */}
        {error && (
          <div className="p-4 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive text-sm">
            {error}
          </div>
        )}

        {/* Stats */}
        {doneSegments.length > 0 && (
          <div className="flex flex-wrap items-center gap-3">
            <Badge variant="secondary" className="gap-1.5">
              <Zap className="h-3 w-3" />
              {selectedTranscriberModel}
            </Badge>
            {segments.length > 1 && (
              <Badge variant="secondary" className="gap-1.5">
                <AudioLines className="h-3 w-3" />
                {segments.length} segments
              </Badge>
            )}
            {useLLMCleanup && selectedGeneratorModel && doneSegments.some((s) => s.cleanedText) && (
              <Badge variant="secondary" className="gap-1.5">
                <Sparkles className="h-3 w-3" />
                {selectedGeneratorModel}
              </Badge>
            )}
            {processingTime != null && (
              <Badge variant="outline" className="gap-1.5">
                <Clock className="h-3 w-3" />
                {processingTime.toFixed(0)}ms
              </Badge>
            )}
          </div>
        )}

        {/* Side-by-side panels */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Input */}
          <Card className="flex flex-col">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg">Input Audio</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 space-y-4">
              {inputMode === "file" ? (
                <>
                  {/* Drop zone */}
                  <div
                    className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors ${
                      isDraggingAudio
                        ? "border-primary bg-primary/5"
                        : "border-muted-foreground/25 hover:border-primary/50"
                    }`}
                    onClick={() => audioInputRef.current?.click()}
                    onDragOver={(e) => {
                      e.preventDefault();
                      setIsDraggingAudio(true);
                    }}
                    onDragLeave={() => setIsDraggingAudio(false)}
                    onDrop={handleAudioDrop}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") audioInputRef.current?.click();
                    }}
                    role="button"
                    tabIndex={0}
                  >
                    <Upload className="h-8 w-8 mx-auto mb-2 text-muted-foreground" />
                    <p className="text-sm text-muted-foreground">
                      Drop audio file here or click to browse
                    </p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Supports WAV, MP3, FLAC, OGG, and more
                    </p>
                  </div>
                  <input
                    ref={audioInputRef}
                    type="file"
                    accept="audio/*"
                    className="hidden"
                    onChange={(e) => {
                      if (e.target.files?.[0]) processAudioFile(e.target.files[0]);
                      e.target.value = "";
                    }}
                  />
                </>
              ) : (
                /* Mic recording */
                <div className="border-2 border-dashed rounded-lg p-8 text-center transition-colors border-muted-foreground/25">
                  {isRecording ? (
                    <div className="space-y-4">
                      <div className="flex items-center justify-center gap-2">
                        <span className="relative flex h-3 w-3">
                          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75" />
                          <span className="relative inline-flex rounded-full h-3 w-3 bg-red-500" />
                        </span>
                        <span className="text-sm font-medium text-red-600">Recording</span>
                        <span className="text-sm font-mono text-muted-foreground">
                          {formatDuration(recordingDuration)}
                        </span>
                      </div>
                      <Button variant="destructive" onClick={stopRecording}>
                        <Square className="h-4 w-4 mr-2" />
                        Stop Recording
                      </Button>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      <Mic className="h-8 w-8 mx-auto text-muted-foreground" />
                      <Button onClick={startRecording}>
                        <Mic className="h-4 w-4 mr-2" />
                        Start Recording
                      </Button>
                      <p className="text-xs text-muted-foreground">
                        Click to record from your microphone
                      </p>
                    </div>
                  )}
                </div>
              )}

              {/* Audio file info & player */}
              {audioFile && !isRecording && (
                <div className="p-4 bg-muted/30 rounded-lg border space-y-3">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2 min-w-0">
                      <Mic className="h-4 w-4 shrink-0 text-muted-foreground" />
                      <span className="text-sm font-medium truncate">{audioFile.name}</span>
                      <span className="text-xs text-muted-foreground">
                        ({formatFileSize(audioFile.size)})
                      </span>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 w-7 p-0 text-muted-foreground hover:text-destructive shrink-0"
                      onClick={() => {
                        revokeBlobUrl();
                        setAudioFile(null);
                        setAudioDuration(null);
                      }}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                  <audio
                    controls
                    src={audioFile.dataUri}
                    className="w-full"
                    onLoadedMetadata={(e) => {
                      const dur = (e.target as HTMLAudioElement).duration;
                      if (Number.isFinite(dur)) setAudioDuration(dur);
                    }}
                  >
                    <track kind="captions" />
                  </audio>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Output */}
          <Card className="flex flex-col">
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="text-lg">
                  {doneSegments.length > 0 ? "Transcription" : "Output"}
                </CardTitle>
                {allText && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={() => handleCopy(allText, "all")}
                  >
                    {copiedId === "all" ? (
                      <>
                        <Check className="h-3 w-3" />
                        Copied!
                      </>
                    ) : (
                      <>
                        <Copy className="h-3 w-3" />
                        Copy All
                      </>
                    )}
                  </Button>
                )}
              </div>
            </CardHeader>
            <CardContent className="flex-1 overflow-hidden">
              {segments.length > 0 ? (
                <div className="space-y-3 max-h-[500px] overflow-y-auto">
                  {segments.map((seg, i) => (
                    <div
                      key={`seg-${seg.startMs}-${seg.endMs}`}
                      className="p-3 bg-muted/30 rounded-lg border space-y-2"
                    >
                      {/* Timestamp + copy */}
                      <div className="flex items-center justify-between">
                        {segments.length > 1 && (
                          <span className="text-xs font-mono text-muted-foreground">
                            {formatTime(seg.startMs)} – {formatTime(seg.endMs)}
                          </span>
                        )}
                        {seg.status === "done" && (seg.cleanedText || seg.rawText) && (
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-6 gap-1 text-xs ml-auto"
                            onClick={() =>
                              handleCopy(seg.cleanedText || seg.rawText || "", `seg-${i}`)
                            }
                          >
                            {copiedId === `seg-${i}` ? (
                              <>
                                <Check className="h-3 w-3" />
                                Copied
                              </>
                            ) : (
                              <Copy className="h-3 w-3" />
                            )}
                          </Button>
                        )}
                      </div>

                      {/* Status / Text */}
                      {seg.status === "pending" && (
                        <p className="text-sm text-muted-foreground italic">Waiting...</p>
                      )}
                      {seg.status === "transcribing" && (
                        <p className="text-sm text-muted-foreground italic flex items-center gap-2">
                          <ReloadIcon className="h-3 w-3 animate-spin" />
                          Transcribing...
                        </p>
                      )}
                      {seg.status === "cleaning" && (
                        <div className="space-y-1">
                          <p className="text-sm">{seg.rawText}</p>
                          <p className="text-xs text-muted-foreground italic flex items-center gap-1">
                            <Sparkles className="h-3 w-3" />
                            Cleaning up with LLM...
                          </p>
                        </div>
                      )}
                      {seg.status === "done" && (
                        <div className="space-y-1">
                          <p className="text-sm whitespace-pre-wrap">
                            {seg.cleanedText || seg.rawText || "(no speech)"}
                          </p>
                          {seg.cleanedText && seg.rawText && (
                            <p className="text-xs text-muted-foreground italic">
                              Raw: {seg.rawText}
                            </p>
                          )}
                          {seg.error && <p className="text-xs text-destructive">{seg.error}</p>}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="h-80 flex items-center justify-center text-muted-foreground">
                  <div className="text-center">
                    <Mic className="h-12 w-12 mx-auto mb-3 opacity-20" />
                    <p>Record or upload audio and click &ldquo;Transcribe&rdquo;</p>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Help text */}
      <div className="mt-6 text-xs text-muted-foreground space-y-1">
        <p>
          Record from your microphone or upload an audio file, then transcribe with a Whisper model.
          Enable <strong>VAD chunking</strong> to split long recordings into speech segments using
          Silero VAD. Enable <strong>LLM cleanup</strong> to post-process transcripts — removing
          filler words, fixing grammar, and handling self-corrections.
        </p>
      </div>
    </div>
  );
};

export default TranscribePlaygroundPage;
