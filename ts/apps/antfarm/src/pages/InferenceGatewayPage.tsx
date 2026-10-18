import {
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  DashboardPage,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  Input,
  Label,
  Textarea,
} from "@antfly/design-system";
import type { InferenceGatewayModel, InferenceGatewayOperation } from "@antfly/sdk";
import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useApiConfig } from "@/hooks/use-api-config";
import { inferenceGatewayRequest } from "@/lib/inference-gateway-request";

const samples: Record<InferenceGatewayOperation, Record<string, unknown>> = {
  "chat/completions": { messages: [{ role: "user", content: "Hello" }], stream: true },
  decisions: {
    input: "Please refund my order",
    questions: [
      { name: "refund", type: "predicate", instructions: "Does the customer request a refund?" },
    ],
  },
  embeddings: { input: ["Hello world"] },
  embed: { input: ["Hello world"] },
  rerank: { query: "Hello", documents: ["Hello world", "Goodbye"] },
  chunk: { input: "First paragraph.\n\nSecond paragraph." },
  read: { images: [{ url: "data:image/png;base64,..." }] },
  extract: { schema_version: 2, input: ["Ada works at Antfly"], tasks: [] },
  rewrite: { inputs: ["Please rewrite this sentence."] },
  transcribe: { audio: "<base64 audio>" },
  dictate: { audio: "<base64 audio>" },
  synthesize: { text: "Hello from Antfly", format: "mp3" },
  predict: { input: [[1, 2]] },
  generate: { messages: [{ role: "user", content: "Hello" }] },
  "generate/batch": {
    requests: [{ custom_id: "example", body: { messages: [{ role: "user", content: "Hello" }] } }],
  },
};
const taskOperation: Record<string, InferenceGatewayOperation> = {
  generate: "chat/completions",
  decide: "decisions",
  embed: "embeddings",
  rerank: "rerank",
  chunk: "chunk",
  read: "read",
  extract: "extract",
  rewrite: "rewrite",
  transcribe: "transcribe",
  synthesize: "synthesize",
  predict: "predict",
};

export default function InferenceGatewayPage() {
  const { client } = useApiConfig();
  const [params] = useSearchParams();
  const [models, setModels] = useState<InferenceGatewayModel[]>([]);
  const [model, setModel] = useState(params.get("model") ?? "");
  const [operation, setOperation] = useState<InferenceGatewayOperation>("chat/completions");
  const [request, setRequest] = useState(JSON.stringify(samples["chat/completions"], null, 2));
  const [output, setOutput] = useState("");
  const [error, setError] = useState("");
  const [running, setRunning] = useState(false);
  const [audio, setAudio] = useState<string | null>(null);
  const [requestId, setRequestId] = useState<string | null>(null);
  const controller = useRef<AbortController | null>(null);
  useEffect(() => {
    const abort = new AbortController();
    client.inference
      .models(abort.signal)
      .then((catalog) => {
        setModels(catalog.data);
        const initial = catalog.data.find((entry) => entry.id === params.get("model"));
        if (initial?.task && taskOperation[initial.task]) {
          const value = taskOperation[initial.task];
          setOperation(value);
          setRequest(JSON.stringify(samples[value], null, 2));
        }
      })
      .catch((err: unknown) => {
        if (!abort.signal.aborted) setError(err instanceof Error ? err.message : String(err));
      });
    return () => {
      abort.abort();
      controller.current?.abort();
    };
  }, [client, params]);
  useEffect(
    () => () => {
      if (audio) URL.revokeObjectURL(audio);
    },
    [audio]
  );
  const selected = models.find((entry) => entry.id === model);
  function selectOperation(value: InferenceGatewayOperation) {
    setOperation(value);
    setRequest(JSON.stringify(samples[value], null, 2));
  }
  function selectModel(value: string) {
    setModel(value);
    const task = models.find((entry) => entry.id === value)?.task;
    if (operation !== "generate/batch" && task && taskOperation[task])
      selectOperation(taskOperation[task]);
  }
  async function run() {
    controller.current?.abort();
    const abort = new AbortController();
    controller.current = abort;
    setRunning(true);
    setError("");
    setOutput("");
    setAudio(null);
    setRequestId(null);
    try {
      const body = inferenceGatewayRequest(operation, request, model);
      const response = await client.inference.invoke(operation, body, abort.signal);
      setRequestId(response.headers.get("x-request-id"));
      if (response.ok && response.headers.get("content-type")?.startsWith("audio/")) {
        const reader = response.body?.getReader();
        if (!reader) throw new Error("Audio response has no body");
        const chunks: Uint8Array<ArrayBuffer>[] = [];
        let size = 0;
        try {
          for (;;) {
            const next = await reader.read();
            if (next.done) break;
            size += next.value.byteLength;
            if (size > 64 << 20) throw new Error("Audio exceeded 64 MiB");
            chunks.push(new Uint8Array(next.value));
          }
        } finally {
          await reader.cancel();
          reader.releaseLock();
        }
        setAudio(
          URL.createObjectURL(
            new Blob(chunks, { type: response.headers.get("content-type") ?? "audio/mpeg" })
          )
        );
        return;
      }
      const reader = response.body?.getReader();
      if (!reader) throw new Error("Response has no body");
      const decoder = new TextDecoder();
      let text = "";
      let size = 0;
      try {
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          size += next.value.byteLength;
          if (size > 2 << 20)
            throw new Error("Response exceeded 2 MiB; use the SDK for larger results");
          text += decoder.decode(next.value, { stream: true });
          setOutput(text);
        }
        text += decoder.decode();
        if (!response.headers.get("content-type")?.includes("text/event-stream")) {
          try {
            text = JSON.stringify(JSON.parse(text), null, 2);
          } catch {
            /* Preserve provider text. */
          }
        }
        setOutput(text);
      } finally {
        await reader.cancel();
        reader.releaseLock();
      }
      if (!response.ok) throw new Error(`Request failed (${response.status})`);
    } catch (err) {
      if (!abort.signal.aborted) setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (controller.current === abort) setRunning(false);
    }
  }
  return (
    <DashboardPage>
      <DashboardPageHeader>
        <DashboardPageTitle>Inference gateway</DashboardPageTitle>
        <DashboardPageDescription>
          Run configured models and generation chains using their public IDs.
        </DashboardPageDescription>
      </DashboardPageHeader>
      <Card>
        <CardHeader>
          <CardTitle>Request</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="gateway-model">
              {operation === "generate/batch" ? "Default model (optional)" : "Model"}
            </Label>
            <Input
              id="gateway-model"
              list="gateway-models"
              value={model}
              onChange={(event) => selectModel(event.target.value)}
              placeholder="generators/support-chat"
            />
            <datalist id="gateway-models">
              {models.map((entry) => (
                <option key={entry.id} value={entry.id} />
              ))}
            </datalist>
          </div>
          {selected && (
            <p className="text-sm text-muted-foreground">
              Availability: {selected.availability ?? "unknown"} · Catalog:{" "}
              {selected.freshness ?? "unknown"}
              {selected.gateway_capabilities &&
                ` · ${Object.entries(selected.gateway_capabilities)
                  .filter(([, support]) => support === "supported")
                  .map(([name]) => name)
                  .join(", ")}`}
            </p>
          )}
          <div className="space-y-2">
            <Label htmlFor="gateway-operation">Operation</Label>
            <select
              id="gateway-operation"
              className="bg-background border rounded-md px-3 py-2 w-full"
              value={operation}
              onChange={(event) => selectOperation(event.target.value as InferenceGatewayOperation)}
            >
              {Object.keys(samples).map((value) => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </div>
          <Label htmlFor="gateway-body">JSON request</Label>
          <Textarea
            id="gateway-body"
            className="font-mono min-h-56"
            value={request}
            onChange={(event) => setRequest(event.target.value)}
          />
          <div className="flex gap-2">
            <Button
              disabled={running || (operation !== "generate/batch" && !model.trim())}
              onClick={() => void run()}
            >
              Run
            </Button>
            <Button
              variant="outline"
              disabled={!running}
              onClick={() => {
                controller.current?.abort();
                setRunning(false);
              }}
            >
              Cancel
            </Button>
          </div>
          {error && (
            <p role="alert" className="text-destructive">
              {error}
            </p>
          )}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Response</CardTitle>
        </CardHeader>
        <CardContent>
          {requestId && <p className="text-sm text-muted-foreground">Request {requestId}</p>}
          {audio && (
            <audio controls src={audio}>
              <track kind="captions" />
            </audio>
          )}
          <pre aria-live="polite" className="whitespace-pre-wrap break-words text-sm font-mono">
            {output || (running ? "Waiting for response…" : "Run a request to see its response.")}
          </pre>
        </CardContent>
      </Card>
    </DashboardPage>
  );
}
