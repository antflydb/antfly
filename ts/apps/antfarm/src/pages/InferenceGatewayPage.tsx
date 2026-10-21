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
  "chat/completions": { messages: [{ role: "user", content: "Hello" }] },
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

function sampleRequest(
  operation: InferenceGatewayOperation,
  model?: InferenceGatewayModel
): string {
  // Streaming requires affirmative gateway support; unknown includes manually
  // entered IDs and catalog entries without capability metadata.
  const sample =
    operation === "chat/completions"
      ? { ...samples[operation], stream: model?.gateway_capabilities?.streaming === "supported" }
      : samples[operation];
  return JSON.stringify(sample, null, 2);
}

type RequestDraft = {
  model: string;
  operation: InferenceGatewayOperation;
  request: string;
  automaticOperation: boolean;
  requestIsSample: boolean;
};

function modelDefaults(
  draft: RequestDraft,
  model: string,
  catalog: readonly InferenceGatewayModel[]
): RequestDraft {
  const target = catalog.find((entry) => entry.id === model);
  const operation =
    draft.automaticOperation && target?.task
      ? (taskOperation[target.task] ?? draft.operation)
      : draft.operation;
  return {
    ...draft,
    model,
    operation,
    request: draft.requestIsSample ? sampleRequest(operation, target) : draft.request,
  };
}

export default function InferenceGatewayPage() {
  const [params] = useSearchParams();
  const initialModel = params.get("model") ?? "";
  // A different deep link starts a new draft and cancels the previous view's
  // discovery/invocation. Ordinary catalog refreshes retain the current draft.
  return <InferenceGatewayPlayground key={initialModel} initialModel={initialModel} />;
}

function InferenceGatewayPlayground({ initialModel }: { initialModel: string }) {
  const { client } = useApiConfig();
  const [publicInferenceEnabled, setPublicInferenceEnabled] = useState<boolean | null>(null);
  const [models, setModels] = useState<InferenceGatewayModel[]>([]);
  const [{ model, operation, request }, setDraft] = useState<RequestDraft>(() => ({
    model: initialModel,
    operation: "chat/completions",
    request: sampleRequest("chat/completions"),
    automaticOperation: true,
    requestIsSample: true,
  }));
  const [output, setOutput] = useState("");
  const [error, setError] = useState("");
  const [running, setRunning] = useState(false);
  const [audio, setAudio] = useState<string | null>(null);
  const [requestId, setRequestId] = useState<string | null>(null);
  const controller = useRef<AbortController | null>(null);
  useEffect(() => {
    const abort = new AbortController();
    setPublicInferenceEnabled(null);
    client.inference
      .models(abort.signal)
      .then((catalog) => {
        if (abort.signal.aborted) return;
        setPublicInferenceEnabled(catalog.public_inference_enabled !== false);
        setModels(catalog.data);
        // Reconcile against the latest draft, not the deep link or the state
        // captured before discovery. Only untouched defaults may be replaced.
        setDraft((draft) => modelDefaults(draft, draft.model, catalog.data));
      })
      .catch((err: unknown) => {
        if (!abort.signal.aborted) setError(err instanceof Error ? err.message : String(err));
      });
    return () => {
      abort.abort();
      controller.current?.abort();
    };
  }, [client]);
  useEffect(
    () => () => {
      if (audio) URL.revokeObjectURL(audio);
    },
    [audio]
  );
  const selected = models.find((entry) => entry.id === model);
  function selectOperation(value: InferenceGatewayOperation) {
    setDraft((draft) => ({
      ...draft,
      operation: value,
      automaticOperation: false,
      request: sampleRequest(
        value,
        models.find((entry) => entry.id === draft.model)
      ),
      requestIsSample: true,
    }));
  }
  function selectModel(value: string) {
    setDraft((draft) => modelDefaults(draft, value, models));
  }
  async function run() {
    if (publicInferenceEnabled !== true) return;
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
      {publicInferenceEnabled === false ? (
        <Card>
          <CardHeader>
            <CardTitle>Public inference is disabled</CardTitle>
          </CardHeader>
          <CardContent>
            Ask your deployment administrator to enable the inference gateway. Database indexing and
            query inference remain available.
          </CardContent>
        </Card>
      ) : (
        <>
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
                  onChange={(event) =>
                    selectOperation(event.target.value as InferenceGatewayOperation)
                  }
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
                onChange={(event) => {
                  const value = event.target.value;
                  setDraft((draft) => ({
                    ...draft,
                    request: value,
                    requestIsSample: false,
                    // An edited body belongs to the current operation. Discovery
                    // must not reinterpret it as another task's request.
                    automaticOperation: false,
                  }));
                }}
              />
              <div className="flex gap-2">
                <Button
                  disabled={
                    publicInferenceEnabled !== true ||
                    running ||
                    (operation !== "generate/batch" && !model.trim())
                  }
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
                {output ||
                  (running ? "Waiting for response…" : "Run a request to see its response.")}
              </pre>
            </CardContent>
          </Card>
        </>
      )}
    </DashboardPage>
  );
}
