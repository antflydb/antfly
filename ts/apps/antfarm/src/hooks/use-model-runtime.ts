import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useApiConfig } from "@/hooks/use-api-config";

// Private Antfarm wire types; model-cache controls are not a public SDK contract.
export type ModelRuntimeState = "loading" | "loaded" | "in_use";
export interface ModelRuntimeInstance {
  runtime_id: string;
  state: ModelRuntimeState;
  runtime_kind: "primary" | "composite";
  active_requests: number;
  startup_managed: boolean;
  backend?: string | null;
  device?: string | null;
  residency_mode: "backend_managed" | "streamed" | "resident";
  last_used_ms: number;
}
export interface ModelRuntime {
  task: string;
  model: string;
  load_state: string;
  runtimes: ModelRuntimeInstance[];
}
interface ModelRuntimeResponse {
  object: "model_runtime";
  models: ModelRuntime[];
}
type RuntimeActionResponse = { outcome: string; model?: ModelRuntime };

export class ModelRuntimeError extends Error {
  readonly code?: string;
  readonly status?: number;

  constructor(message: string, code?: string, status?: number) {
    super(message);
    this.name = "ModelRuntimeError";
    this.code = code;
    this.status = status;
  }
}

async function runtimeRequest<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...init,
    headers: { "content-type": "application/json", ...init?.headers },
  });
  const body = (await response.json().catch(() => undefined)) as
    | {
        code?: string;
        error?: string | { code?: string; message?: string };
        message?: string;
      }
    | undefined;
  if (!response.ok) {
    const nestedError = typeof body?.error === "object" ? body.error : undefined;
    throw new ModelRuntimeError(
      nestedError?.message ?? body?.message ?? `Runtime request failed (${response.status})`,
      nestedError?.code ?? (typeof body?.error === "string" ? body.error : body?.code),
      response.status
    );
  }
  return body as T;
}

export interface ModelRuntimeStateResult {
  models: ModelRuntime[];
  loading: boolean;
  refreshing: boolean;
  actionPending: boolean;
  error: string | null;
  supported: boolean;
  refresh: () => Promise<void>;
  load: (task: string, model: string) => Promise<RuntimeActionResponse>;
  unload: (runtimeId: string) => Promise<RuntimeActionResponse>;
  runtimeFor: (task: string, model: string) => ModelRuntime | undefined;
}

export function useModelRuntime(): ModelRuntimeStateResult {
  const { inferenceApiUrl } = useApiConfig();
  // Lifecycle endpoints always describe the local runtime. Callers decide
  // whether a catalog row is local or provider-managed from its connection.
  const endpoint = `${inferenceApiUrl}/ai/v1/_antfarm/models/status`;
  const [models, setModels] = useState<ModelRuntime[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [actionPending, setActionPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [supported, setSupported] = useState(true);
  const endpointGenerationRef = useRef(0);
  const refreshRequestRef = useRef(0);
  const refreshAbortRef = useRef<AbortController | null>(null);
  const actionAbortRef = useRef<AbortController | null>(null);
  const supportedRef = useRef(true);

  const refresh = useCallback(async () => {
    const generation = endpointGenerationRef.current;
    const request = ++refreshRequestRef.current;
    refreshAbortRef.current?.abort();
    const controller = new AbortController();
    refreshAbortRef.current = controller;
    setRefreshing(true);
    try {
      const response = await runtimeRequest<ModelRuntimeResponse>(endpoint, {
        signal: controller.signal,
      });
      if (generation !== endpointGenerationRef.current || request !== refreshRequestRef.current) {
        return;
      }
      setModels(response.models ?? []);
      supportedRef.current = true;
      setSupported(true);
      setError(null);
    } catch (cause) {
      if (
        controller.signal.aborted ||
        generation !== endpointGenerationRef.current ||
        request !== refreshRequestRef.current
      ) {
        return;
      }
      const runtimeError = cause as ModelRuntimeError;
      if (runtimeError.status === 404 || runtimeError.status === 405) {
        supportedRef.current = false;
        setSupported(false);
        setModels([]);
        setError(null);
      } else {
        setError(runtimeError.message);
      }
    } finally {
      if (generation === endpointGenerationRef.current && request === refreshRequestRef.current) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, [endpoint]);

  useEffect(() => {
    endpointGenerationRef.current += 1;
    setLoading(true);
    setModels([]);
    setError(null);
    supportedRef.current = true;
    setSupported(true);
    setActionPending(false);
    void refresh();
    const poll = () => {
      if (supportedRef.current && document.visibilityState === "visible") void refresh();
    };
    const timer = window.setInterval(poll, 5_000);
    document.addEventListener("visibilitychange", poll);
    return () => {
      endpointGenerationRef.current += 1;
      refreshRequestRef.current += 1;
      refreshAbortRef.current?.abort();
      actionAbortRef.current?.abort();
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", poll);
    };
  }, [refresh]);

  const mutate = useCallback(
    async (path: "load" | "unload", body: object) => {
      const generation = endpointGenerationRef.current;
      actionAbortRef.current?.abort();
      const controller = new AbortController();
      actionAbortRef.current = controller;
      setActionPending(true);
      setError(null);
      try {
        const response = await runtimeRequest<RuntimeActionResponse>(
          `${inferenceApiUrl}/ai/v1/_antfarm/models/${path}`,
          { method: "POST", body: JSON.stringify(body), signal: controller.signal }
        );
        if (generation !== endpointGenerationRef.current || controller.signal.aborted) {
          throw new DOMException("Runtime endpoint changed", "AbortError");
        }
        await refresh();
        if (generation !== endpointGenerationRef.current || controller.signal.aborted) {
          throw new DOMException("Runtime endpoint changed", "AbortError");
        }
        return response;
      } catch (cause) {
        if (controller.signal.aborted || generation !== endpointGenerationRef.current) throw cause;
        const message = cause instanceof Error ? cause.message : "Runtime action failed";
        setError(message);
        throw cause;
      } finally {
        if (generation === endpointGenerationRef.current && actionAbortRef.current === controller) {
          setActionPending(false);
        }
      }
    },
    [inferenceApiUrl, refresh]
  );

  const runtimeMap = useMemo(
    () => new Map(models.map((model) => [`${model.task}\u0000${model.model}`, model])),
    [models]
  );

  return {
    models,
    loading,
    refreshing,
    actionPending,
    error,
    supported,
    refresh,
    load: useCallback((task, model) => mutate("load", { task, model }), [mutate]),
    unload: useCallback((runtimeId) => mutate("unload", { runtime_id: runtimeId }), [mutate]),
    runtimeFor: useCallback((task, model) => runtimeMap.get(`${task}\u0000${model}`), [runtimeMap]),
  };
}
