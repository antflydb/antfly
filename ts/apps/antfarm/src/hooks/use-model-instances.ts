import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useApiConfig } from "@/hooks/use-api-config";

import type { components } from "@antfly/sdk";

export type ModelInstance = components["schemas"]["InferenceModelInstance"];
// A catalog-row projection of the flat instance inventory. Shared instances
// retain their ID when shown under more than one supported task.
export interface ModelInstancesForModel {
  task: string;
  model: string;
  load_state: string;
  runtimes: ModelInstance[];
}
type ModelInstanceList = components["schemas"]["InferenceModelInstanceList"];
type InstanceActionResponse =
  | ModelInstance
  | components["schemas"]["InferenceModelInstanceUnloadResponse"];

export class ModelInstanceError extends Error {
  readonly code?: string;
  readonly status?: number;

  constructor(message: string, code?: string, status?: number) {
    super(message);
    this.name = "ModelInstanceError";
    this.code = code;
    this.status = status;
  }
}

async function instanceRequest<T>(url: string, init?: RequestInit): Promise<T> {
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
    throw new ModelInstanceError(
      nestedError?.message ?? body?.message ?? `Model instance request failed (${response.status})`,
      nestedError?.code ?? (typeof body?.error === "string" ? body.error : body?.code),
      response.status
    );
  }
  return body as T;
}

export interface ModelInstancesStateResult {
  models: ModelInstancesForModel[];
  loading: boolean;
  refreshing: boolean;
  actionPending: boolean;
  error: string | null;
  supported: boolean;
  refresh: () => Promise<void>;
  load: (task: string, model: string) => Promise<InstanceActionResponse>;
  unload: (runtimeId: string) => Promise<InstanceActionResponse>;
  runtimeFor: (task: string, model: string) => ModelInstancesForModel | undefined;
}

export function useModelInstancesForModel(): ModelInstancesStateResult {
  const { inferenceApiUrl } = useApiConfig();
  // Instance endpoints describe this inference server. Callers decide
  // whether a catalog row is local or provider-managed from its connection.
  const endpoint = `${inferenceApiUrl}/ai/v1/models/instances`;
  const [models, setModels] = useState<ModelInstancesForModel[]>([]);
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
      const response = await instanceRequest<ModelInstanceList>(endpoint, {
        signal: controller.signal,
      });
      if (generation !== endpointGenerationRef.current || request !== refreshRequestRef.current) {
        return;
      }
      const groups = new Map<string, ModelInstancesForModel>();
      for (const instance of response.data) {
        for (const task of instance.tasks) {
          const key = `${task}\u0000${instance.model}`;
          const group: ModelInstancesForModel = groups.get(key) ?? {
            task,
            model: instance.model,
            load_state: "idle",
            runtimes: [],
          };
          group.runtimes.push(instance);
          if (instance.state === "loading") group.load_state = "loading";
          groups.set(key, group);
        }
      }
      setModels([...groups.values()]);
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
      const runtimeError = cause as ModelInstanceError;
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
    async (
      path: "load" | "unload",
      body: { task?: string; model?: string; instance_id?: string }
    ) => {
      const generation = endpointGenerationRef.current;
      actionAbortRef.current?.abort();
      const controller = new AbortController();
      actionAbortRef.current = controller;
      setActionPending(true);
      setError(null);
      try {
        const response = await instanceRequest<InstanceActionResponse>(
          `${endpoint}${path === "unload" ? `/${encodeURIComponent(body.instance_id!)}` : ""}`,
          {
            method: path === "unload" ? "DELETE" : "POST",
            body: path === "load" ? JSON.stringify(body) : undefined,
            signal: controller.signal,
          }
        );
        if (generation !== endpointGenerationRef.current || controller.signal.aborted) {
          throw new DOMException("Model instance endpoint changed", "AbortError");
        }
        await refresh();
        if (generation !== endpointGenerationRef.current || controller.signal.aborted) {
          throw new DOMException("Model instance endpoint changed", "AbortError");
        }
        return response;
      } catch (cause) {
        if (controller.signal.aborted || generation !== endpointGenerationRef.current) throw cause;
        const message = cause instanceof Error ? cause.message : "Model instance action failed";
        setError(message);
        throw cause;
      } finally {
        if (generation === endpointGenerationRef.current && actionAbortRef.current === controller) {
          setActionPending(false);
        }
      }
    },
    [endpoint, refresh]
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
    unload: useCallback((runtimeId) => mutate("unload", { instance_id: runtimeId }), [mutate]),
    runtimeFor: useCallback((task, model) => runtimeMap.get(`${task}\u0000${model}`), [runtimeMap]),
  };
}
