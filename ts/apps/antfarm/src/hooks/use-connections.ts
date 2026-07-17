import type { ConnectedModelType, Connection } from "@antfly/sdk";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useApiConfig } from "@/hooks/use-api-config";

const FETCH_TIMEOUT = 15000; // 15 seconds — model expansion fans out to providers
const MAX_CACHE_ENTRIES = 16;

export interface ConnectionsState {
  connections: Connection[];
  /** False when the server predates the /connections endpoint. */
  supported: boolean;
  loading: boolean;
  error: string | null;
  retry: () => void;
}

export interface ConnectedModelsState {
  providers: Connection[];
  supported: boolean;
  loading: boolean;
  error: string | null;
  retry: () => void;
}

// Cache connection data per API endpoint + expansion so dashboards and
// dropdowns share one fetch per session.
type ConnectionsResult = { connections: Connection[]; supported: boolean };

const connectionsCache = new Map<string, ConnectionsResult>();
const connectionsInFlight = new Map<string, Promise<ConnectionsResult>>();

function cacheConnections(key: string, result: ConnectionsResult) {
  connectionsCache.delete(key);
  connectionsCache.set(key, result);
  if (connectionsCache.size > MAX_CACHE_ENTRIES) {
    connectionsCache.delete(connectionsCache.keys().next().value as string);
  }
}

function useConnectionsInternal(includeModels: boolean): ConnectionsState {
  const { apiUrl, client } = useApiConfig();
  const cacheKey = `${apiUrl}|models=${includeModels}`;
  const cached = connectionsCache.get(cacheKey) ?? null;
  const [connections, setConnections] = useState<Connection[]>(cached?.connections ?? []);
  const [supported, setSupported] = useState(cached?.supported ?? true);
  const [loading, setLoading] = useState(!cached);
  const [error, setError] = useState<string | null>(null);
  const isMountedRef = useRef(true);

  const fetchConnections = useCallback(
    async (signal?: AbortSignal, options?: { refresh?: boolean }) => {
      setLoading(true);
      setError(null);

      try {
        const requestKey = `${cacheKey}|refresh=${Boolean(options?.refresh)}`;
        let request = connectionsInFlight.get(requestKey);
        if (!request) {
          request = (async () => {
            const controller = new AbortController();
            const timeoutId = window.setTimeout(() => {
              controller.abort(new DOMException("Request timed out", "TimeoutError"));
            }, FETCH_TIMEOUT);
            try {
              const response = await client.connections.list({
                include: includeModels ? ["models"] : undefined,
                refresh: options?.refresh,
                signal: controller.signal,
              });
              return {
                connections: response?.connections ?? [],
                supported: response !== undefined,
              };
            } finally {
              window.clearTimeout(timeoutId);
            }
          })();
          connectionsInFlight.set(requestKey, request);
          request.then(
            () => connectionsInFlight.delete(requestKey),
            () => connectionsInFlight.delete(requestKey)
          );
        }
        const result = await request;

        if (signal?.aborted || !isMountedRef.current) return;
        cacheConnections(cacheKey, result);

        setConnections(result.connections);
        setSupported(result.supported);
        setLoading(false);
      } catch (err) {
        if (signal?.aborted) return;
        if (!isMountedRef.current) return;

        const message = err instanceof Error ? err.message : "Failed to fetch connections";
        setError(message);
        setLoading(false);
      }
    },
    [cacheKey, client, includeModels]
  );

  const retry = useCallback(() => {
    fetchConnections(undefined, { refresh: true });
  }, [fetchConnections]);

  useEffect(() => {
    isMountedRef.current = true;

    const fromCache = connectionsCache.get(cacheKey);
    if (fromCache) {
      setConnections(fromCache.connections);
      setSupported(fromCache.supported);
      setLoading(false);
      setError(null);
      return () => {
        isMountedRef.current = false;
      };
    }

    const controller = new AbortController();
    void fetchConnections(controller.signal);

    return () => {
      isMountedRef.current = false;
      controller.abort();
    };
  }, [cacheKey, fetchConnections]);

  return { connections, supported, loading, error, retry };
}

/** All configured connections (inference, web search, external IO, CDC sources). */
export function useConnections(): ConnectionsState {
  return useConnectionsInternal(false);
}

/** All configured connections with inference provider model listings expanded. */
export function useConnectionsWithModels(): ConnectionsState {
  return useConnectionsInternal(true);
}

/** Inference provider connections with live model listings. */
export function useConnectedModels(): ConnectedModelsState {
  const state = useConnectionsInternal(true);
  return {
    providers: state.connections.filter((connection) => connection.kind === "inference"),
    supported: state.supported,
    loading: state.loading,
    error: state.error,
    retry: state.retry,
  };
}

/** Models exposed by the inference connection selected for playground requests. */
export function useSelectedInferenceModelNames(kind: ConnectedModelType): {
  models: string[];
  loading: boolean;
} {
  const { inferenceConnectionId } = useApiConfig();
  const { providers, loading } = useConnectedModels();
  const selected = providers.find((connection) => connection.id === inferenceConnectionId);
  const listed = selected?.inference?.models;
  const models = useMemo(
    () =>
      [
        ...(listed?.[kind === "other" ? "other" : `${kind}s`] ?? []),
        ...(kind === "generator" ? (listed?.other ?? []) : []),
      ].map((model) => model.name),
    [kind, listed]
  );
  return { models: useMemo(() => [...new Set(models)], [models]), loading };
}

/**
 * Per-provider-type live model name suggestions for a model kind.
 *
 * Providers whose listing APIs do not classify generator models by task
 * (OpenAI, Ollama) report them under "other", so generator suggestions merge
 * that bucket. Embedders/rerankers only use their explicit task bucket to avoid
 * suggesting chat models in embedding index forms. Only connected providers
 * contribute.
 */
export function liveModelSuggestions(
  providers: Connection[],
  kind: Exclude<ConnectedModelType, "other">
): Record<string, string[]> {
  const suggestions: Record<string, string[]> = {};
  for (const connection of providers) {
    const provider = connection.inference;
    if (!provider || connection.status !== "connected") continue;
    const models = provider.models;
    if (!models) continue;
    const names = [
      ...(models[`${kind}s`] ?? []),
      ...(kind === "generator" ? (models.other ?? []) : []),
    ]
      .map((model) => model.name)
      .filter((name) => name.length > 0);
    if (names.length === 0) continue;
    const existing = suggestions[provider.provider] ?? [];
    suggestions[provider.provider] = [...new Set([...existing, ...names])];
  }
  return suggestions;
}
