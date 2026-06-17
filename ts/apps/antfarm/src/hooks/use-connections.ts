import type { ConnectedModelType, Connection } from "@antfly/sdk";
import { useCallback, useEffect, useRef, useState } from "react";
import { useApiConfig } from "@/hooks/use-api-config";

const FETCH_TIMEOUT = 15000; // 15 seconds — model expansion fans out to providers

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
const connectionsCache = new Map<string, { connections: Connection[]; supported: boolean }>();

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
      const controller = new AbortController();
      const timeoutId = window.setTimeout(() => {
        controller.abort(new DOMException("Request timed out", "TimeoutError"));
      }, FETCH_TIMEOUT);
      const abortFromParent = () => controller.abort(signal?.reason);
      signal?.addEventListener("abort", abortFromParent, { once: true });

      try {
        const response = await client.connections.list({
          include: includeModels ? ["models"] : undefined,
          refresh: options?.refresh,
          signal: controller.signal,
        });

        if (!isMountedRef.current) return;

        const result = {
          connections: response?.connections ?? [],
          supported: response !== undefined,
        };
        connectionsCache.set(cacheKey, result);

        setConnections(result.connections);
        setSupported(result.supported);
        setLoading(false);
      } catch (err) {
        if (signal?.aborted) return;
        if (!isMountedRef.current) return;

        const message = err instanceof Error ? err.message : "Failed to fetch connections";
        setError(message);
        setLoading(false);
      } finally {
        window.clearTimeout(timeoutId);
        signal?.removeEventListener("abort", abortFromParent);
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
    fetchConnections(controller.signal);

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
