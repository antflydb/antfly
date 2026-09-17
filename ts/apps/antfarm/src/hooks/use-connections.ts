import type { ConnectedModelType, Connection } from "@antfly/sdk";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useApiConfig } from "@/hooks/use-api-config";

const FETCH_TIMEOUT = 15000; // 15 seconds — model expansion fans out to providers
// Bounds how stale a rendered model list can be. Remote provider listings are
// additionally cached server-side for the same window (connections.zig); the
// embedded inference listing is served live on every request.
const CACHE_TTL_MS = 30_000;
// Ticks more often than the TTL so a window that expires mid-tick is picked up
// promptly; a tick whose cache entry is still fresh does no work.
const REVALIDATE_TICK_MS = 10_000;
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
type ConnectionsPayload = { connections: Connection[]; supported: boolean };
type ConnectionsResult = ConnectionsPayload & { capturedAtMs: number; forced: boolean };
type CacheListener = (entry: ConnectionsResult) => void;

const connectionsCache = new Map<string, ConnectionsResult>();
const connectionsInFlight = new Map<string, Promise<ConnectionsPayload>>();
const cacheListeners = new Map<string, Set<CacheListener>>();

function isFresh(entry: ConnectionsResult): boolean {
  return Date.now() - entry.capturedAtMs < CACHE_TTL_MS;
}

/**
 * Publish a payload to the cache and every mounted consumer of the key.
 *
 * A forced refresh bypasses the server's result cache and re-queries every
 * provider, so it is authoritative but slow; an ordinary read is answered from
 * that same cache and is stale by construction. The two settle in either
 * order, so a plain read must not overwrite a still-fresh forced result —
 * otherwise an explicit Retry visibly does nothing.
 */
function cacheConnections(
  key: string,
  payload: ConnectionsPayload,
  forced: boolean
): ConnectionsResult {
  const existing = connectionsCache.get(key);
  if (!forced && existing?.forced && isFresh(existing)) return existing;

  const entry: ConnectionsResult = { ...payload, capturedAtMs: Date.now(), forced };
  connectionsCache.delete(key);
  connectionsCache.set(key, entry);
  if (connectionsCache.size > MAX_CACHE_ENTRIES) {
    connectionsCache.delete(connectionsCache.keys().next().value as string);
  }
  for (const listener of cacheListeners.get(key) ?? []) listener(entry);
  return entry;
}

function useConnectionsInternal(includeModels: boolean): ConnectionsState {
  const { apiUrl, client } = useApiConfig();
  const cacheKey = `${apiUrl}|models=${includeModels}`;
  // Seed from the cached payload however old it is. Showing the last known
  // inventory while revalidating beats blanking the page back to a skeleton.
  const cached = connectionsCache.get(cacheKey) ?? null;
  const [connections, setConnections] = useState<Connection[]>(cached?.connections ?? []);
  const [supported, setSupported] = useState(cached?.supported ?? true);
  const [loading, setLoading] = useState(cached == null);
  const [error, setError] = useState<string | null>(null);
  const isMountedRef = useRef(true);
  // Requests this consumer is itself waiting on, so a sibling's result cannot
  // clear a skeleton that belongs to an outstanding foreground load.
  const foregroundPendingRef = useRef(0);

  const fetchConnections = useCallback(
    async (signal?: AbortSignal, options?: { refresh?: boolean; background?: boolean }) => {
      // A background revalidation must never take the page back to its loading
      // or error state: consumers treat both as full-page short-circuits.
      const background = options?.background ?? false;
      const refresh = Boolean(options?.refresh);
      if (!background) {
        foregroundPendingRef.current += 1;
        setLoading(true);
        setError(null);
      }

      try {
        const requestKey = `${cacheKey}|refresh=${refresh}`;
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
        const entry = cacheConnections(cacheKey, result, refresh);

        setConnections(entry.connections);
        setSupported(entry.supported);
        setError(null);
        setLoading(false);
      } catch (err) {
        if (signal?.aborted) return;
        if (!isMountedRef.current) return;

        // A failed revalidation keeps the last good inventory on screen; only a
        // foreground load has nothing to fall back to.
        if (background) return;

        const message = err instanceof Error ? err.message : "Failed to fetch connections";
        setError(message);
        setLoading(false);
      } finally {
        if (!background) foregroundPendingRef.current -= 1;
      }
    },
    [cacheKey, client, includeModels]
  );

  const retry = useCallback(() => {
    fetchConnections(undefined, { refresh: true });
  }, [fetchConnections]);

  // Converge every consumer of the same key on one result, so a refresh driven
  // by one mounted component does not leave its siblings disagreeing about
  // which models exist.
  useEffect(() => {
    const listener: CacheListener = (entry) => {
      setConnections(entry.connections);
      setSupported(entry.supported);
      // Recovered data must also retire this consumer's error, or a sibling
      // stays on an error screen while rendering a healthy inventory.
      setError(null);
      if (foregroundPendingRef.current === 0) setLoading(false);
    };
    let listeners = cacheListeners.get(cacheKey);
    if (!listeners) {
      listeners = new Set();
      cacheListeners.set(cacheKey, listeners);
    }
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) cacheListeners.delete(cacheKey);
    };
  }, [cacheKey]);

  useEffect(() => {
    isMountedRef.current = true;

    const fromCache = connectionsCache.get(cacheKey);
    if (fromCache) {
      setConnections(fromCache.connections);
      setSupported(fromCache.supported);
      setLoading(false);
      setError(null);
    }

    const controller = new AbortController();
    if (!fromCache || !isFresh(fromCache)) {
      void fetchConnections(controller.signal, { background: fromCache != null });
    }

    return () => {
      isMountedRef.current = false;
      controller.abort();
    };
  }, [cacheKey, fetchConnections]);

  // Model files may be installed out of band by `antfly inference pull`, so the
  // expanded inventory is revalidated while the model UI stays open. The fetch
  // is not forced: forcing bypasses the server-side cache that keeps dashboards
  // off remote provider listing APIs, and the embedded inference listing is
  // served live either way.
  useEffect(() => {
    if (!includeModels) return;

    const revalidate = () => {
      if (document.visibilityState === "hidden") return;
      const entry = connectionsCache.get(cacheKey);
      // Another consumer of this key already refreshed inside the window.
      if (entry && isFresh(entry)) return;
      // A forced refresh is already fetching authoritative data for this key.
      if (connectionsInFlight.has(`${cacheKey}|refresh=true`)) return;
      void fetchConnections(undefined, { background: true });
    };

    const timer = window.setInterval(revalidate, REVALIDATE_TICK_MS);
    // A backgrounded tab stops revalidating; catch it up as soon as it returns
    // rather than leaving a stale list on screen for a further tick.
    document.addEventListener("visibilitychange", revalidate);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", revalidate);
    };
  }, [cacheKey, fetchConnections, includeModels]);

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
