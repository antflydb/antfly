import type { ConnectedModel, ConnectedModelType, Connection } from "@antfly/sdk";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isProductEnabled } from "@/config/products";
import { useApiConfig } from "@/hooks/use-api-config";
import {
  INFERENCE_MODEL_GROUPS,
  INFERENCE_MODEL_GROUP_TYPES,
  inferenceModelGroups,
} from "@/lib/inference-model-tasks";

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

function useConnectionsInternal(includeModels: boolean, enabled = true): ConnectionsState {
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
  const activeCacheKeyRef = useRef(cacheKey);
  activeCacheKeyRef.current = cacheKey;
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

        if (signal?.aborted || !isMountedRef.current || activeCacheKeyRef.current !== cacheKey) {
          return;
        }
        const entry = cacheConnections(cacheKey, result, refresh);

        setConnections(entry.connections);
        setSupported(entry.supported);
        setError(null);
        setLoading(false);
      } catch (err) {
        if (signal?.aborted) return;
        if (!isMountedRef.current || activeCacheKeyRef.current !== cacheKey) return;

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
    if (!enabled) return;
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
  }, [cacheKey, enabled]);

  useEffect(() => {
    isMountedRef.current = true;

    if (!enabled) {
      setConnections([]);
      setSupported(false);
      setLoading(false);
      setError(null);
      return () => {
        isMountedRef.current = false;
      };
    }

    const fromCache = connectionsCache.get(cacheKey);
    if (fromCache) {
      setConnections(fromCache.connections);
      setSupported(fromCache.supported);
      setLoading(false);
      setError(null);
    } else {
      // Inventory belongs to its API endpoint. Do not render the previous
      // endpoint's providers while the first request for this key is pending.
      setConnections([]);
      setSupported(true);
      setLoading(true);
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
  }, [cacheKey, enabled, fetchConnections]);

  // Model files may be installed out of band by `antfly inference pull`, so the
  // expanded inventory is revalidated while the model UI stays open. The fetch
  // is not forced: forcing bypasses the server-side cache that keeps dashboards
  // off remote provider listing APIs, and the embedded inference listing is
  // served live either way.
  useEffect(() => {
    if (!includeModels || !enabled) return;

    // A poll belongs to this endpoint's effect lifetime. Without this signal,
    // a response started for a previous endpoint can resolve after a switch
    // and update the newly mounted consumer with the old inventory.
    const controller = new AbortController();

    const revalidate = () => {
      if (document.visibilityState === "hidden") return;
      const entry = connectionsCache.get(cacheKey);
      // Another consumer of this key already refreshed inside the window.
      if (entry && isFresh(entry)) return;
      // A forced refresh is already fetching authoritative data for this key.
      if (connectionsInFlight.has(`${cacheKey}|refresh=true`)) return;
      void fetchConnections(controller.signal, { background: true });
    };

    const timer = window.setInterval(revalidate, REVALIDATE_TICK_MS);
    // A backgrounded tab stops revalidating; catch it up as soon as it returns
    // rather than leaving a stale list on screen for a further tick.
    document.addEventListener("visibilitychange", revalidate);
    return () => {
      controller.abort();
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", revalidate);
    };
  }, [cacheKey, enabled, fetchConnections, includeModels]);

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

interface DirectModelInfo {
  capabilities?: string[];
  inputs?: string[];
}

type DirectModelsResponse = Partial<
  Record<(typeof INFERENCE_MODEL_GROUPS)[number], Record<string, DirectModelInfo>>
>;

function directModelEntries(models: Record<string, DirectModelInfo> | undefined): ConnectedModel[] {
  return Object.entries(models ?? {}).map(([name, info]) => ({
    name,
    capabilities: info.capabilities,
    inputs: info.inputs,
  }));
}

function useDirectInferenceModels(enabled: boolean): ConnectedModelsState {
  const { inferenceApiUrl } = useApiConfig();
  const [providers, setProviders] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(enabled);
  const [error, setError] = useState<string | null>(null);
  const requestRef = useRef(0);
  const generationRef = useRef(0);
  const abortRef = useRef<AbortController | null>(null);
  const foregroundPendingRef = useRef(false);

  const fetchModels = useCallback(
    async (background = false) => {
      if (!enabled) return;
      if (background && foregroundPendingRef.current) return;
      const generation = generationRef.current;
      const request = ++requestRef.current;
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;
      if (!background) {
        foregroundPendingRef.current = true;
        setLoading(true);
      }
      try {
        const response = await fetch(`${inferenceApiUrl}/ai/v1/models`, {
          credentials: "same-origin",
          signal: AbortSignal.any([controller.signal, AbortSignal.timeout(FETCH_TIMEOUT)]),
        });
        if (!response.ok) throw new Error(`Model discovery failed (${response.status})`);
        const body = (await response.json()) as DirectModelsResponse;
        if (generation !== generationRef.current || request !== requestRef.current) return;
        const groups = Object.fromEntries(
          INFERENCE_MODEL_GROUPS.map((group) => [group, directModelEntries(body[group])])
        );
        const modelKinds = [
          ...new Set(
            INFERENCE_MODEL_GROUPS.filter((group) => groups[group].length > 0).map(
              (group) => INFERENCE_MODEL_GROUP_TYPES[group]
            )
          ),
        ];
        setProviders([
          {
            id: "local-inference",
            name: "local-inference",
            display_name: "Local inference",
            kind: "inference",
            status: "connected",
            capabilities: modelKinds.map((kind) => `models.${kind}`),
            inference: {
              provider: "antfly",
              configured_model_types: modelKinds,
              models: groups,
            },
          } as Connection,
        ]);
        setError(null);
      } catch (cause) {
        if (
          controller.signal.aborted ||
          generation !== generationRef.current ||
          request !== requestRef.current
        ) {
          return;
        }
        if (background) return;
        setProviders([]);
        setError(cause instanceof Error ? cause.message : "Failed to discover inference models");
      } finally {
        if (!background && generation === generationRef.current && request === requestRef.current) {
          foregroundPendingRef.current = false;
          setLoading(false);
        }
      }
    },
    [enabled, inferenceApiUrl]
  );

  useEffect(() => {
    if (!enabled) return;
    generationRef.current += 1;
    void fetchModels();
    const poll = () => {
      if (document.visibilityState === "visible") void fetchModels(true);
    };
    const timer = window.setInterval(poll, CACHE_TTL_MS);
    document.addEventListener("visibilitychange", poll);
    return () => {
      generationRef.current += 1;
      requestRef.current += 1;
      foregroundPendingRef.current = false;
      abortRef.current?.abort();
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", poll);
    };
  }, [enabled, fetchModels]);

  return { providers, supported: true, loading, error, retry: () => void fetchModels() };
}

/** Inference provider connections with live model listings. */
export function useConnectedModels(): ConnectedModelsState {
  const inferenceOnly = isProductEnabled("inference") && !isProductEnabled("antfly");
  const state = useConnectionsInternal(true, !inferenceOnly);
  // Full Antfarm builds prefer the database connection catalog so they can
  // route remote providers. Servers from before `/connections` still expose
  // the local `/ai/v1/models` catalog, so retain that as a compatibility path.
  const useDirect = inferenceOnly || !state.supported;
  const direct = useDirectInferenceModels(useDirect);
  if (useDirect) return direct;
  return {
    providers: state.connections.filter((connection) => connection.kind === "inference"),
    supported: state.supported,
    loading: state.loading,
    error: state.error,
    retry: state.retry,
  };
}

/** Models exposed by the inference connection selected for playground requests. */
export interface SelectedInferenceModel {
  name: string;
  display_name?: string;
  dimensions?: number;
  configured?: boolean;
  capabilities?: string[];
  inputs?: string[];
}

export function useSelectedInferenceModels(kind: ConnectedModelType): {
  models: SelectedInferenceModel[];
  loading: boolean;
} {
  const { inferenceConnectionId } = useApiConfig();
  const { providers, loading } = useConnectedModels();
  const selected = providers.find((connection) => connection.id === inferenceConnectionId);
  const listed = selected?.inference?.models;
  const models = useMemo<SelectedInferenceModel[]>(
    () =>
      [
        ...inferenceModelGroups(kind).flatMap((group) => listed?.[group] ?? []),
        ...(kind === "generator" ? (listed?.other ?? []) : []),
      ] as SelectedInferenceModel[],
    [kind, listed]
  );
  return {
    models: useMemo(
      () => [...new Map(models.map((model) => [model.name, model])).values()],
      [models]
    ),
    loading,
  };
}

/** Model IDs exposed by the inference connection selected for playground requests. */
export function useSelectedInferenceModelNames(kind: ConnectedModelType): {
  models: string[];
  loading: boolean;
} {
  const { models, loading } = useSelectedInferenceModels(kind);
  return { models: useMemo(() => models.map((model) => model.name), [models]), loading };
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
      ...inferenceModelGroups(kind).flatMap((group) => models[group] ?? []),
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
