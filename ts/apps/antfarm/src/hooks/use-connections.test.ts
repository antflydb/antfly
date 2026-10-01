/**
 * Unit tests for liveModelSuggestions
 */
import type { Connection } from "@antfly/sdk";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { liveModelSuggestions, useConnectionsWithModels } from "./use-connections";

const { listConnections, apiUrlRef, client } = vi.hoisted(() => {
  const listConnections = vi.fn();
  return {
    listConnections,
    apiUrlRef: { current: "http://connections-test-0" },
    // Stable identity, as in the real provider, where the client is held in
    // state — an unstable client would re-run every effect on every render.
    client: { connections: { list: listConnections } },
  };
});

vi.mock("@/hooks/use-api-config", () => ({
  useApiConfig: () => ({ apiUrl: apiUrlRef.current, client }),
}));

// The connections cache is module scoped and keyed by API URL, so each test
// gets its own endpoint rather than inheriting a previous test's inventory.
let apiUrlSeq = 0;
function isolateCache() {
  apiUrlSeq += 1;
  apiUrlRef.current = `http://connections-test-${apiUrlSeq}`;
}

function providerConnection(overrides: Partial<Connection> = {}): Connection {
  return {
    id: "openai",
    name: "openai",
    kind: "inference",
    status: "connected",
    capabilities: ["models.embed", "models.generate"],
    inference: {
      provider: "openai",
      models: {
        embedders: [{ name: "text-embedding-3-small" }],
        other: [{ name: "gpt-4o" }],
      },
    },
    ...overrides,
  };
}

describe("liveModelSuggestions", () => {
  it("does not merge unclassified models into embedder suggestions", () => {
    const suggestions = liveModelSuggestions([providerConnection()], "embedder");
    expect(suggestions.openai).toEqual(["text-embedding-3-small"]);
  });

  it("ignores providers that are not connected", () => {
    const suggestions = liveModelSuggestions([providerConnection({ status: "error" })], "embedder");
    expect(suggestions.openai).toBeUndefined();
  });

  it("ignores connections without model expansions", () => {
    const suggestions = liveModelSuggestions(
      [
        providerConnection({
          inference: { provider: "openai" },
        }),
      ],
      "embedder"
    );
    expect(suggestions).toEqual({});
  });

  it("dedupes models across instances of the same provider type", () => {
    const first = providerConnection();
    const second = providerConnection({
      name: "openai-2",
      inference: {
        provider: "openai",
        models: {
          embedders: [{ name: "text-embedding-3-small" }, { name: "text-embedding-3-large" }],
        },
      },
    });
    const suggestions = liveModelSuggestions([first, second], "embedder");
    expect(suggestions.openai).toEqual(["text-embedding-3-small", "text-embedding-3-large"]);
  });

  it("returns generator models for the generator kind", () => {
    const connection = providerConnection({
      name: "claude",
      inference: {
        provider: "anthropic",
        models: {
          generators: [{ name: "claude-sonnet-4-5" }],
        },
      },
    });
    const suggestions = liveModelSuggestions([connection], "generator");
    expect(suggestions.anthropic).toEqual(["claude-sonnet-4-5"]);
  });

  it("merges unclassified models into generator suggestions", () => {
    const suggestions = liveModelSuggestions([providerConnection()], "generator");
    expect(suggestions.openai).toEqual(["gpt-4o"]);
  });
});

describe("useConnectionsWithModels", () => {
  beforeEach(() => {
    listConnections.mockReset();
    isolateCache();
  });

  it("shares an in-flight provider inventory request across consumers", async () => {
    let resolve!: (value: { connections: Connection[] }) => void;
    listConnections.mockReturnValue(new Promise((done) => (resolve = done)));

    const first = renderHook(() => useConnectionsWithModels());
    const second = renderHook(() => useConnectionsWithModels());

    expect(listConnections).toHaveBeenCalledTimes(1);
    act(() => resolve({ connections: [] }));
    await waitFor(() => expect(first.result.current.loading).toBe(false));
    expect(second.result.current.loading).toBe(false);

    first.unmount();
    second.unmount();
  });
});

describe("useConnectionsWithModels revalidation", () => {
  let now = 1_700_000_000_000;

  beforeEach(() => {
    listConnections.mockReset();
    isolateCache();
    now = 1_700_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  /** Expire the client cache window, then trigger the revalidation path. */
  async function revalidate() {
    now += 31_000;
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"));
      await Promise.resolve();
    });
  }

  function renderRecording() {
    const loadingSeen: boolean[] = [];
    const renders: { loading: boolean; count: number }[] = [];
    const view = renderHook(() => {
      const state = useConnectionsWithModels();
      loadingSeen.push(state.loading);
      renders.push({ loading: state.loading, count: state.connections.length });
      return state;
    });
    return { ...view, loadingSeen, renders };
  }

  it("refreshes a stale inventory without flipping the page back to loading", async () => {
    listConnections.mockResolvedValue({ connections: [providerConnection()] });
    const { result, loadingSeen, unmount } = renderRecording();
    await waitFor(() => expect(result.current.loading).toBe(false));

    const settledAt = loadingSeen.length;
    // Hold the revalidation in flight so any loading flip is observable rather
    // than batched away with its own resolution.
    let release!: (value: { connections: Connection[] }) => void;
    listConnections.mockReturnValue(new Promise((done) => (release = done)));
    await revalidate();

    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(2));
    expect(result.current.loading).toBe(false);
    expect(result.current.connections).toHaveLength(1);

    await act(async () => {
      release({
        connections: [providerConnection(), providerConnection({ id: "ollama", name: "ollama" })],
      });
      await Promise.resolve();
    });

    await waitFor(() => expect(result.current.connections).toHaveLength(2));
    // Every render after the first load kept loading false: consumers treat it
    // as a full-page skeleton short-circuit.
    expect(loadingSeen.slice(settledAt)).not.toContain(true);
    unmount();
  });

  it("keeps the last good inventory when a background refresh fails", async () => {
    listConnections.mockResolvedValue({ connections: [providerConnection()] });
    const { result, unmount } = renderRecording();
    await waitFor(() => expect(result.current.loading).toBe(false));

    listConnections.mockRejectedValue(new Error("provider unreachable"));
    await revalidate();
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(2));

    expect(result.current.connections).toHaveLength(1);
    expect(result.current.error).toBeNull();
    expect(result.current.loading).toBe(false);
    unmount();
  });

  it("does not apply a background poll after switching API endpoints", async () => {
    listConnections.mockResolvedValueOnce({
      connections: [providerConnection({ id: "endpoint-a" })],
    });
    const view = renderRecording();
    await waitFor(() => expect(view.result.current.connections[0]?.id).toBe("endpoint-a"));

    let resolvePollA!: (value: { connections: Connection[] }) => void;
    listConnections.mockReturnValueOnce(new Promise((done) => (resolvePollA = done)));
    await revalidate();
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(2));

    apiUrlRef.current = `${apiUrlRef.current}-b`;
    let resolveEndpointB!: (value: { connections: Connection[] }) => void;
    listConnections.mockReturnValueOnce(new Promise((done) => (resolveEndpointB = done)));
    view.rerender();
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(3));

    await act(async () => {
      resolveEndpointB({ connections: [providerConnection({ id: "endpoint-b" })] });
      await Promise.resolve();
    });
    await waitFor(() => expect(view.result.current.connections[0]?.id).toBe("endpoint-b"));

    await act(async () => {
      resolvePollA({ connections: [providerConnection({ id: "endpoint-a-late" })] });
      await Promise.resolve();
    });
    expect(view.result.current.connections[0]?.id).toBe("endpoint-b");
    view.unmount();
  });

  it("does not force a provider refetch, leaving the server cache to bound staleness", async () => {
    listConnections.mockResolvedValue({ connections: [] });
    const { result, unmount } = renderRecording();
    await waitFor(() => expect(result.current.loading).toBe(false));

    await revalidate();
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(2));

    for (const [params] of listConnections.mock.calls) {
      expect(params.refresh).toBeUndefined();
    }
    unmount();
  });

  it("forces a refetch only on an explicit retry", async () => {
    listConnections.mockResolvedValue({ connections: [] });
    const { result, unmount } = renderRecording();
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      result.current.retry();
    });
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(2));

    expect(listConnections.mock.calls[1][0].refresh).toBe(true);
    unmount();
  });

  it("seeds a remount from a stale entry instead of blanking the list", async () => {
    listConnections.mockResolvedValue({ connections: [providerConnection()] });
    const first = renderRecording();
    await waitFor(() => expect(first.result.current.loading).toBe(false));
    first.unmount();

    now += 31_000; // entry is now stale, but still the best thing to show
    const second = renderRecording();

    // The very first render, before any effect runs, already carries the
    // cached inventory — the list never renders empty.
    expect(second.renders[0]).toEqual({ loading: false, count: 1 });
    expect(second.result.current.connections).toHaveLength(1);
    expect(second.loadingSeen).not.toContain(true);
    second.unmount();
  });

  it("converges sibling consumers on one refreshed result", async () => {
    listConnections.mockResolvedValue({ connections: [providerConnection()] });
    const first = renderRecording();
    const second = renderRecording();
    await waitFor(() => expect(first.result.current.loading).toBe(false));

    listConnections.mockResolvedValue({
      connections: [providerConnection(), providerConnection({ id: "ollama", name: "ollama" })],
    });
    await revalidate();

    await waitFor(() => expect(first.result.current.connections).toHaveLength(2));
    // The sibling never issued its own request; it converged off the shared cache.
    expect(second.result.current.connections).toHaveLength(2);
    expect(listConnections).toHaveBeenCalledTimes(2);
    first.unmount();
    second.unmount();
  });

  it("propagates an explicit retry to sibling consumers", async () => {
    listConnections.mockResolvedValue({ connections: [providerConnection()] });
    const first = renderRecording();
    const second = renderRecording();
    await waitFor(() => expect(first.result.current.loading).toBe(false));
    expect(second.result.current.connections).toHaveLength(1);

    // A forced refresh is keyed separately from the cache-backed read, so the
    // sibling cannot join it in flight — it must converge off the cache.
    listConnections.mockResolvedValue({
      connections: [providerConnection(), providerConnection({ id: "ollama", name: "ollama" })],
    });
    await act(async () => {
      first.result.current.retry();
    });

    await waitFor(() => expect(first.result.current.connections).toHaveLength(2));
    expect(second.result.current.connections).toHaveLength(2);
    first.unmount();
    second.unmount();
  });

  it("discards a response that began before the cached entry was captured", async () => {
    // The mount read is held open while a forced retry overtakes it; the two
    // are keyed separately, so they can settle in either order.
    let releaseSlow!: (value: { connections: Connection[] }) => void;
    listConnections.mockReturnValueOnce(new Promise((done) => (releaseSlow = done)));
    const { result, unmount } = renderRecording();
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(1));

    now += 1_000;
    listConnections.mockResolvedValue({
      connections: [providerConnection(), providerConnection({ id: "ollama", name: "ollama" })],
    });
    await act(async () => {
      result.current.retry();
    });
    await waitFor(() => expect(result.current.connections).toHaveLength(2));

    now += 1_000;
    await act(async () => {
      releaseSlow({ connections: [providerConnection()] });
      await Promise.resolve();
    });

    // The older in-flight read must not roll the inventory back.
    expect(result.current.connections).toHaveLength(2);
    unmount();
  });

  it("keeps a forced refresh that a cache-backed read would otherwise clobber", async () => {
    // A forced refresh bypasses the server cache and fans out to providers, so
    // it is slow but authoritative. A concurrent background read is answered
    // from that same cache and is stale by construction — it must not win.
    listConnections.mockResolvedValue({ connections: [providerConnection()] });
    const first = renderRecording();
    await waitFor(() => expect(first.result.current.loading).toBe(false));

    now += 31_000;
    let releaseForced!: (value: { connections: Connection[] }) => void;
    listConnections.mockImplementation((params: { refresh?: boolean }) =>
      params?.refresh
        ? new Promise((done) => (releaseForced = done))
        : Promise.resolve({ connections: [providerConnection()] })
    );

    await act(async () => {
      first.result.current.retry();
    });
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(2));

    now += 1_000;
    // A second consumer mounts against the stale entry and issues the
    // cache-backed read, which lands first with the pre-pull inventory.
    const second = renderRecording();
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(3));

    now += 10_000;
    await act(async () => {
      releaseForced({
        connections: [providerConnection(), providerConnection({ id: "ollama", name: "ollama" })],
      });
      await Promise.resolve();
    });

    // The user's explicit refresh must be what they end up looking at.
    await waitFor(() => expect(first.result.current.connections).toHaveLength(2));
    expect(second.result.current.connections).toHaveLength(2);
    first.unmount();
    second.unmount();
  });

  it("clears a sibling's error once any consumer refreshes successfully", async () => {
    listConnections.mockRejectedValue(new Error("server restarting"));
    const first = renderRecording();
    const second = renderRecording();
    await waitFor(() => expect(first.result.current.error).not.toBeNull());
    expect(second.result.current.error).not.toBeNull();

    listConnections.mockResolvedValue({ connections: [providerConnection()] });
    await act(async () => {
      first.result.current.retry();
    });

    await waitFor(() => expect(first.result.current.error).toBeNull());
    // The sibling shares the recovered data, so it must not stay on an error
    // screen while rendering a healthy inventory.
    expect(second.result.current.connections).toHaveLength(1);
    expect(second.result.current.error).toBeNull();
    first.unmount();
    second.unmount();
  });

  it("does not drop this consumer's skeleton while its own request is in flight", async () => {
    listConnections.mockResolvedValue({ connections: [providerConnection()] });
    const first = renderRecording();
    await waitFor(() => expect(first.result.current.loading).toBe(false));

    now += 31_000;
    let releaseForced!: (value: { connections: Connection[] }) => void;
    listConnections.mockImplementation((params: { refresh?: boolean }) =>
      params?.refresh
        ? new Promise((done) => (releaseForced = done))
        : Promise.resolve({ connections: [providerConnection()] })
    );

    await act(async () => {
      first.result.current.retry();
    });
    await waitFor(() => expect(first.result.current.loading).toBe(true));

    // A sibling's background read completes and broadcasts while this
    // consumer's own refresh is still outstanding.
    const second = renderRecording();
    await waitFor(() => expect(listConnections).toHaveBeenCalledTimes(3));

    expect(first.result.current.loading).toBe(true);

    await act(async () => {
      releaseForced({ connections: [providerConnection()] });
      await Promise.resolve();
    });
    await waitFor(() => expect(first.result.current.loading).toBe(false));
    first.unmount();
    second.unmount();
  });

  it("stops revalidating once unmounted", async () => {
    listConnections.mockResolvedValue({ connections: [] });
    const { result, unmount } = renderRecording();
    await waitFor(() => expect(result.current.loading).toBe(false));
    unmount();

    await revalidate();
    expect(listConnections).toHaveBeenCalledTimes(1);
  });
});
