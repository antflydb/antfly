import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ModelInstanceError,
  modelInstanceTask,
  useModelInstancesForModel,
} from "./use-model-instances";

const { inferenceApiUrlRef } = vi.hoisted(() => ({
  inferenceApiUrlRef: { current: "http://inference.test" },
}));

vi.mock("@/hooks/use-api-config", () => ({
  useApiConfig: () => ({ inferenceApiUrl: inferenceApiUrlRef.current }),
}));

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("useModelInstancesForModel", () => {
  const fetchMock = vi.fn<typeof fetch>();

  beforeEach(() => {
    inferenceApiUrlRef.current = "http://inference.test";
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("maps discovery tasks to supported persistent instance tasks", () => {
    expect(modelInstanceTask("decider")).toBe("classifier");
    expect(modelInstanceTask("extractor")).toBe("extractor");
    expect(modelInstanceTask("predictor")).toBeNull();
    expect(modelInstanceTask("synthesizer")).toBeNull();
  });

  it("loads the local runtime inventory without changing cache activity", async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        object: "list",
        data: [
          {
            id: "runtime-1",
            model: "acme/embed",
            tasks: ["embedder", "reader"],
            state: "loaded",
            active_handles: 0,
            startup_managed: false,
            unloadable: true,
            residency_mode: "resident",
            idle_ms: 10,
          },
        ],
      })
    );

    const hook = renderHook(() => useModelInstancesForModel());
    await waitFor(() => expect(hook.result.current.loading).toBe(false));

    expect(fetchMock).toHaveBeenCalledWith(
      "http://inference.test/ai/v1/models/instances",
      expect.objectContaining({ credentials: "same-origin" })
    );
    expect(hook.result.current.runtimeFor("embedder", "acme/embed")?.runtimes[0]).toMatchObject({
      id: "runtime-1",
      state: "loaded",
    });
    expect(hook.result.current.runtimeFor("reader", "acme/embed")?.runtimes[0]).toBe(
      hook.result.current.runtimeFor("embedder", "acme/embed")?.runtimes[0]
    );
  });

  it("refreshes inventory after an exact-runtime unload", async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ object: "list", data: [] }))
      .mockResolvedValueOnce(jsonResponse({ outcome: "unloaded" }))
      .mockResolvedValueOnce(jsonResponse({ object: "list", data: [] }));
    const hook = renderHook(() => useModelInstancesForModel());
    await waitFor(() => expect(hook.result.current.loading).toBe(false));

    await act(async () => {
      await hook.result.current.unload("runtime-1");
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "http://inference.test/ai/v1/models/instances/runtime-1",
      expect.objectContaining({ method: "DELETE", body: undefined })
    );
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it("preserves structured busy conflicts", async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ object: "list", data: [] }))
      .mockResolvedValueOnce(
        jsonResponse({ error: "RUNTIME_IN_USE", message: "Model is serving a request" }, 409)
      );
    const hook = renderHook(() => useModelInstancesForModel());
    await waitFor(() => expect(hook.result.current.loading).toBe(false));

    let thrown: unknown;
    await act(async () => {
      try {
        await hook.result.current.unload("runtime-1");
      } catch (error) {
        thrown = error;
      }
    });

    expect(thrown).toBeInstanceOf(ModelInstanceError);
    expect(thrown).toMatchObject({ code: "RUNTIME_IN_USE", status: 409 });
    expect(hook.result.current.error).toBe("Model is serving a request");
  });

  it("stops automatic polling when lifecycle endpoints are unsupported", async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ error: "not found" }, 404));
    const hook = renderHook(() => useModelInstancesForModel());
    await waitFor(() => expect(hook.result.current.supported).toBe(false));

    act(() => document.dispatchEvent(new Event("visibilitychange")));
    await act(async () => Promise.resolve());
    expect(fetchMock).toHaveBeenCalledTimes(1);

    fetchMock.mockResolvedValueOnce(jsonResponse({ object: "list", data: [] }));
    await act(async () => hook.result.current.refresh());
    expect(hook.result.current.supported).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("ignores inventory returned by a previous endpoint", async () => {
    let resolveOld!: (response: Response) => void;
    fetchMock
      .mockReturnValueOnce(new Promise((resolve) => (resolveOld = resolve)))
      .mockResolvedValueOnce(
        jsonResponse({
          object: "list",
          data: [{ id: "new", tasks: ["embedder"], model: "new/model", state: "loaded" }],
        })
      );

    const hook = renderHook(() => useModelInstancesForModel());
    inferenceApiUrlRef.current = "http://new-inference.test";
    hook.rerender();

    await waitFor(() =>
      expect(hook.result.current.runtimeFor("embedder", "new/model")).toBeDefined()
    );

    await act(async () => {
      resolveOld(
        jsonResponse({
          object: "list",
          data: [{ id: "old", tasks: ["embedder"], model: "old/model", state: "loaded" }],
        })
      );
      await Promise.resolve();
    });

    expect(hook.result.current.runtimeFor("embedder", "old/model")).toBeUndefined();
    expect(hook.result.current.runtimeFor("embedder", "new/model")).toBeDefined();
  });

  it("clears a pending action when the runtime endpoint changes", async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ object: "list", data: [] }))
      .mockImplementationOnce(
        (_input, init) =>
          new Promise((_resolve, reject) => {
            init?.signal?.addEventListener(
              "abort",
              () => reject(new DOMException("Aborted", "AbortError")),
              { once: true }
            );
          })
      )
      .mockResolvedValueOnce(jsonResponse({ object: "list", data: [] }));

    const hook = renderHook(() => useModelInstancesForModel());
    await waitFor(() => expect(hook.result.current.loading).toBe(false));

    let action: Promise<unknown> = Promise.resolve();
    act(() => {
      action = hook.result.current.unload("runtime-1");
    });
    const rejection = expect(action).rejects.toMatchObject({ name: "AbortError" });
    await waitFor(() => expect(hook.result.current.actionPending).toBe(true));

    inferenceApiUrlRef.current = "http://new-inference.test";
    hook.rerender();

    await waitFor(() => expect(hook.result.current.loading).toBe(false));
    expect(hook.result.current.actionPending).toBe(false);
    await rejection;
  });
});
