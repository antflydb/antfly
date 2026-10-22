import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useModelSelection } from "./use-model-selection";

const { inferenceConnectionRef, searchParamsRef, setInferenceConnectionId, setSearchParams } =
  vi.hoisted(() => {
    const inferenceConnectionRef = { current: "local-inference" };
    const searchParamsRef = { current: new URLSearchParams() };
    return {
      inferenceConnectionRef,
      searchParamsRef,
      setInferenceConnectionId: vi.fn((id: string) => {
        inferenceConnectionRef.current = id;
      }),
      setSearchParams: vi.fn((updater: (current: URLSearchParams) => URLSearchParams) => {
        searchParamsRef.current = updater(searchParamsRef.current);
      }),
    };
  });

vi.mock("@/hooks/use-api-config", () => ({
  useApiConfig: () => ({
    inferenceConnectionId: inferenceConnectionRef.current,
    setInferenceConnectionId,
  }),
}));

vi.mock("react-router-dom", () => ({
  useSearchParams: () => [searchParamsRef.current, setSearchParams],
}));

describe("useModelSelection", () => {
  beforeEach(() => {
    inferenceConnectionRef.current = "local-inference";
    searchParamsRef.current = new URLSearchParams();
    setInferenceConnectionId.mockClear();
    setSearchParams.mockClear();
  });

  afterEach(cleanup);

  it("applies a connection deep link once, then preserves a user connection change", async () => {
    searchParamsRef.current = new URLSearchParams("connection=remote&model=model-a");
    const hook = renderHook(() =>
      useModelSelection({ models: ["model-a", "model-b"], defaultModel: "model-a" })
    );

    await waitFor(() => expect(setInferenceConnectionId).toHaveBeenCalledWith("remote"));
    hook.rerender();
    expect(setInferenceConnectionId).toHaveBeenCalledTimes(1);

    act(() => {
      inferenceConnectionRef.current = "local-inference";
      hook.rerender();
    });

    await waitFor(() => expect(searchParamsRef.current.get("connection")).toBe("local-inference"));
    expect(setInferenceConnectionId).toHaveBeenCalledTimes(1);
  });

  it("replaces a stale model deep link with the available fallback", async () => {
    searchParamsRef.current = new URLSearchParams("connection=local-inference&model=old-model");
    const hook = renderHook(() =>
      useModelSelection({ models: ["new-model"], defaultModel: "new-model" })
    );

    await waitFor(() => expect(hook.result.current.model).toBe("new-model"));
    await waitFor(() => expect(searchParamsRef.current.get("model")).toBe("new-model"));
    expect(searchParamsRef.current.get("connection")).toBe("local-inference");
  });

  it("keeps a model deep link while its requested connection is being applied", async () => {
    searchParamsRef.current = new URLSearchParams("connection=remote&model=remote-model");
    let models = ["local-model"];
    const hook = renderHook(() => useModelSelection({ models }));

    await waitFor(() => expect(setInferenceConnectionId).toHaveBeenCalledWith("remote"));
    expect(searchParamsRef.current.get("connection")).toBe("remote");
    expect(searchParamsRef.current.get("model")).toBe("remote-model");

    act(() => {
      inferenceConnectionRef.current = "remote";
      models = [];
      hook.rerender();
    });

    await waitFor(() => expect(hook.result.current.model).toBe(""));
    expect(searchParamsRef.current.get("model")).toBe("remote-model");

    act(() => {
      models = ["remote-first", "remote-model"];
      hook.rerender();
    });

    await waitFor(() => expect(hook.result.current.model).toBe("remote-model"));
    expect(searchParamsRef.current.get("connection")).toBe("remote");
    expect(searchParamsRef.current.get("model")).toBe("remote-model");
  });

  it("keeps a requested model deep link while model discovery is empty", async () => {
    searchParamsRef.current = new URLSearchParams(
      "connection=local-inference&model=requested-model"
    );
    const hook = renderHook(() => useModelSelection({ models: [] }));

    await waitFor(() => expect(hook.result.current.model).toBe(""));
    expect(searchParamsRef.current.get("model")).toBe("requested-model");
  });
});
