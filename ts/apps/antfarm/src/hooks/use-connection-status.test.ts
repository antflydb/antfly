import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useConnectionStatus } from "./use-connection-status";

const config = vi.hoisted(() => ({ apiUrl: "/db/v1", inferenceApiUrl: "", inference: true }));
vi.mock("@/hooks/use-api-config", () => ({ useApiConfig: () => config }));
vi.mock("@/config/products", () => ({
  isProductEnabled: (product: string) => product !== "inference" || config.inference,
}));

const fetchMock = vi.fn<typeof fetch>();
beforeEach(() => {
  config.apiUrl = "/db/v1";
  config.inference = true;
  fetchMock.mockReset();
  vi.stubGlobal("fetch", fetchMock);
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("connection checks", () => {
  it.each([
    [200, "connected"],
    [401, "unauthorized"],
    [403, "forbidden"],
    [404, "unavailable"],
    [502, "unavailable"],
    [503, "unavailable"],
  ])("classifies HTTP %i as %s for both services", async (status, expected) => {
    fetchMock.mockImplementation(async () => new Response(null, { status }));
    const { result } = renderHook(() => useConnectionStatus());
    await waitFor(() => expect(result.current.antfly).toBe(expected));
    expect(result.current.inference).toBe(expected);
  });

  it("distinguishes a transport failure from an HTTP rejection", async () => {
    fetchMock.mockImplementation(async (url) => {
      if (String(url).endsWith("/status")) throw new TypeError("Failed to fetch");
      return new Response(null, { status: 403 });
    });
    const { result } = renderHook(() => useConnectionStatus());
    await waitFor(() => expect(result.current.antfly).toBe("disconnected"));
    expect(result.current.inference).toBe("forbidden");
  });

  it("recovers from an authentication failure after retry", async () => {
    fetchMock.mockImplementation(async () => new Response(null, { status: 401 }));
    const { result } = renderHook(() => useConnectionStatus());
    await waitFor(() => expect(result.current.antfly).toBe("unauthorized"));
    fetchMock.mockImplementation(async () => new Response(null, { status: 200 }));
    act(() => result.current.retry());
    await waitFor(() => expect(result.current.antfly).toBe("connected"));
    expect(result.current.inference).toBe("connected");
  });

  it("bounds the initial checks even when a mount cancellation signal is supplied", async () => {
    vi.useFakeTimers();
    fetchMock.mockImplementation(
      (_url, options) =>
        new Promise((_resolve, reject) => {
          options?.signal?.addEventListener("abort", () => reject(options.signal?.reason));
        })
    );
    // Use a timer-backed timeout to make the browser deadline deterministic.
    vi.spyOn(AbortSignal, "timeout").mockImplementation((ms) => {
      const controller = new AbortController();
      setTimeout(() => controller.abort(new DOMException("Timed out", "TimeoutError")), ms);
      return controller.signal;
    });
    const { result } = renderHook(() => useConnectionStatus());
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(result.current.antfly).toBe("disconnected");
    expect(result.current.inference).toBe("disconnected");
    vi.restoreAllMocks();
  });

  it("ignores an old endpoint's failure after switching API URLs", async () => {
    let rejectOld: (reason: Error) => void = () => {};
    fetchMock.mockImplementation((url) =>
      String(url) === "/db/v1/status"
        ? new Promise((_resolve, reject) => {
            rejectOld = reject;
          })
        : Promise.resolve(new Response(null, { status: 200 }))
    );
    const { result, rerender } = renderHook(() => useConnectionStatus());
    config.apiUrl = "/new/db/v1";
    rerender();
    await waitFor(() => expect(result.current.antfly).toBe("connected"));
    await act(async () => rejectOld(new TypeError("Old connection closed")));
    expect(result.current.antfly).toBe("connected");
  });

  it("skips a disabled inference product", async () => {
    config.inference = false;
    fetchMock.mockResolvedValue(new Response(null, { status: 200 }));
    const { result } = renderHook(() => useConnectionStatus());
    await waitFor(() => expect(result.current.antfly).toBe("connected"));
    expect(result.current.inference).toBe("connected");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
