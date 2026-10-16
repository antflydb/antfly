import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { useAppleOcr } from "./use-apple-ocr";

const mocks = vi.hoisted(() => ({
  endpoint: "http://first",
  connection: "local-inference",
  request: vi.fn(),
}));
vi.mock("@/hooks/use-api-config", () => ({
  useApiConfig: () => ({
    inferenceApiUrl: mocks.endpoint,
    inferenceConnectionId: mocks.connection,
  }),
}));
vi.mock("@/lib/utils", () => ({ fetchInferenceJson: mocks.request }));
beforeEach(() => {
  mocks.endpoint = "http://first";
  mocks.connection = "local-inference";
  mocks.request.mockReset();
});
afterEach(cleanup);

it("uses the server capability instead of the browser platform", async () => {
  mocks.request.mockResolvedValue({ apple_ocr: { available: true } });
  const { result } = renderHook(useAppleOcr);
  await waitFor(() => expect(result.current.available).toBe(true));
  expect(mocks.request.mock.calls[0][0]).toBe("http://first/ai/v1/_antfarm/readers");
});

it("treats older servers as unavailable", async () => {
  mocks.request.mockRejectedValue(new Error("404"));
  const { result } = renderHook(useAppleOcr);
  await waitFor(() => expect(result.current.loading).toBe(false));
  expect(result.current.available).toBe(false);
});

it("discards capabilities from an endpoint that was replaced", async () => {
  let resolveFirst!: (value: unknown) => void;
  mocks.request.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        resolveFirst = resolve;
      })
  );
  mocks.request.mockResolvedValue({ apple_ocr: { available: false } });
  const { result, rerender } = renderHook(useAppleOcr);
  mocks.endpoint = "http://second";
  rerender();
  expect(mocks.request.mock.calls[0][1].signal.aborted).toBe(true);
  await waitFor(() => expect(result.current.loading).toBe(false));
  await act(async () => resolveFirst({ apple_ocr: { available: true } }));
  expect(result.current.available).toBe(false);
});

it("does not probe local Apple OCR for a remote connection", () => {
  mocks.connection = "remote-inference";
  const { result } = renderHook(useAppleOcr);
  expect(result.current.available).toBe(false);
  expect(result.current.loading).toBe(false);
  expect(mocks.request).not.toHaveBeenCalled();
});

it("aborts a local probe when switching to a remote connection and probes again on return", async () => {
  let resolveLocal!: (value: unknown) => void;
  mocks.request.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        resolveLocal = resolve;
      })
  );
  mocks.request.mockResolvedValue({ apple_ocr: { available: true } });
  const { result, rerender } = renderHook(useAppleOcr);
  mocks.connection = "remote-inference";
  rerender();
  expect(mocks.request.mock.calls[0][1].signal.aborted).toBe(true);
  expect(result.current.available).toBe(false);
  expect(result.current.loading).toBe(false);
  await act(async () => resolveLocal({ apple_ocr: { available: true } }));
  expect(result.current.available).toBe(false);
  expect(mocks.request).toHaveBeenCalledTimes(1);

  mocks.connection = "local-inference";
  rerender();
  await waitFor(() => expect(result.current.available).toBe(true));
  expect(mocks.request).toHaveBeenCalledTimes(2);
});
