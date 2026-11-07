import { afterEach, expect, it, vi } from "vitest";
import { InferenceClient, InferenceTransportError } from "../src/inference-client.js";

afterEach(() => vi.unstubAllGlobals());

it.each([
  "ECONNRESET",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "EAI_AGAIN",
  "UND_ERR_SOCKET",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_BODY_TIMEOUT",
])("recognizes %s without retaining private transport causes", (code) => {
  const raw = Object.assign(new TypeError("private request URL"), {
    cause: Object.assign(new Error("private socket details"), { code }),
  });
  const error = InferenceTransportError.from(raw);
  expect(error).toBeInstanceOf(InferenceTransportError);
  expect(error?.code).toBe(code);
  expect(error?.retryable).toBe(true);
  expect(error?.message).not.toContain("private");
  expect(error).not.toHaveProperty("cause");
});

it("does not retry unknown, permanent, cyclic, or excessively deep error causes", () => {
  expect(InferenceTransportError.from(new TypeError("invalid URL"))).toBeUndefined();
  expect(
    InferenceTransportError.from(
      Object.assign(new Error("certificate"), { code: "CERT_HAS_EXPIRED" })
    )
  ).toBeUndefined();
  const cycle = Object.assign(new Error("cycle"), { cause: undefined as unknown });
  cycle.cause = cycle;
  expect(InferenceTransportError.from(cycle)).toBeUndefined();
  let error: Error = Object.assign(new Error("connection reset"), { code: "ECONNRESET" });
  for (let i = 0; i < 20; i++) error = Object.assign(new Error("wrapped"), { cause: error });
  expect(InferenceTransportError.from(error)).toBeUndefined();
});

it("normalizes failures on both direct and OpenAPI transports", async () => {
  vi.stubGlobal("fetch", async () => {
    throw Object.assign(new Error("connection reset"), { code: "ECONNRESET" });
  });
  const client = new InferenceClient({ baseUrl: "http://inference.test" });
  await expect(client.generate({ model: "m", messages: [] })).rejects.toBeInstanceOf(
    InferenceTransportError
  );
  await expect(client.embed("m", ["hi"])).rejects.toBeInstanceOf(InferenceTransportError);
  await expect(client.openGenerateStream({ model: "m", messages: [] })).rejects.toBeInstanceOf(
    InferenceTransportError
  );
});

it("preserves abort reasons without normalizing them", async () => {
  const controller = new AbortController();
  const reason = Object.assign(new Error("caller stopped"), { code: "ECONNRESET" });
  vi.stubGlobal("fetch", async () => {
    controller.abort(reason);
    throw reason;
  });
  const client = new InferenceClient({ baseUrl: "http://inference.test" });
  await expect(
    client.generate({ model: "m", messages: [] }, { signal: controller.signal })
  ).rejects.toBe(reason);
});
