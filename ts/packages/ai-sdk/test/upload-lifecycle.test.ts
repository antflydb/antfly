import { createHash } from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AntflyClient } from "@antfly/sdk";
import { StandaloneSourcesClient } from "@antfly/sdk/sources";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import mediaTypes from "../../../../go/pkg/sources/local_media_types.json" with { type: "json" };
import { AssetUploadTimeoutError, uploadFile } from "../src/asset/upload.js";

const sourceId = "11111111-1111-1111-1111-111111111111";
const bytes = new TextEncoder().encode("uploaded bytes");
const digest = createHash("sha256").update(bytes).digest("hex");
let root: string;
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "antfly-upload-lifecycle-"));
});
afterEach(async () => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  await rm(root, { recursive: true, force: true });
});

type Phase = "profile" | "job" | "lookup" | "query";
function environment(mimeType: keyof typeof mediaTypes = "video/mp4", block?: Phase) {
  let blockedSignal: AbortSignal | undefined;
  const extension = mediaTypes[mimeType];
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const url = new URL(request.url);
    const phase = url.pathname.endsWith("/profile")
      ? "profile"
      : url.pathname.includes("/jobs/")
        ? "job"
        : url.pathname.includes("/documents/")
          ? "lookup"
          : url.pathname.endsWith("/query")
            ? "query"
            : "sync";
    if (phase === block) {
      blockedSignal = request.signal;
      return new Promise<Response>((_resolve, reject) => {
        if (request.signal.aborted) reject(request.signal.reason);
        else
          request.signal.addEventListener("abort", () => reject(request.signal.reason), {
            once: true,
          });
      });
    }
    if (phase === "profile")
      return Response.json({
        version: 1,
        connections: { local: { provider: "local" } },
        sources: [
          {
            id: sourceId,
            connection: "local",
            table: "files",
            publication: "media",
            config: {},
            identity: { namespace: sourceId },
          },
        ],
      });
    if (phase === "sync")
      return Response.json(
        { version: 1, id: "job", state: "running", started_at: 1 },
        { status: 202 }
      );
    if (phase === "job")
      return Response.json({ version: 1, id: "job", state: "completed", started_at: 1 });
    if (phase === "query")
      return Response.json({ responses: [{ hits: { hits: [{ _id: "uploaded" }] } }] });
    return Response.json({
      source: { id: sourceId, relative_key: `objects/sha256/${digest}.${extension}` },
      metadata: { sha256: digest },
      content: {
        resource: {
          type: "source",
          version: `"${digest}"`,
          mime_type: mimeType,
          length: bytes.length,
        },
      },
    });
  });
  vi.stubGlobal("fetch", fetchMock);
  return {
    options: {
      client: new AntflyClient({ baseUrl: "http://db.test" }),
      sources: new StandaloneSourcesClient({ baseUrl: "https://sources.test", token: "test" }),
      sourceId,
      table: "files",
      localRoot: root,
      data: bytes,
      mimeType,
      pollIntervalMs: 1,
    },
    fetchMock,
    blocked: () => blockedSignal,
  };
}

it.each(
  Object.entries(mediaTypes)
)("preserves supported MIME %s using .%s", async (mimeType, extension) => {
  const { options } = environment(mimeType as keyof typeof mediaTypes);
  await uploadFile(options);
  expect(await readFile(join(root, "objects", "sha256", `${digest}.${extension}`))).toEqual(
    Buffer.from(bytes)
  );
});

it("normalizes MIME aliases and parameters before selecting a canonical extension", async () => {
  const { options } = environment("audio/flac");
  await uploadFile({ ...options, mimeType: " Audio/X-FLAC ; codecs=flac" });
  expect(await readdir(join(root, "objects", "sha256"))).toEqual([`${digest}.flac`]);
});

it.each([
  "audio/unknown",
  "toString",
])("rejects unsupported MIME %s before touching the source", async (mimeType) => {
  const { options, fetchMock } = environment();
  await expect(uploadFile({ ...options, mimeType })).rejects.toThrow(
    "Unsupported local upload MIME"
  );
  expect(fetchMock).not.toHaveBeenCalled();
  expect(await readdir(root)).toEqual([]);
});

it.each<Phase>([
  "profile",
  "job",
  "lookup",
  "query",
])("deadline aborts a stalled %s request", async (phase) => {
  vi.useFakeTimers();
  const { options, blocked } = environment("video/mp4", phase);
  const pending = uploadFile({
    ...options,
    timeoutMs: 5000,
    waitFor: phase === "query" ? "searchable" : "published",
  });
  const failure = expect(pending).rejects.toMatchObject({
    name: "AssetUploadTimeoutError",
    state: phase === "query" ? "searchable" : "published",
  });
  await vi.waitFor(() => expect(blocked()).toBeDefined(), { interval: 1, timeout: 1000 });
  await vi.advanceTimersByTimeAsync(5000);
  await failure;
  expect(blocked()?.aborted).toBe(true);
  expect(blocked()?.reason).toBeInstanceOf(AssetUploadTimeoutError);
});

it("preserves the caller's abort reason instead of reporting a timeout", async () => {
  const controller = new AbortController();
  const reason = new Error("request disconnected");
  controller.abort(reason);
  const { options, fetchMock } = environment();
  await expect(uploadFile({ ...options, signal: controller.signal })).rejects.toBe(reason);
  expect(fetchMock).not.toHaveBeenCalled();
  expect(await readdir(root)).toEqual([]);
});
