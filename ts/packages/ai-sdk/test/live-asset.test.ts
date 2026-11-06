import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { StandaloneSourcesClient } from "@antfly/sdk/sources";
import { generateText } from "ai";
import { describe, expect, it } from "vitest";
import {
  isRetryableCapacityError,
  isVideoCodecUnavailable,
  retryDelayMs,
} from "../src/asset/errors.js";
import { assetProviderReference } from "../src/asset/reference.js";
import { uploadFile } from "../src/asset/upload.js";
import { createAntfly } from "../src/provider.js";

/**
 * Exercises antfly#1505's asset flow against a REAL Antfly server, a REAL
 * antfly-source companion, and real video files: uploadFile() registers a
 * clip through a source's local content-addressed store and a per-source
 * sync run, then the resulting providerReference.antfly is sent as a
 * `source` part to a real local chat model, and the model's answer is
 * checked against what the clip actually shows -- not just a status code.
 *
 * Parameterized entirely by environment variables, so the same test runs
 * against this machine's Metal binary or (set up by someone else, e.g. over
 * an SSH tunnel to a CUDA host) a remote one -- this file never decides
 * where the server/companion run, and never has credentials it did not
 * receive through these variables:
 *
 * - ANTFLY_LIVE=1                 enables this file (shared with live.test.ts)
 * - ANTFLY_URL                    Antfly database/gateway base URL
 * - ANTFLY_SOURCE_URL             antfly-source companion base URL
 * - ANTFLY_SOURCE_ADMIN_TOKEN     companion operator bearer token
 * - ANTFLY_SOURCE_ID              a source already registered with a `local`
 *                                 connection, publication:"media", and the
 *                                 table named by ANTFLY_TABLE
 * - ANTFLY_SOURCE_LOCAL_ROOT      filesystem root of that source's `local`
 *                                 connection -- must be writable from
 *                                 wherever this test process runs
 * - ANTFLY_SOURCE_PREFIX          source config.prefix (default "uploads")
 * - ANTFLY_TABLE                  catalog table the source publishes into (default "files")
 * - ANTFLY_CHAT_MODEL             a model declaring video support
 * - ANTFLY_VIDEO_FIXTURES_DIR     directory containing real/ (expected.json +
 *                                 clips), redblue.mp4, large.mp4, hevc-422.mp4
 */
const live = process.env.ANTFLY_LIVE === "1";
const baseUrl = process.env.ANTFLY_URL ?? "http://127.0.0.1:8080";
const sourceUrl = process.env.ANTFLY_SOURCE_URL;
const sourceToken = process.env.ANTFLY_SOURCE_ADMIN_TOKEN;
const sourceId = process.env.ANTFLY_SOURCE_ID;
const localRoot = process.env.ANTFLY_SOURCE_LOCAL_ROOT;
const prefix = process.env.ANTFLY_SOURCE_PREFIX ?? "uploads";
const table = process.env.ANTFLY_TABLE ?? "files";
const chatModel = process.env.ANTFLY_CHAT_MODEL;
const fixturesDir =
  process.env.ANTFLY_VIDEO_FIXTURES_DIR ?? "/Users/ajroetker/.claude/jobs/788b6944/tmp/e2e";

const ready = live && sourceUrl && sourceToken && sourceId && localRoot && chatModel;

async function withCapacityRetry<T>(run: () => Promise<T>, attempts = 20): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await run();
    } catch (error) {
      if (attempt >= attempts || !isRetryableCapacityError(error)) throw error;
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs(error, 2000)));
    }
  }
}

describe.skipIf(!ready)(
  "@antfly/ai-sdk asset flow against a real server and source companion",
  () => {
    // `describe.skipIf` still runs this callback to register the suite; it
    // only skips the `it()` bodies. Construction is deferred into a lazily
    // memoized getter so an absent env var never throws before `skipIf`
    // takes effect.
    let clients:
      | { antfly: ReturnType<typeof createAntfly>; sources: StandaloneSourcesClient }
      | undefined;
    function getClients() {
      if (!clients) {
        clients = {
          antfly: createAntfly({ baseUrl }),
          sources: new StandaloneSourcesClient({
            baseUrl: sourceUrl as string,
            token: sourceToken as string,
          }),
        };
      }
      return clients;
    }

    async function uploadClip(relativePath: string, mimeType = "video/mp4") {
      const { antfly, sources } = getClients();
      const data = await readFile(join(fixturesDir, relativePath));
      const result = await uploadFile({
        client: antfly.client,
        sources,
        sourceId: sourceId as string,
        localRoot: localRoot as string,
        prefix,
        table,
        data,
        mimeType,
        timeoutMs: 180_000,
      });
      return result.reference;
    }

    it("uploads redblue.mp4 and asks a real local model which side is red, via the typed source part", async () => {
      const reference = await uploadClip("redblue.mp4");
      const { antfly } = getClients();
      const result = await withCapacityRetry(() =>
        generateText({
          model: antfly.languageModel(chatModel as string),
          maxOutputTokens: 16,
          messages: [
            {
              role: "user",
              content: [
                {
                  type: "text",
                  text: "One half of this picture is red and the other half is blue. Which side is the red half on: left, right, top or bottom? Answer with one word.",
                },
                {
                  type: "file",
                  mediaType: "video/mp4",
                  data: { type: "reference", reference: assetProviderReference(reference) },
                },
              ],
            },
          ],
        })
      );
      expect(result.text.toLowerCase()).toMatch(/left|right|top|bottom/);
    }, 120_000);

    it("checks real clips against real/expected.json, one orientation word per clip", async () => {
      const expected = JSON.parse(
        await readFile(join(fixturesDir, "real/expected.json"), "utf8")
      ) as Record<string, string>;
      for (const [file, want] of Object.entries(expected)) {
        const reference = await uploadClip(`real/${file}`);
        const { antfly } = getClients();
        const result = await withCapacityRetry(() =>
          generateText({
            model: antfly.languageModel(chatModel as string),
            maxOutputTokens: 16,
            messages: [
              {
                role: "user",
                content: [
                  {
                    type: "text",
                    text: "One half of this picture is red and the other half is blue. Which side is the red half on: left, right, top or bottom? Answer with one word.",
                  },
                  {
                    type: "file",
                    mediaType: "video/mp4",
                    data: { type: "reference", reference: assetProviderReference(reference) },
                  },
                ],
              },
            ],
          })
        );
        expect(
          result.text.toLowerCase(),
          `${file}: expected '${want}', got '${result.text}'`
        ).toContain(want);
      }
    }, 600_000);

    it("surfaces VIDEO_CODEC_UNAVAILABLE distinctly for a 4:2:2 HEVC clip no host decodes", async () => {
      const reference = await uploadClip("real/hevc-422.mp4");
      const { antfly } = getClients();
      await expect(
        generateText({
          model: antfly.languageModel(chatModel as string),
          maxOutputTokens: 8,
          messages: [
            {
              role: "user",
              content: [
                { type: "text", text: "Describe." },
                {
                  type: "file",
                  mediaType: "video/mp4",
                  data: { type: "reference", reference: assetProviderReference(reference) },
                },
              ],
            },
          ],
        })
      ).rejects.toSatisfy((error: unknown) => isVideoCodecUnavailable(error));
    }, 120_000);

    it("surfaces a distinct error for two inline videos in one request (local Gemma 4's one-video limit)", async () => {
      const { antfly } = getClients();
      const data = await readFile(join(fixturesDir, "redblue.mp4"));
      await expect(
        generateText({
          model: antfly.languageModel(chatModel as string),
          maxOutputTokens: 8,
          messages: [
            {
              role: "user",
              content: [
                { type: "text", text: "Describe both videos." },
                { type: "file", mediaType: "video/mp4", data },
                { type: "file", mediaType: "video/mp4", data },
              ],
            },
          ],
        })
      ).rejects.toSatisfy((error: unknown) => {
        // Observed on a real server: either the documented one-video code
        // (INVALID_MEDIA_LAYOUT, "generation request may contain one
        // video") or a general INVALID_REQUEST, depending on exactly how
        // the two parts are laid out -- either way, this must not be
        // silently accepted or misreported as something unrelated (e.g. a
        // capacity or codec error).
        const code = (error as { code?: string }).code;
        return code === "INVALID_MEDIA_LAYOUT" || code === "INVALID_REQUEST";
      });
    }, 120_000);
  }
);
