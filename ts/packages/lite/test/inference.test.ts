// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

/**
 * Tests for the Inference class (embedded inference without a database),
 * mirroring the C ABI's "Embedded inference without a database" contract
 * (see zig/CAPI.md "Inference" and antfly.h), and the same coverage as
 * go/pkg/lite/inference_cgo_test.go and py/packages/lite/tests/test_inference.py.
 */
import { existsSync, mkdtempSync, readdirSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { CancelledError, InvalidArgumentError, NotFoundError } from "../src/errors.js";
import { Inference } from "../src/inference.js";
import type { PullProgress } from "../src/types.js";
import { describeWithLibrary } from "./helpers.js";

function tempModelsDir(): string {
  return mkdtempSync(join(tmpdir(), "antfly-lite-inference-"));
}

function hasModelPrefix(owner: string, prefix: string): boolean {
  const dir = join(homedir(), ".antfly", "inference", "models", owner);
  if (!existsSync(dir)) return false;
  try {
    return readdirSync(dir).some((name) => name.startsWith(prefix));
  } catch {
    return false;
  }
}

function hasQwenEmbeddingModel(): boolean {
  return hasModelPrefix("Qwen", "Qwen3-Embedding-0.6B-GGUF");
}

function hasGemmaGenerateModel(): boolean {
  return hasModelPrefix("ggml-org", "gemma-4-e2b-it-gguf");
}

const GEMMA_MODEL = "ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0";

const PULL_TEST_MODEL = process.env.ANTFLY_INFERENCE_PULL_TEST_MODEL;

describeWithLibrary("Inference", () => {
  describe("open / close", () => {
    it("opens with defaults and closes", async () => {
      const inf = await Inference.open();
      expect(inf).toBeInstanceOf(Inference);
      await inf.close();
      // Double close is a no-op.
      await inf.close();
    });

    it("opens with options (temp models dir + budgets)", async () => {
      const inf = await Inference.open({
        modelsDir: tempModelsDir(),
        hostBudgetMb: 64,
        backendBudgetMb: 64,
        processMemoryBudgetMb: 64,
        combinedBudgetMb: 64,
        kvBudgetMb: 16,
        scratchBudgetMb: 16,
        callTimeoutMs: 30_000,
      });
      try {
        const models = (await inf.listModels()) as { data: unknown[] };
        expect(models.data).toEqual([]);
      } finally {
        await inf.close();
      }
    });

    it("rejects calls made after close", async () => {
      const inf = await Inference.open({ modelsDir: tempModelsDir() });
      await inf.close();
      await expect(inf.chunk({ input: "hi" })).rejects.toBeInstanceOf(InvalidArgumentError);
      await expect(inf.listModels()).rejects.toBeInstanceOf(InvalidArgumentError);
    });
  });

  describe("calls that need no model", () => {
    it("chunk returns data", async () => {
      const inf = await Inference.open({ modelsDir: tempModelsDir() });
      try {
        const result = (await inf.chunk({
          input: "Ants live in colonies. Workers gather food.",
        })) as { data: unknown[] };
        expect(Array.isArray(result.data)).toBe(true);
        expect(result.data.length).toBeGreaterThan(0);
      } finally {
        await inf.close();
      }
    });

    it("listModels on an empty temp models dir has empty data", async () => {
      const inf = await Inference.open({ modelsDir: tempModelsDir() });
      try {
        const result = (await inf.listModels()) as { data: unknown[] };
        expect(result.data).toEqual([]);
      } finally {
        await inf.close();
      }
    });

    it("embed with a missing model rejects with NotFoundError carrying MODEL_NOT_FOUND", async () => {
      const inf = await Inference.open({ modelsDir: tempModelsDir() });
      try {
        let caught: unknown;
        try {
          await inf.embed({ model: "no/such-model", input: ["hello"] });
        } catch (err) {
          caught = err;
        }
        expect(caught).toBeInstanceOf(NotFoundError);
        const err = caught as NotFoundError;
        expect(err.message).toContain("MODEL_NOT_FOUND");
        expect(err.body).toBeTruthy();
        expect((err.body as { error?: string }).error).toBe("MODEL_NOT_FOUND");
      } finally {
        await inf.close();
      }
    });

    it("pull({}) rejects with InvalidArgumentError carrying the JSON error body", async () => {
      const inf = await Inference.open({ modelsDir: tempModelsDir() });
      try {
        let caught: unknown;
        try {
          await inf.pull({});
        } catch (err) {
          caught = err;
        }
        expect(caught).toBeInstanceOf(InvalidArgumentError);
        const err = caught as InvalidArgumentError;
        expect(err.body).toBeTruthy();
      } finally {
        await inf.close();
      }
    });

    it("generate with stream:true rejects with InvalidArgumentError", async () => {
      const inf = await Inference.open({ modelsDir: tempModelsDir() });
      try {
        await expect(inf.generate({ input: "hello", stream: true })).rejects.toBeInstanceOf(
          InvalidArgumentError
        );
      } finally {
        await inf.close();
      }
    });

    it("generateStream with a missing model rejects with NotFoundError carrying MODEL_NOT_FOUND", async () => {
      const inf = await Inference.open({ modelsDir: tempModelsDir() });
      try {
        let caught: unknown;
        try {
          await inf.generateStream(
            { model: "no/such-model", messages: [{ role: "user", content: "hi" }] },
            () => {}
          );
        } catch (err) {
          caught = err;
        }
        expect(caught).toBeInstanceOf(NotFoundError);
        const err = caught as NotFoundError;
        expect(err.message).toContain("MODEL_NOT_FOUND");
        expect((err.body as { error?: string } | undefined)?.error).toBe("MODEL_NOT_FOUND");
      } finally {
        await inf.close();
      }
    });

    it("generateStream with invalid request JSON rejects with InvalidArgumentError", async () => {
      const inf = await Inference.open({ modelsDir: tempModelsDir() });
      try {
        await expect(inf.generateStream("not valid json", () => {})).rejects.toBeInstanceOf(
          InvalidArgumentError
        );
      } finally {
        await inf.close();
      }
    });
  });

  describe.skipIf(!hasQwenEmbeddingModel())("with the local Qwen embedding model installed", () => {
    it("embeds two inputs and returns vectors", async () => {
      const inf = await Inference.open();
      try {
        const result = (await inf.embed({
          model: "Qwen/Qwen3-Embedding-0.6B-GGUF",
          input: ["a", "b"],
        })) as { data: unknown[] };
        expect(result.data.length).toBe(2);
      } finally {
        await inf.close();
      }
    });
  });

  describe.skipIf(!hasGemmaGenerateModel())("with the local Gemma generate model installed", () => {
    it("streams more than two chat.completion.chunk chunks", async () => {
      const inf = await Inference.open();
      try {
        const chunks: Array<{ object?: string }> = [];
        await inf.generateStream(
          {
            model: GEMMA_MODEL,
            messages: [{ role: "user", content: "Count from one to twenty in words." }],
            max_tokens: 48,
          },
          (chunk) => {
            chunks.push(chunk as { object?: string });
          }
        );
        expect(chunks.length).toBeGreaterThan(2);
        for (const chunk of chunks) {
          expect(chunk.object).toBe("chat.completion.chunk");
        }
      } finally {
        await inf.close();
      }
    });

    it("returning false after 2 chunks rejects CancelledError with exactly 2 callbacks", async () => {
      const inf = await Inference.open();
      try {
        let count = 0;
        let caught: unknown;
        try {
          await inf.generateStream(
            {
              model: GEMMA_MODEL,
              messages: [{ role: "user", content: "Count from one to twenty in words." }],
              max_tokens: 48,
            },
            () => {
              count++;
              return count < 2;
            }
          );
        } catch (err) {
          caught = err;
        }
        expect(caught).toBeInstanceOf(CancelledError);
        expect(count).toBe(2);
      } finally {
        await inf.close();
      }
    });
  });

  describe.skipIf(!PULL_TEST_MODEL)(
    "pull (network-gated, ANTFLY_INFERENCE_PULL_TEST_MODEL)",
    () => {
      it("cancelling on the first report rejects CancelledError and leaves the model unlisted, then a full pull succeeds", async () => {
        const modelsDir = tempModelsDir();
        const inf = await Inference.open({ modelsDir });
        try {
          let reports = 0;
          let caught: unknown;
          try {
            await inf.pull({ model: PULL_TEST_MODEL }, () => {
              reports++;
              return false;
            });
          } catch (err) {
            caught = err;
          }
          expect(caught).toBeInstanceOf(CancelledError);
          expect(reports).toBeGreaterThan(0);

          const afterCancel = (await inf.listModels()) as { data: Array<Record<string, unknown>> };
          const namesAfterCancel = afterCancel.data.map((m) => String(m.id ?? m.model ?? ""));
          expect(namesAfterCancel.some((n) => n.includes(PULL_TEST_MODEL as string))).toBe(false);

          const events: PullProgress[] = [];
          const result = (await inf.pull({ model: PULL_TEST_MODEL }, (p) => {
            events.push(p);
          })) as { models: unknown[]; models_dir: string };
          expect(events.length).toBeGreaterThan(0);
          for (const event of events) {
            expect(typeof event.model).toBe("string");
            expect(typeof event.file).toBe("string");
            expect(typeof event.bytesDownloaded).toBe("bigint");
          }
          expect(Array.isArray(result.models)).toBe(true);

          const models = (await inf.listModels()) as { data: Array<Record<string, unknown>> };
          expect(models.data.length).toBeGreaterThan(0);
          const names = models.data.map((m) => String(m.id ?? m.model ?? ""));
          expect(names.some((n) => n.includes(PULL_TEST_MODEL as string))).toBe(true);
        } finally {
          await inf.close();
        }
      }, 180_000);
    }
  );
});
