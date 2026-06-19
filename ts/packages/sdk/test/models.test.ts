import { mkdtemp, readFile, stat, writeFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import { baseModelName, modelDir, parseModelRef, pullHuggingFaceModel } from "../src/models.js";

describe("model helpers", () => {
  it("uses the Antfly inference model layout", () => {
    expect(modelDir("/tmp/models", "antflydb/clipclap:gguf:Q4_K")).toBe("/tmp/models/antflydb/clipclap");
  });

  it("returns base names for tagged model refs", () => {
    expect(baseModelName("antflydb/gliner2-base-v1:gguf:Q4_K")).toBe("antflydb/gliner2-base-v1");
  });

  it("rejects unsafe model refs", () => {
    for (const model of ["../clipclap", "antflydb/..", "./clipclap", "antflydb/.", "antflydb/clipclap/extra"]) {
      expect(() => parseModelRef(model)).toThrow();
    }
  });

  it("pulls the default clipclap Q4_K file", async () => {
    const root = await mkdtemp(join(tmpdir(), "antfly-sdk-models-"));
    const fetchMock = async (input: string | URL | Request): Promise<Response> => {
      const url = String(input);
      if (url.endsWith("/api/models/antflydb/clipclap/tree/main?recursive=1")) {
        return Response.json([
          { path: "config.json", type: "file", size: 2 },
          { path: "clipclap-Q4_K.gguf", type: "file", size: 4 },
          { path: "clipclap-F16.gguf", type: "file", size: 4 },
        ]);
      }
      if (url.endsWith("/antflydb/clipclap/resolve/main/config.json")) return new Response("{}");
      if (url.endsWith("/antflydb/clipclap/resolve/main/clipclap-Q4_K.gguf")) return new Response("q4_k");
      return new Response("missing", { status: 404 });
    };

    const dir = await pullHuggingFaceModel("antflydb/clipclap:gguf:Q4_K", {
      modelsDir: root,
      huggingFaceBaseUrl: "https://hf.test",
      fetch: fetchMock as typeof fetch,
    });

    expect(dir).toBe(join(root, "antflydb", "clipclap"));
    await expect(stat(join(dir, "clipclap-Q4_K.gguf"))).resolves.toBeTruthy();
    await expect(stat(join(dir, "clipclap-F16.gguf"))).rejects.toThrow();
    const manifest = await readFile(join(dir, "model_manifest.json"), "utf8");
    expect(manifest).toContain('"variant": "Q4_K"');
    expect(manifest).toContain('"source": "antflydb/clipclap:gguf:Q4_K"');
  });

  it("defaults the reranker base ref to Q4_K", async () => {
    const root = await mkdtemp(join(tmpdir(), "antfly-sdk-models-"));
    const fetchMock = async (input: string | URL | Request): Promise<Response> => {
      const url = String(input);
      if (url.endsWith("/api/models/antflydb/mxbai-rerank-base-v1/tree/main?recursive=1")) {
        return Response.json([
          { path: "config.json", type: "file", size: 2 },
          { path: "mxbai-rerank-base-v1.Q4_K.gguf", type: "file", size: 4 },
          { path: "mxbai-rerank-base-v1.Q8_0.gguf", type: "file", size: 4 },
        ]);
      }
      if (url.endsWith("/antflydb/mxbai-rerank-base-v1/resolve/main/config.json")) return new Response("{}");
      if (url.endsWith("/antflydb/mxbai-rerank-base-v1/resolve/main/mxbai-rerank-base-v1.Q4_K.gguf")) return new Response("q4_k");
      return new Response("missing", { status: 404 });
    };

    const dir = await pullHuggingFaceModel("antflydb/mxbai-rerank-base-v1", {
      modelsDir: root,
      huggingFaceBaseUrl: "https://hf.test",
      fetch: fetchMock as typeof fetch,
    });

    await expect(stat(join(dir, "mxbai-rerank-base-v1.Q4_K.gguf"))).resolves.toBeTruthy();
    await expect(stat(join(dir, "mxbai-rerank-base-v1.Q8_0.gguf"))).rejects.toThrow();
    const manifest = await readFile(join(dir, "model_manifest.json"), "utf8");
    expect(manifest).toContain('"source": "antflydb/mxbai-rerank-base-v1:gguf:Q4_K"');
  });

  it("preserves repo-relative paths for files with duplicate basenames", async () => {
    const root = await mkdtemp(join(tmpdir(), "antfly-sdk-models-"));
    const fetchMock = async (input: string | URL | Request): Promise<Response> => {
      const url = String(input);
      if (url.endsWith("/api/models/antflydb/gliner2-base-v1/tree/main?recursive=1")) {
        return Response.json([
          { path: "config.json", type: "file", size: 4 },
          { path: "encoder_config/config.json", type: "file", size: 4 },
          { path: "gliner2-encoder.Q4_K.gguf", type: "file", size: 7 },
          { path: "gliner2-head.Q4_K.gguf", type: "file", size: 4 },
        ]);
      }
      if (url.endsWith("/antflydb/gliner2-base-v1/resolve/main/config.json")) return new Response("root");
      if (url.endsWith("/antflydb/gliner2-base-v1/resolve/main/encoder_config/config.json")) return new Response("encd");
      if (url.endsWith("/antflydb/gliner2-base-v1/resolve/main/gliner2-encoder.Q4_K.gguf")) return new Response("encoder");
      if (url.endsWith("/antflydb/gliner2-base-v1/resolve/main/gliner2-head.Q4_K.gguf")) return new Response("head");
      return new Response("missing", { status: 404 });
    };

    const dir = await pullHuggingFaceModel("antflydb/gliner2-base-v1:gguf:Q4_K", {
      modelsDir: root,
      huggingFaceBaseUrl: "https://hf.test",
      fetch: fetchMock as typeof fetch,
    });

    await expect(readFile(join(dir, "config.json"), "utf8")).resolves.toBe("root");
    await expect(readFile(join(dir, "encoder_config", "config.json"), "utf8")).resolves.toBe("encd");
    const manifest = await readFile(join(dir, "model_manifest.json"), "utf8");
    expect(manifest).toContain('"name": "encoder_config/config.json"');
  });

  it("rejects pulls without a matching runnable artifact", async () => {
    const root = await mkdtemp(join(tmpdir(), "antfly-sdk-models-"));
    const fetchMock = async (input: string | URL | Request): Promise<Response> => {
      const url = String(input);
      if (url.endsWith("/api/models/antflydb/clipclap/tree/main?recursive=1")) {
        return Response.json([
          { path: "config.json", type: "file", size: 2 },
          { path: "clipclap-Q4_K.gguf", type: "file", size: 4 },
        ]);
      }
      return new Response("missing", { status: 404 });
    };

    await expect(
      pullHuggingFaceModel("antflydb/clipclap:gguf:Q5_K", {
        modelsDir: root,
        huggingFaceBaseUrl: "https://hf.test",
        fetch: fetchMock as typeof fetch,
      })
    ).rejects.toThrow(/no downloadable files/);
    await expect(stat(join(root, "antflydb", "clipclap", "model_manifest.json"))).rejects.toThrow();
  });

  it("does not write support files before artifacts succeed", async () => {
    const root = await mkdtemp(join(tmpdir(), "antfly-sdk-models-"));
    const fetchMock = async (input: string | URL | Request): Promise<Response> => {
      const url = String(input);
      if (url.endsWith("/api/models/antflydb/clipclap/tree/main?recursive=1")) {
        return Response.json([
          { path: "config.json", type: "file", size: 2 },
          { path: "clipclap-Q4_K.gguf", type: "file", size: 4 },
        ]);
      }
      if (url.endsWith("/antflydb/clipclap/resolve/main/config.json")) return new Response("{}");
      return new Response("missing", { status: 404 });
    };

    await expect(
      pullHuggingFaceModel("antflydb/clipclap:gguf:Q4_K", {
        modelsDir: root,
        huggingFaceBaseUrl: "https://hf.test",
        fetch: fetchMock as typeof fetch,
      })
    ).rejects.toThrow(/downloading clipclap-Q4_K.gguf failed/);
    await expect(stat(join(root, "antflydb", "clipclap", "config.json"))).rejects.toThrow();
    await expect(stat(join(root, "antflydb", "clipclap", "model_manifest.json"))).rejects.toThrow();
  });

  it("reports resume progress and resolved source for bare model refs", async () => {
    const root = await mkdtemp(join(tmpdir(), "antfly-sdk-models-"));
    const dir = join(root, "antflydb", "clipclap");
    await mkdir(dir, { recursive: true });
    await writeFile(join(dir, "config.json"), "{}");
    const fetchMock = async (input: string | URL | Request): Promise<Response> => {
      const url = String(input);
      if (url.endsWith("/api/models/antflydb/clipclap/tree/main?recursive=1")) {
        return Response.json([
          { path: "config.json", type: "file", size: 2 },
          { path: "clipclap-Q4_K.gguf", type: "file", size: 4 },
        ]);
      }
      if (url.endsWith("/antflydb/clipclap/resolve/main/clipclap-Q4_K.gguf")) return new Response("q4_k");
      return new Response("missing", { status: 404 });
    };
    const progress: Array<{ downloaded: number; total: number; blobsDone: number; blobsTotal: number; resumedBytes: number }> = [];

    await pullHuggingFaceModel("antflydb/clipclap", {
      modelsDir: root,
      huggingFaceBaseUrl: "https://hf.test",
      fetch: fetchMock as typeof fetch,
      onProgress: (p) => progress.push(p),
    });

    expect(progress[0]).toMatchObject({ downloaded: 2, total: 6, blobsDone: 1, blobsTotal: 2, resumedBytes: 2 });
    expect(progress.at(-1)).toMatchObject({ downloaded: 6, blobsDone: 2, blobsTotal: 2, resumedBytes: 2 });
    const manifest = await readFile(join(dir, "model_manifest.json"), "utf8");
    expect(manifest).toContain('"source": "antflydb/clipclap:gguf:Q4_K"');
  });

  it("raises a typed disk-space error before downloading", async () => {
    const fetchMock = async (input: string | URL | Request): Promise<Response> => {
      const url = String(input);
      if (url.endsWith("/api/models/antflydb/clipclap/tree/main?recursive=1")) {
        return Response.json([{ path: "clipclap-Q4_K.gguf", type: "file", size: 100 }]);
      }
      return new Response("missing", { status: 404 });
    };

    await expect(
      pullHuggingFaceModel("antflydb/clipclap", {
        modelsDir: await mkdtemp(join(tmpdir(), "antfly-sdk-models-")),
        huggingFaceBaseUrl: "https://hf.test",
        fetch: fetchMock as typeof fetch,
        diskFreeBytes: () => 50,
      })
    ).rejects.toMatchObject({ name: "ModelPullError", code: "disk_space" });
  });
});
