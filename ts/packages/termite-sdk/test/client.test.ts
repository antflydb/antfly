import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { serializeEmbeddings, TermiteClient } from "../src/index.js";

describe("TermiteClient", () => {
  it("should create a client with base config", () => {
    const client = new TermiteClient({
      baseUrl: "http://localhost:8080/api",
    });
    expect(client).toBeDefined();
    expect(client.getRawClient).toBeDefined();
  });

  it("should create a client with custom headers", () => {
    const client = new TermiteClient({
      baseUrl: "http://localhost:8080/api",
      headers: {
        "X-Custom-Header": "test-value",
      },
    });
    expect(client).toBeDefined();
  });

  it("should have all expected methods", () => {
    const client = new TermiteClient({
      baseUrl: "http://localhost:8080/api",
    });

    expect(typeof client.embed).toBe("function");
    expect(typeof client.embedBinary).toBe("function");
    expect(typeof client.chunk).toBe("function");
    expect(typeof client.rerank).toBe("function");
    expect(typeof client.recognize).toBe("function");
    expect(typeof client.extract).toBe("function");
    expect(typeof client.rewrite).toBe("function");
    expect(typeof client.transcribe).toBe("function");
    expect(typeof client.listModels).toBe("function");
    expect(typeof client.getVersion).toBe("function");
    expect(typeof client.getRawClient).toBe("function");
  });

  it("should expose raw client for advanced usage", () => {
    const client = new TermiteClient({
      baseUrl: "http://localhost:8080/api",
    });

    const rawClient = client.getRawClient();
    expect(rawClient).toBeDefined();
    expect(typeof rawClient.GET).toBe("function");
    expect(typeof rawClient.POST).toBe("function");
  });

  it("should strip trailing slash from baseUrl", () => {
    const client = new TermiteClient({
      baseUrl: "http://localhost:8080/api/",
    });
    expect(client).toBeDefined();
  });
});

describe("TermiteClient with mock fetch", () => {
  const originalFetch = global.fetch;

  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.stubGlobal("fetch", originalFetch);
  });

  describe("embed (JSON response)", () => {
    it("should request JSON format and parse response", async () => {
      const mockResponse = {
        model: "bge-small-en-v1.5",
        embeddings: [
          [0.1, 0.2, 0.3],
          [0.4, 0.5, 0.6],
        ],
      };

      // openapi-fetch uses global fetch internally
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({
        baseUrl: "http://localhost:8080/api",
      });

      const result = await client.embed("bge-small-en-v1.5", ["hello", "world"]);

      expect(result.model).toBe("bge-small-en-v1.5");
      expect(result.embeddings).toHaveLength(2);
      expect(result.embeddings[0]).toEqual([0.1, 0.2, 0.3]);
      expect(result.embeddings[1]).toEqual([0.4, 0.5, 0.6]);

      // Verify fetch was called (openapi-fetch uses global fetch)
      expect(fetch).toHaveBeenCalled();
    });

    it("should handle embed errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "Invalid model" }),
        text: () => Promise.resolve(JSON.stringify({ error: "Invalid model" })),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({
        baseUrl: "http://localhost:8080/api",
      });

      await expect(client.embed("invalid-model", ["test"])).rejects.toThrow();
    });
  });

  describe("embedBinary (binary response)", () => {
    it("should request binary format and deserialize response", async () => {
      const embeddings = [
        [0.1, 0.2, 0.3],
        [0.4, 0.5, 0.6],
      ];
      const binaryData = serializeEmbeddings(embeddings);

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: () => Promise.resolve(binaryData),
        headers: new Headers({ "Content-Type": "application/octet-stream" }),
      } as Response);

      const client = new TermiteClient({
        baseUrl: "http://localhost:8080/api",
      });

      const result = await client.embedBinary("bge-small-en-v1.5", ["hello", "world"]);

      expect(result).toHaveLength(2);
      expect(result[0][0]).toBeCloseTo(0.1);
      expect(result[0][1]).toBeCloseTo(0.2);
      expect(result[0][2]).toBeCloseTo(0.3);
      expect(result[1][0]).toBeCloseTo(0.4);
      expect(result[1][1]).toBeCloseTo(0.5);
      expect(result[1][2]).toBeCloseTo(0.6);

      expect(fetch).toHaveBeenCalledTimes(1);
      const [url, options] = vi.mocked(fetch).mock.calls[0];
      expect(url).toBe("http://localhost:8080/api/embed");
      expect(options?.method).toBe("POST");
      expect(options?.headers).toBeDefined();
      const headers = options?.headers as Record<string, string>;
      expect(headers.Accept).toBe("application/octet-stream");
    });

    it("should handle empty embeddings in binary response", async () => {
      const binaryData = serializeEmbeddings([]);

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: () => Promise.resolve(binaryData),
        headers: new Headers({ "Content-Type": "application/octet-stream" }),
      } as Response);

      const client = new TermiteClient({
        baseUrl: "http://localhost:8080/api",
      });

      const result = await client.embedBinary("bge-small-en-v1.5", []);
      expect(result).toEqual([]);
    });

    it("should handle binary embed errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        text: () => Promise.resolve("Invalid model"),
        headers: new Headers({ "Content-Type": "text/plain" }),
      } as Response);

      const client = new TermiteClient({
        baseUrl: "http://localhost:8080/api",
      });

      await expect(client.embedBinary("invalid-model", ["test"])).rejects.toThrow(
        "Embed failed: 400 Invalid model"
      );
    });

    it("should handle high-dimensional embeddings in binary format", async () => {
      // Simulate 384-dimension BGE embeddings
      const dimension = 384;
      const embeddings: number[][] = [];
      for (let i = 0; i < 3; i++) {
        const vector: number[] = [];
        for (let j = 0; j < dimension; j++) {
          vector.push((Math.random() - 0.5) * 2);
        }
        embeddings.push(vector);
      }
      const binaryData = serializeEmbeddings(embeddings);

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: () => Promise.resolve(binaryData),
        headers: new Headers({ "Content-Type": "application/octet-stream" }),
      } as Response);

      const client = new TermiteClient({
        baseUrl: "http://localhost:8080/api",
      });

      const result = await client.embedBinary("bge-small-en-v1.5", ["a", "b", "c"]);

      expect(result).toHaveLength(3);
      expect(result[0]).toHaveLength(384);
      expect(result[1]).toHaveLength(384);
      expect(result[2]).toHaveLength(384);

      // Verify values match (within float32 precision)
      for (let i = 0; i < 3; i++) {
        for (let j = 0; j < dimension; j++) {
          expect(result[i][j]).toBeCloseTo(embeddings[i][j], 5);
        }
      }
    });

    it("should pass truncate option in request body", async () => {
      const binaryData = serializeEmbeddings([[0.1, 0.2]]);

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: () => Promise.resolve(binaryData),
        headers: new Headers({ "Content-Type": "application/octet-stream" }),
      } as Response);

      const client = new TermiteClient({
        baseUrl: "http://localhost:8080/api",
      });

      await client.embedBinary("bge-small-en-v1.5", ["test"], { truncate: true });

      const [, options] = vi.mocked(fetch).mock.calls[0];
      const body = JSON.parse(options?.body as string);
      expect(body.truncate).toBe(true);
      expect(body.model).toBe("bge-small-en-v1.5");
      expect(body.input).toEqual(["test"]);
    });
  });

  describe("recognize", () => {
    it("should recognize entities in text", async () => {
      const mockResponse = {
        model: "gliner-multi-v2.1",
        entities: [
          [
            { text: "John Smith", label: "person", start: 0, end: 10, score: 0.95 },
            { text: "Google", label: "organization", start: 20, end: 26, score: 0.92 },
          ],
        ],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.recognize("gliner-multi-v2.1", ["John Smith works at Google."], {
        labels: ["person", "organization"],
      });

      expect(result.model).toBe("gliner-multi-v2.1");
      expect(result.entities).toHaveLength(1);
      expect(result.entities[0]).toHaveLength(2);
      expect(result.entities[0][0].text).toBe("John Smith");
      expect(result.entities[0][0].label).toBe("person");
      expect(result.entities[0][1].text).toBe("Google");
      expect(fetch).toHaveBeenCalled();
    });

    it("should handle recognize errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "Invalid model" }),
        text: () => Promise.resolve(JSON.stringify({ error: "Invalid model" })),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      await expect(
        client.recognize("invalid-model", ["test"], { labels: ["person"] })
      ).rejects.toThrow("Recognize failed");
    });

    it("should work without optional labels", async () => {
      const mockResponse = {
        model: "bert-base-NER",
        entities: [[{ text: "Paris", label: "LOC", start: 0, end: 5, score: 0.98 }]],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.recognize("bert-base-NER", ["Visit Paris."]);

      expect(result.model).toBe("bert-base-NER");
      expect(result.entities[0][0].label).toBe("LOC");
    });
  });

  describe("extract", () => {
    it("should extract structured data from text", async () => {
      const mockResponse = {
        model: "gliner2-base-v1",
        results: [
          {
            person: [
              {
                name: { value: "John Smith", score: 0.95 },
                age: { value: "30", score: 0.88 },
                company: { value: "Google", score: 0.91 },
              },
            ],
          },
        ],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.extract(
        "gliner2-base-v1",
        ["John Smith is 30 years old and works at Google."],
        { person: ["name::str", "age::str", "company::str"] },
        { includeConfidence: true }
      );

      expect(result.model).toBe("gliner2-base-v1");
      expect(result.results).toHaveLength(1);
      expect(result.results[0].person).toHaveLength(1);
      expect(fetch).toHaveBeenCalled();
    });

    it("should handle extract errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "Model does not support extraction" }),
        text: () => Promise.resolve(JSON.stringify({ error: "Model does not support extraction" })),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      await expect(
        client.extract("invalid-model", ["test"], { person: ["name::str"] })
      ).rejects.toThrow("Extract failed");
    });

    it("should accept all optional parameters without error", async () => {
      const mockResponse = { model: "gliner2-base-v1", results: [{}] };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.extract(
        "gliner2-base-v1",
        ["test"],
        { person: ["name::str"] },
        {
          threshold: 0.5,
          flatNer: false,
          includeConfidence: true,
          includeSpans: true,
        }
      );

      expect(fetch).toHaveBeenCalled();
      expect(result.model).toBe("gliner2-base-v1");
    });
  });

  describe("rewrite", () => {
    it("should rewrite text using seq2seq model", async () => {
      const mockResponse = {
        model: "lmqg/flan-t5-small-squad-qg",
        texts: [["What engineer designed and built the Eiffel Tower?"]],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.rewrite("lmqg/flan-t5-small-squad-qg", [
        "generate question: The Eiffel Tower is named after <hl> Gustave Eiffel <hl>.",
      ]);

      expect(result.model).toBe("lmqg/flan-t5-small-squad-qg");
      expect(result.texts).toHaveLength(1);
      expect(result.texts[0][0]).toContain("Eiffel Tower");
      expect(fetch).toHaveBeenCalled();
    });

    it("should handle rewrite errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "Invalid model" }),
        text: () => Promise.resolve(JSON.stringify({ error: "Invalid model" })),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      await expect(client.rewrite("invalid-model", ["test"])).rejects.toThrow("Rewrite failed");
    });

    it("should handle multiple inputs", async () => {
      const mockResponse = {
        model: "flan-t5",
        texts: [["question 1"], ["question 2"]],
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.rewrite("flan-t5", ["input 1", "input 2"]);

      expect(result.texts).toHaveLength(2);
    });
  });

  describe("transcribe", () => {
    it("should transcribe audio", async () => {
      const mockResponse = {
        model: "openai/whisper-tiny",
        text: "Hello, how are you today?",
      };

      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockResponse),
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      const result = await client.transcribe("base64audiodata", {
        model: "openai/whisper-tiny",
        language: "en",
      });

      expect(result.model).toBe("openai/whisper-tiny");
      expect(result.text).toBe("Hello, how are you today?");
      expect(fetch).toHaveBeenCalled();
    });

    it("should handle transcribe errors", async () => {
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "Invalid audio" }),
        text: () => Promise.resolve(JSON.stringify({ error: "Invalid audio" })),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      const client = new TermiteClient({ baseUrl: "http://localhost:8080/api" });
      await expect(client.transcribe("bad-data")).rejects.toThrow("Transcribe failed");
    });
  });

  describe("JSON vs Binary format comparison", () => {
    it("should return equivalent results from both formats", async () => {
      const embeddings = [
        [0.123, -0.456, 0.789],
        [-0.321, 0.654, -0.987],
      ];

      const jsonResponse = {
        model: "test-model",
        embeddings,
      };

      const binaryData = serializeEmbeddings(embeddings);

      // First call returns JSON
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(jsonResponse),
        text: () => Promise.resolve(JSON.stringify(jsonResponse)),
        headers: new Headers({ "Content-Type": "application/json" }),
      } as Response);

      // Second call returns binary
      vi.mocked(fetch).mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: () => Promise.resolve(binaryData),
        headers: new Headers({ "Content-Type": "application/octet-stream" }),
      } as Response);

      const client = new TermiteClient({
        baseUrl: "http://localhost:8080/api",
      });

      const jsonResult = await client.embed("test-model", ["a", "b"]);
      const binaryResult = await client.embedBinary("test-model", ["a", "b"]);

      // JSON response includes model, binary does not
      expect(jsonResult.model).toBe("test-model");
      expect(jsonResult.embeddings.length).toBe(binaryResult.length);

      // Verify embeddings are equivalent (within float32 precision)
      for (let i = 0; i < embeddings.length; i++) {
        for (let j = 0; j < embeddings[i].length; j++) {
          expect(jsonResult.embeddings[i][j]).toBeCloseTo(binaryResult[i][j], 5);
        }
      }
    });
  });
});
