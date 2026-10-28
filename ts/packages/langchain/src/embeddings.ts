import type { ContentPart, InferenceClient } from "@antfly/sdk";
import { Embeddings } from "@langchain/core/embeddings";
import { count, mapConcurrent, type OperationOptions, vectors } from "./utils.js";

export interface AntflyEmbeddingsOptions {
  client: InferenceClient;
  model: string;
  batchSize?: number;
  dimensions?: number;
  maxConcurrency?: number;
  truncate?: boolean;
  queryInstruction?: string;
}
/** Dense text embeddings. Model selection and downloads are always explicit. */
export class AntflyEmbeddings extends Embeddings {
  private readonly options: AntflyEmbeddingsOptions;
  constructor(options: AntflyEmbeddingsOptions) {
    super({
      maxConcurrency: count(options.maxConcurrency ?? 4, "maxConcurrency", 1),
      maxRetries: 0,
    });
    if (!options.model) throw new TypeError("model is required");
    count(options.batchSize ?? 64, "batchSize", 1);
    if (options.dimensions !== undefined) count(options.dimensions, "dimensions", 1);
    this.options = { ...options };
  }
  private async embed(
    texts: string[],
    task: "RETRIEVAL_DOCUMENT" | "RETRIEVAL_QUERY",
    options?: OperationOptions
  ): Promise<number[][]> {
    const size = this.options.batchSize ?? 64;
    const batches = Array.from({ length: Math.ceil(texts.length / size) }, (_, i) =>
      texts.slice(i * size, (i + 1) * size)
    );
    const result = (
      await mapConcurrent(batches, this.options.maxConcurrency ?? 4, async (batch) => {
        options?.signal?.throwIfAborted();
        return this.caller.call(async () => {
          options?.signal?.throwIfAborted();
          return vectors(
            await this.options.client.embedDense(this.options.model, batch, {
              truncate: this.options.truncate,
              task_type: task,
              dimensions: this.options.dimensions,
              instruction: task === "RETRIEVAL_QUERY" ? this.options.queryInstruction : undefined,
              error_policy: "fail_fast",
              signal: options?.signal,
            }),
            batch.length,
            this.options.dimensions
          );
        });
      })
    ).flat();
    return vectors(result, texts.length, this.options.dimensions);
  }
  /** Each input is one multimodal item; parts within an item share one embedding. */
  async embedMultimodal(inputs: ContentPart[][], options?: OperationOptions): Promise<number[][]> {
    if (inputs.some((parts) => !parts.length))
      throw new TypeError("Multimodal input must contain parts");
    const results = (
      await mapConcurrent(inputs, this.options.maxConcurrency ?? 4, async (parts) => {
        options?.signal?.throwIfAborted();
        return this.caller.call(async () => {
          options?.signal?.throwIfAborted();
          return vectors(
            await this.options.client.embedDense(this.options.model, parts, {
              dimensions: this.options.dimensions,
              task_type: "RETRIEVAL_DOCUMENT",
              error_policy: "fail_fast",
              signal: options?.signal,
            }),
            1,
            this.options.dimensions
          );
        });
      })
    ).flat();
    return vectors(results, inputs.length, this.options.dimensions);
  }

  embedDocuments(texts: string[], options?: OperationOptions): Promise<number[][]> {
    return this.embed(texts, "RETRIEVAL_DOCUMENT", options);
  }
  async embedQuery(text: string, options?: OperationOptions): Promise<number[]> {
    return (await this.embed([text], "RETRIEVAL_QUERY", options))[0];
  }
}
