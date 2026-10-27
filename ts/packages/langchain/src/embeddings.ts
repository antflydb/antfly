import type { InferenceClient } from "@antfly/sdk";
import { Embeddings } from "@langchain/core/embeddings";
import { count, type OperationOptions, vectors } from "./utils.js";

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
    const result: number[][] = [];
    const size = this.options.batchSize ?? 64;
    for (let i = 0; i < texts.length; i += size) {
      options?.signal?.throwIfAborted();
      const batch = texts.slice(i, i + size);
      const value = await this.caller.call(async () => {
        options?.signal?.throwIfAborted();
        return this.options.client.embedDense(this.options.model, batch, {
          truncate: this.options.truncate,
          task_type: task,
          dimensions: this.options.dimensions,
          instruction: task === "RETRIEVAL_QUERY" ? this.options.queryInstruction : undefined,
          error_policy: "fail_fast",
          signal: options?.signal,
        });
      });
      result.push(...vectors(value, batch.length, this.options.dimensions ?? result[0]?.length));
    }
    return result;
  }
  embedDocuments(texts: string[], options?: OperationOptions): Promise<number[][]> {
    return this.embed(texts, "RETRIEVAL_DOCUMENT", options);
  }
  async embedQuery(text: string, options?: OperationOptions): Promise<number[]> {
    return (await this.embed([text], "RETRIEVAL_QUERY", options))[0];
  }
}
