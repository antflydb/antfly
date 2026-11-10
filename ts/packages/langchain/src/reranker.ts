import type { InferenceClient } from "@antfly/sdk";
import { Document, type DocumentInterface } from "@langchain/core/documents";
import { BaseDocumentCompressor } from "@langchain/core/retrievers/document_compressors";
import { RunnableLambda } from "@langchain/core/runnables";
import { count, type OperationOptions, record } from "./utils.js";

export interface AntflyRerankerOptions {
  client: InferenceClient;
  model: string;
  topN?: number;
}
export class AntflyReranker extends BaseDocumentCompressor {
  constructor(private readonly options: AntflyRerankerOptions) {
    super();
    if (!options.model) throw new TypeError("model is required");
    if (options.topN !== undefined) count(options.topN, "topN");
  }
  async rerank(
    query: string,
    documents: DocumentInterface[],
    options?: OperationOptions & { topN?: number }
  ): Promise<Document[]> {
    const limit = count(options?.topN ?? this.options.topN ?? documents.length, "topN");
    if (!documents.length || !limit) return [];
    options?.signal?.throwIfAborted();
    const response = await this.options.client.rerank(
      this.options.model,
      query,
      documents.map((doc) => doc.pageContent),
      options
    );
    const data = response.data;
    if (
      !Array.isArray(data) ||
      data.length !== documents.length ||
      new Set(data.map((item) => item.index)).size !== documents.length ||
      data.some(
        (item) =>
          !Number.isSafeInteger(item.index) ||
          item.index < 0 ||
          item.index >= documents.length ||
          typeof item.score !== "number" ||
          !Number.isFinite(item.score)
      )
    )
      throw new Error("Reranking must return each input index exactly once with a finite score");
    return [...data]
      .sort((a, b) => b.score - a.score || a.index - b.index)
      .slice(0, limit)
      .map((item) => {
        const original = documents[item.index];
        return new Document({
          id: original.id,
          pageContent: original.pageContent,
          metadata: {
            ...original.metadata,
            _antfly: {
              ...record(original.metadata._antfly ?? {}, "provenance"),
              rerank_score: item.score,
              rerank_index: item.index,
            },
          },
        });
      });
  }
  compressDocuments(documents: DocumentInterface[], query: string): Promise<Document[]> {
    return this.rerank(query, documents);
  }
  asRunnable() {
    return RunnableLambda.from<{ query: string; documents: DocumentInterface[] }, Document[]>(
      (input, config) => this.rerank(input.query, input.documents, { signal: config?.signal })
    );
  }
}
