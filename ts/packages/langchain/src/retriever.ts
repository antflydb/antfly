import { AsyncLocalStorage } from "node:async_hooks";
import type { AntflyClient, TableQueryRequest } from "@antfly/sdk";
import type { CallbackManagerForRetrieverRun } from "@langchain/core/callbacks/manager";
import type { DocumentInterface } from "@langchain/core/documents";
import { BaseRetriever, type BaseRetrieverInput } from "@langchain/core/retrievers";
import type { RunnableConfig } from "@langchain/core/runnables";
import type { AntflyReranker } from "./reranker.js";
import { type AntflyFilter, count, document, hits, intersect, record } from "./utils.js";
import type { AntflyVectorStore } from "./vectorstore.js";

export interface AntflyRetrieverOptions extends BaseRetrieverInput {
  client?: AntflyClient;
  table?: string;
  vectorStore?: AntflyVectorStore;
  index?: string;
  contentField?: string;
  metadataField?: string;
  mode?: "dense" | "text" | "hybrid";
  k?: number;
  fetchK?: number;
  filter?: AntflyFilter;
  fullTextIndex?: string;
  reranker?: AntflyReranker;
}
export interface AntflyRetrieverCallOptions extends RunnableConfig {
  filter?: AntflyFilter;
}
export class AntflyRetriever extends BaseRetriever {
  lc_namespace = ["antfly", "retrievers"];
  private readonly options: AntflyRetrieverOptions;
  private readonly callContext = new AsyncLocalStorage<AntflyRetrieverCallOptions>();
  override invoke(
    input: string,
    config?: AntflyRetrieverCallOptions
  ): Promise<DocumentInterface[]> {
    return this.callContext.run(config ?? {}, () => super.invoke(input, config));
  }
  constructor(options: AntflyRetrieverOptions) {
    super(options);
    if (options.vectorStore ? options.client || options.table : !options.client || !options.table)
      throw new TypeError("Supply vectorStore or client/table, exclusively");
    const k = count(options.k ?? 4, "k");
    const fetchK = count(options.fetchK ?? Math.max(20, k), "fetchK");
    if (fetchK < k) throw new TypeError("fetchK must be >= k");
    if (options.mode && !["dense", "text", "hybrid"].includes(options.mode))
      throw new TypeError("Invalid retrieval mode");
    this.options = { ...options, k, fetchK };
  }
  async _getRelevantDocuments(
    query: string,
    _runManager?: CallbackManagerForRetrieverRun,
    config?: AntflyRetrieverCallOptions
  ): Promise<DocumentInterface[]> {
    config = this.callContext.getStore() ?? config;
    const opts = this.options;
    const k = opts.k ?? 4;
    if (!k) return [];
    config?.signal?.throwIfAborted();
    const store = opts.vectorStore;
    const client = store?.client ?? opts.client;
    const table = store?.table ?? opts.table;
    if (!client || !table) throw new Error("Missing client/table");
    const index = store?.index ?? opts.index ?? "vectors";
    const contentField = store?.contentField ?? opts.contentField ?? "text";
    const metadataField = store?.metadataField ?? opts.metadataField ?? "metadata";
    const mode = opts.mode ?? "hybrid";
    const request: TableQueryRequest = {
      limit: opts.reranker ? opts.fetchK : k,
      fields: [contentField, metadataField],
      filter_query: intersect(store?.filter, intersect(opts.filter, config?.filter)),
    };
    if (mode !== "text") {
      request.indexes = [index];
      if (store) request.embeddings = { [index]: await store.embedQuery(query, config) };
      else {
        const info = await client.indexes.get(table, index);
        const indexConfig = record(info.config);
        if (indexConfig.type !== "embeddings" || indexConfig.sparse || indexConfig.external)
          throw new Error(
            "Native semantic retrieval requires a managed dense index; supply vectorStore for external vectors"
          );
        if (indexConfig.field && indexConfig.field !== contentField)
          throw new Error("Managed index content field mismatch");
        request.semantic_search = query;
      }
    }
    if (mode !== "dense") {
      request.full_text_search = { match: query, field: contentField };
      request.full_text_index = opts.fullTextIndex;
    }
    if (mode === "hybrid") request.merge_config = { strategy: "rrf" };
    const found = hits(await client.tables.query(table, request, { signal: config?.signal }));
    const documents = found.map((hit) => document(hit, contentField, metadataField, true));
    return (
      opts.reranker
        ? await opts.reranker.rerank(query, documents, { signal: config?.signal })
        : documents
    ).slice(0, k);
  }
}
