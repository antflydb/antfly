import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { AntflyClient, CreateIndexRequest, TableQueryRequest } from "@antfly/sdk";
import type { Callbacks } from "@langchain/core/callbacks/manager";
import type { DocumentInterface } from "@langchain/core/documents";
import type { EmbeddingsInterface } from "@langchain/core/embeddings";
import type { RunnableConfig } from "@langchain/core/runnables";
import {
  type MaxMarginalRelevanceSearchOptions,
  VectorStore,
  VectorStoreRetriever,
  type VectorStoreRetrieverInput,
} from "@langchain/core/vectorstores";
import { AntflyEmbeddings } from "./embeddings.js";
import {
  type AntflyFilter,
  count,
  type DistanceMetric,
  document,
  hits,
  intersect,
  isStatus,
  type OperationOptions,
  record,
  validateDocuments,
  vectors,
  write,
} from "./utils.js";

const operationContext = new AsyncLocalStorage<OperationOptions>();
class CancellableRetriever<V extends AntflyVectorStore> extends VectorStoreRetriever<V> {
  override invoke(input: string, options?: RunnableConfig): Promise<DocumentInterface[]> {
    return operationContext.run(options ?? {}, () => super.invoke(input, options));
  }
}

export interface AntflyVectorStoreOptions {
  client: AntflyClient;
  table: string;
  index?: string;
  contentField?: string;
  metadataField?: string;
  distanceMetric?: DistanceMetric;
  dimensions?: number;
  filter?: AntflyFilter;
  batchSize?: number;
  /** Await full index visibility by default. False accepts committed_pending. */
  wait?: boolean;
}
export interface AntflyAddOptions extends OperationOptions {
  ids?: string[];
}
export class AntflyVectorStore extends VectorStore {
  declare FilterType: AntflyFilter;
  readonly client: AntflyClient;
  readonly table: string;
  readonly index: string;
  readonly contentField: string;
  readonly metadataField: string;
  readonly filter?: AntflyFilter;
  private readonly options: AntflyVectorStoreOptions;
  private dimension?: number;
  private provision = false;
  private validated = false;
  private provisioning?: Promise<void>;
  constructor(embeddings: EmbeddingsInterface, options: AntflyVectorStoreOptions) {
    super(embeddings, { table: options.table, index: options.index });
    if (!options.table) throw new TypeError("table is required");
    this.options = { ...options };
    this.client = options.client;
    this.table = options.table;
    this.index = options.index ?? "vectors";
    this.contentField = options.contentField ?? "text";
    this.metadataField = options.metadataField ?? "metadata";
    for (const field of [this.contentField, this.metadataField]) {
      if (!field || field.startsWith("_") || field.includes("."))
        throw new TypeError("Content and metadata fields must be ordinary top-level names");
    }
    if (!this.index || this.contentField === this.metadataField)
      throw new TypeError("Index must be named and document fields must differ");
    if (
      options.distanceMetric &&
      !["cosine", "l2_squared", "inner_product"].includes(options.distanceMetric)
    )
      throw new TypeError("Invalid distance metric");
    if (options.dimensions !== undefined) count(options.dimensions, "dimensions", 1);
    count(options.batchSize ?? 64, "batchSize", 1);
    this.dimension = options.dimensions;
    this.filter = options.filter;
  }
  override asRetriever(
    kOrFields?: number | Partial<VectorStoreRetrieverInput<this>>,
    filter?: this["FilterType"],
    callbacks?: Callbacks,
    tags?: string[],
    metadata?: Record<string, unknown>,
    verbose?: boolean
  ): VectorStoreRetriever<this> {
    const retriever = super.asRetriever(kOrFields, filter, callbacks, tags, metadata, verbose);
    return new CancellableRetriever({
      vectorStore: this,
      k: retriever.k,
      filter: retriever.filter,
      ...(retriever.searchType === "mmr"
        ? { searchType: "mmr" as const, searchKwargs: retriever.searchKwargs }
        : { searchType: "similarity" as const }),
      callbacks: retriever.callbacks,
      tags: retriever.tags,
      metadata: retriever.metadata,
      verbose: retriever.verbose,
    });
  }
  _vectorstoreType(): string {
    return "antfly";
  }
  /** Constructors attach only. Explicit creation never replaces a table. */
  static async create(
    embeddings: EmbeddingsInterface,
    options: AntflyVectorStoreOptions
  ): Promise<AntflyVectorStore> {
    const store = new AntflyVectorStore(embeddings, options);
    await store.client.tables.create(store.table);
    store.provision = true;
    if (store.dimension) await store.ensureValidated(store.dimension);
    return store;
  }
  private async ensure(dimension?: number): Promise<void> {
    if (!(await this.client.tables.get(this.table))) throw new Error("Table response was empty");
    let info: Awaited<ReturnType<AntflyClient["indexes"]["get"]>>;
    try {
      info = await this.client.indexes.get(this.table, this.index);
    } catch (error) {
      if (!this.provision || !isStatus(error, 404)) throw error;
      const size = dimension ?? this.dimension;
      if (size === undefined) return;
      const config: CreateIndexRequest = {
        type: "embeddings",
        external: true,
        dimension: size,
        distance_metric: this.options.distanceMetric ?? "cosine",
        description: `langchain-antfly:${JSON.stringify({ version: 1, content_field: this.contentField, metadata_field: this.metadataField })}`,
      };
      try {
        await this.client.indexes.create(this.table, this.index, config);
      } catch (race) {
        if (!isStatus(race, 409)) throw race;
      }
      info = await this.client.indexes.get(this.table, this.index);
    }
    const config = record(info.config, "index configuration");
    if (config.type !== "embeddings" || config.sparse || config.external !== true)
      throw new Error(
        "AntflyVectorStore requires an external dense embeddings index; use AntflyRetriever for managed indexes"
      );
    if (
      this.options.distanceMetric &&
      (config.distance_metric ?? "l2_squared") !== this.options.distanceMetric
    )
      throw new Error("Index distance metric mismatch");
    const expected = config.dimension;
    if (typeof expected !== "number" || expected < 1)
      throw new Error("Index has no dense dimension");
    if (
      (dimension !== undefined && expected !== dimension) ||
      (this.dimension !== undefined && this.dimension !== expected)
    )
      throw new Error(`Embedding dimension mismatch: expected ${expected}`);
    if (
      typeof config.description === "string" &&
      config.description.startsWith("langchain-antfly:")
    ) {
      const declaration = record(JSON.parse(config.description.slice("langchain-antfly:".length)));
      if (
        declaration.content_field !== this.contentField ||
        declaration.metadata_field !== this.metadataField
      )
        throw new Error("Existing adapter document fields are incompatible");
    }
    this.dimension = expected;
    this.provision = false;
  }
  async validate(dimension?: number): Promise<void> {
    await this.ensureValidated(dimension, true);
  }
  private async ensureValidated(dimension?: number, refresh = false): Promise<void> {
    // Serialize resource creation within this instance; conflicting external creators are checked after 409.
    while (this.provisioning) await this.provisioning;
    if (!refresh && this.validated && (dimension === undefined || dimension === this.dimension))
      return;
    this.validated = false;
    const pending = this.ensure(dimension);
    this.provisioning = pending;
    try {
      await pending;
      this.validated = !this.provision;
    } finally {
      this.provisioning = undefined;
    }
  }
  async embedQuery(query: string, options?: OperationOptions): Promise<number[]> {
    options ??= operationContext.getStore();
    options?.signal?.throwIfAborted();
    await this.ensureValidated();
    const vector =
      this.embeddings instanceof AntflyEmbeddings
        ? await this.embeddings.embedQuery(query, options)
        : await this.embeddings.embedQuery(query);
    options?.signal?.throwIfAborted();
    vectors([vector], 1, this.dimension);
    await this.ensureValidated(vector.length);
    return vector;
  }
  async addDocuments(
    documents: DocumentInterface[],
    options?: AntflyAddOptions
  ): Promise<string[]> {
    validateDocuments(documents);
    this.ids(documents, options?.ids);
    if (!documents.length) return [];
    options?.signal?.throwIfAborted();
    await this.ensureValidated(undefined, true);
    const size = this.options.batchSize ?? 64;
    const embedded: number[][] = [];
    for (let i = 0; i < documents.length; i += size) {
      options?.signal?.throwIfAborted();
      const texts = documents.slice(i, i + size).map((doc) => doc.pageContent);
      const value =
        this.embeddings instanceof AntflyEmbeddings
          ? await this.embeddings.embedDocuments(texts, options)
          : await this.embeddings.embedDocuments(texts);
      embedded.push(...vectors(value, texts.length, this.dimension ?? embedded[0]?.length));
    }
    return this.addVectors(embedded, documents, options);
  }
  private ids(documents: DocumentInterface[], supplied?: string[]): string[] {
    if (supplied && supplied.length !== documents.length)
      throw new TypeError("IDs must align with documents");
    const ids = documents.map((doc, i) => supplied?.[i] ?? doc.id ?? randomUUID());
    if (ids.some((id) => typeof id !== "string" || !id) || new Set(ids).size !== ids.length)
      throw new TypeError("IDs must be unique nonempty strings within a batch");
    return ids;
  }
  async addVectors(
    values: number[][],
    documents: DocumentInterface[],
    options?: AntflyAddOptions
  ): Promise<string[]> {
    validateDocuments(documents);
    const ids = this.ids(documents, options?.ids);
    vectors(values, documents.length, this.dimension);
    if (!documents.length) return [];
    options?.signal?.throwIfAborted();
    await this.ensureValidated(values[0].length, true);
    const rows: [string, unknown][] = documents.map((doc, i) => [
      ids[i],
      {
        [this.contentField]: doc.pageContent,
        [this.metadataField]: doc.metadata,
        _embeddings: { [this.index]: values[i] },
      },
    ]);
    await write(
      this.client,
      this.table,
      rows,
      [],
      this.options.batchSize ?? 64,
      this.options.wait ?? true,
      options
    );
    return ids;
  }
  async delete(options: { ids: string[]; signal?: AbortSignal }): Promise<void> {
    if (!Array.isArray(options?.ids) || options.ids.some((id) => typeof id !== "string" || !id))
      throw new TypeError("Explicit nonempty IDs are required; delete never drops a table");
    await write(
      this.client,
      this.table,
      [],
      [...new Set(options.ids)],
      this.options.batchSize ?? 64,
      this.options.wait ?? true,
      options
    );
  }
  async searchHits(
    vector: number[],
    k: number,
    filter?: AntflyFilter,
    includeVectors = false,
    options?: OperationOptions
  ): Promise<Record<string, unknown>[]> {
    options ??= operationContext.getStore();
    count(k, "k");
    if (!k) return [];
    options?.signal?.throwIfAborted();
    vectors([vector], 1, this.dimension);
    await this.ensureValidated(vector.length);
    const request: TableQueryRequest = {
      embeddings: { [this.index]: vector },
      indexes: [this.index],
      limit: k,
      fields: [this.contentField, this.metadataField, ...(includeVectors ? ["_embeddings"] : [])],
      filter_query: intersect(this.filter, filter),
    };
    try {
      return hits(await this.client.tables.query(this.table, request, options));
    } catch (error) {
      this.validated = false;
      throw error;
    }
  }
  async similaritySearchVectorWithScore(
    vector: number[],
    k: number,
    filter?: AntflyFilter
  ): Promise<[DocumentInterface, number][]> {
    const found = await this.searchHits(vector, k, filter);
    return found.map((hit) => {
      if (typeof hit._distance !== "number" || !Number.isFinite(hit._distance))
        throw new Error("Dense distance unavailable; hybrid scores are not distances");
      return [document(hit, this.contentField, this.metadataField), hit._distance];
    });
  }
  async similaritySearchWithScore(
    query: string,
    k = 4,
    filter?: AntflyFilter
  ): Promise<[DocumentInterface, number][]> {
    count(k, "k");
    if (!k) return [];
    return this.similaritySearchVectorWithScore(await this.embedQuery(query), k, filter);
  }
  async similaritySearch(
    query: string,
    k = 4,
    filter?: AntflyFilter
  ): Promise<DocumentInterface[]> {
    return (await this.similaritySearchWithScore(query, k, filter)).map(([doc]) => doc);
  }
  async getByIds(ids: string[]): Promise<DocumentInterface[]> {
    const result: DocumentInterface[] = [];
    if (!ids.length) return result;
    await this.client.tables.get(this.table);
    let missing = false;
    for (const id of new Set(ids)) {
      if (this.filter) {
        const found = hits(
          await this.client.tables.query(this.table, {
            limit: 1,
            fields: [this.contentField, this.metadataField],
            filter_query: intersect(this.filter, { ids: [id] }),
          })
        );
        result.push(...found.map((hit) => document(hit, this.contentField, this.metadataField)));
      } else {
        try {
          const source = await this.client.tables.lookup(this.table, id, {
            fields: `${this.contentField},${this.metadataField}`,
          });
          result.push(
            document({ _id: id, _source: source }, this.contentField, this.metadataField)
          );
        } catch (error) {
          if (!isStatus(error, 404)) throw error;
          missing = true;
        }
      }
    }
    if (missing) await this.client.tables.get(this.table);
    return result;
  }
  async maxMarginalRelevanceSearch(
    query: string,
    options: MaxMarginalRelevanceSearchOptions<AntflyFilter>
  ): Promise<DocumentInterface[]> {
    const k = count(options.k, "k");
    const fetchK = count(options.fetchK ?? Math.max(20, k), "fetchK");
    const lambda = options.lambda ?? 0.5;
    if (fetchK < k || !Number.isFinite(lambda) || lambda < 0 || lambda > 1)
      throw new TypeError("Require fetchK >= k and 0 <= lambda <= 1");
    if (!k) return [];
    const vector = await this.embedQuery(query);
    const found = await this.searchHits(vector, fetchK, options.filter, true);
    const candidates: number[][] = [];
    for (const hit of found) {
      let source = record(hit._source);
      if (!source._embeddings)
        source = record(
          await this.client.tables.lookup(this.table, String(hit._id), { fields: "_embeddings" })
        );
      const value = record(source._embeddings, "stored embeddings")[this.index] as number[];
      vectors([value], 1, vector.length);
      candidates.push(value);
    }
    const cosine = (a: number[], b: number[]) => {
      const norm = Math.hypot(...a) * Math.hypot(...b);
      return norm ? a.reduce((sum, value, i) => sum + value * b[i], 0) / norm : 0;
    };
    const selected: number[] = [];
    while (selected.length < Math.min(k, found.length)) {
      let best = -1;
      let bestScore = -Infinity;
      for (let i = 0; i < candidates.length; i++) {
        if (selected.includes(i)) continue;
        const relevance = cosine(vector, candidates[i]);
        const score = selected.length
          ? lambda * relevance -
            (1 - lambda) * Math.max(...selected.map((j) => cosine(candidates[i], candidates[j])))
          : relevance;
        if (score > bestScore) {
          best = i;
          bestScore = score;
        }
      }
      selected.push(best);
    }
    return selected.map((i) => document(found[i], this.contentField, this.metadataField));
  }
  static async fromDocuments(
    documents: DocumentInterface[],
    embeddings: EmbeddingsInterface,
    options: AntflyVectorStoreOptions & AntflyAddOptions
  ): Promise<AntflyVectorStore> {
    const store = new AntflyVectorStore(embeddings, options);
    validateDocuments(documents);
    store.ids(documents, options.ids);
    if (documents.length) {
      try {
        await store.client.tables.get(store.table);
      } catch (error) {
        if (!isStatus(error, 404)) throw error;
        try {
          await store.client.tables.create(store.table);
        } catch (race) {
          if (!isStatus(race, 409)) throw race;
        }
      }
    }
    store.provision = true;
    await store.addDocuments(documents, options);
    return store;
  }
  static async fromTexts(
    texts: string[],
    metadatas: Record<string, unknown>[] | Record<string, unknown>,
    embeddings: EmbeddingsInterface,
    options: AntflyVectorStoreOptions & AntflyAddOptions
  ): Promise<AntflyVectorStore> {
    if (Array.isArray(metadatas) && metadatas.length !== texts.length)
      throw new TypeError("Metadata must align with texts");
    return AntflyVectorStore.fromDocuments(
      texts.map((pageContent, i) => ({
        pageContent,
        metadata: Array.isArray(metadatas) ? metadatas[i] : metadatas,
      })),
      embeddings,
      options
    );
  }
}
