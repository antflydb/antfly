import type {
  ArtifactIndexSource,
  CreateIndexRequest,
  EmbedderConfig,
  EmbeddingsIndexConfig,
  EnrichmentConfig,
  GraphIndexSource,
  IndexConfig,
} from "./types.js";

const MAX_ARTIFACT_SOURCES = 64;

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function relationshipFieldActive(value: unknown): boolean {
  return value !== undefined && value !== null && value !== false;
}

/**
 * Enforces the cross-field request relationships published through Antfly's
 * OpenAPI vendor extensions. The server remains authoritative, while this
 * keeps direct SDK calls from paying for a guaranteed failed request.
 */
export function validateCreateIndexRequestRelationships(
  config: CreateIndexRequest | IndexConfig | Record<string, unknown>
): void {
  if (!isRecord(config)) throw new TypeError("index config must be an object");
  const object = config as Record<string, unknown>;
  const hasSources = relationshipFieldActive(object.sources);
  if (object.type === "full_text" && hasSources && relationshipFieldActive(object.artifact_name)) {
    throw new TypeError("Index sources cannot be combined with artifact_name.");
  }
  if (object.type === "graph" && hasSources && relationshipFieldActive(object.source)) {
    throw new TypeError("Index sources cannot be combined with source.");
  }
  if (object.type !== "embeddings") return;

  if (hasSources) {
    for (const field of [
      "external",
      "field",
      "template",
      "chunker",
      "embedding_name",
      "source_artifact_name",
    ] as const) {
      if (relationshipFieldActive(object[field])) {
        throw new TypeError(`Index sources cannot be combined with ${field}.`);
      }
    }
  }
  if (
    relationshipFieldActive(object.source_artifact_name) &&
    !relationshipFieldActive(object.embedding_name)
  ) {
    throw new TypeError("Embedding source_artifact_name requires a non-empty embedding_name.");
  }
  if (
    typeof object.embedding_name === "string" &&
    typeof object.source_artifact_name === "string" &&
    Array.isArray(object.enrichments)
  ) {
    const enrichment = object.enrichments.find(
      (candidate) =>
        isRecord(candidate) &&
        candidate.kind === "embedding" &&
        candidate.name === object.embedding_name
    );
    if (isRecord(enrichment) && enrichment.source_artifact_name !== object.source_artifact_name) {
      throw new TypeError(
        "Embedding source_artifact_name must match the authoritative embedding enrichment."
      );
    }
  }
}

function validateRequiredString(value: unknown, path: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${path} must be a non-empty string`);
  }
}

function validateOptionalString(value: unknown, path: string): asserts value is string | undefined {
  if (value !== undefined && typeof value !== "string") {
    throw new TypeError(`${path} must be a string`);
  }
}

/** Returns whether a graph source-owner template is stable across replay and deletion. */
export function isValidGraphMaterializedSourceTemplate(value: string): boolean {
  return /^\{\{\s*(?:_doc\.key|_artifact\.value(?:\.[A-Za-z0-9_]+)*)\s*\}\}$/.test(value.trim());
}

function validateOnlyKeys(
  value: Record<string, unknown>,
  allowed: readonly string[],
  path: string
): void {
  for (const key of Object.keys(value)) {
    if (!allowed.includes(key)) throw new TypeError(`${path}.${key} is not supported`);
  }
}

function cloneJsonValue(value: unknown, path: string): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError(`${path} must be finite`);
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item, index) => cloneJsonValue(item, `${path}[${index}]`));
  }
  if (typeof value === "object") {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(`${path} must contain only JSON values`);
    }
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, cloneJsonValue(item, `${path}.${key}`)])
    );
  }
  throw new TypeError(`${path} must contain only JSON values`);
}

function validateArtifactNames(
  artifacts: readonly unknown[]
): asserts artifacts is readonly string[] {
  if (artifacts.length === 0) throw new TypeError("at least one artifact source is required");
  if (artifacts.length > MAX_ARTIFACT_SOURCES) {
    throw new RangeError(`at most ${MAX_ARTIFACT_SOURCES} artifact sources are allowed`);
  }
  const seen = new Set<string>();
  artifacts.forEach((artifact, index) => {
    validateRequiredString(artifact, `artifacts[${index}]`);
    if (seen.has(artifact))
      throw new TypeError(`duplicate artifact source ${JSON.stringify(artifact)}`);
    seen.add(artifact);
  });
}

/** Builds the shared artifact-only source shape used by full-text and vector indexes. */
export function artifactIndexSources(...artifacts: string[]): ArtifactIndexSource[] {
  validateArtifactNames(artifacts);
  return artifacts.map((artifact) => ({ artifact }));
}

export interface FullTextArtifactSourceConfig {
  artifact: string;
  /** Source-local projection; overrides the index-level field for this stream. */
  field?: string;
}

/** Validates and copies full-text sources with optional source-local fields. */
export function fullTextArtifactIndexSources(
  ...sources: FullTextArtifactSourceConfig[]
): FullTextArtifactSourceConfig[] {
  sources.forEach((source, index) => {
    if (!isRecord(source)) throw new TypeError(`sources[${index}] must be an object`);
  });
  validateArtifactNames(sources.map((source) => source.artifact));
  return sources.map((source, index) => {
    validateOnlyKeys(
      source as unknown as Record<string, unknown>,
      ["artifact", "field"],
      `sources[${index}]`
    );
    validateOptionalString(source.field, `sources[${index}].field`);
    const field = source.field?.trim();
    if (source.field !== undefined && !field) {
      throw new TypeError(`sources[${index}].field must not be empty`);
    }
    return { artifact: source.artifact, ...(field ? { field } : {}) };
  });
}

export interface ArtifactFullTextIndexOptions {
  /** Artifact-only convenience form. Mutually exclusive with sources. */
  artifacts?: readonly string[];
  /** Artifact streams with optional source-local field projections. */
  sources?: readonly FullTextArtifactSourceConfig[];
  /** Optional content field inherited by sources that do not select one. */
  field?: string;
  memOnly?: boolean;
}

/** Builds a full-text index over generated chunk or textual asset streams. */
export function artifactFullTextIndexConfig(
  name: string,
  options: ArtifactFullTextIndexOptions
): IndexConfig;
/** Convenience overload for whole-artifact projection. */
export function artifactFullTextIndexConfig(name: string, ...artifacts: string[]): IndexConfig;
export function artifactFullTextIndexConfig(
  name: string,
  ...args: [ArtifactFullTextIndexOptions] | string[]
): IndexConfig {
  validateRequiredString(name, "index name");
  const options: ArtifactFullTextIndexOptions =
    args.length === 1 && isRecord(args[0])
      ? (args[0] as unknown as ArtifactFullTextIndexOptions)
      : { artifacts: args as string[] };
  validateOnlyKeys(
    options as unknown as Record<string, unknown>,
    ["artifacts", "sources", "field", "memOnly"],
    "options"
  );
  if (options.artifacts !== undefined && options.sources !== undefined) {
    throw new TypeError("artifacts and sources are mutually exclusive");
  }
  if (options.sources !== undefined && !Array.isArray(options.sources)) {
    throw new TypeError("sources must be an array");
  }
  const sources = Array.isArray(options.sources)
    ? fullTextArtifactIndexSources(...options.sources)
    : Array.isArray(options.artifacts)
      ? artifactIndexSources(...options.artifacts)
      : (() => {
          throw new TypeError("artifacts or sources must be an array");
        })();
  validateOptionalString(options.field, "field");
  if (options.memOnly !== undefined && typeof options.memOnly !== "boolean") {
    throw new TypeError("memOnly must be a boolean");
  }
  const field = options.field?.trim();
  if (options.field !== undefined && !field) throw new TypeError("field must not be empty");
  return {
    name,
    type: "full_text",
    sources,
    ...(field ? { field } : {}),
    ...(options.memOnly ? { mem_only: true } : {}),
  };
}

/**
 * Validates and copies ordered graph sources. Earlier sources win when more
 * than one source materializes the same edge identity.
 */
export function graphIndexSources(...sources: GraphIndexSource[]): GraphIndexSource[] {
  sources.forEach((source, index) => {
    if (!isRecord(source)) throw new TypeError(`sources[${index}] must be an object`);
    validateOnlyKeys(
      source,
      ["artifact", "path", "format", "mention_edge_type", "nodes", "edge", "context"],
      `sources[${index}]`
    );
  });
  validateArtifactNames(sources.map((source) => source.artifact));
  sources.forEach((source, index) => {
    validateOptionalString(source.path, `sources[${index}].path`);
    if (
      source.path !== undefined &&
      !/^(\$|\$\.[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*(\[\*\])?)?$/.test(source.path)
    ) {
      throw new TypeError(
        `sources[${index}].path must be $, a dot-separated field path, or end in [*]`
      );
    }
    if (
      source.format !== undefined &&
      source.format !== "extraction_relation" &&
      source.format !== "extraction_graph"
    ) {
      throw new TypeError(`sources[${index}].format is invalid`);
    }
    validateOptionalString(source.mention_edge_type, `sources[${index}].mention_edge_type`);
    if (source.nodes !== undefined) {
      if (!isRecord(source.nodes)) throw new TypeError(`sources[${index}].nodes must be an object`);
      validateOnlyKeys(source.nodes, ["model", "source", "target"], `sources[${index}].nodes`);
    }
    if (
      source.nodes?.model !== undefined &&
      source.nodes.model !== "document" &&
      source.nodes.model !== "external"
    ) {
      throw new TypeError(`sources[${index}].nodes.model is invalid`);
    }
    if (source.edge !== undefined) {
      if (!isRecord(source.edge)) throw new TypeError(`sources[${index}].edge must be an object`);
      validateOnlyKeys(source.edge, ["type", "weight", "metadata"], `sources[${index}].edge`);
    }
    if (
      source.nodes?.source !== undefined &&
      (typeof source.nodes.source !== "string" ||
        !isValidGraphMaterializedSourceTemplate(source.nodes.source))
    ) {
      throw new TypeError(`sources[${index}].nodes.source must use _doc.key or _artifact.value`);
    }
    for (const [fieldName, value] of [["target", source.nodes?.target]] as const) {
      if (
        value !== undefined &&
        typeof value !== "string" &&
        (typeof value !== "number" || !Number.isFinite(value))
      ) {
        throw new TypeError(
          `sources[${index}].nodes.${fieldName} must be a string or finite number`
        );
      }
    }
    for (const [fieldName, value] of [
      ["type", source.edge?.type],
      ["weight", source.edge?.weight],
    ] as const) {
      if (
        value !== undefined &&
        typeof value !== "string" &&
        (typeof value !== "number" || !Number.isFinite(value))
      ) {
        throw new TypeError(
          `sources[${index}].edge.${fieldName} must be a string or finite number`
        );
      }
    }
    if (source.edge?.metadata !== undefined && !isRecord(source.edge.metadata)) {
      throw new TypeError(`sources[${index}].edge.metadata must be an object`);
    }
    if (source.context !== undefined) {
      if (!isRecord(source.context)) {
        throw new TypeError(`sources[${index}].context must be an object`);
      }
      validateOnlyKeys(source.context, ["doc_fields"], `sources[${index}].context`);
    }
    const docFields = source.context?.doc_fields;
    if (docFields !== undefined) {
      if (!Array.isArray(docFields)) {
        throw new TypeError(`sources[${index}].context.doc_fields must be an array`);
      }
      if (docFields.some((field) => typeof field !== "string" || field.length === 0)) {
        throw new TypeError(
          `sources[${index}].context.doc_fields entries must be non-empty strings`
        );
      }
      if (new Set(docFields).size !== docFields.length) {
        throw new TypeError(`sources[${index}].context.doc_fields must be unique`);
      }
    }
  });
  return sources.map((source) => ({
    ...source,
    nodes: source.nodes === undefined ? undefined : { ...source.nodes },
    edge:
      source.edge === undefined
        ? undefined
        : {
            ...source.edge,
            metadata:
              source.edge.metadata === undefined
                ? undefined
                : (cloneJsonValue(
                    source.edge.metadata,
                    "graph source edge metadata"
                  ) as NonNullable<GraphIndexSource["edge"]>["metadata"]),
          },
    context:
      source.context === undefined
        ? undefined
        : {
            ...source.context,
            doc_fields:
              source.context.doc_fields === undefined ? undefined : [...source.context.doc_fields],
          },
  }));
}

export interface ArtifactEmbeddingSourceConfig {
  /** Stable name of the generated embedding artifact. */
  artifact: string;
  /** Optional upstream chunk artifact consumed by this embedding enrichment. */
  sourceArtifact?: string;
  /** Source text field. Defaults to `text`. */
  field?: string;
  /** Optional producer template; mutually exclusive with an empty field. */
  template?: string;
}

interface ArtifactEmbeddingIndexOptionsBase {
  sources: ArtifactEmbeddingSourceConfig[];
  embedder: EmbedderConfig;
}

export type ArtifactEmbeddingIndexOptions = ArtifactEmbeddingIndexOptionsBase &
  (
    | {
        /** Dense is the default. Omit dimension when the server can probe it. */
        sparse?: false;
        dimension?: number;
        distanceMetric?: NonNullable<EmbeddingsIndexConfig["distance_metric"]>;
      }
    | {
        /** Sparse indexes reject dense-only dimension and distance settings at compile time. */
        sparse: true;
        dimension?: never;
        distanceMetric?: never;
      }
  );

interface RuntimeArtifactEmbeddingIndexOptions extends ArtifactEmbeddingIndexOptionsBase {
  dimension?: number;
  sparse?: boolean;
  distanceMetric?: NonNullable<EmbeddingsIndexConfig["distance_metric"]>;
}

/**
 * Builds a multi-source vector index and its table-level embedding
 * enrichments through one typed SDK path.
 */
export function artifactEmbeddingIndexConfig(
  name: string,
  options: ArtifactEmbeddingIndexOptions
): IndexConfig {
  // Keep runtime checks for JavaScript consumers and values crossing an
  // untyped boundary even though TypeScript callers get a discriminated union.
  validateRequiredString(name, "index name");
  if (!isRecord(options)) throw new TypeError("options must be an object");
  validateOnlyKeys(
    options,
    ["sources", "embedder", "dimension", "sparse", "distanceMetric"],
    "options"
  );
  const runtimeOptions: RuntimeArtifactEmbeddingIndexOptions = options;
  if (!Array.isArray(runtimeOptions.sources)) throw new TypeError("sources must be an array");
  runtimeOptions.sources.forEach((source, index) => {
    if (!isRecord(source)) throw new TypeError(`sources[${index}] must be an object`);
    validateOnlyKeys(
      source,
      ["artifact", "sourceArtifact", "field", "template"],
      `sources[${index}]`
    );
    validateOptionalString(source.sourceArtifact, `sources[${index}].sourceArtifact`);
    validateOptionalString(source.field, `sources[${index}].field`);
    validateOptionalString(source.template, `sources[${index}].template`);
  });
  validateArtifactNames(runtimeOptions.sources.map((source) => source.artifact));
  if (runtimeOptions.sparse !== undefined && typeof runtimeOptions.sparse !== "boolean") {
    throw new TypeError("sparse must be a boolean");
  }
  if (runtimeOptions.sparse && runtimeOptions.dimension !== undefined) {
    throw new TypeError("dimension must be omitted for sparse embedding indexes");
  }
  if (runtimeOptions.sparse && runtimeOptions.distanceMetric !== undefined) {
    throw new TypeError("distanceMetric must be omitted for sparse embedding indexes");
  }
  if (
    runtimeOptions.dimension !== undefined &&
    (!Number.isInteger(runtimeOptions.dimension) || runtimeOptions.dimension <= 0)
  ) {
    throw new RangeError("dimension must be a positive integer");
  }
  if (!isRecord(runtimeOptions.embedder)) throw new TypeError("embedder must be an object");
  if (
    typeof runtimeOptions.embedder.provider !== "string" ||
    runtimeOptions.embedder.provider.length === 0
  ) {
    throw new TypeError("embedder.provider is required");
  }
  if (
    runtimeOptions.distanceMetric !== undefined &&
    runtimeOptions.distanceMetric !== "l2_squared" &&
    runtimeOptions.distanceMetric !== "inner_product" &&
    runtimeOptions.distanceMetric !== "cosine"
  ) {
    throw new TypeError("distanceMetric is invalid");
  }

  const sources = artifactIndexSources(...runtimeOptions.sources.map((source) => source.artifact));
  const enrichments: EnrichmentConfig[] = runtimeOptions.sources.map((source, index) => {
    const field = (source.template?.length ?? 0) > 0 ? "" : (source.field ?? "text");
    if (field.length === 0 && (source.template?.length ?? 0) === 0) {
      throw new TypeError(`sources[${index}] requires field or template`);
    }
    if (source.sourceArtifact === "") {
      throw new TypeError(`sources[${index}].sourceArtifact cannot be empty`);
    }
    return {
      name: source.artifact,
      kind: "embedding",
      ...(field.length > 0 ? { field } : {}),
      ...(source.template !== undefined ? { template: source.template } : {}),
      ...(source.sourceArtifact !== undefined
        ? { source_artifact_name: source.sourceArtifact }
        : {}),
      ...(runtimeOptions.dimension !== undefined
        ? { expected_dims: runtimeOptions.dimension }
        : {}),
    };
  });

  return {
    name,
    type: "embeddings",
    sources,
    enrichments,
    embedder: runtimeOptions.embedder,
    ...(runtimeOptions.sparse ? { sparse: true } : {}),
    ...(runtimeOptions.dimension !== undefined ? { dimension: runtimeOptions.dimension } : {}),
    ...(runtimeOptions.distanceMetric !== undefined
      ? { distance_metric: runtimeOptions.distanceMetric }
      : {}),
  };
}
