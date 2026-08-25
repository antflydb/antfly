import type {
  ArtifactIndexSource,
  EmbedderConfig,
  EmbeddingsIndexConfig,
  EnrichmentConfig,
  GraphIndexSource,
  IndexConfig,
} from "./types.js";

const MAX_ARTIFACT_SOURCES = 64;

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

function validateArtifactNames(artifacts: readonly string[]): void {
  if (artifacts.length === 0) throw new TypeError("at least one artifact source is required");
  if (artifacts.length > MAX_ARTIFACT_SOURCES) {
    throw new RangeError(`at most ${MAX_ARTIFACT_SOURCES} artifact sources are allowed`);
  }
  const seen = new Set<string>();
  artifacts.forEach((artifact, index) => {
    if (artifact.length === 0) throw new TypeError(`artifacts[${index}] is required`);
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

/** Builds a full-text index over generated chunk or textual asset streams. */
export function artifactFullTextIndexConfig(name: string, ...artifacts: string[]): IndexConfig {
  if (name.length === 0) throw new TypeError("index name is required");
  return { name, type: "full_text", sources: artifactIndexSources(...artifacts) };
}

/**
 * Validates and copies ordered graph sources. Earlier sources win when more
 * than one source materializes the same edge identity.
 */
export function graphIndexSources(...sources: GraphIndexSource[]): GraphIndexSource[] {
  validateArtifactNames(sources.map((source) => source.artifact));
  sources.forEach((source, index) => {
    if (
      source.format !== undefined &&
      source.format !== "extraction_relation" &&
      source.format !== "extraction_graph"
    ) {
      throw new TypeError(`sources[${index}].format is invalid`);
    }
    if (
      source.nodes?.model !== undefined &&
      source.nodes.model !== "document" &&
      source.nodes.model !== "external"
    ) {
      throw new TypeError(`sources[${index}].nodes.model is invalid`);
    }
    for (const [fieldName, value] of [
      ["type", source.edge?.type],
      ["weight", source.edge?.weight],
    ] as const) {
      if (typeof value === "number" && !Number.isFinite(value)) {
        throw new TypeError(`sources[${index}].edge.${fieldName} must be finite`);
      }
    }
    const docFields = source.context?.doc_fields;
    if (docFields !== undefined) {
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
  const runtimeOptions: RuntimeArtifactEmbeddingIndexOptions = options;
  if (name.length === 0) throw new TypeError("index name is required");
  validateArtifactNames(runtimeOptions.sources.map((source) => source.artifact));
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
  if (
    typeof (runtimeOptions.embedder as { provider?: unknown }).provider !== "string" ||
    (runtimeOptions.embedder as { provider: string }).provider.length === 0
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
    const field = source.field ?? "text";
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
      ...(runtimeOptions.dimension !== undefined ? { expected_dims: runtimeOptions.dimension } : {}),
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
