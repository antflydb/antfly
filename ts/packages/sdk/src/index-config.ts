import type {
  ArtifactIndexSource,
  EmbeddingsIndexConfig,
  EnrichmentConfig,
  GraphIndexSource,
  IndexConfig,
  EmbedderConfig,
} from "./types.js";

const MAX_ARTIFACT_SOURCES = 64;

function validateArtifactNames(artifacts: readonly string[]): void {
  if (artifacts.length === 0) throw new TypeError("at least one artifact source is required");
  if (artifacts.length > MAX_ARTIFACT_SOURCES) {
    throw new RangeError(`at most ${MAX_ARTIFACT_SOURCES} artifact sources are allowed`);
  }
  const seen = new Set<string>();
  artifacts.forEach((artifact, index) => {
    if (artifact.length === 0) throw new TypeError(`artifacts[${index}] is required`);
    if (seen.has(artifact)) throw new TypeError(`duplicate artifact source ${JSON.stringify(artifact)}`);
    seen.add(artifact);
  });
}

/** Builds the shared artifact-only source shape used by full-text and vector indexes. */
export function artifactIndexSources(...artifacts: string[]): ArtifactIndexSource[] {
  validateArtifactNames(artifacts);
  return artifacts.map((artifact) => ({ artifact }));
}

/** Validates and copies graph sources, preserving per-source path and format. */
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
  });
  return sources.map((source) => ({ ...source }));
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

export interface ArtifactEmbeddingIndexOptions {
  sources: ArtifactEmbeddingSourceConfig[];
  embedder: EmbedderConfig;
  /** Dense dimension. Omit when the server can probe it; must be omitted for sparse indexes. */
  dimension?: number;
  sparse?: boolean;
  distanceMetric?: NonNullable<EmbeddingsIndexConfig["distance_metric"]>;
  /**
   * Optional compatibility assertion. When omitted, Antfly requires semantic
   * equivalence of the effective producer models.
   */
  vectorSpace?: string;
}

/**
 * Builds a multi-source vector index and its table-level embedding
 * enrichments through one typed SDK path.
 */
export function artifactEmbeddingIndexConfig(
  name: string,
  options: ArtifactEmbeddingIndexOptions
): IndexConfig {
  if (name.length === 0) throw new TypeError("index name is required");
  validateArtifactNames(options.sources.map((source) => source.artifact));
  if (options.sparse && options.dimension !== undefined) {
    throw new TypeError("dimension must be omitted for sparse embedding indexes");
  }
  if (options.sparse && options.distanceMetric !== undefined) {
    throw new TypeError("distanceMetric must be omitted for sparse embedding indexes");
  }
  if (options.vectorSpace !== undefined && options.vectorSpace.length === 0) {
    throw new TypeError("vectorSpace cannot be empty when provided");
  }
  if (options.dimension !== undefined && (!Number.isInteger(options.dimension) || options.dimension <= 0)) {
    throw new RangeError("dimension must be a positive integer");
  }
  if (
    typeof (options.embedder as { provider?: unknown }).provider !== "string" ||
    (options.embedder as { provider: string }).provider.length === 0
  ) {
    throw new TypeError("embedder.provider is required");
  }
  if (
    options.distanceMetric !== undefined &&
    options.distanceMetric !== "l2_squared" &&
    options.distanceMetric !== "inner_product" &&
    options.distanceMetric !== "cosine"
  ) {
    throw new TypeError("distanceMetric is invalid");
  }

  const sources = artifactIndexSources(...options.sources.map((source) => source.artifact));
  const enrichments: EnrichmentConfig[] = options.sources.map((source, index) => {
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
      ...(options.dimension !== undefined ? { expected_dims: options.dimension } : {}),
      ...(options.vectorSpace !== undefined ? { vector_space: options.vectorSpace } : {}),
    };
  });

  return {
    name,
    type: "embeddings",
    sources,
    enrichments,
    embedder: options.embedder,
    ...(options.sparse ? { sparse: true } : {}),
    ...(options.dimension !== undefined ? { dimension: options.dimension } : {}),
    ...(options.distanceMetric !== undefined ? { distance_metric: options.distanceMetric } : {}),
  };
}
