import {
  graphIndexSources,
  type IndexConfig,
  validateCreateIndexRequestRelationships,
} from "@antfly/sdk";

export function parseAdvancedIndexConfig(source: string): IndexConfig {
  let value: unknown;
  try {
    value = JSON.parse(source);
  } catch (error) {
    throw new Error(error instanceof Error ? `Invalid JSON: ${error.message}` : "Invalid JSON.");
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Index configuration must be a JSON object.");
  }
  const config = value as Record<string, unknown>;
  if (typeof config.name !== "string" || !config.name.trim()) {
    throw new Error("Index configuration requires a non-empty name.");
  }
  if (config.type !== "embeddings" && config.type !== "full_text" && config.type !== "graph") {
    throw new Error('Index configuration type must be "embeddings", "full_text", or "graph".');
  }
  if (config.sources !== undefined) {
    if (!Array.isArray(config.sources)) {
      throw new Error("Index sources must be an array.");
    }
    if (config.sources.length === 0 || config.sources.length > 64) {
      throw new Error("Index sources must contain between 1 and 64 items.");
    }
    if (config.type === "graph") {
      graphIndexSources(...(config.sources as Parameters<typeof graphIndexSources>));
    } else {
      const seen = new Set<string>();
      const allowedKeys =
        config.type === "full_text" ? new Set(["artifact", "field"]) : new Set(["artifact"]);
      config.sources.forEach((source, index) => {
        if (!source || typeof source !== "object" || Array.isArray(source)) {
          throw new Error(`Index sources[${index}] must be an object.`);
        }
        const keys = Object.keys(source);
        const unsupportedKey = keys.find((key) => !allowedKeys.has(key));
        if (unsupportedKey !== undefined) {
          throw new Error(
            `Index sources[${index}] does not support ${JSON.stringify(unsupportedKey)}.`
          );
        }
        const sourceConfig = source as Record<string, unknown>;
        const artifact = sourceConfig.artifact;
        if (typeof artifact !== "string" || !artifact.trim()) {
          throw new Error(`Index sources[${index}].artifact must be a non-empty string.`);
        }
        if (
          config.type === "full_text" &&
          sourceConfig.field !== undefined &&
          (typeof sourceConfig.field !== "string" || !sourceConfig.field.trim())
        ) {
          throw new Error(`Index sources[${index}].field must be a non-empty string.`);
        }
        if (seen.has(artifact)) {
          throw new Error(`Index sources contains duplicate artifact ${JSON.stringify(artifact)}.`);
        }
        seen.add(artifact);
      });
    }
  }
  if (config.type === "embeddings") {
    for (const field of ["embedding_name", "source_artifact_name"] as const) {
      if (
        config[field] !== undefined &&
        (typeof config[field] !== "string" || !config[field].trim())
      ) {
        throw new Error(`Embedding ${field} must be a non-empty string.`);
      }
    }
  }
  validateCreateIndexRequestRelationships(config);
  return value as IndexConfig;
}

export function usesArtifactBackedIndexSource(config: IndexConfig): boolean {
  const raw = config as unknown as Record<string, unknown>;
  if (Array.isArray(raw.sources) && raw.sources.length > 0) return true;

  switch (config.type) {
    case "full_text":
      return typeof raw.artifact_name === "string" && raw.artifact_name.trim().length > 0;
    case "embeddings":
      return (
        (typeof raw.embedding_name === "string" && raw.embedding_name.trim().length > 0) ||
        (typeof raw.source_artifact_name === "string" && raw.source_artifact_name.trim().length > 0)
      );
    case "graph":
      return raw.source !== null && typeof raw.source === "object" && !Array.isArray(raw.source);
    default:
      return false;
  }
}
