import { graphIndexSources, type IndexConfig } from "@antfly/sdk";

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
      config.sources.forEach((source, index) => {
        if (!source || typeof source !== "object" || Array.isArray(source)) {
          throw new Error(`Index sources[${index}] must be an object.`);
        }
        const keys = Object.keys(source);
        if (keys.some((key) => key !== "artifact")) {
          throw new Error(`Index sources[${index}] only supports artifact.`);
        }
        const artifact = (source as Record<string, unknown>).artifact;
        if (typeof artifact !== "string" || !artifact.trim()) {
          throw new Error(`Index sources[${index}].artifact must be a non-empty string.`);
        }
        if (seen.has(artifact)) {
          throw new Error(`Index sources contains duplicate artifact ${JSON.stringify(artifact)}.`);
        }
        seen.add(artifact);
      });
    }
  }
  const mutuallyExclusiveWithSources =
    config.type === "full_text"
      ? ["artifact_name"]
      : config.type === "graph"
        ? ["source"]
        : ["external", "field", "template", "chunker", "embedding_name", "source_artifact_name"];
  if (config.sources !== undefined) {
    const conflict = mutuallyExclusiveWithSources.find((field) => config[field] !== undefined);
    if (conflict) {
      throw new Error(`Index sources cannot be combined with ${conflict}.`);
    }
  }
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
