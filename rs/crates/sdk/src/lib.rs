#![allow(clippy::all)]

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const MAX_ARTIFACT_SOURCES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConfigError(pub String);

impl std::fmt::Display for IndexConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for IndexConfigError {}

fn validate_artifact_names<'a>(
    artifacts: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<&'a str>, IndexConfigError> {
    let artifacts: Vec<_> = artifacts.into_iter().collect();
    if artifacts.is_empty() {
        return Err(IndexConfigError(
            "at least one artifact source is required".into(),
        ));
    }
    if artifacts.len() > MAX_ARTIFACT_SOURCES {
        return Err(IndexConfigError(format!(
            "at most {MAX_ARTIFACT_SOURCES} artifact sources are allowed"
        )));
    }
    let mut seen = std::collections::HashSet::with_capacity(artifacts.len());
    for (index, artifact) in artifacts.iter().enumerate() {
        if artifact.is_empty() {
            return Err(IndexConfigError(format!("artifacts[{index}] is required")));
        }
        if !seen.insert(*artifact) {
            return Err(IndexConfigError(format!(
                "duplicate artifact source {artifact:?}"
            )));
        }
    }
    Ok(artifacts)
}

pub fn artifact_index_sources<'a>(
    artifacts: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<Value>, IndexConfigError> {
    Ok(validate_artifact_names(artifacts)?
        .into_iter()
        .map(|artifact| json!({ "artifact": artifact }))
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphIndexSourceSpec {
    pub artifact: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<GraphArtifactFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mention_edge_type: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphArtifactFormat {
    ExtractionRelation,
    ExtractionGraph,
}

pub fn graph_index_sources(
    sources: Vec<GraphIndexSourceSpec>,
) -> Result<Vec<GraphIndexSourceSpec>, IndexConfigError> {
    validate_artifact_names(sources.iter().map(|source| source.artifact.as_str()))?;
    Ok(sources)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactEmbeddingSourceSpec {
    pub artifact: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_artifact: Option<String>,
    #[serde(default = "default_embedding_source_field")]
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
}

fn default_embedding_source_field() -> String {
    "text".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArtifactEmbeddingIndexOptions {
    pub sources: Vec<ArtifactEmbeddingSourceSpec>,
    pub embedder: Value,
    pub dimension: Option<u32>,
    #[serde(default)]
    pub sparse: bool,
    pub distance_metric: Option<String>,
    pub vector_space: Option<String>,
}

pub fn artifact_embedding_index_config(
    name: &str,
    options: ArtifactEmbeddingIndexOptions,
) -> Result<Value, IndexConfigError> {
    if name.is_empty() {
        return Err(IndexConfigError("index name is required".into()));
    }
    validate_artifact_names(
        options
            .sources
            .iter()
            .map(|source| source.artifact.as_str()),
    )?;
    if options.sparse && options.dimension.is_some() {
        return Err(IndexConfigError(
            "dimension must be omitted for sparse embedding indexes".into(),
        ));
    }
    if options.sparse && options.distance_metric.is_some() {
        return Err(IndexConfigError(
            "distance_metric must be omitted for sparse embedding indexes".into(),
        ));
    }
    if options.dimension == Some(0) {
        return Err(IndexConfigError(
            "dimension must be positive when provided".into(),
        ));
    }
    if !matches!(
        options.distance_metric.as_deref(),
        None | Some("l2_squared" | "inner_product" | "cosine")
    ) {
        return Err(IndexConfigError("distance_metric is invalid".into()));
    }
    if options
        .embedder
        .get("provider")
        .and_then(Value::as_str)
        .map_or(true, str::is_empty)
    {
        return Err(IndexConfigError("embedder.provider is required".into()));
    }
    if options.vector_space.as_deref() == Some("") {
        return Err(IndexConfigError(
            "vector_space cannot be empty when provided".into(),
        ));
    }

    let mut enrichments = Vec::with_capacity(options.sources.len());
    for (index, source) in options.sources.iter().enumerate() {
        if source.field.is_empty() && source.template.as_deref().unwrap_or_default().is_empty() {
            return Err(IndexConfigError(format!(
                "sources[{index}] requires field or template"
            )));
        }
        if source.source_artifact.as_deref() == Some("") {
            return Err(IndexConfigError(format!(
                "sources[{index}].source_artifact cannot be empty"
            )));
        }
        let mut enrichment = serde_json::Map::new();
        enrichment.insert("name".into(), json!(source.artifact));
        enrichment.insert("kind".into(), json!("embedding"));
        if !source.field.is_empty() {
            enrichment.insert("field".into(), json!(source.field));
        }
        if let Some(template) = &source.template {
            enrichment.insert("template".into(), json!(template));
        }
        if let Some(source_artifact) = &source.source_artifact {
            enrichment.insert("source_artifact_name".into(), json!(source_artifact));
        }
        if let Some(dimension) = options.dimension {
            enrichment.insert("expected_dims".into(), json!(dimension));
        }
        if let Some(vector_space) = &options.vector_space {
            enrichment.insert("vector_space".into(), json!(vector_space));
        }
        enrichments.push(Value::Object(enrichment));
    }

    let mut config = serde_json::Map::new();
    config.insert("name".into(), json!(name));
    config.insert("type".into(), json!("embeddings"));
    config.insert(
        "sources".into(),
        Value::Array(artifact_index_sources(
            options
                .sources
                .iter()
                .map(|source| source.artifact.as_str()),
        )?),
    );
    config.insert("enrichments".into(), Value::Array(enrichments));
    config.insert("embedder".into(), options.embedder);
    if options.sparse {
        config.insert("sparse".into(), json!(true));
    }
    if let Some(dimension) = options.dimension {
        config.insert("dimension".into(), json!(dimension));
    }
    if let Some(distance_metric) = options.distance_metric {
        config.insert("distance_metric".into(), json!(distance_metric));
    }
    Ok(Value::Object(config))
}

pub fn normalize_base_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    trimmed
        .strip_suffix("/api/v1")
        .unwrap_or(trimmed)
        .to_string()
}

pub fn new_client(base_url: &str, http_client: reqwest::Client) -> Client {
    Client::new_with_client(&normalize_base_url(base_url), http_client)
}

pub fn new_client_with_token(
    base_url: &str,
    token: &str,
) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))?,
    );
    let http_client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    Ok(new_client(base_url, http_client))
}

include!(concat!(env!("OUT_DIR"), "/client.rs"));

#[cfg(test)]
mod tests {
    use super::{
        ArtifactEmbeddingIndexOptions, ArtifactEmbeddingSourceSpec,
        artifact_embedding_index_config, artifact_index_sources, normalize_base_url,
    };
    use serde_json::json;

    #[test]
    fn normalizes_local_and_cloud_urls() {
        assert_eq!(
            normalize_base_url("http://localhost:8080"),
            "http://localhost:8080"
        );
        assert_eq!(
            normalize_base_url("http://localhost:8080/"),
            "http://localhost:8080"
        );
        assert_eq!(
            normalize_base_url("http://localhost:8080/api/v1"),
            "http://localhost:8080"
        );
        assert_eq!(
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance"),
            "https://platform.antfly.io/cloud/v1/instance"
        );
        assert_eq!(
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance/api/v1"),
            "https://platform.antfly.io/cloud/v1/instance"
        );
    }

    #[test]
    fn builds_multi_source_index_with_automatic_vector_space() {
        let config = artifact_embedding_index_config(
            "document_vectors",
            ArtifactEmbeddingIndexOptions {
                sources: vec![
                    ArtifactEmbeddingSourceSpec {
                        artifact: "title_dense_v1".into(),
                        source_artifact: Some("title_chunks_v1".into()),
                        field: "text".into(),
                        template: None,
                    },
                    ArtifactEmbeddingSourceSpec {
                        artifact: "body_dense_v1".into(),
                        source_artifact: Some("body_chunks_v1".into()),
                        field: "text".into(),
                        template: None,
                    },
                ],
                embedder: json!({ "provider": "antfly", "model": "antflydb/clipclap" }),
                dimension: Some(384),
                sparse: false,
                distance_metric: Some("cosine".into()),
                vector_space: None,
            },
        )
        .unwrap();
        assert_eq!(config["sources"].as_array().unwrap().len(), 2);
        assert!(config["enrichments"][0].get("vector_space").is_none());
        assert!(config.get("field").is_none());
    }

    #[test]
    fn validates_artifact_source_limits() {
        assert!(artifact_index_sources(["same", "same"]).is_err());
        let artifacts: Vec<_> = (0..65).map(|i| format!("a{i}")).collect();
        assert!(artifact_index_sources(artifacts.iter().map(String::as_str)).is_err());

        let invalid = artifact_embedding_index_config(
            "vectors",
            ArtifactEmbeddingIndexOptions {
                sources: vec![ArtifactEmbeddingSourceSpec {
                    artifact: "dense_v1".into(),
                    source_artifact: None,
                    field: "text".into(),
                    template: None,
                }],
                embedder: json!({ "provider": "antfly" }),
                dimension: Some(0),
                sparse: false,
                distance_metric: None,
                vector_space: None,
            },
        );
        assert!(invalid.is_err());
    }
}
