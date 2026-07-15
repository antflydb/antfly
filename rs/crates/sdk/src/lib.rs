#![allow(clippy::all)]

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

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
) -> Result<Vec<ArtifactIndexSourceSpec>, IndexConfigError> {
    Ok(validate_artifact_names(artifacts)?
        .into_iter()
        .map(|artifact| ArtifactIndexSourceSpec {
            artifact: artifact.to_owned(),
        })
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactIndexSourceSpec {
    pub artifact: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphIndexSourceSpec {
    pub artifact: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<GraphArtifactFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mention_edge_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<GraphNodeMappingSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge: Option<GraphEdgeMappingSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<GraphContextMappingSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphNodeMappingSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<GraphNodeModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphNodeModel {
    Document,
    External,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphEdgeMappingSpec {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<GraphTemplateOrNumber>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<GraphTemplateOrNumber>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum GraphTemplateOrNumber {
    Template(String),
    Number(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphContextMappingSpec {
    pub doc_fields: Vec<String>,
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
    for (source_index, source) in sources.iter().enumerate() {
        let Some(context) = source.context.as_ref() else {
            continue;
        };
        let mut seen = std::collections::HashSet::with_capacity(context.doc_fields.len());
        for (field_index, field) in context.doc_fields.iter().enumerate() {
            if field.is_empty() {
                return Err(IndexConfigError(format!(
                    "sources[{source_index}].context.doc_fields[{field_index}] is required"
                )));
            }
            if !seen.insert(field) {
                return Err(IndexConfigError(format!(
                    "sources[{source_index}].context.doc_fields contains duplicate {field:?}"
                )));
            }
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactEmbeddingIndexOptions {
    pub sources: Vec<ArtifactEmbeddingSourceSpec>,
    pub embedder: types::EmbedderConfig,
    pub dimension: Option<u32>,
    #[serde(default)]
    pub sparse: bool,
    pub distance_metric: Option<ArtifactEmbeddingDistanceMetric>,
    pub vector_space: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactEmbeddingDistanceMetric {
    L2Squared,
    InnerProduct,
    Cosine,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactEmbeddingEnrichmentSpec {
    pub name: String,
    pub kind: ArtifactEmbeddingEnrichmentKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(
        rename = "source_artifact_name",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_artifact: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_dims: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_space: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactEmbeddingEnrichmentKind {
    Embedding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactEmbeddingIndexConfigSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: ArtifactEmbeddingIndexKind,
    pub sources: Vec<ArtifactIndexSourceSpec>,
    pub enrichments: Vec<ArtifactEmbeddingEnrichmentSpec>,
    pub embedder: types::EmbedderConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimension: Option<u32>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub sparse: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_metric: Option<ArtifactEmbeddingDistanceMetric>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactEmbeddingIndexKind {
    Embeddings,
}

fn is_false(value: &bool) -> bool {
    !*value
}

pub fn artifact_embedding_index_config(
    name: &str,
    options: ArtifactEmbeddingIndexOptions,
) -> Result<ArtifactEmbeddingIndexConfigSpec, IndexConfigError> {
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
        enrichments.push(ArtifactEmbeddingEnrichmentSpec {
            name: source.artifact.clone(),
            kind: ArtifactEmbeddingEnrichmentKind::Embedding,
            field: (!source.field.is_empty()).then(|| source.field.clone()),
            template: source.template.clone(),
            source_artifact: source.source_artifact.clone(),
            expected_dims: options.dimension,
            vector_space: options.vector_space.clone(),
        });
    }

    Ok(ArtifactEmbeddingIndexConfigSpec {
        name: name.to_owned(),
        kind: ArtifactEmbeddingIndexKind::Embeddings,
        sources: artifact_index_sources(
            options
                .sources
                .iter()
                .map(|source| source.artifact.as_str()),
        )?,
        enrichments,
        embedder: options.embedder,
        dimension: options.dimension,
        sparse: options.sparse,
        distance_metric: options.distance_metric,
    })
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
        ArtifactEmbeddingDistanceMetric, ArtifactEmbeddingIndexOptions,
        ArtifactEmbeddingSourceSpec, GraphArtifactFormat, GraphContextMappingSpec,
        GraphEdgeMappingSpec, GraphIndexSourceSpec, GraphNodeMappingSpec, GraphNodeModel,
        GraphTemplateOrNumber, artifact_embedding_index_config, artifact_index_sources,
        graph_index_sources, normalize_base_url,
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
                embedder: serde_json::from_value(
                    json!({ "provider": "antfly", "model": "antflydb/clipclap" }),
                )
                .unwrap(),
                dimension: Some(384),
                sparse: false,
                distance_metric: Some(ArtifactEmbeddingDistanceMetric::Cosine),
                vector_space: None,
            },
        )
        .unwrap();
        let config = serde_json::to_value(config).unwrap();
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
                embedder: serde_json::from_value(
                    json!({ "provider": "antfly", "model": "antflydb/clipclap" }),
                )
                .unwrap(),
                dimension: Some(0),
                sparse: false,
                distance_metric: None,
                vector_space: None,
            },
        );
        assert!(invalid.is_err());
    }

    #[test]
    fn builds_graph_sources_with_local_mappings() {
        let sources = graph_index_sources(vec![GraphIndexSourceSpec {
            artifact: "relations_v1".into(),
            path: Some("$.relations[*]".into()),
            format: Some(GraphArtifactFormat::ExtractionRelation),
            mention_edge_type: None,
            nodes: Some(GraphNodeMappingSpec {
                model: Some(GraphNodeModel::Document),
                source: Some("{{source}}".into()),
                target: Some("{{target}}".into()),
            }),
            edge: Some(GraphEdgeMappingSpec {
                edge_type: Some(GraphTemplateOrNumber::Template("{{relation}}".into())),
                weight: Some(GraphTemplateOrNumber::Number(1.0)),
                metadata: None,
            }),
            context: Some(GraphContextMappingSpec {
                doc_fields: vec!["title".into(), "url".into()],
            }),
        }])
        .unwrap();
        let encoded = serde_json::to_value(sources).unwrap();
        assert_eq!(encoded[0]["nodes"]["source"], "{{source}}");
        assert_eq!(encoded[0]["edge"]["weight"], 1.0);
        assert_eq!(encoded[0]["context"]["doc_fields"][1], "url");
    }
}
