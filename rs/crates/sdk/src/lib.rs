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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactIndexSourceSpec {
    pub artifact: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FullTextArtifactSourceSpec {
    pub artifact: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
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
pub struct ArtifactFullTextIndexConfigSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: ArtifactFullTextIndexKind,
    pub sources: Vec<FullTextArtifactSourceSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub mem_only: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactFullTextIndexKind {
    FullText,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArtifactFullTextIndexOptions {
    pub field: Option<String>,
    pub mem_only: bool,
}

pub fn artifact_full_text_index_config<'a>(
    name: &str,
    artifacts: impl IntoIterator<Item = &'a str>,
) -> Result<ArtifactFullTextIndexConfigSpec, IndexConfigError> {
    artifact_full_text_index_config_with_options(
        name,
        artifacts,
        ArtifactFullTextIndexOptions::default(),
    )
}

pub fn artifact_full_text_index_config_with_options<'a>(
    name: &str,
    artifacts: impl IntoIterator<Item = &'a str>,
    options: ArtifactFullTextIndexOptions,
) -> Result<ArtifactFullTextIndexConfigSpec, IndexConfigError> {
    if name.is_empty() {
        return Err(IndexConfigError("index name is required".into()));
    }
    let field_was_set = options.field.is_some();
    let field = options
        .field
        .map(|field| field.trim().to_owned())
        .filter(|field| !field.is_empty());
    if field_was_set && field.is_none() {
        return Err(IndexConfigError("field must not be empty".into()));
    }
    let sources = artifact_index_sources(artifacts)?
        .into_iter()
        .map(|source| FullTextArtifactSourceSpec {
            artifact: source.artifact,
            field: None,
        })
        .collect();
    Ok(ArtifactFullTextIndexConfigSpec {
        name: name.to_owned(),
        kind: ArtifactFullTextIndexKind::FullText,
        sources,
        field,
        mem_only: options.mem_only,
    })
}

/// Builds a full-text union whose artifact streams use different record
/// shapes. A source-local field overrides the optional shared field.
pub fn artifact_full_text_index_config_for_sources(
    name: &str,
    mut sources: Vec<FullTextArtifactSourceSpec>,
    options: ArtifactFullTextIndexOptions,
) -> Result<ArtifactFullTextIndexConfigSpec, IndexConfigError> {
    if name.is_empty() {
        return Err(IndexConfigError("index name is required".into()));
    }
    validate_artifact_names(sources.iter().map(|source| source.artifact.as_str()))?;
    for (index, source) in sources.iter_mut().enumerate() {
        if let Some(field) = source.field.take() {
            let field = field.trim().to_owned();
            if field.is_empty() {
                return Err(IndexConfigError(format!(
                    "sources[{index}].field must not be empty"
                )));
            }
            source.field = Some(field);
        }
    }
    let field_was_set = options.field.is_some();
    let field = options
        .field
        .map(|field| field.trim().to_owned())
        .filter(|field| !field.is_empty());
    if field_was_set && field.is_none() {
        return Err(IndexConfigError("field must not be empty".into()));
    }
    Ok(ArtifactFullTextIndexConfigSpec {
        name: name.to_owned(),
        kind: ArtifactFullTextIndexKind::FullText,
        sources,
        field,
        mem_only: options.mem_only,
    })
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphNodeMappingSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<GraphNodeModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<GraphTemplateOrNumber>,
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
        if let Some(path) = source.path.as_deref()
            && !valid_graph_artifact_path(path)
        {
            return Err(IndexConfigError(format!(
                "sources[{source_index}].path must be $, a dot-separated field path, or end in [*]"
            )));
        }
        if let Some(nodes) = source.nodes.as_ref() {
            if let Some(GraphTemplateOrNumber::Number(value)) = nodes.target.as_ref()
                && !value.is_finite()
            {
                return Err(IndexConfigError(format!(
                    "sources[{source_index}].nodes.target must be finite"
                )));
            }
        }
        if let Some(edge) = source.edge.as_ref() {
            for (field_name, value) in [
                ("type", edge.edge_type.as_ref()),
                ("weight", edge.weight.as_ref()),
            ] {
                if let Some(GraphTemplateOrNumber::Number(value)) = value
                    && !value.is_finite()
                {
                    return Err(IndexConfigError(format!(
                        "sources[{source_index}].edge.{field_name} must be finite"
                    )));
                }
            }
        }
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

fn valid_graph_artifact_path(path: &str) -> bool {
    if path.is_empty() || path == "$" {
        return true;
    }
    let Some(mut trimmed) = path.strip_prefix("$.") else {
        return false;
    };
    if let Some(without_wildcard) = trimmed.strip_suffix("[*]") {
        trimmed = without_wildcard;
    }
    !trimmed.is_empty()
        && trimmed.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
        })
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
    pub embedder: types::ManagedEmbedderConfig,
    pub dimension: Option<u32>,
    #[serde(default)]
    pub sparse: bool,
    pub distance_metric: Option<ArtifactEmbeddingDistanceMetric>,
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
    pub embedder: types::ManagedEmbedderConfig,
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
    let mut enrichments = Vec::with_capacity(options.sources.len());
    for (index, source) in options.sources.iter().enumerate() {
        let has_template = !source.template.as_deref().unwrap_or_default().is_empty();
        if source.source_artifact.as_deref() == Some("") {
            return Err(IndexConfigError(format!(
                "sources[{index}].source_artifact cannot be empty"
            )));
        }
        enrichments.push(ArtifactEmbeddingEnrichmentSpec {
            name: source.artifact.clone(),
            kind: ArtifactEmbeddingEnrichmentKind::Embedding,
            field: if has_template {
                None
            } else if source.field.is_empty() {
                Some(default_embedding_source_field())
            } else {
                Some(source.field.clone())
            },
            template: source.template.clone(),
            source_artifact: source.source_artifact.clone(),
            expected_dims: options.dimension,
            vector_space: None,
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

mod graph_identifier_policy_generated;
pub use graph_identifier_policy_generated::{
    GRAPH_IDENTIFIER_POLICY_VERSION, GRAPH_IDENTIFIER_UNICODE_VERSION,
    MAX_GRAPH_IDENTIFIER_CODE_POINTS, MAX_GRAPH_IDENTIFIER_UTF8_BYTES, is_valid_graph_identifier,
};

pub fn normalize_base_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    ["/db/v1", "/auth/v1", "/ai/v1", "/api/v1"]
        .into_iter()
        .find_map(|suffix| trimmed.strip_suffix(suffix))
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

fn relationship_field_active(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> bool {
    object
        .get(field)
        .is_some_and(|value| !value.is_null() && value.as_bool() != Some(false))
}

/// Validate the cross-field request relationships published through Antfly's
/// OpenAPI vendor extensions before sending a create-index request.
pub fn validate_create_index_request_relationships(
    request: &types::CreateIndexRequest,
) -> Result<(), IndexConfigError> {
    let value = serde_json::to_value(request)
        .map_err(|error| IndexConfigError(format!("serialize create index request: {error}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| IndexConfigError("index config must be an object".into()))?;
    let has_sources = relationship_field_active(object, "sources");
    match object.get("type").and_then(serde_json::Value::as_str) {
        Some("full_text") if has_sources && relationship_field_active(object, "artifact_name") => {
            return Err(IndexConfigError(
                "index sources cannot be combined with artifact_name".into(),
            ));
        }
        Some("graph") if has_sources && relationship_field_active(object, "source") => {
            return Err(IndexConfigError(
                "index sources cannot be combined with source".into(),
            ));
        }
        Some("embeddings") => {
            if has_sources {
                for field in [
                    "external",
                    "field",
                    "template",
                    "chunker",
                    "embedding_name",
                    "source_artifact_name",
                ] {
                    if relationship_field_active(object, field) {
                        return Err(IndexConfigError(format!(
                            "index sources cannot be combined with {field}"
                        )));
                    }
                }
            }
            if relationship_field_active(object, "source_artifact_name")
                && !relationship_field_active(object, "embedding_name")
            {
                return Err(IndexConfigError(
                    "embedding source_artifact_name requires a non-empty embedding_name".into(),
                ));
            }
            if let (Some(embedding_name), Some(source_artifact_name), Some(enrichments)) = (
                object
                    .get("embedding_name")
                    .and_then(serde_json::Value::as_str),
                object
                    .get("source_artifact_name")
                    .and_then(serde_json::Value::as_str),
                object
                    .get("enrichments")
                    .and_then(serde_json::Value::as_array),
            ) && enrichments.iter().any(|candidate| {
                candidate.get("kind").and_then(serde_json::Value::as_str) == Some("embedding")
                    && candidate.get("name").and_then(serde_json::Value::as_str)
                        == Some(embedding_name)
                    && candidate
                        .get("source_artifact_name")
                        .and_then(serde_json::Value::as_str)
                        != Some(source_artifact_name)
            }) {
                return Err(IndexConfigError(
                    "embedding source_artifact_name must match the authoritative embedding enrichment"
                        .into(),
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

/// Validate every inline index before sending a create-table request.
pub fn validate_create_table_request_relationships(
    request: &types::CreateTableRequest,
) -> Result<(), IndexConfigError> {
    let mut indexes = request.indexes.iter().collect::<Vec<_>>();
    indexes.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    for (name, config) in indexes {
        validate_create_index_request_relationships(config)
            .map_err(|error| IndexConfigError(format!("invalid index {name:?}: {error}")))?;
    }
    Ok(())
}

include!(concat!(env!("OUT_DIR"), "/client.rs"));

/// Build an Antfly inference embedder without exposing generated union variant names.
pub fn antfly_embedder(model: impl Into<String>) -> types::ManagedEmbedderConfig {
    types::ManagedEmbedderConfig::AntflyEmbedderConfig(types::AntflyEmbedderConfig {
        api_url: None,
        model: model.into(),
        provider: types::AntflyEmbedderConfigProvider::Antfly,
    })
}

impl Default for types::CreateFullTextIndexRequest {
    fn default() -> Self {
        Self {
            artifact_name: None,
            description: None,
            enrichments: Vec::new(),
            field: None,
            mem_only: None,
            sources: None,
            type_: types::CreateFullTextIndexRequestType::FullText,
            version: 0,
        }
    }
}

impl Default for types::CreateEmbeddingsIndexRequest {
    fn default() -> Self {
        Self {
            chunk_size: 1024,
            chunker: None,
            coverage_policy: None,
            description: None,
            dimension: None,
            distance_metric: None,
            embedder: None,
            embedding_name: None,
            enrichments: Vec::new(),
            execution: None,
            external: false,
            field: None,
            mem_only: None,
            min_weight: None,
            publication_policy: Some(types::IndexPublicationPolicy::Progressive),
            source_artifact_name: None,
            sources: None,
            sparse: false,
            template: None,
            top_k: std::num::NonZeroU64::new(10).expect("10 is non-zero"),
            type_: types::CreateEmbeddingsIndexRequestType::Embeddings,
            version: 0,
        }
    }
}

impl Default for types::CreateGraphIndexRequest {
    fn default() -> Self {
        Self {
            algebraic_planning: None,
            artifact: None,
            description: None,
            edge_types: Vec::new(),
            enrichments: Vec::new(),
            max_edges_per_document: None,
            resolvers: Vec::new(),
            source: None,
            sources: None,
            summarizer: None,
            template: None,
            type_: types::CreateGraphIndexRequestType::Graph,
            version: 0,
        }
    }
}

impl Default for types::CreateAlgebraicIndexRequest {
    fn default() -> Self {
        Self {
            derive_from_schema: None,
            description: None,
            enrichments: Vec::new(),
            type_: types::CreateAlgebraicIndexRequestType::Algebraic,
            version: 0,
        }
    }
}

/// Actionable storage admission detail extracted from a create-index error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageResourceExhaustedError {
    pub message: String,
    pub retry_after_ms: u64,
}

impl types::CreateIndexError {
    /// Returns typed retry metadata when this is the documented HTTP 429
    /// storage-admission response.
    pub fn storage_resource_exhausted(&self) -> Option<StorageResourceExhaustedError> {
        if self.code.as_deref() != Some("storage_resource_exhausted")
            || self.retryable != Some(true)
        {
            return None;
        }
        Some(StorageResourceExhaustedError {
            message: self.message.clone().unwrap_or_else(|| self.error.clone()),
            retry_after_ms: self.retry_after_ms?.get(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactEmbeddingIndexOptions, ArtifactEmbeddingSourceSpec, ArtifactFullTextIndexOptions,
        FullTextArtifactSourceSpec, GraphArtifactFormat, GraphContextMappingSpec,
        GraphEdgeMappingSpec, GraphIndexSourceSpec, GraphNodeMappingSpec, GraphNodeModel,
        GraphTemplateOrNumber, antfly_embedder, artifact_embedding_index_config,
        artifact_full_text_index_config, artifact_full_text_index_config_for_sources,
        artifact_full_text_index_config_with_options, graph_index_sources, normalize_base_url,
        types, validate_create_index_request_relationships,
        validate_create_table_request_relationships,
    };

    #[test]
    fn builds_document_and_chunk_embedding_sources() {
        let config = artifact_embedding_index_config(
            "document_vectors",
            ArtifactEmbeddingIndexOptions {
                sources: vec![
                    ArtifactEmbeddingSourceSpec {
                        artifact: "document_dense_v1".into(),
                        source_artifact: None,
                        field: "semantic_content".into(),
                        template: None,
                    },
                    ArtifactEmbeddingSourceSpec {
                        artifact: "document_chunk_dense_v1".into(),
                        source_artifact: Some("document_chunks_v1".into()),
                        field: "text".into(),
                        template: None,
                    },
                ],
                embedder: antfly_embedder("antflydb/clipclap"),
                dimension: Some(384),
                sparse: false,
                distance_metric: None,
            },
        )
        .expect("valid multi-source index");
        let value = serde_json::to_value(config).expect("serialize multi-source index");
        assert_eq!(value["sources"].as_array().map(Vec::len), Some(2));
        assert_eq!(value["enrichments"].as_array().map(Vec::len), Some(2));
        assert!(value.get("embedding_name").is_none());
    }

    #[test]
    fn template_only_embedding_source_omits_noop_field() {
        let config = artifact_embedding_index_config(
            "templated_vectors",
            ArtifactEmbeddingIndexOptions {
                sources: vec![ArtifactEmbeddingSourceSpec {
                    artifact: "templated_v1".into(),
                    source_artifact: None,
                    field: "text".into(),
                    template: Some("{{ title }}: {{ body }}".into()),
                }],
                embedder: antfly_embedder("antflydb/clipclap"),
                dimension: None,
                sparse: false,
                distance_metric: None,
            },
        )
        .expect("valid template-only index");
        let value = serde_json::to_value(config).expect("serialize template-only index");
        assert_eq!(
            value["enrichments"][0]["template"],
            "{{ title }}: {{ body }}"
        );
        assert!(value["enrichments"][0].get("field").is_none());
    }

    #[test]
    fn builds_multi_source_full_text_index() {
        let config = artifact_full_text_index_config(
            "document_text",
            ["document_text_v1", "document_chunks_v1"],
        )
        .expect("valid multi-source full-text index");
        assert_eq!(config.sources.len(), 2);
        assert_eq!(config.sources[1].artifact, "document_chunks_v1");

        let configured = artifact_full_text_index_config_with_options(
            "document_text",
            ["document_text_v1", "document_chunks_v1"],
            ArtifactFullTextIndexOptions {
                field: Some(" text ".into()),
                mem_only: true,
            },
        )
        .expect("valid full-text options");
        assert_eq!(configured.field.as_deref(), Some("text"));
        assert!(configured.mem_only);

        let per_source = artifact_full_text_index_config_for_sources(
            "document_text",
            vec![
                FullTextArtifactSourceSpec {
                    artifact: "document_text_v1".into(),
                    field: Some(" summary ".into()),
                },
                FullTextArtifactSourceSpec {
                    artifact: "document_chunks_v1".into(),
                    field: Some("text".into()),
                },
            ],
            ArtifactFullTextIndexOptions::default(),
        )
        .expect("valid per-source projections");
        assert_eq!(per_source.sources[0].field.as_deref(), Some("summary"));
    }

    #[test]
    fn validates_graph_sources_and_preserves_per_source_mapping() {
        let sources = graph_index_sources(vec![GraphIndexSourceSpec {
            artifact: "relations_v1".into(),
            path: Some("$.relations[*]".into()),
            format: Some(GraphArtifactFormat::ExtractionRelation),
            mention_edge_type: None,
            nodes: Some(GraphNodeMappingSpec {
                model: Some(GraphNodeModel::Document),
                target: Some(GraphTemplateOrNumber::Number(42.0)),
            }),
            edge: Some(GraphEdgeMappingSpec {
                edge_type: Some(GraphTemplateOrNumber::Template("{{ _item.type }}".into())),
                weight: Some(GraphTemplateOrNumber::Number(0.8)),
                metadata: None,
            }),
            context: Some(GraphContextMappingSpec {
                doc_fields: vec!["title".into(), "url".into()],
            }),
        }])
        .expect("valid graph source");
        assert_eq!(sources[0].path.as_deref(), Some("$.relations[*]"));
        assert_eq!(
            sources[0]
                .nodes
                .as_ref()
                .and_then(|nodes| nodes.target.as_ref()),
            Some(&GraphTemplateOrNumber::Number(42.0))
        );

        let duplicate = GraphIndexSourceSpec {
            artifact: "same".into(),
            path: None,
            format: None,
            mention_edge_type: None,
            nodes: None,
            edge: None,
            context: None,
        };
        assert!(graph_index_sources(vec![duplicate.clone(), duplicate.clone()]).is_err());
        assert!(
            graph_index_sources(vec![GraphIndexSourceSpec {
                artifact: "relations".into(),
                path: Some("$.relations[0]".into()),
                format: None,
                mention_edge_type: None,
                nodes: None,
                edge: None,
                context: None,
            }])
            .is_err()
        );
        assert!(
            serde_json::from_str::<GraphNodeMappingSpec>(r#"{"source":"{{ _doc.key }}"}"#).is_err()
        );
    }

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
            normalize_base_url("http://localhost:8080/db/v1/"),
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
        assert_eq!(
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance/db/v1"),
            "https://platform.antfly.io/cloud/v1/instance"
        );
        assert_eq!(
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance/auth/v1"),
            "https://platform.antfly.io/cloud/v1/instance"
        );
        assert_eq!(
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance/ai/v1"),
            "https://platform.antfly.io/cloud/v1/instance"
        );
    }

    #[test]
    fn create_index_request_uses_path_owned_identity() {
        let request = types::CreateEmbeddingsIndexRequest {
            dimension: std::num::NonZeroU64::new(512),
            coverage_policy: Some(types::DerivedCoveragePolicy::Partial),
            ..Default::default()
        };
        let request: types::CreateIndexRequest = request.into();
        let value = serde_json::to_value(request).expect("serialize create index request");
        assert_eq!(value["type"], "embeddings");
        assert_eq!(value["dimension"], 512);
        assert_eq!(value["coverage_policy"], "partial");
        assert!(value.get("name").is_none());
    }

    #[test]
    fn create_index_relationship_validator_rejects_orphaned_source_artifact() {
        let invalid: types::CreateIndexRequest = types::CreateEmbeddingsIndexRequest {
            source_artifact_name: Some("chunks_v1".parse().expect("valid artifact name")),
            ..Default::default()
        }
        .into();
        let error = validate_create_index_request_relationships(&invalid)
            .expect_err("source artifact must require embedding name");
        assert!(error.to_string().contains("embedding_name"));

        let valid: types::CreateIndexRequest = types::CreateEmbeddingsIndexRequest {
            embedding_name: Some("dense_v1".parse().expect("valid embedding name")),
            source_artifact_name: Some("chunks_v1".parse().expect("valid artifact name")),
            ..Default::default()
        }
        .into();
        validate_create_index_request_relationships(&valid)
            .expect("released v0.2 singular form remains valid");

        let mismatch: types::CreateIndexRequest = serde_json::from_value(serde_json::json!({
            "type": "embeddings",
            "embedding_name": "dense_v1",
            "source_artifact_name": "wrong_chunks_v1",
            "enrichments": [{
                "name": "dense_v1",
                "kind": "embedding",
                "source_artifact_name": "chunks_v1"
            }]
        }))
        .expect("deserialize mismatched request fixture");
        let error = validate_create_index_request_relationships(&mismatch)
            .expect_err("inline enrichment mismatch must be rejected");
        assert!(
            error
                .to_string()
                .contains("authoritative embedding enrichment")
        );
    }

    #[test]
    fn create_table_relationship_validator_scopes_inline_index_errors() {
        let request: types::CreateTableRequest = serde_json::from_value(serde_json::json!({
            "indexes": {
                "semantic": {
                    "type": "embeddings",
                    "source_artifact_name": "document_chunks_v1"
                }
            }
        }))
        .expect("deserialize invalid table fixture");

        let error = validate_create_table_request_relationships(&request)
            .expect_err("inline index must be validated before transport");
        assert!(error.to_string().contains("semantic"));
        assert!(error.to_string().contains("embedding_name"));
    }

    #[test]
    fn antfly_embedder_helper_emits_typed_provider_config() {
        let value = serde_json::to_value(antfly_embedder("antflydb/clipclap"))
            .expect("serialize Antfly embedder");
        assert_eq!(value["provider"], "antfly");
        assert_eq!(value["model"], "antflydb/clipclap");
    }

    #[test]
    fn created_index_response_is_discriminated() {
        let created: types::CreatedIndex = serde_json::from_value(serde_json::json!({
            "name": "thumbnail",
            "type": "embeddings",
            "dimension": 512,
            "distance_metric": "cosine",
            "sources": [{"artifact": "thumbnail_dense_v1"}],
            "embedding_name": "thumbnail_dense_v1",
            "source_artifact_name": "thumbnail_chunks_v1",
            "chunker": {
                "provider": "antfly",
                "model": "fixed",
                "store_chunks": false
            }
        }))
        .expect("deserialize created embeddings index");
        match created {
            types::CreatedIndex::EmbeddingsIndex(index) => {
                assert_eq!(index.name, "thumbnail");
                assert_eq!(index.dimension.map(std::num::NonZeroU64::get), Some(512));
                assert_eq!(index.embedding_name.as_deref(), Some("thumbnail_dense_v1"));
                assert_eq!(
                    index.source_artifact_name.as_deref(),
                    Some("thumbnail_chunks_v1"),
                );
                assert_eq!(index.sources.as_ref().map(Vec::len), Some(1));
                assert_eq!(
                    index.chunker.and_then(|chunker| chunker.model),
                    Some("fixed".to_string()),
                );
            }
            other => panic!("unexpected created index variant: {other:?}"),
        }
    }

    #[test]
    fn create_index_error_preserves_storage_retry_contract() {
        let error: types::CreateIndexError = serde_json::from_value(serde_json::json!({
            "code": "storage_resource_exhausted",
            "error": "storage_resource_exhausted",
            "message": "storage capacity is temporarily exhausted",
            "retryable": true,
            "retry_after_ms": 1250
        }))
        .expect("valid create index error");
        let exhausted = error
            .storage_resource_exhausted()
            .expect("typed storage error");
        assert_eq!(exhausted.retry_after_ms, 1250);
        assert_eq!(
            exhausted.message,
            "storage capacity is temporarily exhausted"
        );
    }
}
