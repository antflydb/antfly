#![allow(clippy::all)]

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

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

impl Default for types::CreateFullTextIndexRequest {
    fn default() -> Self {
        Self {
            description: None,
            enrichments: Vec::new(),
            mem_only: None,
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
            source_artifact_name: None,
            sparse: false,
            summarizer: None,
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
            description: None,
            edge_types: Vec::new(),
            enrichments: Vec::new(),
            max_edges_per_document: None,
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
    use super::{normalize_base_url, types};

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
