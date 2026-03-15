use antfly_client::types::{QueryHit, QueryResponses};
use reqwest::blocking::Client;
use serde_json::Value;
use url::Url;

/// HTTP client for communicating with an Antfly server.
///
/// The base URL should include the API version prefix with a trailing slash,
/// e.g. `http://localhost:8080/api/v1/`.
pub struct AntflyClient {
    base_url: Url,
    http: Client,
}

/// A single search result from Antfly.
pub struct SearchHit {
    pub id: String,
    pub score: f64,
    pub source: Value,
}

impl From<QueryHit> for SearchHit {
    fn from(hit: QueryHit) -> Self {
        Self {
            id: hit.id,
            score: hit.score as f64,
            source: Value::Object(hit.source),
        }
    }
}

#[derive(Debug)]
pub enum ClientError {
    InvalidUrl(String, url::ParseError),
    Request(reqwest::Error),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::InvalidUrl(url, err) => write!(f, "invalid url '{}': {}", url, err),
            ClientError::Request(err) => write!(f, "request failed: {}", err),
        }
    }
}

impl AntflyClient {
    pub fn new(base_url: &str) -> Result<Self, ClientError> {
        // Ensure trailing slash so Url::join works correctly with relative paths.
        let normalized = if base_url.ends_with('/') {
            base_url.to_string()
        } else {
            format!("{}/", base_url)
        };
        let base_url = Url::parse(&normalized)
            .map_err(|e| ClientError::InvalidUrl(base_url.to_string(), e))?;
        Ok(Self {
            base_url,
            http: Client::new(),
        })
    }

    /// Full-text search shorthand.
    ///
    /// Sends `POST /tables/{table}/query` with a `full_text_search` body.
    pub fn search(
        &self,
        table: &str,
        query: &str,
        limit: Option<i64>,
    ) -> Result<Vec<SearchHit>, ClientError> {
        let mut body = serde_json::json!({
            "full_text_search": { "query": query },
        });
        if let Some(n) = limit {
            body["limit"] = serde_json::json!(n);
        }
        self.search_raw(table, &body)
    }

    /// Send an arbitrary query body to Antfly and parse the results.
    ///
    /// The body is sent as-is to `POST /tables/{table}/query`.
    /// Callers (e.g. the query builder functions) construct the JSON.
    pub fn search_raw(
        &self,
        table: &str,
        body: &Value,
    ) -> Result<Vec<SearchHit>, ClientError> {
        let path = format!("tables/{}/query", table);
        let url = self
            .base_url
            .join(&path)
            .map_err(|e| ClientError::InvalidUrl(path, e))?;

        let resp = self
            .http
            .post(url)
            .json(body)
            .send()
            .map_err(ClientError::Request)?
            .error_for_status()
            .map_err(ClientError::Request)?;

        let resp_body: QueryResponses = resp.json().map_err(ClientError::Request)?;

        let hits = resp_body
            .responses
            .into_iter()
            .next()
            .and_then(|r| r.hits)
            .map(|h| h.hits)
            .unwrap_or_default();

        Ok(hits.into_iter().map(SearchHit::from).collect())
    }

    /// Ensure a table exists in Antfly, creating it if necessary.
    ///
    /// Sends `POST /tables/{table}` with a minimal config. Ignores 409 (already exists).
    pub fn ensure_table(&self, table: &str) -> Result<(), ClientError> {
        let path = format!("tables/{}", table);
        let url = self
            .base_url
            .join(&path)
            .map_err(|e| ClientError::InvalidUrl(path, e))?;

        let body = serde_json::json!({ "num_shards": 1 });

        let resp = self
            .http
            .post(url)
            .json(&body)
            .send()
            .map_err(ClientError::Request)?;

        // 200/201 = created, 409 = already exists — both fine
        let status = resp.status();
        if status.is_success() || status.as_u16() == 409 {
            Ok(())
        } else {
            resp.error_for_status().map_err(ClientError::Request)?;
            Ok(())
        }
    }

    /// Insert a single document via the batch API.
    ///
    /// Uses `sync_level: "full_text"` so the document is immediately searchable.
    pub fn sync_document(
        &self,
        table: &str,
        doc_id: &str,
        doc: &Value,
    ) -> Result<(), ClientError> {
        let path = format!("tables/{}/batch", table);
        let url = self
            .base_url
            .join(&path)
            .map_err(|e| ClientError::InvalidUrl(path, e))?;

        let body = serde_json::json!({
            "inserts": { doc_id: doc },
            "sync_level": "full_text",
        });

        self.http
            .post(url)
            .json(&body)
            .send()
            .map_err(ClientError::Request)?
            .error_for_status()
            .map_err(ClientError::Request)?;

        Ok(())
    }

    /// Delete a single document via the batch API.
    pub fn delete_document(&self, table: &str, doc_id: &str) -> Result<(), ClientError> {
        let path = format!("tables/{}/batch", table);
        let url = self
            .base_url
            .join(&path)
            .map_err(|e| ClientError::InvalidUrl(path, e))?;

        let body = serde_json::json!({
            "deletes": [doc_id],
        });

        self.http
            .post(url)
            .json(&body)
            .send()
            .map_err(ClientError::Request)?
            .error_for_status()
            .map_err(ClientError::Request)?;

        Ok(())
    }
}
