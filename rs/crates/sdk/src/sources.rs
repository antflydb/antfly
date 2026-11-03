// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Control the optional antfly-source companion at its own base URL.
use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug)]
pub enum SourceServiceError {
    Transport(reqwest::Error),
    InvalidConfiguration(&'static str),
    Http { status: StatusCode, code: String },
}
impl std::fmt::Display for SourceServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(_) => write!(f, "source service transport failed"),
            Self::InvalidConfiguration(message) => write!(f, "{message}"),
            Self::Http { status, code } => write!(f, "source service returned {status}: {code}"),
        }
    }
}
impl std::error::Error for SourceServiceError {}
impl From<reqwest::Error> for SourceServiceError {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSyncJob {
    pub version: u32,
    pub id: String,
    pub state: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub code: Option<String>,
}

// Deliberately has no Debug implementation: its operator token is private.
#[derive(Clone)]
pub struct SourcesClient {
    client: Client,
    base: Url,
    token: String,
}
impl SourcesClient {
    pub fn new(base_url: &str, token: &str) -> Result<Self, SourceServiceError> {
        let base = Url::parse(base_url)
            .map_err(|_| SourceServiceError::InvalidConfiguration("invalid source service URL"))?;
        let loopback = matches!(base.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || !(base.scheme() == "https" || base.scheme() == "http" && loopback)
        {
            return Err(SourceServiceError::InvalidConfiguration(
                "source service requires HTTPS or loopback HTTP without URL credentials",
            ));
        }
        if token.is_empty() || token.contains(['\r', '\n']) {
            return Err(SourceServiceError::InvalidConfiguration(
                "invalid source service token",
            ));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        Ok(Self {
            client,
            base,
            token: token.into(),
        })
    }
    async fn request(
        &self,
        method: Method,
        path: &[&str],
        body: Option<&Value>,
    ) -> Result<Value, SourceServiceError> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| SourceServiceError::InvalidConfiguration("invalid source service URL"))?
            .pop_if_empty()
            .extend(path);
        let mut request = self.client.request(method, url).bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let value: Value = response.json().await.unwrap_or(Value::Null);
            return Err(SourceServiceError::Http {
                status,
                code: value
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("request_failed")
                    .into(),
            });
        }
        Ok(response.json().await?)
    }
    pub async fn put_secret(&self, key: &str, value: &str) -> Result<Value, SourceServiceError> {
        self.request(
            Method::PUT,
            &["v1", "secrets", key],
            Some(&json!({"value": value})),
        )
        .await
    }
    pub async fn revoke_secret(&self, key: &str) -> Result<Value, SourceServiceError> {
        self.request(Method::DELETE, &["v1", "secrets", key], None)
            .await
    }
    pub async fn profile(&self) -> Result<Value, SourceServiceError> {
        self.request(Method::GET, &["v1", "profile"], None).await
    }
    pub async fn put_connection(
        &self,
        name: &str,
        connection: &Value,
    ) -> Result<Value, SourceServiceError> {
        self.request(Method::PUT, &["v1", "connections", name], Some(connection))
            .await
    }
    pub async fn revoke_connection(&self, name: &str) -> Result<Value, SourceServiceError> {
        self.request(Method::DELETE, &["v1", "connections", name], None)
            .await
    }
    pub async fn put_source(&self, source: &Value) -> Result<Value, SourceServiceError> {
        let id = source.get("id").and_then(Value::as_str).ok_or(
            SourceServiceError::InvalidConfiguration("source requires an id"),
        )?;
        self.request(Method::PUT, &["v1", "sources", id], Some(source))
            .await
    }
    pub async fn remove_source(&self, id: &str) -> Result<Value, SourceServiceError> {
        self.request(Method::DELETE, &["v1", "sources", id], None)
            .await
    }
    pub async fn sync(&self, restart: bool) -> Result<SourceSyncJob, SourceServiceError> {
        let value = self
            .request(
                Method::POST,
                &["v1", "sync"],
                Some(&json!({"restart": restart})),
            )
            .await?;
        serde_json::from_value(value)
            .map_err(|_| SourceServiceError::InvalidConfiguration("invalid source job response"))
    }
    pub async fn current_job(&self) -> Result<SourceSyncJob, SourceServiceError> {
        let value = self.request(Method::GET, &["v1", "jobs"], None).await?;
        serde_json::from_value(value)
            .map_err(|_| SourceServiceError::InvalidConfiguration("invalid source job response"))
    }
    pub async fn job(&self, id: &str) -> Result<SourceSyncJob, SourceServiceError> {
        let value = self.request(Method::GET, &["v1", "jobs", id], None).await?;
        serde_json::from_value(value)
            .map_err(|_| SourceServiceError::InvalidConfiguration("invalid source job response"))
    }
    pub async fn cancel_job(&self, id: &str) -> Result<SourceSyncJob, SourceServiceError> {
        let value = self
            .request(Method::DELETE, &["v1", "jobs", id], None)
            .await?;
        serde_json::from_value(value)
            .map_err(|_| SourceServiceError::InvalidConfiguration("invalid source job response"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_urls_require_safe_transport_and_no_embedded_credentials() {
        for url in [
            "http://remote.example",
            "https://user:pass@example.com",
            "https://example.com?token=x",
        ] {
            assert!(SourcesClient::new(url, "token").is_err());
        }
        assert!(SourcesClient::new("http://localhost:8082", "token").is_ok());
    }
    #[test]
    fn interrupted_job_receipts_keep_optional_fields_optional() {
        let job: SourceSyncJob = serde_json::from_value(json!({"version":1,"id":"job","state":"interrupted","started_at":1,"code":"service_restarted"})).unwrap();
        assert_eq!(job.state, "interrupted");
        assert!(job.finished_at.is_none());
    }
}
