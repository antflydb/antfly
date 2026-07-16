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

#[cfg(test)]
mod tests {
    use super::normalize_base_url;

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
}
