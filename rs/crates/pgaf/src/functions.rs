use pgrx::prelude::*;

use crate::client::AntflyClient;

/// Search an Antfly collection and return (id, score, data) tuples.
///
/// Usage:
///   SELECT * FROM antfly_search('http://localhost:8080/api/v1/', 'my_collection', 'search query');
///
/// Returns a set of (id TEXT, score FLOAT8, data JSONB) rows.
#[pg_extern]
fn antfly_search(
    base_url: &str,
    collection: &str,
    query: &str,
    limit: pgrx::default!(Option<i32>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(id, String),
        name!(score, f64),
        name!(data, pgrx::JsonB),
    ),
> {
    let client = AntflyClient::new(base_url).unwrap_or_else(|e| {
        pgrx::error!("pgaf: failed to create client: {}", e);
    });

    let hits = client
        .search(collection, query, limit.map(|l| l as i64).or(Some(10)))
        .unwrap_or_else(|e| {
            pgrx::error!("pgaf: search failed: {}", e);
        });

    let rows: Vec<_> = hits
        .into_iter()
        .map(|hit| (hit.id, hit.score, pgrx::JsonB(hit.source)))
        .collect();

    TableIterator::new(rows)
}

/// Check connectivity to an Antfly server.
#[pg_extern]
fn antfly_status(base_url: &str) -> String {
    match AntflyClient::new(base_url) {
        Ok(_client) => format!("pgaf: connected to {}", base_url),
        Err(e) => format!("pgaf: error: {}", e),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_antfly_status() {
        // Should return an error string for an unreachable server, not panic
        let result = crate::functions::antfly_status("http://127.0.0.1:19999");
        assert!(result.starts_with("pgaf:"));
    }
}
