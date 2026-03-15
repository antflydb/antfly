/// ParadeDB-style query builder functions for the @@@ operator.
///
/// These functions live in the `pgaf` schema so users write:
///   SELECT * FROM docs WHERE content @@@ pgaf.search('fix computer');
///
/// Each returns a JSON string that scan.rs detects and passes as a structured
/// query body to Antfly's query API.
#[pgrx::pg_schema]
mod pgaf {
    use pgrx::prelude::*;

    /// Full-text search query builder.
    ///
    /// Usage:
    ///   SELECT * FROM docs WHERE content @@@ pgaf.search(
    ///       'fix computer',
    ///       filter_prefix => 'tenant:acme:'
    ///   );
    #[pg_extern(immutable, parallel_safe)]
    fn search(
        query: &str,
        filter_prefix: pgrx::default!(Option<&str>, "NULL"),
        filter_query: pgrx::default!(Option<&str>, "NULL"),
    ) -> String {
        let mut obj = serde_json::json!({
            "full_text_search": { "query": query },
        });
        if let Some(fp) = filter_prefix {
            obj["filter_prefix"] = serde_json::json!(fp);
        }
        if let Some(fq) = filter_query {
            obj["filter_query"] = serde_json::json!({ "query": fq });
        }
        obj.to_string()
    }

    /// Semantic (vector) search query builder.
    ///
    /// Usage:
    ///   SELECT * FROM docs WHERE content @@@ pgaf.semantic(
    ///       'fix my broken computer',
    ///       indexes => ARRAY['embedding_idx']
    ///   );
    #[pg_extern(immutable, parallel_safe)]
    fn semantic(
        query: &str,
        indexes: Vec<String>,
        filter_prefix: pgrx::default!(Option<&str>, "NULL"),
    ) -> String {
        let mut obj = serde_json::json!({
            "semantic_search": query,
            "indexes": indexes,
        });
        if let Some(fp) = filter_prefix {
            obj["filter_prefix"] = serde_json::json!(fp);
        }
        obj.to_string()
    }

    /// Hybrid search query builder (full-text + semantic via RRF).
    ///
    /// Usage:
    ///   SELECT * FROM docs WHERE content @@@ pgaf.hybrid(
    ///       full_text => 'computer repair',
    ///       semantic => 'fix my broken computer',
    ///       indexes => ARRAY['embedding_idx']
    ///   );
    #[pg_extern(immutable, parallel_safe)]
    fn hybrid(
        full_text: pgrx::default!(Option<&str>, "NULL"),
        semantic: pgrx::default!(Option<&str>, "NULL"),
        indexes: pgrx::default!(Option<Vec<String>>, "NULL"),
        filter_prefix: pgrx::default!(Option<&str>, "NULL"),
    ) -> String {
        let mut obj = serde_json::json!({});
        if let Some(ft) = full_text {
            obj["full_text_search"] = serde_json::json!({ "query": ft });
        }
        if let Some(sem) = semantic {
            obj["semantic_search"] = serde_json::json!(sem);
        }
        if let Some(idx) = indexes {
            obj["indexes"] = serde_json::json!(idx);
        }
        if let Some(fp) = filter_prefix {
            obj["filter_prefix"] = serde_json::json!(fp);
        }
        obj.to_string()
    }
}
