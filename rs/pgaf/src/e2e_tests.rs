/// End-to-end tests for pgaf.
///
/// Tests that check `ANTFLY_TEST_URL` env var skip automatically when no
/// server is available. The query builder tests run without a server.
///
/// Run all tests:       `cargo pgrx test pg18`
/// Run with antfly:     `ANTFLY_TEST_URL=http://localhost:8080/api/v1/ cargo pgrx test pg18`
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn antfly_url() -> Option<String> {
        std::env::var("ANTFLY_TEST_URL").ok()
    }

    #[pg_test]
    fn test_index_am_full_text_e2e() {
        let Some(base_url) = antfly_url() else {
            return;
        };

        Spi::run("CREATE TABLE e2e_docs (id serial PRIMARY KEY, content text)").unwrap();
        Spi::run(
            "INSERT INTO e2e_docs (content) VALUES
                ('how to fix a broken computer'),
                ('best recipes for chocolate cake'),
                ('introduction to machine learning')",
        )
        .unwrap();

        Spi::run(&format!(
            "CREATE INDEX idx_e2e ON e2e_docs USING antfly (content) WITH (url = '{}')",
            base_url
        ))
        .unwrap();

        // Give antfly time to index
        std::thread::sleep(std::time::Duration::from_secs(2));

        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM e2e_docs WHERE content @@@ 'fix computer'",
        )
        .unwrap()
        .unwrap();

        assert!(count > 0, "Expected at least 1 result for 'fix computer', got {count}");
    }

    #[pg_test]
    fn test_antfly_search_function_e2e() {
        let Some(base_url) = antfly_url() else {
            return;
        };

        Spi::run("CREATE TABLE e2e_search (id serial PRIMARY KEY, content text)").unwrap();
        Spi::run(
            "INSERT INTO e2e_search (content) VALUES
                ('rust programming language tutorial'),
                ('python data science handbook')",
        )
        .unwrap();

        Spi::run(&format!(
            "CREATE INDEX idx_e2e_search ON e2e_search USING antfly (content) WITH (url = '{}')",
            base_url
        ))
        .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(2));

        let count = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM antfly_search('{}', 'e2e_search', 'rust programming')",
            base_url
        ))
        .unwrap()
        .unwrap();

        assert!(count > 0, "Expected results from antfly_search()");
    }

    #[pg_test]
    fn test_antfly_status_connected_e2e() {
        let Some(base_url) = antfly_url() else {
            return;
        };

        let result = Spi::get_one::<String>(&format!(
            "SELECT antfly_status('{}')",
            base_url
        ))
        .unwrap()
        .unwrap();

        assert!(
            result.contains("connected"),
            "Expected 'connected' in status, got: {result}",
        );
    }

    #[pg_test]
    fn test_query_builder_search_produces_json() {
        let result =
            Spi::get_one::<String>("SELECT pgaf.search('hello world')").unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["full_text_search"]["query"], "hello world");
    }

    #[pg_test]
    fn test_query_builder_semantic_produces_json() {
        let result = Spi::get_one::<String>(
            "SELECT pgaf.semantic('hello world', ARRAY['emb_idx'])",
        )
        .unwrap()
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["semantic_search"], "hello world");
        assert_eq!(v["indexes"][0], "emb_idx");
    }

    #[pg_test]
    fn test_query_builder_hybrid_produces_json() {
        let result = Spi::get_one::<String>(
            "SELECT pgaf.hybrid(full_text => 'hello', semantic => 'world', indexes => ARRAY['idx'])",
        )
        .unwrap()
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["full_text_search"]["query"], "hello");
        assert_eq!(v["semantic_search"], "world");
        assert_eq!(v["indexes"][0], "idx");
    }

    #[pg_test]
    fn test_query_builder_filter_prefix() {
        let result = Spi::get_one::<String>(
            "SELECT pgaf.search('hello', filter_prefix => 'tenant:acme:')",
        )
        .unwrap()
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["full_text_search"]["query"], "hello");
        assert_eq!(v["filter_prefix"], "tenant:acme:");
    }
}
