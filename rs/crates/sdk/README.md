# antfly-sdk

Async Rust client for the Antfly API. Crate `antfly-sdk`, edition 2024.
Licensed Apache-2.0 (see [LICENSE](LICENSE)).

The client is generated at build time by [Progenitor](https://github.com/oxidecomputer/progenitor)
from the crate-local `openapi.yaml` (see `build.rs`). This bundled input is
synchronized from the root public spec by `make generate`,
and SDK CI checks that the two files match. Published crates therefore build
without a repository checkout. Before generation, `build.rs`
preprocesses the spec (stripping non-JSON media types, unifying error
response schemas, normalizing mutation success responses, and marking
OpenAPI code fences as `text` so they aren't treated as Rust doctests) and
injects client-side validation for `create_table`/`create_index` request
relationships that Progenitor can't express. The generated code lands in
`OUT_DIR/client.rs` and is pulled in via `include!` in `src/lib.rs`.

## Install

```toml
[dependencies]
antfly-sdk = { path = "../sdk" }  # or a git/registry dependency once published
```

## Usage

Search currently needs raw HTTP: the generated `QueryRequest` has `Default`,
but cannot express `semantic_search` or `full_text_search`. Deserializing JSON
into that type silently discards unknown fields, so a successful compile does
not prove search intent reaches the server.

The following raw HTTP example preserves `semantic_search` in the transmitted
JSON. Add `reqwest = { version = "0.12", features = ["json"] }`, `serde_json = "1"`
and `tokio = { version = "1", features = ["macros", "rt-multi-thread"] }` to
`Cargo.toml`:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let body = serde_json::json!({
        "table": "wikipedia",
        "semantic_search": "anatomy and physiology",
        "fields": ["title", "url"],
        "limit": 5
    });
    let response = reqwest::Client::new()
        .post("http://localhost:8080/db/v1/tables/wikipedia/query")
        .json(&body)
        .send().await?
        .error_for_status()?;
    println!("{}", response.text().await?);
    Ok(())
}
```

`Client::new(baseurl)` takes the server URL without the `/db/v1` prefix; each
generated method (e.g. `query_table`, `batch_write`) builds its own
`/db/v1/...` path itself. Mutation endpoints that can return either a
completed result or a pending commit decode into `MutationOutcome<T>`
(`Completed(T)` or `Committed(types::CommittedMutationOutcome)`).

## Used by

[`antfly-postgres`](../postgres) depends on `antfly-sdk` for its generated types
(e.g. `QueryResponses`, `QueryHit`) while keeping its own blocking HTTP
client, since Postgres extensions cannot run an async runtime.
