# antfly-sdk

Async Rust client for the Antfly API. Crate `antfly-sdk`, edition 2024.
Licensed Apache-2.0 (see the [root README](../../../README.md#license)).

The client is generated at build time by [Progenitor](https://github.com/oxidecomputer/progenitor)
from the root `openapi.yaml` (see `build.rs`). Before generation, `build.rs`
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

```rust
use antfly_sdk::Client;
use antfly_sdk::types::QueryRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("http://localhost:8080");

    // QueryRequest has no Default impl, but every field is `#[serde(default)]`,
    // so it's easiest to build one from JSON with only the fields you need.
    let body: QueryRequest = serde_json::from_value(serde_json::json!({
        "table": "wikipedia",
        "semantic_search": "anatomy and physiology",
        "fields": ["title", "url"],
        "limit": 5,
    }))?;

    let result = client.query_table("wikipedia", &body).await?;
    println!("{:?}", result.into_inner());

    Ok(())
}
```

`Client::new(baseurl)` takes the server URL without the `/db/v1` prefix; each
generated method (e.g. `query_table`, `batch_write`) builds its own
`/db/v1/...` path itself. Mutation endpoints that can return either a
completed result or a pending commit decode into `MutationOutcome<T>`
(`Completed(T)` or `Committed(types::CommittedMutationOutcome)`).

## Used by

[`rs/crates/pgaf`](../pgaf) depends on `antfly-sdk` for its generated types
(e.g. `QueryResponses`, `QueryHit`) while keeping its own blocking HTTP
client, since Postgres extensions cannot run an async runtime.
