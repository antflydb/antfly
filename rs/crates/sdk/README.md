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

## Bounded request admission

On native Tokio runtimes, every generated database, SQL, authentication,
inference and management method shares a finite request owner. Defaults are
100 active requests, 100 queued calls and a one-second queue wait. Cloning a
`Client` shares its owner. Admission happens before URL/request serialization.
Queued calls use FIFO semaphore admission; canceling a future removes its waiter
and reservation. A zero queue count or wait selects immediate local refusal.
Zero active capacity is invalid.

```rust
use antfly_sdk::{Client, RequestAdmission, RequestAdmissionConfig};
use std::time::Duration;

let admission = RequestAdmission::new(RequestAdmissionConfig {
    max_active: 16,
    max_queued: 32,
    max_wait: Duration::from_millis(250),
})?;
let client = Client::new("http://localhost:8080")
    .with_request_admission(admission.clone());
let other = client.clone();
```

Separate clients can explicitly share `RequestAdmission`. Assigning an owner
with `with_request_admission` changes that handle; existing clones retain their
previous owner. `request_admission().stats()` reports active/queued/closed
state; concurrent fields are independent observations. `client.close()` rejects
new and queued work across the shared owner. Active response bodies keep their
permit until EOF, read failure or drop, including raw responses retained by
`Error::UnexpectedResponse`. Drop or consume those responses to release capacity.
The transport body retires before its permit. Reqwest manages its connection
pool; dropping all client handles releases their pool ownership.

Use `client_capacity_error(&error)` to distinguish `Closed`, `QueueFull` and
`WaitExpired` local refusals. They are marked unsent and are not automatically
retried. SDK-created HTTP clients retain at most 100 idle connections per host
and disable automatic redirects and retries, including for mutating SQL. A
caller-supplied reqwest client keeps its caller-selected transport policy.

The queue budget is separate from reqwest's transport timeout. Wrap the entire
SDK future in the caller's original timeout/cancellation scope when it must
cover both. This is a request/body concurrency bound, not a complete bound on
caller payloads, decoded result memory or post-body CPU. Native tests cover the
owner and real HTTP body lifetime; WebAssembly is not qualified by these checks.

## Used by

[`antfly-postgres`](../postgres) depends on `antfly-sdk` for its generated types
(e.g. `QueryResponses`, `QueryHit`) while keeping its own blocking HTTP
client, since Postgres extensions cannot run an async runtime.
