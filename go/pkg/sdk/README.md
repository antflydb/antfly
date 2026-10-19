# Antfly Go SDK

Go client for the Antfly HTTP API. Module `github.com/antflydb/antfly/go/pkg/sdk`,
package `sdk`. Licensed Apache-2.0 (see the [root README](../../../README.md#license)).

The base client (`oapi/client.gen.go`) is generated with
[oapi-codegen](https://github.com/oapi-codegen/oapi-codegen) from the root
`openapi.yaml`:

```go
//go:generate go tool oapi-codegen --config=cfg.yaml ../../../openapi.yaml
```

`sdk.Client`/`sdk.AntflyClient` wrap the generated `oapi.Client` with typed
request/response helpers, request-size bounds, and retry/merge utilities. The
API it talks to is rooted at `/db/v1` (plus `/auth/v1` and `/ai/v1` for auth
and inference).

## Install

```bash
go get github.com/antflydb/antfly/go/pkg/sdk
```

## Usage

Reuse a client to share its HTTP connections and bounded local admission. Antfly
and inference constructors in package `sdk` default to 100 active HTTP requests, 100 FIFO waiters
and a one-second admission wait. `sdk.NewClient` shares one admission owner across
its database and inference surfaces. Configure these limits with
`Config.RequestAdmission`:

```go
client, err := sdk.NewClient(sdk.Config{
    BaseURL: "http://localhost:8080",
    RequestAdmission: &sdk.RequestAdmissionConfig{
        MaxConcurrentRequests: 16,
        MaxQueuedRequests: 32,
        MaxQueueWait: 250 * time.Millisecond,
    },
})
if err != nil {
    return err
}
defer client.Close()
```

Set the queue capacity or wait to zero for fail-fast behavior. Queue waiting
observes the original request context and consumes a supplied `http.Client`'s
timeout. Unsent refusals return `*sdk.ClientCapacityError`, with `Reason` and
`RequestSent == false`; context cancellation and expiry remain available through
`errors.Is`. The admission wrapper adds no application retries and preserves
SQL's no-redirect/no-replay transport contract.

SDK-owned HTTP clients use a 90-second operation ceiling that starts before
local queue waiting and remains in force through response body consumption.
Shorter caller deadlines win. To select a different timeout, supply an
`http.Client` with the desired `Timeout`; supplied clients retain their policy.
A deadline after transmission does not prove that a mutation had no effect.

SDK-owned HTTP clients return redirects without following them for all operations,
including table creation and deletion. A redirect does not establish whether
the original mutation had an effect. Supplied HTTP clients retain their redirect
policy; SQL continues to enforce its separate no-replay contract. A returned
redirect is a response from a sent operation, not an unsent capacity refusal.

A response holds its slot until `Body.Close` finishes. Typed SDK methods own
that cleanup; callers using `sdk.BoundedHTTPClient` directly must close responses.
`Close` wakes queued callers and releases SDK-owned idle connections. A supplied
HTTP client and its pool remain caller-owned. Clients explicitly sharing a
`BoundedHTTPClient` share its admission and closing lifetime. The server enforces
its own execution limits independently of these per-client counts.

```go
package main

import (
	"context"
	"log"
	"net/http"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	"github.com/antflydb/antfly/go/pkg/sdk/query"
)

func main() {
	ctx := context.Background()
	client, err := antfly.NewAntflyClient("http://127.0.0.1:8080", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	// Batch write, then full-text query, a table.
	client.Batch(ctx, "wikipedia", antfly.BatchRequest{
		Inserts: map[string]any{"doc-1": map[string]any{"title": "Korea", "body": "..."}},
	})
	q := query.NewQueryString(`body:"Korea"`)
	results, err := client.Query(ctx, antfly.QueryRequest{
		Table: "wikipedia", FullTextSearch: &q, Fields: []string{"title", "url"}, Limit: 5,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", results)
}
```

`NewClient(Config)` builds a consolidated client exposing both `Antfly()` and
`Inference()` from one `Config.BaseURL`. `NewAntflyClientWithOptions` and
`NewAntflyClientWithToken` take `oapi.ClientOption`s (`WithBasicAuth`,
`WithApiKey`, `WithToken`) for auth.

The shared gate is also available in the lightweight `admission` subpackage,
which depends only on the standard library. The top-level SDK admission types
are aliases of that implementation. `Stats()` returns coherent active/queued
counts, closed state and local policy for diagnostics.

When the SDK owns a cloned standard HTTP transport, its per-host connection
limit and idle retention follow the configured active-operation ceiling.
Unspecified idle-per-host capacity can retain a full admitted burst; total idle
retention is bounded across targets. Explicitly tighter transport settings remain
in effect, and supplied HTTP transports keep their original settings and owner.
The connection-pool policy adds no operation retry.

## Internal cluster administration

`admin.NewInternalClient` now uses the same bounded HTTP admission as the public
SDK: 100 active operations, 100 local waiters and a one-second queue ceiling.
Use `NewInternalClientWithAdmission` for explicit limits or to share an existing
`sdk.BoundedHTTPClient`; passing a second policy for an already bounded client
is rejected. Close the client when finished. Closing shared admission affects
all clients using it; a caller-supplied HTTP transport remains caller-owned.

`AddMetadataPeerContext`, `RemoveMetadataPeerContext` and
`GetMetadataStatusContext` preserve the original context through waiting,
transmission and response decoding. Their existing variants remain available.
The default owned client keeps a 90-second operation ceiling, including queue
time, and does not follow redirects. Supplied HTTP clients keep their own
timeout and redirect policies. Capacity refusals wrap `sdk.ClientCapacityError`
with `RequestSent == false`; no peer mutation is automatically retried.

The typed standby client also uses bounded admission by default. Use
`admin.NewStandbyClientWithAdmission` to customize or share its admission owner,
and close the client when finished. Explicit `PathStyleAuto` negotiation keeps
one admission lease and the original timeout through both route attempts and
response decoding. Cancellation during fallback remains cancellation.
An unpinned mutation first discovers the route with a status read, then sends
the mutation once. A mutation returning 404 is never replayed automatically.
If the caller cannot read status, select the known legacy or canonical path
explicitly. Failed discovery closes the unsent mutation body. Owned standby
clients do not follow HTTP redirects; caller-supplied clients retain their
redirect policy. Directly constructed generated clients retain their contracts.

Successful route discoveries are remembered for up to 256 targets per process.
Keys are copied and limited to 4 KiB; larger URLs still work without the global
cache. Eviction affects later clients' discovery, not a live client's pinned
route. Unavailable or unauthorized responses do not establish a route.


## Subpackages

- `query` — query DSL helpers (`query.NewQueryString`, etc.)
- `oapi` — the generated OpenAPI client and models
- `admin`, `chunking` — generated clients for the admin and chunking specs
- `modelcache` — shared model-cache helpers

See `examples/quickstart` for tables, indexes, hybrid search, reranking, and
a retrieval agent end to end.

## Model-aware media chunks

`InferenceClient.ChunkAndEmbed` accepts `oapi.InferenceChunkRequest` with an
explicit `Embedding` target. Text, PDF, images, GIF, audio and video share typed
source coordinates and descriptor/vector pairs. Build text input with
`input.FromInferenceChunkRequestInput0(text)`; use the generated media/source
union helpers for inline base64 media or version-pinned HTTP/S3 objects.

`ChunkAndEmbedStream` invokes its consumer for each chunk and returns the terminal
coverage response. Its optional start callback receives source/plan/model context
before vectors. EOF without a terminal record is an error; a consumer error
closes delivery. Retain model identity with each vector. Check `Status`, `Gaps`
and `NextContinuation` before declaring complete coverage. Resume with the same
source and policy plus `Continuation`. `Embedding.Execute = &falseValue` requests
planning without inference. See the
[shared contract](../../../docs/plans/model-aware-media-chunking.md) for the HTTP
shape, rendering policies, security, limits and database artifact integration.
