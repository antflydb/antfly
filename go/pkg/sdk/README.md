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

## Client admission

Reuse a client to reuse HTTP connections. Optional local admission bounds active
operations and waiting across the database and inference APIs:

```go
client, err := sdk.NewClient(sdk.Config{
    BaseURL: "http://localhost:8080",
    Admission: &sdk.ClientAdmission{
        MaxInFlight: 16,
        MaxQueued: 32,
        MaxWait: 100 * time.Millisecond,
    },
})
```

These are illustrative limits, not server capacity estimates. Queue waits respect
the request context's deadline and cancellation. `errors.Is(err, sdk.ErrClientBusy)`
identifies a request rejected before dispatch. Close streaming response bodies to
release capacity; fully reading a response releases it at EOF. The pool preserves
the supplied HTTP client's settings and does not automatically retry writes.
Server admission still protects the instance across all clients. Structured
`APIError` values expose `Reason`, `Stage`, `RetryAfterSeconds`, and the optional
`ExecutionStarted` flag; an absent flag leaves the outcome unknown.

## Install

```bash
go get github.com/antflydb/antfly/go/pkg/sdk
```

## Usage

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

## Subpackages

- `query` — query DSL helpers (`query.NewQueryString`, etc.)
- `oapi` — the generated OpenAPI client and models
- `admin`, `chunking` — generated clients for the admin and chunking specs
- `modelcache` — shared model-cache helpers

See `examples/quickstart` for tables, indexes, hybrid search, reranking, and
a retrieval agent end to end.
