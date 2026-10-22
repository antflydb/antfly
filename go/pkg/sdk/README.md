# Antfly Go SDK

Go client for the Antfly HTTP API. Module `github.com/antflydb/antfly/go/pkg/sdk`,
package `sdk`. Licensed Apache-2.0 (see the [root README](../../../README.md#license)).

The base client (`oapi/client.gen.go`) is generated with
[oapi-codegen](https://github.com/oapi-codegen/oapi-codegen) from the root
`openapi.yaml`:

```go
//go:generate python3 ../../../scripts/generate_go_sdk.py --config=cfg.yaml ../../../openapi.yaml
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
`Inference()` from one `Config.BaseURL`.
`NewAntflyClientWithToken(baseURL, httpClient, token)` takes an HTTP client and a
token directly. `NewAntflyClientWithOptions` takes `oapi.ClientOption`s; wrap
authentication helpers (which return `oapi.RequestEditorFn`) with
`oapi.WithRequestEditorFn`:

```go
// Import "github.com/antflydb/antfly/go/pkg/sdk/oapi" as well as antfly.
basic, err := antfly.NewAntflyClientWithOptions(baseURL,
    oapi.WithRequestEditorFn(antfly.WithBasicAuth("admin", "password")))
apiKey, err := antfly.NewAntflyClientWithOptions(baseURL,
    oapi.WithRequestEditorFn(antfly.WithApiKey("key-id", "key-secret")))
token, err := antfly.NewAntflyClientWithOptions(baseURL,
    oapi.WithRequestEditorFn(antfly.WithToken("encoded-key")))
// Handle err after each constructor in application code.
```

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
