# Antfly Genkit Plugin

Firebase [Genkit](https://github.com/firebase/genkit) plugin that exposes an
Antfly table/index as a Genkit retriever. Module
`github.com/antflydb/antfly/go/pkg/genkit/antfly`, package `antfly`.
Licensed Apache-2.0 (see the [root README](../../../../README.md#license)).

Built on [`go/pkg/sdk`](../../sdk) — `go.mod` replaces
`github.com/antflydb/antfly/go/pkg/sdk` with `../../sdk` for local
development.

## Install

```bash
go get github.com/antflydb/antfly/go/pkg/genkit/antfly
```

## Usage

```go
package main

import (
	"context"

	"github.com/antflydb/antfly/go/pkg/genkit/antfly"
	"github.com/firebase/genkit/go/genkit"
)

func main() {
	ctx := context.Background()
	g := genkit.Init(ctx, genkit.WithPlugins(&antfly.Antfly{}))

	ds, retriever, err := antfly.DefineRetriever(ctx, g, antfly.IndexConfig{
		TableName: "wikipedia",
		IndexName: "title_body",
	}, nil)
	if err != nil {
		panic(err)
	}

	// Index documents.
	_ = antfly.Index(ctx, nil /* []*ai.Document */, ds)

	// Retrieve with genkit.
	_, _ = retriever.Retrieve(ctx, nil /* *ai.RetrieverRequest */)
}
```

`Antfly.Init` reads the server URL from `ANTFLY_URL` (falling back to a
hardcoded default) and constructs an `*sdk.AntflyClient`. `DefineRetriever`
requires a non-empty `TableName` and `IndexName` and registers one Genkit
`ai.Retriever` per table/index pair. `RetrieverOptions` (passed via
`ai.RetrieverRequest.Options`) controls `Count`, `MetadataKeys`, a bleve
`FilterQuery`, and `OrderBy`.

## Types

- `Antfly` — the plugin (`Name()`, `Init(ctx)`)
- `IndexConfig` — `{TableName, IndexName}` passed to `DefineRetriever`
- `Docstore` — holds the `*sdk.AntflyClient`, table, and index name; implements `Retrieve`
- `RetrieverOptions` — per-request options (`Count`, `MetadataKeys`, `FilterQuery`, `OrderBy`)
- `Retriever(g, class)` — looks up a previously defined retriever by table:index class
- `Index(ctx, docs, ds)` — helper that batch-inserts `[]*ai.Document` into the docstore's table, keyed by an xxhash of the document text

Retrieved documents store text under the `text` metadata key and free-form
metadata under the `metadata` key.
