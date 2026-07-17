# docsaf Derived Document Design

docsaf writes source document rows. Antfly owns extraction and all derived
children.

The source row is intentionally small:

```json
{
  "id": "docs/guide.md",
  "url": "s3://docs/guide.md",
  "filename": "guide.md",
  "mime_type": "text/markdown",
  "sha256": "...",
  "source_path": "docs/guide.md",
  "doc_type": "source_document"
}
```

The Antfly table configuration attaches a `document_extraction` asset producer
to the `url` field. That producer materializes the derived hierarchy:

```text
source_document
  -> document_units_v1
  -> document_chunks_v1
  -> document_chunk_dense_v1
  -> full-text/vector indexes
  -> entity mentions and graph artifacts
```

docsaf does not write extracted sections, chunks, vectors, or graph evidence as
top-level rows. Those records are generated artifacts whose lifecycle is owned by
the source row's artifact manifest.

## Responsibilities

docsaf is responsible for:

- Traversing content sources such as filesystems, web crawls, Git, S3, and
  Google Drive.
- Computing stable source document IDs and source fingerprints such as SHA-256.
- Capturing source metadata: filename, MIME type, size, timestamps, ETag,
  version, and source path.
- Emitting source rows for linear merge.
- Creating table/index configuration that enables `document_extraction`,
  unit-derived chunking, and retrieval indexes.

Antfly is responsible for:

- Fetching the source URL.
- Routing by content type, filename, response metadata, and magic bytes.
- Extracting canonical document units.
- Chunking document units.
- Writing manifests, generations, child range descriptors, and merge plans.
- Deleting stale units/chunks on source updates or source deletion.
- Indexing units/chunks for full-text, vector, and graph search.
- Reprocessing artifacts through per-document and table-level repair APIs.

## Source URLs

Production sync should use URLs Antfly can fetch directly: S3, HTTPS, or another
configured remote-content source. Local examples may use inline `data:` URLs via
`SourceDocumentOptions.InlineContent`; this is convenient but should not be used
for large files.

## Go API

Use `BuildSourceDocuments` to traverse a source without local extraction:

```go
source := docsaf.NewFilesystemSource(docsaf.FilesystemSourceConfig{
    BaseDir: "./docs",
    IncludePatterns: []string{"**/*.md", "**/*.pdf", "**/*.docx"},
})

docs, err := docsaf.BuildSourceDocuments(ctx, source, docsaf.SourceDocumentOptions{
    BaseURL: "s3://docs-bucket",
})
records := docsaf.SourceDocumentRecords(docs)
```

`ContentProcessor` implementations are extraction utilities for local tooling.
They are not the docsaf-to-Antfly synchronization model.

## CLI Shape

`go/pkg/docsaf/cmd/docsaf` prepares and syncs source rows:

```bash
docsaf prepare --dir ./docs --base-url s3://docs-bucket --output docs.json
docsaf load --input docs.json --table docs --create-table
docsaf sync --dir ./docs --base-url s3://docs-bucket --table docs --create-table
```

For local smoke tests:

```bash
docsaf sync --dir ./docs --inline-content --table docs --create-table
```

Created tables include a hierarchy graph index whose artifact producer writes
`document_units_v1`, plus a full-text index over unit-derived chunks from
`document_chunks_v1`, plus a managed vector index over `document_chunk_dense_v1`.
The vector index consumes embeddings generated from the same chunk artifact
stream rather than asking the embedding index to re-chunk source rows.

The public SDK helper for that vector index is:

```go
embedder, err := antfly.NewEmbedderConfig(antfly.AntflyEmbedderConfig{
    Model: "antflydb/clipclap:gguf:Q4_K",
})
index, err := antfly.NewArtifactEmbeddingIndexConfig(
    "document_vectors",
    antfly.ArtifactEmbeddingIndexConfig{
        SourceArtifactName: docsaf.DefaultDocumentChunksArtifact,
        EmbeddingName:      "document_chunk_dense_v1",
        SourceField:        "text",
        Embedder:           *embedder,
        DistanceMetric:     antfly.DistanceMetricCosine,
    },
)
```

## Query Contract

Applications should query units/chunks through Antfly hierarchy controls instead
of assuming docsaf section rows:

```json
{
  "semantic_search": "termination clause",
  "hierarchy": {
    "return_level": "chunk",
    "include": ["source", "unit"],
    "rollup": "source"
  }
}
```

Results carry ancestry such as `parent_doc_key`, `parent_unit_id`, artifact
identity, and hydrated source/unit documents when requested.
