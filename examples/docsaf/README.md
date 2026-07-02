# docsaf - Source Document Sync to Antfly

`docsaf` syncs source document rows into Antfly. It does not locally split files
into section rows, chunks, vectors, or graph evidence. Antfly owns extraction and
derived artifact lifecycle from the source row.

## Model

`docsaf prepare` and `docsaf sync` emit rows shaped like:

```json
{
  "id": "guide.md",
  "url": "s3://docs-bucket/guide.md",
  "filename": "guide.md",
  "mime_type": "text/markdown",
  "sha256": "...",
  "source_path": "guide.md",
  "doc_type": "source_document"
}
```

When `--create-table` is used, the example creates:

- `document_units`: a graph index backed by the `document_units_v1` extraction
  artifact.
- `document_text`: a full-text index over `document_chunks_v1`.
- `document_vectors`: a managed vector index over `document_chunk_dense_v1`,
  generated from `document_chunks_v1`.

PDFs and slide decks are partitioned by Antfly's `document_extraction` producer
before chunking. PDFs produce page units; slide decks produce slide units; chunks
are derived from those units.

## Build

From the repository root:

```bash
(cd go/pkg/docsaf && go build -o ../../../examples/docsaf/docsaf ./cmd/docsaf)
```

## Commands

Prepare source rows into JSON:

```bash
./docsaf prepare \
  --dir ./docs \
  --base-url s3://docs-bucket \
  --output docs.json
```

Load prepared rows:

```bash
./docsaf load \
  --input docs.json \
  --table docs \
  --create-table
```

Traverse and load in one command:

```bash
./docsaf sync \
  --dir ./docs \
  --base-url s3://docs-bucket \
  --table docs \
  --create-table
```

For local smoke tests, inline file bytes as `data:` URLs:

```bash
./docsaf sync \
  --dir ./docs \
  --inline-content \
  --table docs \
  --create-table
```

Authorize Google Drive access for a personal Drive account:

```bash
./docsaf auth google-drive \
  --client-secret ./client_secret.json
```

Then sync a Drive folder:

```bash
./docsaf sync \
  --source google-drive \
  --drive-folder "https://drive.google.com/drive/folders/..." \
  --inline-content \
  --table docs \
  --create-table
```

Developers with the Google Cloud CLI can also use Application Default
Credentials:

```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/drive.readonly

./docsaf sync \
  --source google-drive \
  --drive-folder "https://drive.google.com/drive/folders/..." \
  --inline-content \
  --table docs
```

Service accounts are also supported for shared folders:

```bash
./docsaf sync \
  --source google-drive \
  --drive-folder "https://drive.google.com/drive/folders/..." \
  --drive-credentials ./service-account.json \
  --inline-content \
  --table docs \
  --create-table
```

## Flags

Source flags:

- `--source`: source type, `filesystem` (default) or `google-drive`.
- `--dir`: directory containing source documents.
- `--base-url`: fetchable URL prefix for source documents.
- `--inline-content`: encode source bytes into `data:` URLs for local smoke
  tests and private sources.
- `--max-inline-bytes`: maximum bytes per source row when using
  `--inline-content`. Defaults to 3 MiB for filesystem sources and 100 MiB for
  Google Drive sources.
- `--id-prefix`: optional stable prefix for source document IDs.
- `--include`: include pattern; repeatable and supports `**`.
- `--exclude`: exclude pattern; repeatable and supports `**`.

Google Drive source flags:

- `--drive-folder`: Google Drive folder ID or folder URL.
- `--drive-token-file`: token cache created by `docsaf auth google-drive`.
- `--drive-credentials`: Google service account JSON or path.
- `--drive-access-token`: pre-obtained OAuth access token; also reads
  `GOOGLE_DRIVE_ACCESS_TOKEN`.
- `--drive-concurrency`: parallel Drive downloads.
- `--drive-include-shared-drives`: include shared/team drives.

If no explicit Drive credentials or docsaf token cache are configured, docsaf
falls back to Google Application Default Credentials. If the docsaf token cache
exists but is corrupt, docsaf warns and still tries Application Default
Credentials.

Auth flags:

- `docsaf auth google-drive --client-secret`: OAuth client secret JSON for a
  Google installed/desktop app.
- `docsaf auth google-drive --token-file`: where to write the token cache.
- `docsaf auth google-drive --port`: local OAuth callback port; `0` chooses a
  free port.

Load/sync flags:

- `--url`: Antfly API URL, default `http://localhost:8080/db/v1`.
- `--table`: table name, default `docs`.
- `--create-table`: create the table with derived hierarchy indexes.
- `--num-shards`: number of shards for a new table.
- `--batch-size`: linear merge batch size.
- `--max-request-bytes`: maximum encoded linear merge request bytes.
- `--token`: bearer token for Antfly Cloud auth; also reads `ANTFLY_TOKEN`
  or `ANTFLY_AUTH_TOKEN`.
- `--chunk-size`: target size for unit-derived chunks.
- `--chunk-overlap`: overlap for unit-derived chunks.
- `--embedding-provider`: embedding provider for managed vector search (`antfly` by default; any Antfly SDK embedder provider is supported).
- `--embedding-model`: embedding model for managed vector search (`antflydb/clipclap:gguf:Q4_K` by default).
- `--embedding-config-json`: full Antfly SDK `EmbedderConfig` JSON for provider-specific settings; overrides `--embedding-provider` and `--embedding-model`.
- `--embedding-dims`: expected embedding dimension; `0` lets Antfly probe.
- `--dry-run`: preview linear merge changes without applying them.

## Notes

Production sync should use URLs Antfly can fetch directly, such as S3 or HTTPS.
Inline content is useful for small local tests only.

For private Google Drive folders, use `--inline-content` unless Antfly can fetch
the emitted Drive links directly. The CLI authenticates locally to traverse and
download Drive files; the derived document extraction worker later reads the
source row URL from Antfly. Drive sync raises the default inline limit to 100
MiB to match the Drive download cap. For larger files, stage the content behind
Antfly-readable URLs and sync it with a source that does not require Drive
downloads.

The source-row design is documented in
[`go/pkg/docsaf/DOCSAF.md`](../../go/pkg/docsaf/DOCSAF.md).
