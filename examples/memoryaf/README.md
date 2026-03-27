# memoryaf + docsaf

This example shows how to turn documentation into `memoryaf` records and keep them synchronized across multiple `docsaf` backends.

It has two modes:

- `sync`: one-shot import/update/delete from `filesystem`, `git`, `s3`, `google-drive`, or `web`
- `watch`: monitor a local directory with `fsnotify` and re-sync after changes

The example uses:

- `docsaf` to parse files into section-level documents from multiple sources
- `memoryaf` to store searchable memories with stable source references
- `fsnotify` to trigger re-syncs when files change

## What It Syncs

Each `docsaf.DocumentSection` becomes a `memoryaf` memory with:

- `source_backend=<filesystem|git|s3|google_drive|web>`
- `source_id=<backend-specific stable document identity>`
- `source_path=<relative path or object key>`
- `source_url=<section URL or backend-native source URL>`
- `source_version=<backend-specific version hint when available>`
- `section_path=<heading hierarchy>`

The sync tool manages only memories created by its configured `--user-id` and `--project`.

## Usage

Start Antfly separately, then run:

```bash
cd examples/memoryaf
GOWORK=off go run . sync \
  --source filesystem \
  --dir ../../docs \
  --url http://localhost:8080/api/v1 \
  --project antfly-docs
```

Watch a directory continuously:

```bash
cd examples/memoryaf
GOWORK=off go run . watch \
  --source filesystem \
  --dir ../../docs \
  --url http://localhost:8080/api/v1 \
  --project antfly-docs \
  --debounce 750ms
```

Sync from Git:

```bash
cd examples/memoryaf
GOWORK=off go run . sync \
  --source git \
  --git-url github.com/antflydb/colony \
  --git-ref main \
  --git-subpath docs \
  --url http://localhost:8080/api/v1 \
  --project colony-docs
```

Sync from S3:

```bash
cd examples/memoryaf
GOWORK=off go run . sync \
  --source s3 \
  --s3-bucket company-docs \
  --s3-prefix handbook \
  --s3-endpoint s3.amazonaws.com \
  --s3-use-ssl \
  --url http://localhost:8080/api/v1 \
  --project handbook
```

Sync from Google Drive:

```bash
cd examples/memoryaf
GOWORK=off go run . sync \
  --source google-drive \
  --drive-folder-id https://drive.google.com/drive/folders/<folder-id> \
  --drive-credentials-json /path/to/service-account.json \
  --url http://localhost:8080/api/v1 \
  --project shared-drive-docs
```

Sync from the web:

```bash
cd examples/memoryaf
GOWORK=off go run . sync \
  --source web \
  --web-start-url https://docs.example.com \
  --web-max-depth 2 \
  --web-max-pages 200 \
  --url http://localhost:8080/api/v1 \
  --project public-docs
```

## Notes

- Watch mode is currently local-filesystem only.
- `sync` now accepts `filesystem`, `git`, `s3`, `google-drive`, and `web`.
- Each backend gets its own stable `source_id` shape so re-syncs update the same memories instead of creating duplicates.
- Because `memoryaf.UpdateMemoryArgs` does not update source-reference fields, the example replaces a memory when immutable source metadata changes.
