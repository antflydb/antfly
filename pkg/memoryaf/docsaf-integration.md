# docsaf Integration

This package should treat `docsaf` as a document extraction layer, not as a competing storage model.

The durable pattern is:

1. `docsaf` traverses a backend and produces `DocumentSection`s.
2. An adapter maps each section into a `memoryaf` record.
3. The external document remains canonical.
4. `memoryaf` stores searchable memory plus a stable reference back to the source.

## Why This Shape

Markdown-backed memory systems usually want three things at once:

- editable files as the source of truth
- stable links back to the exact note or heading
- semantic retrieval across many files and backends

`docsaf` already solves the extraction side. `memoryaf` solves the retrieval side. The bridge is a stable source reference on each memory.

## Source Reference Contract

Use these fields on `StoreMemoryArgs`:

- `source_backend`
- `source_id`
- `source_path`
- `source_url`
- `source_version`
- `section_path`

Recommended meanings:

- `source_backend`: backend family such as `filesystem`, `git`, `s3`, `google_drive`, or `web`
- `source_id`: stable backend-native identifier
- `source_path`: human-meaningful path within the backend
- `source_url`: canonical openable URL
- `source_version`: version marker for change detection
- `section_path`: heading hierarchy inside the document

## Backend Mapping

Filesystem:

- `source_backend=filesystem`
- `source_id=filesystem:<relative-path>`
- `source_path=<relative-path>`
- `source_url=<docsaf base URL + path>` when available
- `source_version=<optional mtime or content hash>`

Git / GitHub:

- `source_backend=git`
- `source_id=<repo-url>@<ref>:<path>`
- `source_path=<repo-relative-path>`
- `source_url=<blob URL from docsaf>`
- `source_version=<commit SHA when known, otherwise ref>`

S3:

- `source_backend=s3`
- `source_id=s3://<bucket>/<key>`
- `source_path=<prefix-stripped key>`
- `source_url=<s3://... or configured base URL>`
- `source_version=<etag, version ID, or last modified marker>`

Google Drive:

- `source_backend=google_drive`
- `source_id=<drive file ID>`
- `source_path=<folder-relative path>`
- `source_url=<webViewLink or docsaf-derived URL>`
- `source_version=<revision or modifiedTime>`

Web:

- `source_backend=web`
- `source_id=<canonical URL>`
- `source_path=<URL path>`
- `source_url=<canonical URL>`
- `source_version=<etag, last-modified, or crawl timestamp>`

## Markdown Systems

For markdown-file memory systems specifically:

- keep the markdown file authoritative
- store one memory per section, decision, or extracted fact
- set `section_path` from `docsaf.DocumentSection.SectionPath`
- set `source_url` to the heading URL when available
- use `source_id` plus `source_version` to detect stale memories during re-ingest

Do not make `memoryaf` rewrite markdown files directly unless you intentionally want a bidirectional sync system. That is a separate problem from retrieval.

## Re-ingest Strategy

For future sync jobs, use this identity model:

- matching key: `source_id + section_path + memory_type`
- freshness check: compare `source_version`
- update policy:
  - if source version changed, update the memory content in place when the meaning is still the same
  - if the section materially changed meaning, write a new memory and set `supersedes`
  - if the source disappeared, either delete the memory or mark it stale in a higher-level workflow

## Session Memory

External source references also work for ephemeral memories. That lets an agent keep short-lived session notes tied to a specific document section without polluting long-term memory.

Recommended use:

- persistent memory for stable facts, runbooks, decisions, and conventions
- ephemeral memory for in-progress notes, temporary summaries, and active debugging context
