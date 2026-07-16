# Antfly server benchmark fixture

This fixture targets the normal single-node `antfly swarm` public HTTP API.
Create `antfly-benchmark` with `create-table.json`, then load the canonical
corpus with `tools/load_antfly_search_benchmark.py`. The explicit schema is a
benchmark correctness requirement: it indexes `body` once as standard text and
keeps `corpus_ordinal` numeric. Do not replace it with the schema-less default,
which also creates a `body.exact` keyword subfield for bodies up to 1 KiB and
therefore measures an additional index that Tantivy and Quickwit do not build.

Use `http://127.0.0.1:8080/db/v1` as the benchmark and loader base URL. The
request templates are relative to that normal public API prefix.

The loader preserves the kernel benchmark's zero-based corpus ordinal. Normal
batches use `sync_level=write`, matching the declared process-durable profile;
the final batch uses `sync_level=full_index` so timed loading does not return
until the corpus is searchable. Search requests select only the reserved
`_id` field. Responses therefore contain IDs and scores (plus the API's empty
`_source` object), never stored document bodies.

`schema-v2.json` is the immutable migration fixture for a corpus that was
already created with schema version 1. It has identical validation and runtime
indexing semantics; its `required` array is reordered because the public API
derives versions from a document-schema change and ignores a caller-supplied
version by design. Apply it with `PUT
/db/v1/tables/antfly-benchmark/schema`; the server must retain the prior read
generation until `full_text_index_v2` is complete. This fixture exists to test
real production migration and must not be used to rewrite version 1 in place.

The process-durable server command is structurally:

```sh
zig-out/bin/antfly swarm \
  --host 127.0.0.1 \
  --port 8080 \
  --health-port 4200 \
  --data-dir /path/to/data
```
