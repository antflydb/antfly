# Antfly server benchmark fixture

This fixture targets the normal single-node `antfly swarm` public HTTP API.
Create `antfly-benchmark` with `create-table.json`, then load the canonical
corpus with `tools/load_antfly_search_benchmark.py`. The explicit schema is a
benchmark correctness requirement: it indexes `body` once as analyzed text and
keeps `corpus_ordinal` numeric. Do not replace it with the schema-less default,
which also creates a `body.exact` keyword subfield for bodies up to 1 KiB and
therefore measures an additional index that Tantivy and Quickwit do not build.

Use `http://127.0.0.1:8080/db/v1` as the benchmark and loader base URL. The
request templates are relative to that normal public API prefix. The `body`
field explicitly uses Antfly's `simple` analyzer (Unicode-word tokenization and
ASCII lowercasing, with no stop words or stemming). This is the analyzer used
by the embedded Tantivy contract and is the closest built-in match to
Quickwit 0.8.2's default simple/lowercase analyzer; the benchmark analyzer
fixture records the remaining Unicode-tokenization boundary explicitly.

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

`schema-v3-simple.json` exists only to migrate preserved corpora that were
created by the earlier benchmark fixture before it declared `simple` and
therefore inherited Antfly's stemming/stop-word `standard` analyzer. Such a
generation is not comparable to the Tantivy/Quickwit artifacts: its smaller
index is an analyzer-semantic difference, not a format win. New benchmark
tables start with `simple` in version 1 and do not need this corrective step.

The process-durable server command is structurally:

```sh
zig-out/bin/antfly swarm \
  --host 127.0.0.1 \
  --port 8080 \
  --health-port 4200 \
  --data-dir /path/to/data
```
