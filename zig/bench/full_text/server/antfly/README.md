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
until the corpus is searchable. Search requests ask for no stored fields and
therefore return only result identity/scoring metadata.

The process-durable server command is structurally:

```sh
zig-out/bin/antfly swarm \
  --host 127.0.0.1 \
  --port 8080 \
  --health-port 4200 \
  --data-dir /path/to/data
```
