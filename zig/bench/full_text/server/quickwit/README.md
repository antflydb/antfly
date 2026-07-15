# Quickwit server benchmark fixture

This fixture targets the Quickwit 0.8 REST contract. Create the index by
posting one explicitly named storage profile to `/api/v1/indexes`, then ingest
the normalized corpus as NDJSON records with `corpus_ordinal` and `body`
fields:

- `index-config.json` is the search-only profile and sets `store_source=false`.
- `index-config-source-retaining.json` is the product-storage-equivalent
  profile and sets `store_source=true`, matching Antfly's retained primary
  document.

Do not compare their disk totals under one label. Quickwit's local data
directory can also contain both authoritative index splits and a rebuildable
`indexer-split-cache`; the benchmark inventory reports those subtrees
separately instead of treating their sum as the persisted search artifact.

In both profiles the body field itself is indexed with positions and field
norms but is not separately stored. Normal search responses therefore return
the stable ordinal and score without the corpus body. The source-retaining
profile retains the original JSON source as one product-level record. The
optional stored `marker` field exists only so freshness probes can prove that
their own write became searchable.

Run `tools/run_search_server_benchmark.py` once per product and durability
profile, supplying these request templates for Quickwit. Use `--index-command`
for the corpus loader, `--indexed-documents` for load throughput,
`--server-data-dir` for disk footprint, and `--server-pid` for RSS/CPU. Do not
compare this fixture with an Antfly run until the query analyzer and durability
manifests are explicitly compatible; Quickwit's built-in `default` tokenizer
is not identical to Antfly's current ASCII-lowercase analyzer on all Unicode
input.

The endpoint and mapping fields are based on Quickwit's official
[REST API](https://quickwit.io/docs/reference/rest-api) and
[index configuration](https://quickwit.io/docs/configuration/index-config)
documentation.

The reproducible corpus loader is:

```sh
python3 tools/load_quickwit_search_benchmark.py \
  --corpus /path/to/canonical.jsonl \
  --base-url http://127.0.0.1:7280 \
  --index antfly-benchmark
```

It stays below Quickwit's 10 MiB request limit, preserves zero-based corpus
ordinals, and uses `commit=force` only on the final batch so the timed load does
not return until the full corpus is searchable.

`process-durable.json` declares the local single-node profile. Normal and
freshness writes use Quickwit's default `commit=auto`: acknowledgement follows
the ingest write-ahead log, while searchable visibility follows the configured
60-second commit interval. The benchmark freshness timeout must therefore be
greater than 60 seconds. The final corpus-loader request alone uses
`commit=force` to establish a fully searchable load boundary.
