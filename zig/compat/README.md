# Compatibility Corpus

This directory holds the shared Go/Zig correctness corpus for the Antfly DB
surface. The corpus is language-neutral by design.

## Rules

- The canonical corpus is plain files, not Go structs and not Gob.
- `datadriven` is allowed as a Go runner frontend, but not as the shared data
  format.
- Each case must be runnable against both the Go backend and the Zig `storage/db`
  package.
- Outputs are compared after normalization, not by raw engine response payloads.

## Case Layout

Each case lives under `compat/cases/<area>/<name>/` and may contain:

- `schema.json`: optional DB or document schema for the case.
- `enrichments.json`: optional explicit shared enrichment definitions to register before indexes.
- `indexes.json`: index definitions to register before writes.
- `ops.ndjson`: ordered write and delete operations.
- `queries.json`: search requests to execute after the operations.
- `expected.json`: normalized expected results.

Supported ops in `ops.ndjson`:

- `{"op":"batch",...}` for writes/deletes
- `{"op":"reopen"}` to close and reopen the DB before subsequent operations
- `{"op":"begin_transaction",...}`
- `{"op":"write_transaction",...}`
- `{"op":"commit_transaction",...}`
- `{"op":"abort_transaction",...}`

`batch` ops may also include `sync_level`:

- `write`
- `full_index`
- `async_index`

`batch` ops may additionally include:

- `timestamp_ns` to force the write timestamp used for `:t` metadata / TTL
- `predicates` for optimistic version checks

Transaction ops use:

- `txn` for a stable alias within the case
- `timestamp_ns` on begin/commit/abort
- `predicates` and optional `expect_error` on `write_transaction`

## Normalized Results

Expected result files should compare only stable fields:

- `total_hits`
- ordered hit IDs
- optional `graph_results` keyed by query name
- optional scores or distances, with tolerance handled by the runner
- optional selected aggregation buckets

Do not compare backend-specific metadata, timings, or raw serialization.

## Request Shape

The shared runners support both the legacy single-query shape and the newer
Antfly-style composed request shape. The composed shape uses:

- `full_text`
- `full_text_queries`
- `dense_queries`
- `sparse_queries`
- `graph_queries`
- `merge_config`
- `return_mode`
- `fields`
- `include_all_fields`

Non-search requests can also use:

- `lookup`
- `get_timestamp`
- `get_transaction_status`

Graph query `result_ref` values should use Antfly-compatible names such as:

- `$full_text_results`
- `$aknn_results.<index_name>`
- `$graph_results.<query_name>`

Fusion weights in `merge_config.weights` should use:

- `full_text` for the full-text result set
- embedding index names for dense/sparse result sets

Chunk-backed search requests may set:

- `return_mode = "parent"`
- `return_mode = "chunk"`
- `return_mode = "parent_with_chunks"`
- `max_chunks_per_parent`

Expected hits may include nested `chunk_hits` when parent-grouped chunk search
results are being validated.

Lookup expectations use:

- `found`
- `json`

Timestamp expectations use:

- `timestamp_ns`

Transaction expectations use:

- `status`

## Current Zig Limitations

- Full-text, dense-vector, and sparse-vector indexing are synchronous through
  `DB.batch()`.
- Index catalog persistence is implemented for text, dense, and sparse indexes.
- Dense-vector doc-key mappings are stored in DB metadata so KNN hits can be
  hydrated back to source documents.
- Graph query routing is wired through the DB layer, including `result_ref`
  expansion from prior result sets.
- The shared corpus currently covers full-text, dense, sparse, fusion, graph
  expansion, chunk-backed full-text/dense result shaping, lookup/projection,
  and timestamp/TTL visibility on the Zig side.
- The shared corpus still does not cover split-aware behavior or distributed
  ownership/lease semantics.
- The Go compat runner still does not support ordinary batch predicates and
  still treats some chunk-return shaping as unsupported.

## Runners

- Zig runner: `~/bin/zig build compat`
- Go runner: from the `antfly` repo root, run
  `GOCACHE=/tmp/go-build go run ./cmd/compat-runner zig/compat/cases`

## HBC Isolate

For raw HBC engine comparison, use the isolate tools instead of the DB/adapter
benchmarks.

- Zig isolate: from `antfly/zig`, run
  `ZIG_GLOBAL_CACHE_DIR=.zig-global-cache zig build hbc-isolate -- --storage-backend lsm --docs 8192 --dims 128 --queries 25 --repeats 10 --k 10`
- Go isolate: from the `antfly` repo root, run
  `GOCACHE=/tmp/go-build go run zig/compat/hbc_compare_go/main.go --docs 8192 --dims 128 --queries 25 --repeats 10 --k 10`

The Go isolate is intentionally run from the `antfly` module root so it can
reuse the main repo's `go.mod` / `go.sum` without introducing a second copy of
the full dependency graph.

There is also a standalone helper module in `compat/hbc_compare_go/` for
isolated local runs if you explicitly want to execute it from `antfly/zig`
instead.

## Dense Ingest Isolate

For primary-store ingest comparisons against Pebble on the same document shape
used by `bench/vectors/dense_stack_bench`, run:

- Zig dense ingest:
  `zig build dense-stack-bench -- --ingest-only --docs 50000 --dims 1536 --batch-size 500 --sync-level write`
- Pebble dense ingest:
  `cd compat/pebble_overwrite_go && go run ./cmd/dense_ingest --docs 50000 --dims 1536 --batch-size 500`

The Pebble helper intentionally measures the same `{"embedding":[...]}` JSON
payload shape and emits per-batch plus final ingest-summary JSON so the Zig DB
path can be compared against a Pebble-only baseline without involving HBC or
the Go Antfly catalog/runtime.
