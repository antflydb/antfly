# Benchmarks and build tools

Run these commands from `zig/`. Artifact targets build and install into `zig-out`; execute the installed binary to run a benchmark or tool. Build flags belong to `zig build`, and runtime arguments belong to the binary.

The commands below preserve the former build-run defaults. Replace the arguments with your chosen workload. File arguments remain relative to the working directory.

| Build target | Run command with previous defaults |
|---|---|
| `antfly-storage-bench` | Installs `storage_bench` with DB/query, analytics, ingest, provisioned-ingest, HBC, and summary subcommands; their workloads are listed below. |
| `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench` |
| `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench` |
| `artifact-rebuild-bench` | `./zig-out/bin/artifact_rebuild_bench` |
| `backend-bench` | `./zig-out/bin/backend_bench --samples 3 --keys 20000 --value-size 128 --hit-repeats 3 --miss-repeats 3 --scan-repeats 5` |
| `batch-bench` | `./zig-out/bin/batch_bench` |
| `bench` | `./zig-out/bin/bench` |
| `bench-tokenizer` | `./zig-out/bin/tokenizer_benchmark` |
| `db-split-bench` | `./zig-out/bin/db_split_bench` |
| `antfly-storage-bench` | `./zig-out/bin/storage_bench ingest --docs 5000 --dims 1536 --batch-size 500 --sync-level write --status-probe-every 1 --max-dense-lsm-run-bytes 1073741824 --max-dense-l0-runs 64 --max-status-probe-ns 500000000` |
| `dense-profile-summary` | `./zig-out/bin/dense_profile_summary` |
| `dense-stack-bench` | `./zig-out/bin/dense_stack_bench` |
| `graph-pattern-bench` | `./zig-out/bin/graph_pattern_query_bench --mode exact --fanout 10000 --tags-per-post 8 --target-degree 100000 --match-every 10 --warmup 5 --samples 30` |
| `antfly-storage-bench` | `./zig-out/bin/storage_bench hbc-search` |
| `hbc-isolate` | `./zig-out/bin/hbc_isolate` |
| `hbc-leaf-debug` | `./zig-out/bin/hbc_leaf_debug` |
| `hbc-parity` | `./zig-out/bin/hbc_parity` |
| `antfly-storage-bench` | `./zig-out/bin/storage_bench hbc-read --samples 3 --vectors 10000 --dims 128 --queries 200 --k 10 --batch-size 1000 --leaf-size 128 --storage host --build both` |
| `antfly-storage-bench` | `./zig-out/bin/storage_bench hbc-split` |
| `hbc-storage-read-bench` | `./zig-out/bin/hbc_storage_read_bench --docs 75000 --dims 512 --queries 1000 --candidates 800` |
| `hbc-trace` | `./zig-out/bin/hbc_trace` |
| `antfly-storage-bench` | `./zig-out/bin/storage_bench hbc-write --samples 3 --vectors 10000 --dims 128 --batch-size 1000 --leaf-size 128 --storage host` |
| `json-bench` | `./zig-out/bin/json_bench` |
| `lib-image-bench` | `./zig-out/bin/lib-image-bench image-decode-suite 25` |
| `lib-pdf-bench` | `./zig-out/bin/lib-pdf-bench suite lib/pdf/testdata/simple_text_fixture.pdf 25` |
| `lib-sql-parser-bench` | `./zig-out/bin/lib-sql-parser-bench` |
| `lmdb-commit-compare` | `./zig-out/bin/lmdb_commit_compare` |
| `lsm-backend-bench` | `./zig-out/bin/lsm_backend_bench --samples 3 --keys 20000 --value-size 128 --hit-repeats 5 --miss-repeats 5 --short-scan-len 64 --short-scan-repeats 16 --full-scan-repeats 5 --reopen-repeats 5 --mixed-repeats 3 --storage host --cache both` |
| `lsm-backend-bench-compare` | `./zig-out/bin/lsm_backend_bench_compare --before /tmp/lsm-before.jsonl --after /tmp/lsm-after.jsonl` |
| `lsm-write-bench` | `./zig-out/bin/lsm_write_bench --samples 3 --keys 20000 --hot-keys 1000 --overwrite-rounds 20 --value-size 128 --batch-size 1000 --storage host --mode both` |
| `lsm-write-bench-compare` | `./zig-out/bin/lsm_write_bench_compare --before /tmp/lsm-write-before.jsonl --after /tmp/lsm-write-after.jsonl` |
| `managed-host-wal-bench` | `./zig-out/bin/managed_host_wal_bench` |
| `merge-cost` | `./zig-out/bin/merge_cost_bench` |
| `merge-cycle` | `./zig-out/bin/merge_cycle_bench` |
| `open-bench` | `./zig-out/bin/open_bench` |
| `antfly-storage-bench` | `./zig-out/bin/storage_bench provisioned-ingest --docs 50000 --dims 1536 --batch-size 100 --sync-level write --max-bulk-clone-calls 0 --max-bulk-clone-bytes 0 --max-bulk-clone-peak-bytes 0 --max-data-block-cache-bytes 805306368 --max-peak-footprint-bytes 3221225472 --max-ingest-ms 60000` |
| `provisioned-warmup-bench` | `./zig-out/bin/provisioned_warmup_bench` |
| `antfly-api-bench` | `./zig-out/bin/api_bench --docs 5000 --dims 384 --queries 25 --repeats 10 --k 100 --batch-size 250 --search-threads 5 --sync-level write` |
| `antfly-api-bench -Dapi-bench-standalone=true` | `./zig-out/bin/api_standalone_bench --mode standalone` |
| `quickstart-bench` | `./zig-out/bin/quickstart_bench` |
| `rabitq-bench` | `./zig-out/bin/rabitq_bench` |
| `raft-apply-bench` | `./zig-out/bin/raft_apply_bench` |
| `recall-harness` | `./zig-out/bin/recall_harness` |
| `regex-bench` | `./zig-out/bin/regex_bench` |
| `replay-bench` | `./zig-out/bin/replay_bench` |
| `rw-lock-bench` | `./zig-out/bin/rw_lock_bench` |
| `search-bench-bitpack-bench` | `./zig-out/bin/search_benchmark_bitpack_bench` |
| `search-bench-codec-bench` | `./zig-out/bin/search_benchmark_codec_bench` |
| `search-impact-layout-analyze` | `./zig-out/bin/search_impact_layout_analyze` |
| `sparse-split-bench` | `./zig-out/bin/sparse_split_bench` |
| `split-bench` | `./zig-out/bin/split_bench` |
| `storage-fixture-promote` | `./zig-out/bin/storage_fixture_promote` |
| `text-segment-write-bench` | `./zig-out/bin/text_segment_write_bench --samples 3 --docs 20000 --batch-size 1000 --terms-per-doc 12 --merge-width 8 --storage host` |
| `wand-skip-bench` | `./zig-out/bin/wand_skip_bench` |
| `lmdb-bench` | Builds both `lmdb_bench_c` and `lmdb_bench_zig`; their invocation presets are below. |

## Presets

These former public run targets are now arguments to the installed binaries. Build the canonical target once before running several cases.

| Former preset | Build target | Run command |
|---|---|---|
| `bench-bge-m3-metal-managed-e2e` | `quickstart-bench` | `./zig-out/bin/quickstart_bench --mode standalone-wiki --model BAAI/bge-m3 --dims 1024 --backend metal --chunk-tokens 200 --batch-size 8` |
| `bench-bge-m3-native-managed-e2e` | `quickstart-bench` | `./zig-out/bin/quickstart_bench --mode standalone-wiki --model BAAI/bge-m3 --dims 1024 --backend native --chunk-tokens 200 --batch-size 8` |
| `bench-image` | `lib-image-bench` | `./zig-out/bin/lib-image-bench image-decode-suite 25` |
| `bench-pdf` | `lib-pdf-bench` | `./zig-out/bin/lib-pdf-bench suite lib/pdf/testdata/simple_text_fixture.pdf 25` |
| `db-split-bench-repeat` | `db-split-bench` | `./zig-out/bin/db_split_bench --samples 5` |
| `derived-log-bench` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench` |
| `derived-log-bench-adaptive` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --adaptive` |
| `derived-log-bench-adaptive-repeat` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 5 --adaptive` |
| `derived-log-bench-adaptive-repeat-long` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 15 --adaptive` |
| `derived-log-bench-adaptive-stress` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 5 --adaptive --sync-delay-us 2000` |
| `derived-log-bench-async` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --async-io` |
| `derived-log-bench-async-repeat` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 5 --async-io` |
| `derived-log-bench-async-repeat-long` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 15 --async-io` |
| `derived-log-bench-async-repeat-stress` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 5 --async-io --sync-delay-us 2000` |
| `derived-log-bench-repeat` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 5` |
| `derived-log-bench-repeat-long` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 15` |
| `derived-log-bench-repeat-stress` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 5 --sync-delay-us 2000` |
| `derived-log-bench-worker` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --worker-thread` |
| `derived-log-bench-worker-repeat` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 5 --worker-thread` |
| `derived-log-bench-worker-repeat-stress` | `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench --samples 5 --worker-thread --sync-delay-us 2000` |
| `docid-doc-set-bench` | `antfly-storage-bench` | `./zig-out/bin/storage_bench doc-set --samples 1 --repeats 16 --small 32 --medium 1024 --large 16384` |
| `docid-query-bench` | `antfly-storage-bench` | `./zig-out/bin/storage_bench query --docs 4096 --queries 16 --repeats 8 --filter-size 256 --limit 32` |
| `docid-write-bench` | `antfly-storage-bench` | `./zig-out/bin/storage_bench write --docs 512 --batch-size 128 --body-repeat 1` |
| `lmdb-fixture-promote` | `storage-fixture-promote` | `./zig-out/bin/storage_fixture_promote` |
| `split-bench-repeat` | `split-bench` | `./zig-out/bin/split_bench --samples 5` |
| `wal-bench` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench` |
| `wal-bench-adaptive` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --adaptive` |
| `wal-bench-adaptive-repeat` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 5 --adaptive` |
| `wal-bench-adaptive-repeat-long` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 15 --adaptive` |
| `wal-bench-adaptive-stress` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 5 --adaptive --sync-delay-us 2000` |
| `wal-bench-async` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --async-io` |
| `wal-bench-async-repeat` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 5 --async-io` |
| `wal-bench-async-repeat-long` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 15 --async-io` |
| `wal-bench-async-repeat-stress` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 5 --async-io --sync-delay-us 2000` |
| `wal-bench-repeat` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 5` |
| `wal-bench-repeat-long` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 15` |
| `wal-bench-repeat-stress` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 5 --sync-delay-us 2000` |
| `wal-bench-worker` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --worker-thread` |
| `wal-bench-worker-repeat` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 5 --worker-thread` |
| `wal-bench-worker-repeat-stress` | `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench --samples 5 --worker-thread --sync-delay-us 2000` |
| `lmdb-bench` | `lmdb-bench` | `./zig-out/bin/lmdb_bench_c --cycles 8 --keys 512 --dups 32 --named-keys 128` |
| `lmdb-bench` | `lmdb-bench` | `./zig-out/bin/lmdb_bench_zig --cycles 8 --keys 512 --dups 32 --named-keys 128` |
| `lmdb-bench-worker` | `lmdb-bench` | `./zig-out/bin/lmdb_bench_zig --cycles 8 --keys 512 --dups 32 --named-keys 128 --worker-thread` |
| `lmdb-bench-async` | `lmdb-bench` | `./zig-out/bin/lmdb_bench_zig --cycles 8 --keys 512 --dups 32 --named-keys 128 --async-io` |
| `lmdb-bench-adaptive` | `lmdb-bench` | `./zig-out/bin/lmdb_bench_zig --cycles 8 --keys 512 --dups 32 --named-keys 128 --adaptive` |
| `lmdb-bench-repeat` | `lmdb-bench` | `./zig-out/bin/lmdb_bench_c --samples 5 --cycles 8 --keys 512 --dups 32 --named-keys 128` |
| `lmdb-bench-repeat` | `lmdb-bench` | `./zig-out/bin/lmdb_bench_zig --samples 5 --cycles 8 --keys 512 --dups 32 --named-keys 128` |
| `lmdb-bench-mmap` | `lmdb-bench` | `./zig-out/bin/lmdb_bench_zig --cycles 8 --keys 512 --dups 32 --named-keys 128 --write-map --map-async` |

## DB query comparisons

From the repository root, `python3 scripts/run_db_query_matrix.py --profile smoke`
builds once and compares storage match-all/full-text/sparse query paths plus six
public query shapes and analytics workloads. `--suite storage`, `--suite public`,
or `--suite analytics` limits the workload.
The default bounded profile preserves the previous storage case sizes and the
100k-document public workload. `--public-docs 300000` selects the larger workload;
`--storage-arg=--flag` and `--public-arg=--flag` append driver arguments. Use
`--skip-build` to reuse binaries. Results include environment metadata, commands,
exit statuses, stdout/stderr, combined JSONL and summary JSONL.

`antfly-storage-bench` installs one `storage_bench` executable. Its subcommands
share a single compiled DB implementation. Document identity is a DB workload,
with no feature-specific public target or matrix. Benchmark JSON event names
retain their existing schema.
WAL and derived-log are storage components: build `antfly-storage-wal-bench` or
`antfly-storage-db-derived-bench`, then choose worker/async/repeat/stress workloads
with the binary arguments listed above. Enrichment correctness belongs to
`antfly-storage-db-enrichment-test`, narrowed with `--test-filter`.

The public-query driver keeps separate measurement boundaries: `--mode handler`
uses the typed query API and measures internal stages; `--mode local` also measures
the production `httpx` routes over HTTP. Standalone mode launches the existing
Antfly executable, which links the shared runtime kernels. The
`api_standalone_bench` artifact omits in-process handler code, so
production qualification can reuse a built Antfly executable without compiling
another copy of its server implementation. Build it with
`zig build antfly-api-bench -Dapi-bench-standalone=true`; the default builds
`api_bench` for internal-stage and local HTTP measurements.

### Analytics comparisons

For direct invocations from `zig/`:

```sh
zig build antfly-storage-bench
./zig-out/bin/storage_bench analytics --docs 20000 --repeats 25 --batch-size 500
./zig-out/bin/storage_bench summary --input /tmp/db-combined.jsonl
```

Algebraic aggregation is a DB workload, with no separate feature target or
matrix. `--suite analytics` adds LSM analytics, adaptive coverage, cold/warm
reads, graph traversal, and matching public-query runs without schema, with
schema only, and with algebraic planning. `--suite all` includes these workloads.
The bounded profile preserves the production sweep's 50k analytics documents,
five repeats, 1k batches, 5k churn operations, 10k graph/public documents,
128-dimensional public vectors, and three public repeats. Smoke uses small data
while retaining all coverage and correctness gates.

```sh
# From the repository root:
python3 scripts/run_db_query_matrix.py --suite analytics --profile smoke
python3 scripts/run_db_query_matrix.py --suite analytics --profile bounded \
  --baseline /tmp/prior/comparison.stderr \
  --summary-arg=--max-algebraic-query-ms-ratio-vs-baseline --summary-arg=1.25
python3 scripts/run_db_query_matrix.py --suite analytics --profile bounded \
  --analytics-arg=--algebraic-bulk-ingest \
  --analytics-arg=--lsm-flush-threshold-bytes --analytics-arg=1048576
```

`--analytics-docs` changes analytics scale (graph capped at 10k), and
`--public-docs` changes public-query scale. Additional per-driver options use
`--analytics-arg`/`--public-arg`; measured absolute limits and baseline ratios use
`--summary-arg`. The matrix logs exact commands, raw results, and parameters and
runs `storage_bench summary` over the combined JSONL. A failed benchmark, missing
result, correctness mismatch, missing coverage, or exceeded limit fails the run.
`comparison.stderr` contains detailed comparisons and can serve as the next
baseline. Existing benchmark event names remain stable, and `matrix_case`
identifies the orchestration case without overwriting the driver's case names.

The former integration wrapper and archive-evidence checker are removed.
Correctness and planner ownership remain in normal DB/API/metadata/graph suites.
Dynamic-template regressions remain in `antfly-unit-test`.

## Vector write regression workloads

The former `antfly-storage-vectorindex-write-test` wrapper is removed. Build
both tools once, then run the same bounded HBC and dense-ingest cases directly:

```sh
zig build antfly-storage-bench
./zig-out/bin/storage_bench hbc-write --samples 1 --vectors 5000 --dims 1536 --batch-size 500 --leaf-size 168 --storage host
./zig-out/bin/storage_bench ingest --docs 5000 --dims 1536 --batch-size 500 --sync-level write --status-probe-every 1 --max-dense-lsm-run-bytes 1073741824 --max-dense-l0-runs 64 --max-status-probe-ns 500000000
```
