# Benchmarks and build tools

Run these commands from `zig/`. Artifact targets build and install into `zig-out`; execute the installed binary to run a benchmark or tool. Build flags belong to `zig build`, and runtime arguments belong to the binary.

The commands below preserve the former build-run defaults. Replace the arguments with your chosen workload. File arguments remain relative to the working directory.

| Build target | Run command with previous defaults |
|---|---|
| `algebraic-bench` | `./zig-out/bin/algebraic_bench --docs 20000 --repeats 25 --batch-size 500` |
| `algebraic-summary` | `./zig-out/bin/algebraic_summary` |
| `antfly-storage-db-bench` | Installs `db_query_bench`, `db_write_bench`, and `db_doc_set_bench`; their previous workloads are listed below. |
| `antfly-storage-db-derived-bench` | `./zig-out/bin/derived_log_bench` |
| `antfly-storage-wal-bench` | `./zig-out/bin/wal_bench` |
| `artifact-rebuild-bench` | `./zig-out/bin/artifact_rebuild_bench` |
| `backend-bench` | `./zig-out/bin/backend_bench --samples 3 --keys 20000 --value-size 128 --hit-repeats 3 --miss-repeats 3 --scan-repeats 5` |
| `batch-bench` | `./zig-out/bin/batch_bench` |
| `bench` | `./zig-out/bin/bench` |
| `bench-tokenizer` | `./zig-out/bin/tokenizer_benchmark` |
| `db-split-bench` | `./zig-out/bin/db_split_bench` |
| `dense-ingest-guardrail` | `./zig-out/bin/dense_ingest_guardrail --docs 5000 --dims 1536 --batch-size 500 --sync-level write --status-probe-every 1 --max-dense-lsm-run-bytes 1073741824 --max-dense-l0-runs 64 --max-status-probe-ns 500000000` |
| `dense-profile-summary` | `./zig-out/bin/dense_profile_summary` |
| `dense-stack-bench` | `./zig-out/bin/dense_stack_bench` |
| `graph-pattern-bench` | `./zig-out/bin/graph_pattern_query_bench --mode exact --fanout 10000 --tags-per-post 8 --target-degree 100000 --match-every 10 --warmup 5 --samples 30` |
| `hbc-bench` | `./zig-out/bin/hbc_bench` |
| `hbc-isolate` | `./zig-out/bin/hbc_isolate` |
| `hbc-leaf-debug` | `./zig-out/bin/hbc_leaf_debug` |
| `hbc-parity` | `./zig-out/bin/hbc_parity` |
| `hbc-read-bench` | `./zig-out/bin/hbc_read_bench --samples 3 --vectors 10000 --dims 128 --queries 200 --k 10 --batch-size 1000 --leaf-size 128 --storage host --build both` |
| `hbc-split-bench` | `./zig-out/bin/hbc_split_bench` |
| `hbc-storage-read-bench` | `./zig-out/bin/hbc_storage_read_bench --docs 75000 --dims 512 --queries 1000 --candidates 800` |
| `hbc-trace` | `./zig-out/bin/hbc_trace` |
| `hbc-write-bench` | `./zig-out/bin/hbc_write_bench --samples 3 --vectors 10000 --dims 128 --batch-size 1000 --leaf-size 128 --storage host` |
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
| `provisioned-dense-ingest-guardrail` | `./zig-out/bin/provisioned_dense_ingest_guardrail --docs 50000 --dims 1536 --batch-size 100 --sync-level write --max-bulk-clone-calls 0 --max-bulk-clone-bytes 0 --max-bulk-clone-peak-bytes 0 --max-data-block-cache-bytes 805306368 --max-peak-footprint-bytes 3221225472 --max-ingest-ms 60000` |
| `provisioned-warmup-bench` | `./zig-out/bin/provisioned_warmup_bench` |
| `public-query-guardrail` | `./zig-out/bin/public_query_guardrail --docs 5000 --dims 384 --queries 25 --repeats 10 --k 100 --batch-size 250 --search-threads 5 --sync-level write` |
| `public-query-standalone-guardrail` | `./zig-out/bin/public_query_standalone_guardrail` |
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
| `docid-doc-set-bench` | `antfly-storage-db-bench` | `./zig-out/bin/db_doc_set_bench --samples 1 --repeats 16 --small 32 --medium 1024 --large 16384` |
| `docid-query-bench` | `antfly-storage-db-bench` | `./zig-out/bin/db_query_bench --docs 4096 --queries 16 --repeats 8 --filter-size 256 --limit 32` |
| `docid-write-bench` | `antfly-storage-db-bench` | `./zig-out/bin/db_write_bench --docs 512 --batch-size 128 --body-repeat 1` |
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
public query shapes. `--suite storage` or `--suite public` limits the workload.
The default bounded profile preserves the previous storage case sizes and the
100k-document public workload. `--public-docs 300000` selects the larger workload;
`--storage-arg=--flag` and `--public-arg=--flag` append driver arguments. Use
`--skip-build` to reuse binaries. Results include environment metadata, commands,
exit statuses, stdout/stderr, combined JSONL and summary JSONL.

`antfly-storage-db-bench` installs `db_query_bench`, `db_write_bench`, and
`db_doc_set_bench`. Document identity is a DB workload, with no feature-specific
public target or matrix. Benchmark JSON event names retain their existing schema.
WAL and derived-log are storage components: build `antfly-storage-wal-bench` or
`antfly-storage-db-derived-bench`, then choose worker/async/repeat/stress workloads
with the binary arguments listed above. Enrichment correctness belongs to
`antfly-storage-db-enrichment-test`, narrowed with `--test-filter`.
