# E2E Coverage

This directory holds portable end-to-end tests for Antfly Zig.

Use product-area names for test files. Do not use migration labels like `*_parity.py`.

## Current Coverage

- `test_quickstart.py`
  - serverless quickstart flow
  - text search
  - published query
  - internal artifact inspection
- `test_graph.py`
  - serverless graph neighbors, traverse, and shortest-path queries
  - stateful public graph neighbors, traverse, shortest-path, field-edge extraction, and graph pattern queries
- `test_sparse.py`
  - stateful public sparse-vector import
  - dense+sparse hybrid query coverage
  - merge/pruner coverage
  - reranker coverage
- `test_retrieval.py`
  - stateful public retrieval-agent pipeline queries
  - semantic and hybrid bounded retrieval coverage
  - tree-search coverage from prior retrieval hits and `$roots`
  - JSON generation step coverage
  - bounded classification/confidence/follow-up coverage
  - fixed-body SSE streaming coverage
  - rejection of invalid tree-search requests without start nodes or seed hits
- `test_transactions.py`
  - stateful public multi-shard single-table batch commit
  - atomic multi-key balance transfer semantics
  - repeated multi-shard commit and post-commit recovery/health verification
  - client-timeout multi-shard batch atomicity check
  - stateful public atomic multi-table batch commit
  - mixed insert/delete across tables
  - abort on invalid table without partial writes
  - multi-table transform commit
  - stateful public OCC read/modify/write commit
  - stale-version conflict handling
  - cross-table OCC commit
  - OCC transform commit
  - two-writer OCC lost-update protection
  - concurrent OCC read/modify/write with only one winner
  - session transaction begin/stage/commit with transforms
  - savepoint rollback around staged transforms
  - explicit session read stage with snapshot verification
  - explicit session write stage and commit
  - explicit session delete stage with abort preservation
  - explicit session delete stage and commit
  - mixed explicit session read/write/delete commit
  - session stale-version read conflict response
- `test_schema_migration.py`
  - stateful public schema update
  - migration metadata
  - full-text rebuild from `full_text_index_v0` to `full_text_index_v1`
- `test_index_lifecycle.py`
  - shared backend-agnostic table index lifecycle coverage
  - stateful and serverless both run here when the behavior is part of the shared `/tables/...` contract
  - rejection of non-Go public full-text chunk-source config
  - chunker-driven full-text routing from template chunks, normalized to index-visible semantics
  - serverless-only publication and materialization assertions live here too when they are still part of the index lifecycle area
  - chunk-aware full-text publication-state specific assertions
  - named vector/sparse publication-action visibility during metadata-only republish
- `test_sync_levels.py`
  - shared backend-agnostic `sync_level=full_text` visibility on `/tables/{table}/batch`
  - serverless-specific `sync_level=enrichments` rejection when background materialization is required
- `test_transforms.py`
  - stateful public `$max` update semantics
  - upsert with transforms
  - concurrent `$max` updates converge to the highest value
  - atomic `$inc` counter semantics
  - mixed `$max`/`$inc`/`$addToSet`/`$set` updates
  - serverless transform visibility across `latest` then `published`
- `test_foreign_sources.py`
  - shared backend-agnostic live PostgreSQL foreign-table query coverage
  - filtered foreign-table query coverage
  - unsupported foreign aggregation rejection
  - Antfly-to-Postgres join coverage
- `test_cdc.py`
  - stateful public PostgreSQL CDC snapshot import coverage
  - stateful public PostgreSQL CDC insert/update/delete streaming coverage
  - explicit slot/publication config coverage on the replicated table contract
- `test_backup_restore.py`
  - stateful public table backup to `file://`
  - drop and restore round-trip through `/tables/{table}/backup` and `/tables/{table}/restore`
  - document lookup and full-text query still work after restore
  - cluster `/backup`, `/backups`, and `/restore` round-trip for local `file://` backups
  - backend-gated remote cluster round-trip coverage for `s3://` and `gs://`
## Harnesses

- `serverless_api`
  - serverless-oriented fixture
  - can auto-start `./zig-out/bin/antfly swarm`
- `stateful_api`
  - stateful API fixture
  - intended to run against either Go or Zig implementations
  - when auto-starting Zig locally, it launches `antfly swarm`
  - configured with `ANTFLY_STATEFUL_URL`
  - optional `ANTFLY_STATEFUL_API_ROOT`
  - use `/db/v1` for Go Antfly; local Zig `antfly swarm` serves stateful routes at the root

## Shared vs Serverless-Specific

- Prefer backend-agnostic tests for product-contract behavior.
  - If both stateful and serverless should support the same `/tables/...` behavior, keep it in a shared file like `test_index_lifecycle.py`.
- Keep one product-area file even when some tests are backend-specific.
  - If an assertion is still part of index lifecycle, keep it in `test_index_lifecycle.py` and use `serverless_api` directly for serverless-only publication checks.
- Normalize visibility semantics explicitly in shared tests when needed.
  - Example: chunker-driven full-text routing is shared behavior, but the visibility wait differs by backend today, so the shared test uses index-visible semantics:
    - stateful: `sync_level=full_text`
    - serverless: write then explicit publish
- Keep publication/status checks in the same product-area file when they are part of that lifecycle.
  - Example: chunker-driven full-text searchability and serverless `full_text_source_mode` / publication-action assertions now both live in `test_index_lifecycle.py`.

## Objectstore Integration

`test_backup_restore.py` includes real remote-backend coverage behind the
`objectstore_integration` marker.

Run only those tests with:

```bash
uv run --project e2e/antfly pytest e2e/antfly/test_backup_restore.py -m objectstore_integration -q
```

S3-compatible envs:

```bash
export OBJECTSTORE_S3_INTEGRATION=1
export OBJECTSTORE_S3_TEST_BUCKET=my-test-bucket
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
```

GCS envs:

```bash
export OBJECTSTORE_GCS_INTEGRATION=1
export OBJECTSTORE_GCS_TEST_BUCKET=my-test-bucket
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export GOOGLE_CLOUD_PROJECT=my-project
```

When these envs are absent, the remote backup tests skip cleanly.

## Main Gaps

Compared with `../antfly/e2e`, the biggest missing areas are:

- linear merge
  - Go source: `linear_merge_test.go`
- auth and secrets
  - Go sources: `auth_test.go`, `secrets_test.go`
- broader graph-query depth beyond the current public matrix
  - Go source: `graph_test.go`
- retrieval generation / remote content / providers
  - Go sources: `retrieval_generation_test.go`, `remote_content_test.go`, `builtin_providers_test.go`
- replication and CDC
  - Go sources: `cdc_replication_test.go`, remaining CDC parts of `foreign_table_test.go`
  - Zig now has `test_cdc.py` covering snapshot import, logical-stream
    insert/update/delete, and restart/resume on the unified `swarm` path; the
    remaining gap is stricter exported-snapshot cutover semantics and broader
    CDC parity depth
- cluster-management behavior
  - Go sources: `online_shard_split_test.go`, `autoscaling_test.go`
- additional search/query coverage
  - Go sources: `sparse_test.go`, `retrieval_generation_test.go`

## Near-Term Priorities

1. Expand graph coverage with more Go graph matrix cases.
2. Expand broader stateful retrieval-generation depth beyond pipeline-only retrieval.
3. Broaden backup/restore coverage beyond the current local + remote matrix.
4. Decide whether provider/auth/secrets tests belong in this portable suite or a separate integration suite with external dependencies.
