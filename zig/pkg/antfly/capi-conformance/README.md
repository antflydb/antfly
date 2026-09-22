# libantfly Conformance Cases

Shared, declarative test cases for the `libantfly` C ABI and every language
binding built on it. Each binding runs the same cases through its own public
API, so the bindings stay behaviorally identical without each one reinventing
its smoke tests.

Runners:

| Runner | Location | Command |
|---|---|---|
| Reference (raw C ABI via `antfly.h`) | `zig/pkg/antfly/src/capi/conformance_runner.zig` | `zig build capi-conformance` |
| Go | `go/pkg/lite/conformance_cgo_test.go` | `go test -tags libantfly -run Conformance` |
| Python | `py/packages/lite/tests/test_conformance.py` | `uv run pytest tests/test_conformance.py` |
| Rust | `rs/crates/lite/tests/conformance.rs` | `cargo test -p antfly-lite --features libantfly --test conformance` |

`zig build lite-test` runs all of them. A new C ABI behavior that bindings
should agree on belongs here as a case, not only in one binding's tests.

## Case Format

Each `cases/*.json` file is one case:

```json
{
  "name": "document_roundtrip",
  "description": "What the case proves.",
  "open": {"create": true, "mode": "writer", "profile": "native", "no_sync": true},
  "steps": [
    {"op": "batch", "timestamp": 1, "writes": [{"key": "doc:a", "value": {"title": "a"}}]},
    {"op": "lookup", "key": "doc:a", "expect": {"json_subset": {"title": "a"}}},
    {"op": "lookup", "key": "doc:missing", "expect": {"error": "ANTFLY_NOT_FOUND"}}
  ]
}
```

A runner creates a fresh temporary directory per case. The database lives at
`<tmp>/db.aflite`. `open` is applied before the first step and uses the same
fields as `reopen` below; `create: true` creates a new database, otherwise an
existing one is opened. Every field of `open` is optional:

| Field | Default | Meaning |
|---|---|---|
| `create` | `false` | Create a new database instead of opening one |
| `mode` | `"writer"` | `"writer"`, `"readonly"`, or `"status_only"` |
| `profile` | `"native"` | `"native"` or `"hosted"` |
| `no_sync` | `false` | Skip fsync (tests use `true`) |
| `busy_timeout_ms` | `0` | Wait this long for another writer's lock |
| `path` | `"db.aflite"` | File name inside the case directory |

Runners open with the binding's options-taking open/create call, which maps
to `antfly_lite_open_with_options` / `antfly_lite_create_with_options`.

## Expectations

A step without `expect` must succeed. Otherwise `expect` may combine:

| Key | Meaning |
|---|---|
| `error` | The call fails with this `antfly_error_code_name`, e.g. `"ANTFLY_BUSY"`. No other key is checked. |
| `json_subset` | The result parses as JSON and contains this value recursively: objects match when every expected key matches, arrays match element-wise with equal length, scalars match exactly. |
| `contains` | Every string is a substring of the raw result text. |
| `not_contains` | No string is a substring of the raw result text. |
| `equals` | The scalar result equals this value (booleans, integers, strings). |

## Operations

Values written as JSON objects in a case (`value`, `request`, `config`,
`schema`) are serialized with the runner's JSON encoder before the call.
"Result" is the JSON text the call returns.

| `op` | Fields | Call | Result |
|---|---|---|---|
| `batch` | `writes: [{key, value?, delete?}]`, `timestamp` | `antfly_db_batch` (typed write intents, no predicates, `sync_level` 0; `delete: true` omits `value`) | none |
| `batch_json` | `request` | `antfly_db_batch_json` | JSON |
| `lookup` | `key` | `antfly_db_lookup_json` | JSON document |
| `scan` | `request` | `antfly_db_scan_json` | JSON |
| `search` | `request` | `antfly_db_search_json` | JSON |
| `stats` | | `antfly_db_stats_json` | JSON |
| `status` | | `antfly_lite_status_json` | JSON |
| `capabilities` | | `antfly_lite_capabilities_json` | JSON |
| `check` | | `antfly_lite_check_json` | JSON |
| `pending_work_stats` | | `antfly_db_pending_work_stats_json` | JSON |
| `run_until_idle` | | `antfly_db_run_until_idle` | none |
| `get_schema` | | `antfly_db_get_schema_json` | JSON |
| `set_schema` | `schema` | `antfly_db_set_schema_json` | none |
| `list_indexes` | | `antfly_db_list_indexes_json` | JSON |
| `add_index` | `config` | `antfly_db_add_index_json` | none |
| `delete_index` | `name` | `antfly_db_delete_index` | boolean: whether it existed |
| `list_enrichments` | | `antfly_db_list_enrichments_json` | JSON |
| `add_enrichment` | `config` | `antfly_db_add_enrichment_json` | none |
| `delete_enrichment` | `kind`, `name` | `antfly_db_delete_enrichment` | boolean: whether it existed |
| `begin_transaction` | `txn_id` (32 hex chars), `timestamp` | `antfly_db_begin_transaction_with_id` (no participants) | none |
| `write_transaction` | `txn_id`, `writes` | `antfly_db_write_transaction` (no predicates) | none |
| `resolve_transaction` | `txn_id`, `status` (`"committed"`/`"aborted"`), `commit_version` | `antfly_db_resolve_intents` | none |
| `transaction_status` | `txn_id` | `antfly_db_get_transaction_status` | string: `"pending"`, `"committed"`, or `"aborted"` |
| `commit_version` | `txn_id` | `antfly_db_get_commit_version` | integer |
| `backup` | | `antfly_lite_backup` | none; the runner keeps the bytes |
| `restore_open` | `path`, open fields | `antfly_lite_restore_backup_json` of the kept bytes to `path` (no replace), then closes the current handle and opens `path` as the current handle | none |
| `reopen` | open fields | closes the current handle, then opens with these fields (default path: the current handle's path) | none |
| `open_second` | open fields | opens another handle while the current one stays open, then closes it if the open succeeded | none |
| `close` | | closes the current handle; only `reopen` or `restore_open` may follow | none |

`batch` timestamps are nanoseconds and must increase within a case.
