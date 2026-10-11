# Read requirements

Search, HTTP SQL, and PostgreSQL expose the same freshness options through
`read_requirements`. These options apply to writable Iceberg tables. A receipt
identifies one physical table and object generation; it is a minimum write
frontier, not an exact snapshot or a transaction identifier.

The ingestion response includes `read_receipt`. Pass that value back unchanged
as `after_write`; applications should not construct WAL coordinates themselves.
The original top-level receipt fields remain available for compatibility.

```json
{
  "read_requirements": {
    "visibility": "accepted",
    "after_write": {"table_id": 7, "object_generation": 0, "wal_lsn": 42},
    "wait_ms": 5000
  }
}
```

| Visibility | Meaning | Search | SQL |
| --- | --- | --- | --- |
| `accepted` | Durable ingestion WAL, including pending changes | Default | Opt in |
| `committed` | Committed Iceberg snapshot | Unsupported | Default |
| `published` | Published search index generation | Opt in | Unsupported |

Each option is optional. Omitting `visibility` preserves the surface's default.
`after_write` requires the selected visibility to cover the receipt. `wait_ms`
bounds readiness polling, defaults to 5000, and accepts 0 through 60000. Zero
checks readiness once under the statement I/O budget without retrying. Positive
budgets also cap catalog and WAL polling I/O. SQL binding preflight and scan
readiness checks share that budget, including PostgreSQL streaming fallback. Statement deadlines and cancellation still
apply. Unsupported visibility modes fail explicitly.

HTTP SQL accepts the same object on `/sql` requests and prepared executions.
Requirements belong to execution rather than preparation. A receipt for a table
outside the statement, or for an obsolete object generation, fails validation.

PostgreSQL clients can use either the scalar setting or the full JSON setting:

```sql
SET antfly.read_visibility = 'accepted';
SET antfly.read_requirements = '{"visibility":"accepted","after_write":{"table_id":7,"object_generation":0,"wal_lsn":42},"wait_ms":5000}';
SHOW antfly.read_requirements;
RESET antfly.read_requirements;
```

`SET LOCAL`, transaction rollback, savepoints, and `RESET ALL` follow PostgreSQL
setting scope. Prepared and streaming execution use the current settings at
execution time. Setting `antfly.read_visibility` changes only the visibility;
it preserves a receipt and wait budget already set with `read_requirements`.
Reset the full object to clear those options.

An accepted SQL transaction retains its first read cut for each table. Supply a
receipt before that first read. A later requirement ahead of the retained cut
fails with SQLSTATE `40001`; it cannot advance that cut. Restart the transaction
with the required receipt. Normal SQL transaction failure rules still apply.
An unmet readiness requirement returns `53300`; malformed or mismatched receipts
return `22023`, and unsupported visibility returns `0A000`.

The TypeScript `readAfterWrite`, Python `read_after_write`, and Go `ReadAfterWrite`
helpers accept an ingestion receipt and produce accepted read requirements.
They preserve an explicit zero wait budget.

`lake_read` (search), `lake_visibility` (HTTP SQL), and
`antfly.lake_visibility` (PostgreSQL) remain compatibility aliases. The canonical
HTTP object cannot be combined with its legacy alias in one request. MCP exposes
`read_requirements` in its compact query schema. Composed query lookups and graph
reads forward the requirements to their leaf reads; existing snapshot tokens
continue to pin those reads.
