# Standalone SQLite to Antfly Lite migration

`antfly-migrate` is a separate Zig executable in `zig/pkg/antfly-migrate`.
It links SQLite's C API and the public embedded Antfly C ABI. Neither SQLite
nor migration code is linked into the Antfly server or Lite CLI by this change.
The migration build steps are explicit and are not part of default installation.

The version 1 implementation imports explicitly mapped relational rows into a
new `.aflite` file. It accepts application-owned SQL schema statements or a
stable snapshot of an existing aflite schema seed. It never writes directly to
Antfly storage pages or duplicates the SQL/catalog implementation.

## Ownership and extension boundaries

The plan describes source selection, target tables, column mappings, checked
conversions, and verification keys. The runner owns preflight, bounded copying,
verification, and publication. `sqlite.zig` owns source snapshots and typed
values; `aflite.zig` owns the embedded SQL transport and its value encoding.

A later source adapter should supply typed cells and a consistent source cut;
a later destination adapter should supply schema binding, atomic batches, and
verification reads. Keep those interfaces grounded in the first concrete
adapters before introducing plugin loading or a generalized migration framework.
Source SQL handles projections, joins, constants, and computed fields. The
built-in transformations remain explicit and small.

## Plan contract

Plans are strict JSON with `version: 1`; unknown fields and unsupported versions
fail. All paths resolve relative to the plan file, independently of the process
working directory. See the executable examples in
[`zig/pkg/antfly-migrate/examples`](../../zig/pkg/antfly-migrate/examples).

- `sources`: named SQLite files. Source connections are read-only. Every query
  must be one parameter-free, read-only SELECT usable as a derived table.
- `destination`: a new file ending in `.aflite`. Existing files, directories,
  and dangling symlinks are refused; there is no overwrite flag.
- `schema`: exactly one of `statements` or `seed`. Statements run in listed
  order in the isolated destination, outside a transaction, matching Antfly
  DDL semantics. They are trusted application configuration, not translated
  SQLite DDL. A seed is copied through Antfly's stable snapshot API; the seed
  itself remains unchanged. Mapped target tables must be empty; unmapped seed
  tables are retained.
- `tables`: source name, deterministic source query, target table, ordered
  column mappings, and a nonempty verification key. Each target appears once.
  List parents before children when target foreign keys require that order.
- `write_mode`: `insert` (default) requires an empty mapped target table;
  `upsert` applies only mapped columns through the declared key and preserves
  omitted columns and unrelated existing rows in a schema seed. Omitted
  non-null columns still need insertion defaults before conflict handling.
- `columns`: target `name`, source projection `from`, `type`, optional
  `nullable` (default true), and optional `transform` (default `identity`).
- `key`: mapped target columns forming a non-null, unique source key. Integer,
  text, and boolean identity mappings are accepted. Use a target PRIMARY KEY
  or UNIQUE index matching this key for efficient verification.
- `batch_rows`: 1–1,000, default 100. `batch_bytes`: 1 KiB–8 MiB, default 1 MiB.
  Requests exceeding the byte limit split into smaller atomic batches; a
  single row that cannot fit fails. These are encoded row/request bounds,
  not a process-wide memory quota. JSON parsing and SQLite query execution
  have their own allocation costs.
- `verification_rows`: 1–1,000, default 100. Verification groups source keys
  independently of write batches, with the same retained-byte bound.

Identifiers are restricted to ASCII letters, digits, and underscores, with a
letter or underscore first. Source aliases allow mapping broader original
SQLite names without ambiguous cross-dialect quoting.

## Type preservation

SQLite's declared column affinity does not guarantee the stored value's type.
The migrator checks the storage class of every selected cell.

| Plan type | Accepted SQLite values | Destination |
| --- | --- | --- |
| `integer` | INTEGER | signed 64-bit integer, never passed through a float |
| `real` | finite REAL or exactly representable INTEGER up to ±2^53 | number |
| `boolean` | INTEGER 0 or 1 | boolean |
| `text` | valid UTF-8 TEXT | string |
| `json` | valid JSON in UTF-8 TEXT | JSONB |
| `text` with `base64` | BLOB | explicitly encoded text |

SQL NULL is accepted only for nullable columns and never for keys. JSON `null`
is a JSON value distinct from SQL NULL: JSON parameters are serialized and cast
as JSONB. Verification uses the SQL response's independent `sql_nulls` flags.
The Antfly SQL ABI returns integer cells as decimal strings and JSON cells as
serialized JSON strings; the sink decodes them by declared column type.

BLOBs, timestamps, and application enums have no guessed interpretation.
Date/time values can be selected as integer epochs or explicit text. Triggers,
sequences, collations, generated expressions, FTS virtual tables, and views are
not automatically translated. Applications supply an authoritative target
schema and choose which logical rows to select.

## Execution and publication

1. Validate the plan and reject an occupied destination.
2. Open source read transactions and force a snapshot immediately on each
   connection. Hold them through preflight, copy, and verification.
3. Check source projection columns, key uniqueness, nullability, conversions,
   and encoded row size with a full streaming preflight scan.
4. Create a randomly named sibling staging directory with mode 0700. Create
   the staged aflite database or copy the schema seed. Bind target columns,
   check their types, and require empty mapped target tables.
5. Copy bounded batches with parameterized multi-row INSERT statements.
   Every submitted statement is an atomic target mutation. Incomplete and
   unknown mutation outcomes fail without publishing or retrying.
6. Compare target counts and every mapped cell by source key in bounded key
   groups. Match responses by typed keys independently of response ordering,
   rejecting missing/duplicate keys. JSON object member order is ignored.
   Insert mode verifies the entire target row count; upsert mode verifies all
   selected keys while allowing unrelated destination rows.
7. Close the database, run Antfly's file integrity check, restrict the file to
   mode 0600, and fsync it. Atomically hard-link it to the destination path,
   refusing a racing creator, then fsync the destination directory. This
   requires a local POSIX filesystem supporting hard links and directory sync.
8. Remove the unpublished staging directory on normal success or failure.
   A process crash may leave an isolated staging directory; version 1 does
   not resume it. Failure after the publication link reports
   `PublishedDurabilityUnknown`; inspect the destination before retrying.

`--dry-run` performs source preflight and creates an ephemeral staged schema to
bind target columns and types. It does not copy rows or publish the destination.
It does not prove all target row constraints will accept the data; those checks
are enforced during actual INSERT and verified before publication.

`verify --plan seed-plan.json` requires a schema seed, takes an isolated stable
snapshot and validates its mapped values without copying rows or publishing a
destination. Write batches are additionally bounded by SQL parameter and AST
preparation budgets. SQL error reporting includes SQLSTATE/schema column
identifiers without printing row values or diagnostic text.

Read transactions provide one stable cut per source database, not a common cut
across multiple files. Long reads can retain WAL pages or delay writers on
rollback-journal databases. Stop the source application for a multi-file
migration, especially one combining SQLite with filesystem history. Queries
must be deterministic across repeated execution against the same source cut;
random/time-dependent expressions are outside this contract.

## Codex integration

The `antfly/integration` Codex checkout already owns an authoritative target
schema in `codex-rs/antfly/src/schema.rs` and a history-aware importer in
`codex-rs/antfly-import`. Its relational state maps to `codex_*` tables, while
searchable rollout items live in document tables.

Keep Codex's complete plan and schema version checks in Codex. A future Codex
orchestrator should capture the stopped application's sources, initialize the
schema, import relational state, reconstruct rollout history with the existing
Rust importer, drain search indexing, verify application behavior, and publish
one completed file. The generic runner's relational publish is not itself a
complete Codex-home cutover.

Define one writer for each table touched by both relational copying and history
reconstruction, especially threads, sections, and history projections. Preserve
metadata deliberately; rebuild derived projections deliberately. Fork/revert
lineage flattening, protocol item conversion, trigger replacements, revision
counters, and memory-file discovery remain Codex responsibilities.

The [complete Codex profile](codex-sqlite-migration.md) exports the authoritative
target DDL, covers every runtime table, captures source snapshots and emits a
metadata overlay after history replay. It records the importer's current
timestamp-preservation limitation as an application acceptance gate. Existing
SQLite migration bookkeeping must not be copied into `codex_schema_migrations`
as if Antfly migrations had run.

## Subsequent work

- SQLite inspection and automatic plans using `sqlite_schema`, `table_xinfo`,
  `index_list`/`index_xinfo`, and `foreign_key_list`, with unresolved semantics
  reported explicitly. Never equate a successful row copy with schema parity.
- Durable source snapshots through SQLite's backup API, identified by content
  digest, to support restart without recapturing a different source cut.
- Resume checkpoints committed in the same transaction as destination batches,
  bound to source snapshot, plan hash, target schema, and an ordered stable key.
  Use keyset pagination rather than OFFSET. Tables without usable keys need an
  explicit snapshot-local identity policy.
- Batched key verification/canonical typed hashes for larger datasets, and
  read-only plan reports with target constraint preflight where supported.
- JSONL/document source and sink support, then remote Antfly destinations.
- Codex orchestration and application-level acceptance fixtures in the Codex
  repository, reusing its history reconstruction rather than porting it to Zig.
