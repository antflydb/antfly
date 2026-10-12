# Codex SQLite to Antfly migration profile

The Codex profile in `zig/pkg/antfly-migrate/profiles/codex` generates the full
relational migration and metadata overlay from immutable SQLite captures. It
uses the authoritative schema in the `antfly/integration` Codex checkout,
pinned by a source-file SHA-256 and recorded Git commit. The generic Zig
runner remains application independent.

The profile accounts for all 31 application relational tables, all discovered
source tables, and the three document tables. It records source snapshots and
rollout inventory in a private bundle; it does not switch Codex configuration
or claim a completed application cutover. See the
[generic migration design](sqlite-aflite-migration.md) for batching,
verification, snapshot and publication contracts.

## Sources and mappings

`prepare.py` takes separate `--codex-home` and optional `--sqlite-home` inputs.
The latter must match Codex's configured `sqlite_home` / `CODEX_SQLITE_HOME`;
it defaults to the Codex home. It does not combine unrelated developer stores
in a `sqlite/` subdirectory with the selected runtime databases.

| SQLite source | Antfly destination / disposition |
| --- | --- |
| `state_5.sqlite`: `thread_sections` | `codex_thread_sections`; preserve appearance and seed Pinned only when missing |
| `projects`, `project_roots`, `project_idempotency_keys` | corresponding `codex_*` tables, before dependent thread rows |
| `threads` | `codex_threads`; source metadata, explicit booleans, millisecond timestamp fallback from seconds only for missing/null values |
| `thread_dynamic_tools`, `thread_spawn_edges`, `thread_attachments` | corresponding `codex_*` tables; tools and attachment payloads become JSONB |
| Legacy `thread_artifacts` | attachment rename adapter when it is the sole attachment table (`artifact_type` → `attachment_type`) |
| `guardian_review_feedback` | `codex_guardian_review_feedback`; BLOB → base64 TEXT, `record_bytes = length(record)` |
| `remote_control_enrollments` | `codex_remote_control_enrollments`; nullable boolean remains nullable |
| `external_agent_config_imports` | `codex_external_agent_config_imports`; successes/failures become JSONB |
| `backfill_state`, `rollout_migration_state`, `rollout_migration_skipped_rollouts`, `thread_read_receipts` | corresponding `codex_*` tables |
| `goals_1.sqlite`: goals and continuation deferrals | `codex_thread_goals`, `codex_thread_goal_continuation_deferrals`; preserve budgets, usage, status and times |
| `queue_1.sqlite`: items and revisions | `codex_queued_items`, `codex_queued_thread_revisions`; preserve order and current revision values |
| `memories_1.sqlite`, `memories_v2_1.sqlite` | merge into versioned `codex_stage1_outputs`, `codex_jobs`, `codex_consolidation_progress`, with `v1` / `v2` in primary keys |
| `thread_history_1.sqlite`: turns, items, realtime items, projection state | corresponding `codex_thread_*` relational tables; baseline copy is exact for mapped fields, later replay rebuilds projections |
| `agent_message_board_1.sqlite` | tombstones, channels, posts, subscriptions and opt-outs → `codex_message_board_*`; preserve microsecond timestamps and sequences, derive `is_root = (id = root)` |
| `board_templates` | retain in source snapshot; the integration has no equivalent Antfly template backend |
| Every `_sqlx_migrations` | retain in source snapshots; never treat these as applied Antfly migrations |
| `logs_2.sqlite` | retain separately/in place; Antfly StateRuntime currently uses a no-op log sink |

The target schema includes migrations 1, 20, 21 and 22 from the pinned Codex
schema. The profile applies the actual target DDL, including indexes and
foreign keys, and records these Antfly versions after their DDL. It removes
the schema's initial Pinned INSERT because the mapped section query supplies
the existing row or its absent-source default. A target schema change, unknown
source table or unclassified source column requires review instead of silently
losing data.

Missing optional databases/tables yield explicit empty mappings, not invented
state. Existing sources are backed up with SQLite's online backup API under a
read transaction; the captured files use DELETE journal mode and mode 0600.
The bundle directory has mode 0700. Versioned memory rows are prepared in
`control.sqlite` without conflating v1 and v2 identities. The immutable source
snapshots retain singleton keys and SQLite implementation bookkeeping.

## Phase ownership

1. **Relational baseline (`relational.json`).** Copy the complete mapped
   database state into `01-relational.aflite`, verify every mapped value, and
   publish only this intermediate artifact. It is not yet a resumable Antfly
   Codex home: raw rollout documents and thread-store extra state are missing.
2. **History reconstruction.** Use the Codex-owned importer against the
   captured rollout home, targeting the baseline with `--replace`. It owns
   `codex_history_items`, history projections, `extra`, `next_ordinal`, fork
   lineage, subagent history bounds and multi-agent version. It resolves the
   fork/revert chain and assigns target ordinals. Preserve all skipped/failed
   diagnostics; a successful exit alone does not prove complete coverage.
3. **Metadata restoration (`metadata-overlay.json`).** Stable-snapshot the
   history-populated baseline, upsert only captured SQLite metadata, verify
   it and publish `02-complete.aflite`. Omitted history-owned columns and
   destination-only rows remain intact. Source thread IDs require unique
   target keys; omitted non-null columns must have insertion defaults, as
   with normal PostgreSQL INSERT ON CONFLICT semantics.
4. **Application acceptance and cutover.** Verify history coverage and the
   invariants below, drain indexes, then explicitly configure Codex to use the
   accepted file. Keep the original SQLite and filesystem history until this
   step is accepted. Preparation and validation never perform this switch.

`captured-codex-home/state_5.sqlite` is a separate importer input. Its
`rollout_path` values point at the corresponding copied files, preserving the
SQLite-selected tip when several fork/revert rollouts exist. The relational
snapshot retains the original paths, and the final overlay restores them for
legacy path lookup compatibility. Copying rollouts without rebasing this
importer input can select a different leaf through the importer's fallback.

The three document tables have explicit ownership:

- `codex_history_items`: reconstructed raw protocol items through Codex, not
  fabricated from paginated `item_json` projections.
- `codex_approvals`: initially empty; no corresponding SQLite source exists.
- `codex_memory_notes`: local files in `memories/` and `memories_v2/` remain
  authoritative. Use the Codex memories backend to index notes, retaining
  namespace/path identity. Dense enrichment is optional and follows primary
  import and full-text verification; `--no-semantic` keeps validation local.

## Data-preservation gates

Before a full cutover, stop all Codex clients and keep them stopped throughout
both database and rollout capture. Per-database backups while clients are
running are useful validation inputs but do not establish one application cut.
`capture_history.py --source-quiesced` is an operator assertion of this entire
capture interval. It detects changed rollout sizes/mtimes and missing selected
files, and records a completion receipt only after the copied importer input
is prepared. A failed capture remains isolated and incomplete.

Require every source thread ID to have imported history, resolve all lineages,
and reconcile raw-header history modes with SQLite metadata. For retained
references (attachments, read receipts and turn/item IDs), validate identity
and ordinal changes caused by flattening. SQLite byte offsets are archived in
the snapshots, not reinterpreted as aflite locations: `rollout_byte_offset`,
`rollout_end_byte_offset` and `next_rollout_byte_offset` have no target meaning.

**The existing Codex importer is not a lossless timestamp converter.** Its
`ImportedThread.items` discards each outer `RolloutLine.timestamp`, and
`projection::apply_batch` uses import time as the fallback for events without
an explicit lifecycle timestamp. A complete lossless cutover requires a
Codex-owned timestamp-preserving replay path and validation against captured
`created_at_ms`, `started_at_ms`, `completed_at_ms`, turn start/end/duration and
realtime creation times. Do not overlay old projection ordinals after
flattening to hide this difference. The plan records this acceptance gate;
it does not authorize accepting rewritten historical times.

Also check sections (including appearance/position), projects, attachments,
goals and continuation deferrals, queue order/revisions, both memory versions,
message-board ordering/tombstones/subscriptions, thread read receipts, and
external-agent import records. Reopen and integrity-check the final file.
The manifest stays `cutover_ready: false` until full history and application
acceptance are performed; the preparation scripts do not automatically certify
those checks. Nonempty unsupported board templates need an application
compatibility decision, not silent deletion.

## Local validation scope

The October 11 local capture contained 1,590 thread records, 48 goals, 48 spawn
edges, five remote-control enrollments, one section and 857,528 projected
history items. It inventoried 1,658 rollout files totaling about 31.6 GB, with
no missing SQLite-selected rollout paths. The v2 memories database and message
board database were absent; these were classified as absent, not populated
with guessed values. Both memory namespaces are covered by fixtures.

The full 31-table plan passed preflight against the real captured SQLite data,
including every projected item. A separate 27-table validation baseline
excluding the large history projections was actually copied and exactly
verified, including all thread metadata and goals. History replay and metadata
overlay were exercised separately on captured legacy and paginated samples.
This evidence is not a full 31 GB history import, a common stopped-application
capture, or application acceptance. The runnable private bundle is under
`/private/tmp`; its source data and identifiers are not checked into Git.

Counting rows in the local log database reported `database disk image is
malformed`. It was neither repaired nor changed. This does not invalidate the
separately validated runtime stores, but the existing logs need independent
handling if they are to be archived or recovered.

Wide tables and large real metadata exposed two compatibility boundaries:
SQL statements are limited to 1,024 parameters, and older ABI 3 libraries have
smaller AST/request/transaction budgets. The runner caps mapped cells per
batch, and verification fetches bounded key groups rather than one query per
row. Use a recent `libantfly` built from main with the expanded SQL limits;
ABI 3 alone is not proof that a historical build accepts this dataset. Never
truncate metadata to accommodate an older library.
