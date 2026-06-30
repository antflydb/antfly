# Foreign Keys

Foreign keys are relational-table constraints over the committed relational base
store. They are not derived indexes: they must be enforced by the same
transaction boundary that commits relational rows and their reverse-reference
metadata.

The implemented production contract is intentionally narrow: `on_delete:
"restrict"`, bounded nullable-column `on_delete: "set_null"`, and bounded local
`on_delete: "cascade"` foreign keys can reference the parent row's `_id`
document key or a declared unique parent column tuple in the local
relational store. The schema, runtime catalog, persisted catalog, relational
write participant, transaction-intent resolution path, reverse-reference
keyspace, and OpenAPI surface all use that shape. Hosted transaction planning
also registers the referenced parent table/range as a 2PC participant for
enforced immediate single-column `_id` child writes and sends an explicit
parent-existence check in the prepare request. When routed FK-ref owner ranges
exist, hosted insert-only child writes proven by `expected_version: 0` also
register the exact owner range and send an explicit FK-ref write mutation in the
same 2PC. Version-predicated child updates/deletes read the old child row at
planning time, require the row version to match the predicate, and route FK-ref
deletes for old parent keys plus FK-ref writes for new parent keys. Unversioned
full-row child writes read the current child row before prepare, inject either
the observed row version or `expected_version: 0` for a missing row as a
row-participant predicate, and use that proof to route FK-ref deletes for old
parent keys plus FK-ref writes for new parent keys.
Hosted restrict parent deletes require routed FK-ref owner ranges for exact
parent-key checks and fail before prepare when owner topology is absent or
transitional. Hosted primary-key `set_null` and bounded primary-key `cascade`
parent deletes use the same owner topology: the coordinator registers the
FK-ref owner participants as conflict points and writes a deterministic durable
FK action schedule mutation to one owner participant in the same 2PC as the
parent delete. The schedule record stores the action (`set_null` or `cascade`),
constraint, parent table/key, stable action-job id, worker id, and page limit.
After commit, metadata controller maintenance discovers pending schedules,
seeds idempotent action jobs across the current FK-ref owner groups, and each
owner group owns a deterministic child-table/child-key cursor over the FK-ref
prefix. When the child table is a single local group owned by the same DB, the
job uses the DB-local fast path and mutates child rows in the same local
transaction. When child placement can differ from FK-ref ownership, the
table-write source claims the durable owner cursor, executes a bounded
source-layer 2PC that deletes FK-ref rows on the owner participant and rewrites
or deletes exact child rows on their resolved child participants, then advances
the durable owner cursor only after the participant transaction commits. That
gives
cross-table primary-key FKs the same recovery substrate as ordinary relational
writes, makes missing parents fail prepare on the parent participant, and makes
known child references fail prepare, rewrite, or delete on the FK-ref owner or
child participant once the scheduled action drains. Child reference creation,
restrict parent-delete checks, and routed set-null/cascade parent deletes also
stage an internal FK conflict intent
keyed by `(constraint, parent_table, parent_key)`, so concurrent prepares for
the same parent reference conflict before either transaction can commit. Hosted
same-table and cross-table FK references to declared unique parent tuples can
route parent existence checks through configured unique-owner ranges: the
coordinator derives the encoded unique tuple with the same relational row
encoder as the storage participant, registers the unique-owner group as a 2PC
participant, and sends a parent-existence check that names the parent unique
constraint. Hosted parent deletes over declared unique targets read the old
parent row before prepare, inject the observed row version as a row-participant
predicate when the caller did not supply one, encode the deleted unique tuple,
and route `restrict`, `set_null`, or bounded `cascade` enforcement through the
FK-ref owner range for that encoded tuple. Missing unique-owner or FK-ref owner
topology fails closed before prepare. The
transaction prepare contract has explicit FK-ref mutation, child-key set-null,
and child-key cascade surfaces so a routed FK-ref owner participant can durably
write/delete reverse-reference rows without pretending they are user documents,
and an exact child participant can rewrite or delete the child row without
scanning unrelated child ranges.
The metadata and API catalog layers expose FK-ref owner-range records and a
resolver for `(child_table, constraint, parent_table, parent_key) -> owner
groups`, and the metadata control loop treats those owner ranges as first-class
reconciled topology. SQL constraint families outside the implemented foreign-key
and unique-constraint model remain future work.

## Goals

- Enforce referential integrity for relational tables without introducing a
  second authoritative store.
- Reuse the existing transaction/2PC machinery for cross-shard and cross-table
  writes.
- Keep the first production version narrow enough to be exact and durable.
- Leave graph indexes and join planning as query/relationship tools, not as the
  source of truth for integrity constraints.

## Out Of Scope

- General SQL constraint coverage outside foreign keys and unique constraints.
- References to arbitrary non-unique parent columns.
- Alternate FK encodings for this initial feature set. The routed FK-ref
  keyspace is the durable production encoding; future changes should use
  explicit catalog/schema migrations instead of alternate encodings.

## Composite Primary Keys

Composite primary keys should be first-class relational row identity, not a
thin alias for ordinary unique constraints. The durable storage layer can still
keep a hidden physical row key for placement, WAL, and row-version ownership,
but the public relational identity for a table with
`primary_key.columns = ["tenant_id", "order_id"]` is the typed tuple in that
declared order.

Current production shape:

- Table schemas accept an explicit `primary_key` definition with ordered relational
  columns. `_id` remains the default single-column document-key identity for
  tables that do not declare `primary_key`; `_id` cannot be mixed into a
  composite primary-key tuple.
- Every primary-key component must be declared, scalar, non-null, and
  type-stable. Missing, null, array, object, or dynamically typed primary-key
  components fail before prepare.
- Primary-key tuples use the same canonical typed tuple encoder used
  by unique constraints and FK parent tuples. The encoding must include type
  tags, stable column order, and unambiguous absent/null handling so tuple keys
  remain byte-sortable, reproducible after restart, and safe across schema
  reload.
- The reconciler derives a reserved primary-key owner range named
  `__antfly_primary_key__` using the same owner-range model as unique-owner
  ranges. Splits, merges, placement changes, and repair preserve owner-range
  epochs and fail closed when topology is missing or transitional.
- Storage maintains a durable primary-key index mapping encoded primary tuple to the
  hidden physical row key. The primary-key owner participant is the conflict
  point for duplicate detection, parent existence checks, and FK proof rows.
- Primary-key component updates are parent-key updates. They route through the
  existing update action semantics and maintain the primary-key owner row plus
  FK-ref movement; silent in-place key mutation is not allowed.
- Foreign keys can reference a parent table's primary-key tuple by naming the
  parent primary-key columns. The coordinator registers the
  primary-key owner participant, not broad-fanout parent ranges, for existence,
  restrict, set-null, cascade, deferred proof, and repair operations.
- Keep unique constraints as secondary candidate keys. A FK to a composite
  primary key and a FK to a composite unique constraint share tuple encoding and
  FK-ref row shape, but they resolve through distinct catalog owner records.
- Unique-owner integrity validation and repair include the reserved primary-key
  owner, so missing primary-key owner rows can be rebuilt from committed base
  rows.

Public API shape:

- Relational clients address rows through structured primary-key selectors, for
  example `{ "primary": { "tenant_id": "t1", "order_id": "o9" } }`, or through
  declared unique-key selectors, for example
  `{ "unique": { "name": "customers_email_key", "values": { "tenant_id": "t1", "email": "a@example.test" } } }`.
  They do not address rows by the hidden physical row key.
- `POST /tables/{table}/rows/batch` accepts `insert`, `upsert`, `update`, and
  `delete` operations. `insert` compiles to the existing batch write path with a
  non-existence version predicate on the derived row identity; `upsert` uses the
  existing overwrite-or-create batch semantics; `update` compiles to a
  non-upsert transform after resolving a primary or unique selector and rejects
  primary-key component patches; `delete` compiles to a normal batch delete
  after resolving a primary or unique selector. Missing unique selectors fail
  write requests rather than becoming scans or silent no-ops.
- `POST /tables/{table}/rows/get` accepts primary-key and unique-key selectors
  and returns the structured identity, row JSON, version, and optional physical
  key for diagnostics. Missing unique selectors return `found: false`.
- The public physical key, when requested, is diagnostic only. It is derived
  from the canonical typed primary-key tuple and remains storage-owned for
  placement, WAL, row-version ownership, and hidden base-row addressing.
- Unique-key selectors resolve through durable unique-owner rows using the same
  tuple encoder and owner-routing contract as FK and uniqueness enforcement.
  They are not query scans.
- A future SQL DSL should compile `PRIMARY KEY (tenant_id, order_id)`,
  `REFERENCES customers(tenant_id, customer_id)`, `INSERT`, `ON CONFLICT`,
  point `UPDATE`, and point `DELETE` into this structured API rather than making
  SQL text the primitive storage API.

Remaining follow-up work:

- More chaos coverage should combine row writes, owner-range movement, action
  jobs, and repair in one modeled workload. The current VOPR coverage includes
  metadata owner-topology convergence for composite primary-key parent/child
  schemas under deterministic transport faults.
- SQL-facing adapters should map `PRIMARY KEY (a, b)` and references to that
  key into the catalog form above.

## Public Contract

The supported shape is `on_delete: "restrict"`, bounded nullable-column
`on_delete: "set_null"`, or bounded `on_delete: "cascade"` foreign keys to a
parent table's `_id`, declared `primary_key.columns`, or declared
`unique_constraints.columns` tuple with routed owner topology.

```json
{
  "version": 1,
  "storage_mode": "relational",
  "default_type": "order",
  "document_schemas": {
    "order": {
      "schema": {
        "type": "object",
        "properties": {
          "id": { "type": "keyword" },
          "customer_id": { "type": "keyword" },
          "amount": { "type": "numeric" }
        },
        "required": ["id", "customer_id"],
        "additionalProperties": false
      }
    }
  },
  "foreign_keys": [
    {
      "name": "orders_customer_id_fkey",
      "columns": ["customer_id"],
      "references": {
        "table": "customers",
        "columns": ["_id"]
      },
      "on_delete": "restrict"
    }
  ]
}
```

For v1:

- `references.columns` may be exactly `["_id"]`, the existing Antfly document
  key / relational primary key. `_id` is a primary-key pseudo-column, not a
  relational tuple component, so it cannot be mixed into composite unique-parent
  references such as `["_id", "email"]`.
- otherwise, `references.columns` must exactly match a declared
  `unique_constraints.columns` tuple on the same local table, or be resolved by
  the hosted catalog against the referenced parent table's declared unique
  constraint before prepare. Local single-table execution only enforces
  same-table unique targets; hosted cross-table unique targets require parent
  unique-owner topology and fail closed when the parent constraint cannot be
  proven from catalog metadata.
- child columns must be declared relational columns with typed scalar encodings
  compatible with the parent target.
- nullable child columns are allowed; `null` means no reference. For unique
  tuple targets, any absent nullable component means no reference.
- non-null child columns follow the existing `required_fields` / `NOT NULL`
  behavior.
- `match` defaults to `simple`. Explicit `match: "simple"` is accepted and
  preserved through the schema/runtime catalog; for composite nullable FKs, any
  absent child component means no reference. `match: "full"` is also accepted:
  either every child component must be absent, producing no reference, or every
  child component must be present and validated as one composite reference.
  A partially-null `MATCH FULL` reference is a FK violation. DB transaction
  prepare and distributed commit planning both validate this reference shape
  before durable intents or participant prepare calls are emitted, so
  partially-null composite references fail consistently in local and routed
  writes. `match: "partial"` remains rejected until its row-subset parent
  matching semantics are implemented.
- schema input accepts canonical JSON enum tokens and SQL-style spellings for
  supported values: case is ignored, spaces/hyphens/underscores are equivalent,
  `NO ACTION` maps to `no_action`, `SET NULL` maps to `set_null`,
  full clause-shaped action tokens such as `ON DELETE CASCADE` and
  `ON UPDATE SET NULL` map to their canonical action values,
  `INITIALLY DEFERRED` maps to `deferred`, compound deferrability clauses such
  as `DEFERRABLE INITIALLY DEFERRED` and
  `NOT DEFERRABLE INITIALLY IMMEDIATE` are normalized into canonical
  `deferrable`/`timing` catalog fields, `MATCH FULL` maps to `full`, and
  `NOT VALID` maps to the canonical `unvalidated` validation state.
  `deferrable` accepts either JSON booleans or SQL-style string tokens
  `DEFERRABLE` / `NOT DEFERRABLE`; compound string clauses may also supply the
  initial timing when the separate `timing` field is omitted. Compound
  deferrability clauses are accepted in either `timing` or `deferrable`; if both
  fields provide timing or deferrability, the values must agree.
  The compiled schema remains canonical. Unsupported SQL actions such as
  `SET DEFAULT` are rejected instead of being stored as inert catalog values.
- referenced parent tables must be relational tables.
- `on_update` defaults to `restrict`. `restrict` and `no_action` are enforced
  as parent-key update checks for supported primary-key and unique parent
  targets: changing a referenced parent tuple is rejected while live child
  references exist.
  `restrict` is never relaxed by `timing: "deferred"`; deferred `no_action` is
  checked against the transaction's final staged reverse-reference state, so the
  parent tuple and the child references can be rewritten in either order inside
  one local relational transaction. Local same-table unique-parent
  `on_update: "set_null"` is implemented for nullable child FK columns: changing
  the referenced unique tuple rewrites affected child rows through the normal
  relational participant and deletes the old reverse-reference rows. Local
  same-table unique-parent `on_update: "cascade"` is also implemented: changing
  the referenced unique tuple rewrites affected child FK columns to the new
  parent tuple and moves reverse-reference rows from the old tuple to the new
  tuple. Distributed mutating update actions are represented by the same durable
  action schedule/job/page executor used for high-fanout parent deletes:
  `update_set_null` and `update_cascade` pages route child-row rewrites through
  source-layer 2PC, pin the affected child rows, and register downstream unique
  and FK participants before prepare. SQL-facing adapters that expose statement
  boundaries must map
  `restrict` to a statement-time check, while the raw DB transaction API
  prepares row intents at commit.
- Primary-key identity rewrites are intentionally separate from referenced
  unique-tuple updates. Point row-batch rewrites, claimed mutation-source
  rewrites, and joined mutation-source rewrites use a native typed
  `rewrite_identity` operation that claims the old row with OCC predicates,
  materializes the new row from typed assignments or source-derived joined
  assignments, emits an internal `relational_identity_rewrites`
  request, moves primary/unique owner rows to the new physical owner, moves or
  rewrites FK reverse-reference rows, runs parent `on_update`
  restrict/no-action/set-null/cascade semantics, and replays secondary plus
  embedded-JSON index maintenance from the new committed row image. Bound tables
  execute the operation locally; provisioned and hosted table-write sources route
  same-owner rewrites to the owner group and fail closed when old and new keys
  would cross owner ranges. Claimed and joined mutation-source rewrites persist
  durable transaction markers so commit cannot confuse a logical identity update
  with an ordinary delete plus insert. Cross-owner point, claimed-source, and
  joined-source primary-key rewrites remain fail-closed until routing can carry
  one durable staged rewrite per affected target/source row pair.
- `on_delete` supports `restrict`, `no_action`, `set_null`, and `cascade`;
  `no_action` is preserved as a first-class catalog value. `restrict` is never
  relaxed by `timing: "deferred"`; deferred `no_action` validates the final
  reverse-reference state at commit, so a transaction can delete the parent
  first and rewrite or delete the child reference later in the same local
  transaction. SQL-facing adapters that expose statement boundaries must map
  `restrict` to a statement-time check, while the raw DB transaction API
  prepares row intents at commit. `set_null` requires every child FK column to be
  nullable and rewrites affected child rows through the relational participant
  during parent delete with bounded local fanout. Local `cascade` deletes
  affected child rows through the same participant with bounded depth and
  fanout. The local store can also explain a parent delete through the same
  participant path without mutating rows, so operators can see whether a delete
  would be allowed and how many set-null or cascade child rows it would plan.
- `timing` defaults to `immediate`. `deferred` is accepted for local relational
  transactions and validates parent existence against the participant's final
  staged state at commit. Omitted `deferrable` defaults to false unless
  `timing: "deferred"` is present, preserving existing schema JSON as
  deferrable initially deferred. `deferrable: true` with
  `timing: "immediate"` represents SQL `DEFERRABLE INITIALLY IMMEDIATE`:
  transaction-level timing overrides may defer the constraint for that
  transaction while the catalog default remains immediate. `deferrable: false`
  with `timing: "deferred"` is rejected. Deferred `on_delete: "no_action"` and
  `on_update: "no_action"` validate the final reverse-reference state for parent
  deletes and referenced unique tuple changes; `restrict` remains restrictive
  and is never relaxed into deferred `no_action` semantics.
  Transaction participants can carry a durable
  `foreign_key_constraint_timing_overrides` prepare field for named constraints.
  Overrides are accepted only for enforced deferrable FKs and may set effective
  timing to either `immediate` or `deferred` for that transaction. The override
  is persisted as a transaction metadata intent, collected during intent
  resolution/recovery, and applied to the relational participant before row
  intents are replayed, so recovery sees the same effective timing as the
  original prepare. Distributed commit requests expose the same capability as a
  table-scoped option on the child table that owns the FK constraint. The
  structured request keeps `constraint_name` separate from `timing`, but timing
  accepts SQL-shaped aliases such as `SET CONSTRAINTS DEFERRED`,
  `SET CONSTRAINTS ALL IMMEDIATE`, and
  `SET CONSTRAINTS orders_customer_id_fkey DEFERRED` in addition to
  canonical `deferred`/`immediate`. If a SQL-shaped timing value names a
  specific constraint, that name must match the structured `constraint_name`
  field or appear in its comma-separated constraint target list. Unquoted
  targets use the same case-insensitive space/hyphen/underscore normalization as
  other enum-like schema tokens; double-quoted SQL identifiers match exactly and
  may escape a literal quote by doubling it. Unquoted `ALL` is accepted as an
  explicit broadcast target.
  Internal prepare and action-page operation fields likewise accept canonical
  `delete` / `update` plus SQL-shaped variants such as `ON DELETE`,
  `ON UPDATE`, `parent-delete`, and `parent_update`; durable records and
  responses still use canonical operation names.
  coordinator uses the effective timing when building parent-existence,
  externalized-proof, and parent-delete/update absence checks, and it sends the
  durable override only to participants for that child table; parent-table
  participants validate proof records without being asked to accept a constraint
  they do not own.
  Distributed/routed child writes and
  reference-changing transforms externalize deferred parent-existence checks
  through exact prepare-time proof records: the coordinator registers the
  parent/unique-owner participant and writes a matching
  `(constraint, child table, child key, parent table, parent key, optional
  parent unique constraint, timing)` proof to the child participant. The child
  participant only skips local deferred validation for those exact references:
  primary-key proofs must not name a parent unique constraint, same-table
  unique-parent proofs must name the matching local unique constraint, and
  cross-table unique-parent proofs must carry a non-empty parent constraint name
  proved by the coordinator. Parent/unique-owner participants validate that the
  proof names the receiving parent table before accepting either a primary-key
  row check or a unique-constraint tuple check, so a constraint-name match alone
  cannot make a proof valid on the wrong table. Explicit proof records are
  rejected unless the receiving participant has a relational FK schema that can
  resolve the named constraint. Unrelated deferred FKs still validate locally and
  fail closed if no exact proof is present. Restrictive distributed parent deletes
  over deferred FKs use the same exactness rule: the coordinator routes a timed
  parent-delete proof to the FK-ref owner range, and the child participant
  accepts it only when the proof timing matches the enforced runtime FK. Mutating
  deferred action sets still require the large-operation action executor
  described below.
- `validation_state` defaults to `enforced`. `unvalidated` is accepted for
  local online adoption: it records the catalog entry but does not enforce
  writes or maintain reverse-reference rows until the same constraint is applied
  as `enforced`. Job-owned states such as `validating` and `invalid` remain
  reserved and are rejected in public schema creation/update today; terminal
  invalid adoption state is represented by durable FK integrity job records,
  metadata scheduler diagnostics, and an internal table-metadata validation
  record keyed by constraint name. The internal record is separate from
  runtime-applied schema JSON so the data plane never has to accept reserved
  public states. Default admin integrity validation, repair, list, and
  parent-delete explain operate on active `enforced` / `immediate` constraints.
  Explicitly naming an immediate `unvalidated` constraint is allowed for online
  adoption validation/repair, and the schema controller described below uses
  that path to advance non-enforced catalog entries. Tables with no active
  constraints are no-ops unless a specific adoption constraint is named.

The API rejects unsupported shapes during schema validation rather than
accepting constraints that silently degrade. Unknown fields on
`foreign_keys`, `foreign_keys[].references`, and `unique_constraints` are
rejected; unsupported options must be added deliberately with storage and
transaction semantics.

## Runtime Schema

The compiled runtime schema carries a normalized foreign-key catalog:

```zig
pub const ForeignKey = struct {
    name: []const u8,
    child_columns: []const []const u8,
    parent_table: []const u8,
    parent_columns: []const []const u8,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
    timing: ForeignKeyTiming,
    deferrable: bool,
    match: ForeignKeyMatch,
    validation_state: ForeignKeyValidationState,
};

pub const ForeignKeyAction = enum {
    restrict,
    no_action,
    set_null,
    cascade,
};

pub const ForeignKeyTiming = enum {
    immediate,
    deferred,
};

pub const ForeignKeyMatch = enum {
    simple,
    full,
    partial,
};

pub const ForeignKeyValidationState = enum {
    enforced,
    unvalidated,
    validating,
    invalid,
};
```

The runtime representation uses column paths resolved against
`relational_columns`, not raw public JSON pointers. Schema compilation must
validate that:

- every child column exists and has a scalar, comparable representation;
- the nullability of the child column is known;
- the parent target is supported by the current feature level;
- the constraint name is unique within the table;
- multiple FK definitions may use the same child-column set when they have
  distinct constraint names, because reverse-reference and conflict rows are
  keyed by constraint name plus parent identity.

Changing the FK catalog is a schema/storage change, not a derived-index-only
change. Local same-table schema updates can add or drop FK catalog entries by
constraint name through the synchronous validation path: additions validate
existing child rows and install reverse-reference rows before the catalog flip;
drops remove the old reverse-reference rows. Reusing a constraint name with
different semantics is rejected; use an explicit drop plus a new name. Hosted
adoption progress is tracked through durable FK job/progress records; terminal
valid results are promoted through the metadata table-update path, and terminal
invalid results are stored in internal table metadata without changing the
runtime FK schema.

## Physical State

Parent existence can be checked by the parent table's relational row key. Parent
delete protection needs a reverse reference index; otherwise a delete would
require scanning the child table.

The relational write participant maintains reverse FK rows transactionally with
child rows:

```text
fkref:<constraint_id>:<parent_table>:<parent_key>:<child_table>:<child_key> -> empty
```

The exact encoding should use internal key namespaces, encoded components, and
range-friendly ordering. The important ordering is:

```text
constraint_id, parent_table, parent_key, child_table, child_key
```

That makes parent delete validation a bounded prefix scan over one parent key.

Reverse rows are secondary integrity metadata. They are not a relationship query
index and should not be exposed as the graph model. If they are lost or found
corrupt, they must be rebuildable from the committed relational child rows.

### Routed reverse-reference ownership

Hosted parent-delete planning uses a parent-key-routed FK reverse-reference
index with catalog-visible ownership, similar in spirit to distributed SQL
backing indexes for constraints: the system pays synchronous index write
amplification so FK checks become targeted key/range lookups instead of broad
child-table fanout.

The routed keyspace should be owned by the referenced parent key, not by the
child row key:

```text
fkref:<child_table_id>:<constraint_id>:<parent_table_id>:<encoded_parent_key>:<child_key> -> empty
```

The routing catalog should treat this as an internal constraint index, not as a
user table:

```zig
pub const ForeignKeyReferenceRange = struct {
    child_table_id: u64,
    constraint_id: u64,
    parent_table_id: u64,
    start_parent_key: []const u8,
    end_parent_key: ?[]const u8,
    group_id: u64,
    topology_epoch: u64,
    state: RangeState,
};
```

Given `(child_table, constraint, parent_table, parent_key)`, the coordinator can
resolve the exact FK-ref owner range and register only that group as a 2PC
participant. Parent delete planning then routes to the reverse-reference range
for each referencing constraint and deleted parent key instead of registering
every child table range.

This ownership model changes distributed write planning:

1. A child insert/update/delete still registers the child row participant.
2. For each changed non-null FK value, the coordinator registers the parent row
   participant for existence validation.
3. The coordinator also registers the FK-ref owner participant for the old and
   new parent keys and sends explicit reverse-reference write/delete intents.
4. Parent deletes register the parent row participant plus the exact FK-ref
   owner participant for every referencing constraint and deleted key.
5. `restrict` checks scan only the exact FK-ref prefix. `set_null` and
   `cascade` write a deterministic action schedule to one FK-ref owner
   participant and rely on controller-owned durable action jobs to drain child
   rewrites/deletes in bounded pages after the parent-delete transaction
   commits.

The FK-ref owner participant is the distributed conflict point. Child reference
creation and parent delete for the same `(constraint, parent_key)` must conflict
through that participant even when the child row, parent row, and reverse-ref
index live on different groups.

Operationally, FK-ref ranges need the same production lifecycle as other
internal range-owned state:

- split/merge metadata with topology epochs;
- rebuild from committed child rows;
- validation that every committed child reference has exactly one routed FK-ref
  row;
- repair that can recreate missing FK-ref rows and prune stale rows without
  mutating user rows;
- large-operation planning for hot parent keys with many child refs;
- one production FK-ref layout for this new feature set: routed ownership is the
  durable encoding, and future format changes must be explicit catalog/schema
  migrations.

Parent-key ownership can create hot spots for high-fanout parents. That is an
expected property of exact FK enforcement, not a reason to fall back to table
scans. The mitigation is split support, bounded interactive limits, explicit
large-operation execution for `set_null` / `cascade`, and operational visibility
for high-cardinality parent keys.

## Write Path

Foreign-key enforcement belongs in the relational write participant, not only in
the HTTP/API layer. Batch writes and committed transaction intents both enter
the same participant semantics.

On child insert/update:

1. Project and validate the relational row as today.
2. Extract the old FK values, if the row already exists.
3. Extract the new FK values from the projected row.
4. For each non-null new FK value, stage a parent-existence check in the
   relational write participant.
5. Update reverse FK rows for changed references in the same commit batch.
6. Before any durable store batch is applied, validate all staged parent checks
   against the participant's final planned row, unique, and delete state plus
   the committed store.
7. Reject the write with `ForeignKeyViolation` if the parent is absent in that
   final state.

This makes same-batch and same-transaction parent/child creation independent of
input ordering: a child write may appear before its parent write as long as the
participant's final committed state contains the parent and does not delete it.

On child delete:

1. Load the old FK values from the committed child row.
2. Delete the corresponding reverse FK rows with the child row.

On parent delete:

1. Scan reverse FK rows for the parent key.
2. If any live child reference exists, reject with `ForeignKeyViolation`.
3. Allow the delete only when no references exist or when the same transaction
   also deletes/updates those child references away.

This makes FK state part of the committed relational base-store mutation, not an
eventually consistent derived artifact.

For hosted distributed execution, the desired write path is the same invariant
with explicit constraint-index participants:

- child row writes register the owning child row range, every required parent
  row range, and every routed FK-ref owner range for old/new references;
- parent deletes register the owning parent row range plus every routed FK-ref
  owner range for the deleted parent key and matching referencing constraint;
- FK-ref owner participants own reverse-reference writes/deletes and
  parent-delete prefix scans;
- the coordinator rejects before prepare when it cannot derive the full
  participant set from public writes, resolved old rows, and catalog metadata.

The current hosted implementation has the first routed production subset. It
registers parent-existence participants for primary-key child writes and, for
same-table declared unique parent tuples, unique-owner participants when the
referenced tuple's owner range is configured. If FK-ref owner ranges are
configured and the child write has an `expected_version: 0` predicate, it also
registers the exact owner range for the new parent key and sends an explicit
FK-ref write mutation to that participant. If the child write or delete has a
positive version predicate, the coordinator reads the old child row from the
owning group, verifies the read version matches the predicate, and routes FK-ref
deletes for old parent keys before prepare. Without either insert-only proof or
a version-bound old row, owner-ranged child writes/deletes fail before prepare
because they could otherwise leave stale reverse-reference rows behind.
Primary-key `restrict`, `set_null`, and bounded `cascade` parent deletes require
exact FK-ref owner ranges. If owner topology is missing, transitional, or cannot
cover the parent key, the coordinator fails before prepare instead of widening to
child-table fanout.

The catalog-facing owner range record, resolver, first-class raft transitions,
durable projection, metadata status count, reconciliation-plan convergence, and
placement planning for FK-ref owner groups exist, so a coordinator can resolve
configured owner ranges for a parent key and the metadata leader can repair drift
between desired and committed FK-ref owner topology. The storage transaction
layer accepts explicit FK-ref write/delete mutations in the prepare request.
Those mutations are validated against the runtime FK catalog, materialize as
exact internal reverse-reference keys on commit, delete those keys on commit
when requested, and are considered by parent-delete validation. Hosted
coordinator work now uses exact FK-ref owner routing for wide
`set_null`/`cascade`, durable validation/repair jobs for online adoption and
repair, and explicit deferrable timing/proof records in distributed prepare.

## Transaction And 2PC Semantics

Existing 2PC gives the atomic commit substrate, but it does not by itself define
foreign-key semantics. FK support layers constraint-aware participants and exact
FK-ref/unique-owner conflict protection onto that substrate.

For single-shard parent/child writes, the relational participant writes reverse
rows in the local prepare path and validates parent existence against the final
planned participant state before the durable commit batch is applied.

For cross-shard or cross-table writes:

- the hosted coordinator registers the referenced parent table/range participant
  for enforced immediate single-column `_id` child writes before prepare;
- the parent participant receives a parent-existence validation instruction and
  rejects prepare when the referenced parent key is absent from its final staged
  state plus committed store;
- the child row participant is marked as having parent checks externalized when
  the coordinator registers a parent or unique-owner validation participant. The
  storage participant still maintains reverse-reference rows for the child row,
  but it skips its local parent-existence probe during transaction-intent
  resolution because that validation is owned by the registered parent-side
  participant. The flag is persisted as a per-transaction internal intent marker
  so participant recovery and explicit resolve paths preserve the same
  semantics, and the marker is skipped during final intent materialization;
- when FK-ref owner ranges are configured, insert-only child writes with a
  matching `expected_version: 0` predicate also register the exact owner range
  for the new parent key and send a FK-ref write mutation to that participant;
- unversioned full-row child writes read the current row before prepare, inject
  either the observed row version or `expected_version: 0` for a missing row as
  a row-participant predicate, and use that durable proof to route old/new
  FK-ref owner mutations;
- owner-ranged child writes/deletes with positive version predicates read the
  old child row, verify the read version against the predicate, and route
  FK-ref deletes for old parent keys plus FK-ref writes for new parent keys;
- owner-ranged deletes and transforms that are neither provable inserts nor
  bound to a verified old row fail before prepare;
- parent-existence validation is fail-closed at the participant boundary: the
  receiving participant must be a relational table whose runtime
  `default_type` matches the requested `parent_table`, and malformed or
  cross-table-mismatched prepare payloads are rejected before ordinary key
  existence is considered;
- hosted restrict parent deletes require the exact FK-ref owner range for the
  deleted parent key. Missing, transitional, or incomplete owner topology fails
  before prepare; broad child-table fanout is not used for this new feature
  set;
- FK-ref owner or child participants receive parent-delete validation
  instructions and reject prepare when committed reverse-reference rows or
  staged child writes would leave a surviving child reference. A participant
  only evaluates reverse-reference rows whose encoded child table matches that
  participant's runtime table, so misplaced rows for another child table cannot
  block the wrong participant;
- same-table enforced FK child writes to declared unique parent tuples route
  parent-existence checks through configured unique-owner ranges. The check
  carries the parent unique constraint name and encoded tuple identity, and the
  unique-owner participant validates that the tuple exists in its final staged
  unique-index state plus committed store. Same-table or cross-table
  `restrict`, `set_null`, and bounded `cascade` parent deletes over a declared
  unique parent tuple read the deleted parent row before prepare, inject the
  observed row version when the caller did not supply one, encode the old unique
  tuple with the same unique-index encoder, and route enforcement through the
  FK-ref owner range for that encoded tuple. `restrict` sends an
  encoded-tuple parent-delete check to the owner. `set_null` and `cascade`
  register the FK-ref owner as a conflict participant and write a deterministic
  action schedule record for the encoded tuple. Missing unique-owner or FK-ref
  owner topology still fails before prepare.
  Primary-key `set_null` and bounded primary-key `cascade` use exact routed
  owner discovery: the coordinator registers the owner participants as conflict
  points and writes one durable action schedule record to a resolved FK-ref owner
  in the same 2PC as the parent delete. Controller maintenance then seeds
  idempotent action jobs across the current owner groups, and those jobs page
  through child rows to apply exact set-null or cascade actions. Missing,
  transitional, or incomplete owner topology fails before prepare because broad
  child-table fanout is not a deterministic dependency graph;
- hosted transforms on tables with active foreign keys are planned from the same
  final-row semantics as storage transforms when the coordinator can prove that
  final value before prepare. Non-reference transforms are routed normally.
  Primary-key FK reference transforms are supported when they are bound by a
  positive version predicate, or by an insert-only predicate for deterministic
  upsert creation; the coordinator reads and verifies the old row when needed,
  coalesces same-key writes, deletes, and transforms using the same ordering as
  the storage participant, resolves the final row value, registers the
  parent-existence participant for the new FK value, and registers routed FK-ref
  owner write/delete mutations for old/new parent keys. Unversioned
  FK-reference transforms read the current row before prepare and inject the
  observed row version or `expected_version: 0` for a missing row before
  routing owner mutations. FK-reference transforms still fail before prepare
  when they target unsupported non-primary FK shapes;
- transaction prepare requests can carry explicit FK-ref writes/deletes for the
  FK-ref owner participant. The storage participant applies those internal rows
  exactly and parent-delete validation treats staged FK-ref writes as blockers
  and staged FK-ref deletes as removing that child reference;
- the routed form replaces child-table fanout with FK-ref owner participants
  resolved by `(child_table, constraint, parent_table, parent_key)` whenever the
  owner topology exists;
- routed child reference creation and parent-delete validation prepare through
  the same FK-ref owner range for the relevant parent key. Reference removal,
  moved-reference updates, and FK-reference transforms use read-pinned old/new
  FK planning before registering owner mutations;
- prepare must fail if the parent is absent or if a parent delete conflicts with
  a child reference that would survive the transaction;
- recovery must either commit both the relational row and reverse FK rows, or
  abort both.

The correctness invariant is:

```text
At every committed read generation, every non-null child FK has exactly one
matching committed parent row, and every committed child FK is represented by a
matching reverse-reference row.
```

## Concurrency

The race to protect is:

1. transaction A validates that parent `p1` exists for child `c1`;
2. transaction B deletes parent `p1`;
3. both commit, leaving `c1 -> p1` dangling.

The reverse-reference key range is the natural conflict point. Parent delete
must conflict with concurrent child reference creation for the same parent key.
The current implementation uses internal FK conflict intents keyed by
`(constraint, parent_table, parent_key)`. Child reference creation and
parent-delete validation both stage a delete intent on that key. The intent is a
transactional conflict marker only: commit deletes the internal marker, leaving
no durable lock row behind.

The long-term shape should be explicit reverse-reference intent/range conflict
handling, because it also supports future cascade planning and repair tooling.
With routed ownership, the FK-ref owner range provides that conflict point
directly: prepare for a child reference write and prepare for a parent delete of
the same key must both acquire or validate the same owner-range conflict state.

## Rebuild And Repair

Reverse FK rows are deterministic from child rows, so the system should include
a rebuild path:

1. Scan child relational rows for one constraint.
2. Extract each non-null FK value.
3. Verify the parent exists.
4. Rewrite the reverse FK row.
5. Report orphaned child rows instead of silently dropping them.

The storage-level validation/repair primitive exists for the current local
relational store shape: it scans committed child rows, reports missing parents
and missing/stale reverse-reference rows, returns owned violation rows for
operator diagnostics, recreates missing reverse-reference rows, and prunes stale
reverse-reference rows whose child row is missing or now points elsewhere. The
DB layer exposes the same local primitive for validation, repair, and violation
listing with runtime schema metadata. Temporal foreign keys use that metadata to
validate parent coverage through temporal unique-owner ranges, so repair rebuilds
period-aware reverse-reference rows only when the parent coverage that accepted
the write still exists. The public admin endpoint
`POST /tables/{table}/foreign-key-integrity` exposes the same operation for
local/bound, provisioned, and hosted tables with `validate`, `dry_run`,
`repair`, `list`, `plan`, and `progress` actions plus optional `constraint_name`,
document-key range bounds, and a violation-detail limit. When `constraint_name`
is present, every local, provisioned, hosted, and internal group execution path
validates the name against the runtime FK catalog and scans only that
constraint's reverse-reference metadata; unknown names fail instead of returning
a misleading empty report.
Public and internal integrity action names accept case-insensitive
space/hyphen/underscore variants such as `DRY RUN` or `explain-delete`, while
responses and durable progress rows keep canonical snake_case mode names.
The same public/admin endpoint accepts `{"action":"requeue_action_job"}` for
operator retry of failed high-fanout `set_null`/`cascade` action jobs. Requeue
requires the durable action-job identity (`job_id`, `action_job_action`,
`constraint_name`, `parent_table`, `parent_key`, `worker_id`, and optional
`updated_parent_key`/`page_limit`), resolves the appropriate owner groups
through the table-write source, and returns the owner action-job statuses after
clearing failed claim/error state while preserving the stored child cursor and
applied-child count. Healthy pending or claimed jobs are retried by normal
controller rounds; the DB rejects explicit requeue unless the durable job is
dead-lettered with `status: "invalid"` or a retained `last_error`.
It also accepts `{"action":"requeue_action_schedule"}` for failed action
schedules that could not seed owner jobs, usually because FK-ref ownership was
missing or stale when the parent delete committed. Schedule requeue requires
the durable schedule identity (`schedule_id`, `action_schedule_action_job_id`,
`action_schedule_action`, `constraint_name`, `parent_table`, `parent_key`,
`worker_id`, and optional `updated_parent_key`/`page_limit`), resolves the
schedule owner through the table-write source, and returns the requeued
schedule status after clearing the terminal error and moving it back to
`pending`. Requeue increments a durable schedule-level `requeue_count` and
records `last_requeued_at_ns`, so repeated topology-recovery attempts remain
visible after DB reopen and through public/admin status responses. Healthy
pending schedules are retried by normal controller rounds and do not consume
explicit requeue counters.
`dry_run` runs the repair traversal and reports the reverse-reference rows that
would be created or deleted without mutating the store. Hosted execution fans out
through the same internal group RPC family used by write routing, forwards the
same constraint scope to remote groups, aggregates every resolved table range,
and fails the request rather than returning a partial result when a group cannot
be routed. Online schema adoption can later use the same scanner to validate a
newly added FK before making it enforced.
`{"action":"plan"}` is catalog-only for provisioned and hosted sources: it
resolves the requested table/span into deterministic per-group child-range work
units without opening shard DBs. Each work unit records the target group, phase,
planned action, optional constraint scope, and clipped document-key lower/upper
bounds. The planned action matches the operation that will run (`validate`,
`dry_run`, `repair`, or `list`) so a background worker can execute a returned
unit directly without translating it through request context. Provisioned and
hosted `validate`, `dry_run`, `repair`, and `list` execute those same clipped
work units instead of one broad per-group scan, so the stored progress rows are
real range checkpoints. Bound/local sources expose the same shape as a single
child-range work unit for validation-style actions. `progress` is a status
listing action and still queries each resolved group once, returning all stored
FK integrity progress rows for that group. `validate`, `dry_run`, `repair`,
`list`, and `progress` responses also include the same `work_units` array so
operators and future background workers can correlate execution/progress with
the exact resumable range boundaries.
The response also includes `work_statuses`, a worker-facing manifest keyed by
the same unit boundary. Each status carries a deterministic `claim_key` plus
`planned`, `pending`, `claimed`, `incomplete`, `complete`, or `invalid` state.
The state is derived by joining the planned unit to durable per-group progress
rows and durable per-group claim rows by group, mode, constraint name, clipped
document-key range, and claim key. `plan` reports `planned` without opening
shard DBs unless a matching claim row already exists; execution responses report
`complete` or `invalid` after the unit records progress, and `progress` reports
`pending` when a planned unit has no matching progress or claim row yet.
`work_claims` exposes the persisted lease owner, lease deadline, attempt count,
and unit boundary for claimed units. Claim rows live in the target group DB
under internal metadata and support same-worker renewal plus expired-lease
takeover while rejecting active leases held by another worker. The group DB also
exposes a claim-and-run primitive that acquires the lease, executes the selected
`validate`, `dry_run`, or `repair` mode for that exact unit range, and records
the normal durable progress row before returning the report. Bound, provisioned,
hosted, and internal group write sources expose the same claimed work-unit
operation, so a coordinator can dispatch a planned unit to the owning group
through the normal internal group write route instead of opening group storage
directly.
The public `foreign-key-integrity` operation also accepts `worker_id`,
`lease_ms`, `max_work_units`, and optional `job_id` for `validate`, `dry_run`,
and `repair`. When `worker_id` is present, the public/admin handler runs a
bounded scheduler pass: it plans child-range units for committed child rows and,
for provisioned/hosted tables with active FK-ref owner ranges, owner-range units
for the routed parent-key spans that store reverse-reference rows. Each unit's
`phase` is part of its claim/progress identity. `child_range` units recreate
missing owner rows from child rows; `owner_range` units scan the routed owner
span and prune stale rows whose child row is missing or now references a
different parent. The pass reads durable progress and claim state without
mutating every unit, selects pending/incomplete or expired-lease units, claims
and runs up to `max_work_units`, then returns refreshed progress, claims,
statuses, and the effective `job_id`. If the caller does not supply `job_id`,
the handler derives a stable one from table name, action, constraint scope, and
document-key range, so repeated admin/controller passes resume the same durable
job without external ID allocation. The pass records durable job intent before
claiming work and marks the job `complete` or `invalid` when the refreshed work
statuses are terminal. Local/bound execution stores that record in the table DB.
Provisioned execution stores the same record in every group DB that owns
planned work for the pass, alongside the group-local claim and progress rows.
Hosted execution routes selected units to the owning group leader through the
same internal group claimed work-unit route. That routed request carries the
unit phase, job identity, and per-pass work-unit limit to the owning leader,
which records the group-local job shard next to the claim/progress rows before
and after the claimed unit runs. The bounded pass is therefore safe to call
repeatedly by an external coordinator or future in-process background job.

The same public/admin worker endpoint also accepts `controller: "schema"` with
`worker_id`. In that mode the table-write source reads the authoritative table
schema, selects the named FK when `constraint_name` is provided, or otherwise
selects the first FK whose `validation_state` is not `enforced`, derives the
stable `job_id` from that selected constraint and range, and then runs the same
bounded worker pass. If no schema-declared FK needs adoption work, the
controller returns a complete, valid empty result without creating a job record.
This is the in-process controller primitive: hosted/local/provisioned sources
all use the same durable claim, progress, routing, and job-record machinery as
ordinary worker passes instead of maintaining a separate migration scanner.

The write-source layer also exposes a bounded schema-controller maintenance
pass. Bound/local sources apply it to their single table. Provisioned and hosted
sources scan the authoritative catalog snapshot, find tables with non-enforced
FK declarations, and run at most `max_tables` schema-controller worker passes
per invocation. This gives hosted runtimes an in-process primitive that can be
scheduled from an existing background loop without external HTTP polling while
preserving the same stable job ids and durable per-group progress rows.
The same maintenance pass also scans durable group-local FK integrity job
records exposed by `.progress`, finds incomplete validate/dry-run/repair jobs,
deduplicates them by `job_id`, and advances up to `max_jobs` bounded worker
passes using the controller worker identity. These job-controller results are
kept distinct from schema-adoption results, so a partial or user-created job can
finish without mutating catalog validation state; only schema-adoption results
are eligible for the `validation_state: "enforced"` promotion path.
The maintenance summary is conservative: if a pending schema adoption, durable
integrity job, FK action schedule, or FK action job is discovered but not
finished in this bounded round because of a controller budget, a skipped claim,
or an unseeded schedule, the round reports `complete: false`. A later round may
turn the same summary complete after the durable records have all reached a
terminal state; operators should not infer that no FK work remains from an empty
or budget-limited partial pass.
Hosted/provisioned progress scans are retry-tolerant: if a catalog range exists
but its local group DB is not materialized yet, the progress scan skips that
group and the next metadata round observes it again. Correctness-bearing
operations, including explain-delete, claimed validation/repair work, set-null
pages, and cascade pages, still fail closed on missing groups.

Group DB FK integrity job records are keyed by `job_id`. A record stores table,
action, worker identity, optional constraint scope, document-key range, lease
duration, per-pass work-unit limit, status, attempt count, created/updated
timestamps, completion/validity, the cumulative aggregate report, durable
diagnostic-pass counters, first/last violation timestamps, and a bounded JSON
sample of violation diagnostics with sample count and truncation metadata. Each
bounded pass merges its report and distinct new samples into the existing job
record up to the bounded limit; completion only sets terminal status and does
not erase useful failure context gathered by an earlier pass. The pass counters
continue to advance even when later passes observe the same violation and the
deduped sample set does not grow. These records are the durable intent/resume
substrate for hosted job controllers; they survive DB reopen independently from
the in-memory background job lane.

When hosted FK refs move to routed ownership, repair becomes a two-phase
constraint-index job:

1. Scan child row ranges and derive the expected routed FK-ref owner for every
   non-null reference.
2. Write missing FK-ref rows to the owner range through a repair transaction or
   resumable job participant.
3. Scan FK-ref owner ranges and prune rows whose child row is missing or now
   points to a different parent.
4. Validate parent existence through parent row participants.
5. Persist per-range progress and violation rows so a crash can resume without
   treating partial repair as success.

Repair may rebuild internal constraint metadata. It must not silently change
child rows or parent rows; orphaned children remain explicit violations for an
operator or application-level migration to fix.

The same endpoint also exposes parent-delete planning with
`{"action":"explain_delete","doc_key":"..."}`. The operation resolves the
target document key to its owning group, optionally scopes to `constraint_name`,
and returns `delete_plan` with existence, allow/block status, block reason,
planned set-null updates, planned cascade deletes, planned row deletes, planned
index deletes, and planned writes. `doc_key` is required for this action; range
bounds are ignored. Local storage runs the delete planner without committing its
prepared batch. Hosted/provisioned routing uses FK-ref owner ranges: the planner
reads the parent row, resolves each applicable owner range, drains the owner
prefix through child-table/child-key cursor pages, and fails closed rather than
returning a partial plan if the owner topology or cursor contract is invalid.

DB-local FK action jobs provide the first durable data-changing
large-operation unit. A job is keyed by `job_id`, action (`set_null` or
`cascade`), constraint, parent table, and parent key. The scheduler can create
the job as a pending durable record without claiming a lease or mutating
children. Each later worker claim leases the job, scans one
child-table/child-key cursor page from the committed FK-ref owner prefix,
applies those exact child-key actions in a local transaction when the child
table and FK-ref owner are the same single group, and persists the next cursor,
applied-child count, status, attempts, and lease metadata.
Action-job and action-schedule entry points accept SQL-style action spellings
with case-insensitive space/hyphen/underscore normalization, such as `SET NULL`
or `UPDATE CASCADE`, but persist canonical actions (`set_null`, `cascade`,
`update_set_null`, `update_cascade`) so resumed jobs and idempotent schedule
matches never depend on caller spelling.
Reopening the DB and claiming the same job resumes from the stored cursor; a
different worker must wait for the lease to expire unless the job is already
complete. Durable FK integrity progress, validation/repair claims, action
schedules, and action-job leases use realtime timestamps, not process-local
monotonic uptime, so hosted controllers can compare progress and lease expiry
across DB reopen, process restart, and remote owner groups. The table-write
source exposes both the schedule-only and page-run action-job primitives as
owned responses. Local tables run them against group `0`.
Finishing an action page is fenced by the durable claim record: the current
stored worker id, claim timestamp, lease deadline, and attempt number must still
match the worker's claimed record before the DB accepts a cursor/status update.
If a lease expires and another worker reclaims the job, the stale worker's
finish is rejected as a busy claim and cannot roll back the newer worker's
cursor, status, or applied-child count.
Finish also validates page outcome shape before touching the durable row:
successful incomplete pages must provide both resume-cursor fields, complete
pages must not provide a cursor or error, cursor fields must be both present or
both absent, and failed pages that report applied children must include the
cursor needed for explicit requeue.
If page execution fails after the lease is claimed, the DB records the failed
pass durably with `status: "invalid"` and `last_error` while preserving the
resume cursor and applied-child count. The job also persists
`failure_count`, `first_failed_at_ns`, and `last_failed_at_ns` so repeated
dead-letter loops survive DB reopen and can be diagnosed without controller
logs. Autonomous maintenance reports invalid jobs but does not retry them
automatically; an explicit action-job requeue is required after the cause is
fixed so pathological failures do not spin forever. Requeue clears the active
claim/error state but increments `requeue_count` and records
`last_requeued_at_ns` while preserving the durable cursor and failure history.
The DB rejects requeue for healthy pending or claimed jobs, so those recovery
counters only represent explicit dead-letter recovery attempts.
The DB claim path enforces the same dead-letter policy: an incomplete action
job with `status: "invalid"` or a retained `last_error` cannot be claimed again
by a direct worker call, even after its old lease expires, until the requeue API
clears the failure state.
Completed action jobs are also idempotent at the claim boundary: a later worker
claim with the same durable job identity returns the completed record unchanged
instead of renewing the lease or rewriting the worker id, page limit, claim time,
or attempt count. This keeps completion metadata stable when hosted controllers,
admin tools, or retried client requests rediscover an already-drained job.

DB-local FK action schedule records provide the next queue level above action
jobs. A schedule record is keyed by a stable `schedule_id` and stores the
action-job id plus action, constraint, parent table, parent key, and page
limit. Generated action-job ids and schedule ids include the mutating action
(`set_null` or `cascade`) as part of their versioned identity, so a later schema
revision that changes the action for the same constraint and parent key cannot
collide with an older durable action queue. Action schedules and jobs carry
`cascade_depth` and `cascade_max_depth` as explicit durable fields rather than
deriving policy from the id string. Root parent-delete schedules start at depth
`0`, recursive cascade pages seed downstream schedules at `depth + 1`, and the
depth/max fields are persisted on both schedule and job records so controller
retries, requeues, and DB reopen do not reset the policy. Hosted/internal
requests that create or seed action work must send both lineage fields; requeue
uses the stored record lineage and cannot replace policy from the request. The
DB rejects records whose max depth is zero or whose depth exceeds the max. The
metadata FK controller lists
pending schedule records, seeds the owner action jobs idempotently, marks the
schedule `seeded`, and then lets the existing action-job controller advance the
child-cursor pages. This survives DB reopen and avoids requiring the foreground
parent-delete request to keep polling until the operation finishes.
Routed parent-delete admission writes one durable schedule record into each
resolved FK-ref owner group that participates in the delete check. The
`schedule_id` includes the owner group, so a split/merge or transitional
topology does not depend on a single arbitrary owner to remember that a
large-operation action must be resumed. The seeded action-job id remains stable
for the `(action, child table, constraint, parent table, parent key)` action
unit, and each owner group schedules that idempotent job through the group-local
action-job API so the local cursor drains only the FK-ref prefix it owns.
Controller maintenance returns action-schedule scan/seed counters and the
seeded schedule statuses separately from action-job page statuses. Metadata
runtime status records the same counters, so operators can distinguish "the
large operation was discovered and seeded" from "all child pages have drained"
without manually correlating schedule records with owner action jobs. Terminal
invalid schedules are counted separately and are not repeatedly re-seeded by
autonomous controller rounds until their owner topology problem is corrected.
Metadata status keeps the latest failed schedule group, schedule id,
action-job id, action, status, error, constraint, parent identity, scheduled
group count, cascade depth, requeue count, and last requeued timestamp. It also
records last-round completed action-job counts, the last-round observed
`applied_children` sum from returned action-job statuses, invalid action-job
counts, failed action-job totals, and the latest failed action-job group, id,
action, status, error,
constraint, parent identity, resume child cursor, attempts, and applied-child
count, preserving the most recent controller-visible page failure after the
round result has been freed.
The applied-child counter is intentionally a last-round observation because
durable action-job records store cumulative per-job child counts; summing them
across metadata rounds would overcount retries and resumed jobs.
Terminal invalid validation results expose both sampled detail and cross-pass
aggregates. Metadata status keeps the latest invalid table, constraint, job id,
row counters, sampled missing-parent/missing-ref/stale-ref violation counts,
truncation flag, and first sampled violation for fast diagnosis, while
separately accumulating terminal-invalid missing-parent, missing-ref, and
stale-ref row totals, sampled violation totals, truncation-result totals, and
sampled violation-kind totals across controller passes. Those aggregates let
an operator distinguish a repeatedly failing adoption/repair pass from a single
stale status sample without scraping controller logs.
Action schedules fail closed if seeding resolves no owner action jobs: the
controller records the schedule as `invalid` with
`last_error: "NoForeignKeyActionOwnerGroups"`, reports the round incomplete,
and does not acknowledge the schedule as `seeded` until a retry can create at
least one durable owner job. Invalid schedules are terminal for autonomous
controller rounds, but the DB, table-write source, public/admin endpoint, and
hosted internal group route expose an explicit requeue primitive. Requeue
validates the durable schedule/action-job identity, rejects completed
schedules and healthy pending schedules, clears `last_error`, updates the
intended worker/page limit, and returns the schedule to `pending` after the
owner topology problem has been corrected. The DB seeded transition enforces
the same terminal policy as the controller: an invalid schedule or a schedule
with retained `last_error` cannot be marked seeded directly until the explicit
requeue path clears the failure state. Schedule requeue preserves the failure
history by incrementing `requeue_count` and recording `last_requeued_at_ns`;
later successful seeding keeps those fields on the durable schedule record.

Provisioned and hosted-local bounded page execution is deliberately split by
ownership. If the catalog proves the whole child table and, when configured,
the FK-ref owner prefix are the same single group, execution uses the DB-local
fast path to avoid recursive source checkout and unnecessary participant
traffic. Otherwise the table-write source first resolves FK-ref owner metadata
for `(child table, constraint, parent table, parent key)` and fails closed when
no routable owner range exists; each owner group can then seed a pending
durable job or drain one durable action page and report its group-local status.
In the routed path, the owner DB provides the durable lease, status, and FK-ref
cursor; source-layer 2PC applies cross-group effects, with an owner participant
for FK-ref deletes and child participants for `set_null` or `cascade` row
actions. A routed `cascade` page also treats each deleted child row as a parent
delete inside the same participant-planning pass: it scans the catalog for FKs
that reference the child table, fails closed if any required FK-ref owner
topology is missing, and writes downstream action schedules/conflict checks into
the same 2PC. Recursive/deep cascades therefore advance one durable action-page
boundary at a time instead of depending on an unbounded foreground transaction.
The 2PC transaction id for a routed action page is derived from the durable
action-job identity, resume cursor, page limit, cascade depth, and requeue
generation. Lease handoff after a process crash therefore retries the same
unrecorded page with the same participant transaction identity, while cursor
advancement or explicit operator requeue produces a distinct transaction id.
If a routed cascade page is already at `cascade_max_depth`, planning fails
closed before commit when the deleted child table has enforced downstream
`cascade` or `set_null` dependents. The owning action job is then recorded as
invalid/dead-letter with `last_error: "ForeignKeyCascadeDepthLimit"` by the
same page-failure path and must be explicitly requeued after schema or policy
correction.
Same-table relational FKs have two table identities in this flow. Ownership
metadata resolves by catalog table id/name, so a self-reference on table `docs`
uses `docs` as the parent owner table. The DB-local FK-ref prefix still uses the
runtime relational table name stored in the FK definition, for example `row`.
Durable action pages therefore route and validate owner topology with the
catalog parent identity, then scan and mutate committed FK-ref rows with the
runtime parent identity. This keeps same-table schedules compatible with local
storage keys without losing catalog-ranged ownership.
Hosted remote owner groups use the internal group action-job HTTP endpoint for
both schedule-only requests and bounded page execution on the owner node, plus a
separate internal progress endpoint to list that owner's durable action-job
records. The metadata FK controller can now discover and resume existing
durable schedule/action-job records from local, provisioned, hosted-local, and
hosted-remote owner DBs, respecting leases and reporting scanned/executed
action-job counts plus per-group action status. If an action page fails after a
durable claim, the owning DB marks the job `invalid` with `last_error`; the
schema controller refreshes progress, includes that failed job status in the
round result, counts the attempted page, and continues with other jobs instead
of aborting the whole maintenance round. Later maintenance rounds keep the
invalid job visible but do not claim it again until a public/internal requeue
operation clears the failed state. Metadata status distinguishes depth-limit
failures from generic action failures with cumulative and last-round
`action_jobs_depth_limit_failed` counters, and the compact last-failed action
job snapshot includes `cascade_depth`, `cascade_max_depth`, and whether the
depth limit was exhausted. The same snapshot includes the durable failure and
requeue counters/timestamps so operators can distinguish a first failed page
from a repeatedly requeued job that is still failing. Hosted autonomous
scheduling is owned by the metadata leader: each metadata lifecycle round runs
bounded controller passes when enabled, and incomplete rounds receive a
configurable follow-up budget before yielding. The production execution model is
request-independent controller ownership plus durable owner-routed action
schedules/jobs. Further hardening should concentrate on additional
failure-injection evidence for recursive/high-fanout cascades and operator
policy for permanent failures, not on adding broad fanout or request-polling
paths.

DB runtime stats expose cumulative `foreign_keys` counters for child-write
rejects, parent-delete rejects, validation/dry-run/repair runs, scanned child
rows, referenced child rows, scanned reverse-reference rows, missing parents,
missing reverse references, stale reverse references, repaired reverse
references, and deleted stale reverse references.
Writable local DB validation, dry-run, and repair operations also persist a
scoped progress row under internal metadata. The row records operation mode,
optional constraint name, scanned document-key range, completion status,
validity, timestamp, and the latest integrity report counters. Status-only
readers still return validation results without attempting metadata writes.
The public and hosted `foreign-key-integrity` response includes the latest
per-group progress entries so operators can observe the durable cursor/report
state without reading storage metadata directly. For `validate` and `dry_run`,
progress validity means no violations were found in the scanned range. For
`repair`, progress validity means the repair did not leave unrepairable missing
parent rows; the report still preserves the repaired/deleted reference counters.
`{"action":"progress"}` returns every stored FK integrity progress row for each
resolved group without running validation, dry-run, or repair, so operators can
list outstanding or recent integrity work without knowing its exact mode, range,
or constraint. Progress also returns durable FK integrity job records with the
latest pass report, a cumulative aggregate report that adds each pass exactly
once, the latest merged violation sample set, sample count, and truncation flag,
so schema-controller and worker-pass diagnostics survive across bounded passes
and do not depend on the caller observing the failing pass live.

Hosted online validation is built from range discovery, status correlation,
group-local claim persistence, group-local claim-and-run execution/routing,
bounded claimable-unit scheduler passes, stable job identity, schema FK
discovery, and catalog-wide table scanning. Those surfaces are exposed by
`plan`, `work_units`, `work_statuses`, `work_claims`, durable claim rows,
durable per-range progress rows, the DB claim-and-run primitive, the internal
group claimed work-unit route, the public worker-pass request fields,
schema-controller mode, the source-level schema-controller maintenance pass,
stable public/admin `job_id` derivation, and bounded job-record writes for
local, provisioned, local-hosted, and remote-hosted claimed work. Metadata-owned
background rounds now provide the first automatic hosted/provisioned scheduling
shape: the metadata leader runs bounded schema-controller maintenance passes
per lifecycle round, with explicit service config for enablement, worker id,
lease duration, maximum tables per pass, maximum durable jobs per pass, maximum
work units per pass, action-job page size, violation limit, and follow-up pass
budget. If a pass reports incomplete work because a validation/repair job,
action schedule, or action job still has remaining pages, the service runs
additional bounded follow-up passes up to `max_followup_rounds` before yielding
the lifecycle round. The default worker id is
`metadata-fk-schema-controller`; the default lifecycle round advances at most
four tables, sixteen durable validation jobs, sixteen durable action jobs, one
work unit per table/pass, action pages of 1024 children, and four follow-up
passes. Each non-empty pass logs
scanned, pending, executed, job-scanned, job-executed, claim, terminal-valid,
terminal-invalid, invalid-action-schedule, invalid-action-job, completion, and
validity counters, exposes cumulative and last-round counters in metadata
status, accumulates cumulative and last-round FK
integrity report counters for the work it actually ran, and promotes terminal
valid adoption jobs through the metadata table-update path. Metadata status
also records the last failed action-schedule group/id/action-job/action/status/error,
constraint, parent identity, scheduled group count, cascade depth, requeue
count, last requeued timestamp, the last failed action-job
group/id/action/status/error, constraint, parent identity, resume cursor,
attempts, and applied-child count, plus the last terminal
invalid validation/adoption table, constraint, job id,
diagnostic pass count, violating pass count, first/last violation timestamps,
missing-parent, missing-ref, stale-ref, per-kind aggregate violation counts,
sample-count, truncation fields, and a compact first-violation snapshot with
group, kind, child identity, parent identity, and observed parent key when
applicable. The durable
integrity job record remains the source for full bounded violation samples and
cross-pass merged diagnostics, while status gives operators an allocation-free
summary of the latest controller-visible failure. Terminal worker/controller
results are hydrated from that durable job record before metadata adoption and
status accounting consume them, so the public result, metadata status, and
`foreign_key_validation_json` all reflect the cross-pass aggregate report and
merged violation samples rather than only the final work unit that completed the
job.
Hosted/provisioned progress discovery skips catalog ranges whose local group DB
is not materialized yet and retries on the next round; correctness-bearing
execution still requires the addressed group to exist.
Bound/local maintenance already performs the terminal catalog transition for
local tables: after a `validate` or `repair` schema-controller pass completes
and is valid, it rewrites the table schema to set the selected FK
`validation_state` to `enforced` and applies that schema through the normal DB
schema path, so the final flip re-runs enforcement validation before becoming
durable. Provisioned and hosted sources intentionally do not mutate catalog
state from table-write sources because they only hold a read-only catalog
snapshot. Instead, metadata service rounds consume their terminal valid results,
rewrite the selected FK to `enforced`, and enqueue the resulting table record
through the same schema-update/table-upsert path used by admin schema changes.
`AdminSource.updateForeignKeyValidationState` exposes the same catalog flip for
external coordinators. Terminal invalid adoption results are surfaced through
the durable FK job record (`status: "invalid"`, `valid: false`, aggregate
report, and bounded violation sample diagnostics), metadata scheduler warnings,
and `foreign_key_validation_json` in the internal table record. That metadata
records the constraint name, action, optional job id, terminal validity, report
counters, aggregate report counters across diagnostic/completion passes,
truncation flag, bounded-sample count, compact per-kind sample counts, and a
small `violation_samples` array with decoded parent/observed tuple values
without writing reserved states into runtime schema JSON. The durable job record
remains the source for the full bounded sample set merged across passes. A later
terminal valid promotion clears the stale invalid entry for that constraint
while the job's aggregate report preserves the earlier diagnostic history.
Durable validate/dry-run/repair jobs are also resumed by the same metadata-owned
background pass without relying on request polling.

Diagnostics should report:

- FK catalog entries and enforcement state;
- reverse-reference row counts per constraint;
- orphan and stale-reference counts found during validation/repair;
- violation rows with constraint name, child table/key, parent table/key, and
  decoded parent tuple values. Stale-reference rows also report the observed
  parent key and decoded observed tuple values when the child now points
  elsewhere;
- parent-delete rejects;
- child-write rejects;
- repair/rebuild progress.

## Long-Term Production Shape

The broader production shape is a distributed relational constraint system. The
constraint catalog, backing indexes, validation jobs, and 2PC participants must
all agree on the same committed-row invariant; graph indexes and join plans stay
query features, not integrity sources.

### Constraint catalog

Runtime schema has the first durable pieces of a normalized constraint catalog
and should continue to grow around them:

- `foreign_keys`;
- `unique_constraints`;
- delete action policy (`restrict` / `no_action`, `set_null`, `cascade`) and
  update action policy (`restrict` / `no_action`, `set_null`, `cascade`), with
  mutating update actions using the same durable FK action schedule/job/page
  executor as high-fanout parent deletes;
- timing policy (`immediate`, local `deferred` commit-time parent-existence
  validation, local deferred `no_action` delete/update final-state
  reverse-reference validation, distributed deferred child parent checks backed
  by exact coordinator proof records, and restrictive deferred parent-delete
  checks backed by timed FK-ref owner proofs. Distributed mutating action sets
  use durable action schedules/jobs so the same final-state checks and exact
  owner-routed child rewrites can be resumed across bounded pages);
- validation state (`enforced` and local `unvalidated` are public schema states;
  `validating` and `invalid` are reserved for hosted online validation jobs);
- backing physical index names;
- validation / repair cursors;
- violation counters and last validation error metadata.

Constraint catalog changes are schema/storage migrations. Local same-table
add/drop changes by constraint name run under the DB apply lock. New `enforced`
constraints validate existing rows and install required backing indexes before
the runtime schema is persisted. New `unvalidated` foreign keys persist the
catalog entry without write-time enforcement or reverse-reference maintenance.
Applying the same FK definition later with `validation_state: "enforced"` runs
the same validation/repair build before the catalog flip. Changes that reuse a
constraint name with different semantics are rejected. Hosted and provisioned
online validation/adoption runs through durable, cursor-backed FK integrity jobs
that can be advanced by public worker passes or autonomous metadata-owned
controller rounds.

### Constraint indexes

Constraints need synchronous internal indexes maintained in the same transaction
as relational rows:

- child reverse-reference rows for FK parent-delete checks;
- unique rows for parent candidate keys and standalone unique constraints;
- optional pending-intent or range-lock rows for distributed conflict detection;
- validation/repair progress rows for online jobs.

These are integrity metadata, not user-facing search indexes. They must be
rebuildable from committed relational rows and constraint catalog state.
Hosted distributed FK reverse-reference rows should be range-owned by the
referenced parent key, with a catalog resolver that maps
`(child_table, constraint, parent_table, parent_key)` to the FK-ref owner group.

### Constraint participants

Transaction planning must register every table/range needed to validate and
mutate constraint state:

- child table participant writes or deletes the child row;
- parent table participant validates parent existence;
- FK-ref owner participant writes/deletes reverse refs and scans or locks the
  reverse-ref range for parent deletes;
- unique-index participant validates candidate-key uniqueness;
- cascade/set-null participant plans and applies affected child writes.

The participant set has to be known by prepare time. Recovery must either commit
all row/index/constraint metadata mutations or abort all of them.

## Roadmap

### 1. Distributed primary-key `restrict` with routed FK-ref ownership

Use exact parent-key-routed FK-ref ownership for cross-table and cross-shard
enforcement. Hosted mutation planning does not use broad child-table fanout for
this new feature set; missing, transitional, or incomplete owner topology fails
closed before prepare.

Work:

- use the existing explicit FK-ref mutation prepare fields as the owner
  participant's durable write/delete API. This substrate is implemented at the
  prepare parser, transaction request, and storage participant boundary;
- add FK-ref range metadata for
  `(child_table_id, constraint_id, parent_table_id, parent_key_span)`. The
  current metadata/API snapshot shape has the record type and catalog projection
  hook, the in-memory table manager owns, clones, replaces, lists, and removes
  those records with table-drop cleanup, and the raft apply store has first-class
  upsert/remove transition commands plus durable projection/reopen support. The
  metadata state and reconciler include FK-ref owner ranges in desired/current
  topology, emit upsert/remove plan entries, apply them through the in-process,
  HTTP, and simulation metadata services, expose
  `projected_foreign_key_ref_ranges` status, and plan replica placement for
  owner groups using the parent table's placement policy. Reconciliation now
  derives a deterministic full-span owner range for each active primary-key FK
  whose parent table exists, preserves any existing split owner ranges for that
  FK identity, and removes owner ranges when the FK is no longer declared or no
  longer active. Owner ranges carry validated lifecycle states: `active`,
  `splitting`, `merging`, and `rebuilding`. Only `active` ranges are routable;
  transitional ranges still participate in topology hashing and make ownership
  appear configured, so coordinators fail closed instead of writing through a
  range whose handoff or rebuild has not finished. The table manager rejects
  overlapping ranges for the same `(child table, constraint, parent table)` FK
  identity unless the write is replacing the exact same start key. The in-memory
  metadata manager now exposes semantic begin/finish operations for FK-ref range
  split, merge, and rebuild, with those operations moving ranges through
  non-routable transitional states before publishing active ownership again. The
  raft metadata log also has semantic begin/finish transition commands for those
  operations, so replicated metadata replay no longer has to express lifecycle
  changes as ad hoc raw upsert/remove rows. The metadata service, HTTP service,
  simulation service, and table workflow expose typed helpers that validate and
  propose those semantic commands. Internal metadata HTTP routes and client
  helpers expose the same name-based lifecycle contract for operators and
  controllers: child table from the route, constraint name and parent table from
  the request body, and range selection by parent-key span. Public/admin
  orchestration for split/merge/rebuild data movement should drive these typed
  lifecycle operations instead of issuing raw upsert/remove metadata changes
  directly;
- add a catalog resolver for
  `(child_table, constraint, parent_table, parent_key) -> owner group`. The
  resolver is implemented over `AdminSnapshot.foreign_key_ref_ranges` and
  returns configured owner groups plus a topology epoch hash. Group-level
  prepare/begin validation now accepts either the normal table range epoch for
  row participants or the FK-ref identity epoch for owner participants, and
  rejects stale epochs so coordinators cannot write FK-ref rows to an old owner
  range after split, merge, or rebuild;
- move hosted distributed reverse-reference maintenance onto the FK-ref owner
  participant instead of the local child row participant. Insert-only child
  writes with `expected_version: 0` now register the exact owner participant and
  send a FK-ref write mutation. Unversioned full-row writes and
  version-predicated updates/deletes read the old child row when needed, verify
  or inject the row version proof, and route old/new FK-ref owner mutations.
  FK-reference transforms also coalesce same-key writes, deletes, and transforms
  before prepare, read-pin or validate the affected row when needed, then
  register old/new FK-ref owner participants plus parent-existence participants.
  Transforms that target unsupported non-primary FK shapes remain fail-closed;
- make parent deletes register only the FK-ref owner ranges for each deleted
  parent key and referencing constraint. This exact routed path is implemented
  for primary-key and declared unique-target `restrict`, `set_null`, and bounded
  `cascade` FKs. Owner scans use a deterministic child-table/child-key page
  cursor in the internal storage/API contract; interactive parent deletes drain
  those cursor pages before prepare and fail closed if the owner topology or
  cursor contract is invalid;
- use the FK-ref owner participant as the conflict point between child reference
  creation and parent delete;
- support split/merge/rebuild of FK-ref ranges using topology epochs. The
  table-manager protocol, raft transition commands, metadata service helpers,
  simulation helpers, table workflow methods, internal metadata HTTP routes, and
  HTTP client methods for those state transitions are the control-plane shape.
  Public/admin orchestration must drive these typed lifecycle transitions and
  observe data-movement progress; raw upsert/remove metadata changes are not a
  supported control-plane interface for this feature set;
- make parent-delete explain use the same FK-ref owner resolver rather than
  child-table range fanout. Hosted/provisioned explain now reads the parent row,
  resolves configured FK-ref owner ranges, scans bounded owner prefixes, reports
  routed `restrict` blockers and first-hop `set_null` / `cascade` child counts,
  and fails closed on incomplete scans. The storage layer also has an
  owner validation/dry-run/repair primitive for both one routed
  `(constraint, parent_table, parent_key)` prefix and a parent-key span owned by
  one FK-ref range: it verifies each owner ref row against the current child row
  and can prune stale owner rows without scanning every child range. Hosted
  validation and repair use the existing plan/claim/run path with phase-aware
  durable work units: `child_range` creates missing owner rows, and
  `owner_range` prunes stale routed owner rows for the parent-key spans owned in
  metadata;
- retain fail-closed behavior for operations whose final FK value or affected
  FK-ref owner set cannot be known before prepare.

Invariant:

```text
Every committed non-null child FK has exactly one committed routed FK-ref row
owned by that constraint and parent key. A child insert referencing parent p and
a delete of parent p must conflict at the FK-ref owner unless the same
transaction removes or rewrites every surviving child reference.
```

### 2. Repair and validation tooling

Add operational tooling before broadening constraint semantics. The local
storage primitive exists; the public endpoint and hosted group fanout cover
interactive validate/repair/list operations.

Implemented:

- public/admin `validate constraint` backed by the DB primitive;
- public/admin `dry_run` repair preview that reports repair counters without
  mutating reverse-reference rows;
- public/admin `repair constraint index` backed by the DB primitive;
- public/admin `list constraint violations` backed by owned violation rows;
- optional constraint-name scope for all four actions, including hosted remote
  group fanout;
- hosted routing for the same operations across all resolved table ranges;
- DB/runtime stats for orphan count, reverse-ref scans, parent-delete rejects,
  child-write rejects, validation runs, dry-run runs, repair runs, and repair
  progress;
- durable local progress rows for validation, dry-run, and repair operations,
  keyed by `(mode, constraint, lower_doc_key, upper_doc_key)` so distinct range
  checkpoints do not overwrite each other;
- temporal-FK validation and repair use period catalog metadata plus temporal
  unique-owner coverage, not scalar parent lookup, when deciding whether a
  missing reverse-reference row can be rebuilt;
- temporal-FK parent actions use that same coverage proof before mutating a
  child: `restrict` / `no_action` reject only when final coverage is broken,
  while `set_null` and bounded `cascade` leave still-covered children untouched
  and only update or delete children whose final spans are no longer covered by
  remaining parent intervals;
- public/hosted integrity responses that return the latest per-group progress
  rows alongside aggregate report counters and violations;
- worker-facing `work_statuses` that join planned units to durable progress rows
  and expose deterministic per-unit `claim_key`s for future hosted background
  workers;
- durable group-local claim rows keyed by `claim_key`, with lease owner,
  deadline, attempt count, same-worker renewal, and expired-lease takeover;
- group-local claim-and-run execution for FK integrity units, which claims a
  lease and records the same durable progress row as interactive validation,
  dry-run, or repair;
- bound, provisioned, hosted, and internal group table-write routing for claimed
  FK integrity work units, so hosted workers can execute a planned unit on its
  owning group through the same routed internal write surface used by normal
  group-local operations;
- public bounded worker-pass execution for FK integrity `validate`, `dry_run`,
  and `repair`, using `worker_id`, `lease_ms`, and `max_work_units` to select
  unclaimed, incomplete, or expired-lease units, route them to their owning
  groups, and return refreshed progress/claim/status state plus the effective
  job id;
- explicit or generated `job_id` worker-pass persistence for local,
  provisioned, and hosted claimed-unit execution, preserving job intent, worker
  identity, lease/pass limits, status, attempts, completion, validity, and
  latest report across reopen in the DBs that own the planned work;
- bounded FK job violation diagnostics: every worker pass stores its own latest
  report, adds that pass once to the aggregate report, and merges distinct
  reported violations into a bounded JSON sample, sample count, truncation flag,
  diagnostic-pass counters, violating-pass counters, and first/last violation
  timestamps, while completion without fresh samples preserves any prior
  diagnostic sample for the same durable job;
- DB owner validation/dry-run/repair for a single parent-key prefix and for a
  routed FK-ref owner range parent-key span. The primitive scans owner rows,
  detects rows whose child row is missing or now references a different parent,
  and repairs by deleting those stale owner rows without mutating child rows;
- phase-aware worker planning and claimed execution for provisioned/hosted FK
  integrity jobs. Worker plans append `owner_range` units for routable FK-ref
  owner ranges that still match enforced immediate schema FKs, internal/remote
  claimed-unit requests carry the phase, and progress records are keyed by
  `(phase, mode, constraint, range)` so owner-range scans cannot overwrite
  child-range checkpoints for the same constraint/span;
- DB-local durable FK action jobs for data-changing `set_null` and `cascade`
  fanout. Each claimed pass scans one FK-ref child cursor page, applies exact
  child-key actions in a transaction, and persists the resume cursor and applied
  count before the next pass. Page completion is claim-fenced against the
  current durable worker, claim timestamp, lease deadline, and attempt number, so
  stale workers cannot advance cursors after lease handoff. Failed page execution
  persists `last_error` and an invalid status for diagnostics/retry visibility,
  and the finish path rejects ambiguous cursor/error combinations before
  updating the durable job row. A duplicate finish for an already-finished claim
  is also rejected and cannot double-count `applied_children` or rewind the
  resume cursor.
  Durable records canonicalize SQL-style action aliases before storage, so
  direct DB callers, transaction action-schedule mutations, hosted internal
  routes, public/admin requeue operations, and source-layer distributed action
  pages compare the same canonical action identity across retries. Source-layer
  distributed action pages derive their 2PC transaction id from the durable
  action-job identity, resume cursor, page limit, cascade depth, and requeue
  count so lease handoff after a crash reuses the same transaction identity but
  cursor advancement or explicit requeue creates a new one.
  Operators/controllers can explicitly requeue an incomplete failed job after
  the underlying cause is corrected; requeue validates the durable action
  identity, rejects completed jobs and healthy pending/claimed jobs, clears
  claim/error/lease state, updates the intended worker/page limit, and preserves
  the existing child cursor, attempt count, and applied-child count so large
  operations resume rather than restart.
  The table-write source layer exposes the same operation for local,
  provisioned, hosted-local, and hosted-remote owner groups, and the internal
  group route accepts `requeue_only` action-job requests so hosted controllers
  can drive owner-local retry without waiting for stale leases to expire.
  Action schedules have the same explicit requeue shape for topology failures
  before owner jobs exist: DB-local, table-write source, public/admin, and
  hosted internal group routes validate the schedule/action-job identity,
  reject completed schedules and healthy pending schedules, clear `last_error`,
  update worker/page limits, increment `requeue_count`, record
  `last_requeued_at_ns`, and move the schedule back to `pending` for a later
  bounded seeding pass.
  FK progress rows, validation/repair claim leases, action schedules, and
  action-job claim leases are timestamped with realtime so autonomous hosted
  controllers can make consistent expiry decisions across owner DBs instead of
  comparing local monotonic clock domains;
- DB-local durable FK action schedule records. Bound/local controller
  maintenance discovers pending schedule records, seeds the corresponding
  action jobs, marks schedules `seeded`, returns schedule statuses and
  schedule scan/seed counters, and then advances the visible action jobs
  through the existing action-job controller. Metadata runtime status records
  both schedule and action-job counters. If schedule or action-job budgets leave
  durable records pending, the maintenance round remains incomplete even though
  the work it did run is committed. Provisioned and hosted sources expose
  schedule progress/marking across local and remote owner groups through
  internal group routes. The generic controller refuses to mark a schedule
  `seeded` when the scheduler reports zero owner jobs, preserving the schedule
  as incomplete rather than acknowledging a large operation that has no
  resumable action work;
- owner-routed table-write action-job execution for local/provisioned and
  hosted-local owner groups. The source layer resolves FK-ref owner ranges from
  catalog metadata, fails closed when ownership is not configured, and returns
  an operator-visible status per owner group;
- metadata FK controller resumption of existing durable action jobs visible in
  local/provisioned/hosted owner DBs. The controller lists table data ranges
  plus FK-ref owner ranges, uses hosted internal group progress for remote
  owners, skips jobs leased by other workers, advances bounded pages through the
  same table-write action-job primitive, and records action-job
  scanned/executed counts and per-group status in the maintenance summary;
- recursive routed cascade planning for durable action pages. Each cascade page
  deletes only the current owner page of children, then atomically seeds the
  next-level `set_null`/`cascade` schedules or restrictive conflict checks for
  rows that reference those deleted children, so deep cascades are represented
  as resumable queue records across owner groups rather than a single unbounded
  transaction. Generated recursive schedules carry durable depth/max metadata
  and fail closed at the configured max depth before another mutating dependent
  layer can be committed;
- metadata leader-owned autonomous FK controller rounds for hosted/provisioned
  validation, repair, action schedules, and action jobs. Configured lifecycle
  rounds run bounded work without request polling, use follow-up rounds for
  incomplete work, and expose cumulative/last-round counters plus compact last
  action-job and terminal-invalid diagnostics in metadata status.

Ongoing hardening:

- keep expanding failure-injection coverage for durable `set_null` / `cascade`
  schedule and action-job recovery across participant retries, topology
transitions, process restarts, and lease handoff. The production shape is
already exact owner-routed scheduling plus idempotent, cursor-backed action
pages; future work should prove more interleavings, not add broad fanout paths.

Repair may rebuild missing/corrupt secondary metadata, but it must not silently
mutate user rows. Orphaned child rows should be reported for explicit operator
action.

### 3. Online FK add/drop

Allow FK catalog mutation through an explicit migration path.

Implemented locally:

- constraint-name additions are accepted when the relational base-column catalog
  is unchanged;
- existing child rows are scanned before the catalog is enabled;
- parent existence must validate for every non-null child FK value;
- missing reverse-reference rows are created before the runtime schema is
  persisted;
- `unvalidated` FK additions are catalog-only and not enforced by the write
  participant;
- applying the same FK definition as `enforced` later validates existing rows,
  repairs missing reverse-reference rows, and persists the enforced state only
  after validation succeeds;
- constraint-name drops delete the old reverse-reference rows;
- same-name semantic changes are rejected so backing rows cannot be reused under
  a different constraint contract.

Remaining distributed/online job flow:

1. Run the unvalidated-to-enforced validation/build through a hosted resumable
   job instead of one local apply-lock scan, using the durable progress row as
   the per-range checkpoint substrate.
2. Build reverse-reference rows from committed child rows across every resolved
   table range.
3. Validate parent existence for every non-null child value across
   participants.
4. Enforce or dual-write for concurrent writes during distributed validation.
5. Atomically flip every range's catalog entry to `enforced`.

Drop flow:

1. Mark the FK not enforced for new writes.
2. Preserve the catalog entry while readers/repair see the transition.
3. Asynchronously remove reverse-reference rows.
4. Remove the catalog entry after cleanup completes.

### 4. Unique constraints

Local unique constraints over ordered non-`json` relational column tuples are
implemented as committed integrity rows maintained by the relational write
participant. They are required before foreign keys can reference non-primary
parent columns. Local FK-to-unique resolution is implemented.

Implemented:

- `unique_constraints` in the parsed, runtime, and persisted schema catalog;
- committed unique keys such as
  `unique:<constraint>:<encoded_column_tuple> -> row_id`;
- insert/update/delete enforcement in the local relational write participant;
- transaction-intent commit enforcement through the same participant;
- nullable behavior: rows with any absent nullable component create no unique
  row, so multiple partial tuples are allowed;
- local same-table schema updates can add unique constraint names by scanning
  existing rows, rejecting duplicate present tuples, and installing committed
  unique rows before the catalog flip;
- local same-table schema updates can drop unique constraint names by deleting
  their backing rows;
- same-name semantic changes are rejected;
- the transaction prepare contract has explicit unique constraint write/delete
  mutations. A unique-owner participant validates the declared constraint,
  committed owner, and same-transaction handoff before writing or deleting the
  committed unique integrity row, so future distributed unique ownership can
  target a first-class storage operation instead of smuggling unique rows
  through user document writes;
- unique-owner range topology has a first-class catalog shape:
  `UniqueConstraintRangeRecord` maps
  `(table_id, constraint_name, encoded_tuple span) -> group_id` with topology
  epochs and `active`/`splitting`/`merging`/`rebuilding` states. The catalog
  resolver maps `(table, constraint, encoded_tuple) -> owner group` and validates
  group topology epochs against that unique-owner topology, matching the FK-ref
  owner routing contract;
- the table manager owns unique-owner ranges, rejects overlapping spans for the
  same table/constraint, prevents group collisions with table and FK-ref owner
  ranges, removes unique-owner ranges with their table, and supports two-phase
  split/merge/rebuild state transitions with topology epoch bumps;
- metadata raft transition commands, binary codecs, durable apply-store keys,
  projection-list APIs, projection signals, and reopen tests exist for
  unique-owner upsert/remove plus split/merge/rebuild lifecycle commands;
- metadata services and hosted metadata services expose unique-owner mutation
  helpers, projected list/free APIs, admin-snapshot projection, projection-cache
  invalidation, and status counters for projected unique-owner ranges;
- metadata state capture, reconcile planning, reconcile summary counters, service
  apply paths, hosted simulation batching, and placement planning treat
  `UniqueConstraintRangeRecord` as a first-class constraint-owner range. Desired
  unique-owner topology is diffed against projected topology, stale ranges are
  removed by owner identity, range updates preserve lifecycle state/epoch
  changes, and owner groups receive replica placement using the owning table's
  placement role and replica count;
- table workflow exposes unique-owner range upsert/remove and two-phase
  split/merge/rebuild lifecycle methods, matching the FK-ref owner range API;
- hosted distributed single-table and multi-table insert-only writes
  (`expected_version: 0`), version-bound updates, version-bound deletes, and
  unversioned full-row writes/deletes on multi-range tables route complete
  unique tuple changes through the configured unique-owner range. Updates
  compare the old row with the new row and register the exact unique
  delete/write handoff before prepare. Unversioned full-row writes first read
  the current row from its table group, inject either the observed row version or
  `expected_version: 0` for a missing row as a prepare predicate on the row
  participant, and use that durable proof for unique-owner mutation planning.
  Deletes remove the old tuple from the same owner range. Unversioned deletes
  use the same observed-version proof for unique-owner cleanup. Missing owner
  topology and transitional owner topology still fail closed before prepare;
- transforms that can change a unique column or upsert a row on a multi-range
  unique table use the same final-row planning model as FK-reference
  transforms. The coordinator coalesces same-key writes, deletes, and
  transforms in storage order, read-pins or validates the affected row when
  needed, then routes old/new tuple mutations through the unique-owner range.
  Transforms that cannot touch unique columns still route only to the row's
  owning group. Single-range tables continue to use local participant
  enforcement;
- the storage layer exposes a unique-constraint integrity reconciliation
  primitive over a document-key range. It scans committed relational rows,
  derives expected unique integrity rows, reports missing, stale, and duplicate
  unique rows, supports non-mutating dry-run repair, and can rebuild missing or
  mismatched unique rows plus prune stale unique rows without mutating user
  rows. Duplicate user rows are reported and left for explicit operator action;
- the public table operation
  `POST /tables/{table}/unique-integrity` exposes the same primitive with
  `validate`, `dry_run`, and `repair` actions plus optional document-key range
  bounds. Local/bound sources execute directly, provisioned sources fan out
  across every resolved local table range, and hosted sources route each group
  to its leader through the internal group `unique-integrity` operation.
  Public and internal unique-integrity action names use the same
  case-insensitive space/hyphen/underscore normalization as FK integrity
  actions, while responses keep canonical snake_case mode names.
  Responses include aggregate counters, per-group counters, and stored progress
  rows. `validate`, `dry_run`, and `repair` persist durable progress records
  keyed by `(mode, lower_doc_key, upper_doc_key)`; `progress` lists those rows
  without rescanning table data. Dry-run reports the rows it would repair or
  delete without mutating integrity rows; repair rebuilds missing/mismatched
  unique rows and deletes stale unique rows, while preserving duplicate user
  rows for explicit operator resolution;
- provisioned and hosted unique-integrity responses include
  `owner_topology`, a catalog inspection of unique-owner ranges for the table.
  It reports declared/configured constraint counts, active/transitional range
  counts, a topology epoch, and the observed owner ranges. `complete` is true
  only when every declared unique constraint has active contiguous coverage from
  the empty encoded tuple through the open-ended upper bound, with no
  transitional or stale configured constraints.

Remaining work:

- hosted/distributed unique validation/build jobs using the same durable
  validation-state model and unique-owner topology.

The unique index is an integrity index. It cannot be eventually consistent and
cannot be replaced by a search, algebraic, or graph index.

### 5. FKs to unique parent columns

Local same-table FK targets can reference declared unique parent column tuples.
Hosted distributed child writes can validate same-table or cross-table unique
parent tuples by routing the parent check through the referenced parent table's
configured unique-owner ranges. Cross-table targets are admitted by schema
parsing, then proved by hosted catalog metadata before prepare.

Implemented:

- allow same-table FK references to target a declared unique parent column
  tuple;
- encode child values with the same tuple encoder as the parent unique index;
- probe the parent unique index, then validate the referenced parent row still
  exists;
- restrict parent unique tuple updates while child reverse references exist;
- maintain and repair reverse FK rows keyed by the unique tuple;
- hosted same-table unique-parent checks use the same tuple encoder, include the
  parent unique constraint name in the prepare request, and validate against the
  unique-owner participant's final staged unique-index state;
- hosted cross-table unique-parent checks resolve the referenced parent table's
  schema, require a matching declared unique constraint, route through that
  parent table's unique-owner topology, and mark the child participant's local
  parent checks externalized;
- hosted versioned same-table and cross-table `restrict` parent deletes over
  unique tuples route an encoded-tuple parent-delete check through the FK-ref
  owner participant;
- hosted versioned same-table and cross-table `set_null` and bounded `cascade`
  parent deletes over unique tuples route through the same FK-ref owner
  topology, delete the exact owner ref rows, and send exact encoded-tuple
  child-key actions to the affected child-row participants;
- hosted unversioned same-table and cross-table parent deletes over unique
  tuples read the parent row before prepare, inject the observed row version as
  a parent-row predicate, encode the old unique tuple from that read, and route
  `restrict`, `set_null`, or bounded `cascade` through the FK-ref owner range;
- hosted parent updates that change a referenced unique tuple read and
  version-pin the old parent row, route the old encoded tuple through the
  FK-ref owner participant, and either send an update absence check for
  `restrict`/`no_action` or write a conflict check plus durable `update_set_null`
  / `update_cascade` action schedule for mutating actions;
- child reference writes, child reference deletes, parent delete/update absence
  checks, and mutating parent action schedules all stage the same durable
  FK-conflict intent keyed by `(constraint, parent_table, parent_key)`, so
  child-reference and parent-delete/update races meet at the FK-ref owner
  participant before commit.

### 6. Composite FKs

Composite FKs to declared unique parent tuples are implemented locally and in
hosted distributed child-write planning when the referenced parent table has
unique-owner topology.

Implemented:

- ordered multi-column child references for unique parent targets;
- use one canonical tuple encoder for unique indexes, FK probes, and reverse
  references;
- validate child column order against the parent target order;
- null behavior: any child component null or absent means no reference;
- validation/list output includes decoded parent tuple values for unique and
  composite FK targets;
- hosted same-table and cross-table composite FK child writes route the encoded
  parent tuple through the referenced parent table's unique-owner range and mark
  the child participant's local parent check externalized.

Remaining work:

- support composite row identity if Antfly later adds composite primary keys.

### 7. `on_delete: set_null`

Local `set_null` is implemented for nullable child FK columns with bounded local
fanout. It is a child update, not just a parent-delete check.

Implemented:

- parent delete scans the reverse-reference range;
- local same-table unique-parent update with `on_update: "set_null"` scans the
  old reverse-reference range and clears nullable child FK columns that point at
  the changed tuple;
- local same-table unique-parent update with `on_update: "cascade"` scans the
  old reverse-reference range, copies the new parent tuple cells into the child
  FK columns, and moves reverse-reference rows to the new tuple;
- planner creates child updates that set the FK columns to null;
- child updates go through the normal relational write participant so derived
  indexes and reverse refs are updated consistently;
- reject schemas where the child columns are non-nullable;
- local execution enforces a max affected-row limit;
- local parent-delete explain uses the same participant planner, aborts before
  commit, and reports existence, allow/block status, block reason, planned
  set-null updates, cascade deletes, row deletes, index deletes, and writes;
- the public/hosted foreign-key integrity operation exposes the same
  parent-delete explain report through `action: "explain_delete"`. Hosted and
  provisioned explain use exact FK-ref owner discovery, drain the owner prefix
  through child-table/child-key cursor pages, report the first-hop child set-null
  count, and fail closed when owner topology or page cursors are invalid;
  otherwise they route to the target document key's owning group.
- hosted distributed parent deletes for primary-key and declared unique-target
  `set_null` use exact routed FK-ref owner discovery.
  The coordinator writes a deterministic FK action schedule mutation to a
  resolved FK-ref owner in the same 2PC as the parent delete. Controller
  maintenance seeds idempotent action jobs across the current owner groups, and
  each job drains child-cursor pages to validate FK metadata, rewrite nullable
  FK columns or delete child rows, and update derived indexes and local reverse
  refs through the normal relational write participant.
- missing, transitional, or incomplete FK-ref owner topology fails before
  prepare. Hosted distributed `set_null` uses exact owner routing as the
  production contract for this new feature set.

Ongoing hardening:

- keep expanding operator policy and alerting around the existing durable
  schedule/action-job diagnostics, especially thresholds for repeated
  dead-letter schedules, stuck leases, topology churn during page execution, and
  permanent action-job failures.

### 8. `on_delete: cascade`

Local cascade is implemented with bounded recursive delete planning. Cascade is
the highest-risk action because it recursively plans deletes.

Implemented:

- parent delete scans the reverse-reference range;
- planner recursively prepares child deletes through the normal relational write
  participant;
- duplicate/cyclic local deletes are suppressed by the batch row-delete set;
- local execution enforces max depth and max affected-row limits;
- local parent-delete explain uses the same recursive planner, aborts before
  commit, and reports planned cascade child deletes alongside other delete-plan
  counters;
- the public/hosted foreign-key integrity operation exposes the same
  parent-delete explain report through `action: "explain_delete"`. Hosted and
  provisioned explain use exact FK-ref owner discovery, drain the owner prefix
  through child-table/child-key cursor pages, report the first-hop cascade child
  delete count, and fail closed when owner topology or page cursors are invalid;
  otherwise they route to the target document key's owning group.
- hosted distributed parent deletes for primary-key and declared unique-target
  cascade use exact routed FK-ref owner discovery.
  The coordinator writes a deterministic FK action schedule to a resolved
  FK-ref owner in the same 2PC as the parent delete. Controller-seeded action
  jobs drain child-cursor pages, validate the FK metadata, verify each child
  still references the parent identity, and delete exact child rows through the
  normal relational write participant. Local recursive cascade planning still
  runs on that participant, so same-range descendants are deleted with the same
  row/index/reverse-ref cleanup.
- hosted distributed parent deletes for primary-key and declared unique-target
  cascade fail closed when FK-ref owner topology is absent; broad child-range
  fanout is not used for cascade because it is not a deterministic dependency
  graph.

Ongoing hardening:

- strengthen recovery/idempotence proofs for recursive durable cascade action
  pages across retry, topology transition, and process restart boundaries;
- define cross-table allowed cyclic cases beyond local duplicate suppression;
- keep expanding operator policy for depth-cap failures, repeated dead-letter
  action jobs, and dependency chains that require policy changes. Metadata
  status already exposes cumulative and last-failure action schedule/job
  diagnostics, including cascade-depth-limit failures;
- keep expanding failure-injection coverage for recovery after participant
  prepare failures in distributed cascade action rounds.

Cascades should not be implemented as background best-effort cleanup. The parent
delete and every cascaded child delete must share one atomic commit boundary, or
the operation should be rejected and retried through a bounded job system with
explicit intermediate state.

### 9. Deferrable constraints

Deferrable constraints validate final transaction state instead of each write as
it arrives.

Implemented:

- local `timing: "deferred"` FK parent-existence checks are staged and
  validated against the participant's final planned state before commit;
- same-transaction parent/child writes can be prepared in either input order;
- missing parents still reject at local commit;
- `on_delete: "restrict"` remains restrictive and is never relaxed by deferred
  timing;
- deferred `on_delete: "no_action"` checks parent deletes against the final
  reverse-reference state, so child reference rewrites/deletes can be prepared
  after the parent delete inside one local transaction;
- `on_update: "restrict"` checks for referenced unique parent tuples remain
  restrictive and are never relaxed by deferred timing;
- deferred `on_update: "no_action"` checks referenced unique parent tuple
  changes against the final reverse-reference state, so parent updates and child
  reference rewrites can be prepared in either order;
- transaction-level timing overrides for named deferrable constraints can set
  effective `immediate` or `deferred` behavior, are planned by distributed
  commits as child-table-scoped options, and are persisted with transaction
  intents so replay/recovery uses the same effective timing;
- distributed/routed child writes and reference-changing transforms register
  parent or unique-owner participants and carry exact deferred proof records to
  child participants, so the child side skips only the FK reference that the
  coordinator proved elsewhere;
- exact parent-check proof records are the only distributed parent-check
  externalization mechanism; relational participants require a matching record
  for the child key, parent identity, constraint, and effective timing before
  skipping local parent validation;
- distributed/routed restrictive parent deletes over deferred FKs register the
  FK-ref owner participant and carry an exact timed parent-delete proof, so the
  owner validates its final reverse-reference state before prepare succeeds;
- provisioned and hosted-local `set_null` / `cascade` action jobs use the
  DB-local fast path only when catalog metadata proves the owner prefix and the
  full child table are the same single group; otherwise a claimed owner page is
  executed as a source-layer 2PC that deletes FK refs on the owner participant
  and mutates child rows on their resolved child participants;
- action-job page finish is fenced by the durable claim identity and the current
  row status, so duplicate finish calls after a successful cursor advance are
  rejected without double-counting applied children;
- routed child-row update action pages (`set_null`, `update_set_null`, and
  `update_cascade`) read and pin each affected child row before prepare,
  compute the row's final FK value, and route any unique-owner mutations or
  downstream FK parent-update participants introduced by that final row into the
  same source-layer 2PC;
- mixed immediate/deferred local transactions keep proof exactness per
  constraint: an externalized proof for one deferred FK never suppresses local
  final-state validation for another immediate FK on the same row;
- deferred FK writes fail closed when the exact proof set is missing,
  incomplete, or mismatched.

Ongoing hardening:

- continue expanding recovery and failure-injection coverage for routed action
  pages that combine child-row rewrites, unique-owner handoff, and downstream FK
  parent checks across retries, topology transitions, and process restarts;
- continue expanding negative tests for missing exact proofs, uncommon
  cross-table / unique-parent mixed-timing variants, and SQL-style timing text
  variants. The distributed transaction parser accepts ordinary
  `SET CONSTRAINTS ... IMMEDIATE|DEFERRED` spellings, quoted identifiers,
  target lists, whitespace separators, and a single trailing statement
  terminator.

Deferrable constraints depend on the distributed participant model, unique
indexes, and cascade/set-null planning. The current implementation enforces the
final-state model locally and through exact routed proofs; continued hardening
should expand recovery and negative-proof coverage for uncommon mixed-action
transactions.

## Recommended Order

The major implementation order was:

1. Distributed/table-resolved primary-key `restrict`.
2. Repair and validation tooling.
3. Distributed/online hardening for FK add/drop validation.
4. Distributed/online hardening for unique constraints.
5. Cross-table/table-resolved FK targets for unique parent columns and composite
   unique tuples.
6. Composite row identity, if Antfly adds composite primary keys.
7. Distributed/large-operation execution for `on_delete: set_null`, including
   hosted remote page execution through the same owner-cursor/source-2PC model,
   autonomous controller ownership, and recovery diagnostics.
8. Distributed recursive graph and large-operation execution for
   `on_delete: cascade`.
9. Mutating `on_update` actions backed by the same durable FK action-job
   executor.
10. Distributed deferrable parent-delete/action constraint sets.

Production work should continue to harden idempotence/recovery proof tests
across topology changes and restart boundaries, dead-letter/requeue operator
policy, and broader SQL adapter parity edge cases. Distributed cascades and
deferred parent-delete constraint sets use full transaction planning across the
affected owner and child rows, so they should keep receiving the widest
failure-injection coverage.

## Testing Plan

Minimum coverage:

- schema validation rejects unsupported FK shapes;
- child insert succeeds when parent exists;
- child insert fails when parent is missing;
- nullable child FK accepts `null`;
- child FK update moves the reverse-reference row;
- child delete removes the reverse-reference row;
- parent delete is rejected while a child reference exists;
- parent delete succeeds after child delete;
- same transaction can create parent and child;
- same transaction can delete/update child and then delete parent;
- parent-delete explain reports restrict blocks, set-null updates, and cascade
  deletes without mutating committed rows;
- crash/reopen preserves FK catalog and reverse-reference rows;
- repair rebuilds reverse-reference rows from relational child rows, including
  temporal FK refs whose parent coverage is proven through temporal owner rows;
- temporal parent deletes with `set_null` or bounded `cascade` skip children
  that remain fully covered by other parent intervals and apply the action once
  final coverage is broken;
- cross-shard 2PC failure/recovery does not leave dangling references.
- routed FK-ref ownership resolves only the owner range for a deleted parent key;
- child writes register FK-ref owner participants for old/new parent keys;
- parent deletes conflict with concurrent child reference creation at the FK-ref
  owner participant;
- FK-ref range split/merge preserves ownership and rejects stale topology epochs;
- routed repair recreates missing FK-ref rows in the owner range and the
  DB owner-prefix/owner-range primitive prunes stale rows without mutating child
  rows.
- FK integrity `plan` returns deterministic per-group child-range work units
  clipped to the requested span, and hosted/provisioned validate, dry-run,
  repair, and list execute by those planned units.
- FK integrity `work_statuses` expose deterministic claim keys and progress
  state for planned units so hosted validation workers can resume by durable
  unit identity.
- FK integrity group DBs persist claim leases by `claim_key`, reject active
  leases held by another worker, allow same-worker renewal, and allow expired
  takeover after reopen.
- FK integrity group DBs can claim and run one validation, dry-run, or repair
  unit and persist progress for that exact unit boundary.
- FK integrity worker planning includes routed `owner_range` units for active
  FK-ref owner ranges, and group-local claim-and-run execution records
  phase-isolated progress for those owner spans.

## Documentation Status

- `RELATIONAL.md`: documents FK support in the supported constraints section and
  links to this design.
- `SCHEMA.md`: documents public schema syntax, validation rules, and migration
  limits.
- API/OpenAPI schema: exposes `foreign_keys` on `TableSchema`.
- Operational surface: `POST /tables/{table}/foreign-key-integrity` validates,
  dry-runs repairs, repairs, lists FK reverse-reference violations, plans
  per-range validation work, reports stored progress, exposes per-unit
  worker-status claim keys, advances bounded worker passes with explicit or
  generated durable `job_id` records, and explains parent deletes for local and
  hosted table storage.
- In-process FK maintenance surface: table-write sources expose a bounded
  schema-controller maintenance pass that scans bound/local or catalog-backed
  tables for non-enforced FK declarations and scans durable group-local
  validate/dry-run/repair job records to advance incomplete jobs without
  requiring public HTTP polling. Bound/local maintenance also promotes a
  terminal valid adoption job to `enforced` by applying the rewritten schema
  through the ordinary local schema validator. Metadata-owned provisioned/hosted
  rounds invoke the same bounded pass automatically and promote terminal valid
  adoption jobs through the metadata table-update path; durable job-controller
  results are reported separately and do not mutate catalog validation state.
  Catalog-backed hosted/provisioned progress discovery tolerates
  not-yet-created group DBs by skipping the group for that round; validation,
  repair, parent-delete explain, set-null, and cascade execution continue to
  require the target group to exist.
  Service config controls whether the pass runs, worker id, lease duration,
  table/job/work-unit limits, and violation limit. Non-empty rounds log
  aggregate scheduler counters and metadata status exposes cumulative and
  last-round scheduler counters plus cumulative and last-round FK integrity
  report counters. Terminal invalid adoption jobs remain durable FK job records,
  bounded job violation samples, scheduler diagnostics, and internal table
  validation metadata instead of runtime schema states.
  `AdminSource.updateForeignKeyValidationState` provides the same explicit
  catalog transition primitive for external coordinators.
- Unique operational surface: `POST /tables/{table}/unique-integrity`
  validates, dry-runs repair, repairs, and reports stored progress for unique
  integrity rows on local, provisioned, and hosted relational tables.
- Planning surface: the DB and public/hosted operation can explain one parent
  delete using the same FK participant path as commit and return non-mutating
  set-null/cascade/reject counters.
- Large-operation surface: high-fanout `set_null`/`cascade` runs through durable
  owner-routed action schedules and action jobs with explicit requeue,
  dead-letter, depth-limit, lease, and metadata-status diagnostics.
