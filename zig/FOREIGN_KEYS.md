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
Hosted restrict parent deletes use routed FK-ref owner ranges for exact
parent-key checks when that topology is configured; otherwise they
conservatively register all ranges of child tables that declare a matching
primary-key FK and scan child reverse-reference rows plus staged child writes.
Hosted primary-key `set_null` and bounded primary-key `cascade` parent deletes
use the same owner topology when it is available: the coordinator scans the
FK-ref owner prefix through a deterministic child-table/child-key page contract
before prepare, registers the exact child-row participants discovered by the
cursor-drained scan, sends explicit FK-ref delete mutations to the owner
participant, and sends direct child-key set-null or cascade actions to each
child participant. The current interactive coordinator drains all owner pages
into one 2PC before prepare; the same cursor contract is the substrate for
future durable, multi-round large-operation execution when a hot parent should
not be planned in one foreground request. That gives
cross-table primary-key FKs the same recovery substrate as ordinary relational
writes, makes missing parents fail prepare on the parent participant, and makes
known child references fail prepare, rewrite, or delete on the FK-ref owner or
child participant. Child reference creation, restrict parent-delete checks, and
routed set-null/cascade parent deletes also stage an internal FK conflict intent
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
topology still fails closed before prepare. Without FK-ref owner topology,
hosted
primary-key `set_null`
falls back to broad child-range fanout: every matching child range joins the
2PC and the child participant plans the nullable-column rewrite from its local
reverse-reference rows. Hosted primary-key `cascade` does not use that fallback;
it requires routed owner topology so the coordinator can send exact child-key
delete actions. The
transaction prepare contract has explicit FK-ref mutation, child-key set-null,
and child-key cascade surfaces so a routed FK-ref owner participant can durably
write/delete reverse-reference rows without pretending they are user documents,
and an exact child participant can rewrite or delete the child row without
scanning unrelated child ranges.
The metadata and API catalog layers expose FK-ref owner-range records and a
resolver for `(child_table, constraint, parent_table, parent_key) -> owner
groups`, and the metadata control loop treats those owner ranges as first-class
reconciled topology. Broader SQL constraint support remains future work.

## Goals

- Enforce referential integrity for relational tables without introducing a
  second authoritative store.
- Reuse the existing transaction/2PC machinery for cross-shard and cross-table
  writes.
- Keep the first production version narrow enough to be exact and durable.
- Leave graph indexes and join planning as query/relationship tools, not as the
  source of truth for integrity constraints.

## Non-Goals For The First Cut

- General SQL constraint coverage.
- References to arbitrary non-unique parent columns.
- Composite row identity.
- Deferrable constraints.
- Legacy compatibility with alternate relational encodings.

## Public Contract

The supported shape is `on_delete: "restrict"`, bounded local nullable-column
`on_delete: "set_null"`, or bounded local `on_delete: "cascade"` foreign keys
to either a parent table's `_id` or, for same-table local constraints, a
declared unique parent column tuple.

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
  key / relational primary key.
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
- referenced parent tables must be relational tables.
- `on_update` is not supported; changing a referenced unique parent tuple is
  restricted while live child references exist.
- `on_delete` supports `restrict`, `set_null`, and `cascade`. `set_null`
  requires every child FK column to be nullable and rewrites affected child rows
  through the relational participant during parent delete with bounded local
  fanout. Local `cascade` deletes affected child rows through the same
  participant with bounded depth and fanout. The local store can also explain a
  parent delete through the same participant path without mutating rows, so
  operators can see whether a delete would be allowed and how many set-null or
  cascade child rows it would plan.
- `timing` defaults to `immediate`. `deferred` is accepted for local relational
  transactions and validates parent existence against the participant's final
  staged state at commit. Distributed/routed FK parent checks are immediate-only
  today; writes that would need externalized parent checks for a deferred
  constraint fail closed until distributed transaction-local constraint sets
  exist.
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
    timing: ForeignKeyTiming,
    validation_state: ForeignKeyValidationState,
};

pub const ForeignKeyAction = enum {
    restrict,
    set_null,
    cascade,
};

pub const ForeignKeyTiming = enum {
    immediate,
    deferred,
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
- each child-column set has at most one FK definition.

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

The current hosted parent-delete planner is conservative: for primary-key
`restrict` FKs it registers every range of each matching child table, then each
child participant scans its local reverse-reference rows for the deleted parent
key. That is correct, but it is not the desired distributed shape. The desired
shape is a parent-key-routed FK reverse-reference index with catalog-visible
ownership, similar in spirit to distributed SQL backing indexes for constraints:
the system pays synchronous index write amplification so FK checks become
targeted key/range lookups instead of broad child-table fanout.

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
   `cascade` use the same prefix to discover child rows, then expand the
   transaction to the affected child row participants before prepare.

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
- migration-free adoption for this new feature set by making the routed keyspace
  the only hosted distributed FK-ref layout.

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
Primary-key `restrict` parent deletes use exact FK-ref owner ranges when
configured; if no owner topology exists, they fall back to registering all
matching child table ranges.

The catalog-facing owner range record, resolver, first-class raft transitions,
durable projection, metadata status count, reconciliation-plan convergence, and
placement planning for FK-ref owner groups exist, so a coordinator can resolve
configured owner ranges for a parent key and the metadata leader can repair drift
between desired and committed FK-ref owner topology. The storage transaction
layer accepts explicit FK-ref write/delete mutations in the prepare request.
Those mutations are validated against the runtime FK catalog, materialize as
exact internal reverse-reference keys on commit, delete those keys on commit
when requested, and are considered by parent-delete validation. Remaining hosted
coordinator work is resumable large-operation execution for wide
set-null/cascade fanout, online validation/rebuild jobs, and deferrable
constraint planning.

## Transaction And 2PC Semantics

Existing 2PC gives the atomic commit substrate, but it does not by itself define
foreign-key semantics. FK support still needs constraint-aware participants and
conflict protection.

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
- hosted restrict parent deletes register the exact FK-ref owner range for the
  deleted parent key when routed ownership is configured. Without owner
  topology, they conservatively register every range of each child table whose
  schema declares a matching primary-key FK to the parent table;
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
  encoded-tuple parent-delete check to the owner. `set_null` and `cascade` scan
  the owner prefix through the bounded child listing contract, send FK-ref
  deletes to the owner, and send exact child-key actions to each child-row
  participant. Missing unique-owner or FK-ref owner topology still fails before
  prepare.
  Primary-key `set_null` and bounded primary-key `cascade` use exact routed
  owner discovery when FK-ref owner topology exists: the coordinator scans the
  bounded owner prefix, registers only the discovered child-row participants,
  sends FK-ref deletes to the owner participant, and sends direct child-key
  set-null or cascade actions to the child participants. Without owner topology,
  `set_null` falls back to broad child-range fanout and child participants plan
  the local nullable-column rewrites during prepare; cascade fails closed
  because broad fanout is not a deterministic cascade graph;
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
listing. The public admin endpoint
`POST /tables/{table}/foreign-key-integrity` exposes the same operation for
local/bound, provisioned, and hosted tables with `validate`, `dry_run`,
`repair`, `list`, `plan`, and `progress` actions plus optional `constraint_name`,
document-key range bounds, and a violation-detail limit. When `constraint_name`
is present, every local, provisioned, hosted, and internal group execution path
validates the name against the runtime FK catalog and scans only that
constraint's reverse-reference metadata; unknown names fail instead of returning
a misleading empty report.
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

Group DB FK integrity job records are keyed by `job_id`. A record stores table,
action, worker identity, optional constraint scope, document-key range, lease
duration, per-pass work-unit limit, status, attempt count, created/updated
timestamps, completion/validity, the latest aggregate report, and a bounded JSON
sample of violation diagnostics with sample count and truncation metadata. Each
completed pass merges distinct new samples into the existing job sample up to
the bounded limit; completion without fresh samples preserves any previously
recorded diagnostic sample, so a later terminal pass does not erase the useful
failure context gathered by an earlier bounded pass. These records are the
durable intent/resume substrate for hosted job controllers; they survive DB
reopen independently from the in-memory background job lane.

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
prepared batch. Hosted/provisioned routing uses FK-ref owner ranges when they
are configured: the planner reads the parent row, resolves each applicable
owner range, scans the bounded owner prefix, and fails closed rather than
returning a partial plan when the owner scan is incomplete. When no routed owner
topology applies, explain falls back to the existing local/group path.

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
or constraint.

Remaining hosted online-validation work is no longer range discovery, status
correlation, group-local claim persistence, group-local claim-and-run
execution/routing, bounded claimable-unit scheduler passes, stable job identity,
schema FK discovery, or catalog-wide table scanning; those are exposed by
`plan`, `work_units`, `work_statuses`, `work_claims`, durable claim rows,
durable per-range progress rows, the DB claim-and-run primitive, the internal
group claimed work-unit route, the public worker-pass request fields,
schema-controller mode, the source-level schema-controller maintenance pass,
stable public/admin `job_id` derivation, and bounded job-record writes for
local, provisioned, local-hosted, and remote-hosted claimed work. Metadata-owned
background rounds now provide the first automatic hosted/provisioned scheduling
shape: the metadata leader runs one bounded schema-controller maintenance pass
per round, with explicit service config for enablement, worker id, lease
duration, maximum tables per round, maximum durable jobs per round, maximum
work units per pass, and violation limit. The default worker id is
`metadata-fk-schema-controller`; the default round advances at most four tables,
sixteen durable jobs, and one work unit per pass. Each non-empty round logs
scanned, pending, executed, job-scanned, job-executed, claim, terminal-valid,
terminal-invalid, completion, and validity counters, exposes cumulative and
last-round counters
in metadata status, accumulates cumulative and last-round FK integrity report
counters for the work it actually ran, and promotes terminal valid adoption jobs
through the metadata table-update path.
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
counters, and truncation flag without writing reserved states into runtime
schema JSON. A later terminal valid promotion clears the stale invalid entry for
that constraint. Durable validate/dry-run/repair jobs are also resumed by the
same metadata-owned background pass without relying on request polling.

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
- action policy (`restrict`, `set_null`, `cascade`);
- timing policy (`immediate`, and local `deferred` commit-time validation;
  distributed/routed deferred validation remains reserved for the distributed
  planner);
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
constraint name with different semantics are rejected. Distributed
validation-state jobs still need hosted resumable cursors before online
migrations can span tables, ranges, and groups.

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

Generalize the current local-store and broad-fanout hosted primary-key FK to
real cross-table and cross-shard enforcement with exact parent-key routing.

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
  the request body, and range selection by parent-key span. Production still
  needs higher-level admin/public endpoints, controllers that drive data
  movement before finishing lifecycle transitions, and hosted validation of
  rebuilt owner ranges;
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
  for primary-key `restrict` FKs when owner ranges are configured, with broad
  child-table fanout retained only as the no-owner-topology fallback. Owner
  scans use a deterministic child-table/child-key page cursor in the internal
  storage/API contract; interactive parent deletes drain those cursor pages
  before prepare and fail closed if the owner topology or cursor contract is
  invalid;
- use the FK-ref owner participant as the conflict point between child reference
  creation and parent delete;
- support split/merge/rebuild of FK-ref ranges using topology epochs. The
  table-manager protocol, raft transition commands, metadata service helpers,
  simulation helpers, table workflow methods, internal metadata HTTP routes, and
  HTTP client methods for those state transitions exist. Public/admin
  orchestration endpoints, controllers, and data-movement observation still need
  to drive them instead of issuing raw upsert/remove metadata changes directly;
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
- bounded FK job violation diagnostics: completed job records merge distinct
  reported violations into a bounded JSON sample, sample count, and truncation
  flag, while completion without fresh samples preserves any prior diagnostic
  sample for the same durable job;
- DB owner validation/dry-run/repair for a single parent-key prefix and for a
  routed FK-ref owner range parent-key span. The primitive scans owner rows,
  detects rows whose child row is missing or now references a different parent,
  and repairs by deleting those stale owner rows without mutating child rows;
- phase-aware worker planning and claimed execution for provisioned/hosted FK
  integrity jobs. Worker plans append `owner_range` units for routable FK-ref
  owner ranges that still match enforced immediate schema FKs, internal/remote
  claimed-unit requests carry the phase, and progress records are keyed by
  `(phase, mode, constraint, range)` so owner-range scans cannot overwrite
  child-range checkpoints for the same constraint/span.

Remaining work:

- distributed large-operation execution and recovery for high-fanout
  `set_null` / `cascade` parent deletes. The validation/repair controller can
  already discover and advance durable integrity jobs without a public request
  loop, and routed parent-delete planning now consumes FK-ref owner pages by
  cursor; high-fanout data-changing operations still need a durable operation
  record, persisted resume cursor, idempotent participant execution across
  bounded rounds, and operator-visible recovery diagnostics.

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
  `restrict`, `set_null`, or bounded `cascade` through the FK-ref owner range.

Remaining work:

- make parent-update and child-reference races use distributed range/participant
  conflict protection instead of the current local-store reverse-reference scan.

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
  provisioned explain use exact FK-ref owner discovery when owner topology
  exists, drain the owner prefix through child-table/child-key cursor pages,
  report the first-hop child set-null count, and fail closed only if owner
  topology or page cursors are invalid; otherwise they route to the target
  document key's owning group.
- hosted distributed parent deletes for primary-key and declared unique-target
  `set_null` use exact routed FK-ref owner discovery when owner topology exists.
  The coordinator scans the owner prefix for the deleted parent key or encoded
  unique tuple using the internal page cursor until the scan is complete,
  registers only the discovered child-row participants, sends FK-ref delete
  mutations to the owner
  participant, and sends direct child-key set-null actions to each child
  participant. Each child participant validates the FK metadata, rewrites the
  nullable FK columns for that exact child key, and updates derived indexes and
  local reverse refs through the normal relational write participant.
  Interactive execution still stages all discovered child actions in one 2PC;
  durable chunked execution is the remaining large-operation step for very hot
  parent keys.
- without FK-ref owner topology, hosted distributed parent deletes for
  primary-key `set_null` fall back to broad child-range fanout. Each child
  participant receives a parent-delete instruction, scans its local
  reverse-reference rows for the deleted parent key, stages child row rewrites
  that remove the FK columns, and removes the local FK-ref rows in the same 2PC
  prepare. This is bounded by the same local set-null limit on each participant.

Remaining work:

- add hosted durable execution on top of the owner-scan cursor for fanout that
  should not be planned in one foreground 2PC;
- add explicit large-operation mode for distributed fanout, retry, recovery,
  and progress diagnostics.

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
  provisioned explain use exact FK-ref owner discovery when owner topology
  exists, drain the owner prefix through child-table/child-key cursor pages,
  report the first-hop cascade child delete count, and fail closed only if
  owner topology or page cursors are invalid; otherwise they route to the target
  document key's owning group.
- hosted distributed parent deletes for primary-key and declared unique-target
  cascade use exact routed FK-ref owner discovery when owner topology exists.
  The coordinator scans the bounded owner prefix for the deleted parent key or
  encoded unique tuple using the internal page cursor until the scan is
  complete, registers only the discovered child-row participants, sends FK-ref
  delete mutations to the owner participant, and sends direct child-key cascade
  actions to each child participant. Each child participant validates the FK
  metadata, verifies the child still references the parent identity, and deletes
  the exact child row through the normal relational write participant. Local
  recursive cascade planning still runs on that participant, so same-range
  descendants are deleted with the same row/index/reverse-ref cleanup.
- hosted distributed parent deletes for primary-key and declared unique-target
  cascade fail closed when FK-ref owner topology is absent; broad child-range
  fanout is not used for cascade because it is not a deterministic dependency
  graph.

Remaining work:

- build a distributed recursive cascade dependency graph from the constraint
  catalog for child rows whose own cascades cross table/range boundaries;
- define cross-table allowed cyclic cases beyond local duplicate suppression;
- produce deterministic cross-participant delete order;
- register all recursively touched tables/ranges as 2PC participants before the
  first prepare;
- support explicit large-operation execution on top of owner-scan cursors for
  cascades that exceed the local interactive limit;
- make recovery resume or abort distributed cascade transactions cleanly.

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
- distributed/routed parent-check externalization remains immediate-only, and
  deferred FK writes fail closed rather than skipping local validation.

Work:

- add transaction-local constraint sets;
- record pending child references, parent creates, parent deletes, unique
  changes, cascades, and set-null updates;
- validate the final transaction state at prepare/commit;
- require all validation participants to be locked or registered before prepare.

Deferrable constraints should come last because they depend on the distributed
participant model, unique indexes, and cascade/set-null planning.

## Recommended Order

Ship the remaining work in this order:

1. Distributed/table-resolved primary-key `restrict`.
2. Repair and validation tooling.
3. Distributed/online hardening for FK add/drop validation.
4. Distributed/online hardening for unique constraints.
5. Cross-table/table-resolved FK targets for unique parent columns and composite
   unique tuples.
6. Composite row identity, if Antfly adds composite primary keys.
7. Distributed/large-operation hardening for `on_delete: set_null`, including
   paginated owner scans and resumable child-action execution.
8. Distributed recursive graph and large-operation hardening for
   `on_delete: cascade`.
9. Distributed deferrable constraint sets.

This keeps the integrity substrate sound before adding features that multiply
write-planning complexity. Distributed cascades and distributed deferrable
constraint sets should come late because they require full transaction planning
across all affected rows, not just validation.

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
- repair rebuilds reverse-reference rows from relational child rows;
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
- Remaining operational docs: explicit large-operation execution/recovery
  diagnostics for high-fanout set-null/cascade.
