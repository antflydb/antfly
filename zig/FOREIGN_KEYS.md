# Foreign Keys

Foreign keys are relational-table constraints over the committed relational base
store. They are not derived indexes: they must be enforced by the same
transaction boundary that commits relational rows and their reverse-reference
metadata.

The implemented production contract is intentionally narrow: single-column
child references to a parent table's `_id` document key with
`on_delete: "restrict"`. The schema, runtime catalog, persisted catalog,
relational write participant, transaction-intent resolution path, reverse
reference keyspace, and OpenAPI surface all use that shape. Broader SQL
constraint support remains future work.

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
- Cascading delete/update actions.
- Deferrable constraints.
- Legacy compatibility with alternate relational encodings.

## Public Contract

The supported shape is primary-key-only foreign keys with
`on_delete: "restrict"`.

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

- `references.columns` must be exactly `["_id"]`, the existing Antfly document
  key / relational primary key.
- child columns must be declared relational columns.
- nullable child columns are allowed; `null` means no reference.
- non-null child columns follow the existing `required_fields` / `NOT NULL`
  behavior.
- referenced parent tables must be relational tables.
- `on_update` is not supported because row identity is the document key.
- `on_delete` supports only `restrict`.

The API rejects unsupported shapes during schema validation rather than
accepting constraints that silently degrade.

## Runtime Schema

The compiled runtime schema carries a normalized foreign-key catalog:

```zig
pub const ForeignKey = struct {
    name: []const u8,
    child_columns: []const []const u8,
    parent_table: []const u8,
    parent_columns: []const []const u8,
    on_delete: ForeignKeyAction,
};

pub const ForeignKeyAction = enum {
    restrict,
};
```

The runtime representation uses column paths resolved against
`relational_columns`, not raw public JSON pointers. Schema compilation must
validate that:

- every child column exists and has a scalar, comparable representation;
- the nullability of the child column is known;
- the parent target is supported by the current feature level;
- the constraint name is unique within the table;
- the same child-column set does not produce conflicting constraints.

Changing the FK catalog is a schema/storage change, not a derived-index-only
change. Until online constraint validation exists, FK additions/removals are
treated like relational column-catalog mutations and rejected on existing tables
or routed through an explicit migration path.

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

## Write Path

Foreign-key enforcement belongs in the relational write participant, not only in
the HTTP/API layer. Batch writes and committed transaction intents both enter
the same participant semantics.

On child insert/update:

1. Project and validate the relational row as today.
2. Extract the old FK values, if the row already exists.
3. Extract the new FK values from the projected row.
4. For each non-null new FK value, probe the parent relational row at the
   transaction read generation.
5. Treat a parent row written in the same transaction as visible.
6. Reject the write with `ForeignKeyViolation` if the parent is absent.
7. Update reverse FK rows for changed references in the same commit batch.

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

## Transaction And 2PC Semantics

Existing 2PC gives the atomic commit substrate, but it does not by itself define
foreign-key semantics. FK support still needs constraint-aware participants and
conflict protection.

For single-shard parent/child writes, the relational participant validates and
writes reverse rows in the local prepare/commit path.

For cross-shard or cross-table writes:

- the child participant must register the parent table/range participant needed
  for parent existence validation;
- parent delete must register or consult child-reference participants for the
  relevant reverse-reference key range;
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
The implementation can provide that with one of:

- write-intent rows under the reverse-reference prefix during child prepare;
- range conflict checks for parent-delete prepare;
- a parent-row version predicate that child prepare pins until commit.

The long-term shape should be explicit reverse-reference intent/range conflict
handling, because it also supports future cascade planning and repair tooling.

## Rebuild And Repair

Reverse FK rows are deterministic from child rows, so the system should include
a rebuild path:

1. Scan child relational rows for one constraint.
2. Extract each non-null FK value.
3. Verify the parent exists.
4. Rewrite the reverse FK row.
5. Report orphaned child rows instead of silently dropping them.

This is not implemented yet. It should be exposed as an offline/admin repair
first. Online schema adoption
can later use the same scanner to validate a newly added FK before making it
enforced.

Diagnostics should report:

- FK catalog entries and enforcement state;
- reverse-reference row counts per constraint;
- orphan count found during validation/repair;
- parent-delete rejects;
- child-write rejects;
- repair/rebuild progress.

## Cascades And Later Features

After primary-key-only `restrict` is production-ready, add features in this
order:

1. **Composite child keys to parent primary key encodings.**
   Useful only if Antfly later supports composite row identity.
2. **Unique constraints.**
   Required before FKs can reference non-primary-key parent columns.
3. **FK references to unique parent columns.**
   The parent target must be backed by a committed unique index, not a derived
   search or algebraic index.
4. **`on_delete: set_null`.**
   Requires update planning for all children in the reverse-reference range and
   bounded safety limits.
5. **`on_delete: cascade`.**
   Requires deterministic cascade ordering, cycle detection, depth limits, and
   failure accounting.
6. **Deferrable constraints.**
   Requires transaction-local constraint sets and validation at commit instead
   of per-write enforcement.

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
- crash/reopen preserves FK catalog and reverse-reference rows;
- repair rebuilds reverse-reference rows from relational child rows;
- cross-shard 2PC failure/recovery does not leave dangling references.

## Documentation Status

- `RELATIONAL.md`: documents FK support in the supported constraints section and
  links to this design.
- `SCHEMA.md`: documents public schema syntax, validation rules, and migration
  limits.
- API/OpenAPI schema: exposes `foreign_keys` on `TableSchema`.
- Remaining operational docs: describe repair/rebuild and parent-delete
  rejection diagnostics once those admin surfaces exist.
