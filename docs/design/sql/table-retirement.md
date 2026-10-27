# Whole-table SQL retirement

Ordinary `DROP TABLE` must remove a constrained table without first publishing a
weaker schema or deleting each row through the mutation API. PostgreSQL is the
semantic oracle. `scripts/check_sql_drop_postgres.py` independently exercises
RESTRICT, child-only ownership, self/external references, active RLS, name reuse,
and the original `sql-0157`/`sql-0159` statements against an isolated PostgreSQL
18+ server. This work strengthens those ordinary DROP contracts; it does not
close or relabel `DROP TABLE ... CASCADE` (`sql-0158`).

## One existing publication protocol

The SQL dispatcher pins the exact namespace relation, immutable table ID,
schema digest and catalog incarnation before ordinary DROP admission. Only a
definite, unproposed `ConstraintRetirementRequired` result may enter coordinated
retirement. The subsequent schema/snapshot capture must match that pinned owner;
a same-name replacement cannot become the deletion target.

The existing FK generation publication record carries a private `sql_drop`
intent. Its prospective schema removes outgoing FKs only and is never published.
It retains all other declarations, indexes and read layouts. A source with no
outgoing FK still receives a private successor epoch for its native fence;
ordinary idempotent schema updates do not change epochs.

| Phase | Durable requirement |
| --- | --- |
| Admit | Exact catalog owner, source/parent descriptors and complete memberships; no incoming FK, dependent view, extension ownership, backup/restore conflict or policy publication in progress |
| Fence source | Every native source owner freezes and drains admitted transactions under the exact plan fence |
| Stage parents | Every outgoing FK has a retire-only parent generation transition |
| Activate parents | Every parent installs its generation tombstones; cancellation can no longer reopen the old contract |
| Acknowledge parents | Every parent returns its replicated activation receipt |
| Complete coverage | Every surviving parent independently acknowledges reference-coverage completion |
| Publish deletion | One metadata transaction deletes the exact table, catalog binding, range membership, column/sequence ownership and policies, releases lifecycle locks, and records the terminal publication |

A self-parent shares the source's dual-role fence. Its own coverage barrier
remains frozen and disappears with that physical namespace; it is not reopened
just to delete the namespace. Every external parent still requires the second
coverage ACK. Mixed self/external plans distinguish these owner sets explicitly.
Ordinary FK ALTER retains its existing successor installation and subsequent
parent coverage protocol.

The coordinator advances from durable receipts. Cold recovery does not replay
SQL, recapture names or infer completion from an empty lookup. Before any parent
activation, cancellation uses the existing durable source/parent cancel path.
After activation, interruption leaves an admitted job that must finish.

## Authorization and row policies

DROP requires current administration of the pinned child table. Retiring that
child's outgoing references does not require parent administration; native plan
validation proves that all transitions belong to the exact child preimage and
carry no successor generation. Ordinary FK ALTER retains parent authorization.

A stable source or parent policy does not require disabling RLS. The private
`sql_drop_control` language allows only source fencing/cancellation and
retire-only parent controls. Native admission rejects schema installation,
transfers, successor grants and every mixed batch effect. Only sync level and
the materialized ordered timestamp may accompany the control. Application still
uses the normal native transaction, source-pin and topology checks; user writes
and unrelated topology changes retain policy enforcement.

Surviving-parent mutations carry signed integrity-read authority through both
primary reads and batched observations. Tokens and database scope remain in
headers. The receiver requires an explicit service credential even in legacy
internal mode, and native readers verify the signed owner and purpose.

Metadata topology capability 72 and data-Raft protocol 27 fence the new control
and lifecycle-lock kind. Private storage engine descriptor/method symbols
advance together to version 37. Public storage-owner ABI and OpenAPI contracts
are unchanged. The marker is omitted from ordinary legacy control bytes.

## Publication and operational behavior

SQL returns `DROP TABLE` only for terminal completion. A bounded request may
return `DDL PENDING` with the existing FK publication token and immutable table
ID. An ambiguous begin retains an `admission_unknown` receipt. Neither response
authorizes replaying the DDL. Parent completion precedes logical deletion, so a
completed ordinary DROP leaves surviving parents usable.

While the durable deletion intent owns the table, native catalog admission
rejects table and ancestor namespace/database renames and new view or extension
dependencies. Ancestor checks stream active lifecycle locks and compact catalog
bindings, rather than decoding complete publication plans. Schema, policy and
other lifecycle changes use the shared table lock. The final native transaction
rechecks dependencies and owner identity before deleting anything.

Late publication commands which lose their plan/revision CAS are native
metadata no-ops before any writes. Later validation and storage failures still
abort apply. A successful response requires the exact durable owner receipt;
a competing command advancing the same revision is insufficient. The native
campaign deliberately repeats intermediate commands and verifies that both
fault profiles still have seven publication revisions.

A bound table whose advertised native snapshot is temporarily absent returns
statement-read unavailability instead of falling back to stateless pages or
claiming unsupported SQL. Cold-owner tests independently wait for serving
schema installation. Named scopes share the existing bounded nonadmission
retry client; unknown and committed outcomes are never replayed.

Work depends on source owners and FK parent edges, not the number of user rows.
The native campaign checks the same seven durable revisions for one and 128
child rows with one source owner and one external parent: begin, source fence,
parent stage, activation, first ACK, coverage ACK and final deletion. This is a
coordinator work-count assertion, not a throughput or latency benchmark.

Incoming-FK admission currently streams active table schemas with bounded
per-table memory and early exit. A durable reverse dependency directory remains
a separate catalog optimization. Terminal publication records retain their full
plan for exact receipt recovery; bounded terminal receipt retention/compaction
is not claimed here.

## Qualification

The strict Debug gate is `antfly-api-sql-drop-retirement-test`. Its mounted
campaign uses real metadata, session and data owners and independent cold
recovery. It checks external, self and mixed references, zero-parent constraints,
read-only denial, child-only authority, RLS source cleanup, an RLS-protected
surviving parent, dependency/name races, safe name reuse, lost early parent ACK
replies, and lost final catalog publication replies. It requires zero skips.

Neighboring gates are `antfly-api-hosted-fk-drop-test`,
`antfly-api-sql-fk-schema-lifecycle-test` and
`antfly-api-sql-tablespace-lifecycle-test`. Passing results must be recorded from
the completed build graph; compilation, an admitted receipt or one successful
subcase is not end-to-end qualification. The authoritative parity ledger stays
unchanged until an original contract has sufficient executable evidence.

Local qualification on 2026-10-26 used pinned Zig 0.17 Debug on macOS arm64.
The combined retirement, hosted FK, FK schema, names/tablespace and runtime-error
gates passed all 108 build steps and 23 tests with zero skips or leaks. The
11-test retirement gate then passed two additional consecutive cached runs,
including deliberate duplicate metadata commands and both cold-recovery faults.
The merged-tree SQL suite passed 1,670 tests with four existing opt-in benchmark
skips and no failures or leaks. The PostgreSQL oracle passed all 50 assertions;
the Go SDK suite, inventory checker tests, license and embedded-boundary checks
also passed. The original parity disposition counts are unchanged.
