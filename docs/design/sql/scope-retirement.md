# Schema and database retirement

Status: implementation in progress. `sql-0105`, `sql-0108` and `sql-0109`
remain unresolved. Capture and reservation tests do not establish public DROP
completion or physical deletion authority.

## Required behavior

[PostgreSQL DROP SCHEMA](https://www.postgresql.org/docs/18/sql-dropschema.html)
requires CASCADE to remove contained objects and their dependents, including
objects outside the selected schema. Removing a referenced table drops the
external foreign-key declaration, not the surviving child's rows. Removing a
sequence drops dependent defaults while preserving borrower data. Schema
retirement must participate in transaction rollback.

[PostgreSQL DROP DATABASE](https://www.postgresql.org/docs/18/sql-dropdatabase.html)
requires another current database and rejects explicit transaction blocks.
Ordinary DROP rejects active connections. FORCE may terminate eligible
connections, but prepared transactions, active logical replication slots and
subscriptions still prevent completion. An ancestor grant must not become an
unrestricted grant to terminate another privileged backend.

The independent `scripts/check_sql_scope_retirement_postgres.py` oracle executes
all three originals unchanged. It checks populated tables, transitive external
views, surviving FK/default borrowers, overloads, rollback, live backends and
prepared-transaction custody. It supplies semantic requirements, not native
completion evidence.

## Implemented preparation

`system_catalog/scope_retirement.zig` captures immutable database/namespace
identities, metadata-group identity, every scoped resource (including overloads), table-definition
fingerprints, owned immutable definition images and indexed range fingerprints
at one native metadata read cut. Definition images survive sibling CASCADE
rewrites; a hash alone cannot reconstruct the admitted predecessor schema.
Global roles, tablespaces and query sources stay outside ancestor membership.
The ordinary catalog admission and retirement capture share one parent-scope
policy, so new catalog kinds cannot silently escape ancestor membership.
Parent/child and table/range indexes bound work to the selected scope. Strict
member/byte admission rejects the next row before decoding it; a truncated list
cannot stand for complete membership. Fingerprinting streams through a bounded
buffer without building another complete encoded manifest.

The private owner-sensitive read crosses the ordinary authenticated metadata
service and opaque storage-owner ABI. Source reads do not reserve a scope,
terminate a connection or acknowledge physical completion. Mounted qualification
uses real authenticated SQL setup and complete metadata/session/data restart.

`metadata/scope_retirement_ledger.zig` implements transaction-local reservation
custody. It stores immutable membership once, compact progress separately, an
ordered work reference and exact leases on selected identities. Namespace jobs
can coexist; an intersecting database job conflicts. Exact replay observes its
original result before vacancy checks and cannot reacquire released leases.
Admission rejects an occupied queue slot if its counter is inconsistent.
Root admission registers every physical deletion child in that same native
transaction. A linear merge of the selected tables and retiring materialized
backings assigns stable ordinals, root-derived child IDs and exact definition/range
commitments. Held and rewritten tables never receive deletion children. The
cyclic work read returns the next compact registration using point reads; it does
not decode the immutable membership body on every idle/coordinator poll. This
requires metadata capability 166. Registrations identify proposed child work;
they grant no physical execution or completion authority.

Terminal progress releases leases, body, work and child registrations atomically
while keeping the compact outcome. Native tests abort every staged admission and
terminal write, including every child-registration write, and reopen the store
between progress steps. Streamed snapshot/checkpoint verification rejects missing,
substituted and orphan registrations before publication.

The private reservation command now crosses the ordinary metadata ingress,
coordinated decoder floor, opaque storage-owner ABI and Raft apply transaction.
Reservation binds its entire body to one metadata-group nonce. The accepting
writer rechecks the immutable catalog and physical-definition/range cut, command
ownership and scope vacancy. Accepted outcomes and expected CAS/conflict
rejections are retained atomically with their effects. Exact observation precedes
current source and command-liveness checks; cancellation cannot make a lost
reservation reply acquire the scope again. Unknown submission outcomes remain
unknown until their immutable query is observed.

Ordinary catalog writes check selected identities and their database/namespace
ancestors with constant-depth point reads. Moving an object checks both parents;
a batch checks its whole footprint before any resource effect. Table-definition
replacement and removal also respect captured table leases. These are metadata
admission fences, not yet a complete query, connection or physical-owner fence.
Disjoint scopes remain writable without a global query lock. Temporary table
and range decoding uses a separate scratch arena per table, so retained manifest
images do not pin all prior parsing scratch. Stale catalog cuts fail before the
scope is walked again. Idle recovery reads one compact cyclic queue item; only
an explicit manifest read loads the immutable membership body.

Lifecycle acquisition and scope reservation check each other's durable table
keys in the same native transaction. Existing FK-validation parent leases also
exclude scope admission, and new parent leases check the scope fence. Validation
admits and encodes its complete parent footprint before staging any edge, so a
conflict on a later parent cannot strand an earlier pair. New table/view definitions check selected table, view, sequence and named-type
source leases during semantic preparation. After native index initialization,
FK parent lookup uses the existing name index, rather than decoding unrelated
table definitions for every declaration. Raw bootstrap/restore writers retain
bounded discovery until initialization; an adopted catalog refuses a missing
index instead of silently scanning.

Private HTTP submission shares the one-attempt lifecycle delivery machinery.
Forwarding headers and socket timeouts use the same bounded budget accepted by
the receiver. A definite pre-proposal error remains distinct from unknown
submission. Native snapshots retain active work, manifests and exact outcomes;
a definition-only portable export rejects active scope custody.

Snapshot and native hot-standby checkpoint validation share a streamed scope
verifier. It checks each head's original admission, incarnation, decoder floor,
canonical manifest, selected catalog identities, exact members and queue. A
reverse pass rejects orphan or additional bodies, leases, work and accepted
receipts. Cancellation retains an exact per-job command link in the same native
transaction as its outcome and custody release. Each canceled head must resolve
its own exact receipt; aggregate counts cannot substitute for this proof.
Allocation high-water cannot survive a missing retained head. Only one bounded
manifest is decoded at a time; member rows do not repeatedly decode that body.
Since native physical progress commands are not admitted yet, recovery rejects
fabricated drained/published progress rather than treating it as an owner ACK.

The captured FK/default/view footprint now has three explicit participant
intents: retire, rewrite and hold. Reverse FK/default directories supply complete
borrower sets and generation fingerprints. One combined retirement selection
produces each surviving table's successor schema once, preserving unrelated
constraints/defaults and avoiding repeated epoch advances for multiple parents.
Survivor rows are never part of this metadata rewrite plan.

All selected tables and views seed one dependency traversal. Shared dependent
views are decoded once, their native forward/reverse edges are retained, and
materialized backing tables carry their actual view owner and physical cut.
Referenced owners outside the deletion set are held by immutable identity;
physical-only document sources do not gain fabricated logical catalog bindings.
The journal leases every participant with its exact intent. Definition jobs,
typed schema cohorts and new references honor those same fences. Recovery
validates survivor preimages, indexed generation fingerprints and exact view-edge
closure. Catalogs without active scopes do not acquire leases per unrelated
view edge during checkpoint validation.

Reservation does not verify connection drainage, physical child ACKs or final
catalog withdrawal. Its progress state cannot stand for those proofs. Native
workers and final publication must verify each independently before advancing.

## Remaining activation

1. Complete routine/named-type CASCADE evolution beyond the captured FK,
   sequence-default and view footprint, including external dependent columns and
   routines. Finish connection/data admission and physical-owner barriers without
   a global query lock. The current metadata footprint and fences do not supply
   deletion authority for those still-unimplemented effects.
2. Drive proposals from the child identities already registered atomically at
   admission, and join each physical child to its root custody. A
   constant-size cyclic native work feed and verified immutable manifest read
   already cross the storage-owner and private metadata boundaries. Reservation
   and cancellation retain exact outcomes under the decoder capability and
   optional originating SQL command fence; that does not authorize child work.
3. Drive existing native retirement workers under that root custody. Validate
   the predicted subtract-only schema evolution between children; a reusable
   display name cannot replace a captured member. Physical and catalog changes
   must remain unobservable as a partially dropped scope.
4. Drain eligible FORCE owners through the existing connection/transaction/lock
   protocols. Retain prepared decisions, role/credential limits and cancellation
   semantics. Do not infer successful cleanup from a missing connection cache.
5. Publish the complete ancestor withdrawal and surviving dependency changes
   under one verified native completion boundary. Integrate transactional schema
   intent, public receipts, retention/GC, snapshots, backup/restore and mixed
   decoder barriers. Qualify lost replies, cancellation, owner churn and complete
   cold recovery before changing the three original dispositions.

Raced committed definition/topology mutations are deterministic no-ops while
scope custody holds their owners. The refusal precedes definition journal and
projection effects; topology apply retains the admitted physical cut. Standalone
admission still reports the precise conflict. Stale maintenance proposals cannot
poison Raft apply, prevent replica restart, or block later disjoint entries;
I/O and corruption errors still propagate. Native coverage batches rejected
schema/range changes with a later disjoint write and verifies exact custody,
snapshot validity and resumed definition writes after cancellation.

Qualified SQL FK parents retain separate database/schema/table components,
including quoted relation identifiers, until authorized native catalog binding.
Only the resolved physical parent reaches durable schema admission. An external
relation sharing the child's name is not a self-reference. The mounted fixture
observes asynchronous CREATE receipts using their full scope and exact table ID
before issuing dependent writes; it never replays an admitted CREATE or INSERT.

## Focused Debug gates

```sh
cd zig
zig build sql-test -Dtest-filter='SQL scope retirement' \
  -Dtest-filter='SQL qualified foreign key' -Doptimize=Debug -j1
zig build antfly-metadata-scope-retirement-test -Doptimize=Debug -j1
zig build antfly-standalone-scope-retirement-test -Doptimize=Debug -j1
zig build antfly-api-sql-scope-retirement-test \
  -Dtest-filter='mounted SQL scope retirement capture' \
  -Dtest-filter='system catalog binds foreign keys once and authorizes cascades by current logical names' \
  -Doptimize=Debug -j1
```

Explicit SQL `DEFAULT nextval(...)` on an existing/unowned sequence remains
unfinished: stored-expression preparation currently rejects the sequence effect.
The mounted dependency fixture uses the existing native `LIKE ... INCLUDING
DEFAULTS` serial borrower path. That proves capture and fencing of native bound
borrowers, without claiming general sequence-default SQL admission. Explicit
sequence defaults must bind immutable catalog references during DDL preparation,
never allocate a value during CREATE or persist a request-owned scalar callback.
