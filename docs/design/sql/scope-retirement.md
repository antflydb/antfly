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
requires metadata capability 170. Registrations identify proposed child work;
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
Drain and child-ready advances retain independent exact outcomes. Recovery
rejoins every completed child to its sealed native plan and signed owner
receipts; a phase or registration is insufficient. Final catalog publication
remains guarded until the common survivor cohort owns that atomic cut.

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

Callable support references join that same footprint. A bounded directory walk
captures external aggregates, operators and casts before any reservation. Their
supporting functions are held by immutable catalog identity; new references to a
retiring function are rejected by the shared, no-write catalog preflight. New
unrelated users of a held support function remain admissible while its definition
is frozen. Typed previews and committed writers use that same check, so a known
conflict cannot look like successful preparation.

Callable-directory reads admit retained bytes before decoding and keep scratch
for only one resource at a time. Snapshot and standby-checkpoint recovery prove
reverse closure from authoritative callable records, independently of rebuilt
derived indexes. Table-only scope jobs do not parse every unrelated callable.
These semantics require metadata capability 170. They supply dependency custody,
not permission to execute or acknowledge physical retirement.

Named-type capture now closes the whole selected domain/enum set through the
native type/table directories. It admits direct column/expression users and the
incoming FK borrowers of their generated keys. The shared schema planner walks
one reverse generated-column graph for the whole set, removes dependent
constraints/indexes/defaults and gives each survivor exactly one new epoch even
when table, sequence and type withdrawals overlap. JSON literal payloads stay
opaque and exact numeric defaults retain their source precision.

Views of removed columns are selected by immutable column ID; views of retained
columns remain present. Typed materialized backings enter retirement through
their view owner, producing one backing/deletion child rather than a competing
schema rewrite. Forward snapshot validation recomputes complete survivor
postimages rather than trusting the captured successor strings. The streamed
reverse pass proves every type borrower and every removed-column view has the
same root custody. Its column cache retains only one bounded scope at a time,
with independent per-table decoding scratch. Capture also fences new uses of
selected types before reservation can lose its admitted footprint. This is
metadata capability 170; it does not replace the required physical row rewrite.

The authenticated materialized-view campaign also qualifies the shared query
population lane. Preparation uses an exact-body administrative read grant on
both local and remote metadata paths, matching the service's existing policy.
Service authentication or a read-only ticket does not substitute for that grant.
Asynchronous population is observed through its durable job ID and idempotency
key; a materialized view's logical object ID is not its hidden backing table ID.
No admitted DDL is replayed to wait for publication.

Reservation does not certify connection drainage, physical child ACKs or final
catalog withdrawal. A separate accepting-writer drain checks durable native
source and prepared-transaction counts. Fenced sources remain counted until
existing decision/lock cleanup acknowledges their retirement. Native source
registration and new commands honor the database fence; exact completion and
cleanup replays remain admissible. FORCE carries the retained root creator,
not the privileged worker identity, into its native termination check. Its
indexed source cursor commits with the fence and exact outcome. Snapshot and
standby verification prove both directions of the source directory.

Query-produced tables and materialized views use the existing metadata-owned
replicated restore job lane. The requesting data node owns one producer
evaluation; it does not create a node-local substitute for that durable job.
Admission returns the persisted staging-plan commitment, which the producer
checks against its exact prepared definition before evaluating source rows.
This administrative proof is separate from a receiving replica's placement
grant; a producer need not be a replica of the new table.

The private job port binds admission, compact status reads and cancellation to
the exact job ID, idempotency namespace/key, request fingerprint and plan
commitment. Cancellation checks the persisted plan again. Admission and cancel
use single-attempt transport with explicit not-proposed witnesses; dropped,
malformed or substituted accepted replies retain an admission-unknown receipt
and never trigger source reevaluation. Status polls load the compact job record
without decoding its staging plan. Metadata leadership controls dispatch through
the existing restore workers. This port requires metadata capability 169; the
native API operation boundary is version 25.

The mounted population campaign executes all eleven unchanged permanent
population/materialized-view originals, exercises lost and malformed admission
replies and plan substitution, and checks cancellation plus publication after
complete owner restart. A producer-only forbidden staging-read fixture proves
that administrative admission does not borrow replica authorization. The scope
campaign separately checks mixed type/FK/default/view capture and complete
survivor postimages, without claiming physical scope completion.

## Native physical preparation

Registered children enter the existing table-retirement worker through compact
root-bound admission. Indexed source/range reads and registered definition
fingerprints avoid decoding the full ancestor body for each child. Each child
owns its source aliases, sealed plan and signed receipts; source and outgoing
parent definitions remain under the ancestor's exact leases. Shared parents
therefore do not acquire competing child lifecycle locks.

A parent that is also a registered deletion child uses its own mandatory source
barrier at the root boundary. The immutable physical plan binds that parent's
registration, definition and range cut. It neither fetches unused parent
fences nor issues another parent's transition into a namespace already being
retired. Metadata refuses such a
transition grant. Surviving parents retain the complete stage, activation,
acknowledgement and reference-coverage protocol. This avoids internal-FK RPCs
and conflicting source/parent fences without reopening any source or weakening
surviving-parent coverage.

A child stops at `physical_ready`. It cannot publish its own catalog deletion,
release root custody or run an independent survivor rewrite. Root progress
requires the exact sealed plan and complete signed receipt set, with one durable
completion link per ordinal. Native snapshot and standby validation rejoin these
proofs and reject missing, substituted or orphaned custody. The mounted campaign
covers a lost source-fence reply, cold recovery, a shared surviving FK parent,
self/cyclic FKs and a hidden materialized backing, followed by snapshot installation
and standby export of the completed physical-preparation cut.

## Database publication

The leader-fenced ancestor supervisor advances compact durable work records.
FORCE traverses the indexed native connection directory and delegates terminated
owner drainage to the existing lock/transaction acknowledgement supervisor.
Idle public HTTP connections enroll before their connection IDs are returned.
Native admission checks termination privileges before reserving an irreversible
root and refuses prepared transactions even with FORCE.

Metadata API issuers run this leader-owned quantum and native lock ACK cleanup
alongside their local session upkeep. These do not depend on the data-owner
repair loop; a failed session quantum cannot starve either supervisor. Scoped
source-directory reads retain their signed administrative authority across both
the HTTP classifier and the private compiled storage projection.

After every child supplies its signed physical-ready receipt, a root without
rewritten survivor schemas publishes in one native writer transaction. It
removes the scoped catalog resources, native table/range records, dependency
edges and sequence ownership, and marks every child and the root published at
one catalog revision. The common relation command journal updates the live
relation directory with the same decision. No child publishes independently.

Published manifests, child registrations and terminal outcomes remain available
for exact recovery and checkpoint verification after live leases are released.
Verification proves the retired immutable IDs absent; it does not pin a
surviving dependency to an obsolete schema forever. Name reuse cannot retarget
an old receipt. Public operation-status recovery only observes this decision;
it never resubmits a name-based DROP.

Local document sources use an explicit document retirement role. They retain
their real schema identity and do not fabricate a relational integrity catalog
or schema increment. The source checks the durable document-schema digest under
the accepting writer before fencing; ordinary producer and transaction replay
debt must drain before the signed receipt is admitted. Metadata decoder floor172
and data-Raft floor33 gate this role and its structural markers.

## Remaining activation

1. Publish rewritten survivor schemas through the common staged schema machinery,
   including physical typed-row evolution for withdrawn named types. Preserve
   surviving rows and unrelated constraints; metadata-only removal is insufficient.
   Shared manifest derivation already produces the postimages, but roots needing
   these rewrites remain guarded before public admission.
2. Integrate transactional schema intent/rollback and public schema DROP with
   that survivor pipeline. Database DROP intentionally rejects explicit
   transactions, matching PostgreSQL.
3. Define bounded archival-proof cleanup without weakening exact terminal
   recovery or snapshot/standby verification. Published proof retention is
   durable, but is not yet a garbage collection policy.
4. Extend admission to external-base and other unsupported storage capabilities
   only with their own verified physical barriers. Local document and relational
   sources share the supervisor and final catalog transaction; external owners
   remain rejected before root reservation.

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
  -Dtest-filter='SQL qualified foreign key' \
  -Dtest-filter='SQL named type retirement' \
  -Dtest-filter='dependent retirement' -Doptimize=Debug -j1
zig build antfly-metadata-scope-retirement-test -Doptimize=Debug -j1
zig build antfly-standalone-scope-retirement-test -Doptimize=Debug -j1
zig build antfly-api-sql-scope-retirement-test \
  -Dtest-filter='mounted SQL scope retirement capture' \
  -Dtest-filter='system catalog binds foreign keys once and authorizes cascades by current logical names' \
  -Dtest-filter='SQL scope retirement population' \
  -Dtest-filter='sequence catalog HTTP grants bind exact bodies' \
  -Doptimize=Debug -j1
```

Explicit SQL `DEFAULT nextval(...)` on an existing/unowned sequence remains
unfinished: stored-expression preparation currently rejects the sequence effect.
The mounted dependency fixture uses the existing native `LIKE ... INCLUDING
DEFAULTS` serial borrower path. That proves capture and fencing of native bound
borrowers, without claiming general sequence-default SQL admission. Explicit
sequence defaults must bind immutable catalog references during DDL preparation,
never allocate a value during CREATE or persist a request-owned scalar callback.
