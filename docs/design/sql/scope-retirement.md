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
Terminal progress releases leases, body and work rows atomically while keeping
the compact outcome. Native tests abort every staged admission and terminal
write and reopen the store between progress steps.

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

Private HTTP submission shares the one-attempt lifecycle delivery machinery.
Forwarding headers and socket timeouts use the same bounded budget accepted by
the receiver. A definite pre-proposal error remains distinct from unknown
submission. Native snapshots retain active work, manifests and exact outcomes;
a definition-only portable export rejects active scope custody.

Reservation does not verify connection drainage, physical child ACKs or final
catalog withdrawal. Its progress state cannot stand for those proofs. Native
workers and final publication must verify each independently before advancing.

## Remaining activation

1. Extend the captured scope to the complete external dependency footprint using
   the existing typed FK, sequence, view, routine and named-type directories.
   Bind root and affected-survivor fences to immutable identities. Block new
   connection/object admission and incompatible table publications without a
   global query lock.
2. Register each deterministic child before its proposal so lost replies
   recover the admitted child rather than allocating another operation. A
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

## Focused Debug gates

```sh
cd zig
zig build sql-test -Dtest-filter='SQL scope retirement' -Doptimize=Debug -j1
zig build antfly-metadata-scope-retirement-test -Doptimize=Debug -j1
zig build antfly-standalone-scope-retirement-test -Doptimize=Debug -j1
zig build antfly-api-sql-scope-retirement-test \
  -Dtest-filter='mounted SQL scope retirement capture' -Doptimize=Debug -j1
```
