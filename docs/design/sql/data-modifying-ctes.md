# Atomic statement effect graphs

Status: PostgreSQL witnesses and production integration design; native execution
is not yet activated and no original case receives credit from these witnesses.

## PostgreSQL contract

Data-modifying CTEs execute once to completion, including unused producers.
Ordinary table reads share the statement snapshot; RETURNING relations convey
new values between producers. A referenced producer requires RETURNING.
Failures roll back every row write in the statement. Sequence effects retain
PostgreSQL's separate lifetime. Nested and recursive mutations retain their
specific rejection contracts. See the [PostgreSQL WITH documentation](https://www.postgresql.org/docs/18/queries-with.html#QUERIES-WITH-MODIFYING).

The unchanged original `sql-0015` updates the same rows through its CTE and
MERGE. PostgreSQL raises `21000` and preserves both original rows. Implementing
that case does not mean executing two independent commits or accepting a last
postimage after folding duplicate writes. The witness generator additionally
covers valid disjoint targets, unused/zero-demand effects, repeated volatile
producer references, old-snapshot versus RETURNING values, multiple dependent
mutations, statement rollback, unique conflicts, MERGE producers and exact
NUMERIC/array captures. These supplemental scenarios do not add original-case
credit.

## Shared execution shape

Use a single top-level statement effect block in the immutable syntax IR.
Read-only WITH retains its existing graph and hot path. Bind every producer,
its output types, privileges and physical references before effects. Mutation
outputs become typed statement-local relation slots, never synthetic physical
tables or fabricated catalog identities. Repeated references open independent
readers over one owned capture. Caller CTE slots must not enter stored views.

Extend the existing mutation capture to include all effect producers and the
root before opening native cursors. This opens one coordinated statement cut;
no later producer may refresh its base-table snapshot. Existing SQL-session
staging remains separate from the current statement's effects, so previous
statements are visible and sibling effects communicate only through RETURNING.
The graph owns reader lifetime until every consumer finishes, then releases
native leases before physical publication.

A statement-local tuple ownership directory must distinguish prior transaction
writes from prior effects in this command. MERGE self-modification raises
`21000`; INSERT duplicate/ON CONFLICT behavior must follow the witnesses rather
than silently folding colliding mutations. Ordinary transaction postimage
folding is not this ownership decision. Normalization, generated values, domain
admission and RETURNING continue through the existing row preparation paths.

Use the existing guarded implicit SQL transaction for autocommit effect blocks
and the existing explicit session for transaction blocks. Collect/stage the
whole graph before the existing native distributed commit. Do not create a
second coordinator or publish a producer early. An error fails the statement;
rollback/savepoint recovery and unknown commit receipts retain their established
native semantics. Backends lacking the required atomic/snapshot authority must
reject before any producer executes.

Graph-wide quotas bound total mutations, capture bytes, work and spill, rather
than multiplying a per-node allowance by the number of CTEs. The statement's
parameter frame, clock, settings, regex/sequence owners and cancellation control
remain shared. Typed captures retain SQL NULL provenance and exact domains.

## Qualification required before activation

The PostgreSQL generator is `scripts/generate_sql_mutation_cte_reference.py`.
Native qualification must execute the unchanged original plus positive sibling
and dependent-producer scenarios on stored document and typed rows, including
cross-owner atomic publication, explicit transactions/savepoints, RLS and
privilege denial, parameter inference, cold owners, cancellation, allocation
faults, quota failures and lost commit replies. Repeated volatile producers
must run once, and failure must leave every table unchanged. Ordinary read CTEs,
recursion, MERGE point planning and mutation RETURNING remain regression gates.
