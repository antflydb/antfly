# SQL ownership of native indexes

SQL native index declarations use the existing `/indexes` configuration,
workers, query execution, and readiness machinery. They do not create a
parallel index service. Ordered relational indexes retain their existing
schema-bound online publication and constraint enforcement paths.

## Declaration and authority

`CREATE INDEX name ON table USING antfly_full_text(column)` and the native
`CREATE TEXT SEARCH` spelling lower to a typed full-text index intent. The
supported intent selects one text column, the standard analyzer, BM25 scoring,
and native highlight/snippet support. Unsupported methods, options, uniqueness,
and column shapes fail explicitly. Index configuration cannot smuggle artifact
sources or resource references past their existing public API validators.

The declaration binds one declared SQL text column under its table guard.
Document aliases resolve through the same immutable column layout used by SQL
reads and mutations. Admission stores the resolved physical selector and its
analyzer mapping together. Nested aliases produce the native dotted selector;
a literal dot inside an individual object key is currently unsupported and
fails before publication. Missing and non-text columns also fail before any
configuration or namespace write. Resolution is DDL work, with no new lookup
added to row writes or queries.

Native indexes created through `/indexes` remain table-local unless explicitly
declared through SQL. SQL-owned native indexes additionally occupy the shared
SQL relation namespace. A compact, sorted table-record descriptor identifies
the selected names; immutable native configuration incarnation headers supply
their generation fences. The owner includes the immutable physical table ID,
schema identity, and native incarnation. Schema changes and index replacement
invalidate stale owner guards without introducing a global revision CAS.

The same authoritative metadata transaction admits the namespace, validates
current table and extension ownership, applies policy/transition guards, changes
the ordinary native configuration and name descriptor, and records its outcome.
Native API replacement retains SQL ownership with a new generation; native API
deletion withdraws the SQL name in that same table-definition CAS.

## Completion and readiness

A committed catalog receipt proves declaration or withdrawal, not completion of
asynchronous native index backfill. Queryability and build progress use the
existing index status and named `full_text_index` query contracts.

Native SQL index mutations use the existing bounded shared catalog outcome
journal. A private request preassigns the physical incarnation and admission
nonce. Its digest binds the complete operation, immutable owning table,
logical authorization target, metadata incarnation, and index name. Public
recovery tokens contain the bound observation, never index configuration or
authority. Recovery checks live table administration permission and observes
the exact retained outcome. Dropping/recreating a name cannot substitute a
later operation's result. Expired or unavailable evidence remains unknown;
recovery never reexecutes a mutation. Native indexes do not fabricate a numeric
SQL resource OID merely to populate an optional receipt identity.

## Backup, restore, and restart

The ordinary metadata table codec carries the SQL descriptor. Namespace source
fingerprints include selected names and native incarnation headers, excluding
unrelated native configuration and presentation state. FK, online index, and
restore publication cuts retain those names during reservations.

Portable table manifests also carry the descriptor. Shared manifest validation,
cloning, private restore creation, table reconstruction, definition comparison,
and backup fencing preserve it for both document and relational tables. The
manifest validates every selected name against a real stored native index and
its nonzero incarnation. Names are not inferred from every native index during
restore. Restoration into an occupied SQL namespace must use the same relation
reservation and collision rules as normal publication.

Shadow rebuild candidates inherit the serving owner's captured storage mode,
range and schema registry. Each typed row decodes with its immutable layout,
so historical AROW rows participate in the ordinary native rebuild rather than
being skipped as document keys. Explicit index analyzers remain authoritative
over inherited schema/observed mappings. Observed-field publication replaces
field mappings while retaining custom analyzer objects used by prepared work.

The combined unreleased decoder capability 135 protects these private records
and operations. Capability 136 remains reserved for independently qualified
connection transaction receivers.

## Qualification

Originals `sql-0696` and `sql-0697` are qualified through their unchanged
Antfly-specific declarations. The mounted Debug campaign verifies historical
AROW searches, updates, source hydration, highlight text/spans, exact lost-ACK
recovery after name reuse, native/SQL deletion and cold metadata/data-owner
reopen. It also creates a document table through the public API, indexes a
nested SQL alias, updates it through SQL, repeats searches/highlights after cold
reopen and proves rejected declarations do not reserve a name.

Qualification gates use Debug and one native build job:

- `zig build antfly-api-sql-index-lifecycle-test -Doptimize=Debug -j1`
- `zig build sql-test -Doptimize=Debug -j1`
- `zig build antfly-metadata-test -Dtest-filter="SQL native index" -Doptimize=Debug -j1`
- `zig build antfly-api-derived-coverage-test -Doptimize=Debug -j1`
- `zig build index-manager-test -Dtest-filter="observed analyzer publication" -Dtest-filter="observed full text analyzers" -Doptimize=Debug -j1`

The mounted gate passes both campaigns without failures, skips or leaks. The
full SQL gate passes 1,683 local and 254 server contracts, with four existing
Debug benchmark-only skips. Manifest/derived-coverage checks pass 128 local and
101 server contracts; the focused catalog and analyzer lifetime checks also
pass without failures or leaks. The immutable 1,586-case inventory is preserved.
See the [current case ledger](../sql-catalog-lifecycle-parity.md) for aggregate
counts and remaining work.
