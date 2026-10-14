# SQL extraction: pre-merge activation work (2026-09)

> Relocated verbatim from `docs/sql-extraction.md` (lines 732–880) at commit 3ce5aff4ea on 2026-10-12. Durable decisions were folded into [SQL extraction](../extraction.md) before the move. This is a historical implementation log; it describes work at the time it was performed, and its counts and test totals are not current guarantees.

### Pre-merge activation work

Standalone ordinary FK publication now uses the durable native owner-control
path, with operation receipts separate from Raft applied watermarks. Catalog,
schema, receipt and fence release are committed together. The linked native
regressions pass initial self-FK publication, ordinary DROP/ADD and reparenting
to two external parent owners, lost source/install replies, cold restart and
canceled hidden-owner retirement (three tests). Two additional native storage
tests pass receipt replay and old-schema pinning through restart. Initial
external-parent CREATE, including MATCH PARTIAL support-index publication, now
passes valid/orphan/protected-parent writes, cold restart and deterministic
cancellation in native standalone. Schema finalization uses an exact durable
node/store/root binding and owner progress, never synthetic Raft placements.
Copied foreign owner bindings do not authorize native publication. This does
not enable that lifecycle on hot-standby deployments without explicit adoption.
The linked native activation suite now passes six tests, including public
external-FK/graph TRUNCATE and cold reopen, plus three repeat runs (18 checks)
without skips or leaks. Restore authority cancellation checks run outside the
metadata mutex; test mutations that require immediate phase-two delivery ask
for write-level acknowledgement rather than the API's proposal-only default.

The same work fixes parent receipt validation for non-first owner ranges and
keeps ordinary owner ACKs off the full-catalog decode path: a revision-checked
point read advances an unchanged projection, while publication or concurrent
catalog changes require the complete immutable snapshot refresh.

Authenticated retirement summaries now use a transactional compressed Patricia
tree, with immutable-value hashes, canonical roots, protected keys and bounded
handoff/GC admission. Eighteen focused tests pass, including native abort,
restart, owner transfer and two-phase GC. In the local Debug benchmark, 10,000
summary point reads took 14.3–16.5 ms. This trades write cost for predictable
reads: at 512 records with insertion batches of 64, measured insert WAL was
741,238 bytes versus 437,920 without the tree (1.69x), and GC WAL was 272,162
versus 46,752 bytes (5.82x). These are small local fixtures, not production
throughput claims or a claim of reduced write amplification.

Standalone and bound-primary hot-standby row-policy publication pass two linked
native tests, including ordered metadata/owner replay and cold reopen. This
does not yet prove the complete promoted-primary failover matrix. The catalog
suite passes 146 tests, and the distributed transaction contract suite passes
102 tests.

Durable idle HTTP connections pass eight focused tests, including authenticated
public handler calls for settings, commit/rollback, DISCARD, prepared-resource
retirement and ownership checks. External-parent and graph TRUNCATE are enabled
through the public route without private test permits. The installed recovery
binary passes 16 tests without skips or leaks (five mounted real-listener
scenarios, two focused contracts and nine import anchors); the SQL boundary
suite passes 69 tests, including rejection of a missing parent admission proof
and unsupported native source/parent receipt authority before job admission.
These results do not establish release readiness for the entire SQL feature.
The TypeScript connection helpers use generated request/response schemas and
bounded, non-retrying SQL transport; the SDK SQL suite passes 30 tests.
The complete TypeScript SDK suite passes 396 tests with one conditional bundle
check skipped.
Follow-up SQL gate repair: `zig build sql-test` now uses the scoped native SQL
imports and compiles the SQL and row-policy contract namespaces by default;
269 tests pass, including a high-fanout join allocation-budget regression.
The parity-style `SQL original` filter passes 12 tests, rather than silently
selecting none. Connection-bound prepared resources can now be closed by the
Python and TypeScript SDKs with the required connection header; focused SQL
SDK suites pass 22 and 31 tests respectively, with TypeScript typechecking.
Full Python and TypeScript SDK suites pass 257 and 397 tests respectively
(one conditional TypeScript bundle check skipped).
The hash-join iterator reuses its candidate scratch across output rows. In a
local ReleaseSafe A/B measurement of the 65,536-pair residual-join fixture,
backing allocations fell from 68,120 to 2,585 (96.2%), cumulative allocated
bytes from 123,311,135 to 96,572,855, and peak tracked memory remained about
1.29 MB. The fixture aggregates its 32,640 matches to isolate join processing
from result materialization. Reproduce the allocation budget with
`zig build sql-test -Doptimize=safe -- --test-filter 'SQL high fanout join'`.
These allocator measurements are not production-throughput evidence or a
complete release gate.

Direct-field dense/sparse vectors now have bounded, resumable snapshot transport
and ordered tail coverage, including oversized historical base artifacts on
full-text-only tables. The focused vector suite passes 15 tests; its 13 storage
tests also pass five additional runs (65 checks) without skips or leaks.
The same 15 tests pass with production admission enabled and test permits
removed. Ordered-artifact compatibility passes 13 tests, portable backup 39,
retained effects 32, and transaction reservations 49. Graph/enrichment/resolver
asynchronous tails remain unsupported.

The next activation cut is still in progress. Ordered-artifact regressions now
pass 54 tests, including durable stale-publication rejection, ambiguous commit
recovery without replay-sequence reuse, owned preparation outside the apply
lock, binary-safe catalog transport, and attempt-bound immutable source layouts.
The graph transfer contract preserves source bytes for receipt verification and
rebinds physical generations using separately authenticated receiver layouts;
the wider source protocol is not advertised yet. Certified source-copy graph
ownership blocks are included in the transferable source-cut digest; omitting
or changing those bytes changes the certificate. Retained-transfer regressions
pass 28 tests, including source-certificate block binding, graph-bearing pinned
source export/reopen/import, oversized frames,
bounded spool caching, authenticated
cold reads, partial-copy corruption repair and restart at exact row boundaries.
The compact publication upload codec and staging primitives now pass retry,
ordered-age pruning, tombstone and allocation-failure checks. These primitives
are not yet connected to the complete Raft/standby upload/finalize path.
Malformed producer graph effects become durable invalid-output rejections;
resource failures and persisted-catalog errors remain failures, not success.
Actual producer integration, complete baseline/provenance admission, historical
proof transfer/adoption, large-publication runtime transport and the full
graph/enrichment/resolver recovery matrix remain activation gates.
These passing foundations are not evidence that those families are enabled.

Sparse immutable segments now bind postings and document maps to per-document
incarnations, preventing stale scores after stable-ordinal replacement and late
compaction publication. Sidecars are point-addressed, query caches are bounded,
and selected compaction inputs are reserved before allocation. The sparse suite
passes 24 tests, including replacement, restart, compaction races, metadata
retirement, corruption and budget denial. A fixed-capacity preflight cache
avoids charging repeated terms for identical document metadata; collisions
conservatively overcount. These are correctness and bounded-work checks, not a
large-scale throughput benchmark.

Native checkpoint capture now waits for an in-flight immutable-table build and
drains the committed mutable tail before exporting its manifest. Previously a
background flush could make the drain yield, producing a self-contained image
without committed retention metadata. Explicit checkpoint admission no longer
overrides the shared maintenance budget. Three checkpoint/backpressure tests
pass; the deterministic contested-cut export/reopen regression also passes ten
additional runs with no skips or leaks. This is correctness evidence, not a
checkpoint-latency benchmark.

The runtime suites pass 50 native implementation and 173 consumer tests. The
three deterministic production DataServer merge/split histories and the native
retirement regression also pass three repeat runs (12 checks), with no skips
or leaks. These cover UNIQUE/FK ownership, public writes, failover, restart and
exact-root cancellation. Native reconciliation and retirement now preserve
the existing storage owner rather than attaching a second opaque owner; cold
retirement reads use the same filesystem runtime.
Generation-transition rejection returns temporary unavailability without
claiming an aborted transaction or authorizing blind replay.

The final API runtime suite passes 338 tests, including shared native/document
restore, hidden handoff capability guards and generated route-policy coverage.
Both focused public write-unavailability regressions pass. The rebuilt mounted
TRUNCATE recovery binary passes all 16 tests after the hidden-owner routing fix;
these results do not rely on a previously built binary without the capability
checks.

The current inventory check still has 1,586 original cases: 103 implemented,
59 explicitly rejected, 43 superseded, and 1,381 unresolved dispositions.
Unresolved dispositions are not a count of distinct missing features. These
activation fixes do not replace the broader SQL parity/release gate.
The exact prepared UUID CREATE TABLE case now has mounted pgwire, durable
catalog snapshot/reopen, provisioned owner, and owner-restart row evidence.
