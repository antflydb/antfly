# SQL extraction: capability reconciliation and exact-source adjudication (2026-10)

> Relocated verbatim from `docs/sql-extraction.md` (lines 30–167) at commit 3ce5aff4ea on 2026-10-12. Durable decisions were folded into [SQL extraction](../extraction.md) before the move. This is a historical implementation log; it describes work at the time it was performed, and its counts and test totals are not current guarantees.

### Current capability reconciliation (2026-10-06)

Reviewed against main `09b78df97`, after lake integration, Loadscape fixes,
embedded execution separation and the Zig 0.17 migration. The chronological
notes below describe checkpoints; they are not an additive list of missing
features. The earlier single-namespace, CTE-source and FK/graph-TRUNCATE limits
have been superseded for the supported providers.

| Surface | Available shape | Actual remaining boundary |
| --- | --- | --- |
| Reads and operators | Joins, sets, CTEs, aggregates/windows, typed batch kernels, spill-backed blocking operators and pgwire continuation pages | Exact original-case adjudication; broader correlated/lateral shapes; remote workloads rather than only local kernel benchmarks |
| Mutations | Native/document DML, joined UPDATE/DELETE, read-only CTE sources, MERGE, normalized RETURNING and guarded UNIQUE conflict ownership | Data-modifying CTE dataflow; broader demand-masked/correlated conflict subqueries and index membership proofs; MERGE partial/expression-key probe planning |
| Recursion | Bounded linear delta worklists, typed deduplication and captured physical sources | Mutual/nonlinear recursion, recursive aggregates/windows and nullable-side recursive self joins reject explicitly |
| Sessions and protocols | READ COMMITTED sessions/savepoints, durable HTTP prepared/connection resources, pgwire scroll/hold cursors, typed settings and ordered multi-namespace lookup | Stronger-isolation provider activation/fault coverage, TTL visibility contract, guarded Lite sessions, restart/failover ownership and complete setting/policy parity; HTTP has no connection-owned cursor resource |
| Catalog and retirement | Native schema/index/constraint DDL; hosted and native standalone external-FK/graph TRUNCATE with durable publication/recovery receipts | Owned sequences/serial declarations, broader schema rewrite/restore and standby-promotion gates; pending receipts are not synchronous publication |
| Row policies | Versioned settings/policy catalog, owner proofs and guarded publication, including native/standby component coverage | Every public protected route, revocation/restore and promoted-primary fault/security validation; unsupported providers remain closed |
| Lake SQL | Read-only Parquet/Iceberg attachments, snapshot/version pinning, pruning, bounded typed cursors, shared caches and spill | Remote end-to-end performance/recovery evidence; writes, nested/binary inference and native SQL decimal semantics are not claimed |

This branch has 1,169 blocking dispositions, down from 1,381 on the main base.
Of these, **130 are original rejection contracts**, not missing positive
functionality; 1,039 require behavior review. Four have recorded partial
evidence and 1,165 have no case-linked evidence. Absence of evidence is not
evidence of absence of an implementation. The 212 newly closed cases have
exact-source executable evidence, not parser-success or component-overlap credit.

The work queue, grouped without double-counting original IDs, is:

| Queue | Blocking cases | Next acceptance criterion |
| --- | ---: | --- |
| Original rejection contracts | 130 | Execute exact SQL/parameters and check deliberate diagnostic/nonadmission behavior; newly supported behavior needs tested supersession |
| Reads, queries, aggregates, windows and joins | 316 | Mounted result/null/type/authorization assertions, plus bounded-work and cancellation cases for the admitted shape |
| Positive DDL and session contracts | 349 | Separate public protocol behavior from obsolete planner fingerprints; verify durable publication, pending receipts and owner recovery where required |
| Mutation and population contracts | 337 | Exact affected rows/RETURNING, one guarded commit, conflict/no-write failure behavior and physical read-back; cross-owner faults where required |
| Lateral, query functions and EXPLAIN | 37 | Distinguish executable shapes from explicit unsupported shapes and fabricated planner/cost contracts |

Start with exact-case evidence on already admitted shapes; implement only gaps
exposed by that comparison. Keep deployment activation gates distinct:
distributed isolation/RLS integrity, owned sequence semantics and
shared artifact-stream certification each need their own fault matrices.
Broader graph/enrichment/resolver provenance, producer completion, adoption,
recovery and all-member capability barriers are shared storage work, not
additional SQL grammar features. The final ordered-artifact checklist below
remains open independently of source-corpus parity.

Use `python3 scripts/check_sql_parity_inventory.py --report` for current
family counts; `--family read --report` limits reporting only. Use
`--evidence --family read` or `--evidence` with repeated `--gate NAME` to run recorded
evidence without claiming release readiness. Families without recorded gates,
unknown gates and gates outside the selected family fail rather than reporting
an empty successful run. Gate selection is not allowed with `--release`, and
`--release --family read` still validates the entire inventory.
Zig 0.17 evidence commands use repeatable `-Dtest-filter=...` options so native
test owners receive compile-time selection, rather than relying on ignored
runtime arguments. Shared gate filters are grouped into one build per owner.
Native standalone TRUNCATE has a linked restart regression in
`standalone/runtime.zig` (`standalone native TRUNCATE external FK and graph
publish after restart`); this inspection does not claim that its linked suite
was rerun in the reconciliation batch.

Initial reconciliation validation on the main base: all 20 audit unit tests pass,
inventory integrity passes with unchanged dispositions, and nine selected
evidence gates pass. The narrowed SQL compiler/prepared-CTE/EXPLAIN run passes
seven server-owner tests plus six local-owner tests; the pgwire selection covers
ordered search paths, prepared reads, cursor forms and session commands. Two
mounted public-handler tests pass exact relational reads and coordinated UNIQUE
default-conflict updates. Initial broad pgwire execution needed permission to
bind local sockets; the final narrowed gates pass without the unrelated listener
tests. This is representative evidence, not an execution of all 1,586 cases or
the linked native TRUNCATE/promoted-standby suites. No case received release
credit merely because an overlapping component test passed.

### Exact-source adjudication batch

The shared `Corpus` loader owns all 1,586 original statements and parameters,
validates ordinal identities and provides O(1) lookup. Seventy-seven original
rejection contracts now assert their precise compiler error and diagnostic
range before backend access. The other original rejection contracts compile
today and still need runtime rejection evidence or a tested supersession.

The native public HTTP fixture executes 115 additional exact reads with
independent, bounded SQLite expectations: full rows, duplicate multiplicity,
explicit column labels, exact integers and SQL NULL provenance. The original
tagged internal parameters are decoded into today's public JSON values without
changing their logical types. Discovery never changes dispositions; only
reviewed cases with a passing mounted gate receive evidence. The reference
cannot mutate its database and has instruction/result limits. SQLite-only
semantics, unsupported syntax and vacuous fixtures are excluded without credit.
In particular, default NULL ordering and division-by-zero semantics cannot be
adjudicated from SQLite. Two additional exact native contracts verify descending
NULL ordering and implicit window labels without imposing unspecified peer
order. The explicit empty WHERE-false contract is the sole empty-result exemption.

That comparison exposed missing `trunc`/`sign` functions and compound window
ORDER BY aliases. The functions use the shared typed scalar pipeline, retaining
exact integer values and checking type, arity and nonfinite inputs. Window
orders now expand aliases once in the output domain, preserve window-input
scope, resolve implicit function labels, reject ambiguous aliases and reuse
computed window slots. Mixed and qualified wildcards expand through the pinned
visible source layout before binding, preserving duplicate output labels and
quoted identifiers. Expansion checks the 1,024-column budget before allocating
the output layout, preventing repeated stars from amplifying binding work. A second
regression bounds final sort heap capacity by actual materialized rows rather
than response headroom, including nested queries; oversized results still fail
with their original result-limit error.

Validation: 27 audit/reference unit tests; the full SQL target (131 server-owner
tests and 265 local-owner tests); native
HTTP execution of all 115 selected reference reads and two native contracts;
and reproducible golden expectations. Work-bound coverage admits a
three-row ordered window under a 256 KiB budget with 4,096-row response headroom,
without weakening result limits. These are correctness/resource bounds, not a
claimed wall-clock speedup. DDL/session and mutation/population adjudication,
the other read fixtures and the distributed activation matrices remain open.

Eleven exact UPDATE/DELETE contracts now execute on a separate two-row native
fixture. Each checks the affected-row count, complete RETURNING values and
labels, NULL provenance, persisted postimage or deletion, and the untouched
row. A mixed RETURNING expression that divides by zero must return `22012`
without changing primary bytes, version or content digest. This is single-owner
behavioral coverage, not certification of old point-index plan fingerprints or
distributed fault matrices.

SELECT and RETURNING share projection parsing and bounded wildcard expansion.
Expansion carries transient pinned-layout ordinals, so duplicate derived labels
do not collapse into ambiguous name lookups. INSERT/UPDATE evaluate prepared
postimages and DELETE evaluates its captured preimage; hidden physical metadata
stays out of stars. MERGE's unqualified star is target-only, while qualified
source stars use its authorized captured domain. A catalog-only dependency pass
shares the identity cache with final binding and deduplicates scan dependencies;
ordinary non-wildcard projections do not allocate an expansion array. Tests
cover allocation failure, pre-write output limits, source/target MERGE domains,
duplicate names, and quoted window labels containing literal dots.

Native discovery also confirmed that `sql-1519` remains a real parser/scope gap:
three-part column references such as `public.usage_records.id` are not admitted.
Its disposition remains unresolved; successful ONLY-qualified table references
and ordinary aliases do not waive that separate contract.
