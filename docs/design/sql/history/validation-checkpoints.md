# SQL extraction: local validation checkpoints and initial slice

> Summary of a verbatim log. Full text: [docs/design/sql/history/validation-checkpoints.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/sql/history/validation-checkpoints.md). Originally relocated verbatim from `docs/sql-extraction.md` (lines 1914–2130, 2155–2225, 2281–2336) at commit 3ce5aff4ea on 2026-10-12; durable decisions were folded into [SQL extraction](../extraction.md).

Local validation checkpoints for the SQL extraction effort, from the initial
compiler/pgwire slice through retained reads, subqueries, MERGE, TRUNCATE and the
parity-inventory release gate. Every check here was a local focused run, explicitly
not full repository/CI parity, distributed-latency, or workload-parity evidence; test
counts are not current guarantees. At the last checkpoint 1,381 of the original
1,586 corpus cases remained unresolved and blocking release.

## Initial slice design choices
- Shared bounded scanner and immutable single-statement compiler; parameters are typed nodes, not SQL substitution (grammar in `zig/pkg/antfly/src/sql/COMPILER_SUPPORT.md`).
- Shared non-executing Describe/Execute binder: one authorized catalog lookup, positional parameter inference, typed JSON literals parsed once; conflicting parameter types fail early.
- SQL writes to generated columns are rejected — native generation stays the single owner of their values. UPDATE reads only preserved columns and relies on the whole-row version fence. Unsupported shapes fail before mutation.
- Shared, bounded, scope-partitioned immutable plan cache for HTTP SQL and pgwire; compiles outside its publication mutex, dedupes concurrent misses, evicts only idle leased plans. Schema resolution, authorization, binding and parameters stay request-local.
- Schema-derivation cache (32 MiB maximum, four concurrent builders) is explicitly not an authorization or routing cache; catalog identity and authorization are fenced per request.
- Shared preparation admission plus an owned read/write execution permit; HTTP overload returns SQLSTATE 53300 with retry guidance.
- Relational reader may choose a READY compound/partial index for an equality prefix plus range suffix, keeping the full predicate as residual; primary key is the deterministic fallback and explicit primary-key ordering disables secondary-index selection. Costing uses a sixteen-candidate shortlist with at most eight snapshot-local index records per candidate (bounded actual probes, not persistent cardinality stats). Planning runs after releasing the shared apply lock so it does not serialize writers.
- Retained readers own the schema/store snapshot, predicates, projections and row-level authorization; leader loss never downgrades retained admission to stale. Pgwire rechecks credentials and read authorization on each retained page.
- The one-shot `antfly sql` command never automatically retries or follows redirects for SQL mutations.
- Document SQL rejects the relational-only stateless fallback when a retained reader is unavailable.

## Parity gate
- The 1,586-case corpus has a provenance-pinned inventory and disposition ledger ([inventory guide](../parity-inventory.md)). `make sql-parity-inventory-check` verifies integrity; `make sql-parity-release-check` intentionally blocks until each case has executable evidence or a justified exclusion — creating the inventory is not parity completion.
- The validator prevents a behavior-required case from being marked `rejected`, or an original rejection from being marked `implemented` without a tested `superseded` disposition, to stop status-only waivers. Partial evidence grants no release credit.
- All 21 referenced `make sql-parity-evidence-check` gates passed; grouping compatible Zig filters into seven build invocations ran in 5m54s with a cold API rebuild (not a controlled before/after measurement).
- Open at the last checkpoint: multi-owner and distributed fault evidence; full graph and external-parent FK cutover for TRUNCATE remained guarded.

## Key measurements
All local; the source labels each as executor/allocation or work-count evidence, not distributed throughput.

| Workload | Result |
|---|---|
| Compiler microbenchmark (Apple Silicon, ReleaseSafe, 10,000 ops ×2) | ~674–999 ns, 928 bytes retained arena |
| 256,000-byte token-heavy input rejected at 128-token quota | ~469–792 ns vs 1.356–1.808 ms to tokenize fully |
| Executor COUNT of 10,000 rows | 40 pages under 512 KiB quota |
| Index costing fixture (96 rows) | descending range visits 6 index records; competing covering index: 7 probes, 1-record scan |
| Nested-shape binding (ReleaseSafe) | ~5.24 µs / 31.4 KiB inferred vs 2.83 µs / 17 KiB with hints |
| 10,000-row pull fixture (ReleaseSafe) | ~98 KiB retained; first 73-row page 0.42 ms; done 63 ms |
| Moving SUM, 10,000 rows, 8,193-wide (ReleaseSafe) | 2.73 MiB aggregate state, ~2 ms |
| Native LSM bookkeeping (Debug, 100×32 rows) | inactive 452 ms vs active 485 ms, ~7.3% overhead, one run, preliminary |
| 100,000-touch bookkeeping fixture | inactive: 1 probe, 0 counter writes; active: 2 probes, 1 coalesced write |
| Membership subquery, 10,000×10,000 (early run) | 3 scans, 1 capture, 8.43 MB peak, ~1.59 s |
| Shape-aware subquery, 10,000 rows, production allocator (final rerun) | membership 57 ms; ordered ANY/EXISTS 30–31 ms; uncorrelated EXISTS 20 ms (fetched 256 inner rows) |
| Top-K replacement reusing displaced root arena (10,000 rows, k=5) | 1,299-byte peak, 44,987 comparisons; Debug ~766 ms → 8 ms; ReleaseSafe ~0.85 ms |
| Joined UPDATE, 4,096 rows (ReleaseSafe, 64 MiB budget) | 8,192 native rows in one capture, 32,797 checkpoints, 9,357,062 query-byte peak, ~19–22 ms |

Subquery timings with the production allocator are not comparable to older timings taken with the leak-checking allocator, and the separate shapes are not an apples-to-apples speedup comparison.

## Notes recorded only here
- `make generate` repeatedly needed a separate cache (e.g. `/private/tmp/antfly-sql-generate-cache`) because the default Zig cache referenced missing generator artifacts.
- The repository-wide license check reported pre-existing header mismatches outside the initial slice.
