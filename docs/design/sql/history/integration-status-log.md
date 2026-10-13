# SQL extraction: integration status, sessions, MERGE, TRUNCATE and online merge log

> Summary of a verbatim log. Full text: [docs/design/sql/history/integration-status-log.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/sql/history/integration-status-log.md). Originally relocated verbatim from `docs/sql-extraction.md` (lines 881–1913) at commit 3ce5aff4ea on 2026-10-12; durable decisions were folded into [SQL extraction](../extraction.md) before the move.

This log tracked SQL extraction integration status: query/mutation feature coverage, session settings and row policies, MERGE lowering, TRUNCATE generation retirement with hosted FK/graph cutover, and distributed online merge artifact authority. Counts and test totals describe the work at the time, not current guarantees; the log itself notes that passing focused component tests is not completion of SQL extraction.

## Integration status

Implemented at the time: ordinal-bound scalar programs, three-valued predicates, top-K, aggregates, joins, derived tables, nonrecursive and linear recursive CTEs, window functions (named WINDOW, GROUPS, EXCLUDE), correlated EXISTS/IN/ANY/ALL/scalar subqueries lowered to grouped hash joins under one coordinated capture, quantified LIKE/ILIKE, set operations, typed INSERT-from-query, RETURNING prepared before commit, ON CONFLICT (targetless, partial and expression unique targets), joined UPDATE/FROM and DELETE/USING, DEFAULT handling, durable READ COMMITTED sessions and savepoints, pgwire PREPARE/EXECUTE, SQL cursors (SCROLL, WITH HOLD), pgwire SET/SHOW/RESET/DISCARD, durable HTTP prepared resources, and dry-run EXPLAIN. Remaining major items listed: broader isolation deployment and fault validation, broader correlated/mutation subqueries and MERGE, unrestricted recursion, owned sequence support, broader retirement/promotion fault coverage, complete route-wide policy/session validation, and the full parity, fault-injection and workload benchmark gates.

### Decisions

- **Pending DDL is not reported as synchronous success**; uncertain restore admission returns an idempotency key and job handle with a non-success status and no-replay guidance.
- **Repeatable-read/serializable sessions** require explicitly capable providers, replicated range tracking and owner-fenced atomic prepare; unsupported providers reject BEGIN.
- **Local CTE/alias reads** pin multiple retained scan views inside one native statement-capture fence rather than pretending separately opened cursors share a snapshot.
- **CTE hints have execution semantics** — `MATERIALIZED` and multiply referenced CTEs share one quota-bound producer; `NOT MATERIALIZED` keeps separate inline scans; single-reference unhinted CTEs stay pipelined.
- **Multiple source matches for an UPDATE target** fail with SQLSTATE 21000 rather than choosing a row arbitrarily.
- **Deferrable ON CONFLICT arbiters remain rejected**, matching PostgreSQL's immediate-arbiter contract; ordinary deferrable UNIQUE shares native claim authority and is not an eligible FK parent target.
- **Linear recursive CTEs only** — mutual/nonlinear recursion, recursive aggregates/windows and nullable-side self joins remain explicitly unsupported.
- **HTTP prepared resources survive restart on the owning node**; failover to another owner is rejected rather than silently retargeting. Pgwire cursors and settings are connection-owned, not restart-durable. WITH HOLD publishes only after confirmed commit.
- **Ordered `search_path` fallback** advances only on exact table absence, not on authorization or other errors.
- **EXPLAIN** binds under real authority without opening a cursor or mutating; ANALYZE and fabricated cost estimates fail explicitly.
- **Mega-branch reference code** (`sql/lower_dml.zig`, `api/sql_adapter_integration.zig`, `api/table_writes/relational_mutation.zig`, `api/http_server.zig`, `api/auth_sql_adapter.zig`) is a behavioral reference to port selectively; no legacy adapter fallback should be introduced to claim parity.

### Rejected approaches

- **Eager INSERT-source capture for conflict-assignment scalar subqueries** — unsound, because an untaken assignment could raise a scalar error on a nonconflicting insert or a WHERE-false conflict; replaced by a post-owner masked Apply evaluated only for rows that actually conflict.
- **Per-owner conflict page allowance** — replaced by one batch-wide page quota; a 32-owner batch with three 64 KiB pages per owner fits a 512 KiB SQL admission budget.
- **VALUES lowered as a UNION plan** — replaced by a flat VALUES source with ordered arms. In the same Debug SQL fixture, INSERT peak fell from 8.49 MB to 2.54 MB; the 512-row membership fixture from about 7.80 MB to 1.51 MB; the ordered subquery fixture from about 7.47 MB to 1.23 MB (local admission-memory observations).

## Session catalog and policy-setting boundary: implemented core, incomplete activation

- A pgwire-only string map was rejected as unsafe: policies must not trust a value a client can change. Settings live in a versioned, typed registry in the SQL catalog (metadata Raft owns durable records; administrator-only mutation route); each statement binds `current_setting` against an immutable owner-captured view. SQL SET changes only an authorized session overlay, never the durable registry. Missing capture, stale generations, dynamic names and client overlays on policy-sensitive values fail closed.
- HTTP `/sql` sessions and pgwire share one command parser for SET/SET LOCAL/RESET/SHOW/RESET ALL; HTTP prepared resources bind the exact session ID and setting-catalog epoch. HTTP has durable idle connections; `DISCARD ALL` refuses active or uncertain transactions.
- Row policies must be catalog-versioned authority, not a SQL projection filter: owners apply USING before pagination with an owner proof tied to the read cut, and check WITH CHECK inside the guarded commit. SQL CREATE/ALTER/DROP POLICY edits drafts only; ENABLE/DISABLE requests a durable owner publication. Active policy publication stays guarded by capability checks until every public route and revocation/failover/restore test is complete.
- Missing credentials never substitute for an absent policy; missing publication credentials disable background polling.
- Original cases `sql-0045` and `sql-0047` remained unresolved pending full catalog-setting parity. Cross-owner failover/security workload gates and promoted-primary public-route coverage remained open.

## MERGE mutation lowering: partial

- MERGE executes only through a backend that stages coordinated target/source range proofs with the mutation in one durable transaction (implicit serializable transaction for autocommit). Read-committed or plain batch backends reject before reading. Unknown decisions keep a reconciliation ID and are never replayed automatically.
- Small `target._id = source.key` sources use a 129-row decision capture then deduplicated primary-key point scans (up to 64 keys per capture); more than 128 rows falls back to the source-preserving hash join. Direct full-key index probes use a 16-row nonunique fanout cap; sources above 32 rows, saturated fanout and non-READY indexes use the coordinated join. This is a conservative crossover heuristic, not a measured adaptive cost model.
- Native `auto_index` alone cannot provide the serializable guarantee (it may choose a primary scan when the index is not READY). The index proof must be a tagged index-span observation pinned to the READY generation; prepare reserves writer keys for old and new tuple prefixes. Merely incrementing a counter at seal would leave a prepare-to-commit phantom window; merely comparing a counter would miss pending writers. An unchanged tuple still advances its span generation on primary update.
- Classifier microbenchmark (10,000 rows, `ANTFLY_SQL_MERGE_CLASSIFY_BENCHMARK=1`): 3.92–4.23 ms with arena reuse vs 4.24–4.69 ms with one arena per row across three local runs.
- **Rejected:** sorted multi-get for the 257 primary-bucket proofs. Across 64 full-range captures, scalar gets took about 8.2 ms vs 12.8 ms batched (sparse counters), and 18.3 ms vs 23.6 ms (all counters populated). Reducing proof cardinality is the needed win.
- Mounted evidence at the time: `sql-0579`, `sql-0581`, `sql-0582`, `sql-0583`, `sql-0584`, `sql-0585`, `sql-0587`, `sql-0588`, `sql-0589`, `sql-0590`, `sql-0623` (read-only CTE source). Data-modifying CTE producers remain unadmitted; most corpus MERGE cases remained unresolved; real cross-owner failover parity not yet shown.

## TRUNCATE generation retirement: hosted public activation verified

- TRUNCATE reuses durable restore staging: reserve fresh table/range identities, prove new owners are empty, fence and drain old owners, publish replacement metadata atomically; old rows are never copied or scanned. Requires whole-table administrator authority; success requires known publication, not job acceptance.
- Admission requires an explicit owner identity capability for every source and untouched FK parent; unsupported pre-cutover attempts cancel durably. A capability mismatch after irreversible cutover is a recovery error that retains fences.
- RESTART IDENTITY uses the fresh owner generation; there are no owned sequences to reset yet. Once added, their counter generation must join the same staging plan.
- Graph TRUNCATE uses an owner-verified Raft-bound seal; graph `begin_cutover` is irreversible, so late cancellation cannot strand a sealed owner. The scoped public guard and test-only admission permit were removed.
- Retirement-set summary (compressed binary Patricia tree) replaced historical tombstone scans; no legacy scan fallback.
- Incoming-FK CASCADE must never truncate an outgoing parent implicitly; untouched parents are fenced without replacing their generations. Ignoring a missing generation in the coordinator would be unsafe. The legacy retirement worker rejects FK-removing targets; FK retirement uses the coordinated five-step generation protocol (pin/reserve, stage tombstones, irreversible activation then publish then ACK, generation-aware validation, resumable GC).

| Measurement (native Debug WAL) | With tree | Without | Ratio |
| --- | --- | --- | --- |
| 64 single-record commits, insert bytes | 130,124 | 56,000 | 2.32x |
| 64 single-record commits, GC bytes | 73,075 | 7,104 | 10.29x |
| 512 records in batches of 64, insert | 741,238 | 437,920 | 1.69x |
| 512 records in batches of 64, GC | 272,162 | 46,752 | 5.82x |

Ten thousand summary point reads took 14.27–16.53 ms locally — a bounded-read/progress tradeoff, not a write-amplification improvement. The installed `antfly-hosted-truncate-fk-recovery` binary passed 16/16 (local Debug timings: external-parent DROP 28.55 s, TRUNCATE baseline 31.96 s, fault recovery 28.62 s, graph baseline/CASCADE 34.86 s, graph fault recovery 6.69 s); relational API unit target 69/69.

### Hosted FK publication defects and open items

- Hosted external-parent FK DROP: a repetition stalled on the pre-DROP parent DELETE at Raft target index 10/applied 9; still under exact-entry investigation, so repeated stable completion remains a release gate.
- A point-probe transaction in the post-publication GC poll returned `Unsupported`; replaced with a cursor-capable snapshot read.
- Byte-equal child index catalog is not a valid invariant (a schema-version bump stages `full_text_index_vN`); the plan re-derives the successor catalog.
- Three-voter self-FK DROP initially stalled at parent stage because schema-migration finalization retired the versioned full-text index after the DROP plan was admitted; fixed by distinguishing generation publication from initial-FK support reservation locks and requiring an all-voter/learner v19 decoder proof.
- GC scans routing-first reference keys including live records; high-churn retirement needs a bounded-scale proof or a maintained generation locator.
- Moving index-derivation helpers out of `api/tables.zig` to a metadata-neutral module remains layering cleanup.
- Standalone initial FK CREATE with external parents and initial MATCH PARTIAL remain guarded; hidden-owner seed/promotion fault coverage remains a release gate.

#### Hosted retirement activation update

- Store-root retirement authority is explicit administrator enrollment (`POST /store-roots/enroll`, `POST /store-roots/enrollment-status`), not ordinary service registration; see [store-root enrollment](../../../store-root-enrollment.md). Initial-FK contracts require protocol v19 on every metadata voter and learner.
- The UUID and Ed25519 seed identify a root lineage, not unique hardware; enrollment is not hardware attestation or cloned-disk detection. Replacement roots require explicit enrollment, not trust-on-first-use.
- Offline cancellation renames only the verified root to trash and retries a signed ACK after restart; permanent generation fences prevent stale rejoin; trash GC is separate from acknowledgement.
- Mounted matrix passing at the time: `antfly-api-hosted-initial-fk-test`, `-fault-test`, `-transfer-test`, and both retirement cases in `-offline-test`. CI runs prebuilt proof binaries in two recovery lanes with a 15-minute hard timeout each.

## Distributed online merge: ordered artifact authority

- The ordered artifact catalog travels in the same data-Raft command as donor admission or receiver acceptance — no separate unfenced catalog-seal window. Data protocol v14 and metadata protocol v20 gate the decoder. Graph, generated vectors, enrichment and resolver tails remain ineligible.
- Each binding carries a physical catalog digest and a canonical logical-definition digest; compatibility ignores ordering and owner-local generations, so a healthy receiver does not rebuild merely because the donor assigned generations independently.
- Replica-local drift does not determine the replicated decision: apply persists a reconciliation intent and retries until physically ready. Portable export, backup and restore reject a pending intent.
- Evidence: one mounted fault case (`test_online_merge_recovers_after_owner_link_outage_and_crash[accept-accept_reply_restart]`), not a repeated matrix. Direct-vector native suite 15 tests plus five receiver soaks (65 total); Debug receiver timings 9.30–9.84 s (large/full-index), 5.06–5.43 s (default sync), 2.27–2.44 s (fulltext-only). Sparse compatibility suite 23 tests. Compaction/selective-query performance comparison remained unmeasured.
