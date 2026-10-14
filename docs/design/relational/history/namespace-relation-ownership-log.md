# Namespace relation ownership publication: implementation log

> Summary of a verbatim log. Full text: [docs/design/relational/history/namespace-relation-ownership-log.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/relational/history/namespace-relation-ownership-log.md). Originally relocated verbatim from `docs/sql-parity-inventory.md` (lines 1445–2488) at commit 3ce5aff4ea on 2026-10-12; durable decisions were folded into [zig/SYSTEM_CATALOG.md](../../../../zig/SYSTEM_CATALOG.md#namespace-relation-ownership) before the move.

This log records building a shared, namespace-qualified relation-name registry (tables, access indexes, constraint-owned indexes) as internal protocol work: the publication planner, writer adoption, durable reconciliation jobs, snapshot/replay verification, leader coordinator scheduling, v32 root publication, bounded relation point-reads, guarded name-sensitive replacements and an internal index-DDL relation kind. Throughout, the log stresses that this was infrastructure: no original SQL parity case received credit, and the inventory stayed at 475 implemented / 136 rejected / 73 superseded / 902 unresolved. Public unqualified SQL index resolution was not enabled.

## Decisions

- **Ownership is derived, never client-supplied.** `TableCut` derives names from the authoritative schema and namespace binding; a UNIQUE index's paired access/rule declarations yield one claim; named UNIQUE/primary-key constraints yield a constraint-index claim; CHECK and FK names reserve nothing. Unknown provenance, dangling index-origin rules and duplicate names are rejected.
- **Transaction-scoped publication keeps the original before cut.** LSM metadata transactions read their own pending writes, but that must not replace the original before cut or publish intermediate names. Repeated producers must present the same ownership fences; compiling the whole transaction detects cross-table collisions and permits atomic name swaps. Validation is one point lookup per distinct name; unchanged cuts write nothing; a stale cut is not accepted just because the same table ID and name still exist.
- **Metadata command rollback uses a bounded first-touch before-image journal** restricted to metadata keys. It is explicitly not a user-row savepoint. Filtered (replay verification-only) journals reject command-local rollback and require outer transaction abort.
- **Writer adoption is gated by a durable marker that is not installed automatically** and is not a serving capability; verified bootstrap/migration must establish the authoritative cut first. Untracked document groups stay untracked: ordinary writers must not silently adopt the protocol before the metadata capability barrier.
- **Replay never repairs missing sender effects.** Binary replay verifies the sender's complete before/after cut; missing/forged/injected claims fail closed and publish no notifications or receipt.
- **Active root is built by an isolated reconciliation job, not by backfilling the registry in place** (`relation_reconciliation.zig`). Pages are bounded by 64 tables, 8192 claims and 4 MiB; preparation happens outside apply, apply CAS-checks the job state and source epoch. Physical lexical cursors (not numeric table IDs) survive restart. "Ready is not a serving state."
- **GC of replaced generations** persists an exclusive lexical cursor per delete page to avoid re-traversing LSM tombstones; one retained job per group prevents ID reuse without per-generation tombstones; a published generation can never be deleted.
- **A separate durable source clock** advances only on name/schema-relevant changes (identity, physical name, public schema bytes). Description, placement, read-layout and runtime-progress changes do not invalidate a build; exact rewrites and job/GC progress do not advance it.
- **Fingerprints are never self-authenticating.** New candidate owners are point-derived from authoritative schemas/bindings so a forged claim and matching forged job hash cannot authenticate each other; native replay rejects correctly fingerprinted pending claims without an authoritative pending-plan source; a forged ready seal cannot authenticate itself via consistent counts.
- **Advance intents carry the durable before cut, not leader-supplied claims or source JSON;** each replica prepares from its own pinned authority. Permanent source failures are derived locally and recorded as a terminal reason; resource, I/O, corruption and stale-generation errors are not reclassified as permanent. Failed generations cannot be rewritten; new generations may rebuild.
- **Unpublished formats get no compatibility decoders:** job format `AFRC04`, compound entry codec `AFRE01` (one active owner plus one reserved successor), live manifest `AFRL01`.
- **Leader scheduling** runs on a per-service `std.Io` lane at a 250-ms cadence, at most one bounded intent per slice; ambiguous admission, term changes and coordinator restart re-observe durable state rather than assuming an intent won. No job apply waiter or polling thread.
- **Publication proof scans** pin one snapshot, verify one bounded page per step, and are renewed at the preparation deadline (after rechecking job, epoch, root and applied-log position) without discarding verified pages. The applied-index fence remains strict: progress is not rebased across unrelated commits. A shared native pool holds eight evidence slots and at most two active scans per store.
- **Root publication is a separate v32 command** carrying the ready cut, exact predecessor generation and membership-bound activation, never a sender-authored proof. Each receiver prepares its own proof; the publishing transaction rechecks and atomically swaps root/live manifest and adopts writers. Published groups retain the v32 floor and reject downgrade.
- **Standby apply treats only `CatalogPublicationProofPending` as a cooperative yield;** corruption and ordinary failures still propagate. Replication completion requires applied EOF, not transport EOF.
- **Snapshot-prefix retries use a replica-local installation receipt (`AMSI01`)** (index, length, SHA-256), committed atomically with the applied watermark and excluded from replicated snapshots, so a retried prefix cannot roll back completed log work.
- **Bulk relation resolution** allows at most 256 lookups in one immutable transaction pinned to one published live root; missing/unpublished roots cannot attest absence; older peers cannot turn unsupported resolution into a miss.
- **Name-sensitive replacements use a guarded wire tag requiring decoder capability 33,** carrying the exact active owner, logical table and incarnation; stale replicated proposals become no-ops. Unrelated source revisions are intentionally not preconditions. The real-service test showed guarded writes must install and observe capability 33 before user admission (mock readiness could not prove this).
- **Index DDL has its own internal relation kind** rather than masquerading as ALTER TABLE with a client-supplied owner; the logical owning table is authorized before object-type diagnostics; constraint-backed indexes must be dropped via their constraint.
- **Relation results carry the owning table's logical name** from the same transaction, so a namespace move cannot turn a physical routing name into an authorization name. This read contract is not a write-admission fence.

## Rejected approaches

- Whole-catalog maps, table scans or user-row scans for validation — rejected throughout in favor of point reads and streaming cursors; "a table scan must not replace the missing point-resolution path."
- Trusting candidate/job fingerprints for replay — a journal-backed FK regression reproduced acceptance of a forged pending schema digest under the earlier insert-only check; changed existing entries are now verified too.
- Hashing the entire mutable table envelope into the source clock — would invalidate builds on description/placement/progress changes.
- A fixed 60-second snapshot lifetime for publication scans — very large/slow catalogs could exceed it; replaced by progress-preserving renewal.
- Accepting a ready job flag in the real service fixture — replaced by waiting for matching completed evidence.
- Replay switching root generations on a ready fingerprint or a full scan under the apply lock — explicitly prohibited.
- Materializing the whole standby receive backlog (two copies) per retry — replaced by one indexed owned record at a time.

## Key measurements

| Item | Result |
| --- | --- |
| Reconciliation page prep, million-table generated source | one 64-table page within 64 KiB scratch |
| 256-table checkpoint import verification | within 16-KiB verifier scratch |
| Unrelated 256-KiB payload | handled under 16-KiB allocator / journal headroom |
| Restore projection | 512 KiB ignored artifact data with 32 KiB scratch |
| Final-boundary check | one seek, at most one successor step, independent of generation size |
| Standby record buffer | 8 KiB budget covers 64 queued 4 KiB records |
| Mutation fold microbenchmark (ReleaseFast, `mutation_fold_bench.zig`, 4,096 inputs, 20 iterations), legacy/indexed | 32.46/0.478 ms one-table distinct; 19.36/0.486 ms across 64 tables; 10.14/0.317 ms half-repeated; 0.171/0.139 ms 32 hot rows; indexed path allocated 1.21 MB vs 0.54 MB cumulatively per fold |
| Exact binary64-to-decimal (64 values, three samples) | old wide formatter 1,366–1,419 ms vs 0.183–0.217 ms for decimal limbs; debug oracle over every exponent and both signs in 860 ms |

The log notes these are CPU/allocation or kernel microbenchmarks, not end-to-end SQL or storage throughput. Late gate counts recorded in the log include 182 catalog tests, 10 opaque metadata-owner tests and 22 linked tests (84/84 steps); 450 hot-standby tests; 23 coordinator tests; 178 standalone catalog tests with one skipped (66 steps).

## Unfinished at the end of the log

Automatic capability-fenced writer adoption and fresh-catalog/standalone publication bootstrap, public unqualified SQL index resolution (PostgreSQL DROP INDEX lowering, scoped authorization/search-path binding), atomic multi-owner DROP, online CONCURRENTLY builds, CASCADE dependency retirement, REINDEX integration, and multi-peer coordinator failover/fault coverage. The registry must not become a second independently committed catalog.
