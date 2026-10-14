# Relational-indexes extraction status

> Summary of a verbatim log. Full text: [docs/design/relational/history/indexes-extraction.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/relational/history/indexes-extraction.md). The original is an implementation ledger, not a statement of available API features.

The extraction covered R3–R6 of the relational restructure (landed in #784): relational indexes and constraints, foreign-key integrity, shared row reads, and relational mutations. SQL ingress, sessions, pgwire, and lake/catalog changes were out of scope. The source was `combine-pr-141-143-144` at `79644dfa1605e8da0f486d021d1c1393577d6265`, extracted onto main at `2b57462b1`. Almost every ledger item landed. One stayed open: distributed stored-generated row rewrite jobs for ALTER. The current restore design is in the [restore architecture](../../relational-restore-architecture.md).

## Decisions

- **Keep main's row machinery.** Only the native descriptor vocabulary and the pure generation/lease decisions came from the source's `storage/schema.zig`. Its row/tuple machinery must not replace main's AROW v2, immutable schema epochs, prepared-row pipeline, transactional row catalog, or LSM hardening.
- **Wire vocabulary lives in OpenAPI.** Native durable tags stay separate, with explicit name-based conversions. Generated enum ordinals must never become persisted tags.
- **No separate public route.** Relational indexes reuse `/tables/{tableName}/indexes` with `type: relational` and ordered composite `keys`. Definitions live only in `schema.relational_indexes`, never duplicated in the artifact catalog. Go uses a pointer to the optional array so that an explicit `[]` remains a drop request.
- **Physical identity is a persisted 12-byte generation/slot, not the name.** Names stay in the catalog. Changed or reintroduced definitions get monotonically newer generations. Generation fingerprints depend on the indexed column semantics, not the whole schema digest, and don't change when a column moves ordinal.
- **Publish durably, then make visible.** The catalog controller compiles before the commit section and publishes a retained plan only after the catalog/outbox transaction commits. Schema, table catalog, index head/blob, and outbox commit in one backend transaction.
- **Atomic index effects.** Production batches prepare index keys from typed rows before apply-exclusive, recheck both identities after admission, and commit coalesced index effects in the same primary/catalog/outbox commit. Deletes remove every document-owned generation, not only the current plan's.
- **Reverse records are document-owned and checksummed.** They reconstruct forward companions without loading the primary row or the current schema. A four-byte document-length footer makes forward-key ownership inspectable without historical layouts.
- **Retirement is generation-local.** It uses a private `(generation, slot, document)` ownership locator, so GC visits only that generation's prefixes. This avoids repeated full-table vacuuming and unbounded retained definition blobs.
- **Backups don't serialize physical index entries.** Both import paths rebuild them from canonically validated AROW rows. This removes stale generations and avoids trusting forged tuples. Retired definition blobs are omitted from manifests.
- **Shared read guards protect FK dependencies.** Transaction prepares keep non-write version predicates as durable shared read guards. They protect exact-key absence but are not range/predicate locks.
- **Referential actions stay synchronous.** Ordinary CASCADE/SET NULL use bounded fixed-point discovery before the atomic distributed commit, with explicit row/byte/time limits. They do not become background sagas or asynchronous action jobs.
- **No silent elevation.** FK declarations require admin permission on every referenced parent. Cascades require write permission on every table whose primary rows change.
- **Explicit rewrite for generated columns.** Adding, removing, or changing generated columns requires `PUT`/`PATCH /tables/{name}/schema?rewrite=true`, which returns `202` and a restore job. Defaults fill only absent fields, never explicit null. Durable replay does not reapply defaults.
- **CHECK semantics.** TRUE and UNKNOWN pass a CHECK. A row-query WHERE accepts only TRUE. A failed CHECK validation never disables enforcement on new writes.
- **Partial indexes need a sound proof.** A query must provably imply the partial-index predicate through a bounded conjunctive per-column domain proof. Otherwise it returns `PartialIndexPredicateNotImplied`. Arbitrary OR and general algebraic implication are out of scope. This is distinct from embedding `coverage_policy: partial`.
- **Independent historical table restores stay closed.** Unrelated snapshots don't prove a consistent cross-table cut. Selected-table restores must include every outgoing FK dependency from both the active and read schemas.
- **Online merge has a default-on admission switch, but Raft groups are ineligible.** `antfly metadata --online-merge-enabled` defaults to `true`, yet Raft-backed donor and receiver admission deliberately reports ineligible. Index, resolver, and enrichment catalogs are replica-local, and a group can add a learner after a singleton admission. Direct proposals fail with `OnlineMergeArtifactCatalogUncoordinated`. Re-enabling online Raft merges needs a Raft-ordered artifact-catalog epoch plus a membership barrier. The guard is per upgraded process, not a fleet-wide flag: an older metadata leader paired with an older data leader can still start an online attempt.
- **Quota rejection cannot discard committed 2PC decisions.** Prepared votes reserve REF3 capacity through the intent ledger. Local OOM and corrupt history are never turned into a successful or deterministically rejected application.
- **Abandoned retained-tail sources were rejected.** Split deltas and the derived change journal were not reused as the merge effects log. Without atomic append, retention admission, and receiver acknowledgement, reuse would lose deletes or mix source revisions. Long write quiescence for all document tables is not an acceptable substitute for the online contract.

## Rejected approaches and fixed defects

- **Installing a layout-only epoch first at startup.** Same-version registry deduplication then discarded the public validator after reopen. Startup now installs the complete validator-bearing epoch once.
- **Starting maintenance before the DB reached its stable address.** Index builds stayed pending indefinitely.
- **Skipping the forward-payload write when an update touched only unselected columns.** This would expose stale versions/TTL metadata, so the write stays.
- **Using a client schema version as proof that integrity planning ran.** Internal prepares also carry the catalog generation-set proof, compared under the apply fence.
- **Treating a first shard's coverage as proof for the whole table, or caching positive coverage across restores** without a durable restore-incarnation proof.
- **Materializing `groupState()` or whole-journal clones for range copy and snapshots.** Replaced with 128-row/1 MiB pages and streamed writers.
- **Recapturing a missing source pin from the live root.** It must never fall back under an existing cursor.
- **Calling the detailed admin snapshot under the transition scheduler lock.** That snapshot may itself observe transitions.

## Key limits and measurements

| Item | Value |
|---|---|
| Index-status list fan-out | one RPC per owner, one 5-second deadline, 16,384 index-owner observations; 32 indexes × 8 owners → 8 RPCs instead of 256 (work count, not latency) |
| Retry/repair admission | at most 128 selected owners per request |
| Expression index keys | 4,096 nodes and 4 MiB literals per index; 4 MiB per-tuple budget, 1 MiB scalar output |
| Defaults/generated | 128 nodes / 16 levels per expression; 256 targets, 4096 nodes, 4 MiB literals per set; 1 MiB per output, 4 MiB per row |
| Retained effects (REF3) | 16 consumers; frames ≤ 16 MiB and 65,536 keys; default cap 256 MiB |
| Online I/O | 32 MiB per-request ceiling; 1 MiB chunks; verifier defaults 1 MiB / 4096 work units / 5 ms |
| Constrained TTL | ≤ 128 candidates per page; 16-root, five-second coordinator slices |
| Typed scan bindings | eight-entry LRU, 2-MiB retention budget |

- ReleaseFast 256-row fixture: 768 primary records examined versus 16 indexed records for the same 16 results. After four update/drop/rebuild/GC cycles there were exactly 256 forward and 256 reverse records. One run reported 206,073 logical bytes, 104,821 active SST bytes, 214,112 obsolete bytes, and 940 WAL bytes. Logical cleanup is not immediate SST reclamation, so these numbers must not be presented as zero post-churn amplification.
- Covering fixture: 128 rows with zero primary probes, versus 128 probes for an uncovered fallback. Forward values total 16,256 bytes versus 524,288 bytes of unselected JSON.
- Tuple microbenchmark: 200 iterations over a 64 KiB payload allocate zero extra key-buffer bytes, versus 71,594,400 cumulative control bytes.
- GC locator regression: with 10,000 unrelated rows, an empty generation visits 0 records, and five memberships with one missing forward entry visit nine.
- Build/verify: 64 rows with 1/8/32 indexes take exactly 192 scan records per index.
- File cleanup: 10,000 files are reclaimed in 162 bounded pages. A 16 MiB snapshot spool is written within a 16 KiB writer budget.
- Max-size chunk encoding uses 4 KiB scratch with less than 1.5x wire expansion.

All of these are work/space counts, not throughput or tail-latency claims.

## Verification notes

- Real three-metadata/three-data cluster tests cover specific owner-crash, reply-loss, quorum-loss, and restart windows. The final executable passed all eleven cluster scenarios. These runs are not an exhaustive distributed fault matrix or power-loss testing. The full network failover/cutover/restore fault matrix remains a release gate.
- The full Debug DB transaction run hit a MachO stack-unwinder crash in the borrowed-VoprIo transaction-recovery fixture, and it is not reported as passing. The 47-case suite passes in ReleaseFast.
- The repository-wide license-header audit reported pre-existing drift (1,072 headers at one point) outside these modules.
- Historical milestone: the CHECK-only foundation was committed as `5f42cf694`.

## Open items recorded in the log

- [ ] Distributed stored-generated row rewrite jobs for ALTER.
- Online Raft merges need a Raft-ordered artifact-catalog epoch and a membership barrier before re-enabling.
- Graph/vector/algebraic artifacts and independently materialized enrichment are not in REF3, so those tables stay ineligible for online merge.
- Online schema evolution during an active source would need a durable layout-transfer protocol.
- Transferring nested native source-pin sidecars would remove the pre-publication snapshot compaction limit.
- The incoming snapshot envelope is still materialized; this is not fully streaming typed-row ingestion.
- Unsupported: constrained raw transforms, constrained active/read-schema topology changes, native relational execution in the artifact-only engine, and SET CONSTRAINTS.
- Antfarm has no visual composite relational-index editor; Raw JSON mode only.
