# VOPR Full-Cluster Scenario Narratives

> Summary of a verbatim log. Full text: [docs/design/vopr/history/full-cluster-scenario-narratives.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/vopr/history/full-cluster-scenario-narratives.md). Originally relocated verbatim from `zig/VOPR.md` (lines 878–1148, 1579–1607, 1729–1808, and 1858–1890 at commit ada708f739) on 2026-10-12.

Long-form narrative of the production-owner full-cluster VOPR scenario versions (v11–v53), the timer/deadline defects found by the transport determinism gate, the original per-version gate tiers, and a superseded v13–v18 recap. Durable decisions were folded into [VOPR.md](../../../../zig/VOPR.md) before the move: each scenario version is described once in its Full-Cluster Scenario Versions table, binding per-mode contracts are listed beneath it, and borrowed-clock invariants are under `VoprIo` Runtime.

## Per-version production-owner narrative, v11–v53 (originally lines 878–1148)

- **v11–v23:** static public/serverless baseline behind the live metadata quorum (v11); real hosted transition path (v12), whose complete 320,000-transition history passes record, fresh-state exact replay, properties, cleanup, and leak checks once transition retry jitter became a stable per-node deterministic input; production-owner graph (v13), overlap with active split (v14), next-owner graph-stream cut (v15), next-owner process + listener reconstruction (v16), scoped short write with stream resumption (v17), all-owner memory pressure (v18), cross-owner join and durable-finalizer takeover (v19/v20), link-plus-memory and listener admission faults (v21/v22), reversible per-node/per-operation service rates (v23).
- **v24–v35:** public hydration, cancellation, in-flight authorization mutation, stale-snapshot retry exhaustion, cancellation under a matched transport outage; then durable-worker cancellation, same-partition failover, partition-owner destruction/reconstruction, retry exhaustion plus cancellation under overlapping resource + link faults, and cancellation followed by worker-owner reconstruction.
- **v36–v40 (global NDJSON query over `/db/v1/query`, tables `docs` and `tenant_b_docs`):** v36 requires exactly two flattened responses in request order with exact disjoint ID sets; v37 cancels after the first table result; v38 revokes `tenant_b_docs` read permission mid-request (exact 403, one assembled result, no protected payload); v39 cuts only the internal `tenant_b_docs/query` stream (exact retryable 503); v40 destroys the `tenant_b_docs` owner process and reconstructs it with a fresh incarnation. Rule: every incomplete public graph, join, or global-query operation fails closed without partial rows, and every faulted history heals and proves a complete fresh request.
- **v42–v48 (production replication runner composed with the deployment, not copied into the harness):** clean snapshot-to-stream path (v42, 160,000 transitions); schema change + duplicate resume querying `users_v2` with exactly four accepted batches (v43, 180,000); target-leader crash/reconstruction, five attempts / three successes (v44, 220,000); source-session `ConnectionResetByPeer` with generations 1/2 and three sessions (v45, 220,000); durable-checkpoint lease revocation returning `CdcWorkLeaseLost` with no duplicate committed batch (v46, 220,000); stale owner losing its lease between target apply and checkpoint publication — the snapshot runner now revalidates `WorkPermit` ownership there — giving one exact replay and four successes (v47, 240,000); source-catalog/authority rotation through metadata Raft returning `ReplicationSourceConfigChanged`, claim B against A, and retiring A (v48, 260,000, production table 6841).
- **v49:** production query-embedding cache owned by node 1's live `ApiHttpServer`, coalesced waiter expiring at its logical deadline under the 2x slowdown, then reconstruction with empty cache, exactly one new computation plus one retained hit, and one stale pooled-connection failure (120,000 transitions). It does not claim a complete public semantic HTTP request or an external inference provider executed.
- **v50:** serverless generation/publication-progress fencing via one production-neutral builder hook (unset in production) after candidate manifest durability and before the guarded progress CAS: candidate 2, cutover 3, losing path returns `HeadChanged`, final head 4, with the ordinary workload running on the same `VoprIo` (120,000).
- **v51:** authenticated multi-tenant isolation with two disjoint Basic identities; cross-table reads/writes return exact 403 and owning-identity lookups exact 404 (120,000).
- **v52:** node 2 disk capacity (2 GiB baseline, above the `ResourceManager` 1 GiB safety floor) lowered to zero under the persistent object-range cache worker, then restored for one reservation/write/release cycle (120,000). Promotes the persistent-cache reservation seam only, not managed-index repair under disk pressure.
- **v53 (not complete):** managed-index publication/reconstruction seam; the 35,000-transition gate exact-replays the bounded lifecycle, but the intended suffix (three indexed documents, node 1 reconstruction, query through all three nodes) is not promoted because its deep run exposes packet-level replay divergence around Raft status publication. Stabilize that gate before adding crashes in candidate/catalog/alias publication gaps or managed-index disk-pressure overlap.
- Each named complete gate (v42–v52 dedicated Debug and ReleaseSafe) passes 15/15 at its cited checkpoint. V42–v53 add modes directly with no legacy aliases, readers, or migration paths.
- Stated non-coverage: metadata/coordinator restart, multiple simultaneous owner failures, process loss composed with storage/resource/partition faults, disjoint placement, derived-state equality, bounded retained-history replay, snapshot-install rehydration, and co-resident v9 fault breadth.

## Timer and deadline defect narrative (originally lines 1579–1607)

Defects found by the CI gate running production Raft transport tests and the determinism audit:

- **HTTP frame queue host-time deadlines.** A bounded-fair standby/scaling history diverged because retry deadlines and jitter came from host time while sleeping on borrowed I/O. Fix: retry readiness and snapshot-transfer deadlines use the owning I/O's monotonic clock; HTTP host construction propagates that authority to route reconciliation, admission retries, policy rechecks, and bootstrap timestamps; snapshot staging names take entropy from that I/O instead of host time and a process-global counter.
- **Queued Raft delivery dropped the request timeout.** A node-status request waiting inside a queued Raft batch became unbounded when socket timeouts were disabled. Fix: preserve the timeout when taking ownership of the copied payload; queued requests do not retain the enqueuer's borrowed cancellation or delivery-tracker pointers. Regression uses a real never-responding HTTP peer on virtual time.
- **DataServer maintenance worker spin.** Readiness checked virtual time but vector publication kept its deadline in host time, so advancing virtual time past host uptime spun the outer loop. Fix: use the borrowed monotonic clock; every bounded attempt (including idle and failed) establishes a retry boundary and releases its reservation before waiting; accepted wakes survive the backoff.

## Production-owner gate tiers by version (originally lines 1729–1808)

Superseded by the gate column of the scenario-version table. The gate was tiered by deterministic work, not by a weaker oracle:

| Target | Scope |
| --- | --- |
| `production-cluster-vopr-smoke-test` | v11 exact replay + v12 bounded lifecycle; included in ordinary `vopr-test` |
| `production-cluster-vopr-deep-test` | complete 320,000-transition v12 history |
| `production-cluster-graph-split-vopr-test` | v14, 400,000 transitions |
| `production-cluster-graph-split-transport-vopr-test` | v15, 450,000 |
| `production-cluster-graph-split-owner-restart-vopr-test` | v16, 650,000 |
| `production-cluster-graph-split-partial-write-vopr-test` | v17, 500,000 |
| `production-cluster-graph-split-resource-pressure-vopr-test` | v18, 550,000 |
| `production-cluster-vopr-test` | every focused production-owner gate through v51, serialized by the build graph's inherited stdio lock |

Record and fresh-state replay, properties, cleanup, and enabled-set equality are identical in every tier; the split keeps a multi-gigabyte, tens-of-minutes witness out of the default edit loop. Exact replay compares canonical wire records structurally and renders only the first mismatch, so the deep gate no longer retains two complete JSONL artifacts.

## v13–v18 completion recap (originally lines 1858–1890)

Superseded: it stated that cancellation under resource faults was not yet composed into the production-owner history, but v33 and v34 later composed join cancellation and retry exhaustion under overlapping resource and link faults. It recorded that the roadmap was not empty, that the system is not equivalent to the Antithesis hypervisor, that no single trace co-resides every standby/data-plane/serverless owner, that live mixed-binary operation is not modeled, and that arbitrary unmodified sidecars or process address spaces remain differential/integration concerns.
