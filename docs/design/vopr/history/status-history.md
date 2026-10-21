# VOPR Status History

> Summary of a verbatim log. Full text: [docs/design/vopr/history/status-history.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/vopr/history/status-history.md). Originally relocated verbatim from `zig/VOPR.md` (lines 3–541, 3884–3923, 4006–4032, 4070–4106, 5368–5426, and the `### Ongoing Roadmap` body at lines 2475–2919, all at commit 271838a195) on 2026-09-16; the living design is [`VOPR.md`](../../../../zig/VOPR.md).

This log tracked VOPR (the deterministic `std.Io`/`VoprIo` simulation engine) as it grew from independent metadata, transaction, Raft, storage, HA, data-plane, serverless, and clock-fault scenarios (status 2026-08-31) into a deployment-shaped full cluster: the real metadata quorum plus three production `DataServer`/data-Raft owners over real `httpx` sockets on `VoprIo`. Each "vN" full-cluster mode adds one composed fault or workload and is promoted only when its record plus fresh-world exact replay, properties, cleanup, and leak checks all pass. Full-cluster v9 was the hosted nine-fault campaign; v11 onward run on production owners.

## Decisions

- **Promotion = complete exact-replay gate.** A mode is "integrated" only when its scenario module is forced into test discovery, its focused command executes at least one matching test, exact replay passes, and the command remains a dependency of `vopr-test`. Bounded/early-cutoff gates (e.g. v12's 2,000-transition early-cancellation gate) are lifecycle evidence only; the complete deep gate is promotion evidence.
- **Results are cited per checkpoint, not as aggregates that weren't rerun.** E.g. the 126-test runtime shard was not rerun after the merge/split repairs, a 2026-08-27 dedicated v12 rerun was externally SIGTERMed, and the `zig build -j1 vopr-test` aggregate was not green (untouched startup/configuration/upgrade/storage/VOPR CLI binaries exiting without a Zig failure stack; legacy automatic split/merge filter failures reproduce on the untouched checkpoint), so none of these were cited as fresh proof. V14 revalidated the shared active-split/graph seam instead.
- **Distributed reads fail closed.** Every incomplete fanout returns the typed retryable 503 `distributed_query_unavailable` (in the OpenAPI contract and Go SDK) with no partial payload; a successful partial 200 is never acceptable. Add an explicit partial-response schema only if product semantics ever require partial results.
- **Strong reads wait for applied ReadState plus full-index visibility.** V13 made public reads route to the current group owner and fence on an applied Raft ReadState and local full-index visibility; v19 extended the same barrier to ordinary strong derived queries. Generalize this barrier across every production strong-read owner; the type system already prevents generic managed-Raft initiation from being passed as a read barrier.
- **No blind write retry.** The workload never retries acknowledged writes and retries only known-idempotent fixed-ID upserts after bounded reads cannot resolve an explicitly ambiguous outcome; pre-proposal 503 is distinguished from post-proposal outcome-unknown 409 (v18).
- **Forward-only VOPR surfaces.** From v27 on, each mode, property, trace revision, observation set, and build target is introduced directly with no aliases, legacy readers, or migration paths "because VOPR is new code" (v27 replaced the earlier between-request authorization mode outright).
- **Scoped faults over broad outages.** The reusable `VoprIo` fault is endpoint-, direction-, and semantic-byte-stream scoped so it does not partition data-Raft or split-control traffic sharing a listener (v15); overlap modes require a dedicated active-overlap witness so faults can't pass merely by being registered together (v21).
- **Restart preserves endpoint identity.** Process-incarnation faults keep the durable root and stable advertised public/Raft ports (v16, v32, v35, v40, v44, v49).
- **Determinism-audit direction.** Expand the audit from an explicit source manifest toward the transitive call graph reachable from every borrowed-`std.Io` seam; optional/default `Threaded` owners (A2A/MCP, cloud-auth and object-store constructors, remote-provider adapters) must receive the scenario's `std.Io` before joining an exact-replay composition, and their native fallbacks are physical differential paths, not VOPR evidence.
- **Trace/observation compaction and CI tiering are operational follow-up** and never permission to weaken the completion or replay oracle.
- **Fidelity scope.** Keep the in-process multi-node mode for owners that borrow `std.Io`. Add a federated agent/broker only for a demonstrated separate-address-space or mixed-binary need; unmodified container runs are differential, never exact replay. Live mixed-version operation waits for two runnable compatible binaries and an explicit upgrade contract. A hosted graphical debugger and a container/hypervisor clone remain non-goals.

## Rejected approaches and superseded diagnoses

- **Whole-trace JSONL render for replay comparison** — replaced by walking canonical wire records directly (the former multi-gigabyte render tail).
- **"Post-acceptance Raft liveness failure" for v11** — lifecycle counters showed zero proposals accepted; the real causes were a native mutex wait pinning the single borrowed-I/O scheduler, missing internal-service fixture credentials (fail-closed middleware rejected every forwarded request), and JWT signing from host realtime. Fixes preserved native blocking semantics and fail-closed auth.
- **The choice-36,457 "166-versus-165 control round" diagnosis** — superseded by model v5 logical-owner/content connection identity plus a one-request/one-completion semaphore control handshake.
- **The v12 readiness mismatch** — traced to transition retry jitter seeded from host randomness; managed services now keep a configured deterministic salt across replacement and the fixture supplies a stable per-node salt.
- **The v9 hosted merge path as production evidence** — v9 did not route the transition through production `DataServer` owners or replicate structural actions through every receiver replica; the focused multi-owner composition and v12 closed that.
- **Lock-order reversal as a fix for address-ordered dual locking** — explicitly ruled out (would reintroduce deadlock); give each owner a stable semantic lock key or route both operations through one coordinator.

## Key measurements

Record plus fresh-world exact replay, ReleaseSafe unless noted. "Transitions" is the history budget.

| Mode | What it composes | Transitions | Gate |
|---|---|---|---|
| v11 | metadata quorum + 3 production DataServers, two tables, serverless catalog | 30,000 | 30/30 smoke |
| v12 | metadata-driven active split, cutover, post-split read | 320,000 | 15/15 (43 min, 8 GB test peak, 11 GB compile peak) |
| v13 | static depth-two public graph on production owners | — | 15/15 |
| v14 | graph during durable nonterminal split | — | 15/15 |
| v15 | + next-owner `/graph-expand` transport cut | 450,000 | 15/15 |
| v16 | + next-owner process stop/reconstruct | 650,000 | 15/15 |
| v17 | + one one-byte short write (must stay complete) | 500,000 | 15/15 |
| v18 | + all-owner memory pressure | 550,000 | 15/15 |
| v19 | public inner `_id` join across two right owners | — | — |
| v20 | durable-shuffle finalizer takeover (64 rows) | 300,000 | 15/15 |
| v21 | memory pressure + link cut overlap | 600,000 | 15/15 |
| v22 | selected-listener socket denial (`ProcessFdQuotaExceeded`) | 500,000 | 15/15 |
| v23 | reversible 2x service-rate slowdown | 90,000 | 15/15 |
| v24 | public document hydration | 90,000 | 15/15 |
| v25 | public cancellation after hydration scheduled | 110,000 | 15/15 |
| v27 | in-flight authorization revocation | 120,000 | 15/15 Debug + ReleaseSafe |
| v28 | stale source snapshot / topology-retry exhaustion | 340,000 | 15/15 |
| v29 | cancellation under scoped `/graph-hydrate` outage | 140,000 | 15/15 |
| v30 | durable-shuffle cancellation at partition worker | 360,000 | 15/15 Debug + ReleaseSafe |
| v31 | partition-worker failover | 360,000 | 15/15 Debug + ReleaseSafe |
| v32 | partition-owner process destruction | 420,000 | 15/15 Debug + ReleaseSafe |
| v33 | join retry exhaustion under memory + link | 420,000 | 15/15 Debug + ReleaseSafe |
| v34 | join cancellation under memory + link | 420,000 | 15/15 Debug + ReleaseSafe |
| v35 | cancellation then owner destruction | 420,000 | 15/15 Debug + ReleaseSafe |
| v42–v48 | production replication backfill: clean, schema-change resume, target-leader restart, provider-query failure, lease revocation, apply-before-checkpoint ownership loss, metadata source-catalog authority change | 160,000 / 180,000 / 220,000 / 220,000 / 220,000 / 240,000 / 260,000 | Debug + ReleaseSafe |
| v49 | node 1's live `ApiHttpServer` query cache under slowdown and restart | 120,000 | Debug + ReleaseSafe |

Supporting gates cited at their checkpoints: `lib-data-storage-test` 67/67 and `antfly-data-runtime-test` 125/125 (merge/data-Raft); table-read shard 61/61; table-write cache-lifecycle shard 98/98; model-v6 task/network suites 27/27; reusable engine 144/144; registered-source `vopr-determinism-audit` 13/13; full-cluster v9 passed all nine histories plus exact replays (2026-08-28). The broader API HTTP gate passed 48/49 (the unrelated 128-abandoned-query admission test missed its minimum-rejection threshold), so it was not cited as green. A 60,000-transition v12 prefix exact-replayed through the formerly divergent region before the full gate passed.

## Verification audits

The verbatim log kept four relocated audit narratives (2026-08-26 design audit and later checkpoints). Their substance: the 2026-08-26 audit forced every exported scenario module into test discovery and gave each integrated row an executing focused test, exact replay, and aggregate dependency; the v11 checkpoint became green after the three repairs above; v13's ownership audit exposed six production defects (cached explicit refresh, non-owner `UnknownGroup` routing, unmanaged direct DB open for local routes, enqueue-only ReadIndex, partial graph 200 from a lagging derived index, unconditional outer read-admission claims). Details of each defect are in [Defects Found](defects-found.md#defects-found).

## Ongoing Roadmap narrative (relocated from VOPR.md)

The roadmap as it stood at relocation (the living version is in [`VOPR.md`](../../../../zig/VOPR.md)):

1. **Deepen the full-cluster reconfiguration seam** — finish v53 (managed-index progressive readiness: provider retry, coherent coverage/replay publication, owner reconstruction, all-node semantic reads, packet-level replay stabilization; then atomic-vs-progressive policy, rate-limit/disk/cancellation overlap, alias fencing, crashes in each generation/catalog/readiness publication gap); then cancellation under storage faults and disk pressure, simultaneous process loss before cancellation drain, broader disk/socket/short-write and restart targets, coordinator/metadata/multi-owner overlap, disjoint donor/receiver placement, retained delete-history replay within a resource budget, partitions, and snapshot install that rehydrates each DB owner and its derived graph/index state. V52 already proves one selected-node persistent-cache capacity denial/healing.
2. **Deepen public distributed operations** — right/nested/foreign and multi-range-left joins, more overlapping-owner faults, and global-query topology, storage, resource, process-loss, and transport histories under the same fail-closed rule (v36–v40 established the ordered two-table global-query baseline, cancellation, revocation, link cut, and tenant-owner process loss).
3. **Compose fault domains** — co-locate HA and the data plane, add managed-index and graph/query disk pressure, broaden v22 listener targets, and extend across storage, restart, serverless lease/object-store, and multi-owner faults; overlap service-rate slowdown with ownership, deadlines, and existing faults.
4. **Broaden workloads** — v51 added two disjoint authenticated table identities; next row-level tenant scoping and identity-mutation races, remote-content secret rotation at actual request use (coherent per-request generations across retry, cancellation, refresh, crash/reopen), remote generation/reranking adapters, metadata-admin mutations, MCP/A2A, cloud-auth refresh, bounded extension invocation, and client/background fairness.
5. **Operationalize** — completed-run evidence from scheduled sharded campaigns, corpus retention/merge, quarantine review, dashboards, search-quality regression tracking; adopt the registered-deployment composer in HA, data-plane, distributed-transaction, and serverless suites.
6. **Keep distributed fidelity explicit** — see the fidelity-scope decision above.

Open item recorded in this roadmap: source-to-source cache transfer and stale-cache pruning still use address-ordered dual locking in production paths not exercised by the v12 topology; fix with a stable semantic lock key or a single coordinator before they become replayable transitions.

See [Defects Found](defects-found.md#defects-found) for the defect inventory.
