# VOPR Follow-ups (2026-09)

> Summary of a verbatim log. Full text: [docs/design/vopr/history/follow-ups-2026-09.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/vopr/history/follow-ups-2026-09.md). Originally relocated verbatim from `zig/VOPR.md` (lines 3236–3852, 655–692, and 3150–3177 at commit 271838a195) on 2026-09-16 during the documentation cleanup.

A sequence of focused runtime-correctness and production reviews on 2026-09-06/07, each reproducing defects and fixing them with regressions, plus the metadata planning fixes and the first soak-investigation findings. Durable decisions were folded into [`VOPR.md`](../../../../zig/VOPR.md) before the move. Recurring caveat in every entry: validations were local focused gates, not fresh aggregate `vopr-test` or remote CI certification.

## Standing rules established

- Passing replay tests alone does not establish std.Io contract conformance: a wrong runtime behavior can replay exactly.
- Model/scenario/ABI version bumps require fresh histories; old histories and transient contexts are rejected, not migrated (VoprIo model version 8; private native ABI versions 4, 5, 6; composed full-cluster scenario version 54; DB/index managed-readiness scenario version 2).
- `runtime_io_abi.zig` is a private, version-checked, same-toolchain bridge that translates errors by name across compilation units — not a general cross-version Zig ABI.
- Shutdown at the public batch boundary returns the conservative `WriteOutcomeUnknown` (HTTP 409): it neither claims nothing committed nor invites a blind retry.
- Cross-clock deadlines are translated at boundaries (native `CLOCK_MONOTONIC` vs the borrowed executor's `.awake` clock); an expired request can never acquire a fresh budget. This is boundary translation, not a claim that query execution is driven by VoprIo.
- Remote join clients classify HTTP 504 as `Timeout`, like 408.
- The planner rejects disk sizes without `disk_bytes_known` as inconclusive; unknown size and known-zero size with live documents stay fenced.
- Neither harness fix relaxes LSM's single-writer guard; initial seeding uses a read-only DB view.

## Defects found and fixed

### Runtime correctness review (2026-09-06, at `6fe629d2e`)

- **P1 cross-archive executor error identity:** raw `IoBorrow` vtables turned `FileNotFound` into `WriteFailed` in a separately compiled consumer; fixed by dispatching through the owning compilation unit.
- **P1 group cancellation skipped ownership cleanup:** queued callbacks now enter with cancellation pending and run their cleanup defers.
- **P2 group argument alignment:** alignments above 16 bytes are rejected before concurrent ownership transfer; `Group.async` uses its eager fallback.
- **P1 protected waits completed early on cancellation:** protected sleep and uncancelable futex waits now reach their real deadline/wake.

### Production reviews (2026-09-06)

- **Restart-safe read identities:** applied reads and ReadIndex requests carry a fresh 128-bit runtime incarnation plus a non-reused counter, so delayed pre-restart responses cannot complete new waiters.
- **Retention/publication ordering:** retention reads published HEAD before listing manifests, avoiding false `PublishedHeadManifestMissing`.
- **UserManager executor ownership:** retains a `runtime_io_abi.Borrow`, not a foreign raw std.Io.
- **HTTP client final-release lifetime:** the last closed request stays counted until it owns the drain mutex.
- **Retention increases after GC:** a durable monotonic `MANIFEST_GC_FLOOR` (separate from the WAL watermark, since publications can share a WAL position) stops increased retention at retired history instead of treating it as corruption. Not fencing for overlapping pruners.
- **Shared recovery ownership:** identity visibility moved to the heap-owned DB core shared by serving writes, transaction recovery, and TTL; recovery borrows the enrichment runtime only per resolution under a shared provider mutex.

### Readiness and replay root causes (2026-09-06–07)

`production-cluster-join-split-vopr-test` had reproduced a 175-byte packet payload replay divergence followed by `VoprIoTeardownStalled` (also at pre-fix `91dab5df73`; choice 11221 vs 11227). The investigation found distinct faults rather than an opaque packet-replay limit:

- Managed readiness fixture never ran repair source discovery and disabled the canonical index worker (now 12/12 DB/index tests).
- A manually built backend omitted its filesystem I/O capability.
- Routing used host-clock elapsed time in `X-Antfly-Routing-Remaining-Ms`, so identical choices produced different packet bytes; internal routed lookup ingress also used host-clock deadlines.
- Six Raft snapshot-sender tasks stayed in uncancelable waits during abort teardown.
- Broadcast-join oracle wrongly required a shuffle worker-attempt ledger.
- Shard I/O re-entered metadata apply and replaced the transition array under a retained pointer (use-after-free during split-request serialization).
- Fixture advanced election ticks at its 1 ms control quantum instead of production's 100 ms Raft cadence; the old hot loop spent over 156,000 transitions advancing only eight virtual seconds.
- Unclassified `ClientShuttingDown` at the public batch boundary (now `WriteOutcomeUnknown`).

Outcome: complete join/split histories and fresh-world exact replays pass in Debug and ReleaseSafe within the unchanged 420,000-transition budget (16/16). Not a certification of v53 managed publication or all transitive callbacks.

### Query deadlines (2026-09-07)

- On Darwin the native and borrowed clocks differ: the pre-fix binary returned immediate 504s for one- and two-shard queries with 1 s or 5 s budgets (observed offset approximately 11 s). After the fix, the reproduction returned 200 for 1 s, 5 s, and 30 s budgets (5–6 ms observed).
- The same mismatch affected joins, multi-shard graph, and semantic queries (`query_timeout` at 1 s, passing at 30 s); fixed with translation in join contexts, graph workers, and semantic planning.
- Enrichment provider contexts now receive translated native deadlines.
- Unrepaired observation: a graph source document containing only `_edges` stalled a `full_index` batch with repeated `ReplayDocumentNotVisible` retries.

### Merge correctness (2026-09-07)

- **Import lock scope:** apply locks were released by `if/else`-scoped `defer`s before copying; now released at function scope.
- **Artifact preservation:** replicated merge copied only primary JSON (losing graph and explicit dense/sparse inputs) and bootstrap replayed historical requests over a current snapshot. Now bootstrap replaces the receiver slice and copies current artifacts; private `_merge_artifacts` commands under durable protocol v5; export pages stop at 128 rows or approximately 1 MiB.
- **Copy fencing:** copy effects accepted only for the exact active receiver transition (`MergeCopyFenced`); finalized/rolled-back receivers admit fresh accepts; retired transition IDs are durable.
- **Copy attempt fencing:** a same-transition failover window let an old donor's delayed clear delete data; each copy now opens a durable `begin_copy` checkpoint with a `(donor_term, sequence)` token ordered term-first (Raft batch protocol v6).
- **Remote control routing:** endpoint validation required the receiver group while controls route to the donor leader (all four remote actions returned HTTP 400); terminal retries now fold to durable no-ops instead of `ConflictingMergeTransition`.
- **Terminal retries after split-start:** live range was compared before recognizing a terminal receipt (`MergeRangeStateMismatch`).
- **Receipts through physical split:** `raftmerge:state` sorted after document keys and was moved to the child; receipts now live in the protected system-metadata prefix. This also exposed L0 run IDs assigned newest-first during physical split rewrites (older writes/tombstones outranking newer data); now oldest-first.

## Metadata planning fixes (2026-09-07)

Rename validation exposed four failures that also reproduced at merged pre-rename commit `6d21a583f`: the candidate-report fixture omitted `disk_bytes_known`, and split polling reopened/reseeded the apply store while the active coordinator owned it. Observation now reads through the coordinator; the terminal record (not an in-memory flag) is authoritative; initial snapshot uses `seedGroupSnapshotFromAuthoritativeStoreIfAbsent` with typed range bounds. Virtual-transport gate 12/12, smoke 5/5, seeded campaigns 2/2, public HTTP 4/4.

## Soak investigation findings

At base seed `0xa17f5500`: a Raft oracle failed after a crash discarded an unpersisted term — version 2 checks volatile monotonicity within an incarnation and durable term/commit across restarts, matching the async Raft storage contract. The distributed-data scenario wrongly required retirement on all three replicas while one was partitioned — version 2 verifies the surviving quorum first, heals, then verifies every replica. An uncertain reconcile-lease proposal stays pending until committed authority is observed. That scenario's four-choice trace is not packet-level replay of the production cluster.
