# Ordered artifact activation gate: implementation log

> Summary of a verbatim log. Full text: [docs/design/ordered-artifacts/history/activation-gate-log.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/ordered-artifacts/history/activation-gate-log.md). Originally relocated verbatim from `docs/sql-extraction.md` (lines 2427–3766) at commit 3ce5aff4ea on 2026-10-12; durable decisions were folded into [Ordered artifact publication](../../ordered-artifacts.md) before the move.

This log records, mechanism by mechanism, what was installed behind the ordered-artifact activation gate. Throughout, the expanded asynchronous-artifact protocol stayed **not advertised**: protocol 15 stayed disabled (completion checklists and authored-vector acceptance do not enable it), and the direct-index proof-adoption command needs data-Raft protocol 17, which is above the activated version. Counts and test totals are as of the time of writing, not current guarantees. The current activation checklist lives in [ordered-artifacts.md](../../ordered-artifacts.md#activation-checklist).

## Recurring principles
These rules come up again and again in the log and explain most of the design choices:
- **Admission, scheduling, dispatch and EOF are not completion.** Queue admission, scan exhaustion, census EOF, a retry, or a sender's completed page never discharges an obligation, advances replay, or grants acceptance. Missing evidence stays pending.
- **Receivers verify independently.** Followers apply a page the leader selected, but they recompute or verify it against their own pinned catalog and causal cut. A sender-supplied bit, a copied leader certificate, or donor bytes never grant acceptance. Portable fingerprints leave out physical roots and local counters, and each receiver checks its own local fences when it commits.
- **Do work off-lock, check fences in the writer.** Variable-size preparation (decoding, hashing, key construction, proof validation) happens outside the apply lock. The final transaction does only fixed-size point checks and CAS.
- **Bounded pages.** Scans and reclamation are capped at 128 records/keys, 64 KiB or 2 ms, with a single oversized item allowed through. Snapshots are released before dispatch. Cursors are fair and wrap, because inserts can land behind a cursor.
- **Local evidence is not portable.** Receiver-local checkpoints, revisions and census records are bound to the physical-root incarnation. They must never be adopted as remote certificates.

## Mechanisms installed (grouped)
- **Upload/publication transport:** compact owned commands, bounded replicated chunks, and a final transaction that rechecks manifest root and begin index. Transport acks live in a fixed 1,024-entry durable ring; evicting an entry drops only a retransmission cache entry. Large dispatch jobs yield after two chunks. Producer admission reserves two job slots and 16 MiB for activation controls. Staged-upload recovery examines at most eight manifest/bitmap pairs and queues a 133-byte hint. The idle-upload detector uses eight observations and five minutes without progress. The root-retry grace period is five minutes.
- **Authority and coverage:** coverage markers and counters are scoped by namespace, producer epoch and catalog digest. Replica-local legacy markers are not scanned into distributed authority. Dirty work is keyed by namespace and authority epoch. Authority activation captures a checksummed, immutable replay-boundary cut, which is not rebuilt from today's journal tip.
- **Provenance proofs:** accepted proofs stage a document-ordered reference atomically. Proof bodies use **APF3 v3**, a compact checksummed binary format chosen because JSON's binary-key expansion meant one legal proof might not fit within the AFB2 block ceiling. Output CAS guards are kept separate from causal inputs. Certified AFB2 snapshots carry APF3 proofs (reader capability v5 rejects older decoders). Online merge carries them as inert, source-pin-scoped evidence, and a durable pending bit refuses the final tail certificate until receiver-local adoption exists.
- **Receiver adoption (direct index only):** preparation recaptures inputs, checks output digests and remaps producer generations. A private data-Raft command (protocol 17) commits adopted receipts atomically. No merge coordinator submits it yet.
- **Required-stream completion:** immutable write plans compile a catalog-wide checklist of native effects, provider templates, index projections, resolver and promotion requirements, paged at 128 requirements/64 KiB. A private ordered completion control commits prefix checkpoints or an exact obligation discharge. Closure handles exist for native/base vectors, document-wide dense/sparse, root chunks (AVP2), singleton assets, extraction-owned scopes, unit children and full-text projections.
- **Chunk/unit/extraction storage:** staged generations with atomic head switch, incarnation high-water marks, bounded GC, extraction named-output directory, unit-child reconciliation registry, and scoped unit job outbox with per-document scheduling directory. The worker checkpoint commits up to 128 retirements per transaction. Generated replay wakes run a four-job page per child.
- **Full-text projection evidence:** a checksummed projection seal in the index metadata is published before the applied-sequence sidecar. Aggregate certificates use one slot per index. Shadow snapshot/replay builds persist an adopted baseline in the seal. A source-gap epoch invalidates candidates after unjournaled writes. Adoption work is scheduled through the `artifact_baseline_adoption` trigger.
- **Authored vectors:** a tagged origin replaces inferring authorship from a missing source hash. Receiver-local authored acceptance is issued in the same transaction as the source revision stamps. Accepted authored vectors skip provider inference.

## Decisions
- Keep transport acks as a bounded retransmission cache. The durable semantic receipt/guard path remains authoritative.
- Finalization authenticates chunks outside the apply lock, then rechecks the manifest root and original begin index in the final transaction. Standby replay carries the small upload control, not another copy of the publication.
- Repair lost root requests through proof-aware retry sweeps with their own durable cursor. This avoids needing an extra transport-to-document reverse index or a mass dispatch reset. Persisted scheduling time stops cold-owner churn from resetting the retry grace.
- Strict current-stream verification is kept separate from retry acknowledgement. Shared graph winner/count outputs can legitimately be superseded, so they are reconciled against the current accepted projection instead of being re-inferred. Private contender and stream outputs stay revision-exact.
- Acceptance receipts and source-proof references belong only to output owners. Read-only dependencies invalidate the input digest but cannot certify their own work.
- Unit chunk input capture binds the immutable extraction template and does not invent a competing top-level worker template. A missing unit with a live primary requires accepted upstream deletion provenance and is not inferred from raw absence.
- Scoped unit identities live in the durable outbox in the same transaction as the replay wake. This is needed because the thin worker journal coalesces generated references into document keys, so putting unit fields on those references would silently drop the scoped work identity.
- A remote unit route is rejected before chunking or publication until a routed child command path exists. Callers cannot supply an unverified route for a durable job.
- Generation heads are canonical read-set guards, not generic artifact-mutation authority. A selected head, including an empty one, never falls back to obsolete physical rows.
- Census EOF requires a current accepted root-chunk manifest, including explicit empty output. A selected generation head cannot borrow an older manifest's acceptance.
- Identical provider definitions shared across consumer routes coalesce into one requirement. Distinct provider inputs never coalesce. Cursor fingerprints cover catalog bytes and the full requirement set.
- Materialization revisions grant a replay boundary only for a fresh all-effects journal append. Unjournaled, imported or ambiguous changes keep an explicit absent boundary and never borrow another document's journal tip. The PR-only materialization record has one strict versioned encoding with no legacy decoder.
- Full-text rebuild durably publishes a rebuilding checkpoint and restart marker before the destructive reset, and publishes clean only after backfill and sync. Split pruning revokes both primary evidence and the physical seal, because mixed-segment replacement is not equivalent to ordinary compaction.
- Identical dense projection replay preserves the ingress artifact instead of rewriting it. Rewriting would invalidate its source revision and cost a redundant vector WAL write.
- Normal publication waits no longer log warnings, inflate error counters or inherit exponential provider/pipeline backoff.

## Rejected approaches
- **JSON proof bodies:** binary-key expansion meant one legal proof might not fit within the AFB2 block ceiling, so they were replaced by APF3.
- **Serializing standard-library hash internals for resumable manifests:** replaced by a resumable hash chain in the new, unreleased manifest.
- **Inferring vector authorship from a missing source hash:** rejected because multimodal provider output can also lack a hash. Flagless historical or imported vectors are not implicitly upgraded to authored.
- **Treating a transport receipt as retry evidence:** generated retry checks require current provenance, because a receipt can outlive its output.
- **Replaying a donor's pre-publication output guard on adoption:** the receiver must compare the imported postimage instead.
- **Reconstructing missing/corrupt boundary evidence from the current journal tip, or substituting a global journal tip for unrecorded source boundaries:** rejected. Historical credit requires a completed snapshot/replay adoption proof.

## Key measurements
| Item | Value |
| --- | --- |
| Ordered-artifact target (APF3 codec) | 206 cases pass |
| Ordered-artifact / retained-transfer (AFB2 proofs) | 208 / 28 cases pass |
| Ordered-artifact target, later milestones | 196 → 197 → 198 → 199 → 200 tests, zero failures or leaks |
| Completion engine fixture | 257 requirements |
| Authored-acceptance retirement fixture | 257 certificates |
| Native census fault fixture | 130 dense/sparse vectors |
| Online merge restart test | three 1 MiB chunks |

Focused checks (from `zig/`, `-Doptimize=debug`): `zig build antfly-ordered-artifact-test`, `antfly-retained-transfer-test`, `antfly-data-runtime-test -- --test-filter 'data runtime ordered artifact upload handoff'`, `antfly-storage-native-fk-test`, and `antfly-standalone-initial-fk-test` (needed for the six native FK/TRUNCATE integration cases).

## Open at time of writing
The log ends with the same four-item "before advertising" list that is now in [the design doc's activation checklist](../../ordered-artifacts.md#activation-checklist):
1. Required-producer-stream enumeration from the immutable catalog, plus the missing requirement verifiers and the all-member drain/seal coordinator.
2. Chunk/unit/document-extraction, neighbor-context asset and promotion producer callbacks.
3. Historic provenance carried with retained effects and snapshots, receiver-local adoption certificates, and contender/promotion recovery.
4. Scoped regeneration, all-member capability/catalog barriers, and the distributed crash/lost-reply/leadership-change/standby-promotion fault matrix.

Other gaps named in the log: graph effect rebinding and promotion identity on adoption, retained-tail provenance, bounded reclamation of abandoned merge evidence, dynamic child-range inspection and routed remote publication, large staged outputs beyond the atomic command limit, index kinds other than full-text, and separate time slicing for publishing-pass scheduling.
