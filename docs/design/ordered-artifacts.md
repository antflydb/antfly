# Ordered artifact publication

Status: implemented behind a gate; the expanded asynchronous-artifact protocol
is **not advertised**. Installed mechanisms must not be treated as feature
activation. Online merge (see [Distributed online merge](#distributed-online-merge))
uses only the activated subset: ordered catalogs and row-derived fulltext plus
direct-field dense/sparse transfer. The dated implementation narrative is in
[the activation-gate log](ordered-artifacts/history/activation-gate-log.md).

## Invariants

- **Protocols 15 and 17 are not advertised.** Completion checklists, authored
  vector acceptance and related machinery do not enable protocol 15; the
  direct-index proof-adoption command requires data-Raft protocol 17, which is
  above the currently activated version, and no merge coordinator submits it.
- **EOF or enumeration is not completion.** Scan exhaustion, discovery
  end-of-page, census EOF, replicated baseline scan completion, queue admission,
  dispatch and transport acknowledgement never discharge an obligation, grant
  acceptance or certify a stream. Discovery cursors must wrap and revalidate
  because concurrent inserts can sort behind them. Completion is only an
  ordered, receiver-verified discharge of an exact obligation.
- **Receiver-local evidence is not portable.** Census checkpoints, local work
  revisions, projection seals, certificates and adopted receipts are bound to
  the receiver's physical-root incarnation, catalog and authority epoch. A
  sender's completed page, a copied leader certificate or transferred donor
  proof bytes never grant receiver acceptance; each receiver independently
  re-derives and verifies, and portable fingerprints exclude physical roots and
  local counters.
- **Acceptance belongs to output owners.** Receipts and source-proof references
  are issued only for outputs (including explicit deletions); read-only
  dependencies invalidate the input digest but cannot certify their own work.
  Generated-only success never bypasses native, projection, resolver or
  promotion requirements.
- **No fabricated success.** Lost producer output is regenerated from the
  pending exact-input obligation; idle-upload abandonment, retries and
  recovery never manufacture a semantic receipt or terminal success. Ambiguous
  outcomes are re-observed, never replayed.
- **Coverage authority is epoch-scoped.** Coverage markers and counters are
  scoped by namespace, producer epoch and catalog digest; replica-local legacy
  markers are never scanned into distributed authority. Missing counters in a
  populated authoritative generation stay pending.

## Mechanisms behind the gate

Publication and upload:

- Compact owned publication commands with bounded replicated upload chunks.
  Finalization authenticates chunks outside the apply lock, then rechecks the
  manifest root and begin index in the final transaction, which also commits
  effects, provenance, semantic receipts, transport acknowledgement, quota
  release and chunk retirement. Standby replay carries the small upload control.
- Transport acknowledgements use a fixed 1,024-entry durable ring; eviction
  removes only a retransmission cache entry.
- Large dispatch jobs yield after two chunks while retaining their queue
  admission and position. Producer admission reserves two job slots and 16 MiB
  for activation controls in both the local queue and the durable upload store.
- Staged uploads survive loss of their finalize job via an incarnation-bound
  receipt bitmap; recovery examines at most eight manifest/bitmap pairs and
  queues a 133-byte hint through reserved control capacity. Incomplete uploads
  are abandoned after eight idle observations spanning five minutes without
  chunk progress, compared atomically in the writer transaction.

Work discovery and retry:

- Root-producer discovery appends immutable-catalog requests to the replay
  journal atomically with a checksummed per-obligation dispatch sequence;
  root-template dispatch is paged in upstream-first order.
- Pending root streams have bounded, proof-aware retry sweeps with a
  five-minute retry grace (persisted scheduling time, awake-clock deadlines).
- Raft baseline discovery, dirty-work pages, obsolete-epoch reclamation,
  authored-acceptance retirement and the native/base-vector census use bounded
  pages of 128 keys, 64 KiB or 2 ms (one oversized key may advance) prepared
  outside apply; followers apply the selected page instead of rescanning.
- Required-work observations carry owner-local revisions; completion requires
  the current revision and authority after receipt validation in the same
  writer snapshot.

Provenance and adoption:

- Accepted proof bodies use APF3 v3: a checksummed binary record of the bounded
  logical read set, historical output CAS guards (distinct from causal inputs)
  and output digests. Certified AFB2 source-copy snapshots (reader capability
  v5) carry APF3 proofs once per publication digest with a selected-output
  bitmap; import stores them under an inert source-proof prefix, never as donor
  receipts. A durable pending bit refuses the final tail certificate until
  receiver-local adoption is implemented.
- Direct-index adoption prepares receiver-owned APF3 off-lock from remapped
  local inputs and postimages, and ordered apply revalidates inputs,
  postimages, catalog and import witness before committing adopted receipts.

Generations and scoped producers:

- Staged chunk and extraction generations have checksummed scope/input/catalog
  identities, resumable append, an atomic head switch and bounded retirement
  with a scope-bound high-water mark. A selected head (including an empty
  output) suppresses legacy tails; heads are canonical read-set guards.
- Unit producers use a root-local durable outbox with a per-document scheduling
  directory; worker turns select one child and a four-job page, maintenance
  checks up to eight jobs per child, and a checkpoint can retire up to 128
  receiver-verified receipts. Remote unit routes are rejected until the routed
  child command path exists.

Completion:

- Immutable write plans compile a catalog-wide completion checklist (native and
  authored effects, every provider template, index projections, resolver and
  promotion requirements), paged at 128 requirements/64 KiB.
- Required-stream completion is a private ordered control: every receiver
  verifies the exact before/after page against its pinned catalog and causal
  cut, then atomically commits a prefix checkpoint or the exact obligation
  discharge with the Raft applied marker and standby outbox.
- Indexed completion pins a receiver-local projection lifecycle epoch that is
  revoked by sidecar replacement, reset, configuration or watermark regression.
  Full-text replay publishes a checksummed projection seal before advancing its
  sidecar; aggregate certificates issue from a retained physical guard, and
  historical rows need a completed snapshot/replay adoption proof (scheduled
  with the `artifact_baseline_adoption` trigger).
- Vector ingress tags authored versus generated origin; current receiver-local
  authored acceptance closes the matching provider requirement without
  inference. Raw imported origin flags grant no acceptance.

## Distributed online merge

Online merge admission carries an ordered artifact catalog in the same
data-Raft command as donor admission or the receiver's initial acceptance; there
is no separate unfenced catalog-seal window. Metadata pins immutable exact
bindings for both owners; native apply validates namespace, prior catalog epoch,
source/receiver scope, integrity catalog and topology before reconciling local
materialization. Data protocol v14 and metadata protocol v20 gate the complete
ordered artifact decoder, including typed-vector transfer, before even an empty
first page is admitted. Eligible document owners automatically support
row-derived fulltext and direct-field dense/sparse indexes; graph, generated
vectors, enrichment and resolver tails remain explicitly ineligible.

All ordered eligible owners capture document-owned base embeddings, including
historical values without an active index and fulltext-only tables. Snapshot
objects preserve exact base-artifact bytes; checksum-bound, restartable chunks
carry large dense/sparse values. Retained vector effects and their Raft marker
commit together; prepared transactions reserve artifact bytes before commit.
Receiver application records positive and deleted vector effects in the derived
replay journal. Direct-field repair reads primary rows and same-snapshot
artifact fallbacks, excluding artifacts whose document no longer exists.

Sparse replacement preserves stable document ordinals using monotone
incarnations and point-addressable immutable posting/docmap sidecars; queries
filter stale postings before scoring, and compaction reserves its working set
before cloning. Native checkpoint capture waits for in-flight immutable flush
publication before certifying the source cut.

Each binding has an exact physical catalog digest and a canonical
logical-definition digest. Compatibility ignores insertion order, absent versus
serialized-empty catalogs, JSON key order and owner-local coverage generations;
configuration differences reject admission. Each owner retains its own ordered
generation, so a healthy receiver does not rebuild merely because the donor
assigned different generations.

Replica-local drift does not determine the replicated decision. After semantic
preflight, native apply persists an exact reconciliation intent and retries
bounded work until the intended catalog is physically ready. The intent fences
unrelated schema, artifact, topology and row-policy publication; private
reconciliation contexts permit only monotonic changes toward it. Obsolete
resolver artifacts use a durable, restartable 128-key cleanup cursor. Final
admission, ordered authority, applied receipt and intent/cursor removal commit
atomically. Failed or repair-unavailable indexes and outstanding cleanup cannot
certify readiness. Portable export, backup capture and restore reject a pending
intent; catalog-preview changes before cutover durably cancel the attempt.

Online merge runs through the existing merge workflow; there is no separate
public online-merge command. `antfly metadata --online-merge-enabled false`
(default `true`; `ServerConfig.online_merge_enabled`) stops new online merges
on that metadata process while keeping the recovery driver, so admitted merges
can finish or durably cancel and release their fences. The adapter requires
the compiled native bundle, non-serverless mode and internal service
authentication; unsupported deployments keep the guarded ordinary workflow.
The online-merge admission guard is a property of upgraded metadata admission
and data proposal processes, not a fleet-wide feature flag during a rolling
upgrade: an older metadata leader paired with an older data leader can still
start an online attempt. Disable online admission on every metadata process or
drain old leaders before relying on the guarded ordinary fallback across the
fleet; replay of already-committed attempts remains supported
([history](relational/history/indexes-extraction.md)).

Open: a repeated full mounted fault matrix (one mounted fault case passes), and
compaction/selective-query performance comparison for sparse replacement.

## Checks

Focused checks (from `zig/`, with `-Doptimize=debug`):

- `zig build antfly-ordered-artifact-test`
- `zig build antfly-retained-transfer-test`
- `zig build antfly-data-runtime-test -- --test-filter 'data runtime ordered artifact upload handoff'`
- `zig build antfly-storage-native-fk-test`
- `zig build antfly-standalone-initial-fk-test` (the six native FK/TRUNCATE
  integration cases require this storage-owner-linked target, not the general
  standalone runtime unit-test target).

## Activation checklist

Before advertising the expanded protocol, finish and validate:

1. Enumerate and schedule every required producer stream from the immutable
   catalog, including unit/chunk scope and shared-output reconciliation. Clear
   each exact-input obligation only after those streams are complete. Bounded
   receiver-verified completion controls and local work-driver submission are
   wired; complete the missing requirement verifiers and the all-member drain/seal
   coordinator (bounded replicated provenance validation is wired). Replicated baseline scan
   completion alone is not an activation certificate.
2. Complete chunk/unit/document-extraction, neighbor-context asset, and promotion
   producer preparation and actual callbacks (root chunk callbacks with an
   existing or receiver-verified reconstructed inventory, and direct/accepted-upstream asset callbacks are wired),
   including remaining input proofs and absence cleanup. Direct document-vector
   authored certification now bypasses inference; historical/adopted authored
   output still requires receiver-local adoption evidence. Staged output sets
   beyond the atomic command limit (large staged outputs) remain required before
   wiring the extraction worker ([history](ordered-artifacts/history/activation-gate-log.md)).
3. Historic provenance carried with retained effects and snapshots, atomic
   receiver-local adoption certificates, and contender/promotion recovery.
   The direct-index ordered adoption participant is wired and tested locally,
   but its bounded coordinator, distributed retry outcomes, other producer
   families and admission/activation barrier are not complete. Graph effect
   rebinding and promotion identity on adoption, retained-tail provenance, and
   bounded reclamation of abandoned merge evidence also remain open as of the
   activation-gate log ([history](ordered-artifacts/history/activation-gate-log.md)).
4. Complete scoped regeneration through the required-stream driver and all-member capability/catalog
   barriers, followed by the distributed crash, lost-reply, leadership-change,
   and standby-promotion fault matrix.
   Root retries now derive identity from the existing exact-input obligation
   and immutable plan, not from transport staging. Finish regeneration for the
   remaining scoped producers and prove it across incomplete/retired uploads,
   membership changes and standby promotion. A retry is not stream completion
   or all-member evidence, and must not replace those barriers.
