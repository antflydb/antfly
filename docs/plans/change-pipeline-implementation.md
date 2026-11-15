# Change pipeline implementation ledger

This tracks implementation of #1698 and #1639 on `feat/change-pipeline` (draft
PR #1700). Both issues remain open until all required implementation and
qualification are complete. The shared proposed design is in PR #1699 on
`docs/change-pipeline-contracts`, at `docs/design/change-pipeline-contracts.md`.

## Required outcome

Implement destination integration, retained committed history, public changefeeds,
consistent bootstrap and bounded retention, actual pgwire logical replication /
pgoutput, and qualification with a pinned existing PostgreSQL CDC client. Do not
substitute polling SQL for the replication protocol or claim root-local history
alone establishes cross-shard transaction completion.

## Existing prerequisite (71151ee55f)

- Shared source transaction envelope, preserving lake retry identity.
- PostgreSQL polling only exposes complete source transactions.
- CDC progress advances after the whole source transaction applies.
- Unsupported mutation messages fail rather than silently advance progress.
- The PostgreSQL parser now retains update before-images and old-image kind,
  identity-key column flags, and before/after completeness. Key-only NULL
  placeholders are removed from non-key fields; unchanged TOAST remains absent
  rather than becoming SQL NULL. Tuple/schema column-count and trailing-byte
  mismatches fail before granting a checkpoint. Value ownership covers both
  images and malformed tuple error paths.
- Expanded parser checks cover changed keys, full old rows, NULL versus unchanged
  TOAST, delete old-row identity and malformed tuples. The full
  `source-transaction-test` target passes; current log:
  `zig/source-before-images-final-test.log`.
- `zig build source-transaction-test -j2` passes native CDC, parser, envelope,
  retry and real Iceberg row-commit checks.

## In progress: native committed history

New implementation files:

- `storage/committed_frame.zig`: bounded versioned before/after tuple frames,
  namespace/transaction/schema identity, authenticated chunk descriptors and pull
  decoding. Standalone frame corruption/cross-chunk test passes.
- `storage/committed_history.zig`: primary-transaction history, persistent consumer
  epochs/acknowledgments, bounded reclamation, before-image capture, and durable
  reservations for transaction intents. No public activation is exposed yet.
- `storage/transaction_intent_value.zig`: extracted shared intent codec and exact
  normalization size accounting, used by transaction and history reservation.
- `storage/docstore.zig`: captures both writer APIs before overwrite and stages
  history before physical commit, alongside existing source authority/revisions.

New `zig build docstore-history-test -j2` target (also registered in unit coverage).
Four storage/transaction tests and the standalone frame test pass: coalesced
images/rollback, reserved quota/ack/reclaim, consumer epoch fencing and shared
retention, and real transaction-manager prepare/reopen/resolve/idempotent recovery.
The 56-case transaction/reservation suite also passes. Current logs:
`zig/committed-history-final.log` and `zig/committed-history-regressions.log`.

Descriptors index every event ordinal and authenticate each immutable chunk, so
resuming late in a transaction does not scan its row prefix. The reader owns one
reusable chunk buffer; returned tuple/schema fields belong to the caller's page
arena. Consumer removal preserves outstanding prepared credits through resolution;
the last consumer cannot invalidate an already prepared commit.

Capture uses the original transaction ID when resolving durable intents. Native
source clock advancement reuses existing authority advancement or advances once
when history is the only consumer. Raft roots require an observed same-transaction
ordered apply marker. Inactive stores cache absence without copying rows.

Important next checks before exposure:

- Exercise real DB row/relational/SQL, cascades, TTL and rollback/savepoint paths.
- Cross-shard completion must be coordinated from durable transaction decisions;
  individual root frames are not complete global transactions.
- Source movement, split/merge/restore and root incarnation fencing need history
  transport/retention capability qualification and protocol gating.
- Root history is private storage infrastructure. Authentication, live policies,
  expiration, snapshots, slot LSN ordering and public cursors are still separate.

## In progress: durable native source admission

- Private `storage/source_admission.zig` defines source binding, immutable
  batch identity, a monotonic source sequence separate from provider checkpoints,
  canonical participant-effect digests, and bounded durable receipt windows.
  Progress heads and receipt rings are isolated by table/source identity; epoch
  or generation changes cannot implicitly abandon the previous binding.
- Source head, receipts, and ring eviction compile to ordinary native transaction
  intents with an exact source-head predicate. Rows and source progress commit
  together; a receipt is never a best-effort second write.
- Retried committed batches resolve before current-schema validation or transform
  expansion. Pending intent receipts also fence changed payloads before recovery.
  Expired receipts reject old source sequences even when opaque checkpoints cycle.
- Metadata writes are protected from ordinary batches and excluded from row
  preparation and row history; trusted source admission is the only compiler.
- Native engine and runtime error boundaries preserve source/history outcomes.
  All 56 runtime error ABI tests and the bounded receipt/pending-replay test pass.
  The new `zig build source-admission-test -j2` target exercises the bound
  destination through native transaction callbacks. The expanded native run
  passes receipt eviction, acknowledgment, reopen, exact retry, conflicting-payload
  rejection, private-key protection, repeated prepare, and a real prepared
  transaction across reopen. Prepared source retries resolve the existing vote
  rather than expanding transforms again; a conflicting retry cannot abort it.
  Transaction read validation excludes the transaction's own pending intents,
  while retaining version checks and conflicts with other writers.
- `zig build source-admission-test source-admission-codec-test
  docstore-history-test docstore-transaction-reservations-test -j2` passes:
  two bound native integration tests, bounded codec/replay tests, four retained
  history tests, and all 56 transaction/reservation regressions. Final local log:
  `zig/source-admission-final-recovery.log`. `make gc` completed safely.

The source command is currently private. Distributed routing/anchor ownership,
replicated transport, managed connector invocation, and writable-Iceberg adapters
still require integration; this does not advertise those destination capabilities.

## In progress: transaction-wide source projection and destination apply

- `storage/source_effects.zig` owns a bounded source-ordered projection plan.
  Full deletes remove preceding transforms for that key; later transforms retain
  their individual upsert behavior against the deleted base. Limits count all
  input operations, including superseded work, and rejected partial plans cannot
  be submitted. Final effects and their bytes belong to the plan arena.
- Managed streaming compiles every member of a complete provider transaction
  before the first destination mutation, then calls canonical `commitBatch`.
  This is required even for one destination table: ordinary routed table batches
  can fan out sequentially across shards. Unsupported transaction capabilities
  fail before falling back to any non-atomic table writes.
- Routed projection uses historical images for key changes, enter/update/leave
  and deletes. FULL before-images can fill unchanged-TOAST holes in new images.
  Filtering or cleanup requiring unavailable historical fields fails explicitly
  with `ReplicationProjectionIncomplete`; it cannot advance progress by
  treating omitted fields or identity placeholders as historical NULL values.
- Private native source commands are rejected at routed/coordinator/participant
  boundaries until source-head anchor and participant transport are implemented,
  instead of silently discarding their durable admission metadata.
- `zig build source-transaction-test -j2` passes the expanded 18-case
  snapshot/stream/parser suite, source envelope checks, and two planner tests.
  Coverage includes native rollback when a later member is invalid, atomic
  multi-table request shape, old-key removal, route enter/leave, historical TOAST
  reconstruction, missing-capability/incomplete-image rejection before mutation,
  snapshot routes, configured transforms, and work-permit revocation. Current
  log: `zig/source-atomic-routing-qualified-final.log`.

This improves atomic application but does not yet connect managed source progress
to durable admission receipts. Lost acknowledgments can still replay transforms
until the source actor persists sequence/pending identity/accepted timestamp and
uses destination receipt resolution. Native source schema qualification must also
validate that a mapping without FULL old rows uses a stable replica-identity key;
arbitrary non-identity key expressions cannot silently inherit that guarantee.
Writable-Iceberg transformation/admission and accepted-versus-catalog-committed
progress remain separate required adapters.

## In progress: durable source actor control authority

- `metadata/source_control.zig` defines immutable source/provider/destination
  binding, explicit generation, stable pending sequence/batch/checkpoint/time,
  payload and delivery-plan digests, original transaction identity and recovery
  manifest. Catalog ordinal is a locator, not source identity. Identity hashes
  use explicit versioned field projections.
- Accepted and authoritatively committed frontiers remain separate. Bounded
  outstanding receipts retain the correlation while writable-Iceberg catalog
  commit lags WAL acceptance. Native adapters can report an already committed
  receipt atomically. This state never claims that an index has published.
- Reservations retain worst-case encoded receipt credits before destination
  admission. Operation count and encoded-byte limits apply independently; only
  committed-prefix proof releases receipt backlog.
- Catalog/revision/binding CAS fences mutation. Replan requires a not-admitted
  proof and retains the original logical batch, source offset, sequence, payload
  and accepted timestamp. Ambiguous attempts cannot be discarded. Rejected
  attempts can retire only under their original reservation identity; a new
  generation requires complete retirement first.
- Metadata transition tag 83 persists the authority and a constant-time per-table
  work counter. Live work blocks table retirement. Both state and counters are
  included in Raft snapshots; the coordinated decoder floor is version 90.
- Private service helpers use linearizable reads, preflight validation, durable
  proposal/application receipts and exact observed state. Lost acknowledgments
  and failed CAS never grant source progress. Corruption, backpressure and
  conflicts retain their runtime error identities.
- Seven standalone control tests pass, along with real metadata reopen, stale
  catalog fencing, snapshot install, table-drop protection, snapshot projection
  registry and decoder-floor checks. `zig build source-control-test
  source-transaction-test -j2` passes, including all 18 CDC regressions. All 56
  runtime error ABI tests pass. Current logs:
  `zig/source-control-complete-qualification.log` and
  `zig/source-control-error-abi-test.log`.

The managed source actor and destination adapters still need to invoke these
operations. Receipt/fencing references are private adapter outputs, not
self-authorizing public strings: native and writable-Iceberg adapters must prove
their engine-specific outcomes before accepting, committing, replanning or
retiring source work. No source actor/public capability is advertised yet.

## Remaining implementation phases

1. Finish root history tests, prepared credit accounting and native integration.
2. Destination-independent admission with durable source checkpoint, replay
   identity/payload fencing and native/Iceberg adapters; managed PostgreSQL routing.
3. Durable feed/slot metadata and root enrollment, coordinated commit completion,
   stable delivery identities and independent per-index readiness events.
4. Consistent durable snapshot/bootstrap cuts, paging/restart, retention pins,
   consumer ack/expiry/backpressure, and source topology transitions.
5. Public HTTP/SQL interfaces and generated specs/SDKs, live authorization,
   before/after projection filtering and revocation/reset semantics.
6. pgwire replication startup, commands, slots/publications/catalog subset,
   CopyBoth/feedback/keepalive/cancel, pgoutput typed messages and stable LSN map.
7. Pinned existing-client snapshot/stream/reconnect/failure qualification, including
   inbound PostgreSQL -> destination commit -> outbound feed with independently
   advancing indexes; record actual versions and unsupported capabilities.

## Working constraints

Use this worktree and its `zig/.zig-cache` and `zig/zig-out`; no temporary build
caches. Do not spawn subagents without explicit authorization. Run appropriate
focused tests and `make gc` after long build sessions. Runtime logs are ignored
local artifacts. Keep draft PR scope/status honest as implementation grows.

## Next code boundaries to use

- Native `api/table_write_source.zig` already has `commitTransactionWithId` and
  cancellation-aware variants; `api/table_writes.zig:BoundTableWriteSource` routes
  to durable transactions. A terminal retry currently returns its old outcome
  without comparing a new payload, so a bare hash-derived transaction ID is not
  sufficient source admission. Add durable source-head/checkpoint and payload
  fencing in the same native transaction; do not bolt on a best-effort receipt.
- `storage/transactions.zig:VersionPredicate` supports internal `exact_value`
  guards. Public `storage/db/types.zig:TransactionVersionPredicate` only carries
  document versions. Any source-head exact predicate must be compiled through a
  trusted private source-admission field, not admitted from public document keys.
- Managed PostgreSQL capture is in `metadata/replication_backfill.zig`; its
  transaction iterator groups complete checkpoint runs, but destination apply
  now submits complete groups through canonical transaction commit. Preserve
  this ordered projection when adding durable destination admission. Key changes and
  filtered enter/leave need provider before-images; the current pgoutput update
  parser now retains old tuples and completeness; routed apply still must consume
  those images before claiming that scope.
- Pgwire `backend.zig:Backend.VTable` has no replication callbacks yet, and
  `protocol.zig` rejects replication startup. Add actual replication commands,
  CopyBoth and feedback through authenticated backend capabilities; HTTP/SQL
  polling alone cannot satisfy #1639.
- Durable metadata commands/snapshot storage live in
  `metadata/storage/raft_apply_store.zig`; feed/slot records need this authority,
  not process-local maps. Public specs remain under `specs/openapi/antfly` and
  require `make generate` before handler implementation.
- Writable Iceberg admission/drain is `serverless/lake_ingestion.zig`. Its WAL
  retains original source batches, but grouped catalog commits fold mutations.
  Add recoverable committed-history references tied to actual catalog commits;
  accepted WAL entries alone are not committed outbound events. Existing external
  snapshot attachment remains a separately qualified capability.

## Source routing integration constraints

Do not copy one root-local command into every distributed participant unchanged.
A routed transaction needs an explicit, topology-fenced source-head anchor plus
payload-fenced data participants and a durable coordinator outcome. The anchor
owns the global predecessor checkpoint and receipt; individual data groups may
not have seen the preceding source transaction. Per-participant effect digests
must bind the routed effects while the source payload digest binds the entire
original transaction. Resolve recent retries through the anchor before routing
against a newer schema/topology; retain enough original participant authority to
finish recovery after lost responses. Source receipts cannot authorize early
global completion while participant resolution is incomplete.

Source progress also needs a persisted monotonic sequence and stable accepted
timestamp/transaction identity, including checkpoint-only polls. Do not order
opaque checkpoints, encode snapshot/LSN phases into numeric ranges, or advance
destination progress only for polls containing rows. Preserve old key/row and
unchanged-TOAST information from pgoutput; source transforms and route
enter/leave/key changes must use source images with declared completeness.
