# Recoverable compaction outcomes

A live diagnostic lost a compaction HTTP response with `ECONNRESET`. The
qualification client correctly refused to replay the unkeyed operation: a
second invocation could compact a newer manifest. A fresh status/count check
cannot establish which compaction completed. That failure remains recorded.

The R2 hosted example now supports a stable `operation_id` on `compact`, plus
`compact_status` with the same identity. These are example API operations, not
new claims of native public API parity. Unkeyed compaction keeps its current
behavior, including the cheap protected no-op for fully paged serving roots.

```json
{"op":"compact","operation_id":"maintenance/2026-10-27/1"}
```

```json
{"op":"compact_status","operation_id":"maintenance/2026-10-27/1"}
```

Status returns `unknown`, `pending`, or `complete` with the original result.
`pending` can include an abandoned attempt; it is not a liveness assertion.
After a transport interruption a client can inspect status, then resubmit the
same keyed request. Completion returns the original `published`, `version` and
`artifact_count`, even after later publications and coordinator restarts.
There is no automatic replay of a differently identified request.

## Durable protocol

The coordinator hashes the bounded operation identity into an Antfly metadata
tree key. Its scope is the database incarnation and tenant; identical IDs in
different tenants cannot share a receipt. The effective compaction document
and artifact-byte limits form a canonical policy fingerprint. Reusing an ID
after those limits change fails with `CompactionOperationConflict`; use a new
ID for work under a new policy. IDs are limited to 256 UTF-8 bytes.

Beginning a new attempt commits a pending receipt and acquisition of the
existing **native** publication lease in one metadata transaction. Native WASM
compaction accepts that lease and builds through `PreparedPublication`.
Immutable uploads remain invisible until finalization. Source protection,
lease expiry, native fencing, allocator/transport limits and cancellation
remain authoritative. A timed-out host does not grant takeover authority.

Finalization validates the pending attempt identity and native lease, checks
the publication read set and resulting HEAD/version, applies the metadata
batch, releases the native lease and stores the completed result in **one
Antfly metadata commit**. This includes a no-op result: its source read set is
validated before committing its receipt. A lost acknowledgement therefore
recovers either the pending attempt with the prior HEAD, or the new HEAD,
complete receipt and released lease together. It cannot recover a published
HEAD without its receipt. Replaying a completed identity never acquires a
lease or applies an old metadata batch.

The receipt survives actor eviction and deletion of platform Durable Object
storage because its authority is the Antfly ordered tree and immutable log in
R2. An expired/replaced attempt cannot finalize. Failed preparation can leave
immutable uploads for the existing collector and a pending receipt for a
subsequent fenced attempt; neither is a successful outcome.

Receipts have the database incarnation's lifetime, like retained mutation
identities. New identities consume durable metadata rather than an unbounded
in-memory map. There is no TTL that silently makes an old operation execute
again. A future retention policy must expose an explicit client watermark or
expiry contract before reclaiming replay authority.

## Validation and gate boundary

Native-backed tests cover actual HEAD publication with an injected lost commit
acknowledgement, reopen and exact result replay, plus expired-owner takeover
and rejection of stale finalization. Actual-workerd tests cover public status,
no-op receipts, subsequent publication, actor restart, erased platform storage
and policy-conflict rejection. Existing unkeyed no-op fencing tests remain.

The frozen corrected control `c44896ac8c` has no such receipt protocol. The gate
driver is deliberately unchanged: unkeyed maintenance transport ambiguities
still fail, cold attempts remain single-attempt, and retry/time budgets remain
unchanged. Enabling keyed maintenance in paired qualification requires a
reviewable compatibility/control proposal and fresh paired evidence; changing
only candidate retry behavior would not fix the original comparison fairly.
