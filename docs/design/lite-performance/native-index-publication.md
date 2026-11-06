# Native index publication

The Lite index LSM is a logical index-state machine inside the native file. It
must not become an independent physical durability domain. An LSM WAL record is
still useful as a recovery encoding; writing that record, installing its
immutable inputs and publishing its replay continuation must share one native
checkpoint.

## Ownership and commit boundary

Preparation owns immutable artifact bytes, deletion bitmaps and a prepared
replacement snapshot. Large artifacts use bounded unpublished construction
leases, so staging does not monopolize writer admission. Artifacts up to 256 KiB
stay in provider-owned memory, independent of request scratch, and join the
activation transaction directly. Larger artifacts use a construction lease that protects
the immutable reader before a public alias exists.

Under the checked final writer admission, a publication validates the captured
index generation, fragment identity, input fingerprint and segment-ID frontier.
It allocates replacement readers and memory before attempting durability. A
borrowed participant travels explicitly through the namespace transaction and
LSM WAL append. The native storage host stages that participant's artifact links
and the encoded WAL append in one native transaction. The participant has no
commit, abort or in-memory activation capability. Unsupported storage providers
refuse this operation; a sequence of independent writes is not a fallback.

WAL directory initialization, rotation and pressure relief do not receive this
participant. The participant belongs only to the actual activation record. LSM
memtable activation follows successful native publication; index snapshot
activation follows successful LSM commit. Every artifact needed by a committed
activation therefore exists in the same durable checkpoint.

A prepared replacement guards new retained snapshot captures across the external
commit and in-memory activation. The native participant arms this guard after
LSM pressure relief and candidate preparation. It releases it after installing the
successor pointer, or on abort after the owner has classified/fenced the failed
publication. This prevents newly visible primary progress from authorizing a
new capture of the old in-memory index. Previously retained snapshots continue
to represent their original cut. Contended capture waits yield the CPU.

## Refusal and uncertainty

A prepublication failure aborts all staged links and the WAL append. Prepared
immutable inputs remain available for another attempt. Alias collisions never
permit overwriting or deleting a foreign artifact. A later attempt may allocate
another numerical segment ID.

A publication error that may have committed fences the native owner and its
serving wrappers. The old in-memory snapshot remains active, but must not be
used to claim the new request failed before commit. A fresh owner reconciles the
checkpoint and WAL. Ordinary file-WAL tail repair must not truncate this atomic
native publication: it could remove activation state from a successful commit.

## Primary progress

The physical activation includes its exact fragment continuation. A partial
fragment cannot advance the coarse primary watermark. Joining coarse progress
requires proving that every preceding fragment and required consumer effect is
complete, preserving the catalog identity and revocation rules used by existing
projection metadata. Advisory status must describe the prepared successor, not
the old in-memory snapshot. The implementation joins complete-fragment primary
progress and its physical index seal through a namespace-scoped metadata participant. It rechecks the
captured authority, including epoch and catalog digest, inside the native
transaction and uses the existing progress/revocation rules. A later coarse
flush skips progress already covered by that atomic publication. Diagnostic
status remains advisory and is refreshed by the separate status service.
These changes still require correctness and performance qualification.

## Verification

Require one native checkpoint for an already initialized multi-artifact
activation, preserved old snapshots, deletion/overwrite correctness, exact
fragment resume after reopening, collision safety, participant-stage rollback,
allocation failure and native durability failures. An uncertain commit must
recover complete activation state and preserve its artifact leases. Existing
WAL crash models and native allocator/reclamation checks remain required.

Operation counts are local feedback. Final throughput, tail latency, active
storage plateau, strict index visibility and unchanged-importer contention are
qualified separately on wolfspider using the retained baseline receipts.

## Explicit drains and finite replay credit

Explicit native drains use bounded producer, independent-consumer and dependent-
consumer turns, followed by physical retirement after releasing source reads.
The connection rotates all namespaces sharing the native replay ledger before
revisiting one namespace. It retains the original producer interval when auditing
terminal failures, so an advanced cursor cannot hide an earlier failed request.
Strict idle finalization follows the bounded service round. No quota or visibility
timeout is raised to permit progress.

The local regression previously retained 13,282 bytes of a 13,425-byte consumer
allowance after two output publications, with producer progress still zero. The
next output could not fit while the producer held the drain. Interleaved service
now finishes that fixture and verifies physical reclamation between outputs.

Explicit drains and compaction must not wait for a detached replay continuation
while holding its writer publication gate. A refused continuation borrow unwinds
all runtime-local work, releases admission, and yields before reacquiring and
refreshing the foreground runtime. Durable progress remains authoritative across
that handoff; callers never replay a document mutation to resolve this wait.

A fair explicit round also advances pending graph replacement construction and
cutover using their existing finite work budgets. Merely reporting a pending
replacement would prevent idle finalization from ever servicing it. Background
scheduling retains blocked resolver debt for a later wake; explicit drains finish
after runnable consumers settle and leave the blocked outcome visible. The
configured stuck-index diagnostic is checked between explicit rounds as well.

Memory-backed artifacts use the same resource-accounted decoded-lease contract as
native range sources. Resident bytes reserve host capacity before allocation;
a declined reservation selects bounded file staging. Decoded leases independently
retain the immutable bytes and their resource owner after the runtime closes.
Optional cache rejection refunds metadata before the normal decoding fallback.

## Producer completion and committed-effect progress

A producer's source watermark and an index's committed-effect watermark answer
separate questions. A multi-document source record can produce several output
publications before the producer completes that record. Each output has its own
durable journal sequence. Index replay must consume those committed effects
without waiting for the unfinished source record; otherwise finite output
credits can form a circular wait. A synchronized visibility request first fences
source completion, then captures and fences the resulting publication cut.
Advancing an index watermark alone never proves future producer output complete.

Native retirement uses a durable cut for each hint lane. Multiple indexes of the
same kind contribute their minimum cut. Enrichment, resolution and promotion
retain their own input lanes. Repair and graph-replacement readers retain all
lanes, and failed catalog loads authorize no reclamation. A canonical payload
can disappear only after every relevant consumer acknowledges it and every
reference is physically deleted, including deletions in the same transaction.
Legacy records without explicit hints retain the conservative uniform minimum.
Continuation state becomes clean only for the exact set of cuts it examined.
