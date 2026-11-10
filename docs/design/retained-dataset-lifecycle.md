# Retained dataset lifecycle

Follow-up to [time-series design #1540](https://github.com/antflydb/antfly/issues/1540).

## Resource and storage contracts

A retained dataset has a stable 32-byte identity, a kind (`records` or `summary`), immutable definition generations, a self-contained schema and its SHA-256 digest, an authorization-policy identity, logical retention, and local residence. Summary definitions additionally describe source identity/filter, dimensions, timestamp, a versioned UTC fixed-duration bucket/origin, measurement laws, and a correction window. Other timezone/calendar semantics are rejected until their decoder and planner contracts are implemented. Nanosecond coverage, origins, durations, and source sequences are decimal strings so JavaScript hosts preserve the full signed domain.

`serverless/datasets` implements the durable lifecycle boundary. Its catalog uses the existing `objectstore.transactional_store` conditional-root backend and immutable ordered pages. It does not introduce another metadata authority protocol. Each dataset has an independent permanent authority namespace. The catalog itself must be durably backed up and reachable by every serving host; moving segment contents does not move the catalog authority. Use shared object storage for a stateless/serverless deployment. Conditional ownership and exact-root publication prevent stale writers from advancing placement after an ownership change.

Definitions are immutable by generation. Evolution installs a new current definition and retains previous definitions/schemas. Existing segments continue to reference their original generation. Evolution establishes no new backfill coverage. The kind is stable across generations.

Segments contain a content digest/length, dataset/generation/schema identity, state-encoding version, half-open event coverage, source partition/incarnation/applied sequence, finalization state, and placement revision. Producers supply complete immutable segment contents. The operator does not certify that arbitrary supplied bytes were captured transactionally from a source table.

This layer accepts already produced record or aggregate-state segments. Automatic native-table snapshot/backfill, committed summary maintenance, source-write finalization fences, aggregate-state decoding, SQL table mounting, public subscriptions, and table-TTL integration remain separate integration stages. In particular, creating this resource does not make ordinary source-table TTL preserve historical aggregates. Hosts must establish those producer and retention proofs before connecting automatic source deletion.

## Placement and access

Location identities are separate from physical store bindings. A host supplies a stable physical-binding fingerprint; the first publication binds it durably and subsequent operations reject rebinding. URLs and credentials are not persisted in dataset metadata. The administrative operator derives the binding fingerprint from the supplied URI, so later invocations must use the same URI spelling for a location identity. Host applications should derive stable fingerprints from their own binding registry.

Access modes are `local`, `queryable`, and `restore_required`. At this storage boundary `queryable` means a host can obtain the immutable bytes directly without restore; it does not imply that the segment's encoding is automatically mounted as a SQL table. Decoder/version admission remains the query host's responsibility. Restore-required objects cannot acquire new read leases until restored.

Migration copies finalized segments only. It reads and SHA-256 verifies the complete source object, performs a conditional immutable destination create, reads/verifies the destination, and then publishes placement through exact-root CAS. Duplicate immutable uploads are accepted only after verification. A lost acknowledgement or a failed catalog CAS leaves the previous placement authoritative. Local and remote copies never become separate coverage entries.

A restore copies verified archive data into a local binding and publishes a new placement revision. It preserves the archive reference and does not authorize deletion of the archive. Repeated migration back to a retained archive can reuse its immutable object. Migrating between arbitrary remote locations or retaining an unbounded replica set is not part of the initial contract.

All object reads, copies, and metadata pages are bounded. Segment contents are limited to 16 MiB; producers split larger outputs into disjoint extents. Metadata rows retain the shared coordinator's 32 KiB bound. Interrupted uploads can leave unreferenced content-addressed objects; this release never automatically deletes an object merely because a catalog publication failed. A bounded orphan collector must use a publication barrier and an inventory of protected references before it is enabled.

## Coverage and planning

The catalog indexes non-overlapping extents by definition generation, source partition/incarnation, and signed event-time end. Sign-biased fixed-width encoding preserves chronological order across the epoch. Publication checks the first extent ending after its start; because existing extents are disjoint, this proves non-overlap without scanning history. The locator and segment metadata commit atomically.

`planRange` seeks directly into the requested partition/generation. It requires contiguous complete coverage, exact segment boundaries, and an explicit output bound. Missing history is an error, not an empty aggregate. A partial segment request is a resolution error. Explicit empty-state segments can certify empty coverage. Different source incarnations and definition generations are never implicitly merged.

A range plan is a preparation result, not a read capability. A host must acquire all selected segment leases and verify all reads before exposing results. It must enforce the current dataset authorization policy, admit schema/state decoders, choose authoritative source topology, and merge aggregate states through their declared laws. This catalog does not manufacture a cross-partition transaction snapshot from independent positions.

## Reader-safe eviction and deletion

Read acquisition registers a pin and validates the segment in one metadata transaction. Migration can proceed while an old local reader remains active. Local eviction first proves no unexpired pins reference that local copy and publishes a retirement barrier, then deletes the object in a resumable second step. New readers use the authoritative remote placement.

Pins have a ten-minute maximum lifetime, a thirty-second collection grace, and a host suspend-inclusive authority deadline. A wall-clock rollback cannot extend a live read. Acquisition cleans expired pins within a bound of 64 active pins per segment. Hosts should share leases across requests where appropriate; this is not one durable consumer per browser.

Logical expiration is based on event-range end plus the definition generation's retention. Expired segments cannot acquire new read leases even if a dependency keeps their bytes physically retained. Existing admitted snapshots remain bounded by their original leases. Local residence is a policy target exposed to a scheduler, not permission to delete logical history or a mandatory minimum residence time.

Durable dependency holds prevent permanent retirement. Hold acquisition races atomically with the retirement barrier; a retired segment cannot gain new dependencies. Releases compare exact consumer/generation identity so an old worker cannot clear a replacement hold. A host must authorize hold/release operations and release a hold only after the downstream requirement is satisfied.

Retirement requires logical expiration, finalized state, no pending old-local reclamation, no dependencies, and no live readers. It fences new readers before collection. Collection deletes current and retained archive copies independently, records completion, and keeps a tombstone to prevent stale publication retries from resurrecting history. Event-ordered compaction replaces fully collected tombstones and their expired pins with one durable retirement floor per source partition/incarnation/definition generation. Publication rejects any extent crossing below that floor; cleanup cannot grow one permanent record per expired segment indefinitely. Producers must never reuse a segment identity for different contents/coverage. Ambiguous delete acknowledgements resume safely.

`requireSourceReclamation` is the distinct coverage proof for a required summary: matching dataset/definition generation, exact source lineage, an incorporated sequence at least as new as required, finalized non-retired state, and covering event extent. It does not authorize local eviction, archive expiration, or deletion of the summary itself. Connecting it to raw TTL requires the native producer/commit integration described above.

## Integration sequence

1. Durable resource, immutable definitions/schema, indexed coverage, verified placement, reader pins, holds, and administrative operations (this change).
2. Native snapshot/backfill producer and committed correction maintenance, with source admission/finalization fencing and transactional progress publication.
3. Source TTL/replay/support dependencies, including lease-owned background scheduling and bounded stalled-retention behavior.
4. Aggregate decoder admission and SQL/JSON summary/history queries, with topology and authorization proof gates.
5. Resumable subscriptions, followed by optional alert consumers and an outbox.

The lifecycle API is designed for ordinary retained records as well as aggregate-state segments. It must not be advertised as automatic migration of an existing live table until the source integration and query routing stages are implemented and qualified.
