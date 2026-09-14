# Durable native backup seals

## Hidden restore owners and HA continuity

Restore placement travels through a node-scoped private provisioning snapshot,
not the public table catalog. Each hidden owner is authorized by the immutable
restore plan, its exact byte range, target namespace, schema digest, and source
artifact checksum. Physical indexes are initialized while the owner is empty;
imports maintain those indexes without starting enrichment or resolver producers.
Validation drains bounded derived-index replay before recording readiness.

HA seeds include hidden native roots and a separate private provisioning
sidecar. Hidden descriptors never enter the seed's public routing catalog.
Owners created after a seed are introduced by an authenticated HA begin record
containing the immutable bootstrap descriptor. That descriptor is persisted in
the native owner, so subsequent replay can reopen it after cache eviction or
process restart without relying on public metadata visibility.

Standalone seeds also authenticate the full indexed metadata checkpoint, so
durable restore jobs and private plans survive promotion. A checkpoint can
represent an empty public catalog, including a first restore whose owners are
all still hidden. Catalog-only legacy seeds do not gain that allowance.

Scoped Raft import/control commits and final two-phase constraint effects retain
transactional HA outbox obligations, including with asynchronous mirroring.
Retry deduplicates already appended records before clearing the obligation.
Final constraint effects retain their scope and encode private binary metadata
as byte arrays; the standby validates owner range and active constraint
generation before applying them. Derived indexes are rebuilt from the original
import payload, avoiding a second unscoped derived-effects stream.

Canceled HA owners retain a compact, durable terminal identity rather than a
native database forever. Before acknowledging cancellation, the receiver
journals the existing replica-retirement work; recovery rechecks the exact
terminal scope and native owner identity, and live Raft placement still wins.
The cleanup lane drains both read-cache and resident writer leases before
retiring the root. A restart after acknowledgement but before cleanup resumes
from that journal without recreating a canceled owner from an older HA record.
Standalone cancellation uses the same cleanup lane, with the immutable
owner-to-job reservation and retained canceled metadata progress as authority.
Its retirement intent is durable before `finish_cancel`, closing the crash
window between metadata completion and physical cleanup admission.
Private provisioning overlays current durable cancellation progress without
changing the immutable plan. That authority can reopen an exact canceled owner
after a crash before its receipt; it cannot reopen a published or different-scope
owner. Standalone skips owners whose cancellation receipts are already durable;
distributed owners retain placement until cohort cancellation completes.

Terminal proof reclamation uses a durable applied-prefix admission floor, not
WAL retention estimates. After contiguous HA apply progress is committed, the
runtime persists the floor before deleting any proof. Each bounded GC slice
checks at most four owners and requires exact native-root/registry retirement;
held writer leases keep their proof. An indexed, persistent cursor resumes work
after restart without rescanning history. Empty ledgers cause no floor writes.

The authenticated terminal seed artifact includes the floor. Offline activation
also persists an anti-rollback anchor outside replaceable live generations before
publishing `ACTIVE`; older checkpoints cannot replace that PVC's state. Runtime
checks the floor before unknown-owner discovery, so delayed old frames cannot
recreate a reclaimed owner. Fresh-target older seeds remain self-contained and
must satisfy existing checkpoint/WAL availability rules. Source-side metadata
reservation tombstones and native terminal-scope checks prevent stale controls
from being reissued as new higher-LSN bootstrap records; neither a retry nor GC
allocates a new owner identity for an old restore attempt.

Native source decoder materialization checkpoints verified byte/hash prefixes.
After every file and directory entry is durable, the decoder is validated once
and receives an exact scope marker. Final installation renames that tree into
the existing generation publisher: it neither hardlinks every file again nor
recursively resyncs the corpus. The deterministic staging identity survives
timeouts and process loss, including the rename-to-publication interval, and
terminal cleanup uses the generation lifecycle's durable reclamation path.

Every replica advances its own bounded CHECK/index catch-up before applying a
committed restore validation control. Local lag is retryable apply work, never
a deterministic command rejection: the Raft/HA receipt stays at the preceding
entry and reads remain fenced. This also covers a restart after primary import
and its receipt became durable but before the physical projection watermark
did, without relying on optional workers or an RPC addressed to the follower.

The common backup cohort freezes and drains every participating owner before
capturing any owner. Native LSM capture then persists a restart-stable seal;
network upload and corpus hashing do not require the write fence.

`DB.sealBackupCohort` pins the selected primary and generated generations and
creates a private `<database>.backup-pins/<fence-identity>` directory containing:

- Hardlinks to immutable LSM runs and index/text segments.
- Exact primary/index manifests and generated metadata.
- Copied committed WAL prefixes, admitted against one 16 MiB total budget.
- A checksummed inventory bound to the table/shard/range namespace, owner,
  cohort attempt, admission epoch, and schema catalog digest.

The directory and inventory are synced before atomic publication. The returned
handle names the exact fence and inventory SHA-256. Both the logical row mode
and document mode share this mechanism. Non-filesystem and logical-only primary
backends fail with an explicit capability error; there is no silent full-corpus
copy under the write fence.

After every handle is journaled, cohort write fences can be released.
`DB.exportBackupCohort` reopens only the named seal, verifies the inventory and
file identities, and streams files through the existing native backup receipt
collector. It does not recapture live state. Same-handle export retries are
idempotent; mismatched handles and missing/released generations fail closed.

Seals are local durable storage, not Raft-replicated files. The cohort receipt
therefore also pins the source node identity; export must not fall back to a
new leader lacking the generation. Losing that node's disk requires an explicit
backup failure/retry, not substitution of a newer logical snapshot.

Release writes a durable attempt tombstone before reclaiming files. Cancellation
also consumes an attempt whose seal response was lost, so delayed seal delivery
cannot recreate an abandoned pin. One deterministic unpublished directory per
attempt makes restart cleanup independent of unrelated backup history.

## Verification

The focused native tests reopen a real LSM database, perform writes after the
seal and inside export, verify that only the original rows appear in the exported
image, and exercise stale digests, repeated export/release, cancellation, and WAL
prefix admission. `antfly-api-restore-provisioning-test` separately verifies that
hidden target owners are durably reserved before resident-cache admission and
cannot accept ordinary reads or writes.

An incompressible-data microbenchmark on a local development machine measured:

| Immutable runs | Physical corpus | Pin and durable link time |
| --- | --- | --- |
| 8 | 0.53 MB | 7.27 ms |
| 8 | 8.39 MB | 7.71 ms |
| 64 | 4.23 MB | 34.40 ms |

The benchmark also asserts inode equality: increasing corpus bytes does not
introduce an accidental copy. The small complete DB seal measured about 29 ms.
These are diagnostic local measurements, not latency guarantees; metadata count
and filesystem sync latency remain part of the short seal interval.
