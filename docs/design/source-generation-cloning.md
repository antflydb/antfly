# Native source-generation cloning

Status: proposed engine work; not implemented or enabled by the current Cloud
follow-up. This document is the contract required to remove full replacement
writes without weakening source publication or rerunning unchanged enrichment.

## Problem

Cloud publishes candidate source rows in an isolated physical scope, verifies
the complete inventory, and then activates its catalog binding and PostgreSQL
pointer. Identical inventories can reuse an accepted scope. A changed inventory
currently creates N replacement rows, even for a one-file edit. Bounded merging
reduces requests, but new document identities can also duplicate derived work.

Updating the accepted scope in place is not an incremental solution: readers
would observe partial candidates before catalog activation. Copying only primary
JSON rows is also insufficient because native units, chunks, vectors, graph
evidence, permissions, and completion receipts belong to their source identity.

## Contract

Implement a native copy-on-write source view rather than a Cloud-only copy loop.
The base view must identify the table UUID, logical source, accepted publication
revision, source/catalog generation, and retained read root. A candidate has an
independent view ID and cannot appear in ordinary accepted-source reads until
activation. A request key makes preparation idempotent across lost responses.

The engine owns the mapping between a logical source identity and physical
document/artifact identities. Cloning a view must retain unchanged native
artifacts and indexes without serializing N primary rows or relaunching their
producers. A view-specific key/permission projection must not mutate the base
artifacts or their provenance. This requires an explicit engine representation;
it cannot be implemented by rewriting source_id fields in Cloud.

Apply upserts and removals to the candidate view. Unchanged records share their
verified immutable content/artifacts. Changed content runs the normal native
producer lifecycle. Removals affect candidate membership only. A ready receipt
binds the resulting inventory digest, producer/catalog generations, and physical
view. Permission validity remains a separately audited, expiring authority.

Activation compares the expected accepted view and revision and atomically
publishes the candidate view. Cloud's durable activation intent still coordinates
its operational pointer with the native receipt. Credential revocation,
cancellation and retirement remain commit fences. A stale table identity, stale
base revision, expired lease, or incompatible producer catalog rejects activation.

Views retain their base dependencies until no accepted/candidate/in-flight reader
references them. Aborting a candidate must neither remove accepted artifacts nor
leak abandoned membership into a later candidate. GC must be resumable, bounded,
and ownership-fenced. A table drop/recreate cannot inherit old view receipts.

## Delivery and acceptance

1. Add the engine view/receipt representation and replica/reopen recovery first.
2. Expose a versioned capability and ordinary authenticated table operation.
   Bind authority to current table grants and source ownership; accept no caller
   supplied physical root or artifact locator.
3. Add the source SDK adapter and Cloud orchestration only after native semantics
   are executable. A capability probe must not silently enable an unimplemented
   protocol. Retain the existing isolated replacement path until that delivery.
4. Compare one-file edits at 1k, 10k, and 100k records. Measure primary bytes,
   artifact/index bytes, model calls, requests, latency, retained storage and GC.
   The target is work proportional to changed membership/content plus bounded
   view metadata, with no unchanged OCR/embedding/extraction calls.

Required regressions cover an unchanged sibling's units/vectors/graph evidence,
policy-only edits, changed producer configuration, concurrent readers, deletes,
failure before/after every activation acknowledgement, stale credential versions,
table replacement, cancelled attempts, dependency retention, process restart,
replica ownership transfer, and aborted-candidate collection. Until these pass,
full replacement remains the correct publication path; changed-source updates
must not be advertised as incremental.

## Append preflight and bulk primary reads

The independent Grepstein append path currently verifies each candidate against
its linearizable primary before writing. Its four-worker window bounds requests,
but the number of round trips remains proportional to the manifest. Ordinary
bulk document-ID search is not a substitute: a query miss need not prove primary
absence while indexing is pending. Using it for preflight would miss late
conflicts and permit partial submission of an otherwise conflicting inventory.

The native optimization needs an authenticated, bounded bulk **primary** lookup
contract. Bind requests and responses to the table UUID, reject duplicates and
partial results, return an explicit found/absent/error outcome and primary version
for every requested key, and preserve all source JSON fields and integer
precision. Read consistency must be explicit and equivalent to the current
linearizable point lookup; shard transport failures cannot be reported as absent.
Pagination/body/key budgets and cancellation must bound retained work. Reuse is
valid only after full document equality; inserts must still carry version-zero
OCC predicates because the preflight and commit are distinct operations.

Test delayed index visibility, table replacement, concurrent inserts, shard
failures, oversized requests and lost responses before switching the loader.
Measure primary read work, HTTP requests and retained bytes at corpus scale.
This API is not implemented by the current follow-up; the loader deliberately
retains authoritative point reads until these semantics exist.
