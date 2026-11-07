# Native commit proofs

Revision 15 keeps the paged COW engine and introduces one-sync publication for
bounded, synced native transactions. It does not remove the older safe protocol.

## Publication

Before mutating pages, establish durability of the exact parent checkpoint.
A checked post-sync witness names its inode, file namespace, sequence, candidate
fingerprint and proof digest. A runtime may also remember that exact witness.
An absent witness requires a file sync before the parent becomes an anchor.
A witness is never emitted for `no_sync` or a failed sync.

Collect the final SHA-256 postimage of every successfully written physical page.
The collection is optional, limited to 16,384 pages and 64 MiB. Allocation failure,
size mismatch or overflow disables proof publication; truncating the collection
is forbidden. Reserve a value-chain manifest with bounded headroom, retain its
pages in the allocator, persist the allocator and seal its final root. All these
page postimages join the collection. The manifest pages themselves are excluded:
their bytes are authenticated by the reference in the checked checkpoint slot.
An insufficient reservation falls back to the older protocol and retires the
unused manifest through normal metadata reclamation.

Each emitted manifest anchors exactly `sequence - 1`, with no parent link. It
binds the file namespace, sequence, durable parent sequence and candidate-only
checkpoint fingerprint, plus sorted unique page IDs and full-page hashes. The
candidate fingerprint excludes its own manifest digest and the inactive slot.
The allocator seal includes both recovery slots, with their proof digests zeroed,
so neither seal nor manifest requires a circular hash.

Write pages, manifest and the checked new slot, then sync once. Only successful
sync permits an acknowledged outcome. A sync failure after slot I/O is an unknown
outcome and prevents further mutations until reconciliation. After success, write
a checked witness without a second sync. A witness-write failure is an optimization
miss, never a reason to report a durable write as uncommitted.

## Recovery

Validate ordinary slot checksums, file bounds and proof reference first. An exact
post-sync witness permits selecting that durable checkpoint. Without one, validate
the manifest digest, exact candidate identity and every physical postimage using
uncached reads. Reject a structurally incomplete candidate and try the older
slot. Cancellation, memory exhaustion and operational I/O errors propagate;
they must not silently select an older state.

A valid recovered manifest without a witness is synced before serving the recovered
state or permitting another proof publication. This avoids depending on an
unknown predecessor whose unsynced pages could disappear in another crash.
Integrity inspection validates postimages even when a matching witness exists.
Logical-only snapshots cannot inspect allocator/proof metadata.

## Retention

A checked candidate can require intermediate pages no longer reachable from its
logical roots. Retain all pages through the prior sequence of every recovery slot
that carries a proof. Reader and deferred-durability horizons further constrain
reuse. Tracking only live document roots or releasing intermediate pages at the
current sequence would invalidate recovery fallback.

The active frame has one allocator ownership claim. A replacing publication
retires that claim as metadata. Continuation pages and optional codec parent links
have explicit ownership and bounded metadata reclamation. Logical-only readers
do not pin this metadata; full inspection snapshots retain the metadata horizon.
Maintenance uses the older safe protocol, retires an inherited active proof and
does not create another one, so idle collection can settle.

An unpublished copied or compacted generation drops its active proof through the
allocator, duplicates its resulting complete proof-free slot, then changes its
namespace and reseals. Snapshot copying retains any newly added allocator pages
and reports the actual output checkpoint and physical length.

## Scope and qualification

Verified allocator maintenance uses the same checked postimage protocol and
one-sync boundary. Unverified maintenance cannot acquire this capability merely
by sealing a ledger: it retains the conservative barriers and does not mint a
trusted allocator certificate. Private namespace rebinding suppresses proof
collection while retiring old proofs and rewriting their namespace-bound seals.
Torn maintenance postimages reject the new slot just as authored postimages do.

Raw native writes, legacy formats, deferred durability and collection
overflow retain the older data-before-checkpoint barriers. Existing revision 13
and 14 files remain readable; writable owners migrate by generation replacement.
This protocol does not itself combine separate primary and index publications.
Foreground preparation, publication cohorts and end-to-end qualification must be
assessed separately before claiming the full architecture is complete.
