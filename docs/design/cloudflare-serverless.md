# Cloudflare hosted serverless runtime

Implementation for [#1278](https://github.com/antflydb/antfly/issues/1278).
See the [runnable example](../../examples/cloudflare-serverless/README.md) for
configuration, API, budgets, and current limitations. This extends the
[serverless engine](../../zig/SERVERLESS.md), using its serving formats and
publication protocol. It does not route the embedded local-storage engine
through a simulated filesystem.

## R2-native coordinator

The Antfly-owned R2 coordinator, shared Lite engine surface, immutable commit
protocol, self-collection and frozen SQLite migration are described in
[the object-storage backend design](antfly-object-storage-backend.md). Fresh
R2-native deployments use `wrangler.r2.jsonc`; the existing configuration remains
the SQLite compatibility profile. The details below describe the original
profile unless explicitly stated otherwise.

## Execution and storage

```text
Browser -> authenticated Worker API -> one suspended WASM lane
                                      |             |
                               R2 host binding   metadata coordinator
                                      |             |
                            immutable WAL pages,    WAL HEAD,
                            native artifacts,       native progress/fences
                            native manifests

WAL receipt -> Queue -> bounded native builder -> fenced published HEAD
Scheduled trigger -> Queue (repair missed admission)
App AI route -> Workers AI binding -> configured AI Gateway
```

`serverless_runtime_wasm.zig` compiles the existing artifact, manifest,
publication, document facts, and query paths for `wasm32-freestanding`.
Host-provided clocks, secure randomness, cancellation, and objectstore.Client
replace native process services. Native filesystem cache and version-selection
capabilities remain outside this profile.

The import `antfly_host.object_call` uses a JSON operation header and separate
binary body. Binaryen Asyncify suspends the original Zig stack while R2/DO I/O
completes. Rewind resumes that call exactly once; it does not replay a request
from its start. JS copies outgoing memory views before suspension and returns
owned reply buffers through the allocation ABI. A trap poisons the instance.
The host imports a precompiled module and applies backpressure rather than
running overlapping suspended stacks. Transport bindings are installed for
each request; cached instances do not retain request-owned RPC stubs.

R2 byte ranges and conditional PUTs preserve objectstore contracts. Missing
objects, CAS conflicts, integrity failures, cancellation, limits, and transport
errors remain distinct. R2 bindings do not provide an atomic conditional
DELETE, so that capability fails closed. The coordinator implements metadata
CAS in one SQLite transaction, retaining native record metadata such as
content type. An authority is selected at initial provisioning and must not
change without an explicit migration.

## Indexed object WAL

The old object WAL rewrites one encoded namespace log. The new opt-in
`wal.IndexedObjectStore` instead commits two authenticated ordered page trees:

- big-endian LSN → existing canonical binary WAL record;
- operation ID → receipt containing committed range, timestamp, payload hash.

The implementation reuses `graph_segment/page_tree.zig`, including bounded
pages, authenticated references, sorted batched updates, and range cursors.
Immutable content-addressed pages live under
`<prefix>/wal-v1/<sha256(incarnation)>/pages/<epoch-attempt>/<digest>`.
Legacy format-1 pages remain readable. A small HEAD stores both
roots, the committed high watermark, retained floor, and upload epoch. One HEAD CAS commits
the batch and its receipt together. Losing writers retry from the new roots;
uploaded losing pages are unreachable, never accepted mutations. Ambiguous
successful CAS responses are resolved through the receipt tree.

Flat-index publication reuses bodies from the verified packed document snapshot
only when their byte length and digest match the current authoritative facts.
Missing or changed bodies are hydrated individually; facts still determine
membership and revision. This bounds cold-rebuild object reads without changing
the persisted formats or replacing the current full in-memory index build.

Append and receipt lookup touch tree paths, not historical log bytes. A batch
shares changed paths and one commit boundary. A captured inclusive LSN range
seeks directly into the record tree, validates a contiguous committed prefix,
and enforces record, byte, and cancellation bounds. Retention never resets the
high watermark or forgets receipts. The format is separate from the old
object WAL; existing native deployments are not silently migrated.

## Visibility and interactive reads

Mutation acceptance returns a versioned incarnation-bound visibility token.
Each live query opens the existing published QuerySession (including native
reader pinning), captures the canonical WAL high watermark, verifies the
required fence, and reads only the admitted tail after the manifest watermark.
Before the first publication the base is empty. A gap, expired prefix, or
unavailable fence fails; it cannot produce a successful stale read.

The tail becomes a bounded map of the latest mutation per document, including
tombstones, plus sorted IDs. A point read consults this map then the native
document facts index. Ordered listing merges an index cursor with sorted
recent IDs. Predicates see current bodies before limiting; the merge refills
after suppression. No interactive path materializes the entire published
database merely to overlay a few writes.

Simple fresh text search builds a bounded transient text segment using the
native tokenizer/index specification. It suppresses changed published IDs,
evaluates recent bodies, and merges scores before fetching final bodies. The
existing document-local term-frequency scores are comparable between sets.
Requesting an extra candidate per changed ID permits refill after suppression.
Corpus-dependent scoring, vector/hybrid queries, counts, aggregations, and
custom search authorization require additional algorithms; they are not
implicitly fresh because point reads are fresh.

Opaque list cursors bind incarnation, query/limit, generation, WAL cutoff, and
last ID. Subsequent pages reopen that generation and cutoff rather than HEAD.
They acquire the generation's native reader lease or fail if it has expired.
Cursors also carry the score/document-ID position for ranked text pages.
Cursors and visibility tokens are positions, not credentials: hosts authorize
each request independently. A physical incarnation must never be reused for a
dropped/recreated database. This is a per-database guarantee, with no claim
about Gmail internals or cross-table transactions.

## Publication, recovery, and collection

The builder's optional WAL input bounds limit the next contiguous prefix and
are disabled by default for existing native users. A publication resumes from
the committed manifest watermark, uses existing ownership fencing and immutable
artifacts, and advertises whether more WAL remains. Queue retries and a periodic
repair trigger drive progress after eviction. Acceptance does not depend on
queue admission succeeding, and a scheduling failure cannot erase an accepted
write receipt.

Hosted collection now has an epoch-fenced WAL compactor and a durable SQLite
frontier/mark set. The collector reuses native publication fencing, retirement
floors, reader pins, and authenticated format decoders. It checkpoints bounded
steps across Queue deliveries, retains retry receipts, and applies a bounded
reader grace period before deleting retired immutable identities. See
[the safety argument and deployment contract](cloudflare-hosted-collection.md).

Fresh scalar filters and ranked text cursors merge current bodies before
limiting; exact unfiltered document counts use fact-count deltas. Vector/hybrid
visibility fences require a sufficiently published generation with no configured
embedding work pending. Native enrichment uses a suspending typed host callback,
durable pending-cycle progress, and conditional idempotent WAL appends; model
results computed from a stale source cannot overwrite newer documents.

## Validation and remaining qualification

`npm run test:native` tests tokens, cursors, the ordered overlay, bounded WAL,
malformed mutation decoding, and allocation failures. Node tests run the actual
instrumented module over asynchronous R2 contract fixtures, including concurrent
writers, ambiguous commit responses, bounded I/O, publication, fresh reads, and
pinned pagination. The workerd test uses persisted local R2 and SQLite DO state
across complete runtime restarts. CI runs these in the WASM qualification job.

These tests establish local protocol behavior. A disposable deployed fixture
also passed; see the [measurements and limits](cloudflare-qualification.md).
The [native reuse audit](cloudflare-native-audit.md) records subsequent adapter
changes, larger local workloads, native compaction, paged dense/sparse updates, resumable bulk reindex, and live
windowed enrichment and SQLite soaks.
The current Wrangler dry-run bundle is about 4.6 MiB uncompressed / 1.4 MiB
gzip; sizes vary with the toolchain. The interactive and maintenance instances each cap WASM memory at 32 MiB
and share an 8 MiB immutable JS cache. Total isolate memory, startup, CPU,
real R2 throttling, broad deployment failure injection, and sustained
compaction/collection need further qualification. The small live fixture
measured latency/CPU and exercised collection and Gateway; it did not measure
total isolate memory. Large builds can exceed admitted Worker budgets and remain an
explicit deployment limitation.


The opt-in paged projection profile bounds dense/sparse update and rebuild
memory. Its hash-local clustering changes ANN routing quality; the native audit
records a low-probe recall limitation and measured results. A globally trained
or hierarchical routing layer remains necessary where the measured
recall/latency tradeoff does not meet application requirements. The resumable
reindex operation rebuilds current native definitions; it does not implement
DDL migration or graph/external-source reconstruction.
