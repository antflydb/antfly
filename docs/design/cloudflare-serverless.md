# Cloudflare hosted serverless runtime

Implementation for [#1278](https://github.com/antflydb/antfly/issues/1278).
See the [runnable example](../../examples/cloudflare-serverless/README.md) for
configuration, API, budgets, and current limitations. This extends the
[serverless engine](../../zig/SERVERLESS.md), using its serving formats and
publication protocol. It does not route the embedded local-storage engine
through a simulated filesystem.

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
`<prefix>/wal-v1/<sha256(incarnation)>/pages/<digest>`. A small HEAD stores both
roots, the committed high watermark, and retained floor. One HEAD CAS commits
the batch and its receipt together. Losing writers retry from the new roots;
uploaded losing pages are unreachable, never accepted mutations. Ambiguous
successful CAS responses are resolved through the receipt tree.

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

Physical collection is **not enabled** for hosted stores. Reader pins alone
cannot make a mark/sweep of content-addressed WAL pages safe: a writer could
have uploaded a page before its HEAD CAS while the collector considers it
unreachable. A future bounded collector must coordinate active upload attempts
and writers, preserve cursor/reader retention, retain idempotency receipts, and
persist sweep progress. R2 lifecycle deletion or a separate lease check followed
by unconditional DELETE is insufficient. Until that protocol is implemented,
history and orphan pages consume durable storage but cannot be prematurely
removed by this runtime.

## Validation and remaining qualification

`npm run test:native` tests tokens, cursors, the ordered overlay, bounded WAL,
malformed mutation decoding, and allocation failures. Node tests run the actual
instrumented module over asynchronous R2 contract fixtures, including concurrent
writers, ambiguous commit responses, bounded I/O, publication, fresh reads, and
pinned pagination. The workerd test uses persisted local R2 and SQLite DO state
across complete runtime restarts. CI runs these in the WASM qualification job.

These tests establish local protocol behavior, not deployed service limits.
The current Wrangler dry-run bundle is about 3.4 MiB uncompressed / 1.1 MiB
gzip; sizes vary with the toolchain. WASM linear memory has a 64 MiB hard cap
and the JS immutable cache an 8 MiB cap. Total isolate memory, startup, CPU,
real R2 latency/throttling, deployment contention/failure injection, AI task
execution through Gateway, and sustained compaction/collection need live
qualification. Large builds can exceed admitted Worker budgets and remain an
explicit deployment limitation.
