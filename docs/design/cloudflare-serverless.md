# Cloudflare serverless architecture

Antfly's Cloudflare profile hosts the application and native Antfly WASM in Workers,
uses Durable Objects for exclusive execution, and persists native metadata, WAL,
receipts and immutable indexes in R2. This extends the existing serverless engine;
Lite shares the native storage primitives rather than emulating a filesystem over R2.
AI Gateway uses Antfly's provider support. See the [runnable example](../../examples/cloudflare-serverless/README.md).

This is the current implementation for #1278. Optional acceleration paths require
explicit activation; the full hosted performance gate and historical memory error
remain open. The other current references are [durability and recovery](antfly-object-storage-backend.md),
[performance](cloudflare-platform-baselines.md), [qualification](cloudflare-independent-http.md)
and [memory investigation](cloudflare-sqlite-memory.md).

## Execution and ownership

The split deployment has public app and private writer, reader and builder Workers.
Deploy compatible roles in writer/reader/builder/app order using
`wrangler.split-*.jsonc`. The app authenticates and routes tenant work; the writer
owns metadata admission and commit order; readers execute bounded native queries;
builders perform resumable publication, enrichment and collection. Queues and a
periodic repair trigger schedule maintenance. Failed Queue admission cannot erase
an accepted receipt. The mail application is a [separate example](cloudflare-mail-service.md).

Tenant identity derives from physical actor routing and configured storage scope,
never an unchecked payload. Incarnations are never reused after recreation.
Logical actors may share an isolate, so native kernels, retained bytes and queued
inputs have aggregate module-wide bounds. Cached kernels rebind transport per
request; they cannot retain RPC stubs, timers, callbacks or I/O promises owned by
another invocation. Queued callers receive permits and execute in their own context.

`new_sqlite_classes` registers a supported Cloudflare actor namespace. Native R2
readers/builders do not thereby acquire application SQL authority. The compatibility
profile in `wrangler.jsonc` and the native profile in `wrangler.r2.jsonc` are distinct.
Authority and persisted formats require explicit provision/migration, not flag rollback.

## Shared native engine and provider contracts

`zig/lib/objectstore` owns typed conditional roots, immutable logs, transactional
metadata and the ordered-page engine. Serverless and embedded Lite reuse these
primitives. The serverless graph-page facade delegates to that shared engine.
Lazy key views, bounded navigation, dictionary values, batched read-ahead and
physical value provenance coexist; dictionary-expanded values never claim a
contiguous original-object locator.

R2 and S3 use captured ETags for conditional writes; GCS uses typed generations
and `ifGenerationMatch`. Create-only is a conditional operation, not check-then-put.
S3 version IDs are not ETags. Unsupported revision/delete capabilities fail closed.
GCS generation fast paths avoid an extra metadata request; compatibility ETag
resolution may need one. The current WASM R2 bridge does not implement GCS generation
writes. Native provider support therefore does not establish deployment or latency
parity for the JavaScript host. Qualify S3/GCS separately.

The WASM ABI suspends one Zig stack through Asyncify around asynchronous object
calls. Headers and binary bodies remain separate; outgoing memory views are copied
before suspension and replies are owned. Rewind resumes exactly once. Traps poison
the instance. Limits distinguish conflicts, missing data, integrity failures,
cancellation, admission exhaustion and transport errors.

## Read and write execution

A live read captures published generation and WAL cutoff, validates its required
incarnation-bound visibility fence, then merges published facts with bounded
unpublished changes and tombstones. Cursor pages bind generation, cutoff, query and
last position and reacquire their reader lease or fail. A missing/expired prefix
cannot become a successful stale result. Tokens and cursors are positions, not credentials.
Point/list/text paths and published vector/hybrid admission have distinct freshness
contracts; unsupported fresh operations must not silently use stale indexes.

Native read-state and document-point projections retain immutable derived state.
Caches include provider/bucket/prefix, namespace, lineage and authenticated root
identity. A cache hit is not writer authority or proof of absence. Bounded affinity
prefers compatible idle kernels without waiting for busy ones or bypassing fairness.
Detached metadata and point continuations acquire dependencies before scarce query
execution. Native graph discovery emits exact descriptors; the host deduplicates
and overlaps only currently discoverable dependencies, then resumes the captured
view. It does not guess keys or share request-owned promises across invocations.

Writers prepare immutable data outside the short authoritative commit, then refresh
native admission and atomically publish WAL progress and keyed receipts. Bounded
groups share paths and commit work without weakening all-or-nothing keyed replay.
Model calls remain outside the commit section. A lost unkeyed response is ambiguous;
unkeyed ingestion must not invent an idempotency identity or silently retry.

Publication continues within large records and postings, not just between records.
Prepared slices, adaptive byte/work budgets and packed immutable batches reuse
native decoders and canonical checksums. Small bodies may reuse canonical bytes
only after facts, length and digest match. Metadata hints and physical locators
remain advisory. Sparse/text/vector projections and runs retain bounded traversal;
ANN routing requires distribution-specific, equal-budget recall qualification.

## Resident and cold serving

Resident serving captures immutable MVCC views while the exclusive writer advances
state. Shared kernels, scoped caches and cohort fetches remain bounded. Required
fences and ownership transitions invalidate uncertain views. Dependency loaders
hold physical reservations until actual completion, even after logical timeout;
late resource errors remain reportable.

`RESIDENT_COLD_PIPELINE` adds compressed committed bootstrap headers, combined
metadata projection and cohort-wide dependency discovery. Native decoding validates
checksums, sizes, ranges and conditions. Missing hints fall back to authoritative
lookup. Existing root/frame and request ceilings remain enforced. Byte admission
bounds active responses and retained transcripts; namespace fairness prevents a
large dependency from starving indefinitely. Current variant sets and activation
checks are in `qualification/read-state-latency.mjs`.

`RESIDENT_DURABLE_BOOTSTRAP` additionally uses provider-local opaque Antfly state
under `serialized-owner-v3`. This is a distinct durable/authority protocol, not
R2-only storage or an SQLite-memory fix. Dirty intent precedes R2 advancement;
application acknowledgment still follows R2 commit. Clean checkpoint export is
try-only maintenance, so it cannot hold the metadata queue waiting for foreground
capacity. Full state transitions are in [durability](antfly-object-storage-backend.md).

## Rollout and source ownership

Native runtime flags default off unless a selected example profile sets them.
General hosts explicitly select indexed WAL/read state/document index/managed GC;
existing prefixes must not be silently reinterpreted. Managed reader lifetimes,
HEAD versions and v3 authority persist after flags are disabled. New incompatible
formats require fresh namespaces or coordinated fenced migration.

The elapsed batching-window experiment remains off: paired measurements regress
reads in one order. Do not promote a feature merely because correctness tests pass.
The current gate, measured limits and operator sequence live in the linked references;
source and failed artifacts remain in Git/local qualification evidence, not per-commit
design documents.
