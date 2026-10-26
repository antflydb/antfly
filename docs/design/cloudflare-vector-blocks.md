# Shared object-backed vector serving

This design connects the existing Antfly serverless publication, immutable page,
vector-block codec and read-protection capabilities. S3, GCS and R2 use the same
native representation and correctness rules. Workers supply bounded execution
and object transport; they do not own a separate vector database.

## Representation and publication

Paged vector root format 3 retains the existing manifest artifact type/version
and routing trees. Readers continue to accept formats 1 and 2. Format 3 owns an
oldest base and at most three newer immutable sorted runs, a logical projection
count, and an optional merge checkpoint. Manifest order defines recency. An empty
run value is a tombstone; equal keys select the newest run. Multiple projections
of a document retain their existing 36-byte routing/ordinal keys.

Each live compact row contains its document identity, a scaled FP16 candidate
plane, conservative quantization metadata, a lossless residual locator, and its
source document LSN/projection ordinal. The shared native `vector_block` codec
encodes residuals and reconstructs every original FP32 bit. Extreme values that
cannot use the bounded compact representation retain legacy inline FP32 storage.
This is a lossless fallback, not a precision concession.

Residual packs contain at most sixteen records and 128 KiB. Each pack has a
namespace-scoped, immutable SHA-256 identity. Readers validate its length and
complete digest before admitting it to a cache or decoding any source vector.
Upload identity and payload checksum must agree before a locator is published.
New writes append a small sorted run; they do not rewrite the vector base.

Vector leaves opt into the shared ordered-page codec's lossless value dictionary.
The 56-byte residual identity occupies a fixed field in every compact row. A
page stores each distinct identity once and replaces its occurrences with one-byte
tags. The authenticated wire format declares the field offset, width, dictionary
and expanded value length. This removes repeated identities without truncating
checksums or weakening namespace isolation. Pages use the dictionary only when
it saves bytes; mixed legacy rows and tombstones can retain ordinary encoding.
Reconstructed keys and values share an arena bounded by the existing 1 MiB page
expansion limit. Decoded cache leases own that arena and retain normal eviction,
integrity and cancellation behavior. Existing pages remain readable; old readers
reject the new wire flag, so reader rollout precedes enabling these writers.

Routing still uses the source-FP32 shard size policy, so changing physical row
encoding does not silently change clustering. Serving clusters use their existing
FP16 scoring fallback without a duplicate RaBitQ payload. Legacy/native flat
segments keep their previous default quantization policy. Queries allocate the
quantizer only when a segment actually carries that representation.

Local clustering retains source FP32 inputs to preserve qualified ANN behavior.
An evaluated candidate-only rebuild lowered synthetic eight-probe recall from
0.45 to 0.44 and was rejected. Its lower publication I/O did not satisfy the
unchanged quality condition. Bounded page geometry addresses publication
amplification while preserving the original clustering inputs.

## Bounded merge maintenance

At four serving runs, publication first performs maintenance. It chooses adjacent
runs with the closest record-count ratio, then the smallest combined count. This
avoids repeatedly merging a large run with tiny incoming updates. Maintenance
consumes no WAL records and preserves the published visibility watermark.

A merge step admits at most 1,024 merged keys, including discarded tombstones,
and approximately 1 MiB of control
rows, 1 MiB of residual output, and 64 distinct source packs. An indivisible row
can cross the control-byte target. Source-pack bytes are reserved before further
tree reads; the step has 4 MiB read and write allowances. The existing transport,
working-memory, cancellation and lease limits still apply.

Rows merge in key order, with newer values overriding older values. Tombstones
are removed only when the selected pair includes the oldest base. Residuals are
verified and repacked in the same order without changing candidate codes,
certificates or original bits. This makes original-value reads local to the
serving ranges rather than to historical publication batches.

The checkpoint records exact immutable input references, private output and an
exclusive key. Its output uses the shared sorted-tree builder with a 128 KiB page
target. Subsequent steps append to that tree without reconstructing its complete
prefix. Checkpoint writers explicitly disable inherited bulk page size classes:
an ordinary COW updater preserves historical 256 KiB pages, but that default
must not promote a requested 128 KiB checkpoint page on its next append. The
explicit target bounds unnecessary range reads during publication while keeping
the shared engine's default update policy intact. Indivisible records retain
the existing maximum-page rule. Inputs remain query-visible until completion; partially merged output is
never mixed into query results. Completion atomically replaces the input pair
through ordinary fenced manifest publication.

Hosted execution yields at every checkpoint. Native callers using the ordinary
whole-prefix builder retain that contract by draining maintenance before consuming
their next WAL prefix. A completed final-index checkpoint reports no pending
maintenance, avoiding an empty hosted rescan when the WAL is also caught up.
Native draining tracks performed work separately from pending work. Deletion-only
prefixes advance the exclusive key even when their private output is empty.
Reindex drains the same staged maintenance through its
existing guarded checkpoint pointer. Readers and collection trace all serving
runs and partial output. Failed uploads or a lost publication race leave scoped,
collectible orphans; they cannot expose a partial replacement.

## Certified exact refinement

Exact scanning evaluates compact candidate intervals. A request-local heap tracks
the best upper distance bounds for distinct documents. Its kth bound certifies
that k distinct prefix documents have an exact distance no greater than that
threshold. A candidate can be discarded only when its lower distance bound is
strictly greater. Ties, degenerate norms and non-finite intervals refine.

The bounds combine the shared codec's quantization certificate, the shared vector
distance bound and an additional dimension-scaled arithmetic allowance. Existing
routing spread estimates remain heuristics and are never used as exact pruning
certificates. Approximate scores never enter the verified result heap.

The ambiguity window holds at most 128 controls and 64 KiB in a request. Every
new source pack reserves its worst-case bytes and read admission before more
pages are consumed. Remaining refinements are grouped by immutable source pack.
An optional document-body source can spend only surplus admission, preserving
all residual reservations as a guaranteed fallback.

Native continuation format 2 can carry at most 32 KiB of unresolved controls,
encoded as base64, alongside the verified heap. Generation, source checksum,
query identity, cutoff and exclusive progress remain bound by the app's HMAC.
Decimal counters satisfy:

`refined + certified_pruned + pending = visited projections`

Large ambiguity windows resolve before yielding. Final responses always resolve
all remaining candidates and expose no partial hits. Native state admission
counts the encoded heap and pending payload and leaves space within the unchanged
128 KiB app payload limit. Format-1 continuations remain readable. The ordinary
512-new-record, dimension-adjusted scan slice, 128 admitted tree/source reads,
4 MiB read allowance, one-second cooperative scan target and five-second native
request deadline remain. Restoring deferred work is separately bounded by the
32 KiB retained-control limit; no cumulative request budget is reset.

## Reusing authoritative sources

Compact rows record their source LSN and projection ordinal. During final
refinement, an immutable document body may supply the original vector when its
facts revision matches, the published index definition fingerprint matches, and
the same native source selector resolves the recorded projection. Source LSNs
come from materialized WAL documents, never from user-supplied metadata.

This avoids a separate residual fetch followed by another fetch of the winning
document body. It is optional: missing source metadata, changed revisions,
unresolvable projections and expensive bodies use the co-published residual.
Integrity failures and cancellation propagate. Facts pages, original-body reads
and residuals share the same request counters and byte budget; optional source
work cannot consume admission reserved for required residuals. Final body
materialization keeps its existing 1 MiB result-body limit.

The residual cache owns no request-external pointers. It admits at most 4,096
entries, with explicit data budgets: 512 KiB for query refinement, 2 MiB for
publication and 4 MiB for maintenance. Namespace domain is part of cache identity.
Every hit still checks cancellation/authority. Failed validation publishes no
cache entry.

WASM kernels additionally reuse the native read-lease cache between requests.
The lease retains its original durable and suspend-inclusive deadlines. Every
resumed session checks retirement; expiry/reuse thresholds force reacquisition.
Changing bucket or prefix clears both decoded manifests and read protections.
No mutable HEAD or WAL frontier is cached across requests.

## Qualification and rollout

Publication admission must separate fixed transport latency from per-prefix
resource pressure. The hosted coordinator adapts its record prefix using native
operation count, transferred bytes, peak working memory and elapsed execution
excluding measured I/O wait. These are conservative feedback signals, not a CPU
billing measurement. Operation, transfer and memory guard bands sit at 75% of
the existing hard limits; those hard limits remain authoritative. A short WAL
tail cannot demonstrate that a larger prefix fits. Only a complete admitted
prefix with low resource use grows the budget, up to the configured maximum.
Queue wait and early cancellation do not shrink it; actual native resource or
execution-budget exhaustion does. This prevents slow fixed network overhead
from trapping publication at one record.

The hosted metadata commit also protects its newly visible generation for
readers. The generation pin, HEAD change and publication-lease release commit
atomically through the native coordinator store. An ordinary first reader can
then capture a freshly observed read-only view without joining the writer queue
to create that pin. Individual HEAD CAS calls for compaction, reindex and legacy
publication establish the same protection in their transaction. Explicit
historical reads and expired protections still use
the normal fenced admission path. No mutable frontier is retained across reads.

Read admission starts when the bounded actor request is accepted, before its
native execution slot becomes available. Each request reserves 32 KiB for its
four metadata records and retains its original admission time and deadline.
Requests arriving before a cohort's observation starts can share that fresh
observation. Later arrivals start another observation. Two reads may execute
through the existing two-slot kernel pool; each WASM instance remains serialized.
Builders still execute one operation at a time. The eight-request actor bound,
input byte budgets, reader working/linear limits and pool memory caps remain.

Bulk collection work checkpoints within a 64-mutation metadata quantum. Fanout
discovery persists an exclusive child offset together with its newly discovered
children. A parent stays pending until every child is discovered. An enumeration
digest detects changed child order or content; discovery restarts conservatively
and retains previously discovered children. Pins are inserted in batches and
cleanup deletes bounded batches. Metadata-tree tracing likewise checkpoints its
child offset. A 250 ms soft target and waiting foreground work determine whether
another complete task starts. Individual I/O and commit work can overrun that
soft target; the existing hard request budgets still govern execution. Native
publication prefix sizing continues to use measured resource pressure.

Deploy compatible readers and collectors before new writers. Older readers must
fail closed on format 3 or format-2 continuation state. Existing roots remain
readable; explicit checkpointed reindex can migrate them. No query performs a
whole-index migration. Keep independent FP32 oracles, near-tie cases, multiple
projections, source-revision mismatch, eviction, cancellation and collection
coverage in regression tests.

ANN pairs must use identical corpus, dimensions, body size, publication prefix,
recycling, object limits, compaction settings and held-out query populations.
All continuation requests and maintenance operations count. Physical GET/byte
measurements remain separate from native transfer accounting. The original
no-extra-GET/no-extra-byte thresholds and paired live/recovery requirements remain
unchanged. A passing local corpus is not a live gate pass.

Exactness can require refining every vector for an ambiguous distribution. The
bounded exhaustive fallback remains necessary; no universal FP16-cost guarantee
is claimed for FP32-exact answers. Qualification applies to the frozen workloads.


Native object-store I/O also needs bounded parallelism below the Worker adapter.
The shared ordered tree now buffers at most eight private COW pages and one MiB
of page bytes. Traversal can read those private pages before upload. Flush groups
use the existing optional batch-PUT capability; clients without it upload
sequentially. Transactional metadata and indexed WAL paths flush all referenced
pages before publishing an authoritative root or HEAD. A partial upload leaves
scoped orphan objects, and a failed flush cannot publish its root. Immutable
collision recovery verifies existing bytes; any missing-object re-upload consumes
a separately charged write attempt. Physical I/O budgets retain their meaning.

A separate opt-in indexed-WAL HEAD format 3 includes one owned, checksummed latest
record of at most 1,536 bytes. The record and operation-ID trees remain complete.
The inline copy commits with the same HEAD CAS, includes its LSN, and is validated
against the HEAD's latest LSN and logical retention boundary. A single-record
latest-tail read can use that HEAD alone; larger records and other ranges use the
existing trees. The HEAD still fits its 4 KiB limit; an oversized serialization
falls back to legacy format 2. Cancellation and count/byte limits still apply.
This WAL format number is independent of vector-run format 3.

This is a native serverless capability shared across object-store providers.
Cloudflare selects it with `WAL_INLINE_TAIL=true`; it defaults to false. Deploy
compatible readers and collectors everywhere before enabling new-format writes.
Qualification first deploys every role with the option disabled, then probes the
sealed full and compact binaries together against an in-memory object binding,
and only then enables the option. Readiness is false during activation. Rollback
to a reader without the capability is disallowed after activation; disabling the
flag alone does not migrate already-persisted format-3 HEADs. Qualification ANN
reports must match the deployed option as well as the exact native hashes.


Decoded coordinator rows can also be reused after a fresh native `observe` has
validated the authority. Every cache access checks revision, epoch and owner
against that observed state, synchronously copies only requested rows, and falls
back to reading the observed immutable root if a concurrent writer has advanced
the cache. An observation never skips fencing or serves an uncommitted overlay.
Migration state and HEAD/WAL/floor metadata are requested together so independent
metadata reads can share a native lookup wave.

The hosted immutable cache opts into write-through for confirmed successful
create-only uploads. A failed or ambiguous response adds nothing. Mutable HEADs,
checkpoint floors, non-create-only writes and oversized entries remain excluded.
Copied page bytes and synthesized internal metadata are bounded by the same
8 MiB charged budget. Only these internal page readers opt in: modification time
is unknown until fetched from the backend. The generic cache retains its legacy
defaults. Hosted caches allow 1,024 entries instead of 128 to use the existing
byte budget for small pages; every key, metadata object and entry has an overhead
charge. The direct native ANN fixture retains the conservative 128-entry,
GET-populated cache and resets it for every held-out query.


The split writer partitions its two compact metadata slots: one for mutation
transactions and one for read-only observations. Together with its existing full
WAL-preparation slot, the writer retains the 64 MiB pool ceiling. The shared
single-script mode keeps its original layout. The observation lane cannot write,
repair, checkpoint or acquire an epoch. This isolates fresh reads from a
suspended metadata mutation without changing authority or fencing semantics.


The WAL-preparation pool uses weighted reservations within its existing 32 MiB
ceiling. A single mutation envelope of at most 16 KiB can use the compact native
binary, an 8 MiB working budget and a 16 MiB linear reservation. Two such prepares
can overlap. Larger groups retain the full binary, the existing working budget
and an exclusive 32 MiB reservation. Metadata mutation and observation each keep
a separate compact slot, so total writer pool memory remains bounded by 64 MiB.
Idle retained hosts count toward reservations and are evicted before changing to
a larger variant. An aged large request drains smaller work, and cancellation
removes its reservation before allowing other waiting jobs to proceed. Every
prepare still writes private pages first; authoritative commit and receipt
fencing remain unchanged.


Merging main's media provenance requires facts format 10 (`AFDFACTA`). New
576-byte roots retain the audit tree and all primary/pending trees. Packed facts
use a 184-byte encoding: main's dependency, audit and admitted-source fields
remain in their original positions; a separate flag and 40-byte pack locator
follow them. Readers retain the deployed 80/120-byte facts and 512-byte v3/v4
roots, plus main's 80/136/144-byte facts and 576-byte v4–v9 roots. Root size
disambiguates the two historical v4 encodings. No old fields are reinterpreted.

Collection walks the audit tree and retains audit bodies. Dependency bodies
become one checkpointable task per body; each task discovers its base and
parent, without a whole-ancestry traversal. Facts fanout is capped at 95 entries
so two references per fact plus continuation remain within the existing
192-child journal batch limit. Bodies with dependencies are excluded from new
body packs. New readers and collectors must precede format-10 publication;
disabling body packing does not downgrade the root format.

Recent WAL checkpoint and enriched mutations retain their typed identities until
needed by a read. The live overlay resolves a requested derived body once from
the pinned session, charges the result against its admitted byte budget, and
retains the owned canonical result. Counts use identities without materializing
bodies. Replaced mutations need no hydration. Public mutation validation rejects
internal checkpoint forms before any idempotent batch append.


The observation tier uses the same native code with a separately enforced 8 MiB
WASM memory maximum and a 4 MiB per-request working budget. Two observation
slots replace the previous single 16 MiB slot. The 16 MiB mutable metadata slot
and 32 MiB weighted WAL pool keep the writer total at 64 MiB. The generic
single-script layout stays unchanged. Build output includes
`antfly-serverless-observe.wasm`; sealed candidate provenance checks its hash.
An actual populated-tree fixture validates parallel reads, owner fencing and the
module's rejected memory growth beyond its maximum.


The final split-writer layout shares a 48 MiB reservation pool between WAL
preparation and mutable metadata, alongside the two 8 MiB observation slots.
Three compact 16 MiB operations can overlap; a full 32 MiB WAL preparation can
coexist with one compact metadata operation. Metadata and WAL never hold a
kernel while awaiting another request on that pool. Durable commits take the
next available reservation ahead of new preparation, with the existing aging
rule preventing starvation. Idle matching variants are reused. Retained idle
hosts and pending instantiations count toward the same bound. This replaces the
fixed WAL/metadata partition while preserving the writer's 64 MiB ceiling.

A cold metadata reader uses native `observe_committed`, which authenticates a
fresh checkpoint and the immutable committed prefix without acquiring writer
ownership. It follows valid seals across owner changes but cannot repair hints,
rotate epochs, checkpoint or PUT. Existing owned sessions continue using
`observe` and remain fenced when ownership changes. Decoded rows still require
an exact revision/epoch/owner match after fresh observation.

Unowned cursors are advisory and retained for reuse only after a committed slot
(`next > 0`). A freshly sealed epoch may have no START hint after a partial
upload; reopening it from the checkpoint preserves authentication. A later
writer acquisition may reuse the authenticated cursor, but still performs its
own ownership transition and fencing. Four bounded observation calls share one
ledger of fewer than 254 operations, 8 MiB transferred, a 4 MiB native working
budget and the original five-second deadline. Exhausting the admission budget
returns to the existing bounded coordinator admission path; corruption and
fencing errors remain errors.

After an authoritative create-only seal succeeds, its independent START and
SEALED recovery hints use the shared optional batch-PUT operation. A fresh
random epoch does not require a preceding START GET. Conditional collisions
still verify the existing objects, and ordinary recovery repairs partial hint
uploads from the accepted seal. No root, commit receipt or storage encoding
changes. The regression suite exercises read-only recovery with both hints
missing, forged cursors, old-owner fencing and a batch failing after its first
hint succeeds.

Split readers now authenticate the committed metadata prefix directly from R2
using the same native observer, rather than requiring a writer RPC for every
view. The reader checks an existing deployment authority without creating it,
checks migration state in the observed tree, and captures HEAD, WAL root,
retirement floor and the existing generation pin from one committed revision.
Fresh cohorts close before observation begins. No mutable HEAD or frontier is
cached across requests. A missing authority, exhausted bounded observation or
pin needing extension falls back to coordinator admission; corruption, fencing,
transport and migration errors fail closed. A pre-cutover authority is refreshed
on the next attempt so a reader can observe completed migration.

Reader metadata and query execution share a weighted 32 MiB pool: four physical
slots allow up to four 8 MiB native observations, two 16 MiB queries, or a query
alongside observations. Retained idle instances count toward the same ceiling.
Metadata preparation releases its host before query admission, so no request
holds a host while waiting for another host from the pool. The original request
deadline and captured view remain unchanged while queued. Cursors requiring
historical admission keep the existing coordinator path.

Metadata log collection overlaps up to eight immutable proof GETs within each
32-key batch. The native retirement operation still reads a fresh checkpoint
and validates each object's identity and revision before allowing deletion.
Speculative keys are restricted to this store's epoch namespace and charged
conservatively as extra operations plus 16 KiB per key. Results and failures
exist only for that proof request. Outstanding work drains before the request
returns, including on invalid-checkpoint failure; no negative result survives
into another transaction. Native fencing and physical deletion ordering are
unchanged.

Warm unowned observations use the same bounded contiguous read-ahead windows as
cold recovery, starting from the newer of the caller's advisory cursor and the
fresh checkpoint. Valid seal hints advance speculation across epoch changes;
they grant no authority and native authentication still decides which state is
committed. Only contiguous immutable commit witnesses can cover a negative seal
probe. Holes, errors and window boundaries require a fresh seal observation.
Mutable-owner recovery keeps its previous speculation policy and operation
bound. A regression follows 80 new commits and a writer replacement without
reverting to one serial seal/slot GET pair per commit.

Point lookups choose a reader slot by a stable UTF-8 document-ID hash. This
retains immutable page/body locality across app isolate restarts and avoids
restarting a volatile round-robin counter for each cold app isolate. Other read
operations retain bounded round-robin routing. The number of readers, actor
admission limits and memory ceilings do not increase.

Physical-epoch tests now distinguish observation from mutation: an old read-only
observer may read a freshly authenticated prefix after writer replacement,
while the old writer must reject even an idempotent mutation replay. The fixture
also checks three local read preparations start while two native queries are
held, with no additional writer RPC, and asserts the reader's four slots remain
inside its original 32 MiB reservation bound.

Idle recovery checkpointing starts after one new committed revision rather than
eight, once the foreground queue drains. It remains an asynchronous hint after
the caller's acknowledgement, with one coalesced task, a 250 ms idle delay and
one bounded retry after native throttling. Native checkpoint CAS and its
one-successful-overwrite-per-second limit remain authoritative. Busy writers
continue relying on bounded log recovery and their existing periodic hint
attempts. This spends additional metadata GETs/PUTs during quiet write traffic
to avoid repeatedly replaying a larger metadata tail for later readers. A test
proves a single acknowledged commit advances the durable recovery floor without
requiring another foreground commit.
