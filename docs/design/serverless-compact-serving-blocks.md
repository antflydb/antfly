# Compact serving blocks and native execution

Tracks the next performance work for [#1278](https://github.com/antflydb/antfly/issues/1278)
and [PR #1320](https://github.com/antflydb/antfly/pull/1320). The implementation
is opt-in and requires local and provider qualification. Local experiments do
not establish another twofold improvement or a hosted gate pass. Existing evidence closures remain immutable.

## Measurement and activation

Separate CPU/dispatch from storage dependency depth with the same corpus and
zero, 20 and 50 ms injected binding delay. Record app/writer/builder physical I/O
at zero delay too, and preserve first-use samples separately. The experiment
harness records bundle hashes and can use the sealed prior native modules as a
control under the same JavaScript. Compare new native code with both new formats
disabled, then enable the formats independently and together. Repeat orders.

The initial prior-module zero-delay leased burst has write p95 about 16 ms;
50 ms binding delay raises it to about 184 ms. Physical writer counters show
roughly four uploads per write in the zero-delay sample. These distributions
include background work, are not additive phase accounting, and do not by
themselves establish a causal speedup for any new implementation.

New activation settings default off:

- Worker `WAL_PACKED_PAGES=true`; native `--wal-packed-pages true` or
  `ANTFLY_SERVERLESS_WAL_PACKED_PAGES=true` with indexed WAL enabled.
- Worker `WAL_DEFERRED_INDEXES=true`; native `--wal-deferred-indexes true` or
  `ANTFLY_SERVERLESS_WAL_DEFERRED_INDEXES=true` with indexed WAL enabled.
- Worker `POINT_READ_BLOCKS=true` with canonical bodies enabled; native
  `--point-read-blocks true` or `ANTFLY_SERVERLESS_POINT_READ_BLOCKS=true`.

All readers, writers, publishers, enrichers and collectors must support the new
formats before activation. A disabled activation flag still reads and maintains
an already persisted new format. Original layouts and resource capacities remain
supported. New formats fail closed in older binaries.

## One immutable object for an unpublished forest

`objectstore.ordered_tree.Forest` buffers at most eight pages and 64 KiB before
ordinary overflow flushing. At the visibility boundary it can relocate the
remaining private data, routing and receipt pages into one content-addressed
object. It releases updated root references only after successful upload and
cancellation checks. Large pages and providers without packing retain ordinary
writes. Encoding expansion has a separate bounded allowance.

A child inside the same pack uses an authenticated backward-relative locator.
The wire representation contains its page digest, extent and enclosing-pack
length, without embedding that enclosing pack's digest into its own hash. The
reader resolves the physical pack from the parent reference, enforces the same
attempt/scope, and rejects forward edges, out-of-bounds extents and detached
relative pages. Every page still has its own complete digest/domain/structure
validation. Existing absolute locators retain their encoding.

WAL HEAD format five persists the packed-page contract. A request-owned small
pack cache can service multiple page reads from one verified physical object.
Its allocator is the enclosing operation's allocator, never a temporary tree
update arena. Whole-pack caching also preflights the remaining native allocation
headroom, leaving room for the selected page and decoder. A narrow point lane
falls back to its authenticated byte range instead of allocating unrelated
siblings or requiring a larger working-set limit. Rejected-growth and backing
allocation-failure byte metrics distinguish the two resource failure classes. Native collection job format two counts logical seen pages and
physical retention marks separately; multiple reachable pages in one object must
all be traversed. Unpacked job format one keeps its previous representation and
count validation.

## Bounded durable deltas and deferred index folding

WAL HEAD format six can keep up to eight records and eight duplicate receipts
in a single owned 1.5 KiB binary delta, base64-encoded within the unchanged 4 KiB
HEAD ceiling. The existing authority CAS publishes records, high watermark and
receipts together. An eligible append does not upload record, document-index or
receipt pages before acknowledgement. Operation IDs above 128 bytes, larger
bodies/batches, an exhausted delta, or an oversized final HEAD take the existing
materialized path. Limits are admission bounds, not silent truncation.

Point and range reads combine the captured immutable base with the exact
contiguous delta interval. They keep the original record and byte budgets,
validate the complete wire structure and return owned payloads. Point lookup
checks newest pending mutations and deletions before the base projection.
Duplicate lookup checks the delta before the receipt tree. Projection-enabled
appends validate mutation payloads before acknowledgement. Activating projection
over older opaque WAL records starts at the new accepting batch; folding does
not retroactively index those records. Reopening without
activation retains the persisted format and bounded overflow behavior.

Folding writes normal authenticated copy-on-write trees and then replaces the
HEAD conditionally without advancing its logical LSN. It clears incremental
parent hints because the physical root changes at the same LSN. Failed uploads
leave the original delta authoritative; a lost fold acknowledgement is safely
retryable. Overflow folds in the accepting commit, providing bounded
backpressure. Pending and newly accepted records/receipts share one tree update
in that commit; intermediate folded roots are not uploaded first. Managed collection folds before capturing its roots, so receipt
retention and epoch fencing include every accepted operation.

Native publication uses the existing guarded maintenance owner and cancellation
bridge to fold in the background. Hosted prepared publication stages the fold
as an explicit optional WAL write; ordinary prepared publication still cannot
write WAL authority. Finalization applies that fold only if the captured WAL
ETag still matches. A concurrent append keeps its newer WAL unchanged while the
publication can commit its original prefix; a later pass retries the fold.
Publication generation, pin and lease dependencies remain strict. The shared
host read-lease recognizer supports HEAD formats five and six; native
certification, visibility requirements and the original expiry remain necessary. This avoids
turning every new append into a publication conflict or performing an unguarded
builder write. All started native I/O still joins through the existing client.

The byte bound means eight records is a maximum, not a guaranteed amortization
factor. Larger workloads and sustained concurrent writers can force foreground
folds. Report their latency and deferred work rather than claiming a one-upload
write for every payload or provider. Native GCS hot-key constraints also remain
unchanged by this format.

## Direct point routing and canonical bodies

Document-facts format thirteen preserves full packed locators for its primary,
pending and audit roots. It can embed up to 4 KiB of an authenticated primary
routing page in the immutable facts control object. Only routing is duplicated;
canonical small bodies remain in their leaf. Request caches own the optional directory payload, while semantic root
descriptors and retained preparations copy only its length and authenticated
references. The admitted facts root binds the
inline routing bytes to the exact page digest and domain. Existing hint budgets
are unchanged; an oversized control object falls back to ordinary object reads.

A request-local reader can use the directory without a routing fetch, then fetch
the selected leaf or small physical block. Small canonical bodies materialize
from that verified leaf. Large bodies retain external immutable references.
Physical canonical locators include the leaf digest and extent within a pack,
so enrichment/audit/checkpoint processing can authenticate and retain the same
body after publication, flag disable, or restart. The fresh WAL overlay remains
authoritative for newer mutations and deletes.

## Typed execution and native I/O scheduling

Native WAL preparation now reads captured transactional values directly, avoiding
an intermediate JSON result and parse. A compact-record mode also avoids
expanding a hot UTF-8 WAL HEAD into decimal byte arrays before parsing it again.
The captured root remains a preparation hint: final native commit still validates
its semantic dependencies and expected revision.

Metadata and WAL page stores expose native batched reads through the shared
`Client.getObjects` capability. Batches contain at most four children and 256 KiB;
larger individual pages use the existing single-read path. The host pays each
physical child from the original request's budgets and joins started I/O before
failure. Providers without the capability retain serial behavior. Scheduling
occurs where native execution already knows the dependency, without an extra
JavaScript planning invocation.

## Placement evidence

`DO_LOCATION_HINT` can select compute placement independently of the bucket's
`R2_LOCATION_HINT`; absent an override, previous actor identities and hints are
preserved. Runtime generation and compute hint select fresh compute identities,
while durable namespace/storage identity remains unchanged. Placement provenance
records an explicit override.

The authenticated `POST /_qualification/placement` endpoint is enabled only by
`QUALIFICATION_PLACEMENT_PROBE=true`. It performs read-only authority HEAD probes
from the app and each runtime role, returning storage latency, RPC duration,
worker versions, configured hints and the app's observed ingress colo. It does
not initialize database authority or issue writes. Actual bucket/actor physical
regions remain unknown when not exposed by the provider.

`node qualification/probe-placement.mjs <base-url> <token-file> <report.json> [tenant]`
collects bounded samples, preserves partial failures and rejects changing
configuration/version identities. It never includes credentials in the report.
These probes instantiate/warm the addressed actor identities and cannot be
reused as a physical-cold qualification population.

Cloudflare documents that [DO creation hints](https://developers.cloudflare.com/durable-objects/reference/data-location/)
and [R2 bucket hints](https://developers.cloudflare.com/r2/reference/data-location/)
are best effort. Existing DOs do not relocate when the hint changes; reusing a
bucket name does not reselect its original location. Consequently a hint is not
proof of colocation. Provider trials must compare observed hop/storage latency
with fresh compute identities and preserve storage provenance.

## Evidence integrity and reproducible control

On October 30, the local audit found 163 missing artifacts among 1,295 entries
in ten earlier closures (159 WASM copies and four native test binaries). Their
manifests and remaining files were not rewritten. Those closures cannot currently
be described as fully verified locally. The audit records exact missing paths;
this is an evidence-availability gap, not a retroactive change to reported runs.

A separate detached worktree rebuilt control commit `ceb03140d7` using the pinned
Zig and Binaryen toolchain. All three hashes match the recorded control built
from `15d9857127`, including full WASM
`2f2ac2ba34bf5b0e4e2fa8cd1b99dcd23d28a1459da9f076d5fef08952047129`.
The regenerated matching modules live in a new recovery directory with a rebuild
log and attestation; no old evidence paths were repopulated or relabeled.
New comparisons must include these modules in their own complete evidence closure.
