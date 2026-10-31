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
unchanged by this format: [GCS documents a one-write-per-second limit for the
same object name](https://docs.cloud.google.com/storage/quotas#objects). A direct
provider HEAD is not the high-throughput immutable-slot adapter, and these local
results do not establish GCS or S3 performance parity.

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

## Committed local results, October 30

Implementation `9e70fb940c` and harness `215fb1e9c3` were checked with the pinned
release toolchain. Full WASM SHA-256 is
`9711adb54900fe7c494fb16a8fa0675b1e83ca2eba9a598bb9a6b0d79759f02c`.
The control rebuild matches its recorded `2f2ac2ba…` binary; every case uses
identical app/writer/reader/builder JavaScript bundle hashes. The final matrix
contains 34 verified cases, including independent format toggles, reversed
orders, zero/20/50 ms injected binding delay and traced outcome checks.

The strongest paired comparison additionally uses identical per-tenant namespace
and storage keys in fresh isolated stores (`--fixed-namespace`), removing the
variant-specific names used in exploratory runs. Each case has four tenants,
512 1 KiB documents per tenant, 128 fenced reads and 64 writes. Capacities,
visibility guarantees and timeouts are unchanged. Completed fenced views may be
reused within their original lifetime; these are sequential dependency-depth
experiments, not an offered-load throughput test or physical-cold qualification.
Initial namespace probes occur before delay injection.

Ranges below are the two trial orders, not confidence intervals:

| Injected delay | Measure | Control | Packed blocks + deferred indexes |
| --- | --- | ---: | ---: |
| 20 ms | Read p95 | 28.72–30.13 ms | 28.91–29.70 ms |
| 20 ms | Write median | 63.19–64.06 ms | 33.51–34.06 ms |
| 20 ms | Write p95 | 84.38–88.98 ms | 60.00–60.19 ms |
| 50 ms | Read p95 | 58.87–59.22 ms | 58.72–59.30 ms |
| 50 ms | Write median | 123.07 ms | 63.54–63.72 ms |
| 50 ms | Write p95 | 175.80–177.19 ms | 118.23–120.80 ms |

Write p95 improves about 29–33%; write median improves about 47–48%. Read p95 is
essentially unchanged. Observed 20 ms burst writer PUT counts fall from 281 to
77 in both orders. These counters include background activity and can be
captured while such work is still active; they are not a per-write accounting
proof. The native protocol test separately proves that an eligible append uses
one HEAD GET and one CAS without private page uploads.

Separate packing/point-routing toggles do not consistently lower latency, and
some exploratory/zero-delay runs regress. Keep the flags opt-in. The earlier
packed-read failures and the failed validations remain in the evidence set.
Directory payload ownership, allocation-headroom checks, format-aware lease
recognition, coalesced overflow folding and pre-ack projection validation are
part of the committed implementation; larger limits and hidden retries are not
its performance mechanism. No overall twofold or fourfold p95 result is claimed.

Both traced control/candidate cases record zero failed native outcomes and zero
resource failure codes. Regressions pass: 449 Node tests, 42 actual-workerd tests,
one mail integration, 55 focused native checks (the ordinary suite skips its opt-in scale case),
the native bootstrap check, and 183 shared object-store tests (two live-provider
cases skip). Mail retains its pre-existing RPC-result-disposal warning. These
checks do not establish exhaustive production allocator/provider coverage.

Evidence is sealed at
`dist/qualification/compact-serving-1793348334397/evidence.json`: 245 artifacts,
243,508,748 bytes, manifest SHA-256
`7975c3b6ea2faf9a9c30d70e8dcd24de1d72734da360ca7f006773b7f8d1784b`.
Every new artifact was independently hashed after sealing. The seal includes
source, committed modules, the matching reconstructed control, experimental
module catalogs, reports, failed attempts and the prior-closure audit. It
preserves the 163 missing prior artifacts as an explicit availability gap, with
zero hash changes among surviving prior artifacts. It does not rewrite old
closures or include credentials. The disposable control worktree was removed;
`make gc` honored the active-native-build guard and deferred shared cleanup.

No paid Cloudflare resources were created. Actual placement remains unverified.
The hosted gate still requires complete compact/window-indexed diagnostics,
verbose-archive separation, a short paired provider run using fresh probe/cold
identities as appropriate, then the original three paired hour trials and 100
physical-cold epochs. Recovered live `SQLITE_NOMEM` is not declared vendor-fixed.

The separate opt-in 50,000-record native collector regression also passes on the
committed implementation: 25,000 retained records and receipts survive two
cycles, cleanup deletes 7,397 objects, and peak operations/step remain 99.
This is the existing **unpacked** native profile; it does not establish the new
packed/deferred layout's 50,000-record or hosted behavior. Its log, test source,
compiled native binary and summary are independently sealed as four artifacts
(7,383,987 bytes) at
`dist/qualification/compact-serving-scale-1793349006920/evidence.json`, SHA-256
`8ab6559cb29a3f57c10e18a173ba63f6cf32686a0d7dcd095af130848e3fde5a`.

The next diagnostic implementation is described in
[bounded, windowed serverless diagnostic evidence](serverless-windowed-diagnostic-evidence.md).
It adds opt-in immutable minute partitions, a separate sanitized verbose archive,
file-backed window catalogs and strict independent-counter reconciliation. The
hosted gate remains pending a new paired provider run; historical failed
captures are preserved.

The new opt-in packed/deferred 50,000-record native regression passes as well.
It retains and verifies 25,000 exact payloads, replays the original receipt, and
checks the newer indexed point record after two collection cycles. The large
128-record, 1 KiB seed batches use materialized trees; the small concurrent
newer record exercises the eligible deferred tail. Both format flags remain
explicitly enabled. Results: 843 first-cycle steps, 397 second-cycle steps,
5,589 sweep deletions, 7,281 cleanup deletions, peak 116 operations per step,
and 1,963 objects after final cleanup. The native runner reports one pass,
zero failures and zero allocator leaks. This is a deterministic in-memory
object-provider correctness test, not a throughput or hosted latency result.
