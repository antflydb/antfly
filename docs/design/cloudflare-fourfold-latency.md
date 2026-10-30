# Fourfold read and write latency work

Issue [#1278](https://github.com/antflydb/antfly/issues/1278), parent
[PR #1320](https://github.com/antflydb/antfly/pull/1320). The user authorized
implementation of these next steps after the 2026-10-29 short paired workload.
This file records the target and safety constraints; an implementation or local
measurement is not a claim of hosted qualification.

The next opt-in implementation and its measurement contract live in
[compact serving blocks and deferred folding](serverless-compact-serving-blocks.md):
shared immutable-forest packing, authenticated inline point routing, bounded
HEAD deltas with guarded background folding, typed metadata reads, native I/O
batches, and independent compute/storage placement probes. The same document
records the October 30 discovery of missing local evidence binaries and the
hash-identical control rebuild. Earlier full-closure verification statements
below describe their recorded verification time, not current file availability.

## Starting evidence and target

Sealed candidate `543a3ea27a` measured read p95 2,167 ms and write p95 2,669 ms
(72 reads and 24 writes) against approved control `c44896ac8c` at 6,136/6,797 ms.
Both sides verified 2,052 bodies and eight receipt replays, with no client or
recorded client-side resource errors. Whole-run diagnostics did **not** qualify:
the collector rejected an ingestion, lost Tail deliveries, exceeded whole-source
retrieval limits, and its driver later failed on a DNS lookup. All original
reports and captured failures must remain intact.

The fourfold targets are read p95 <=541.75 ms and write p95 <=667.25 ms, with the
same corpus, offered load, correctness and memory limits. These are stronger
than the original 1,000/3,000 ms gate; its populations and requirements remain.

Received candidate spans locate actual work rather than admission pressure:

| Received phase | Samples | p95 |
| --- | ---: | ---: |
| Read local view opening | 72 | 556 ms |
| Total read admission | 72 | 1,652 ms |
| Point-read native I/O wait | 72 | 690 ms |
| Mutation admission queue | 24 | 2 ms |
| Writer authority load | 24 | 123 ms |
| Native immutable WAL preparation | 24 | 809 ms |
| Native finalization | 24 | 1,183 ms |
| Recovery refresh | 16 | 1,272 ms |

Spans overlap and must not be added. There were zero completed-view read cache
hits, 32 coalesced lease admissions and nine read pin fallbacks. Offered rounds
are 30 seconds apart and advance visibility fences; extending a stale cache TTL
cannot satisfy a stronger fence. A fresh committed serving cut and a cheaper
commit protocol are the substantive next steps.

## Native serving and metadata authority

Keep immutable WAL records, receipts, search segments and page trees in the
existing native engine. Make the small committed metadata root directly
servable. A fenced read should need one authoritative cut fetch and one point
body fetch, with independent immutable work overlapped. Latest reads continue
fresh authentication. Every query retains native namespace, visibility,
integrity and lease validation; expiry and new fences remain mandatory misses.
The published cut must represent accepted state, never uncommitted preparation.

The selected high-throughput R2/GCS design is the explicitly activated bounded
immutable journal described below. Its short native certificates and persisted
collector grace remove redundant fencing reads without overwriting a hot root
for every commit. Existing namespaces retain their original protocol.

The optional conditional-root comparison uses existing opaque provider write
revisions (R2/S3 ETags, GCS generations). A native authenticated
continuation binds namespace, owner, metadata root, semantic dependencies and
its exact revision. All metadata changes and receipts publish through one
conditional root replacement. Cold reconstruction reads that root directly.
Retained continuation proofs contain bounded data, never request contexts.
A native proof cache can eliminate redundant source reads while a miss verifies
from storage. The authoritative root is never garbage-collected or restored to
an earlier revision; a retired namespace has a durable fence and cannot be
silently reused. Old journal namespaces keep their existing protocol.

The existing append-only log **cannot** simply drop its post-PUT floor check.
Its collector can remove old slots; a delayed writer can recreate one, an ABA
case explicitly guarded today. An optimized log continuation would need a proven
bounded root lifetime/retirement contract or an equally strong atomic guard.
A directly conditional metadata root is an alternative that avoids cross-object
fencing dependencies. Do not remove existing fencing reads until fault tests
prove the replacement across takeover, GC and ambiguous responses.

## Provider throughput constraint and selected immutable path

Cloudflare documents [one write per second to the same R2 key](https://developers.cloudflare.com/r2/platform/limits/).
GCS documents the same [one-write-per-second object limit](https://docs.cloud.google.com/storage/quotas).
A permanent conditional ROOT cannot be the high-throughput default on either provider. It is
retained as an explicit local/provider comparison, not selected for hosted
qualification. Local binding timings do not simulate this provider throttle.
The existing journal and its one-second checkpoint throttle remain the default.

The R2 scaling path is `METADATA_ROOT_BACKEND=bounded-journal` for **new**
namespaces. Its persisted `r2-bounded` authority requires policy v2. Commits keep
using different immutable next-slot keys; ownership seals compete for that same
slot. Native HMAC certificates bind bucket/prefix, source state, owner, private
binding scope and both wall/monotonic anchors. They live at most 30 seconds under
the persisted >=60-second collector grace. Native code validates before AND
after PUT. A delayed write may recreate an old orphan after collection, but it
cannot receive a successful acknowledgement. Identical pre-existing bytes are
not a fresh commit and cannot renew a certificate.

A valid protected source needs one create-only slot PUT for a warm commit and
one fresh next-slot GET for a warm observation. Native code follows and validates
commit/seal frames; a partial replay carries the original proof deadline. Only a
freshly observed end of the accepted prefix can renew observation protection.
Epoch rotation and cold/key-rotated recovery retain the original protocol.
Checkpoints remain bounded background hints and GC barriers remain fresh,
rate-limited storage checks.

After idle expiry, the MAC authenticates past accepted bytes only. Native code
reads the CURRENT checkpoint floor before reusing that source; a newer floor
forces ordinary recovery. A reusable floor plus the next-slot probe takes two
reads without replaying the source frame. Clock regression cannot reset the
anchors. No periodic keep-warm work or TTL extension substitutes for this proof.
The primitives live in the shared native object-store engine and do not require
R2-specific database logic.

## Immutable point and write plans

Use the existing canonical body representation to reduce lookup dependency
depth. Preserve legacy formats and all exact-scan, enrichment, compaction and
collector readers. Measure canonical point publication and read amplification
separately from metadata admission. Batch independent immutable uploads and
read root dependencies together; keep the original operation, byte and deadline
budgets across preparation and commit. An uncertain commit or external callback
must never be blindly replayed.

## Scalable diagnostics

Separate compact, unfiltered provider terminal/resource counters from verbose
phase traces. Bound and partition storage writes; retain all recovered resource
failures, truncation, physical-I/O lifetime evidence and unknown outcomes.
Canonical completeness uses exact provider IDs, never application trace IDs.
Filter terminal roots into the requested closed minute **before** applying the
per-window root population bound; child spans and other minutes are not that
window's terminal population. Keep original failed verdicts as well as derived
results. Use immutable window indexes or bounded streaming reconciliation so
whole-source history and background events do not force ever-larger snapshots.
Source/invocation counts, sample requirements, native memory caps and gate
thresholds must not be raised to make a run pass.

Collector rejection records must preserve a fixed failure taxonomy, including
resource causes. Do not discard an opaque `ObserverTailFailed` or treat exporter
retries as proof that a lost terminal was recovered. Whole-run evidence must
still reconcile before a qualification claim.

## Integration and validation order

The requested remote branch was merged at `63b51ef148`, including
[mail/auth PR #1391](https://github.com/antflydb/antfly/pull/1391). Both test lists
are retained. The combined native rebuild still hashes to
`b8f632e776447ea49fc210a244ecc767350ac2f6ab5f3f7b08f100bbe4705105`.
The sealed hosted run does not change when newer source is merged.

First preserve the current failed diagnostic evidence and remove the disposable
Cloudflare stacks. Implement and fault-test each native protocol independently,
then compare actual-workerd dependency depth at unchanged caps and fixed corpus.
Use fresh disposable namespaces and the unchanged control for hosted trials.
Do not run hour-long qualification until short trials meet their latency and
complete-diagnostic checks. Live mail activation, DNS and real email sending are
separate from this performance work.

## Native authority implementation status

`zig/lib/objectstore/src/conditional_root.zig` now supplies bounded observation,
bootstrap, ownership transfer, conditional append and permanent retirement over
`Client.compareAndPut`. An optional caller-owned proof cache retains sixteen
hashes, scoped to namespace, physical client, provider revision and an independent
host binding identity. With a valid native proof, commit performs one conditional
write and no source reread. A cold or disabled proof verifies the captured source
before attempting CAS. Roots, revisions, owners and nonces are all bound; an
unavailable conditional capability never falls back to an unconditional write.

Tests cover 100 consecutive commits, proof-cache loss, fabricated continuations,
stale owners, retirement, oversized escaped roots, lost accepted acknowledgements,
and absent response revisions. Opaque generation strings above JavaScript's safe
integer range survive unchanged. The native fault tests also cover signed continuations crossing pooled kernels,
key rotation, and transactional KV/fused-read integration. The transactional-store adapter and Worker coordinator now select this protocol
explicitly with `METADATA_ROOT_BACKEND=conditional`. It creates a distinct
`coord-v2` namespace and persists authority mode `r2-conditional`; mismatched
configuration fails instead of forking or upgrading an existing database.
The default remains the journal protocol. All roles, including mail auth, must
use the same setting for a new deployment.

A private host capability key signs native continuation state, revision, nonce,
namespace, binding scope and owner. The key never enters ROOT, responses or
telemetry. Signatures let pooled kernels authenticate accepted source state
without relying on a particular kernel's cache. Key rotation falls back to a
fresh observation. A signed source is not a freshness certificate: its exact
opaque revision still guards every atomic replacement. GC checkpoint barriers
always reobserve ownership, and ROOT is never a collectible log object.

Conditional namespaces persist read policy v2 with 30-second operation roots
and at least 60-second manifest/WAL/metadata collector grace. Only a freshly
observed current manifest head can use an ephemeral pin; historical versions
and exact generation requests retain durable pins. Native query sessions use
request-local bounded manifest acquisition for these read operations, observing
the supplied deadline and GC floor without renewal or the ten-minute pin cache;
maintenance and ordinary long-lived native readers keep their existing leases. The deadline starts before
observation, cannot be extended, and fails on clock regression. Existing policy
v1 and namespaces remain unchanged.

`NATIVE_WRITE_READ_AHEAD=true` opts into a native-derived plan for at most three
independent immutable WAL roots. Each response is at most 16 KiB with 4 KiB of
header allowance, covered by the existing pool read-ahead reservation. The host
releases the planning kernel before issuing the wave, never waits for a busy
permit, and closes retained bytes after mutation. Original operation, byte and
deadline budgets span planning, physical prefetch, mutation, retries, refresh
and final commit. Native decoding still verifies each page; root hints never
authorize a write. Large or packed roots stay on the existing bounded path.

The local experiment `qualification/local-worker-latency.mjs --conditional-root`
compares ordinary journal, bounded journal, canonical bodies and overlapping
WAL roots, with optional permanent conditional-root comparisons. Every case uses the same
frozen native modules and limits, disables completed-view reuse, and opens a
fresh cut for each read so a hot old fence cannot hide authority dependency cost. It verifies body bytes, newer visibility
fences, publication and counts. This is a local dependency experiment, not a
hosted qualification result.

The completed hosted attempt is sealed separately under
`examples/cloudflare-serverless/dist/qualification/fenced-hosted-1793322083103/evidence.json`:
203 artifacts, 771,530,629 bytes, manifest SHA-256
`e7403921c18272877c6e52320b0e0fcd5fee8fc2ca87a07bf071b54f7b8402f2`.
Independent cleanup verified all eleven Workers, namespaces, four Queues, three
buckets and two destinations absent; generated test keys were removed. All
collected failures and the original interrupted driver report remain preserved.


Diagnostic corrections now retain a fixed underlying cause on Tail rejection
markers (including recovered `SQLITE_NOMEM`), retain resource causes on ingestion
rejections, and expose receiver rejection presence independently of the first
eight sampled payload shapes. The per-minute counter reconciler filters terminal
roots before its existing 20,000-root guard. Full immutable window indexing and
separation of verbose archives from canonical proof remain necessary before a
large hosted qualification; these changes do not certify lost deliveries or
increase the 64 MiB source capture budget.


Latest main was merged at `5ff70fbd90` (main tip `c912271651`), retaining the mail
integration already merged at `63b51ef148`. The GCS append-only test conflict was
resolved by keeping both generation-conditional publication tests and main's
credential allocation/deadline regressions.

An actual-workerd regression reproduces and fixes the initial short-cut renewal
conflict. It verifies native mutation plans, newer fenced reads, receipt replay,
publication/counts and reopening after erased platform storage, with platform
SQL and KV access forbidden. Cursor-producing lists/searches and exact generation
requests explicitly retain durable pins; their cohorts cannot coalesce with
short point-cut admissions. The original failed workerd report is retained.


The shared object-store suite now passes 177 tests with two live-provider tests
skipped (179 total). The bounded-journal cases cover 64 consecutive one-PUT
commits, one-GET warm observation, rotation/following seals, forged state and
binding scopes, expired/regressed clocks, a delayed recreated slot after actual
checkpoint/collection, lost acknowledgements, stale owners, two-read idle
revalidation, and transactional KV/fused-read/GC-barrier integration. Checkpoint
deferral within its one-second window is preserved and tested.


## Local comparison after main integration

Actual workerd and the same frozen WASMs, four tenants with 512 1 KiB documents
each, 20 ms injected binding delay, 128 fenced reads and 64 writes per case.
Every read opens a fresh cut; completed-view reuse is disabled. All cases verify
returned body bytes, newer fences after writes, publication and exact counts.
The module hash is
`044efc02e89e4fecce3b7e624527cedcdebb32687f918cec18e1c46e88f2312b`.
Limits and capacities are unchanged. These are dependency-depth experiments,
not Cloudflare SLOs or a fourfold result.

| Forward order | Read p95 | Write p95 | Measured read GETs |
| --- | ---: | ---: | ---: |
| Ordinary journal | 57.92 ms | 194.34 ms | 512 |
| Bounded journal | 58.09 ms | 101.10 ms | 256 |
| Bounded + canonical bodies | 35.56 ms | 98.79 ms | 196 |
| Bounded + canonical + overlap | 35.46 ms | 105.05 ms | 196 |

The reverse-order repeat measures bounded/canonical at 37.21/105.96 ms versus
ordinary journal at 59.72/197.78 ms. Thus the selected profile lowers local read
p95 38–39%, write p95 46–49%, and read GETs 61.7%. The extra planning wave adds
CPU/dispatch overhead on this warm workload, so it remains opt-in and is not
selected. The default existing namespace protocol remains unchanged; new bounded
namespaces require explicit compatible activation.

Reports are `worker-latency-1793327738736.json` and
`worker-latency-1793328092125.json` under the example's
`dist/qualification/latency-lab/`. Both reference copied native modules. Earlier
failed native/workerd/timing attempts remain preserved rather than replaced.

The fourfold hosted targets and complete-diagnostic gate still require fresh
provider qualification. Compact/window-indexed evidence retrieval and exhaustive
provider allocator coverage remain open; the root-population fix and fixed
rejection causes do not certify a complete larger capture. No new paid Cloudflare
resources were created for these local comparisons.


Final merged-source checks pass 430 Node tests, 41 workerd tests, the separate
mail workerd integration, and all 11 native publication/read-plan tests, in
addition to the 177 shared object-store tests above. The earlier concurrent
build run hit an existing three-second timing assertion; the unchanged assertion
passes in the focused 120-test repeat and in the final serial Node suite. Mail
still emits the pre-existing RPC-result-disposal warning seen before this work;
its correctness integration passes, but that warning is not claimed resolved.

The clean implementation commit is `ffea7b18d6742e67d8d8aa4187f473a321205cb3`.
Its final rebuild matches all three module variants in both frozen benchmark
snapshots byte for byte. Evidence is sealed under
`examples/cloudflare-serverless/dist/qualification/bounded-journal-1793329058104/evidence.json`:
80 artifacts, 28,379,882 bytes, manifest SHA-256
`9891b3ca61f0b47a55592bee006eafe3055b8bafdf78bca8cb5ee14347eb597f`.
All 1,144 artifacts in the eight earlier closures remain hash-valid. The cache
collector was run and honored its active-native-build guard. Local test resource
stacks were disposed; no new paid Cloudflare resources were created for this
comparison.


## Cold and idle admission without extra authority writes

The next increment preserves the same immutable journal and thirty-second proof
contract. Cold bounded observers read the current checkpoint and follow its exact
next-slot chain to a fresh absence. They skip the redundant legacy seal-marker
probe; a checkpoint root alone is never a completed data cut. This still follows
ownership seals, validates every parent/revision, and preserves the bounded
replay continuation and original proof deadline.

A new `Client.getObjects` capability expresses independent bounded reads in the
shared native object-store layer. It is deliberately not an atomic snapshot.
Providers without the capability use ordinary serial `getObject` calls with
their existing retry/cancellation behavior. The WASM implementation overlaps at
most four children, accepts at most eight objects and 4 MiB of total declared
response bodies, and joins every started child before returning failure. Native
callers continue to validate typed contents and opaque provider revisions.

Idle bounded-journal observation reads the fresh checkpoint floor and known next
slot in that single native batch. Its clock anchor is captured before either
read starts. A newer checkpoint floor forces normal recovery; delayed reads
cannot renew an expired proof. The original request pays every physical GET and
reserves the full bounded response allowance before I/O. Insufficient operation
or transfer budgets reject the batch before starting children. Diagnostics retain
each child's resource failure even when another child fails first. The explicit
`NATIVE_METADATA_READ_BATCH=false` comparison keeps identical modules and proof
semantics while selecting the serial path.

App cold admission overlaps immutable authority-descriptor loading with GET-only
native observation for explicitly configured bounded namespaces. It must verify
the persisted mode, namespace, storage identity and lease policy before using
rows, issuing a lease, or returning a view. Failure clears unverified observation
state. No speculative authority or ownership writes are allowed. Existing modes
retain their admission order. Idle writers use proof age only as a scheduling
hint to refresh before expensive preparation; native authentication, clocks,
checkpoint floor and competing-slot fencing remain mandatory.

The latency driver now records the first four post-idle reads/writes separately,
can wait a real thirty-one seconds, and can use the selected hosted configuration
rather than enabling all experimental flags. Its namespace-first read sample is
only four observations, is taken before injected binding delay, and is not a
physical cold-isolate qualification. Warm p95 and these small first-use samples
must not be combined into a cold-start SLO claim.


Regression validation for this increment passes 436 Node tests, 41 actual-workerd
tests, the separate mail workerd integration, and 180 shared native object-store
tests (two live-provider tests skipped). The added cold tests hold authority
loading while native observation starts, then verify both successful admission
and storage-identity mismatch rejection with no writes. Native replay tests stop
exactly at an ownership seal, preserve the authenticated zero-slot continuation,
and follow the remaining committed frame before returning a completed cut.


### Same-module idle and leased-read comparisons

All four reports below use native module SHA-256
`2f2ac2ba34bf5b0e4e2fa8cd1b99dcd23d28a1459da9f076d5fef08952047129`,
the hosted configuration, four tenants of 512 documents, unchanged memory
limits, 20 ms injected binding delay, 128 reads and 64 writes per case.

The real 31-second-idle comparison uses **no completed-view reuse**. Post-idle
read medians decrease in both orders, but the first-use population is only four
per case and does not establish a tail SLO:

| Order | Path | First four read p50 / p95 | Warm read p95 | Write p95 |
| --- | --- | ---: | ---: | ---: |
| Forward | Serial | 58.54 / 66.78 ms | 38.38 ms | 231.81 ms |
| Forward | Batched | 45.30 / 67.39 ms | 42.81 ms | 205.09 ms |
| Reverse | Batched | 48.95 / 69.63 ms | 41.69 ms | 211.91 ms |
| Reverse | Serial | 65.98 / 75.23 ms | 43.51 ms | 223.97 ms |

Both paths perform 200 physical GETs. Batching reduces dependency depth, not
object count. Warm results and first-write outliers vary with host load; there
is no claim of a consistent all-metric latency win. The deterministic native
host test independently proves two concurrent GETs versus two serial GETs,
original-budget accounting, and rejection before I/O for insufficient budgets.
Reports: `worker-latency-1793334909963.json` and
`worker-latency-1793335028258.json` in `dist/qualification/latency-lab/`.

A separate no-idle comparison enables the existing validated cut reuse while
retaining the required visibility fence and original thirty-second lifetime:

| Order | Fresh admission read p95 | Leased admission read p95 | GETs fresh / leased |
| --- | ---: | ---: | ---: |
| Forward | 38.07 ms | 31.61 ms | 196 / 72 |
| Reverse | 39.27 ms | 29.32 ms | 196 / 72 |

This lowers read p95 17–25% and GETs 63.3% on this burst population. Write results
vary in direction and do not demonstrate an effect from read-cut reuse. One
forward leased read reaches 234.79 ms; the original report retains that outlier.
Reports: `worker-latency-1793335175129.json` and
`worker-latency-1793335240828.json`. These reports explicitly distinguish reused
cuts from fresh admission. No older report is relabeled as a leased experiment.

Remaining body fetches, native execution and cold admission still matter. The
hosted workload's thirty-second rounds and advancing fences cannot be repaired
by extending a stale lease; fresh admission must remain cheap. These local
samples do not establish a fourfold improvement, provider placement latency,
physical cold-isolate qualification, or complete provider diagnostics.


The committed implementation is `15d985712741f68f0ea708e1fc2d065ae3fb9859`. Its clean rebuild
matches all three module variants in all four frozen experiment snapshots. The
new closure is `examples/cloudflare-serverless/dist/qualification/native-read-batch-1793335521328/evidence.json`:
71 artifacts, 118,665,969 bytes, manifest SHA-256
`ffdf8d5ef4e39d6b47c114ad6f1fda17742ec37bdc78213f953246f43e2c4056`. All 1,224 artifacts in the nine preceding closures
remain hash-valid. The new closure also verifies independently, preserves earlier
failed/sandbox-denied attempts and does not alter any preceding closure. Cache
collection honored the active-build guard. No hosted resources were created.
