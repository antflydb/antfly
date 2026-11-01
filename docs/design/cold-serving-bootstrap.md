# Cold serving plans and durable native bootstrap

This extends #1278 and draft #1320. `RESIDENT_COLD_PIPELINE=true` selects
compressed inline bootstrap headers, native cohort-wide dependency discovery,
combined metadata projections and byte-based dependency admission.
`RESIDENT_DURABLE_BOOTSTRAP=true` additionally selects a new, incompatible
`serialized-owner-v3` namespace protocol. Both remain opt-in. This is an
implementation design, not a declaration that the latency or hosted gates pass.

These flags extend a compatible resident configuration. Graph scheduling also
requires `NATIVE_POINT_CONTINUATIONS=true` and cohort discovery uses
`RESIDENT_READ_COHORTS=true`; committed header reuse uses
`RESIDENT_BOOTSTRAP=true`. The measured complete flag sets are defined in
`examples/cloudflare-serverless/qualification/read-state-latency.mjs`.

## Native cold read plan

Publication preparation compresses the existing bounded manifest/facts-root
bootstrap together with native publication-emitted small routing headers and includes its metadata key in the native inline read set. The
ordinary journal/checkpoint commit owns that copy; publication does not add a
mutable R2 pointer write. The existing 12 KiB root and 16 KiB journal frame
limits still apply. Inline rows may be evicted; absence from the inline capsule
means an ordinary tree lookup, never absence of the actual metadata value.

Point admission projects migration state, publication HEAD, WAL HEAD, GC floor
and bootstrap in one bounded native read. The new point path does not also load
the redundant immutable read-hints row. Manifest/root bootstrap bytes still pass
native content validation. Compression is bounded on both encoding and decoded
size; invalid compressed data fails rather than being silently trusted.

Native cohort discovery continues across members when an immutable dependency
is missing. The host records the native-emitted exact descriptors, deduplicates
them and loads the complete currently discoverable frontier. It then resumes
against the original captured view. Unknown child dependencies require another
frontier; the host never guesses keys or treats unknown metadata as absence.
Visibility errors remain per member; integrity and resource failures fail the
operation. Bodies and oversized results retain the existing individual path.

Detached dependency response admission has the previous two-transcript aggregate budget:
200 KiB, now charged by response size rather than two fixed permits. Scheduling
uses bounded request-owned timer/data tickets, fairness between namespaces and
aging for larger requests. At most four loader tasks per query run concurrently.
Retained transcripts reserve up to 1 MiB per query from the existing global
8 MiB input budget, leaving at least 4 MiB for writers. Descriptor/JSON overhead
and response copies are charged. The native role ceiling remains 64 MiB;
per-query operation, transfer, deadline and lease limits are unchanged.

Logical timeout keeps the physical work's reservations until settlement. Late
resource errors remain observable. Shared I/O promises and R2 handles are never
handed to another request. New metrics distinguish graph waves/nodes, active
response bytes, queued jobs and retained input reservations.

## Provider-local durable read state

The v3 owner persists one provider-local opaque record using the Durable Object
storage API. Antfly still owns metadata trees, WALs, indexes, receipts and capsule
encoding. Capsule import/export run in the 8 MiB observation module, preserving write-module capacity. The actual module capability is checked before activation and on each codec call. No application SQL schema is introduced. The provider's storage
implementation is an additional durable tier; this mode is not R2-only durability
and does not establish that historical platform SQLITE_NOMEM is resolved.

A native capsule binds namespace, bucket, prefix, provider actor scope and native
metadata state. It carries a checksum and validated native root. The checksum
is corruption detection, not a freshness proof. The provider-local commit
protocol grants read admission. Import restores no append certificate, MAC key,
clock anchor or acquired writer flag. An explicit module capability must exist
before accepted metadata mutations. Namespace authority and a local dirty record
may already have been provisioned; incompatible modules fail closed and require
a compatible redeployment or a fresh namespace, rather than automatic rollback.

The record states are:

1. **Dirty intent:** persisted and explicitly synchronized before any native
   operation that can advance accepted metadata, including ownership recovery.
2. **R2 committed:** native durable metadata finalization succeeds. R2 acceptance
   and idempotency receipts remain unchanged.
3. **Acknowledgment:** the application write returns after native R2 commit.
   A confirmed dirty interval coalesces subsequent intents without another local
   write. No optional clean checkpoint is on this acknowledgment path.
4. **Clean capsule:** after one quiet second, native attempts to export confirmed
   state on the coordinator's existing serialized maintenance queue. Export uses
   try-only admission to the 8 MiB observation lane at maintenance priority. Busy
   capacity defers immediately; it cannot retain the metadata queue while waiting
   for a slot needed by foreground preparation/finalization. The provider record
   is persisted and synchronized before a clean checkpoint is confirmed.
5. **Frozen:** new admission stops, queued work and outstanding I/O drain, then a
   durable blocked record is installed before namespace retirement/migration.

A crash before a confirmed clean checkpoint leaves dirty intent and requires
native R2 recovery. A checkpoint failure cannot revoke an already accepted R2
write; it remains visible in diagnostics and invalidates qualification. The
scheduler does not retry checkpoint exceptions automatically, and busy admission
is bounded to four checks within ten seconds. A future write can schedule another
attempt. This is a best-effort accelerator: exhausted busy checks can leave a
dirty record until another write or explicit maintenance attempt. A crash in
that interval takes the ordinary R2 recovery path. The owner never installs a
clean checkpoint concurrently with a write. Scheduling
tracks a host-local dirty generation, so ownership/checkpoint work that leaves
the native metadata revision unchanged can still become clean after quiescence.

Before every local PUT/sync, the in-memory confirmed-dirty latch is cleared.
Only successful synchronization confirms it. This matters when a clean PUT took
effect but its sync failed: the next write must persist dirty intent again, even
though the last confirmed in-memory record was dirty. A lost clean acknowledgment
is safe because the capsule only contains state already committed in R2. Corrupt
capsules are marked dirty before fallback; resource failures are not treated as
corruption. Mandatory dirty durability is checked against the original operation
deadline before R2 advancement. Checkpoints have their own bounded deadline.
Freeze drains pending checkpoints before installing the blocked record.

A clean physical restart can serve its confirmed snapshot while first-write
admission is pending. The snapshot is immutable and linearizes before the
concurrent write; uncertain state is invalidated. The first writer persists a
new intent, then may reuse the clean capsule's native identity/cursor to obtain
**fresh native admission and a new proof**. This avoids unnecessary takeover
seals when no accepted-prefix PUT was outstanding. Dirty reconstruction uses a
new identity and ordinary takeover/fencing. The adapter never marks an imported
capsule as an acquired writer or reuses its prior proof.

## Authority and migration contract

V3 authority is immutable provider-local namespace configuration, with a matching
R2 descriptor and `authority_storage: provider-native-v1`. This removes the R2
authority GET on clean restart. New namespaces first create/validate the R2
configuration and durably adopt it locally. Cached configuration is bound to the
namespace, storage ID, actual actor ID and collector policy. Non-owner observers
recognize the protocol and route admission through the owner; they cannot adopt
its durable storage capability.

V1/v2 adapters and incompatible flag combinations reject v3. Disabling the flag
is not rollback. Retirement must first call the owner's private freeze operation
and drain active work before changing the R2 descriptor. Out-of-band edits to an
active v3 descriptor are outside the protocol. The DATABASE binding and its
logical storage identity are also immutable: rebinding an R2 bucket requires
retirement and a new namespace/storage ID. The adapter cannot independently
identify a different physical bucket behind the same logical binding. No in-place migration or unfreeze
tool is provided; rollout uses a fresh compatible namespace. Native capsule
primitives can support other providers only when their adapters supply an
explicit equivalent exclusive-owner/durability contract.

## Qualification

Keep physically cold object reads, clean Worker reconstruction, dirty recovery,
warmed reads and first writes separate. At 50 ms injected mandatory GET latency,
a 47 ms truly cold target is impossible. This tier removes metadata dependencies;
it cannot make a missing data block arrive faster than its storage tier.

Qualification must include crashes around both local synchronizations and R2
commit, expired proofs, wrong scopes, corrupt capsules, delayed old-owner PUTs,
concurrent readers during first-write admission, both collectors, large bodies,
namespace freeze, and original role/input/I/O bounds. Measure write p95 as well
as cold and warm read p95. Use both execution orders and the approved fixed
control for hosted qualification; current-module local comparisons are design
experiments. Do not reinterpret warm local-cache results as physically cold data.

## Dense-load design correction

The synchronous prototype at `546a104e9a` passed its focused correctness suite
and the first 48-case local matrix, but failed the denser 512-read/128-write run
with `database is busy` (102 failed offered operations; a repeated diagnostic
run failed 46). Those reports are retained under `cold-v4-final-dense32-forward`
and `cold-v4-dense-diagnostic-2`. Instrumented provider get/put/sync calls did not
attribute that error to local capsule storage. It would be incorrect to call
the platform error resolved based on that evidence.

The corrected design removes synchronous per-write clean exports and durability
barriers. Dirty intent and R2 acknowledgment remain mandatory; clean checkpoints
are a coalesced recovery accelerator. Dense offered-load runs, both execution
orders, recovered-error histories and actual-workerd restart tests are required
to evaluate this correction. Earlier synchronous measurements remain historical
and must not be combined with corrected measurements as a single passing run.

The provider storage contract requires ordered writes and a durability barrier
under the exclusive owner. Cloudflare documents [storage output gates](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
and the [storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).
Native capsules do not manufacture those guarantees for another provider.

## Final local measurements

This full matrix predates the main integration described below; the merged
binaries have their own validation and smoke evidence.

Production JavaScript: `eced42c73e`; native source: `72bc259849`; measurement
harness: `eeedd57e3a`. The final 36 cases are independently audited: **7,680
measured reads, 2,304 writes and 144 separate reconstruction reads**, with exact
bodies and latest/minimum-fence checks. Both execution orders use four tenants,
64/512 documents per tenant and 50 ms injected binding latency. The incumbent
and candidates share the same current native modules; this is not the approved
fixed hosted control. CLI labels are `serving-v3` (prior resident implementation,
namespace protocol v2), `serving-v4` (cold pipeline, protocol v2), and
`serving-durable` (cold pipeline plus protocol v3).

The table shows ranges between the two execution orders. Warm/mixed/offered
cells contain **read / write** p95. Startup-inclusive cold cells contain only
first-read p95.

| Profile, p95 in ms | Existing resident | Cold pipeline | Pipeline + durable bootstrap |
| --- | ---: | ---: | ---: |
| Startup-inclusive cold, 64 docs/tenant | 813–928 | 646–696 | 563–629 |
| Startup-inclusive cold, 512 docs/tenant | 1315–1473 | 727–1511 | 768–957 |
| Mixed after cold start, 64 docs/tenant | 656–708 / 804–842 | 657–663 / 821–824 | 181–187 / 415–430 |
| Mixed after cold start, 512 docs/tenant | 913–932 / 828–860 | 729–772 / 767–828 | 237–238 / 377–394 |
| Warm burst, 64 docs/tenant, 32 writes | 114–116 / 245–263 | 60 / 212–230 | 110–124 / 246–258 |
| Warm burst, 512 docs/tenant, 32 writes | 110–176 / 234–244 | 58–61 / 236–246 | 59–108 / 227–228 |
| Warm burst, 512 docs/tenant, 128 writes | 151–155 / 221–222 | 156–168 / 228–233 | 156–157 / 217–228 |
| Offered 32 reads/s, 512 reads + 128 writes | 22–31 / 128–131 | 20 / 126 | 19–20 / 125–126 |

The original short 512-document warm burst showed a write regression before
try-only admission (237–241 ms versus 199–220 ms). It remains in the evidence.
The final matrix repeats that exact short profile as well as the longer profile;
the latter also changes working-set coverage and publication frequency and is
not a substitute for the short-profile check. These small paired experiments do
not establish statistical non-regression or a production throughput SLO.

`--cold-startup` makes the first timed API requests initialize local workerd:
there is no delay-setting control RPC after reconstruction. The harness checks
that each durable owner has a confirmed clean record before restarting it.
Worker-emulator startup is included, so this is not a forecast of actual
Cloudflare cold-start latency. The 144 cold samples are cohort members across
multiple variants and corpora, not 100 independent candidate Worker boots.
Dirty reconstruction remains ordinary R2 recovery and has no equivalent speed
claim. A universal **10× cold-read improvement is not established**.

Earlier application-cold measurements at `e8c69e8a4d` excluded some runtime
startup by configuring delay first: resident-to-durable p95 was 434–457 →
167–200 ms at 64 documents and 775–779 → 292–295 ms at 512. Those results are
kept separately. The final large-corpus cold traces prove three native-emitted
dependencies in one discovery frontier; byte admission still bounds their
physical overlap.

Audits recompute percentiles from individual samples, verify source/experiment,
module and bundle hashes, check complete owner-admission attribution, original
memory/reservation bounds and settled I/O, and reject recovered resource,
storage or checkpoint failures and matching raw runtime errors. Unrelated local
native builds remained active; machine-context snapshots are preserved. Gate
thresholds and the original 64 MiB native role ceiling are unchanged.

Validation: **581 Node tests pass**. The full workerd suite (58 checks) and mail
integration passed before the final try-only refinement; the complete durable
workerd fixture then passed again after that refinement. It covers real expiry,
both collectors, large bodies, concurrent mutations, restart and retirement with
a held R2 PUT. **49 focused native tests pass; three live-provider tests skip**.
The native source/modules did not change after that run. Environment-interrupted
validation attempts and all failed prototypes remain preserved.

Evidence: `dist/qualification/cold-v4-sealed-1793489495127/evidence.json`, **1,772 artifacts / 8,277,652,730 bytes**,
SHA-256 `96da5cd3cf696d5e1e526e203d7a83f6b0819c988a021d6f98ab0f99fe303b50`. It includes source archives, native modules/test
executables, reports, failed attempts and audit scripts. Eight corrected
predecessor closures reverify unchanged; historical gaps and the invalid nested
self-summary are not requalified. Credentials are excluded.

## Remaining qualification and cold costs

The three-node frontier is already discovered together. Attribute remaining
cost separately to runtime startup, provider-local state loading/capsule import,
first native execution/promotion, kernel admission and physical object reads
before changing the storage layout again. Reusing a validated bootstrap across
native replay steps is another bounded CPU optimization to profile. Physical
storage RTT remains a lower bound for missing blocks.

Actual Cloudflare startup and scale qualification, the original paired long
trials with complete whole-window diagnostics, independent physical-cold samples,
and the historical live `SQLITE_NOMEM` remain open. No Cloudflare resources or
credentials were created for these experiments. The new durable owner adapter
is Cloudflare-specific; other providers need the explicit equivalent ownership,
ordered-storage and durability contract before trusting native capsules for
latest reads. S3/GCS performance is not qualified by these local trials.

## Reproduce locally

After building the example, run from `examples/cloudflare-serverless` with a
new output directory for every run:

```sh
node qualification/read-state-latency.mjs --twofold \
  --only=serving-v3,serving-v4,serving-durable \
  --documents=64 --rounds=8 --delay=50 --setup-delay=0 \
  --strong-latest --cold-restart --cold-startup \
  --out=dist/qualification/cold-repeat-UNIQUE
```

Use a different output directory with `--reverse` for the other order. For the
large cold corpus use `--documents=512 --cold-ids=1,137,273`. Replace the cold
flags with `--resident-warmup` for a warm burst; use 8 and 32 rounds to retain
both short and long write-tail profiles. Offered-load profiles add
`--offered-reads=512 --offered-rps=32 --offered-write-every=4`.

## Integration with current main

After the full latency seal, main advanced. Integrated `origin/main` at
`97945ad58e` in merge `2eee524615`. The only conflict was the retained-cut
cache-pressure test: preserve main's deterministic residency/reservation pressure
and the branch's retained-value assertions. Cloudflare JavaScript and shared
object-store files merged unchanged; main's provider/schema dependencies changed
the rebuilt WASM modules, so the full 36-case numbers above remain explicitly
**pre-integration results**.

The merged build passes **581 Node tests, all 58 actual-workerd checks, the mail
integration, 49 focused native checks and two resolved retained-cut tests**.
Three live-provider native checks still skip. A separate, independently audited
four-case startup-inclusive smoke comparison uses both execution orders and the
64-document corpus: **384 measured reads, 128 writes and 48 reconstruction
reads**, with bodies/fences, hashes, owner attribution, limits, resource histories
and settlement checked again. This smaller integration run does not replace the
full matrix or establish a hosted/performance gate pass.

| Merged-build smoke, p95 in ms | Existing resident | Durable bootstrap |
| --- | ---: | ---: |
| Startup-inclusive cold | 681–704 | 407–504 |
| Mixed read / write | 629–632 / 785–798 | 161–162 / 396–398 |

Merged full WASM SHA-256:
`bfce41574f5e44b546636d9670c40bae08c37eeb493babed89c44c5e695fc3e4`.
Integration evidence: `dist/qualification/cold-v4-main-1445-sealed-1793491742482/evidence.json`, **69 artifacts / 791,720,602 bytes**,
SHA-256 `d8836e685e8a670505e43c8ab5145fafdda5a0210eb85ee5584896fc6f62c6e9`. The prior 1,772-artifact seal independently
reverifies unchanged; no old results or seals were rewritten. Live Cloudflare
qualification and historical `SQLITE_NOMEM` remain open. No cloud resources or
credentials were created by the integration checks.

## Local qualification artifact retention

On 2026-11-01 UTC, at the user's request, pruned **1,786 archived payloads /
24,782,331,937 bytes**: 1,713 WASM copies, 30 native executables and 43
Git-backed source archives. Source commits are retained through 40 local
`refs/qualification/source/<sha>` refs. Source archives without a recoverable
Git commit, experiment patches/bundles, reports, samples, logs, manifests, hashes
and toolchain records are retained. Active runtime modules and one exact copy of
each of the two approved control modules are preserved.

The original seals remain historical manifests. **Their removed binary/archive
payloads can no longer be fully reverified.** This intentional pruning does not
change the recorded measurements, but source SHAs alone do not guarantee
byte-identical rebuilds. Use retained lockfiles/toolchains when rebuilding and
verify new hashes; a rebuilt control must not silently replace approved bytes.
The local retention record and append-only removal ledger are:

- `dist/qualification/local-retention-20261101T004120Z/retention.json`
- `dist/qualification/local-retention-20261101T004120Z/removed.jsonl`

The retention record identifies the relocated exact approved control modules.
Source refs are local to this repository; they are not an off-machine backup.
The reusable cleanup tool defaults to dry run, excludes credential paths,
symlinks, active modules and retention records, and preserves unknown/dirty source
archives:

```sh
# From examples/cloudflare-serverless:
python3 -B qualification/prune-local-artifacts.py
python3 -B qualification/prune-local-artifacts.py --apply
python3 -B test/prune-local-artifacts.test.py
```

## Cold-path phase attribution and hosted diagnostic pilot

The follow-up instruments authority lookup, provider-local storage loading,
bootstrap import/queue/instantiation, owner admission, cut hashing and bootstrap
preparation. Spans share trace IDs across services and retain local durations;
never subtract clock offsets across isolates or sum nested spans. With metrics
enabled, API responses include `X-Antfly-Trace-Id`. The local experiment correlates
this with the HTTP trace and records `client_outside_http_trace_ms`: this includes
client/runtime dispatch, transport, startup and response handling, **not solely
platform startup**.

Instrumented baseline `09c259f11c` identified repeated gzip decoding and validation
of the same committed headers on continuation frontiers: twelve decodes for four
cold cohorts. Runtime change `8737fa541e` adds shared object-store
`CommittedHeaders.prepare`, returning a factory containing owned immutable bytes.
Each captured read prepares once, then creates context-local transports from that
factory. It retains no request handles or shared decode promises. Every returned
body remains an independent copy; namespace/version/ETag/range/size validation and
original compressed/plaintext bounds remain enforced. The compatibility `wrap`
API retains its lazy compressed path and synchronous envelope/plaintext checks.
No authority, publication format, resource budget, freshness rule or gate target
changes. This is usable by other JavaScript object-store adapters as well.

A new local control mode uses the **original saved role bundles**, verifies their
module/bundle hashes and exact bindings, and records their original source
provenance. It is not the approved hosted control and does not replace its bytes.
Modules now reside once in an immutable SHA-addressed local store with hard links
for case-local evidence. Mutable build output is copied, never hard-linked into
that store. Repeated qualification no longer creates a full WASM copy per case.

Sixteen local paired cases, both orders, retain **3,328 reads, 832 writes and 96
additional reconstruction reads**. Independent auditing verifies 112 case-local
module/bundle hashes and successful body/fence/resource/settlement checks. Decoding
falls from twelve to four per cold population. These runs are **exploratory**:
the machine was also running other worktrees' Zig/Go builds and tests, and hosted
deployment work overlapped part of the matrix. A captured load average was about
92. Preserve the negative warm result; do not claim a passed latency guard.

| Exploratory p95 in ms, both orders | Instrumented control | Prepare once |
| --- | ---: | ---: |
| Cold, 64 documents/tenant | 504–691 | 436–745 |
| Cold, 512 documents/tenant | 706–758 | 530–685 |
| Mixed after cold, 64 read / write | 166–168 / 397–406 | 162–165 / 411–412 |
| Mixed after cold, 512 read / write | 223–227 / 360–367 | 217 / 354–356 |
| Warm 512, 32 rounds, read / write | 167–168 / 199–219 | 178–584 / 213–597 |
| Offered 32 reads/s, read / write | 70–78 / 136–144 | 78 / 147–166 |

Local import's first native invocation is about 21–25 ms and WASM instantiation
is at most 2 ms in the initial baseline. The smaller headers change cannot
explain hundreds of milliseconds of startup variation. Module compilation before
handler execution is outside those instantiation spans. The hosted deployments'
Wrangler-reported Worker Startup Time is 2–5 ms for the measured application
versions; this is another platform metric, not end-to-end cold latency. There is
no measured justification here for introducing another specialized native module.
First resolve transport/owner placement, startup attribution and reproducible
latency guards; keep the existing 8 MiB point lane and precise capacity promotion.

### Small live pilot, not qualification

`qualification/cold-pilot.mjs` prepares and seals four split Workers and a single
R2 bucket. It enables the same durable native-bootstrap profile without queues,
cron jobs or automatic GC; publication is explicit. Scope validation fixes the
account, disposable names, token path and service roles. Sealed configuration
snapshots and bundles are hash-checked before actions. It captures sanitized Tail
journals, request outcomes, version headers, caller/cohort UUIDs, actor identities
and pool diagnostics. Tokens and request headers are excluded from telemetry.

The pilot `antfly-1278-cold-mvx4qaij-9563` seeds four tenants with 64 documents each
and checks bodies plus fresh visibility fences on **60 point reads**: twelve
initial reads and four batches of twelve after app/writer redeployment. One
reconstruction batch returned **mixed old/new app versions**; it remains recorded
and is rejected as a qualifying cold population. The driver now requires all
response/state versions to match the expected deployment. Redeployment proves
observed actor reconstruction, not independent physical cold Worker isolates.

The three version-consistent reconstructed batches have client p95 **1,283–1,304
ms**. The final strict batch correlates all twelve client trace IDs with both
HTTP and owner/cohort telemetry and the expected app/writer versions. Its HTTP
trace durations are 410–969 ms, versus client durations 947–1,302 ms. Differences
include connection setup, edge routing, dispatch and response handling; these
are not measurements of R2 or native CPU alone. Available journals contain no
non-OK outcomes, truncated events, resource failures or durable-storage/checkpoint
errors. Maximum observed WASM pool linear memory is **19,660,800 bytes**, excluding
JavaScript/platform/SQLite memory. Tail remains diagnostic and may be dropped or
sampled; the original whole-window independent-coverage gate is not satisfied.

Cloudflare [documents](https://developers.cloudflare.com/workers/runtime-apis/performance/)
that its timers only advance after I/O. CPU-only spans can therefore report zero;
use Tail invocation CPU time as an
invocation-wide measurement; a zero native-import duration does not mean zero
CPU. New telemetry preserves the existing platform memory/CPU flags and bounded
caller UUID arrays while redacting arbitrary messages and credentials.

After capture, the cleanup driver detaches the app, stops private actors, waits
for quiescence and drains R2 through the standalone cleanup Worker until an empty
invocation. All four Workers and the bucket were deleted; the disposable token
was removed. Read-only follow-up checks receive explicit missing-Worker errors
for each name. No queues or observability destinations were created. This is a
scoped cleanup receipt, not an account-wide billing audit.

Local evidence is under `dist/qualification/cold-v5-phase/`, with per-case bundles
and reports under `cold-v5-paired-*`. Live reports, sanitized journals, original
sealed configurations, hash records and absence checks remain under
`dist/qualification/antfly-1278-cold-mvx4qaij-9563/`. Older retention receipts and
approved-control modules are untouched.

The bootstrap implementation and diagnostic pilot are complete. **Isolated warm,
burst and cold latency guards remain pending a quiet benchmark host.** Do not
claim a twofold/tenfold improvement, a resolved historical `SQLITE_NOMEM`, or the
full hosted gate. The next qualification must pin/reconcile every serving version,
separate client and owner transit from native/storage work, and collect the
original complete resource-error windows before expanding the workload.

Validation for this follow-up: **587 Node tests and five targeted actual-workerd
checks pass**, including v4/durable publication/reconstruction/freeze lifecycles,
immutable-flight caller ownership, RPC-owned data and native point continuations.
The WASM modules remain unchanged from the integrated native build above. Guarded
`make gc` skips cleanup because other native builds are active; no caches used by
those builds are forcibly removed.
