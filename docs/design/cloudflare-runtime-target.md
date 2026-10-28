# Cloudflare runtime: next architecture steps

This is the implementation order following the
[latency experiments](cloudflare-latency-experiments.md). The latest
[live native protocol diagnostic](cloudflare-native-live.md) publishes the full
2,048-document corpus and completes large-record recovery. Qualification remains
incomplete: read p95 exceeds one second, large-record publication has substantial
checkpoint overhead, and invocation accounting does not yet cover every event
population. Sections below distinguish implemented protocols from proposed work.

## Keep one native engine and one durable truth

Use Antfly's shared native object-storage engine for WAL, metadata, document
artifacts, indexes, recovery and collection. R2 holds authoritative durable
state. The Cloudflare adapter supplies object I/O, execution, scheduling and
provider calls; it does not implement a separate database protocol in JavaScript.
See [deployment modes and storage engines](../architecture.mdx).

Keep the public app and ordinary point-query WASM together. Tenant Durable
Objects provide bounded warm sequencing and native commit execution. Heavy
query and builder execution can retain separate memory/admission lanes; logical
roles need not always require four physical Worker deployments. Consolidate a
role only after checking aggregate isolate memory and request-context ownership.
AI Gateway/model calls remain outside the serialized write-commit path.

The desired paths are:

```mermaid
flowchart LR
    App[App and point-query WASM] --> Writer[Tenant sequencer and native commit]
    Writer --> R2[Antfly durable state in R2]
    App --> Read[Native fresh authority and query plan]
    Read --> R2
    Writer --> Notify[Coalesced Queue notification]
    Notify --> Build[Native resumable builder]
    Build --> R2
    App --> AI[AI Gateway and providers]
```

Queues wake durable work. The native outbox/progress records determine what work
exists and whether it completed, including after lost, duplicated or delayed
notifications. Preserve visibility tokens separating accepted WAL progress from
published index progress; a durable write acknowledgement does not imply every
index or enrichment has caught up.

## 1. Bound the minimum publication unit

Fix this first. The observed budget-one cancellation means reducing the number
of records is insufficient in at least one slow-storage case. Identify the
minimum unit's actual phase/dependency cost before prescribing its split.

Extend existing prefix/job checkpointing with continuations inside a large input
record or artifact. Introduce a durable native publication plan containing source identity, the WAL
prefix, policy/schema identity, stable logical job identity, completed output
references, a bounded continuation and terminal status. Each invocation builds
or uploads a bounded unit, persists progress and yields with deadline headroom.
The plan must support input records whose internal work exceeds one slice.

Independent immutable pages can upload in bounded waves. Parent references are
computed from validated content identities; source protection and all unfinished
outputs remain reachable to GC through the plan. Each resumed slice reacquires
and validates native ownership and source protection. An expired or retired
source cannot be made valid by trusting a saved cursor or old pin expiry.

Only terminal native fenced CAS advances published HEAD/watermarks. A durable
candidate intent makes its completion receipt recoverable after that decision. It rejects a changed source or replaced owner. Intermediate
objects never expose a partial published prefix. Crashes before and after each
checkpoint/finalization must recover to one outcome; abandoned plans eventually
release protected objects through bounded collection.

A continuation has a versioned native envelope with the base manifest identity,
accepted WAL prefix, policy fingerprint, phase, input ordinal/offset, completed
typed output references, and a collector-visible protection root. The phase
boundary must occur inside materialization and index construction, not only
between whole artifacts. Bounds apply to input bytes, allocations, output bytes
and host I/O; record count is an additional bound rather than the only bound.

The job identity survives owner replacement; upload authority does not. Never
resume writes in an expired owner's `UploadScope`. A resumed owner authenticates
completed immutable outputs and protects them through the job, creates a fresh
scope for missing outputs, and regenerates parent references where attempts
change. A lost final acknowledgement recovers the terminal job result. Neither
replaying the whole build behind a host-side cache nor publishing intermediate
HEADs implements this contract.

## 2. Fuse authority and immutable query planning natively

The earlier scheduling experiment starts a query after fresh admission while
obsolete I/O drains. The native plan implemented below also prepares immutable
inputs before that admission completes.

A native read plan can prepare immutable manifest/facts/body reads in parallel
with fresh authority observation. Speculative inputs carry their exact source
generation, WAL cutoff and checksums. Native validation accepts them only when
they match the freshly authenticated view, visibility fence and live source
protection. A changed source selects a bounded replan under the same original
deadline/admission and aggregate I/O budget. No speculative result is returned
before that validation succeeds.

Keep immutable byte caching shared and bounded. Mutable authority is observed
freshly. Avoid per-object coordinator RPCs once the operation has an authenticated
native view. Retain separate bounded heavy-query admission where required.

## 3. Minimize the native write transaction's dependent calls

The existing [object backend](antfly-object-storage-backend.md) already commits
through a conditionally created immutable journal slot. Its throttled checkpoint
is a recovery hint. Preserve that protocol, including takeover/seal checks; a
logical HEAD must not become a hot mutable R2 object rewritten for every step.

Keep one client mutation call into the tenant sequencer. Progressively combine
native WAL preparation, operation-receipt handling and metadata transaction
planning so they share decoded state and immutable inputs. Preserve one
authoritative commit decision binding the WAL head, receipt and durable work
admission. Background index publication remains outside this acknowledgement.

Upload independent immutable output before the final fenced conditional commit.
If ownership, WAL identity or source dependencies change, fail or replan within
the original budget. Warm retained state supplies preparation hints, never an
acknowledgement by itself. Do not remove recovery/seal checks just to remove a GET.

Measure native commit phases to identify remaining calls before extending the
ABI. Optimistic preparation remains available as an incremental control. The
combined native write plan is implemented below; its cloud p95 still requires
qualification.

Do not hold the metadata transaction queue across WAL page construction simply
to combine host calls. That would serialize cold object I/O ahead of unrelated
read admission and could invert the separate kernel/memory reservations. The
combined implementation belongs in native library planning and finalization,
with one bounded caller context and explicit reservations. Prepared immutable
outputs remain outside the final authority transaction; its native dependency
checks and sealed-epoch commit remain mandatory. Measure queue latency as well
as write latency before adopting any combined path.

## 4. Give small bodies a canonical native representation

The experimental inline format removes a GET but retains external body copies,
increasing measured fixture payload by 27–29%. Keep it opt-in for now.

A follow-up versioned body locator should support inline and external/packed
representations through shared native readers, builders, enrichment, reindex,
exact scans and collectors. Eligible small bodies can then have one canonical
representation in their authenticated facts leaf. Large or dependency-bearing
bodies retain external/packed storage. Audit/scheduling indexes stay compact.

Complete the reader/collector rollout and pin-aware retention transition before
removing any fallback copies. Measure both retained bytes and COW write
amplification: saving a GET must not make publication exceed its minimum slice.

The first implementation is opt-in through `CANONICAL_POINT_BODIES`. Native
facts format 12 (`AFDFACTC`) stores eligible bodies of at most 1536 bytes in a
single authenticated primary leaf. It does not upload the external fallback
used by format 11. Existing formats remain readable. Disabling new canonical
writes preserves readers for already published generations; this is not a
binary downgrade path to readers that predate format 12.

Scheduling and audit trees store compact logical canonical references. Their
cursors resolve the body through the primary root of the same pinned generation.
They must not retain physical locators into an obsolete primary leaf: changing
one neighbor would otherwise retain the old leaf indefinitely. Native body
locators used outside those indexes include the full leaf digest, attempt,
record count and exact envelope offset. Readers authenticate the standalone leaf
and selected body. Dependency-bearing and larger bodies keep their existing
external/packed representation. Leaf bodies reuse the operation-local page cache.

Canonical leaves currently disable page packing and value-prefix expansion for
the facts build only, because offsets must identify contiguous authenticated
bytes. This can increase storage calls or COW amplification. Keep the switch off
by default until lifecycle measurements establish its overall tradeoff. Native
facts publication also buffers independent primary/scheduling tree uploads into
bounded batches and flushes them before returning a publishable root. This is
upload batching, not a durable continuation inside a large record.

## 5. Make operations and cost part of the design

Use stable operation identities and durable status/result receipts for
maintenance that can change HEAD. After an ambiguous acknowledgement, recover
the original result. Existing keyed compaction provides this contract; preserve
the failed unkeyed qualification populations and their no-transport-replay rule.

Collect bounded numeric phase metrics: storage calls/bytes, dependent waves,
native/queue/RPC time, stale preparation retries, publication progress, retained
bytes and physical HTTP attempts. Independent invocation accounting and complete
resource/error coverage remain separate requirements from latency measurement.

Physical transport instrumentation now records fixed, disjoint per-operation
latency bins (at most 10, 50, 200, 1000 ms, and over 1000 ms) and counts transport
errors, including failed attempts. These bounded numeric distributions avoid
retaining keys or individual samples. Sum bins across invocations; do not average
per-invocation percentiles or interpret overlapping wait time as serial latency.

Track cost per useful acknowledged operation and published document. Parallel
uploads reduce elapsed time but do not reduce request counts by themselves.
Coalesced notifications, compact metadata commits, fewer immutable objects and
reduced rewrite amplification can improve both latency and cost. Suppress idle
background scans when durable state already proves there is no work.

For disposable testing, persist an exact resource inventory, ownership/scope,
expiry and cleanup state. Archive reports and sealed provenance locally before
cleanup. Teardown stops producers and consumers, removes execution namespaces,
drains/deletes object storage, removes queues and credentials, and verifies
resource absence. A failed gate must not require retaining cloud resources
indefinitely. Expiry is a cleanup trigger, not permission to delete resources
outside the approved scope. Production retention follows its own explicit policy.

## Validate changes locally, then use short cloud confirmation

The earlier latency matrices enable injected delay after seeding. They do not
establish bulk-preparation headroom. Apply the declared storage model to seeding
and publication as well as steady reads/writes in the next fixture.

The separate `examples/cloudflare-serverless/qualification/local-lifecycle-latency.mjs`
fixture now applies uniform injected delay before seeding, bounds publication
convergence, records preparation/publication latency and retained object bytes,
and preserves a failure report. Its reports live under
`dist/qualification/runtime-plan-lab`; earlier sealed evidence remains unchanged.
Compare `before,current,canonical,canonical-combined` with `--only` and use
`--documents=512 --delay=20` for the full local population. These local delays
are an experimental model, not measured Cloudflare storage percentiles. The
`--model=bursty` option adds deterministic jitter and correlated slow windows,
so the same declared load can be replayed. All candidate native modules are
captured once at startup; reports bind every module and generated service bundle
by checksum. A failed case is retained and subsequent selected cases still run.

Extend this uniform-delay model with locally replayable storage delay,
correlated slow periods, queue lag, source changes, lease expiry and interrupted
I/O. Start from measured per-phase data; aggregate operation averages cannot
establish per-GET/PUT latency distributions. Reproduce the stalled publication
case, not just a warm steady checkpoint. Test pauses and reconstruction at every
durable publication boundary and lost acknowledgements after a real commit.

Compare one change at a time against the immediately preceding implementation
on identical seeded data. Track latency, useful throughput, request counts,
rewrite/retained bytes and peak memory together. The older approved control
remains the fixed gate comparison; it cannot isolate each new optimization.

Once the full-size local fixture stays within the existing budgets under the
declared faults/delays, use disposable short cloud trials, then refresh native
module-bound scale/ANN evidence and diagnostic completeness before spending the
reserved hour/cold populations. Keep acceptance thresholds and freshness,
durability, lease and recovery semantics unchanged.

Placement is a subsequent measurement, not the primary fix. Neither tested
placement profile improved the observed default p95 pair, and Cloudflare's
[placement policy](https://developers.cloudflare.com/workers/configuration/placement/)
affects fetch handlers rather than Durable Object RPC execution.


## Local implementation status

The initial full-size lifecycle comparison used four tenants with 512 documents
each and uniform 20 ms delay from the first storage call. Every case completed
preparation and data checks. These are one-pass local workerd measurements under
concurrent local build load, not Cloudflare results or gate qualification.

| Case | Preparation, all tenants | Publication p95 | Read p95 | Builder PUTs | Retained bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Previous native build | 48.1 s | 1322 ms | 128 ms | 704 | 14,277,228 |
| Batched facts uploads | 43.4 s | 1115 ms | 132 ms | 704 | 14,279,149 |
| Canonical bodies | 39.5 s | 1068 ms | 142 ms | 556 | 14,893,655 |
| Canonical plus existing scheduling experiments | 38.1 s | 981 ms | 145 ms | 556 | 14,907,786 |

The report is `runtime-plan-lab/lifecycle-latency-1793151405274.json` and binds
all three native modules and all four generated service bundles. Candidate native
SHA-256 was `29b0faea06cf6bfc357333f89c0a3703c6b2f5c4627772d4a356b3844cdafc50`;
previous native SHA-256 was
`c0f96a553ebdeb2be64c94d1cdbb3aa6bd753c8666282a029aa56af68550e8ce`.
The subsequent scan change consumes canonical bodies directly from the verified
cursor leaf rather than decoding a second copy of that leaf for every row.

Batching improves dependent upload waves without reducing request counts.
Canonical bodies save builder requests but have a retained-byte tradeoff in this
fixture, and neither option establishes a read p95 improvement. Keep the body
format opt-in and measure page packing/COW amplification before enabling it.

Those measurements predate the durable publication continuations and native
read/write plans implemented below. They do not measure those protocols. The bursty local model,
original gate comparison, module-bound scale/ANN evidence, diagnostic completeness
and reserved cloud qualification populations remain separate checks. No new
Cloudflare resources are required for this local work.


The subsequent native scan build also passed the 16,384-row metadata workload
with both full and compact modules: recovery, immutable committed-prefix
observation, hot WAL/outbox rows, branches with 193 children, and three collection
cycles. Peak calls per native operation were 101/100, transfer was below 2.11 MB,
and native working bytes were below 0.98 MB. This checks metadata scale under
existing caps; it does not substitute for full ANN corpus evidence or cloud
qualification.


The final scan build's deterministic bursty comparison also completed at four
by 512 documents (`lifecycle-latency-1793151765883.json`). Preparation was
102.8/97.4/82.3 seconds and publication p95 was 3477/3093/2662 ms for previous,
batched, and canonical-plus-existing-options respectively. Read p95 was
359/360/251 ms; write p95 was 879/990/812 ms. The write regression in the
batching-only case and differing read results across delay models require
repeated measurements; these samples do not establish a stable SLO win.
Canonical retained bytes remained about 4% higher. The final full native hash is
`afcd379963d3488befe1701f902d08a083e1baa52c2f69313e1261394b821eb9`.

Final verification passed 276 Node tests, 25 actual-workerd integration tests,
663 native serverless tests (six skipped), and the focused native facts tests,
including canonical source locators serialized through enrichment descriptors.
The original 117 latency, 108 gate and 35 cleanup artifacts remained hash-valid.
The code and design are incremental progress; the three native protocol changes
listed above and cloud gate qualification are still outstanding.


## Native publication continuations and read/write plans

The implementation adds three separately selectable operator flags:
`PUBLICATION_CONTINUATIONS`, `NATIVE_READ_PLAN`, and `NATIVE_WRITE_PLAN`.
They default off. HTTP inputs cannot activate them, select a private write plan,
or raise publication slice budgets. They do not change the qualification gate,
request deadlines, WASM reservations, object-I/O budgets or authority protocol.
Deploy the compatible modules to all roles before activation. Flag disablement
keeps checkpoint tracing enabled; finish/cancel active jobs and drain collection
frontiers before downgrading collectors that do not recognize publication tasks.

### Publication within one accepted record

`hosted_publication_plan.zig` owns a durable job over one frozen accepted WAL
record, its source generation/policy and its private manifest. Seekable input is
stored as checksummed 32 KiB chunks in native Merkle pages. Text and sparse
posting replacement checkpoints byte offsets inside the record, with private
posting/document roots, rather than replaying an entire posting list on resume.
A step handles at most 256 posting units and 64 KiB of key work; the default is
128 units. An operation-local metadata snapshot serves planning reads without
repeated coordinator RPCs; native guards still validate every mutation. An
indivisible key remains subject to existing native format limits.
Ordinary records smaller than 16 KiB select the existing prepared batched fast
path through a read-only native probe. It bounds the batch before the first
large record and binds the batch to its source generation. Probe and batch share the original
deadline and aggregate I/O budget; the probe neither leases nor uploads.
`PUBLICATION_CONTINUATION_FORCE=true` exercises the protocol for smaller records;
`PUBLICATION_UNITS` selects 1–256 units, without increasing other limits.

Each slice reacquires the native work lease and creates a fresh upload scope.
`PUBLICATION_PLAN` changes and terminal `PUBLICATION_RESULTS/<job-id>` receipts
compare the current HEAD guard in the same coordinator transaction. The pointer
references an authenticated immutable checkpoint, expires after ten minutes of
inactivity and renews on progress. Data collection traces the checkpoint,
private roots and body/input dependencies; metadata/WAL collection retains the
source manifest and frozen WAL position. Cancellation or expiry releases these
protections through ordinary collection.

Only final fenced HEAD CAS makes the completed manifest public. A changed
source produces a superseded receipt. Candidate version and publication fence
are durable before that CAS, so a lost final acknowledgement can recover its
original result, including through subsequent manifest lineage. Terminal status
is durable and stable after later publication. `publish` accepts
`publication_action: step|status|cancel` and `publication_job_id`; an unkeyed
`publish` starts/resumes work. Partial responses keep the old public version and
WAL cutoff and expose only job/phase/offset progress.

This preserves existing document, projection and page format bounds; it does
not introduce arbitrary-size documents. Dense vector families use their existing
bounded native paged runs and resumable merge maintenance, with a separate
family phase. New text/sparse continuations address the unbounded posting-work
multiplier inside an otherwise admissible record. Legacy flat/graph generations
need the existing paged reindex before this protocol can build on them.

### Overlapping native point reads

The previous lookup returns an internal native facts-root descriptor; the host
retains at most eight 4 KiB descriptors per bucket, with a 60-second lifetime.
These are immutable source hints and carry no authority admission or caller I/O
context. Native code verifies facts membership/body inputs while fresh authority
observation runs independently. An explicit Asyncify barrier joins that
observation before opening the normal native live session. The freshly admitted
HEAD, floor, pin and WAL overlay still govern visibility. Root identity or native
row identity validates a prepared result; a changed row selects the ordinary
fresh reader. Bodies larger than 128 KiB defer body materialization to that
reader, avoiding duplicate speculative copies in the small point lane.

Cold reads without a source hint complete admission before query allocation,
allowing reuse of the observer slot while exporting a hint for subsequent reads.
Global preparation permits reserve one eight-MiB observer slot throughout
multi-call authority observation. Every observer must own admission before its
query starts; required observers can bypass an aged large reservation that would
otherwise deadlock suspended queries. New speculation yields to aged work and
resumes on pool capacity changes. Small-lane promotion first joins observation
and shares the original deadline and aggregate I/O budget with its first attempt.
No completed mutable snapshot is cached or returned speculatively.

### Shared native write planning

`wal/write_plan.zig` reads the preparation root through the native transactional
store and stages indexed-WAL updates, receipts and immutable outputs. It returns
an opaque native commit containing exact WAL identity and semantic publication
HEAD/floor dependencies. Comparing HEAD bodies allows lease-only metadata
renewals without accepting a changed published generation. The final native
transaction binds WAL HEAD, operation receipts and the publication outbox.
Preparation never acknowledges a write or holds the metadata queue over output
construction.

A warm commit-only operation can skip a preliminary recovery observation. The
immutable-log append still authenticates the retained root, floor and seal both
before and after its conditional journal-slot creation. Stale roots, replaced
owners and changed dependencies fail closed and trigger bounded native replanning
under the original deadline and one I/O allowance shared by preparation,
recovery and final metadata commit. Exhausting one caller's allowance cannot
reject unrelated queued callers. Cold paths,
rotation and errors use normal recovery. No mutable R2 HEAD is added.

### Local verification and qualification boundary

The focused native protocol target tests authenticated input cursors, cold
posting checkpoints, private publication roots and collector closure, frozen WAL
prefixes, native read input identity, and semantic write dependencies with
append fencing. Actual-WASM tests publish a 12,000-term record under a twelve
artifact-PUT cap per invocation where whole-record publication fails, including
lost checkpoint and final HEAD acknowledgements and repeated instance eviction.
Actual-workerd tests cover 32 concurrent direct lookups and 256 KiB point-lane
promotion with tenant isolation,
fresh epoch takeover/recovery, native write receipt replay, publication
cancellation, both collector frontiers and erased platform storage.

`qualification/native-protocol-latency.mjs` measures baseline, native read,
native write and combined variants using injected uniform/bursty storage delay
from the first write. New evidence belongs in `native-protocol-lab`; earlier
sealed evidence stays unchanged. Local timing characterizes dependency depth,
not Cloudflare latency or a gate pass. Cloud qualification still needs fresh
authorized disposable stacks and the unchanged corrected-control comparison.


### Final local protocol measurements

The final actual-workerd runs used 512 documents per tenant across four tenants,
20 ms uniform/bursty injected storage delay beginning before the first write,
32 point reads and 16 point updates per variant. Each run compared the preserved
previous native modules with disabled flags, isolated native read/write flags,
and all three protocols together. All eight cases verified tenant counts and
exact bodies. Native modules, four service bundles and bindings were sealed at
startup and retained per case; every variant in a delay model used the same JS
bundles. The full current native SHA-256 is
`79c051479981467823ff1eb8afb6f8d72de66e89cb0e207a4bb808c28dab6960`.

| Delay / variant | Read p50/p95 (ms) | Write p50/p95 (ms) | Publication p95 (ms) | Complete preparation (s) |
| --- | ---: | ---: | ---: | ---: |
| uniform / baseline | 61.5 / 144.8 | 230.9 / 299.7 | 1183.9 | 44.46 |
| uniform / native-plans | 40.7 / 147.5 | 154.8 / 263.1 | 1103.4 | 41.79 |
| bursty / baseline | 116.9 / 358.6 | 337.9 / 861.1 | 3266.9 | 93.86 |
| bursty / native-plans | 64.5 / 244.3 | 234.0 / 679.1 | 3130.3 | 91.90 |

Combined uniform median reads/writes improve about 34%/33%; uniform write p95
improves 12%, while read p95 is essentially flat. Combined bursty read/write p95
improves 32%/21%, with preparation about 2% faster. Bursty publication median
increases about 16% despite its slightly lower p95. Isolated native reads do not
improve uniform read p95, and isolated native writes regress bursty write p95 in
this small population. These results support reduced dependency depth without
establishing a consistent tail SLO or a cloud gate pass. Keep flags opt-in.

The publication probe keeps the existing prepared batch for ordinary records;
its read-only prefix scan stops before a large record and binds that batch to
its source generation. This avoids the earlier individual-metadata-call
regression. Failed/exploratory reports remain preserved, including the short-tail
probe failure fixed by bounding its range to the accepted WAL end.

Final validation: 290 Node tests, all 27 workerd tests, the native serverless
suite's 74 build steps (six existing skipped tests), and all five focused native
protocol tests pass. The native tests report no leaks. Additional authority
checks verify that preparing a write changes neither the recovered metadata
root nor its visible WAL HEAD. The original 117 latency, 108 gate, 35 cleanup
and 63 runtime-plan artifacts remain hash-valid. No Cloudflare stacks or
credentials were created or deployed for this work.

Reports: `native-protocol-lab/lifecycle-latency-1793158659957.json` and
`native-protocol-lab/lifecycle-latency-1793158663131.json`.
The local evidence manifest also binds the final modules, source snapshots,
service bundles, test logs and preserved exploratory runs. Cloud gate work still
requires fresh disposable deployments, refreshed module-bound scale/ANN runs,
complete accounting and the unchanged paired hour-long/cold populations.

### Live Cloudflare follow-up

The fresh native protocol deployment completes large-record publication across
actor replacement and substantially improves the full-corpus paired diagnostic.
Read p95 is still 1,138 ms and invocation accounting remains incomplete, so this
is not a gate pass. The evidence, limitations and next design changes are in
[the live native protocol diagnostic](cloudflare-native-live.md).

### Shared packed publication follow-up

The next implementation uses native packed posting batches, prepared atomic
slice commits, adaptive work within records, fresh-read dependency reduction
and disjoint invocation accounting. The local delayed large-record comparison
improves publication substantially; ordinary full-corpus read/write latency is
flat. See [shared object-backed publication](shared-object-publication.md) for
the contracts, rollout flags, measurements and remaining cloud requirements.

### Detached native read follow-up

Fresh authority waits no longer need to retain speculative query stacks. The
optional two-stage protocol uses Antfly's bounded native prepared cache and
best-effort idle-kernel affinity. A held tenant's authority fetch no longer blocks
another tenant's independent read under the existing app reservation. See
[detached native reads](detached-native-reads.md) for the protocol and evidence.

## Shared read-path follow-up

[Shared native read state and point projections](native-read-state.md) extends this
design with fused fresh admission, bounded native overlay reuse, optional atomic
WAL document indexing, foreground cache admission and opportunistic preparation.
The shared engine owns these mechanisms; hosts without the optional WAL
capabilities retain bounded range reads. Earlier measurements and frozen cloud
controls above remain unchanged.
