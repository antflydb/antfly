# Fourfold read and write latency work

Issue [#1278](https://github.com/antflydb/antfly/issues/1278), parent
[PR #1320](https://github.com/antflydb/antfly/pull/1320). The user authorized
implementation of these next steps after the 2026-10-29 short paired workload.
This file records the target and safety constraints; an implementation or local
measurement is not a claim of hosted qualification.

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

Investigate a transport-neutral conditional metadata root using existing opaque
provider write revisions (R2/S3 ETags, GCS generations). A native authenticated
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

## Native authority prototype status

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
integer range survive unchanged. The native object-store suite reports 167
passed, two live-provider tests skipped. This prototype is not yet connected to
the Worker transaction coordinator or enabled for existing databases.

The completed hosted attempt is sealed separately under
`examples/cloudflare-serverless/dist/qualification/fenced-hosted-1793322083103/evidence.json`:
203 artifacts, 771,530,629 bytes, manifest SHA-256
`e7403921c18272877c6e52320b0e0fcd5fee8fc2ca87a07bf071b54f7b8402f2`.
Independent cleanup verified all eleven Workers, namespaces, four Queues, three
buckets and two destinations absent; generated test keys were removed. All
collected failures and the original interrupted driver report remain preserved.
