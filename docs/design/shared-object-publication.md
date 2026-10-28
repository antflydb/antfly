# Shared object-backed publication and read planning

This implementation reduces durable round trips in Antfly's native object
engine. It builds on the shared authenticated ordered-page engine rather than
adding a Cloudflare-specific posting format or a second index implementation.
The generic work controller is in `zig/lib/objectstore/src/work_quantum.zig`;
posting construction and fresh reads remain in the shared serverless engine.
The Cloudflare adapter supplies transactions, transport and scheduling.

## Packed immutable posting batches

Antfly already has packed immutable pages, authenticated routing roots and
bounded incremental edits. Text and sparse continuation slices now use these
primitives for larger batches. Newly written leaves can share one immutable
object; routing parents are serialized only after final child locators exist.
The next checkpoint references that authenticated root. Text's ordinary native
batch publisher also uses incremental packing when its store supports it.

This achieves bounded segment construction without introducing a growing list
of posting runs in every published manifest. Routing-tree edits absorb each
batch eagerly, so queries keep one indexed root and do not acquire a new merge
fanout or compaction requirement. Existing packed-page readers, retention and
collectors continue to apply. Dense vector runs retain their existing protocol.
Large records are still subject to native document, key, page and heap limits.

Frequency reads remain split into batches of at most 256 keys, independently of
how much total posting work a slice prepares. Text frequencies accumulate
correctly across slices; sparse duplicate weights and deletion/replacement retain
the existing native encoding and semantics. A partial upload cannot advance the
checkpoint, and each retry uses a fresh fenced upload scope.

## Prepared publication slices

The native continuation accepts an externally admitted native work lease, like
the ordinary prepared publisher. The outer transaction owns its final release.
Lease renewals, HEAD guards, checkpoint changes and terminal receipts operate on
an operation-local tentative metadata view while artifacts upload separately.

The authoritative final transaction:

1. Validates current native ownership and expiry.
2. Validates the original metadata read set, including checkpoint dependencies.
3. Applies the tentative checkpoint/HEAD/receipt changes and read protection.
4. Releases ownership in the same durable commit.

Concurrent WAL appends are not spurious publication conflicts: the frozen prefix
remains the input, and native WAL retention/floor checks remain authoritative.
A changed source generation, checkpoint, owner or floor rejects finalization.
A terminal result is not returned to HTTP callers before this commit succeeds.
An ambiguous acknowledgement can be recovered from the durable checkpoint or
terminal receipt. Failed preparations leave only ordinary collectible orphans.

The host permits only the namespace's publication metadata keys, bounded numeric
pin/offset keys and canonical terminal job IDs. Guard ETags are checked against
the tentative HEAD locally and the original authoritative records at commit.
Unknown keys, foreign guards, excessive batches and duplicate writes fail closed.
Late terminal receipt reads add their exact ETags to the same bounded read set.

Direct native callers without an external prepared transaction retain the
existing acquire/guard/checkpoint/release path. The native lease and checkpoint
semantics are shared; hosts need not implement a separate database protocol.

## Adaptive work within records

Record batch feedback already existed. The new native controller adjusts posting
work inside a record, using completed units, elapsed time and actual read/write
bytes. It grows only after a full successful quantum with ample headroom;
a short tail alone does not teach a smaller quantum. It shrinks on pressure.
A slice retains the existing byte and request admission limits independently of
its unit estimate. Initial work remains operator-selected (normally 128 units),
with an internal adaptive ceiling of 4,096 units and bounded key-work batches.
This ceiling changes work granularity, not the CPU, memory, object-operation or
transfer allowance of a request.

The next quantum is checkpointed with the private native job. The adapter also
maintains a scheduling ceiling outside the job. Successful guarded commits feed
back the next admitted quantum; existing resource-failure backoff lowers that
ceiling. Native execution takes the minimum of the checkpoint's estimate and
the admitted ceiling, allowing a retry to do less work without modifying or
advancing the last durable checkpoint. Other hosts can use the same native
ceiling and generic controller with their scheduling policy.

## Fresh read dependency reduction

Prepared native reads already overlap immutable work with fresh authority
admission. Selection now consults the fresh committed WAL overlay before
validating an obsolete published row. A recent update or deletion can therefore
satisfy the lookup without traversing a superseded facts tree.

When the published root changed and the overlay is silent, the fresh facts
reader validates membership and materializes the current body in one traversal.
It can reuse already-authenticated prepared bytes only when the fresh row's LSN,
length and digest match. Previously, a changed row could cause a fact lookup
followed by a second traversal for the body. Fresh HEAD, pin/floor checks,
visibility fences, original deadlines and aggregate I/O budgets still apply.
No reusable mutable authority snapshot is introduced.

These changes reduce dependency work, but the measured full-corpus read latency
is essentially unchanged. They are not evidence that the one-second cloud read
SLO has been reached. A retained older base plus a fresh WAL delta would require
an additional explicit retention and admission protocol; this implementation
does not silently introduce that consistency change.

## Operation identities and invocation populations

Native continuation responses expose a SHA-256 identity of the exact serialized
checkpoint or terminal receipt. Checkpoint identity survives acknowledgement
loss and changes when durable progress changes. These are outcome identities,
not evidence that every platform invocation was captured. Logs retain only the
allowlisted digest, job ID, phase, offset and numeric quantum.

The diagnostic collector separately captures Worker and DO counters. Namespace
bindings come from the platform API and are tied to the exact disposable script.
Each population carries explicit event types, time scope and sampling metadata.
Reconciliation compares Worker events and DO RPC/alarm events separately; it
rejects duplicate populations, unknown event types, missing bindings, estimated
counts and empty datasets without an unsampled-zero proof. Confidence intervals
are retained as corroboration and cannot replace explicit sampling evidence.
The physical HTTP attempt and Ray identities remain independently checked.

These changes do not repair the historical missing acknowledged HTTP event.
Whole-execution coverage and quiet-role sampling remain prerequisites for the
cloud gate. No new gate acceptance path or relaxed threshold is introduced.

## Rollout and validation

`PUBLICATION_PREPARED_SLICES` and `PUBLICATION_ADAPTIVE_SLICES` are operator-only,
default-off flags used with `PUBLICATION_CONTINUATIONS`. Public HTTP inputs cannot
activate them or raise the native posting ceiling. Sealed runtime provenance
records both flags. New request fields are omitted when disabled so older
compatible modules do not receive unknown fields.

Deploy compatible full, compact and observation modules across all roles before
activation. Adaptive jobs contain a new optional checkpoint field; older strict
readers cannot parse active jobs containing it. Keep compatible collectors and
drain or cancel these jobs before binary downgrade. Disabling a flag does not
make an existing checkpoint safe for an older collector.

The comparison uses the previous native SHA-256
`79c051479981467823ff1eb8afb6f8d72de66e89cb0e207a4bb808c28dab6960`, distinct
from the frozen qualification control. The same local adapter code is used on
both sides. Private local evidence is under
`examples/cloudflare-serverless/dist/qualification/shared-pipeline-lab/`.

For a 12,000-term record, exact body, final term, sparse result and operation
receipt checks pass on both sides. The final comparison reduces publication
from 108 to 21 slices and metadata calls from 761 to 62. At 10 ms injected
binding latency, publication takes 66.4 seconds before and 7.2 seconds after.
Object PUTs fall from 1,482 to 133 in that delayed run. This is actual native
WASM and native metadata over an in-memory R2-shaped binding, not a cloud SLO.

A separate actual-workerd comparison publishes 512 documents per tenant across
four tenants. The final module, with bursty delays based on 20 ms binding I/O,
prepares the corpus in 96.1/94.6 seconds before/after. Read p95 is 251.4/245.4 ms;
write p95 is 730.2/859.1 ms. The write p95 regression is about 18% in this small
population and remains a rollout concern. There are 32 reads and 16 writes per
side. All counts and fixture bodies pass, with capacities and deadlines unchanged.
The reports retain exact modules, sealed bundles and bindings. An earlier uniform
comparison, before the final recovery-ceiling addition, showed approximately flat
preparation and read/write latency; it is preserved as exploratory evidence.

The final native SHA-256 is
`fd5014a6ccf84984f0d1e9ec26243dce503ffacae7a50b89ba417346385d9aaa`.
Final reports are `publication-1793169995997.json` and
`lifecycle-latency-1793170015296.json`; the earlier uniform report is
`lifecycle-latency-1793169174097.json`. These are small local populations and do
not establish cloud SLOs or an ordinary write-latency improvement.

Validation: all 304 Node tests pass; the full 28-case workerd suite and both
collector variants pass (29 distinct workerd cases across these runs). Native
serverless validation passes all 74 build steps, including 596 tests in its main
602-test population with six existing skips, and the five focused protocol tests
without leaks. The standalone shared quantum test also passes. Fault coverage
includes partial packed uploads, lost finalization acknowledgements, changed
checkpoint guards, expired owners, both collectors, erased platform state and
an admitted recovery ceiling smaller than the saved work estimate.

The initial local test runs are preserved too: a legacy host initialization bug
was caught and fixed before the final workerd run, and the sandbox-only native
run could not bind five test listeners. The complete native rerun with local
socket access passes. Neither failure is represented as a successful run.

The cloud gate remains unpassed. Fresh module-bound cloud trials and complete
invocation coverage are still required; older cloud reports cannot qualify
these modules. No Cloudflare deployment is created for this implementation.
