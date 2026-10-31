# Proof acquisition before execution admission

Fresh metadata observations need remote proof inputs even when all immutable
query data is warm. Holding a WASM slot while those GETs wait competes with
point preparation and execution. This experiment separates the existing proof
acquisition from execution admission, without moving proof validation into the
host or changing the committed-prefix contract.

## Shared protocol and ownership

The existing read-ahead implementation lives in
`zig/lib/objectstore/wasm/read-ahead.mjs`; the example's previous import reexports
it for compatibility. A previously authenticated cursor supplies addresses for
CHECKPOINT, its prior epoch's seal and next slot. A newer checkpoint may supply
two replacement proof addresses. These are advisory addresses. Native immutable
log recovery still authenticates the checkpoint/cursor, follows seals and the
contiguous commit prefix, and selects the root. Native ordered metadata reads
still select every returned row and durable reader pin.

The new `prime` operation waits for the selected initial proof inputs before
reserving a WASM slot. It neither writes metadata nor interprets a JavaScript
result as authority. Each result is consumed once by that request's native
execution. Missing results and transport errors are request-local; negative
observations never enter a reusable cache. Cold observations without a usable
cursor keep the original native path.

Priming begins in the submitting request's context. Admission callbacks only
resolve that submitter's promise; the releasing caller never starts someone
else's I/O. Cohorts close at the first physical proof read, so later requests
cannot join an observation whose proof acquisition has already begun. The
original deadline continues through queuing, storage, execution and draining.

## Admission and bounds

`DETACHED_METADATA_PROOFS` is independent and defaults off. Observer identity
includes the setting. Native hosts and other WASM hosts may reuse the shared
read-ahead/budget code; this does not add a GCS-capable WASM write adapter.

The shared budget permits at most four active proof flights and at most one per
namespace. A stalled namespace's followers wait; other namespaces retain their
own eligibility. Queue ordering favors another eligible namespace over the last
admitted one. There are at most 32 waiting flights, and their queue reservations
share the execution pool's existing queue ceiling with ordinary native jobs.
This does not create a second independent 32-job backlog. Waiting for a proof
permit or for storage holds no WASM reservation. WASM slot counts and memory
limits are unchanged.

A flight owns at most five response bodies of 16 KiB each, with at most 4 KiB
of serialized metadata per response. Its input allowance is 100 KiB, at most
400 KiB across four flights. These are bounded serialized-input allowances;
JavaScript object/VM overhead remains part of the platform's managed-memory
limit. All speculation is also charged conservatively against the original
254-operation/eight-MiB observation budget before dispatch. Native execution
continues charging its own operations and transferred bytes; admission does not
refund the extra reservations or expand the budget.

If there is no request-lifetime port, the original in-kernel path is used.
Queued cancellations remove their reservations before starting I/O. A full queue
returns the existing explicit admission/backpressure result.

## Deadlines and physical settlement

The observation retains its original absolute deadline, at most five seconds,
and a local monotonic deadline prevents a wall-clock rollback from extending it.
A timed-out binding GET cannot necessarily be canceled. The response can fail
promptly, while its proof permit and active-reader count remain held until all
started physical I/O settles. Its combined settlement promise is attached to
the error and registered with the submitting request's lifetime port. It cannot
open replacement input capacity merely because a logical deadline elapsed.

Unused old-epoch probes drain too. Closing a failed flight prevents a late
checkpoint response from launching additional speculative reads. Finalization
captures the native I/O lifetime before attaching its combined lifetime to the
error, avoiding a settlement promise waiting on itself. Fixed-code resource
failure reporting remains attached to physical transport after the HTTP result;
a later successful outcome cannot erase an observed resource failure.

## Validation and rollout

Contracts exercise actual native WASM through the object transport, including
an occupied/stalled proof while another caller uses execution, stale negative
inputs, owner replacement, deadline return before physical settlement, late
resource failure reporting, per-namespace progress, queued cancellation and the
combined queue ceiling. Actual-workerd checks must prove the active flight
counter increments, preserve originating I/O contexts and make progress while
another tenant's authority is suspended. Logs retain the bounded `observe_read`
action label and numeric proof-admission counters.

Run matched delayed and zero-delay ablations in both orders, retaining exact
modules, bundles, bindings and physical I/O. Native modules remain unchanged
from the qualified multiprovider checkpoint. A local latency result is not a
Cloudflare gate pass. Keep the flag off until final local evidence selects it,
then run complete live diagnostic/accounting prerequisites before the unchanged
paired-hour and physical-recovery populations.

Generic native indexed-WAL page reclamation, broader HTTP adoption, coordinated
legacy migration and colocated provider parity remain separately tracked in
[multiprovider serving](multiprovider-serving.md). No collector may use this
read-ahead experiment as a substitute for durable reader/publication protection.

The current implementation passes all 328 Node tests, including six dedicated
proof-lifecycle contracts, and all 31 actual-workerd cases. The optimized
workerd profile asserts that detached proof acquisition actually runs and stays
within its input allowance. The passing full log includes an intentional
legacy `SQLITE_NOMEM` injection, as corrected in the
[SQLite memory audit](cloudflare-sqlite-memory.md). Latency and cloud qualification remain
open at this checkpoint; all failed/intermediate proof tests are preserved.

## Measured selection at source `131a703fed`

The matched actual-workerd runs retain the qualified native modules and vary
only `DETACHED_METADATA_PROOFS`. Four tenants use 512 documents each, 192 reads
and 64 writes with fixed 20/100 ms binding delays. Zero-delay runs use 64
documents per tenant, 96 reads and 32 writes. All completed cases verify exact
bodies, visibility and counts; detached cases observe 196 or 100 proof calls.

| Population | Existing read/write p95, ms | Detached read/write p95, ms |
| --- | ---: | ---: |
| Delayed, existing first | 670 / 1,170 | 572 / 1,177 |
| Delayed, detached first | 582 / 1,172 | 613 / 1,194 |
| Zero delay, existing first | 141 / 141 | 255 / 255 |
| Zero delay, detached first | 236 / 236 | 306 / 306 |

Keep detached proofs **off** in the live candidate. The first-order delayed gain
does not repeat, and both zero-delay orders regress. This is a correctness-tested
independent control, not a demonstrated performance improvement. Retain all
four populations and the failed localhost-listen attempt under
`dist/qualification/proof-overlap-lab/`; do not discard the unfavorable order.
The candidate also keeps affinity and the WAL document index off, as selected by
the separate multiprovider ablation. Live coverage and the gate remain required.
