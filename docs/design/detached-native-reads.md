# Native read preparation without suspended query slots

The first native read plan overlapped immutable preparation with fresh authority
observation, but kept its Asyncify query stack suspended at the authority barrier.
Three same-tenant queries could occupy three of the app's four WASM slots while
that tenant's authority observer occupied the fourth. A second tenant could not
observe its own authority, even with independent storage and metadata.

The new opt-in path separates immutable preparation from fresh execution. The
native prepared working set lives in Antfly's shared serverless query engine,
so other object-backed hosts can use the same protocol. The Cloudflare adapter
owns caller-context scheduling and bounded WASM admission.

## Protocol

1. Start the request's fresh authority observation before joining the bounded
   preparation queue. Preserve its original deadline and caller I/O context.
2. If there is an immutable source hint, run a short native lookup preparation
   with a random 128-bit ticket. Its transport permits only immutable artifact
   GET/HEAD and bucket existence. It cannot read authority, write, or suspend on
   a read-view barrier. Native code authenticates the root, membership and body.
3. Retain the authenticated preparation in the kernel's bounded native cache,
   return only retention status and byte accounting, and release the WASM slot.
   Await authority with no query stack or physical slot retained.
4. Admit the final query against the fresh metadata snapshot. Prefer the previous
   kernel only if it is already idle and has the matching variant/reservation.
   Never wait for a busy kernel to preserve a cache hit.
5. Consume the ticket in native code. Validate the namespace and document,
   then use the existing live reader: fresh pin/floor checks and visibility
   admission still apply; the fresh WAL overlay wins. Prepared bytes are usable
   only against the matching fresh root or a freshly validated matching row.
   Missing, evicted, recycled, foreign or already-consumed tickets use an ordinary
   fresh read. A ticket conveys no authority or membership claim on its own.

Even a read with no source hint uses fresh admission before query execution.
The existing single-call barrier protocol remains available for compatible hosts.

## Resource and failure contracts

The native cache owns every retained byte and no request pointers, transport,
context, live session, admission time or authority promise. It retains at most
four entries and 256 KiB, including entry and document-key charges. Unmaterialized
large bodies are not cached. Bucket/prefix scope changes clear it; each entry
also binds the namespace and document. Consuming an entry transfers ownership to
the request, which frees it on success or failure. Eviction never affects durable
state. Cached immutable bytes require no retention extension or new durable pin.

Retained allocations remain inside the existing WASM linear-memory maximum and
pool reservation. `prepared_read_cache_bytes` reports their native charge; the
physical linear-memory and aggregate-pool counters remain authoritative. Cache
allocation churn can cause the normal kernel recycle and a subsequent cache miss.

Preparation and final execution share the original 30-second deadline, 1,024
object-operation allowance and 32 MiB transfer allowance. Preparation byte use is
also deducted from the native 16 MiB read allowance. Small-lane capacity promotion
subtracts both preparation and earlier attempts before retrying. It cannot replay
integrity failures, traps, arbitrary transport errors or cancellation. Preparation
errors fail the request; an absent immutable hint object is an ordinary miss.
An error still drains already-started authority work before the request ends.

Preparation produces `antfly.read_preparation` diagnostics. The final
`antfly.operation` includes combined native I/O and elapsed work, and reports
`native_read_preparation_operations` separately. These are diagnostic events;
they do not replace independent platform invocation coverage.

## Rollout and local evidence

`NATIVE_READ_DETACHED=true` requires `NATIVE_READ_PLAN=true` and the R2 native
read path. It defaults off. HTTP inputs cannot set native prepare/resume tickets
or accounting fields. Sealed runtime provenance records the flag. Disabled hosts
omit the new request fields, preserving compatibility with older native modules.
Deploy compatible modules before enabling it. There is no persisted format change;
disabling it or recycling a kernel only loses optional immutable work.

The actual-workerd regression holds tenant A's metadata CHECKPOINT fetch while
three A queries are admitted. Tenant B must finish its own read before A is
released, within the unchanged four-slot/32 MiB app limit. The new path passes;
turning it off reproduces `TenantBBlockedBehindTenantAAuthority`. The separate
native tests cover fresh updates/deletes, changed roots, ticket eviction, wrong
namespace/document and spent-byte bounds. Allocation-checked native cache tests
verify ownership, one-use consumption and byte eviction without leaks.

The latency harness is
`examples/cloudflare-serverless/qualification/point-read-latency.mjs`. It publishes
512 documents per tenant across four tenants and offers 192 point reads alongside
64 writes over 16 rounds, verifying exact bodies, visibility fences and final
counts. Fixed bursts apply five times the base binding delay every fourth round,
independently of I/O count. Reports seal modules, bundles and bindings. This
comparison's previous-native control is `fd5014a6ccf84984f0d1e9ec26243dce503ffacae7a50b89ba417346385d9aaa`,
not the frozen Cloudflare qualification control `c44896ac8c`.

An earlier 512-document run comparing ordinary and canonical small-body storage
found read p95 of 1,102/1,099 ms and write p95 of 1,169/1,168 ms. Canonical storage
alone did not resolve scheduling. Local injected-delay measurements characterize
dependency depth, not cloud SLOs. This change does not establish a cloud gate
pass, complete invocation accounting, or a write-tail improvement.

Two fixed-burst comparisons retain both run orders. The first has read p95
1,313 → 1,141 ms and write p95 1,161 → 1,169 ms. The reverse-order comparison,
after the full suites completed, has read p50 281 → 210 ms, read p95
1,079 → 966 ms (10.5% lower), and maximum read 1,406 → 982 ms. Write p95 is
1,157 → 1,160 ms, effectively flat. Each side has 192 reads and 64 writes;
all exact-body, visibility and final-count checks pass. These are two local
comparisons, not statistical confidence in a cloud SLO or a write-tail fix.

Reports are `lifecycle-latency-1793203742127.json` and
`lifecycle-latency-1793204306197.json` beneath
`examples/cloudflare-serverless/dist/qualification/point-read-lab/`. The earlier
canonical-layout comparison is `lifecycle-latency-1793202661970.json`. Each case
preserves its exact measured service bundles and bindings. Deferred-metrics
lifetime cleanup followed the reverse-run bundle capture; `FUSED_READS` was off
in both comparisons, so that branch was not exercised. Its final combined-mode
workerd regression passes separately. The final native full-module SHA-256 is
`7f77ceaa16286537dae81d400452b511c3e922e148b9d215d0b12a6142899db5`.

Validation passes all 307 Node tests and all 30 workerd cases, with final focused
checks for promotion budgets, ticket reuse, public-field sanitization and combined
deferred-metrics operation. The native serverless target passes, including 596
main-population tests with six existing skips and all six focused protocol tests
without leaks. The full workerd log retains one recovered `SQLITE_NOMEM` diagnostic
from local platform storage; all cases finish successfully. This is not evidence
that local emulator memory pressure has been eliminated.

All 1,074 older sealed artifacts remain hash-valid. No Cloudflare resources were
created. The cloud acceptance gate remains unpassed: final-module scale/ANN
coverage, complete invocation accounting and the unchanged three paired
hour-long/cold populations remain necessary. Write-tail latency and fresh
metadata/WAL dependency depth need further work.

## Shared read-path follow-up

[Shared native read state and point projections](native-read-state.md) extends this
design with fused fresh admission, bounded native overlay reuse, optional atomic
WAL document indexing, foreground cache admission and opportunistic preparation.
The shared engine owns these mechanisms; hosts without the optional WAL
capabilities retain bounded range reads. Earlier measurements and frozen cloud
controls above remain unchanged.
