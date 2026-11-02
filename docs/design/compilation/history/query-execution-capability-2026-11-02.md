# Owner-issued query execution capabilities

Status: implemented and qualified with focused and integrated Debug tests.
The storage-free dispatcher, compatibility issuer, retained-query owner and
storage-owner tests pass. All seven production cache phases pass, including an
actual DB layout change. This completes the query compilation family; the
full P4 private-layout objective remains open.

## Operation ownership

Complete query parsing, storage execution, retained-cut handling, aggregation
and response encoding now execute in the existing storage-kernel owner. The
local-query archive is a small storage-free dispatcher. It imports only the
query wire contract and semantic failure identity; it has no DB, Core,
IndexManager, DocStore, parser/backend or engine-descriptor input.

Co-location is deliberate. Aggregation consumes the private Core's index
manager and document store under a single read lease. Splitting that path into
per-posting/candidate/document callbacks would add hot dispatch and complicate
snapshot ownership. The storage-kernel owner already needs those private
layouts, and its earlier emitted text was below the storage-engine/API owners.
The receiving owner's size and compiler cost must still be measured; the
previous separate query archive size is not a predicted saving.

## Capability and compatibility checks

`LocalQueryRequest` carries an owner-issued `QueryExecution`: an independent
version, opaque context and one C callback for the complete operation. There
are no private DB layout fields in the dispatch request. The dispatcher checks
request/capability versions, reserved fields and required pointers before
calling owner code. The private execute symbol is versioned `_v2`, preventing
stale native requests from linking to the changed layout. Public JSON, query
operation ordinals and failure identity version 86 remain unchanged.

Legacy physical CAPI/Lite callers still hold concrete DB values. They obtain a
capability through the storage owner's checked issuer. Its compatibility input
retains all prior compiler/layout/alignment checks before the owner casts a DB
pointer. The issuer is not the query dispatcher. This migrates the query
compilation's private-layout dependency without silently removing safety from
remaining physical consumers.

A capability is valid for one synchronous complete call. The donor and its
read admission remain alive through that call, including joined worker work
and serialization. It stores no temporary allocator adapter. Existing retained
native-cut creation/reopening and generation leases remain inside the complete
owner operation; their cleanup and failure behavior are unchanged. Responses
remain provider-owned C-allocator buffers with an explicit destructor.

## Remaining private-layout coupling

This is one complete operation family, not completion of P4. The engine
versioned descriptor still validates the VTable, DB/Core/handle/registry,
backend runtime/resource manager, prepared owner/allocator/embedding memo,
async/enrichment contexts, relational/document read sessions, index manager,
algebraic index, document store, rewrite/promotion/resolution state, backup and
native generation/staging/writer types. Legacy physical CAPI/Lite callers also
retain the checked issuer input. Removing those checks before migrating their
actual values/borrows would reduce safety and leave the implementation imports.

The next safe family is relational statement/read sessions. The engine still
passes concrete Reader/RelationalSession/DocumentSession and typed-layout/page
values through open-at-snapshot, checkpoint, range proofs, typed-page reads,
batch lookups, normalization and destruction. Replace one complete statement
and session family with owner-issued handles plus whole page/batch operations
and explicit page release. Keep statement generation, range fences,
cancellation, IO, donor admission and close/drain ownership inside that owner;
avoid per-row callbacks.

DB lifecycle is a larger subsequent cut. The engine still returns DB by value
from open, query-cut open, verified restore-source open, managed DB open and
restore DB open. Even openOwned/closeOwned expose concrete DB pointers. Migrate
one lifecycle consumer, such as a restore/query-cut carrier, to an owning
opaque open/close handle and complete operations before dropping its legacy
layout checks. Public/source-only APIs remain separate from those private
native ownership changes. The descriptor currently contains 28 TypeContract
fields plus field-layout tables; this query migration does not remove them.

## Qualification

- Debug dispatcher archive: one second, observed 251 MB compiler RSS in the
  initial focused build. These observations do not qualify release reservations.
- Compatibility DB issuer contract test passes and still rejects all altered
  layout fields before using a poisoned pointer.
- Three dispatcher tests pass: fail-closed capability validation, complete
  operation and response destruction, exact owner error/stage forwarding.
- Integrated Debug validation passes all 68 build steps: three dispatcher, one
  compatibility contract, three retained-query owner and 57 storage-owner tests.
  The retained-query tests cover write/profile generation continuity, deadline
  and cancellation recovery, and remote retained-generation continuation.
- With shared image and lake owners enabled, the integrated template/query/
  storage-owner build passes all 78 steps. A later shared-image/lake run after
  source-test isolation and private media guard changes passes all 69 query
  validation steps (dispatcher, compatibility, retained-query and storage owner).
  The production graph preserves one decisions-module identity across source
  and facade consumers.
- Source regressions pass with both native owner flags: single-range remote
  text continuation (one selected test, 57 successful build steps) and complete
  aggregations with preserved hit paging (one selected test, 105 build steps).
  The physical API fixture uses one source media graph; consumer fixtures retain
  the facade graph.
- Strict production cache qualification passes all seven phases against an
  immutable snapshot of 4,680 authored/construction Zig files (246,638,710 bytes).
  Assets remain shared; the temporary source overlay and its local compiler
  output are removed after completion. The synthetic DB field insertion also
  passed `zig fmt --check` before compilation.

| Edit | Query dispatcher | Storage kernel | Opaque query consumer |
| --- | --- | --- | --- |
| Initial | Built | Built | Built |
| Warm | Cached | Cached | Cached |
| Dispatcher implementation | Built | Cached | Cached |
| Complete query implementation | Cached | Built | Cached |
| Storage implementation | Cached | Built | Cached |
| Actual DB field/layout | Cached | Built | Cached |
| Shared query ABI contract | Built | Built | Built |

The receipt is
`.benchmark-results/compile-costs/opaque-query-execution-cache.json`
(`passed: true`), with one per-phase log. Functional receipts are
`query-source-capability-debug-v2.log` (69/69 steps),
`query-source-aggregation-debug-v2.log` (105/105, selected aggregation test), and
`query-source-remote-cut-debug.log` (57/57, selected continuation test) under the
same directory. Source tests invoke the owner-issued complete callback directly;
only the independent dispatch root imports its implementation, so test-only
literal imports do not invalidate the physical owner.

All iteration uses Debug. No release size, cold-build time, aggregate memory or
serving-performance improvement is claimed from these changes yet.
