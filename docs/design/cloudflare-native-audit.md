# Native serverless reuse audit for Cloudflare

The Cloudflare profile uses the existing serverless storage and execution
protocols. Finishing this profile does not require replacing them with a new
segment architecture. This audit separates implemented native behavior from
host integration and workload limits.

| Capability | Existing native implementation | Hosted integration |
| --- | --- | --- |
| Incremental document state | `build/document_facts_builder.zig` updates authenticated facts and pending-work trees; the builder loads only touched rows when flat search projections are unchanged. | Used directly, including metadata-only changes. |
| Search artifact reuse | `build/builder.zig` compares projections and reuses unchanged text, sparse, and vector artifacts. | Used directly. Changed flat projections can still require the complete view. |
| Publication continuation | Builder publication commits a WAL watermark; subsequent passes resume from it. | Queue dispatch, bounded input prefixes, and adaptive batch admission. This checkpoints between publications, not inside a flat-index rebuild. |
| Compaction | `build/compactor.zig` rebuilds the pinned published view, preserves document facts, reuses artifacts, fences publication, and handles retry/no-op. | Now exposed as `compact` and an optional scheduled Queue job, using host I/O and bounded preflight admission. It preserves the publication's graph families. |
| Enrichment continuation | `enrichment/worker.zig` scans indexed pending work under a finite cycle and persists CAS-protected stage progress. Source pinning and conditional WAL append reject stale model results. | Used directly. The embedding adapter now splits a document into dimension-bounded provider windows. Only a complete document is appended. |
| Retention and collection | Native read pins, work leases, retirement floors, manifest formats, and page validators define authority and reachability. | The durable hosted collector orchestrates these native rules over R2 and coordinated metadata. |

## Adapter changes

The native compactor previously instantiated a threaded fallback type even
when a caller supplied I/O. It now uses that fallback only for native callers
without an I/O token; freestanding callers must supply host I/O. Existing
native calls keep their behavior. Compaction no longer adds an implicit graph
when the source publication has no graph family, which keeps the admitted
hosted artifact set compatible with collection.

Hosted requests now use the existing `WorkingSetAllocator`, with a default
20 MiB requested-allocation limit and an observed peak. ABI transfer buffers
remain owned by the WASM allocator; the object transport distinguishes that
allocator from the request's working allocator. Responses are copied out
before the temporary allocator wrapper goes away. This is allocation admission,
not a measurement of all WASM or JS memory.

`compact` uses native work-lease fencing, publication lineage, document facts,
and reader protection. Defaults admit at most 1,000 documents and 4 MiB of
manifest-declared artifacts. Those are coarse preflight bounds; transitive body
reads and build amplification remain subject to allocation, I/O, and deadline
limits. An empty unpublished database is a no-op. Failed whole-pass attempts
can be retried; compaction has no new intra-build checkpoint format.

The embedding adapter previously rejected any document exceeding one response
window. It now makes up to eight ordered provider calls under the existing
invocation allowance. Default hosted enrichment admits one document per pass
so that document can use the whole allowance. Native document-level progress,
source checks, and publication remain unchanged. Failed windows publish no
partial embedding set; their work is recomputed on retry. Larger documents
still fail explicit admission rather than introducing untracked partial state.

## Measurement and remaining boundaries

`LOG_METRICS=true` logs numeric operation measurements: outcome, operation and
inference counts, transferred bytes, elapsed and I/O-wait time, native working
allocation peak, WASM linear-memory high watermark, combined lane memory, and
retained JS cache charge. It does not log document bodies, tokens, or model
inputs. Observation failures do not change operation outcomes.

A backwards clock still makes lease checks fail closed. The host now marks
that instance unusable so the Worker can replace it; an invalid in-memory clock
guard cannot permanently wedge a reusable lane. Durable fencing and deadlines
remain in the coordinator. A deterministic rollback/reopen test preserves the
original mutation receipt and reads the published document on fresh compute.

`qualification/benchmark.mjs` runs the actual WASM module over the R2 contract
fixture at increasing dataset sizes. It records publication, cold/warm reads,
compaction, object amplification, memory measurements, and explicit admission
failures. This is a local characterization tool; in-memory fixture storage is
outside the measured WASM budget. It does not measure Cloudflare latency, total
isolate memory, or production throughput.

The real unimplemented boundaries are checkpointing *within* a large flat
index build or individual document, and broader deployed capacity/failure
qualification. Additional segment or staging work should follow a measured
workload that exceeds the admitted existing paths. Joining separate mailbox
state into ranked search remains an application/query extension and is not a
missing serverless persistence primitive. The reference app still makes that
limitation explicit.

## Local capacity observations

The same WASM module completed publication, accepted-visibility checks,
cold/warm reads, and native compaction for 100, 1,000, and 5,000 synthetic
documents. Documents contain a short ID-specific prefix and repeated body
text. The 5,000-document compaction run explicitly raised its preflight to
5,000; the default remains 1,000.

| Documents | Publication peak requested allocations | Publication peak linear memory | Compaction peak requested allocations | Compaction peak linear memory |
| --- | ---: | ---: | ---: | ---: |
| 100 | 312,382 B | 4,587,520 B | 120,535 B | 4,259,840 B |
| 1,000 | 1,308,270 B | 8,912,896 B | 1,029,705 B | 6,684,672 B |
| 5,000 | 6,532,286 B | 25,231,360 B | 4,824,209 B | 15,990,784 B |

Two host boundaries were reached before any new serving format was needed:

- The original 1 MiB PUT cap rejected a growing flat artifact. The existing
  R2 adapter already accepts an explicit write limit; the hosted default is now
  4 MiB, with an 8 MiB configuration ceiling. The control run stopped at
  published LSN 3,168 with `ResponseTooLarge`; all 3,200 accepted documents
  remained visible through the bounded overlay.
- A long-lived instance with no recycling exhausted 32 MiB linear memory at
  published LSN 4,368, despite a peak requested working set of 5,712,390 B.
  Its 4,480 accepted documents remained visible through facts plus WAL.
  Recycling between requests at 24 MiB completed all 5,000 documents with two
  instance replacements and preserved the immutable cache.

The existing publication-prefix setting also materially changes amplification.
At 5,000 documents, a 64-record prefix reduced R2 fixture GET bytes from
259,672,081 to 68,308,149, PUT count from 10,640 to 6,658, and WASM/host transferred
bytes from 888,485,212 to 236,024,165. Both runs completed. This does not remove
whole-view rebuild costs or qualify a 64-record prefix for every deployed
latency profile. The host retains its conservative 16-record default and
durable adaptive reduction.

These results bound this synthetic local workload. They do not establish a
general document-count capacity, real R2 timing, total isolate memory, or
long-term storage/cost targets. Failed admission and successful instance
replacement are recorded explicitly in generated reports.

## Deployed follow-up

A second disposable live run on 2026-10-24 UTC passed a 300-document atomic
batch, publication, concurrent writers, native compaction, collection, retained
retry receipts, and a Workers AI embedding through Gateway. It used a
64-record publication ceiling and spanned Worker versions
`8c1d7565-11d4-4f2a-b5ec-be307721acbf` and
`b837a288-cabe-4988-b230-c8f9803deb2a`. Lookup median/p95 were 107/364 ms in this
small fixture. Captured successful publication operations took up to 23 s;
the successful compaction sample took 1.1 s. These observations are not SLOs.

A fresh incarnation then passed a real 48-chunk document through two ordered
Workers AI/Gateway embedding windows, native compaction, and fenced vector
search. Model and dimensions were `@cf/baai/bge-small-en-v1.5` and 384.
Worker version was `665a85ce-3be8-439e-bccf-ed17b47d5b19`. The captured enrichment
sample reported 970,259 B peak requested allocations, 6,225,920 B linear memory,
two inference calls, and about 4 s elapsed time. Total isolate memory remains
unmeasured. This deployed model fixture does not qualify the local 5,000-row
case against real R2 latency or production concurrency.

Validation passed 553 native serverless tests and 28 owner tests (six skipped,
no leaks), 12 portable native contracts, 29 WASM/host tests with both Binaryen
132 packaging paths, and the persisted workerd suite, including forced lane
recycling, compaction admission, scheduled compaction, model-profile checks,
and collection after compaction.
Both live incarnations were drained, and the disposable Worker, maintenance
Queue, dead-letter Queue, and R2 bucket were deleted. Generated reports and
private deployment files remain ignored under `dist/qualification/`.
