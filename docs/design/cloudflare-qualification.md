# Cloudflare hosted profile qualification

This records a disposable live run on 2026-10-24 UTC for issue #1278.
The fixture is deliberately small; it does not establish arbitrary-dataset
capacity or complete native feature parity.

## Durability and collection

The deployed Worker used R2, a SQLite Durable Object coordinator, Queues,
precompiled Asyncify WASM, Workers AI, and AI Gateway. The fixture passed:

- A 300-document atomic batch, idempotent retries, and a fenced lookup.
- Queue publication, text search, and eight concurrent writers.
- Bounded durable collection and preservation of the original retry receipt.
- A real 384-dimension Workers AI embedding through Gateway.

The successful deployment was version
`5744cc1a-17c6-4599-bb7b-98b0031d37d2`. Its WASM SHA-256 was
`bb21c247e36e614d679492c279330d076aeb19b71d3e2b5b201bb831c03fd496`.
The sealed JS bundle SHA-256 was
`5185038684db5b83451c72fa82877888e53199039e1cd2ef448dea2a5f754fbb`.

Lookup latency in this fixture was 139 ms median and 558 ms p95. Captured
Cloudflare tail events for that version reported maximum CPU of 192 ms for
HTTP and 158 ms for Queue invocations, with maximum wall time of 7.8 s and
24.1 s respectively. Captured events had outcome `ok`; application admission
retries are still expected under contention. These are observations, not SLOs.

Initial 256-record publication attempts exceeded the 30-second host deadline
despite modest CPU use. This led to a default 16-record prefix, durable adaptive
batch reduction, separate interactive/maintenance instances, and reuse of the
authenticated packed body snapshot during cold flat-index rebuilds. The local
cold test published 44 changes over 256 existing documents using 30 object GETs
and 125,185 downloaded bytes. Snapshot bytes remain a cache: current facts
determine membership, revision, body digest, and length.

## Native model enrichment

A fresh incarnation on the same disposable resources also passed a one-message
end-to-end native enrichment fixture: publication recorded pending work, the
Queue invoked the configured Workers AI embedding model through Gateway, native
conditional enrichment appended its derived mutation, and publication made the
vector available to a fenced vector query. The configured model was
`@cf/baai/bge-small-en-v1.5`, with 384 dimensions.
After replacement of the deployed Worker version, the same mutation returned
the identical durable visibility receipt and the fenced vector query still
returned the message. The strict comparison used versions
`5d3304c6-6d6b-4065-8881-b29e81fb84a1` and
`12911922-7bd5-4171-8381-00a78feb957c`.

## Bounds and further qualification

Each WASM instance has a 32 MiB linear-memory cap; the shared immutable JS cache
has an 8 MiB cap. Total deployed isolate memory was not measured. Flat indexes
still rebuild in memory, and larger builds may exceed the admitted profile.
The test does not establish sustained production throughput, throttling
behavior, broad fault injection against deployed infrastructure, or capacity
for every model, chunking policy, tenant distribution, and query shape.

The local workerd suite additionally persists R2 and SQLite across complete
runtime restarts and exercises typed enrichment, immutable model profiles,
reader retention, and the collection grace period. Native and WASM tests cover
stale writers, ambiguous commits, pinned pagination, corruption, source changes
during inference, allocation failure, and bounded admission. Those deterministic
tests complement the live fixture; they do not imply every failure was injected
into deployed Cloudflare infrastructure.

Reproduce with the scripts under `examples/cloudflare-serverless/qualification`.
Keep generated configurations, tokens, bundles, and full reports under ignored
`dist/qualification/`. Disable the Queue consumer, drain the generated storage,
and delete the disposable Worker, Queues, and bucket after qualification.
The recorded run's Worker, both Queues, and R2 bucket were deleted after both
incarnations were drained.

Final local checks passed: 553 native serverless tests and 28 owner tests (six
skipped, no failures or leaks), 12 portable native contract tests, 24 WASM/host
tests, and the workerd restart suite. Both the native Binaryen 132 tool and the
pinned npm Binaryen build passed the WASM suite.
