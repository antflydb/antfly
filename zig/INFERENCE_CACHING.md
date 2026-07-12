# Inference caching

## Purpose

Antfly uses two materially different inference caches:

- Query embedding caches retain small immutable CPU vectors and coalesce
  identical concurrent computations.
- Prompt-prefix caches retain model-specific KV blocks, potentially in both
  host and device memory, and skip generation prefill work.

They share resource, isolation, lifecycle, and observability requirements, but
not a cache data structure. A generic LRU cannot safely own backend KV blocks,
and a KV block cache does not provide the completion state required for
singleflight.

## Architecture

```text
public query
    |
    v
ApiHttpServer query-embedding L1
    |  hit: owned vector copy
    |  concurrent miss: wait for producer
    v
ManagedEmbedder provider pacing / embedded inference queue
    |
    v
embedding provider or local model
```

`ApiHttpServer` owns one process-wide `QueryEmbeddingCache`. It uses a
node-supplied `CacheBudget` when its runtime has one and otherwise creates a
local fallback budget. The stack-local `SemanticStatusResolver` and
`QueryPlanningContext` borrow both. The singleflight boundary is outside
`ManagedEmbedder.embedQuery`, so waiters do not consume provider rate-limit
capacity or local inference queue slots.

Cache contents remain process-local. They do not belong in metadata Raft and do
not require distributed invalidation. Multiple API replicas may each perform
one cold computation. An inference-node L2 can be added if measurements show
that cross-API-replica duplication is material.

## Query embedding key

The key is a SHA-256 digest of the effective operation:

- key schema version;
- server-derived security domain and scope;
- provider kind and endpoint;
- model and region;
- output dimensions;
- input type and truncation behavior;
- provider credential identity;
- exact input bytes.

Table and index names are omitted, allowing equivalent embedding
configurations to share results. Search limits, shard placement, filters, and
full-text clauses are also omitted because they do not affect the vector.

Authenticated requests use a principal scope. Anonymous and internal work use
different domain tags, so a username cannot collide with either namespace.
Callers never supply the complete cache namespace.

Text is not trimmed, lowercased, or whitespace-normalized. Those changes can
alter tokenizer output. Templated and multimodal queries currently bypass the
cache because templates can resolve mutable remote content. They can be added
after query preparation exposes a stable, fully rendered text-only operation.

## Ownership and concurrency

Cached vectors and in-flight producer results are cache-owned. Every caller
receives an owned copy, so request cleanup cannot invalidate another request's
result and callers cannot mutate cache contents.

Lookup and flight registration occur under one mutex. A miss installs exactly
one producer before releasing the mutex. Waiters sleep on a condition variable
and are always broadcast on success or error. Errors are delivered to current
waiters but are never cached. Producer computation runs without the cache lock.

Shutdown requires request handling to stop before `ApiHttpServer.deinit`, which
asserts that no flights remain before freeing cache state.

## Eviction and accounting

The query cache uses idle TTL plus true LRU eviction. Hits refresh both expiry
and recency. The default policy is:

- enabled;
- 5-minute idle TTL;
- 64 MiB logical and shared hard limit;
- no error caching;
- reject a result larger than the cache limit.

Each entry charge includes the vector, entry object, key/value storage, and a
conservative allowance for hash-table occupancy. The cache reserves its charge
from `CacheBudget` before publishing the entry and releases it on expiration,
eviction, or shutdown.

`CacheBudget` is the reusable coordination boundary. It atomically enforces an
aggregate process limit while allowing each consumer to implement correct
resource release. When prompt-prefix caching is merged, its host metadata,
host KV copies, and device KV allocations should reserve from the same kind of
node-owned coordinator. A cache that cannot reserve must evict its own entries
or reject admission; resource-pressure observation alone is not enforcement.

## Metrics

The data-server metrics endpoint exports query-cache hits, misses, coalesced
waiters, producers, evictions, expirations, rejected admissions, entries, live
bytes, and aggregate budget use/rejections. These distinguish useful reuse from
high churn, insufficient capacity, or ineffective request affinity.

Do not include cache keys, query text, principal names, credentials, or vector
contents in logs or metric labels.

## Follow-up layers

An inference-node L2 should use another `QueryEmbeddingCache` instance with an
independent quota and metrics. Only query-purpose requests should enter it;
document enrichment must not evict the query working set. Remote calls need an
authenticated internal purpose signal rather than a client-controlled public
flag.

The global NDJSON multiquery endpoint can additionally memoize operations for
the request and execute table searches concurrently while preserving response
order. This provides deterministic one-computation behavior for explicit
multi-table queries, while the process LRU/singleflight protects independent
requests.
