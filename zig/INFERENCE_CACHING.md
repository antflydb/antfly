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

The cache and `ManagedEmbedder` provider clients borrow the API lane's `std.Io`
from `BackendRuntime`; neither creates or owns an executor per request. This
keeps synchronization, network I/O, shutdown, and runtime resource ownership
under the server lifecycle. Provider pacing also sleeps through that runtime
and reserves request start slots without holding its mutex across network I/O,
so throttled providers do not pin worker threads or serialize unrelated runtime
work. Test or embedded construction without a backend runtime uses Zig's
explicit global single-threaded I/O fallback and does not create a private
thread pool.

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
result and callers cannot mutate cache contents. Hit entries are pinned while
the caller-owned copy is made outside the global LRU mutex. Eviction detaches a
pinned entry immediately but retains its memory charge until the final pin is
released, preserving the hard budget without serializing unrelated hit copies.
Flight references similarly keep completed producer results alive while
producers and waiters copy outside the mutex.

Provider results cross a strict validation boundary before they can enter the
cache or search/index code. Dense and sparse batches must match the requested
cardinality, dense dimensions must match configuration, sparse index/value
shapes must agree, sparse indices must be strictly increasing, and every
numeric value must be finite. Malformed upstream or embedded-runtime output is
rejected and never cached.

Lookup and flight registration occur under one mutex. A miss installs exactly
one producer before releasing the mutex. Waiters sleep on that flight's
completion event and are always released on success or error. Public-query
waiters stop waiting at their request deadline without canceling a producer
that may still serve other callers. The producer receives the same absolute
deadline: provider pacing refuses slots that cannot be reached in time, and
remote connect, read, write, and whole-request limits are capped by the
remaining budget. An embedded model call that has already started follows the
embedded runtime's cancellation contract, but its result is never allowed to
extend a waiter's deadline. Completion of one key never wakes waiters for
unrelated keys. Errors are delivered to current waiters but are never cached.
Producer computation runs without the cache lock.

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

Operators can override the policy in the normal inference configuration:

```yaml
inference:
  api_url: http://127.0.0.1:8082
  query_embedding_cache:
    enabled: true
    max_bytes_mb: 64
    ttl_ms: 300000
    max_inflight: 1024
```

`max_bytes_mb: 0` disables result retention while preserving singleflight.
`enabled: false` bypasses both caching and singleflight.

`max_inflight` bounds distinct producer keys before provider pacing and local
inference queueing. Requests for a key already in flight still coalesce when
the limit is reached; new unique misses receive an overload response. This
prevents high-cardinality traffic from turning the singleflight map into an
unbounded upstream queue. Public overload, provider rate-limit, and transient
provider responses include a short `Retry-After` hint so clients can back off
instead of immediately amplifying pressure.

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
bytes, aggregate producer compute time, and aggregate budget use/rejections.
The endpoint also reports in-flight admission rejections and waiter timeouts so
operators can distinguish upstream saturation from cache-capacity churn.
Metrics snapshots also expire a bounded batch from an idle LRU tail, so a quiet
node converges without allowing one scrape to monopolize the cache lock. These
signals distinguish useful reuse from high churn, insufficient capacity, an
inherently slow provider, or ineffective request affinity.

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
