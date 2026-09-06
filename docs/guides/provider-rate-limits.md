# Provider rate limits

Embedders, generators, and rerankers accept the same optional `rate_limit`
execution policy. For example, an embedder configuration can include:

```json
{
  "provider": "openai",
  "model": "text-embedding-3-small",
  "rate_limit": {
    "requests_per_minute": 120,
    "burst": 2,
    "max_concurrency": 4,
    "tokens_per_minute": 120000
  }
}
```

Omitted limits are unlimited; `burst` defaults to one. Every provided number
must be positive. Legacy embedder `requests_per_minute` and `burst` remain
supported, but cannot be combined with `rate_limit`. Nested configuration does
not inherit the legacy environment-based request limit.

## Pacing and concurrency

`rate_limit.pacing` separates two request scheduling contracts:

- `token_bucket` (default): RPM limits attempt admissions, permits configured
  bursts, and allows overlap up to `max_concurrency`. It does not promise a
  minimum interval between arrivals at a remote server.
- `completion`: requires RPM and `burst: 1`. At most one attempt is active;
  after it finishes, the scope waits `ceil(60 seconds / RPM)` before admitting
  another. The interval starts after response consumption, including streaming,
  or transport failure—not before DNS, TLS, or connection setup. Slow responses
  reduce throughput: each request costs its latency plus the RPM interval.

Legacy flat embedder RPM settings (including environment defaults) with
`burst: 1` use completion pacing to preserve the non-bursting path without an
arbitrary fixed safety margin. Legacy larger bursts use token-bucket pacing.
For explicit configuration, for example:

```json
{"rate_limit":{"requests_per_minute":6000,"pacing":"completion"}}
```

Completion delay and 429 cooldowns are shared and survive drained policy
replacement. Waiting remains deadline- and cancellation-aware. Completion
pacing prevents delayed successful attempts from bunching up, but cannot
guarantee zero 429s from account-wide limits, other processes, or requests whose
remote execution outlives a transport failure. Both modes retain retry handling.

## Scope and lifecycle

Within one Antfly process, callers share a quota when their effective provider,
endpoint, operation, model, credential source, and applicable project, region,
or location match. Two named configurations do not get separate capacity.
Conflicting policies for an active scope are rejected, including an unlimited
configuration competing with a limited one. A drained scope can adopt a new
policy while preserving outstanding debt and server cooldowns.

Credential identity describes the source, not the current access token, so
rotation does not reset capacity. Different credential sources remain distinct
even when they happen to resolve to the same token. Account-wide limits and
coordination across replicas require an external quota authority; these limits
do not discover a provider's quota or merge separate models' budgets.

Remote Antfly reranking uses `api_key` (including secret references), falling
back to `ANTFLY_INFERENCE_API_KEY`, for bearer authentication. The same resolved
credential supplies its quota identity; anonymous requests share an anonymous
scope. Embedded reranking does not resolve outbound credentials. Empty explicit
reranker keys are rejected; omit the key to use the provider's default source.

Admission happens for each HTTP attempt, including retries and redirects.
Single and batched asset generation use the same admission scope. Bedrock
credential refresh (STS/ECS/IMDS) is separate from model invocation and does
not consume the model's quota.
Concurrency permits are released after the response finishes, including writes
to a streamed-response consumer, or on transport failure. Waiting honors the
request's deadline and cancellation. A 429 response shares its `Retry-After`
cooldown (seconds or HTTP date) with the scope's other callers.

## Text token budgets

`tokens_per_minute` is a conservative text admission budget, not billing-token
accounting. Each attempt reserves its serialized UTF-8 request body size plus
the configured generation output-token cap (summed across all items in a
generation batch). Failed attempts are not refunded;
retries reserve again. Requests larger than the entire per-minute budget fail
immediately. Generation requires a positive output cap (the generator default
is used when omitted). Media requests are rejected when a token budget is set.

These policies apply to outbound providers. In-process Antfly inference uses
its own admission controls and rejects an enabled outbound `rate_limit`.
Changing embedding execution limits does not invalidate stored vectors or
their derived-coverage fingerprints.
