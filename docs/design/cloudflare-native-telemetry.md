# Native telemetry coverage for Cloudflare qualification

The October 29 isolated live probe passes **telemetry readiness**, including
private Durable Object RPCs, complete terminal lifetimes, independently counted
native deliveries, and a witnessed quiet window. The qualification gate itself
has not passed. No acceptance threshold, resource capacity, native module or
approved control implementation changed in this work.

## Evidence model

Use two provider-native channels outside the database's critical path:

1. OTLP traces and logs supply `faas.invocation_id`, deployment version, trace
   parentage, invocation lifetime, outcome and CPU/wall times. Associate RPC
   console records through explicit parent-span links. Fixture UUIDs and clocks
   never become provider invocation identities.
2. A private, unfiltered Tail Worker persists immutable terminal delivery
   objects to an observability-only R2 bucket. Its total independently checks
   the canonical root population, including fetch, RPC and background events.
   Delivery UUIDs identify storage objects, not invocations. Duplicate delivery,
   missing records or a changed deployment fails reconciliation.

Cloudflare documents [Tail callbacks after producer execution](https://developers.cloudflare.com/workers/observability/logs/tail-workers/)
and [terminal outcomes including asynchronous failures](https://developers.cloudflare.com/workers/runtime-apis/handlers/tail/).
The canonical identity and parentage fields follow the documented
[span attributes](https://developers.cloudflare.com/workers/observability/traces/spans-and-attributes/).
The live SDK emits `cpu_time_ms` and `wall_time_ms`; the decoder accepts these
and their namespaced aliases, rejecting conflicting values. Identical duplicate
OTLP attributes are accepted; conflicting duplicates, dropped attributes,
unknown root schemas and truncation remain failures. Known child
`span_not_ended` warnings do not excuse incomplete invocation roots.

Classic Tail callbacks alone lack canonical private-RPC IDs. They serve as an
independent terminal count, never as an identity substitute. The reconciler
compares count and timestamp populations without using a timestamp as a unique
ID. Adaptive GraphQL counts remain supplementary: observed sample intervals of
1.333 and 1.6 cannot establish an exact population even when estimates happen
to equal eight. Empty adaptive results still cannot establish zero.

## Quiet periods, settlement and scale

The private counter exposes an RPC that makes a credential-free request to the
source. Its expected 401/404 response performs no database operation. Distinct,
version-matched native records before and after a closed quiet window prove the
subscription is live. An empty counter without both witnesses fails. Correlate
witnesses using the existing full-entropy hashed physical-attempt token; never
persist credentials or raw request URLs. Boundary witnesses remain outside SLO
samples and unused cold epochs, and must be applied equally to both sides.

Pagination uses R2's native cursor and at most 32 objects or 4 MiB per page.
When size requires a smaller page, re-list with the smaller native limit rather
than inventing a continuation token. The driver caps each listing at 20,000
objects, 64 MiB, 2,000 pages and 120 seconds. No shared mutable counter row is
written in the producer's request path.

A finished listing of concurrently inserted random keys is not settlement.
Re-list each closed window twice: the first observation starts at least 60
seconds after the window, the second at least 30 seconds after the first ends.
Recompute hashes over selected native deliveries and trace/log records. Require
both independent population reconciliations and stable hashes. Late arrivals,
loss, replay, conflicting identities and incomplete boundaries fail closed.
These time bounds are observation prerequisites, not a claim that Cloudflare
promises delivery by a particular deadline. Actual OTLP delivery in this probe
took several minutes; an early incomplete snapshot was retained as failed.

## Live result

Probe `antfly-1278-telemetry-mvtvvmfy5f18` produced:

- Sixteen canonical fixture terminals: eight fetches and eight private RPCs,
  all with provider identities, versions, outcomes and CPU/wall metadata.
- All eight expected late fetch markers and RPC completion markers attributed
  inside their terminal lifetimes.
- Seventeen independent native terminal deliveries in the observation window:
  the sixteen fixture invocations plus one explicitly identified boundary
  witness. Every terminal matches the canonical population.
- A separate 35,267 ms quiet window with zero terminals and both native boundary
  witnesses present. No sampled-zero inference was used.
- Two stable, independently paginated snapshots, zero observed resource
  failures and `ready: true` in `native-counter-analysis-1793304946856.json`.

The closed populated-window SHA-256 is
`0cfbe7a74dbfac477bb73041da81d14203eba7aac0a86360ff45c8bd4385fc7d`.
Reports, failed early observations, sanitized exports, sealed bundles and
lifecycle records remain under the local qualification evidence closure.
Regression checks pass 378 Node tests and 35 actual-workerd tests. The local
workerd Tail implementation omits `scriptName`; local tests retain those events
as unattributed. Source attribution was verified in Cloudflare, without a local
fallback that fabricates a producer name.

## Remaining full-gate integration

The readiness verifier intentionally has no new full-gate acceptance path yet.
Complete these steps before spending the reserved trial epochs:

1. Provision the same observation configuration for all four producer roles on
   both sides, with exact source/version allowlists. Use observability storage
   separate from database storage. Seal and audit observer bindings and versions
   alongside the unchanged approved control and candidate bundles.
2. Collect contiguous closed windows over the complete execution: setup,
   workload, recovery, quiescence and final asynchronous drain. Reconcile every
   acknowledged physical attempt, every canonical root, private/background
   population and each role's quiet periods. Counter totals cannot replace
   attempt identities or lifetime accounting. If onset/unclosed-lifetime proof
   needs native streaming Tail, validate its availability and schema with a
   separate probe before relying on it.
3. Connect a separately typed native-Tail proof to gate evidence. Recompute
   its immutable artifact hashes and reconciliation during evaluation. Do not
   label it GraphQL, invent `sample_interval: 1`, trust a stored `ready` boolean,
   or relax the existing adaptive-counter rejection path. Demonstrate missing,
   replayed, delayed and truncated evidence fail during adoption.
4. Run the short four-tenant, 512-document paired rehearsal, checking exact
   bodies, receipts, queue/lag and resource outcomes. Then run the unchanged
   three one-hour pairs and 100 fresh candidate cold epochs. Retain every failed
   trial; pass only when every numeric and completeness condition holds.

This establishes a portable evidence contract, not portable Cloudflare
telemetry APIs. AWS/GCS runs need independently validated native identity,
terminal accounting and settlement adapters; object-store correctness tests
alone do not qualify provider resource or latency behavior.

The local closure is
`dist/qualification/native-telemetry-1793304946856/evidence.json`: 120 artifacts,
6,895,260 bytes, source `5ea13b44058f579abc82d8112bc7a0c7119afce5`, manifest
SHA-256 `13da9771aa9181948800d7db5e7039f8cc19746c3ea74c4c38c71506792d9b5e`.
All artifacts verify against that manifest. Independent cleanup checks confirm
all three probe Workers, its DO namespace, R2 bucket/test data, Logpush job and
both OTLP destinations are absent; the disposable local key was removed. The
existing account credential remains local. The six reserved trial stacks were
not deployed and their recovery epochs remain unused.
