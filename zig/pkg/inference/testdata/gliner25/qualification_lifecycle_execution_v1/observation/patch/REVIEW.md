# Preserve idle TTL during metrics and listing observation

Private draft only: no live files changed, no builds/model/process tests, no commits or pushes. Apply only after all three base pins in `apply-pins.json` match. This supersedes the abandoned `/private/tmp/gliner25-cache-post-scrape-v1` proposal, which must remain unapplied.

## Concrete production failure

`Node.metricsHandler` takes `LoadedModelSnapshot`. Its old deinit released ordinary `ModelHandle` values, renewing each model's `last_used_ns`. A metrics scrape interval shorter than keep-alive could therefore keep every cached model resident indefinitely without inference. `modelsHandler` separately constructed the same ordinary handles by hand and had the same side effect.

The final-focused Metal v2 actual-model test exposed this after the first eligible eviction succeeded in 16.109626 seconds of whole maintenance, and reload plus same-cache inference retry succeeded. The final scrape refreshed usage; an expiry calculated before the scrape no longer reached the actual TTL, so final maintenance returned in 2 microseconds with one model still cached. That aggregate remains 67 selected / 65 passed / one expected child-fixture skip / one TTL failure / zero leaks. No isolated backend-destructor timing is inferred.

## Narrow ownership change

- Public `ModelHandle.release()` keeps its existing inference-use semantics and API.
- A private `releaseWithUsage` helper shares decrement, lock ordering, nulling, and deferred-retired destruction. Only `LoadedModelSnapshot.deinit()` calls it with observation semantics; no public flag, request option, environment override or mutable policy is added.
- Snapshot pins still block eviction while active. The final observer of a retired model still performs the same protected physical cleanup before admission is released.
- The model listing replaces its manual handle array with the existing `acquireLoadedModelSnapshot` owner; its single consumer iterates `handles`. Metrics already uses that owner. The listing's filesystem/manifest work continues outside the manager lock, and acquisition still allocates before publishing any handle increments.
- The snapshot's handle slice is documented as borrowed pins, released only through the snapshot owner. Ordinary acquire-from-directory/cache, retire and quarantine callers keep their existing behavior.

## Caller audit

Current production uses of `acquireLoadedModelSnapshot` are metrics and, after this patch, model listing. Other direct `acquireLoadedModel` call sites are actual service tests intentionally retaining a usable model. Managed inference acquisitions and the loader's internal handles still use ordinary release. The existing Metal socket test's snapshot is observation; it retains the same lifetime but no longer refreshes TTL. No unreviewed client-side release knob is introduced.

## Focused regressions

The `loaded model snapshot` filter now selects three model-free tests:

1. Repeated observation pins prevent eviction while active and preserve the exact timestamp after release. An ordinary inference handle finishing while observation remains active refreshes usage; observation release preserves that new value. Production LRU selection rejects one nanosecond before the exact renewed expiry and admits eviction at the boundary.
2. Failure allocating the snapshot vector leaves reference counts, usage and cache state untouched; acquisition can retry immediately and releases without renewing TTL. Allocation occurs before any reference increment, so this covers the single fallible acquisition point.
3. A retired model stays alive under its last observation pin, with its real 64-byte admission lease held. Final snapshot destruction runs the existing fake process-required session under its actual manager teardown ticket, then releases admission and the dormant watchdog entry. No Metal backend is constructed or model loaded.

The actual cache lifecycle test keeps the original pre-scrape timestamp. After the real metrics request and transport idle it asserts identical owner, admission amounts and exact last-use time, then evicts at that original expiry. Production 30-second close protection, phase timing, source pins, physical guards and the unchanged five-second no-op checks remain intact.

Suggested root filters: `loaded model snapshot`; `failed loaded model retires`; `model listing`; `gliner boundary cache pinned small Metal handle retention eviction and reload`. Reuse the existing actual small/Metal environment only for the last test. Formatting and patch applicability are local preflight, not runtime evidence.
