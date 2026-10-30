# Grepstein inference release qualification

Qualify the release candidate and the deployed serving profile separately.
A CUDA backend, a ready pod, a downloaded model, or a passing synthetic
throughput test does not establish complete document extraction. Keep shared
inference as the execution plane; Grepstein does not need an instance-local pool.

## Record the exact candidate

Keep the source revision (and dirty patch digest if applicable), binary and
container digests, GPU model/compute capability, driver, model revisions,
effective resource/request ceilings, environment overrides, and fixture hashes
with every receipt. Historical DGX Spark measurements are not A100 results.
Run correctness before measuring throughput, and exclude competing GPU jobs
from timed runs. Report missing hardware and skipped tests as unqualified.

The Grepstein model selections are:

| Role | Pinned artifact | Required checks |
| --- | --- | --- |
| Reader | `lightonai/LightOnOCR-3-4B:safetensors@b71010f095dc3735de7ee6b9969bff8f8f50b39a` | Full-page completion, GPU recurrent dispatch, numerical parity, bounded memory |
| Alternate reader | `lightonai/LightOnOCR-3-0.8B:safetensors@36d8636925327c671bd8e479c517a004f7566bd9` | The same checks independently; changing model is a new extraction recipe |
| Embedder | `google/embeddinggemma-2:safetensors@914f7f89142e33e77833254d9c9b90c3cef7303b` | Document/query task preparation, dimension, normalization, identity fencing, retrieval ranking |
| Extractor | `fastino/gliner2.5-base-v1:safetensors@54785d51df8d86a0d8f2eb212fec15871aad2c4d` | Exact artifact/backend qualification, entity spans, relation endpoints and long-document coverage |

An installed GLiNER2 checkpoint does not substitute for GLiNER2.5. Request the
pinned model explicitly and verify discovery and the selected route agree.
Gemma 4 generation is a separate capability; enabling it is not a prerequisite
for the initial full-text, embedding, and GLiNER graph pipeline.

## Reader execution and completeness

1. Run the placement, recurrent numerical oracle, ownership, and admission
   tests on the candidate. Test both checkpoint geometries. Default CUDA
   recurrence must remain device-resident; explicitly disabling the core must
   resolve to `host/operator_disabled` at model load. A resident device history
   must fail closed if device execution fails, rather than migrate to an empty
   host history. Check placement logs and recurrent model metrics.
2. Compare the default serving profile with the explicit CPU recurrence
   baseline on identical inputs. Use `benchmark_concurrency.py --backend cuda
   --require-recurrent-placement`; omit `--cuda-recurrent-core` for the default
   and use `--no-cuda-recurrent-core` for the baseline. Keep resource ceilings
   and fixture hashes in the reports. The benchmark rejects missing or mixed
   placement diagnostics when this assertion is requested.
3. Include representative scanned DOJ pages: dense text, rotated scans,
   tables, low contrast, blank pages, and mixed native/scanned PDFs. Preserve
   page order and source/version identity. Compare full text including the
   last visible lines; a short nonempty response is not a completeness proof.
4. Force an output cap too small for a known fixture. Plain `/read` must reject
   incomplete output with `OUTPUT_TRUNCATED`; structured document output must
   preserve `truncated`/`unresolved` status. Materialization must not upgrade
   either to a complete searchable transcript. Exercise the configured accurate
   reader/refinement policy and verify exhausted budgets remain unresolved.

The current 512-token Grepstein example is a qualification limit, not proof
that every corpus page fits. Do not retry an unchanged capped request forever.
Measure the failing pages, then qualify a larger admitted output allowance or
a bounded document refinement policy. A higher serving ceiling and the reader
request must agree; increasing only one does not recover the missing tail.
Also qualify the work-unit capacity: a single image with a 2048-token output
allowance currently needs at least 12 generation admission units, exceeding
the existing eight-unit serving profile. Raising only the token ceiling would
still reject it. Keep request concurrency bounded while qualifying that change.
Retry affected source versions after the policy change and require complete
artifact coverage before semantic or graph enrichment. Avoid automatic image
tiling without a tested ordering/deduplication policy.

## Embeddings and extraction

EmbeddingGemma 2's CUDA/BF16 execution recipe has a different `model_identity`
from CPU/F32, and optimized arithmetic profiles also have distinct identities.
Discover the serving identity, pin it in the index configuration, and use the
same compatible recipe for document and query vectors. A request with an old
identity must fail with a conflict, not silently produce vectors from a changed
recipe. Verify index recovery and query caches preserve the pin. Rebuild into a
new index when the recipe changes; do not mix the two vector spaces. Run the
existing `scripts/embeddinggemma2/qualify.py` against the matching pinned oracle,
and check a small labeled retrieval set using the real task prefixes.

For GLiNER2.5, retain the artifact/backend/feature/length checks in
`models/gliner_boundary_qualification.zig`. Check entity text and offsets,
typed relation endpoints, chunk boundaries, and repeated entities across chunks
against the independent fixtures. Reject requests outside supported contracts;
do not remove an artifact or numerical qualification gate to improve routing.
Graph publication must wait for complete current-version extraction, and a
retry must not duplicate graph edges or leave stale-version edges searchable.

## Cold starts, pressure, and recovery

Through the actual authenticated hosted proxy, exercise each model from a
naturally idle pool: execution discovery, advertised activation, model download,
loading, successful work, and a second warm request. Verify the returned
capability lease and model identity. Record activation and download time apart
from execution. Do not scale down a busy shared production pool for this test.

Test model-loading responses, stale capability leases, overload, deadline
expiry, client cancellation, and a retry after recovery. Check exact error
codes/retry guidance. A temporary loading failure must not become a permanent
document failure, and retries must retain source/version identity. Existing
single-flight activation tests cover the protocol; a release receipt must also
include the actual deployed proxy/operator path.

Under the intended per-role memory and concurrency ceilings, exercise long
prompts and outputs, mixed-size requests, model switching, and cancellation
while work is queued or GPU buffers are retained. Include host, device,
combined/unified, KV, scratch, persistent recurrent state, download staging,
and ephemeral storage. Verify refusal before unsafe allocation, release after
success and failure, sustained readiness, no eviction/restarts, and completion
of a later valid request. Keep request cardinality separate from native
microbatch width. DGX Spark's 128-slot preset and large unified-memory budgets
must not become the shared A100/L4 serving defaults.

## Publication gate

Keep original preparation, full-text extraction, embedding enrichment, and
graph publication as separate resumable stages. Run Grepstein's `qualify`
command against the sealed publication: index readiness alone does not prove
that every source page was extracted. Keep semantic/graph publication disabled
until the required source versions have complete artifacts and the corresponding
model checks above pass. Public hosting additionally needs the source
suppression and exposed-API bounds described in the Grepstein design.
