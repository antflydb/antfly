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
   host history. Full-prefix forwards without a decode cache must acquire
   temporary device state, release its admission on completion or failure,
   and match a fresh cached forward. Check placement logs and recurrent
   model metrics.
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

A 512-token diagnostic, matching the deployed allowance, rejected a cached dense
DOJ page on Glowworm. The Grepstein reader example now requests 2048 tokens; both checkpoint diagnostics
completed that page and retained the checked tail text. This is a bounded
sample, not proof that every corpus page fits. Do not retry an unchanged capped request forever.
Measure the failing pages, then qualify a larger admitted output allowance or
a bounded document refinement policy. A higher serving ceiling and the reader
request must agree; increasing only one does not recover the missing tail.
Also qualify the work-unit capacity: a single image with a 2048-token output
allowance currently needs at least 12 generation admission units, exceeding
the existing eight-unit serving profile. Raising only the token ceiling would
still reject it. Keep request concurrency bounded while qualifying that change.
The proposed reader-policy adjustment is below; retain the current memory,
pixel, and input-token limits and qualify it on the intended hardware before
applying it. Sixteen work units leave headroom above that twelve-unit minimum;
the independent concurrency ceiling still admits one request.

```json
{
  "resources": {
    "inference": {
      "admission": {"max_concurrent_requests": 1, "max_work_units": 16},
      "request_limits": {"max_output_tokens_per_item": 2048}
    }
  }
}
```

This is a partial configuration to merge into the existing policy, not a
replacement for its other limits. Use the native pool probe's
`--reader-max-output-tokens 2048` switch to check the intended ceiling and its
exact rejection boundary after deployment to a qualification environment.
Retry affected source versions after the policy change and require complete
artifact coverage before semantic or graph enrichment. Avoid automatic image
tiling without a tested ordering/deduplication policy.

## Large-page vLLM comparison

The reader throughput target is parity with vLLM, preferably faster, while
preserving complete extraction. A faster short response does not qualify.
Compare both LightOn checkpoints independently, using the original checkpoint,
identical image hashes, image-only prompts, greedy decoding, and the same output
allowance. Include dense full pages and mixed page lengths at concurrency one
and the proposed serving concurrency. Compare pages per second and latency
distributions; token accounting can differ by whether the terminator is counted.

Use `benchmark_vllm_offline.py` to produce a private reference report and
`scripts/qualify_reader_page_latency.py --reference-report REPORT` to compare
native HTTP responses without copying document text into public receipts. The
report records exact, whitespace-insensitive, and alphanumeric comparisons
separately. Only exact comparison is an automatic parity gate
(`--require-reference-parity`); an alphanumeric match alone cannot qualify
punctuation, layout, or correctness. Review every differing full output against
the source image. Record formatting-only differences explicitly rather than
stripping separators or punctuation from production responses to match an oracle.

Verify the GPU has no competing compute processes before each run and that the
server owns the measured GPU process. Drain and unload each model before stopping
its server. Keep uninstrumented timing runs separate from profiler runs and
report the HTTP versus in-process boundary. Disable reference prefix and processor
caches and use fresh media identifiers. Measure cached-weight startup, new-node
artifact download, and first-request compilation separately; a warm local model
load does not establish Cloud cold-start latency.

Qualify the final runtime defaults, not only a diagnostic environment profile.
Recheck mixed-page batching, model eviction/reload, complete tails, memory limits,
and overload behavior with those defaults. DGX Spark results establish a useful
kernel comparison but do not qualify the A100 serving deployment or its container
libraries. Require the paired released runtime/operator and an A100 canary before
changing the live shared reader pool.

### Current large-page investigation

The DGX Spark investigation has not yet met the concurrent throughput target.
With the original LightOn 4B checkpoint, the same two full-page images, greedy
decoding and a 2,048-token output allowance, the ordinary CUDA defaults before
enabling owned greedy continuations
take about 53 seconds for the two pages serially, versus about 57 seconds for
vLLM. A four-request cohort containing two copies of each page takes about
47 seconds after extending the batched attention packet beyond its former
1,024-token context bound. The preceding defaults took about 51 seconds.
The vLLM four-request diagnostic took about 33 seconds. These are HTTP native
versus in-process vLLM measurements on an isolated GB10, not Cloud A100 results.

Native outputs are byte-identical to the corresponding vLLM batched outputs.
The dense page's vLLM serial output differs from its batched output in line
breaks and a trailing Markdown separator; the strict serial-reference parity
assertion therefore fails for that batched reference. Reviewing those edits
does not turn the failed assertion into an automatic correctness pass. Keep
both reference outputs and the source-image review as qualification evidence.

The long-context attention change passes 64 independent GPU numerical and
graph-replay oracle cases, including sparse/permuted physical page tables and
contexts through 8,192 tokens. Full-page throughput remains a separate gate.
Observed decoder cohorts are nearly full for the available live pages; the
remaining profile is dominated by matrix operations. Increasing public
concurrency or temporary-memory ceilings alone is not a demonstrated remedy.
Experimental precision and fusion profiles remain separate from defaults
until their complete-page correctness and throughput are qualified.
With derived immutable fusion weights charged to the device weight budget
rather than temporary scratch, the experimental fused-projection profile
completes the four-page cohort in 37.4–37.9 seconds over two rounds. Output
hashes and complete-tail checks are unchanged. A projection-output BF16 profile
alone takes 39.6–39.9 seconds. Neither measurement establishes default-policy,
0.8B, A100, or sustained-service qualification.

Owning the greedy continuation independently of multi-token bursts reduces the
fused candidate to 35.9–36.0 seconds. Enabling its optional decoder graph gives
35.5 seconds, with four captures, 766 replays, and no graph admission denials.
Both rounds preserve all four output hashes. These remain diagnostic candidates,
not a declaration that the throughput target has been met. Bounded matrix tuning
and a simple batched matrix-vector prototype did not improve on the selected
library path and were not promoted.

Canonical reader profiles now authorize the executor-owned continuation without
requiring a diagnostic environment switch. Its existing pure-greedy, callback,
progress, cancellation and scheduler-ownership checks remain mandatory. A live
functional check timed out one request with `504 INFERENCE_TIMEOUT` while its
peer completed with the expected full output hash; all 38 readiness probes
succeeded. The same check passed four-page 0.8B batching and 4B eviction/reload.
Compilation ran concurrently with this functional check, so its timings are
deliberately excluded from performance qualification.

The long-context packet artifact requires a CUDA 13 driver API. Older drivers
keep ordinary CUDA attention. Decoder graphs remain opt-in, and graph capture
declines projection-rounding modes whose arithmetic is not capture-compatible;
their eager CUDA execution retains the requested precision. Cached immutable
fusion copies consume the device weight budget, independently of scratch, and
still require device/combined admission and release on model retirement.

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
