# GLiNER2.5 CUDA production review — 2026-09-16

**Decision: the demonstrated sustained-training parity blocker is fixed on the
existing small-model fixture; the broad production release remains unqualified.**
V36 passes independent 100-update full-model comparisons at epsilon `1e-6` and
the unchanged default `1e-8`. All 334 weights and both optimizer moments are
numerically identical (zero absolute error) at every sampled checkpoint and
update 100. There are no component-
loss or held-out confidence parity failures. Python computes its own gradients,
norm, clipping multiplier and optimizer updates; no intervention is used.

Default-epsilon first-update heads/full comparisons also have zero numerical error for
all 136/334 weights and both moments. The integration uses the independently
verified CUDA norm primitive, authoritative parameter registration order, and
strict FP32 clipping arithmetic within the existing shared transaction. It
preserves absent-gradient semantics, ownership/rollback, admission and durable
checkpoint identity. CUDA full/heads select this profile; PEFT retains its prior
profile pending adapter-order qualification. CPU/Metal retain legacy arithmetic.

The expanded suite passes **103 tests, with three Metal-only skips**. Both
integrated clipping memory checks pass with zero errors. Published-small heads/
full durable resume and portable reload remain exact. Exact sources, binaries,
failed build-space recovery, successful gates and raw comparisons are recorded
in [the arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md).

V37 extends this to warmed heads/full comparisons at microbatches two and eight:
all four 100-update campaigns pass, with exact weights and moments at all 48
sampled states. Matched eager-Python speedups are approximately 3.26x for
heads/B2, 1.68x for heads/B8, and 1.62x for full/B2. Full/B8 remains 3.1% slower
(95% paired latency interval: 0.8% to 4.0% slower). See the arithmetic follow-up
for throughput, source identities and the profiler findings.

These runs use small/accumulation-two/FP32 with stochastic features disabled
and short repeated synthetic examples. Both implementations regress in relation
F1 on the original two-example held-out fixture: 1 to 0 at epsilon `1e-6`, and
1 to 0.5 at `1e-8` in v36. Matching state is not proof of general learning
quality. **Broad performance qualification remains pending:** the wider release
matrix is open. The compiled Python candidate fails the
100-update equivalence gate (first loss failure at update 33); its faster
timings are diagnostic. The v38 cache-default result is recorded below.

V38 closes the eager full/B8 performance gap: 35.51 versus 33.69 examples/s,
with paired native/Python latency 0.9427 (95% interval 0.9237–0.9703). Its
100-update campaign passes with raw-bit equality for all 334 weights and both
moments at all 12 sampled states. The bounded cache change passes 107 regressions
with three Metal-only skips, including physical cache admission/eviction. Heads/
full durable checkpoint and portable-model hashes are unchanged from v36.
The refreshed v38 matrix also passes heads/B2, heads/B8 and full/B2: all four
100-update rows have raw-bit equality at all 48 sampled states and favor native
in paired throughput (approximately 1.06x–3.21x speedup). Three integrated
memory checks pass with zero errors. These results remain limited to the existing
small-model fixture; broader release qualification is still open.

V39 replaces finite/all-zero validation norms with bounded batched flag checks;
clipping and optimizer arithmetic are unchanged. All 111 selected regressions
pass (three additional Metal-only skips), as do integrated memcheck, initcheck
and racecheck. Published-small heads/full durable model and state hashes are
unchanged. The first sustained row, full/B8, passes 100 updates with raw-bit
identical weights and moments at all 12 sampled states. It measures 39.20 versus
33.31 examples/s, with paired latency 0.8536 (95% interval 0.8398–0.8764).
All four v39 small-model rows now pass 100 updates, with raw-bit identical
weights and moments at all 48 sampled states. Every paired 95% latency interval
favors native. Broader model, real-data, adapter and platform qualification
remains open; see the arithmetic follow-up for the complete throughput table.

V39 broader-model first-update preflights also pass the unchanged parity gates:
base and multilingual, heads-only, batch two, accumulation two and default
epsilon `1e-8`. Base has raw-bit identical weights and both Adam moments in all
three sampled states. Multilingual weights and second moments are also raw-bit
identical; 12 first-moment elements in `relation_scorer.mlp.0.weight` differ by
at most `1.401298464324817e-45` after the first update (one FP32 subnormal step).
Thus multilingual state passes numerical parity but is not fully bit-exact.
Component-loss and held-out confidence checks pass. These close the earlier
first-update discrepancies under the existing tolerances on this fixture;
sustained larger-model training and throughput remain unqualified.

The v40 wider heads-training matrix exposes concrete remaining blockers. Base
and multilingual at B2 pass 100 updates, with bit-exact sampled weights/second
moments and tiny first-moment differences (at most 4.71e-38 for base and
7.35e-40 for multilingual). At B8, base first fails a
sampled state gate at total update 56, then loss parity at total update 59, and
ends with maximum weight error 0.00549456 and a held-out relation-span mismatch.
Multilingual/B8 fails `ResourceLimitExceeded` on its first training step. Those
two rows are not parity/performance qualified. Their raw results are preserved;
the next work is first-gradient tracing and precise resource-admission diagnosis.

V40 controls now identify those failures. Matching native cuBLAS to Python's
12.8 runtime closes base/B8 drift; raising only the multilingual logical encoder
estimate allowance to 8 GiB admits its existing shape under the unchanged
physical CUDA cap. V41 integrates that selection explicitly and binds the
vendor version into the trainer checkpoint identity. The integrated heads/B8
row passes 100 updates at 90.16 versus 52.75 examples/s, and full/B2 passes
100 updates across all 334 trainable tensors at 18.77 versus 9.03 examples/s.
Both report cuBLAS 12.8, with no parity failures; focused CUDA memcheck has
zero errors. Integrated full/B8 now passes 100 updates across all 334 trainable
tensors at 40.01 versus 33.84 examples/s, with a paired latency ratio of 0.850
and no parity failures. The earlier stopped run was a command-session
interruption; the retry completed under unchanged memory guards. Older
runtime-unbound CUDA checkpoints intentionally fail the new fingerprint
validation.

The integrated multilingual heads/B8 row now passes 100 updates across all 136
trainable head tensors at 74.70 versus 52.22 examples/s, using the documented
8 GiB logical encoder-forward allowance. The unchanged physical CUDA cap and
all numerical parity gates remain active.

Successful kernel tests and inference throughput do not establish release
readiness for the complete inference and finetuning feature set.

## Historical investigation

The entries below describe earlier revisions and their results. The current
v38 results above supersede their small-model sustained-parity and eager
full/B8 performance failures.

The subsequent [training arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md)
records LayerNorm and matrix-product parity fixes. Its isolated bitwise checks
and first-update results do not yet close the sustained-training blocker.

The v17 follow-up now has exact forward/seeded-gradient transpose-storage checks
and a clean device memcheck. Both initial full-model microbatches pass state
parity; all traced marginal/proposal outputs are exact and pair-logit error is
1.91e-6. This does not supersede v16's failed sustained training gate: no v17
100-update campaign has run. A bitwise-exact CUDA span-length-feature prototype
addresses another demonstrated rounding mismatch but remains unintegrated.

The later v18 integration passes all 28 selected CUDA checks, clean memory
checking and default-epsilon heads/full first-update state comparisons. All
compared initial full-model forward logits now match Python exactly. Sustained
qualification still fails at total update 35; 256 final weight tensors fail and
both arms regress on the tiny held-out relation fixture. The measured 1.78x
diagnostic speed ratio is not a qualified performance claim. Remaining
loss-gradient seed arithmetic differs even with identical forward logits; see
the follow-up and archived v18 evidence. Keep the release gate closed.

The v19 follow-up fixes a confirmed Tensor.gather backward accumulation-order
mismatch. With identical boundary cotangents, all 24 scorer parameter gradients
now match exactly. All 31 selected CUDA regressions pass, memory checking is
clean, and heads/full first-update checks pass at default epsilon 1e-8.
The normal 100-update run still first fails at update 35;
248 final weight tensors fail, and maximum drift is not improved. The release
remains unqualified. See the follow-up for the fixture input-bit correction,
component checks and source-identified evidence.

The v20 loss-arithmetic follow-up makes the normal initial start/end/inside
cotangents exact, passes 35 selected CUDA checks and clean memory checking,
and retains default-epsilon heads/full first-update parity. The 100-update run
still fails first at update 35, with 257 final weight tensors failing. The
remaining listwise, consistency, query/count and encoder-backward differences
are not closed by the binary-loss kernel. Production qualification stays closed.

## Confirmed defect: P2, integer snapshot metadata admission (fixed)

`src/ops/cuda/gliner25.zig` copied retained integer routing metadata when taking
a tensor snapshot, without checking the snapshot request's
`max_index_metadata_bytes`. A tensor uploaded under a larger allowance could
therefore allocate another host copy under a smaller allowance. Tensor byte
limits and the enclosing allocator still applied; this was not an unbounded
allocation or a demonstrated device out-of-bounds access.

The snapshot now rejects excess metadata before allocating or copying device
storage. Regression coverage checks rejection of an eight-byte proof under a
seven-byte allowance without touching CUDA, and a successful physical snapshot
at the exact twelve-byte limit on CUDA.

## Release blockers and remaining scope

The current V41 status and review checklist are in
[PR_READINESS.md](PR_READINESS.md). The numbered items below preserve the
historical release analysis; where they describe V38/V39/V40 as pending, use
the V41 evidence in that checklist as the current state.

1. **Representative training qualification is incomplete.** V38 passes all
   four small/FP32/heads/full/B2/B8 100-update campaigns, with raw-bit equality
   for all weights and moments at all 48 sampled states and faster paired
   throughput than eager Python CUDA. This does not cover sustained PEFT,
   base/multi, other sequence buckets, production stochastic settings, or
   representative learning quality. V39's boolean-validation optimization is
   integrated and passes 111 regressions (three Metal-only skips), with
   unchanged heads/full durable model and state hashes. Integrated memcheck,
   initcheck and racecheck also pass. Its trainer build and sustained paired
   comparisons are still pending; standalone kernel timings are not qualified
   trainer performance evidence.
2. **The production qualification registry is deliberately empty.** The CUDA
   backend is recognized, but normal serving still requires an exact qualified
   model/request contract. Keep this gate. A reviewed release entry needs
   evidence for its actual checkpoint, precision, task combinations and size
   limits; synthetic benchmark success does not authorize that entry.
3. **Evidence does not cover the entire proposed release matrix.** Longer
   training comparisons cover small, heads/full, FP32, disabled dropout and
   augmentation, and short repeated examples. The follow-up adds first-update
   head-training checks for base/multi. These do not establish learning
   equivalence for base/multi, production randomness, long documents, or reduced
   precision. Hardware execution in this review is on one NVIDIA L4 (SM89).
   Narrow the supported release contract or validate the additional dimensions.

An earlier optimization pass substantially reduced the full-model gap. At
the unchanged default epsilon, that revision was 1.25x faster at microbatch two;
Python retains a 2.46% paired latency advantage at eight. Zig throughput improved
approximately 2.33x and 4.29x respectively versus the original campaign.
Default-epsilon weight parity still fails, so these are diagnostic timings.
See [TRAINING_OPTIMIZATION.md](TRAINING_OPTIMIZATION.md) for the changes,
measurements, separate epsilon experiment and remaining acceptance limits.

## Review and validation

The review inspected CUDA tensor ownership and retained views, shape/index
validation, kernel launch geometry, attention forward/backward scheduling,
optimizer transaction integration, backend selection, serving qualification,
and benchmark failure handling. Existing shared CPU/Metal fixtures remain the
semantic authority; no frozen oracle or tolerance was changed.

- Required-GPU ReleaseFast gate before the fix: **40 passed, zero skipped**.
  This count includes module-discovery tests; it is not forty independent
  end-to-end workloads. The gate requires both a CUDA build and a usable GPU.
- Python CUDA benchmark contract tests: **22 passed**.
- NVIDIA Compute Sanitizer inference memcheck: **zero errors**, published small,
  all ten frozen pipeline cases, batch two.
- NVIDIA Compute Sanitizer full-training memcheck: **zero errors**, published
  small, batch two, four microbatches and two optimizer updates.
- NVIDIA allocation trace: heads, batch two, four microbatches/two updates;
  peak traced device allocations **617,180,456 bytes**, below the reported
  **960,507,432-byte** resident upper bound. All traced device allocations were
  freed at shutdown. The suspected cache-related bound violation was **not
  reproduced**. This single workload does not prove memory admission for every
  shape, cache configuration or concurrent session.
- Post-fix required-GPU ReleaseFast regression gate: **40 passed, zero skipped**,
  including snapshot metadata rejection and exact-limit acceptance. Zig
  formatting and `git diff --check` also pass.

The sanitizer runs use the existing benchmark executables; the snapshot
admission fix is checked separately by the rebuilt hardware regression gate.
Sanitizer timings are excluded from performance comparisons. Memcheck is not
a racecheck or proof of numerical equivalence.

Local review logs are `/tmp/gliner25-production-review-gate.log`,
`/tmp/gliner25-production-review-gate-fixed.log`,
`/tmp/gliner25-production-review-python.log`,
`/tmp/gliner25-review-inference-memcheck-v2.log`,
`/tmp/gliner25-review-training-memcheck.log`, and
`/tmp/gliner25-review-memory.nsys-rep` (exported SQLite alongside it).
These temporary artifacts should be retained in release evidence storage before
the local workspace is cleaned.

## Optimization follow-up validation

The final optimization build passes 46 required-hardware CUDA tests with zero
skips and 24 Python contract tests. The rebuilt inference worker passes all 30
frozen cases across small/base/multi. Compute Sanitizer reports zero errors for
four full-model microbatches and two updates at batch two. The final training
worker, source changes, checkpoint fingerprint compatibility and both epsilon
campaigns are documented in [TRAINING_OPTIMIZATION.md](TRAINING_OPTIMIZATION.md).

## Final optimization review

This pass reviewed the final uncommitted implementation, including the shared
code affected by the CUDA optimizations. No additional implementation fix was
identified. The release blockers above still apply; passing local regression
tests does not close them.

### Architecture and failure paths

- **Backend boundaries:** CUDA uses the existing model graphs, routing
  descriptors, training objectives, optimizer transactions, snapshots,
  checkpoint machinery and serving admission. The new adapter owns CUDA
  storage and dispatch rather than introducing a second trainer. The shared
  attention schedule preserves the Metal launch sequence and parameter ABI.
- **Ownership and validation:** inspected allocation rollback, borrowed weight
  handles, retained views, snapshot copies, integer metadata limits, checked
  shape arithmetic and launch geometry. Fixed-size CUDA attention arrays are
  bounded by the shared layout checks. No new invalid lifetime or demonstrated
  out-of-bounds access was found in these paths.
- **Optimizer changes:** stable grouping preserves duplicate-index order;
  contiguous reduction preserves the existing serial FP32 accumulation order.
  Batched norm readback and inactive-gradient checks retain validation before
  optimizer mutation. The regression suite includes a 257-slot transaction
  crossing the 256-slot batching boundary with invalid slots at 0, 255 and 256.
- **Caching and memory:** cuBLASLt plans and the training allocation pool remain
  bounded. Final-worker allocation traces for heads batch two and full batch
  eight are below their reported resident upper bounds and free all traced
  dynamic device allocations. At that review point, cached physical allocations
  were not generally charged to each job's declared memory budget; the follow-up
  below adds enforcement for managed CUDA training allocations. These measurements
  do not establish admission correctness across arbitrary shape changes,
  cache overrides or concurrent owners. Qualify the intended concurrency and
  memory configuration before making a production memory guarantee.
- **Compatibility and admission:** the CUDA numerical changes have an explicit
  checkpoint fingerprint revision; earlier CUDA checkpoints are rejected
  rather than silently resumed with changed semantics. Normal serving still
  requires a production qualification entry. Benchmark options do not bypass
  this gate.
- **Benchmark integrity:** both arms receive the same explicit optimizer
  configuration. Default epsilon remains `1e-8`; failed weight checks remain
  failures. Frozen fixtures and tolerances are unchanged. Diagnostic observer
  instrumentation is opt-in and absent from ordinary measured training steps.

The architecture is coherent and reuses the established CPU/Metal contracts.
No architectural rewrite is warranted by this review. The main remaining work
is numerical and release qualification, plus validation of memory admission
under the actual supported deployment configuration.

### Fresh checks on the final source

- Required-GPU ReleaseFast gate: **46 selected, 46 passed, zero skipped**.
  As before, this includes module-discovery tests; it is not 46 published-model
  integration cases.
- Python inference/training benchmark contract suites: **24 passed**.
- Compute Sanitizer **racecheck: zero hazards** and **synccheck: zero errors**.
  Each ran the attention forward/five-VJP test and the strict
  gather/grouped-scatter/snapshot/scaled-norm test. Attention coverage includes
  head widths 64/128/256, sequence lengths 1/17/65, ragged and fully masked
  inputs, and dropout zero/nonzero. These are focused kernel checks, not a
  sanitizer run of every model or serving path.
- `git diff --check`: passed.

Fresh logs are `/tmp/gliner25-final-review-gate.log`,
`/tmp/gliner25-final-review-python.log`,
`/tmp/gliner25-final-review-racecheck.log`, and
`/tmp/gliner25-final-review-synccheck.log`; companion `*-tests.log` files record
the sanitizer test selection. All three GPU commands exited successfully.
The earlier final-worker full-model memcheck and 30 frozen inference cases
remain relevant because this review made no implementation changes.

This pass did not rerun the complete server suite, published-model durable
resume jobs, or physical Metal tests. Changes to shared CPU/Metal code still
need their platform release checks. Preserve the benchmark and sanitizer
artifacts outside `/tmp` before relying on them as durable release evidence.

### Requirements to close the release review

1. Resolve default-configuration weight parity, or explicitly approve and
   qualify a different optimizer configuration using longer runs and held-out
   extraction quality. The short `1e-6` experiment alone is insufficient.
2. Define a bounded supported matrix and validate its models, shapes, task
   combinations, precision, randomness, checkpoint resume and deployment
   concurrency. Test additional GPU architectures if they are supported.
3. Complete shared-platform regression checks and archive reproducible evidence
   tied to the final binaries and sources.
4. Only then add reviewed production qualification entries for that exact
   inference contract. Keep unsupported configurations rejected.

Full-model batch-eight throughput is approximately tied with Python in the
matched `1e-6` campaign. This limits performance claims; it is not itself a
correctness defect. No commit or push was performed during this review.

## Release blocker follow-up

### Managed CUDA memory enforcement implemented

Each training owner now applies a physical `DeviceBuffer` ceiling of
`memory.backend_bytes - memory.backend_metadata_bytes` during initialization,
before warmup or weight upload. Allocation reservations are serialized and
checked without overflowing. Failed allocations return their reservation;
cached and deferred-free buffers keep theirs until a successful CUDA free.
The original allocation size is retained independently of logical view lengths.
Failed frees do not create budget credit.

When a new shape cannot fit, the ordinary cache releases idle allocations and
retries. Active tensors and pinned graph buffers cannot be evicted. Cache
environment settings cannot widen this ceiling. Existing logical admission,
host allocator budgets and shared job reservations remain in place.

This closes the unmanaged retention gap for application `DeviceBuffer`
allocations in CUDA training. Module storage, library/driver-private memory
and other GPU clients still require deployment headroom. This is not a claim
that all driver memory is charged to this counter, or that untested workload
and hardware configurations are qualified.

### Final-source lifecycle and failure-path checks

- **43 selected CPU/CUDA tests passed, zero skipped**, covering the CUDA gate's
  substantive operator/trainer tests, shared loss failure paths and closed
  serving qualification. Runtime filters exclude module-discovery tests from
  this count. The earlier required-GPU build passed 48 including discovery.
- **Published small heads and full-model durable resume: both passed.** Each
  compares four microbatches/two updates against uninterrupted training,
  requiring exact final optimizer state and portable-model hashes. Checkpoint
  scratch was removed after the tests.
- **Physical budget memcheck: zero errors**, including retained cache charges,
  eviction for a larger shape, rejection with live tensors occupying the full
  budget, preservation of tensor contents after rejection, and overflow checks.
- **31 Python contract tests passed**, including snapshot validation before
  weight mutation, mode restoration after evaluator failure, disjoint splits,
  invalid annotation offsets, and explicit disabling of query subsampling.

Logs: `/tmp/gliner25-release-final-regressions.log`,
`/tmp/gliner25-release-published-checkpoints.log`,
`/tmp/gliner25-release-budget-memcheck.log`, and
`/tmp/gliner25-release-python-tests.log`.

### Longer numerical and quality evidence

The benchmark can now check up to 512 additional untimed updates with full
state comparisons every ten updates and at the end. It reuses the existing
annotated validation fixture and extraction normalization. Both learned weight
sets are evaluated in the pinned Python CUDA runtime, restoring Python weights
afterward and preserving optimizer state. This isolates learned-weight quality;
it does not replace native inference qualification.

It also accepts explicitly supplied, bounded training/validation files and
records their hashes and counts. Duplicate IDs and normalized exact text
overlap are rejected. The original adapter's unique-surface constraint remains;
ambiguous annotations fail rather than being reinterpreted. Query subsampling
is now explicitly disabled in both benchmark arms, because a ratio of one does
not admit every absent query on arbitrary data. Production defaults are unchanged.

The 100-update small/full/batch-two experiment at matched epsilon `1e-6`:

- Passed initial state and the first three periodic full-state checks, through
  update 33. Component-loss tolerance first fails at microbatch 73, entering
  update 37; later weight and moment checks fail as well.
- At update 100, **273 weight tensors fail** the unchanged criteria. The largest
  absolute weight difference is **0.00785755**. This is a substantial long-run
  mismatch and prevents treating the epsilon change as a parity fix.
- Managed device allocation reservations peaked at **2,810,595,376 bytes**,
  below the enforced **4,160,749,568-byte** ceiling.
- Final extraction decisions match on the two synthetic validation examples,
  but confidence parity fails. Both arms have entity/classification/record F1
  of 1.0 and relation F1 of 0.0, down from initial relation F1 of 1.0. Equal
  implementations can still both regress in quality.
- A second 100-update control without held-out evaluation produces an
  **identical final tensor comparison** and the same first loss failure.
  The evaluation instrumentation is not the source of this observed drift.

Reports are `/tmp/gliner25-release-full-100-eps1e6/` and
`/tmp/gliner25-release-full-100-control/`. These use training binary SHA-256
`f8bf5f9e262c331688c49d92b7fce4523452418cc57b85250eb43ec07c9f31e1`,
before the subsequent benchmark-only change disabling query subsampling.
No throughput claim is drawn from these qualification runs; some work
overlapped compilation and their measured sample count is deliberately small.

The production registry remains empty. Release still needs resolution of the
numerical mismatch, a representative dataset and held-out quality target,
validation of the chosen model/workload/precision/concurrency matrix, and the
remaining physical Metal and additional-GPU platform checks. These are explicit
open requirements; none is waived by a passing fixture or checkpoint test.

### Broader head-training checks

The final benchmark explicitly disables query subsampling in both arms and
uses the multilingual source owner's normal 384 MiB auxiliary allowance (the
small/base fixture allowance is 128 MiB). This corrects the initial multilingual
benchmark admission failure without widening production defaults.

At batch two, one update/two microbatches and matched epsilon `1e-6`:

| Model | Strict state parity | Largest weight difference | Peak managed device reservation |
| --- | --- | ---: | ---: |
| base | pass | 1.11759e-6 | 1,569,555,992 bytes |
| multi | fail: three weight tensors | 1.18427e-5 | 1,958,394,424 bytes |

Both match held-out extraction outputs within the existing confidence bound
after this update. Both remain unqualified for learning quality or longer
training. The small heads/default-epsilon regression still fails the same four
weight tensors, with maximum difference `3.57181e-5`.

Reports: `/tmp/gliner25-release-final-{small-default,base-heads,multi-heads}/`.
Final training worker SHA-256:
`eeb7df362fe2af095cfc6f357dfee2e377998d7a5357a7cbc3dcd640cbb8233a`.

### Subsequent loss-parity investigation

The [loss-parity follow-up](LOSS_PARITY_FOLLOWUP.md) documents a Python-only
one-ULP sensitivity control and corrections to listwise, inside-span and count
derivatives. The control produces comparable 100-update drift. The corrected
native training still fails strict default-epsilon and sustained weight
parity, so the release requirements above remain open.

### Final rebuild and retained evidence

The final training worker repeated the small/full/batch-two, 100-update run
with query subsampling disabled. Its final tensor comparison is identical to
both earlier 100-update reports, including the control without evaluation.
Neither the evaluator nor the query-sampling setting explains the observed
drift on this fixture. The rebuilt inference worker passed all **30 frozen
inference cases** across small, base and multilingual models.

The [evidence bundle](evidence/2026-09-15-release-followup/README.md) retains
raw reports and test logs, including failed comparisons. Its
[summary](evidence/2026-09-15-release-followup/summary.json) records the open
requirements, and its
[source manifest](evidence/2026-09-15-release-followup/source_manifest.json)
records implementation and final binary hashes. These qualification runs
do not establish a new throughput result or production learning quality.

Repository HEAD advanced externally to `e400a6175e32f9448617dcba82b9b80b92181ec2`
during validation. The assistant performed no commit or push and preserved
that external change. Evidence provenance uses the observed source and binary
hashes; the follow-up documentation and evidence remain uncommitted.


The v9 attention arithmetic follow-up passes 32 ML graph tests and eight
selected CUDA regressions, including published small heads/full exact resume.
The first attention block now matches the pinned Python trace, and default
`1e-8` heads training is down to one first-update weight failure. The complete
100-update full-model run still fails: first loss mismatch at total update 35,
243 final weight tensors outside tolerance, maximum error `0.00664854`, and
different relation predictions on the tiny held-out fixture. Release and
performance qualification remain closed. See the updated
[arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md) for evidence and the
remaining GELU/compiler-version and short-sequence backward arithmetic work.


The v10 CUDA 12.8 GELU compatibility module clears the strict default-epsilon
heads first-update gate. All traced encoder-layer inputs/outputs match Python;
ten selected CUDA regressions and seven artifact-script checks pass, and the
production GELU artifact has zero NVIDIA memcheck errors. The 100-update full
run still fails (first loss mismatch at update 35; 271 final weight failures;
maximum `0.00625285`). Release/performance qualification therefore remains
closed. The remaining forward mismatch starts in boundary-head attention,
where pinned Python uses fused efficient attention; backward contractions
also still copy matrix transposes. See the v10 section of the
[arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md).

### v11 dense-gradient follow-up

Dense CUDA backward contractions now retain both operand buffers. The integrated
regression matches pinned Python input/weight-gradient bits, all 12 selected
CUDA/admission checks pass, and NVIDIA memcheck reports zero errors. Strict
heads first-update parity remains passing. See
[the arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md#v11-integrated-results)
for exact scope and evidence, including the missing v11 published-full resume
rerun.

The full-model release gate remains closed: the 100-update campaign first fails
loss parity at update 35, and 252 final weight tensors fail tolerance (maximum
`0.00661910`). Diagnostic timing is faster than Python but is not performance
qualification. Batched backward storage and fused boundary-head attention remain
unresolved arithmetic differences. No tolerances or reference backends changed.

### v12 batched-gradient follow-up

The corrected storage-preserving batched VJP matches pinned Python forward and
gradient bits across all four operand layouts. All 15 selected CUDA/admission
checks pass with zero skips, including published small heads/full durable
resume, and the integrated batched-gradient memory check reports zero errors.
Strict default-epsilon heads first-update parity remains passing.

Full-model release remains blocked: the completed 100-update run first fails
loss parity at update 35, with 252 final weight tensors outside tolerance
(maximum absolute error `0.0066192069`). Diagnostic throughput is 15.65 native
versus 9.02 Python examples/second from two pairs; this does not qualify
performance. No tolerance or reference-backend changes were made.

A separate driver-loaded fused boundary-attention prototype now matches pinned
Python forward and backward bits across six sequence lengths on L4 and passes
NVIDIA memcheck. It is not integrated into the trainer. Its restricted coverage,
artifact generation, capability admission and whole-training validation remain
required before it can affect the release decision. See the v12 section of
[the arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md#v12-retained-batched-gradients).

## v13 final attention integration

The reproducible driver-only D32/zero-dropout boundary-attention integration
passes all 18 selected CUDA checks, including explicit fused durable resume and
recomputation. Integrated and artifact memory checks report zero errors; both
initial microbatch attention boundaries match Python exactly. Strict heads
validation at default Adam epsilon passes. Source/binary hashes and final results
are preserved in the training-arithmetic evidence directory.

Full-model release remains blocked: the final 100-update epsilon-1e-6 campaign
first fails loss tolerance at total update 37 and ends with 272 weight tensors
outside tolerance (maximum error 0.00666746). Diagnostic throughput is 15.110 vs
9.064 examples/second, but performance is not qualified. Standalone SiLU and
prefix-order probes identify remaining arithmetic differences; their corrections
require integration and renewed sustained validation. No commit or push.

## v14 SiLU integration

The shared CUDA 12.8 activation module now implements Python's direct FP32 SiLU
forward and seeded backward. Default CPU/Metal graph lowering stays unchanged.
Exact production-artifact comparisons cover 110,001 values; the integrated strict
VJP fixture, 31 selected ML/import checks, four artifact contract checks and
regeneration pass. All 19 selected CUDA checks have passing results: an initial
18/19 run hit host-memory admission in the full-model restore test during a
concurrent build, and its uncontended rerun passes. Explicit child-process
NVIDIA memcheck reports zero errors. Default-epsilon heads parity passes.

Full-model release remains blocked. The SiLU change makes boundary start/end
projection inputs and outputs exact, but content-prefix arithmetic still differs.
The completed 100-update campaign first fails loss tolerance at update 35 and
ends with 280 weight tensors outside tolerance (max absolute error 0.00672614).
Diagnostic throughput is 14.940 vs 8.934 examples/second, not qualified performance.
Next work is the shared prefix-sum operation with reference-layout-specific
arithmetic; isolated outer and innermost prototypes pass, while single-vector
CUB dispatch still needs a faithful implementation. No commit or push.

## v15 deterministic prefix sums

The shared graph and CUDA training-math module now implement outer-axis,
innermost and deterministic single-vector prefix sums with strict reverse-scan
VJPs. All 21 selected CUDA checks pass, including exact fixture bits and full
resume/recomputation; integrated and production-artifact memchecks report zero
errors. Default-epsilon heads parity and both initial full-model state checks
pass. Content-pooling normalization now matches Python exactly.

Release remains blocked by sustained full-model parity. The 100-update campaign
first fails loss tolerance at update 35 and ends with 272 weight tensors outside
tolerance (max error 0.00665256). Diagnostic throughput is 15.829 vs 9.062
examples/second; performance is not qualified. Generic serial reductions and
two differently associated score additions are the next demonstrated arithmetic
differences. CPU/Metal defaults remain unchanged; CUDA checkpoints bind SM count
because deterministic single-vector scan ordering depends on it. No commit/push.


## v16 reductions and sequential scores

CUDA training now uses the pinned PyTorch FP32 reduction geometry for forward
and AD-generated sums/means, with one checked planner shared by program admission
and dispatch. Global staging is explicitly owned and bounded; no persistent
workspace or semaphore state is introduced. Device-dependent launch geometry
and the artifact are bound into checkpoint identity. Sequential score addition
has a separate graph profile; CPU/Metal and generic defaults remain unchanged.

All 24 selected CUDA checks pass, including published heads/full resume and
portable reload. Exact reduction fixtures, integrated/isolated memcheck and
shared-memory racecheck pass. The old generic-reduction test was omitted from
the binary's build filters and still needs to be included in the next run.
Default-epsilon heads and both initial full-model state checks pass.

Full-model production qualification remains blocked: the 100-update run first
fails loss parity at update 35, with 258 final weight tensors over tolerance and
maximum error 0.00660725. The held-out quality gate also fails. Two diagnostic
timing pairs measure 15.28 vs 9.23 examples/sec; this is not qualified performance.
The next demonstrated mismatch is physical forward-transpose storage: native
copies it, while Python retains a view, changing cuBLAS accumulation order.
See the arithmetic follow-up and archived v16 evidence for exact identities.
