# AR: projection ownership and Metal preparation

Baseline: merge `649b3eb88b`, incorporating `origin/main` at `ea7bea940a`.
Implementation: `e56d304a65`.
Accounting includes replacement owners and metadata, separates inline tests and
generated code, and reports compiled-image bytes separately.

## Accepted implementation

CPU quantized pairs/triples share fixed-cardinality admission, bias and fallback
ownership. Codec lookup reuses the existing typed dot owner. Pair accumulator
schedules remain separate. The typed triple fallback is enabled only for Q4_0,
Q4_1, Q5_0, Q5_1, Q2_K, Q3_K, Q5_K and Q6_K; other formats retain their generic
schedule. Portable ARM single-row Q5_0 also keeps its established generic route.
Microkernel bodies and generated activation/worker regions are unchanged.

CUDA grouped outputs share shape/buffer acquisition and partial transfers.
Error retirement preserves reverse allocation order; explicit fallback retirement
preserves forward order. Normal-null QKV and Q4-span declines now release their
allocations, fixing leaks that `errdefer` did not cover. Failure injection covers
every host allocation position, device budget denial and partial transfer.

Metal shares the gate/up, activation and multiply phase across gathered F32,
planned mixed-precision and staged mixed-precision policies. Precision admission,
pair eligibility, scope/barrier behavior and intermediate release points remain
explicit. An independent F64 oracle covers all three policies at one, three and
eight rows, plus late optional-decline cleanup. This is a shared phase, not a
completed whole-decoder rewrite.

Metal required admission and ordered diagnostics consume the lifecycle catalog's
typed identities. Bulk loading resolves each ordinary/precise library cache once
per range. All four failed-constructor exits now destroy partial ownership;
freeing raw storage previously retained ARC-owned objects. Fault injection proves
the original leak and its retirement. The permanent checker exercises partial
ownership without GPU submission.

Optional preparation uses complete GGUF metadata and registered lazy storage,
including embeddings outside the JIT linear scope. Dense models include automatic
Q8_0 staging. Missing stores, unknown formats, opaque quantized storage, JIT
qualification, explicit kernel overrides and legacy provider callers retain full
preparation. Required pipelines are always prepared before provider publication.
Encoder dispatch retains direct named fields, with no new frame-time compilation
or pipeline lookup.

The catalog contains **458 slots**; dense/Q8 preparation selects **405**, skipping
53 optional slots. Required validation retains 176 entries after the intentional
retirement below. Tests cover every format bit, complete-model census, required
admission, cached identity and live overrides.

## Proven unreachable variants

Three unregistered CUDA BF16 Q4_0 tensor-core bias epilogue entry points are
removed. Pinned CUDA 13.2.86 reproduces the original shipping PTX before rebuilding
the candidate. All **451 retained functions** have identical SASS and resource
usage on SM75, 80, 89, 90, 100, 110 and 120, and independently in standalone SM89.
Artifact regressions reject reintroduction of retired symbols.

Metal retires unused dense direct pair/QKV and I2_S paired I8 shaders, plus four
unused catalog slots. The Q4_0 activation-ID shader remains because its separate
experimental benchmark still consumes it. Creation and retirement ordinals are
independently renumbered. The runtime structure is private; parameter ABIs stay
unchanged.

Device source decreases by **5,350 Metal bytes + 1,005 CUDA bytes**. CUDA images
shrink by 166,608 fatbin bytes, 28,288 standalone SM89 bytes and 1,874 PTX bytes.
Large PTX label-renumbering diffs are not authored-source reductions.

## Validation and performance

The full production Metal projection checker passes output, producer/consumer,
copy, canary, fallback and cancellation checks. Seven focused final regressions
pass without skips, including checkpoint FFN versus CPU. The full native suite
passes 324 tests with two opt-in benchmarks skipped. Baseline x86 Linux projection
compilation, both-backend checking, CUDA source/artifact regressions and generated
freshness (70 files + 129 native regions) pass. x86 runtime performance is unmeasured.
A stale merged CUDA
capture test now initializes `execution_scope` instead of obsolete `audio_scope`.

Glowworm passes **47/47 tests, zero skips, failures or leaks**, covering all nine
named attention/replay, grouped-output and allocation-owner cases. AQ's original
44/44 device receipt is retained independently, closing its queued validation.

Constructor experiments prepare shader libraries before timing. The cache/
validation stage passes AA/AB/BA bounds with 3.7–8.6% lower warm medians.
Separately, optional-format preparation passes AA/AB/BA bounds with 6.8–7.4%
lower first construction; its warm medians are 1.2–2.5% higher, within the preset
5% bound. These are separate stages: percentages must not be added or presented
as model-throughput gains.

Against the merged-main baseline, maintained production source decreases by
**22,134 bytes**, including all replacement metadata:

| Scope | Net bytes removed |
| --- | ---: |
| Metal runtime, catalog, provider and FFN phase | 7,835 |
| CPU projection ownership and routing | 6,218 |
| CUDA output ownership | 1,726 |
| Metal shader source | 5,350 |
| CUDA device source | 1,005 |
| Total | 22,134 |

The final eight-format CPU matrix passes **288/288 profile/order checks per
build**, covering six shapes and biased/unbiased triples, with AA/AB/BA orders
and 21 paired samples each. Candidate median ratios are 0.973–1.033 native and
0.984–1.010 portable. Numerical comparisons use the original complete triple
function. The initial portable Q5_0 single-row experiment passed the preset
bound but consistently cost 4.2–4.5%; retaining its established generic route
reduces that cost to 0.2–0.7%. Both experiments remain retained.

All four model cases pass all AA/AB/BA comparisons with **byte-identical outputs**:

| Model case | AB median / p95 ratio | BA median / p95 ratio |
| --- | ---: | ---: |
| Native decoder, prompt 8 / decode 4 | 1.000 / 0.996 | 1.001 / 1.004 |
| Metal decoder, prompt 8 / decode 4 | 0.989 / 1.004 | 1.002 / 1.015 |
| Metal safetensors encoder, batch 1 / sequence 16 | 1.002 / 0.974 | 1.001 / 1.020 |
| Metal GGUF encoder, batch 1 / sequence 16 | 1.000 / 1.011 | 1.000 / 1.007 |

Each model/order has 32 balanced process arms. The model candidate predates
only the final portable-Q5 adjustment: native specialization reconstructs the
same effective owner tokens on the M4 target, with the rest of CPU source and
all Metal source unchanged. Final CPU matrices separately qualify that adjustment.
This is a source-specialization proof, not a machine-instruction identity claim.
Timing protocols retain AA controls, both candidate orders, immutable binaries,
input hashes and failed results, without automatic failure retries. AQ's older
portable prepared-family AA-control failure remains a limitation; this routing
experiment is a different scope.

Evidence: `.benchmark-assets/1166/ar-campaign/`, including `routing-final/`, `routing-final-v2/`,
`models/`, `selective-preparation/`, `preparation-performance/`, `cuda/`,
`cuda-retirement/` and `metal-retirement/`. Syntax/fixture failures, the original
gathered-phase comparison and corrected F32-only oracle fixture are retained.

The existing CUDA registry and AQ GQA preflight request remain their owners.
No duplicate registry or attention policy framework is introduced. Larger decoder
phase consolidation remains distinct; this campaign does not establish a further
20% reduction of shader arithmetic.
