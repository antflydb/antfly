# AA: native projection policies and decode correctness

Baseline: `adc9e4d78d`, the merge of main through `ef25cb9405`. Main's typed
`antfly_document_runs` control protocol and shared readers module are retained.
Earlier campaigns and upstream changes receive no new deletion credit.

## Ownership and specialization

Metal quantized projections now use native compile-time families for column
reductions, two-column sharing, K-format register tiles, matrix tiles and the
packed Q4 tile. Format decoders, epilogues, geometry and operand precision are
static policies. Q4_K matrix operands remain half precision; Q6_K operands
remain float. Kernel exports retain their launch ABI and scratch ownership.
Florence's NR4 path instantiates the same K tile without adding a model policy.
The old numerical text emitters are removed.

CUDA's ten Q4/Q8 gate/up and down exports instantiate two native inline
families. Legacy zero-point DP4A and GGML stored-half-sum policies remain
separate. The canonical family precedes every production and development
caller. Source validation still requires every external helper exactly once.

CPU K16 panel8 and Q8_K panel4 MR1/MR2/MR4 use native comptime row families.
K16 minimum corrections, integer accumulation boundaries and the existing
architecture-specific dot leaves retain their precision and order. Nonpacked
Q4/Q5 triple projection publication shares an MR2/MR4 owner. Packed and fused
SDOT schedules retain their distinct implementations. Retired generator
branches are deleted rather than left behind a constant-false condition.

A small shared native-source descriptor stores canonical owners as includes
in checked-in standalone artifacts. Runtime JIT sources remain complete in
memory, and semantic source fingerprints cover expanded content. Every new
native owner and the descriptor participate in backend qualification identity.
Storage reduction and maintained-owner reduction are accounted separately.

## Correctness findings

Both Metal paged decode ABIs now share one numerical implementation. All-masked
scores produce zero mass instead of exponentiating sentinel minus sentinel.
A barrier ensures every SIMD group reads the maximum before sum scratch is
reused. The all-tokens-allowed fast path checks the oldest token's actual age;
a short KV cache entirely outside a sliding window is no longer admitted just
because the window is wider than the cache.

The maximum reduction uses SIMD maxima followed by a SIMD fold of group
maxima. Scores are finite or the invalid sentinel. Softmax sum, dot and value
accumulation orders retain their existing trees. Device validation includes
future tokens, expired windows, a one-token expired cache and ragged score
folds, through both generated and compatibility ABIs.

## CUDA native evidence

NVCC 13.2 on native Linux ARM compiled all shipped images for SM75, SM80,
SM89, SM90, SM100, SM110 and SM120. All 454 kernels have identical native
instructions and resources on every target. The complete SM89 cubin is
byte-identical to the merged baseline. The isolated compiler VM was removed;
the original Podman default was preserved.

The two SM89 Q4/Q8 promotion records preserve their original timing samples.
An explicit source-refactor equivalence record binds the new semantic source
to the byte-identical qualified image. This is transferred native-code
qualification, not a new GPU timing measurement. Regression coverage checks
the candidate semantic source and actual cubin hashes. Admission gates remain
unchanged.

Raw receipts and failed prototypes are retained in the ignored
`.benchmark-assets/1166/aa-campaign/` directory. Early iterations include an
invalid MSL static-data-member prototype, stale source-shape assertions and a
stale decode harness case count. Numerical device cases passed before the
case-count assertion was corrected. No failed run is represented as a full
qualification pass.

## Completed direct checks

The selected compiler, renderer, source-owner and independent CPU tests pass:
168 selected, no skips. Real Metal compilation and device execution pass all
167 runtime cases and 50 v2 conformance cases. The 25 Python tooling tests pass.
The compiler bridge expands and pins local include closures, then invokes
Metal's actual `newLibraryWithSource`; it does not produce or claim offline AIR.

Nine CPU panel profiles have an independent F64 dequantized-dot oracle, including
signed inputs and scales and K16 minimum correction. Identical benchmark drivers
are built against the merged baseline and candidate. Fixed 32-arm AA, AB and BA
cohorts pass the unchanged 5% median / 10% p95 gates, without automatic retries.
The largest candidate median ratio is 1.030. K16 MR4 without minimum correction
measures 0.927/0.931 in AB/BA (about 7% faster); other profiles are near parity.
These are isolated panel measurements, not a model-wide speedup claim.

## Inlining regression found by schedule qualification

The additional 43-schedule Metal matrix/packed/tiled/reduction audit caught a
regression outside the default production schedule: Q4_K 64-thread tree and
hybrid reductions at rows8/K2048/N128 measured about 9% slower. Outputs, guards
and resource limits still matched. The failed AA/AB/BA audit and its exact
source programs are retained under `variants-initial/`.

Separate prototypes tested explicit decoder-policy inlining, signed loop
strides, and both. Inlining alone restored all six affected shape checks to
parity in both creation orders; changing stride types alone retained the
regression. The canonical decoder policy now requires `always_inline`, with
source-contract coverage. This preserves the shared native family without
adding a slower call boundary or weakening the 5% performance gate.

The final source passes all 75 default projection shapes and 80 additional
schedule/Florence cases in fixed 32-arm AA/AB/BA cohorts: exact output and guard
parity, no resource regression, and median ratios within 1.05. Additional
coverage includes matrix40/matrix64, packed Q4 tiles and K-family NR2/NR4/NR8
with independent reduction geometries. Seven compatibility decode performance
cases pass the same gates against the merged baseline and an independent F64
attention oracle. Six future/expired cases match exact zero; the old baseline
is rejected by the oracle as an expected negative control. The shortest decode
case is about 12% faster in both comparison orders; long contexts remain near
parity. These claims apply to the measured shapes and M4 Max, not every device
or model geometry.

## Merged build wiring

The combined Metal/CUDA product check passes. Adding the shared source owner
exposed a stale hard-coded CUDA dispatch-input count; the dispatch list now
derives its count, while the storage owner belongs only to qualification
identity. The independent cache test computes the complete ordered bundles
and mutates every new owner individually.

WASM compilation exposed a missing import after main's typed document-run
protocol merge. The browser root now imports a single-threaded, target-matched
`antfly_document_runs` module, and the focused native WasmCompute test root
reuses the runtime's existing owner. This keeps the typed protocol intact.

Both-backend product compilation, WASM32/WASM64 products, and x86 v2/v3/v4
products and narrow-panel oracle specializations compile successfully. The
expanded cache regression passes and removes its isolated fixture caches;
those fixture caches live under this worktree's `.zig-cache`, following the
repository cache policy. Final freshness checks cover 70 generated files and
129 native owned regions, and CUDA source policy passes.

## Composed behavior and native instruction protection

All 15 composed model cases and 42 primitive/width/host-encoding controls pass
output parity and the unchanged 5% median / 10% p95 thresholds. Route assertions
pass. Decoder reset parity is exact, and both decoder outputs pass the
independent reference at tolerance 0.002. The seven protected CPU bodies retain
normalized instructions, resources and required entry alignment; changed
address references resolve to identical constant bytes. This includes the
Q4/Q5 nonpacked triple-projection path now owned by a comptime row family.

All five 120-second ownership soaks pass with finite outputs, no allocation
denials and zero tracked host allocations after teardown. They are stress
checks, not latency evidence.

## Safe/fast math and larger-shape confirmation

The final direct matrix covers 192 projection/decode shapes under both safe
and fast Metal math, plus six exact-zero masked cases per math mode. It
includes matrix rows40/64, full tiles and tails through rows129, and all nine
packed-Q4 geometries at larger K/N. Output and guard parity and resource gates
pass throughout.

One original fast packed-Q4 AB timing missed the 1.05 median gate at 1.05509;
reverse-order BA passed at 1.00675. The same-program AA samples changed from
about 12 to 16 microseconds within that cohort. That failed run is retained,
not relabeled as a pass. A single predeclared confirmation tested all 18 packed
shapes with 2,000 ms warmup, 1,024 launches per arm, 32 fixed arms, and both
AA/BB controls plus AB/BA comparisons. Every case passes the same 1.05 gate:
worst ratios are AA 1.00654, BB 1.00931, AB 1.02991 and BA 1.02564. No source
change or automatic retry occurred. The stronger full-family confirmation
qualifies that case; receipts explicitly retain the initial miss.

## Corrected source ledger

The ledger compares against the merged baseline and charges all replacement
native owners. Two deleted CUDA `SourceFragment` goldens, totaling 8,695 bytes,
were referenced only by tests but declared outside their test blocks. Earlier
provisional figures incorrectly credited those bytes as production deletions.
The accounting tool now separates private source fragments referenced only by
tests; production references keep a fragment in the production budget. Its
regression covers both cases and embedded shader braces. All 26 final tooling
tests pass.

| Budget | Candidate minus merged baseline |
| --- | ---: |
| Maintained production owners/contracts | **+2,483 bytes / +100 lines** |
| Generated production text | −69,330 bytes / −1,144 lines |
| Combined production text | **−66,847 bytes / −1,044 lines** |
| Removed test-only shader goldens | −8,695 bytes / −182 lines |
| Other inline test coverage | +7,278 bytes / +124 lines |
| Tests/benchmark/accounting tooling | +7,837 bytes / +158 lines |

The maintained generator-owner budget falls by 25,602 bytes, offset by the
new native CUDA/Metal owners, CPU helpers and shared contracts. Moving
already-shared generator algorithms into native templates therefore removes
stored expansions and simplifies numerical ownership, but does **not** deliver
a further 20% maintained-code reduction. The measured isolated wins are about
7% for K16 MR4 without minimum correction and 12–16% for short paged decode;
composed models remain within their existing gates.

CUDA fatbin storage grows by 232 bytes; PTX storage falls by 1,936 bytes. Native
SM89 code remains byte-identical. Image bytes are separate from source bytes.

The full measured input manifest is retained unchanged. After timing, only
the accounting tool and its regression test changed to correct the fixture
classification; their old/new hashes and passing tooling checks are recorded
separately. Every runtime, native source, build and generated-image input still
matches the measured snapshot.

CUDA device timing on Glowworm remains a separate qualification step. The AA
queue uses its own worktree, caches, immutable source/helper pins and private
cuDNN links; it waits behind the earlier X/OCR/Z controllers and other branch
work. It stops only its own process group if contention resumes. Static native
qualification and transferred SM89 evidence do not stand in for this run.
