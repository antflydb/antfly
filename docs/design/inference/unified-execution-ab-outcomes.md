# AB: one native CPU Q8 four-column family

Baseline: `cc7a52d1f4`. This batch changes CPU Q8 dot ownership and its test
harness; CUDA/Metal native source and generated GPU images are unchanged.

## Implementation

Ordinary blocks, packed panel4 and fused SDOT now share `q8KDotFour`, specialized
at compile time for MR1/MR2/MR4. The nine public wrappers retain their signatures
and return types. The numerical text emitter and its seven traversal/scaling
helpers are removed; the renderer emits only static calls and return adapters.

Two arithmetic schedules remain explicit in the native owner: eight 32-byte
chunks with scalar integer sums, and sixteen 16-byte chunks with vector SDOT
accumulators. The latter reduces each vector when publishing its scaled output,
matching the old fused ordering. Portable MR4 blocks still use two MR2 results;
fused calls fall back to blocks when native dot-product instructions are absent.
There is no heap allocation or runtime format dispatch in this helper.

The first fractional-timing prototype moved ordinary MR1/MR2 input-scale loads
after the dot. MR1 missed the median gate (AB 1.10158, BA 1.04624). Restoring the
original early scale loads removes the critical-path cost; other schedules
keep scale loads after accumulation to avoid unnecessary live registers. The
failed prototype and raw samples are retained.

## Benchmark correction and direct evidence

The first integer-rounded run produced apparent 7–8% differences at roughly
6–8 ns per call. Integer rounding is too coarse for a 5% gate at that scale.
The initial suspicion of loop-invariant hoisting was **incorrect**: disassembly
of both original binaries confirms all 64/128/256 SDOT operations, including
callee work, execute inside their timed loops. Those audit receipts are kept.

The final harness uses an explicit non-inlined kernel boundary, 20,001 calls per
batch and 21 samples after warmup. It stores whole batch durations and reports
fractional per-call nanoseconds alongside raw batch counts. Disassembly verifies
all nine calls remain inside the timed loops and their complete instruction
closures contain 64, 128 or 256 SDOTs. Baseline and candidate use identical
drivers. Four deterministic signed-byte fixtures pass an independent F64
scalar dot oracle with bit-exact FP32 publication, including negative scales.

All nine row/layout combinations pass fixed 32-arm AA/AB/BA comparisons with the
unchanged 5% median / 10% p95 gates. Final AB/BA comparison maxima are below 1.025 for
median and 1.027 for p95. This is performance parity, not a broad Q8 speedup
claim. Initial rounded and scale-order prototypes are not admission evidence.

The earlier K16 result was rechecked against the original merged baseline
`adc9e4d78d` with the same raw-batch/call-boundary method. All nine cases pass;
K16 MR4 without minimum correction measures median ratios 0.73425/0.73598 in
AB/BA. This approximately 26.5% isolated improvement belongs to the AA comptime
K16 implementation. The old roughly 7% figure used a different inlined timing
context; it should not be treated as a context-independent kernel speedup.
The complete 128/256/512-SDOT K16 call closures are verified too. Composed model
gains are not inferred from these isolated measurements.

Raw receipts are retained under `.benchmark-assets/1166/ab-campaign/`.

## Validation and source budget

The final snapshot passes 23 selected CPU caller/oracle cases, the combined
Metal/CUDA compile check, x86-64 v2/v3/v4 test-binary builds, WASM32/WASM64
product builds and all 70 generated files / 129 managed regions. Seven protected
native function groups retain their normalized instructions and resources;
aligned GEMM/attention entries retain their required 64-byte alignment.
The full Zig source manifest matches the validated build.

Against `cc7a52d1f4`, accounting charges every replacement owner: maintained
production shrinks by **3,385 bytes / 89 lines**, generated production by
**13,839 bytes / 290 lines**, for **17,224 bytes / 379 lines** of total production
text removed. Inline tests grow by 5,336 bytes / 76 lines and receive no
production reduction credit. This batch provides a real maintained-code cut;
it does not establish a 20% whole-kernel reduction.

This is local Apple M4 Max CPU qualification. CUDA/Metal sources and GPU images
are unchanged by this batch; the earlier AA model and GPU receipts remain
associated with their original snapshot. The isolated Glowworm AA queue is
still waiting behind other work and does not qualify this new CPU snapshot.
No fresh CUDA hardware performance result is claimed here.

The two task-created baseline worktrees were audited for production identity,
and their benchmark-only patches retained before removal.
