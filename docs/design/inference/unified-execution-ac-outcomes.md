# AC: one native CPU Q6 panel8 family

Baseline: `338acda1ae`, after the latest main merge recorded in AB.

## Numerical ownership

The ordinary and fused MR1/MR2/MR4 panel entry points now use one native
`q6KPanel8Rows` owner. Row count and accumulation policy specialize at compile
time. Existing entry signatures, return adapters, runtime eligibility and the default
non-fused selection remain unchanged. The fused MR4 wrapper deliberately retains two MR2
tiles rather than creating a larger live accumulator set; the numerical owner
rejects a fused four-row instantiation at compile time.

Ordinary traversal retains sixteen runtime iterations, eight-column integer
vectors and the existing MR1/MR2/MR4 dot leaves. Fused traversal retains sixteen
unrolled subblocks, column-major SDOT updates and four-lane integer accumulators.
Both retain signed integer subscales, final vector reduction and the original
FP32 scale/publication expression. No heap allocation or runtime format switch
is introduced. The renderer emits only calls/adapters for this family; obsolete
narrow-panel branches are removed from its remaining wide-panel traversal.

## Direct qualification

Four deterministic fixtures include zero inputs, signed-byte input extrema,
Q6 weight extrema, signed subscale extrema and negative floating scales. All
six entry points match an independent scalar F64 oracle with bit-exact FP32
publication. Baseline and candidate use identical drivers. The first driver
compile exposed Zig 0.17's restriction on runtime vector indexing; the oracle
now converts its result to an array. That compile failure is retained and is
not performance evidence.

The fixed comparison uses 32 arms in each AA/AB/BA cohort, 20,001 calls per
batch, ten warmup batches and 21 measured batches per profile. Raw integer
batch durations and fractional per-call times are retained. Gates remain 5%
median / 10% p95, with no timing retries or changed thresholds.

All six profiles pass every cohort. Worst candidate/baseline ratios are
1.00322 median / 1.02126 p95 in AB and 1.00227 / 1.02747 in BA. This establishes
local performance parity, not a new Q6 speedup claim.

Disassembly verifies all six kernel calls remain inside their timed loops.
Ordinary kernels retain sixteen iterations containing 8/16/32 SDOTs; fused
kernels retain complete 128/256/512-SDOT call closures. The ordinary entry's
runtime fused branch remains present, and the driver explicitly forces it off.
Static counts including both mutually exclusive paths are kept separate from
executed operation counts.

The first variant placed the fused MR4 two-tile adapter inside the numerical
owner. Moving it back to the existing wrapper simplifies ownership but does
not change LLVM's emitted kernel body. Its frame is 656 bytes versus 640 bytes
at baseline; the ordinary MR4 entry remains 672 bytes. Other entry frames are
unchanged. The bounded 16-byte increase is retained with passing timing gates,
not described as machine-code/resource identity. Initial variant receipts and
its passing comparisons remain under `array-mr4-prototype/`.

## Source budget and scope

Every replacement owner is charged against the baseline. Maintained production
shrinks by **1,138 bytes / 31 lines** and generated production by **4,446 bytes /
80 lines**, for **5,584 bytes / 111 lines** of total production text removed.
Inline tests add 5,383 bytes / 85 lines and receive no production savings credit.
The numerical owner adds 743 bytes, offset by 1,881 bytes removed from its
renderer. This is a modest incremental cut, not a whole-kernel 20% claim.

CUDA and Metal source/images are unchanged by this batch. Glowworm's earlier
AA qualification controller remains waiting behind existing work; no job was
stopped and no new CUDA device timing is claimed. Raw receipts, the fixed
protocol, binaries and the retained initial compile failure are under
`.benchmark-assets/1166/ac-campaign/`.

## Final validation and cleanup

The final implementation passes all 26 selected CPU caller/oracle tests,
including ordinary and forced-fused prepared Q6 panel routes; no cases skip.
Combined Metal/CUDA compilation, x86-64 v2/v3/v4 test-binary builds, WASM32/64
product builds and all 70 generated files / 129 owned regions pass. Cross-target
binaries are compile checks, not execution on those processors.

All seven protected native GEMM, attention and projection bodies retain their
normalized instructions/resources and required alignment. Address differences
resolve to identical constant bytes. These protected-body checks are separate
from the Q6 frame difference recorded above. No fresh composed model timing or
GPU timing matrix is claimed for this CPU-only batch. The final Zig source
manifest matches every validated input.

The task-created baseline worktree contained only the exact recorded test
overlay; production identity was verified before its removal. Its patch, both
benchmark binaries and all raw comparisons remain in the campaign bundle.
Task-scoped cache GC considers only this worktree's active caches/output,
leaving other worktrees and user caches alone. Nothing is pushed.
