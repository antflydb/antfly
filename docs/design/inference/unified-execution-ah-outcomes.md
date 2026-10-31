# AH: Q3 four-column row ownership

Baseline: `1df0d939df`. This batch consolidates the three maintained Q3
no-minimum four-column numerical loops into `q3KNoDmnPanel4Rows`. Private ABI
leaves keep their existing tuple/array results and callers.

Row count specializes at compile time. All sixteen subblocks remain unrolled;
MR4 still computes two MR2 integer-dot tiles before updating its four float
accumulators. Weight scales, signed integer dots, subblock accumulation and the
final activation-scale multiply retain their arithmetic order. No allocation,
function-pointer dispatch or generated expansion is introduced. GPU source and
physical tensor layouts are unchanged.

An identical test overlay in baseline and candidate checks MR1/MR2/MR4 against
independent scalar i64 dots and f64 accumulation with literal panel offsets.
Fixtures cover zero inputs, activation -128/127, Q3 -4/3 and mixed signed values,
with signed, nonuniform power-of-two scales. Existing Q3 single/pair/triple and
shape-gating tests exercise composed callers and route admission.

Qualification uses native and generic AArch64 binaries. The three changed
profiles and nine unchanged K16/Q8 profiles run fixed 32-arm AA/AB/BA cohorts,
25 ms warmup, 21 measured batches, and unchanged 5% median / 10% p95 limits.
Results describe recorded host load; no automatic timing retry is used.
Measured driver calls must remain inside their timing loops, and the generic
static call closure must contain no SDOT. Changed driver bodies are inspected
separately from numerical and timing evidence, with limits declared before
timing: at most 1% instruction growth and 64 additional fixed stack bytes.

Source accounting charges the new owner and all private ABI leaves. Inline
tests and documentation are accounted separately from maintained production.
No new whole-kernel percentage or GPU performance claim is made.

Evidence: `.benchmark-assets/1166/ah-campaign/`.

## Results

Maintained production removes **1,962 bytes / 72 lines**, including the new
owner and all ABI leaves. Generated production is unchanged. New inline test
coverage adds 4,364 bytes / 69 lines and is charged separately.

All twelve native profiles pass all AA/AB/BA cohorts. Worst AB/BA
median ratios are at most 1.00652/1.00042; p95 ratios
are at most 1.02760/1.01147. These are
recorded-load comparisons with no automatic timing retry or relaxed limits.

All twelve generic profiles pass all AA/AB/BA cohorts. Worst AB/BA
median ratios are at most 1.02142/1.00119; p95 ratios
are at most 1.03005/1.01876. These are
recorded-load comparisons with no automatic timing retry or relaxed limits.

All four direct/caller binaries pass 43 selected tests with zero skips.
The broader caller suite passes 122 tests with 1 existing optional-fixture
skip. Metal/CUDA compilation, x86-64 v2/v3/v4 oracle compilation,
WASM32/64 builds and generated-source freshness pass. Only the two AArch64
CPU modes execute; no fresh GPU or x86 device timing is claimed.

All six changed-body resource bounds pass. Native MR1 in the measured test
wrapper shrinks from 445 to 391 instructions and 480 to 16 fixed stack bytes.
Native MR2 and generic MR2/MR4 retain identical instruction words; native MR4
and generic MR1 retain instruction counts and fixed stack sizes while changing
instructions. Equal counts alone are not treated as identity. The complete
measured generic call closure contains zero SDOT, and all measured calls remain
inside their timing loops. These body results do not claim the same frame
reduction in every inlined caller context.

All 8,020 source pins match the measured inputs. Baseline production
is audited unchanged before removing its temporary worktree; its test overlay
and all four executables are retained. No composed-model timing is added in
this CPU-family batch. The read-only Glowworm receipt still reports the earlier
AA CUDA attempt as `waiting_or_running`; it is not fresh qualification for AH.
No branch is pushed.
