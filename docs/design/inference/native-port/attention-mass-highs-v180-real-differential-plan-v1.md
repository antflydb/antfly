# HiGHS v1.8.0 restricted-LP differential experiment

This is an unapplied, training-only experiment plan. It does not add HiGHS to Antfly, authorize a download/build/run, load a model, or change fitting gates. The authoritative implementation remains the frozen Python/SciPy recipe.

## Why this is the next experiment

The bounded Main3 mass profile spent 147.49496 of 164.28444 seconds (89.78%) inside 71 HiGHS dual-simplex solves. Process removal and Zig control-flow alone therefore cannot establish serving viability. The next useful result is whether the already compiled solver-neutral CG controller can call an independently pinned HiGHS v1.8.0 library and reproduce one real restricted LP under the exact frozen retry and witness policy.

## Frozen inputs

- HiGHS tag `v1.8.0`, commit `fcfb5341462f8a7db5ef5038613413f97d6cac3d`, MIT license.
- CG V5 `attention_mass_constraint_generation_v5.zig`, SHA-256 `abdcc63e7d914b174f3fb8db2302db16af126386b610fcf6e80bf1dbb2ec82fa`; compiled 14/14 receipt SHA-256 `faddb16f6f90a0b5ca6ece1ace2e1bd71ca35efa2b7638586020f392d2daad8b`.
- LP V4 `/private/tmp/attention_mass_lp_v4.zig`, SHA-256 `ff1d98cab21450271694ab524dd0733294ae0ff15793c57ed105f4d18d9354de`.
- injected adapter V3 `attention_mass_highs_v180_adapter_v3.zig`, SHA-256 `493637b21fec53d07813fddac5cc874d232ae6680befe504bf639e9e3e6c5525`; guarded injected-API tests 7/7, but no real ABI call.
- fixture exporter `export_attention_mass_highs_restricted_fixture_v1.py`, SHA-256 `1c277ae899839278ee60d001a56e78d751074755c4eaf7856ff6f6194baa916a`. It binds a 1128x288 real restricted matrix, exact 1e-9 weights/duals, and metrics-only 1e-10 oracle without importing NumPy or SciPy.

## Pinned library receipt before linking

A separately authorized fetch/build must use the exact commit and record: source archive/tree SHA-256; `LICENSE.txt`, `highs_c_api.h`, `HighsCallbackStruct.h`, generated `HConfig.h`, compiler/CMake identities and flags; target architecture; `libhighs.a` SHA-256; `nm` symbol inventory; and `otool -L` output. Reject any library unless runtime calls report version 1.8.0, the expected git hash, and `Highs_getSizeofHighsInt()==4`.

First build only one static variant. Keep upstream defaults that affect solver trajectory; do not set threads, parallel, random seed, edge weights, scaling, crash, or update policies. Use `Release`, static library, tests/examples/Zlib off, `HIGHSINT64=OFF`, IPO off. Treat `HIGHS_NO_DEFAULT_THREADS=ON` as a separate later variant because it may change behavior relative to SciPy's bundled build.

## Real C-ABI differential

1. Export the already frozen restricted fixture to a fresh directory and verify every edge before decoding.
2. Compile a separate Zig test executable linking only the pinned static HiGHS archive, C++ runtime, CG V5, LP V4, adapter V3, and a small fixture reader. Do not link the model runtime.
3. Reconstruct the exact LP: 289 columns (`w[0:288], t`), 2256 rows (all upper rows then all lower rows), dense coefficients emitted in original row/column order including signed zero, weight bounds `[1e-9,1e6]`, `t` bounds `[0,+inf]`, objective `min t`.
4. Set exactly `solver=simplex`, `presolve=on`, `simplex_strategy=1`, output/logging off, and primal/dual feasibility tolerance `1e-9`. Preserve raw nonpositive HiGHS row duals. The CG controller alone maps `alpha=-upper_dual`, `beta=-lower_dual`.
5. Require the original-coordinate LP V4 witness to reject the 1e-9 result before retry. Then create a new HiGHS instance, set both tolerances to `1e-10`, solve once, and require the same witness to pass. No clipping, feasibility relaxation, presolve variation, or reuse between attempts.
6. Record status, objective bits, primal/row-dual array hashes, iteration count, every witness residual, callback/deadline state, elapsed assembly/run/extraction time, peak RSS, swap delta, and create/destroy counts. The retry oracle currently has metrics but no saved vectors, so it cannot support a byte-identity claim.
7. Fail the experiment unless both attempts follow the frozen disposition, the full original-coordinate witness result matches, every allocation and HiGHS instance is released, the absolute-deadline/cancellation tests retain CG V5 terminal semantics, and swap remains zero. Numerical mismatch is evidence against integration; it is not grounds to weaken a gate.

A solver-output byte match is recorded if obtained, but the minimum equivalence claim is exact policy/disposition plus unchanged original-coordinate witness gates and stored conversion. Objective/residual differences must be reported in ULP and absolute terms; there is no tolerance-based promotion to production.

## Persistent model and basis reuse: limits and follow-up

HiGHS v1.8.0 exposes `Highs_addRows`, `Highs_getBasis`, `Highs_setBasis`, and `Highs_zeroAllClocks`. These make a reuse experiment possible, not automatically equivalent.

CG V5 rebuilds each restricted problem in ascending full-row order. New violated rows are selected by violation magnitude and can sort before existing rows. Appending them with `Highs_addRows` changes constraint order and raw-dual positions. A persistent instance that merely appends rows therefore cannot be substituted into the frozen controller.

The first reuse experiment must wait for cold real-ABI parity. Then export one complete, successful Python CG round sequence: selected full-row indices in canonical order, restricted matrix hashes, solver options, cold solutions/duals, iterations, witness metrics, and retry decisions. Compare under the same guard:

- **A, cold control:** new HiGHS instance and canonical rebuilt matrix every round.
- **B, basis-seeded canonical rebuild:** new instance and canonical matrix every round; remap the prior column/row basis by full-row identity, mark genuinely new row slacks basic, call `Highs_setBasis`, zero clocks, then solve.

Do not test append-only persistence first. B must preserve canonical matrix/dual ordering, but warm-starting still changes simplex trajectory. It is acceptable only if every round has the same status/retry decision, passes the same restricted and full original-coordinate witnesses, yields identical stored payload bytes and mass metrics, and shows a material guarded timing benefit. Any invalid basis, different retry branch, payload difference, deadline regression, leak, or swap growth rejects reuse. A full saved round sequence is required because the current single fixture cannot establish basis behavior.

## Decision boundary

A passing cold differential authorizes only an isolated real-ABI adapter experiment. It does not establish full fitter parity, serving latency, or production readiness. Basis reuse remains a separate algorithm-trajectory qualification. Reducing LP call count changes constraint-generation scheduling and is outside this experiment.
