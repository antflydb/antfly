# Attention mass full-CG differential V2

This is an isolated training-only diagnostic. It does not modify the inference runtime, fit a payload, access held tensors, or grant execution/publication authority.

## Three guarded phases

Each phase must run alone under the existing process-group guard: 1024 MiB RSS, zero swap growth, 560 second internal deadline, 600 second external timeout, full drain.

1. `export_attention_mass_full_cg_fixture_v2.py` validates the exact successful Main3 owner14/block2048 plan and source/seed closure, loads training-only arrays, and evaluates only the frozen `_mass_context` and `_mass_matrix` expressions for the original seed. It writes one `3000x288` little-endian FP64 matrix. It never calls `fit`.
2. `record_attention_mass_python_full_cg_oracle_v2.py` runs frozen `warm_v4` with `initial_indices=None`. Its wrappers call the original pricing and solver functions first, then record their results. The result includes every restricted-matrix digest, tolerance/retry branch, selected/add-row sequence, objective/iterations, pricing margins, full witness, and terminal arrays.
3. `attention_mass_highs_v180_full_cg_driver_v2.zig` composes unmodified CG V5 with Adapter V4 and the reviewed HiGHS v1.8.0 static library. A solver wrapper records the original restricted matrix from the LP upper rows before calling Adapter V4. The driver fails if the restricted-matrix/tolerance sequence, round diagnostics, final selected rows, or float32 bias bytes differ from the Python oracle.

The native driver must be linked with the exact named modules and static library already used by the restricted V5 differential:

```text
-Mroot=attention_mass_highs_v180_full_cg_driver_v2.zig
-Mattention_mass_lp=/private/tmp/attention_mass_lp_v4.zig
-Mattention_mass_cg=attention_mass_constraint_generation_v5.zig
-Mattention_mass_highs=attention_mass_highs_v180_adapter_v4.zig
zig/zig-out/native-highs-v180/libhighs.a
-lc++ -framework Accelerate
```

The executable receipt must bind the driver, LP V4, CG V5, Adapter V4, Zig executable, exact link argv, reviewed library receipt, `libhighs.a`, and resulting binary before a run is reviewed.

## Required interpretation

A pass establishes a single original-seed full-CG equivalence case. The already-passed restricted V5 fixture separately exercises a real 1e-9 witness rejection and fresh 1e-10 retry. This diagnostic does not establish the full fitter, every cell, basis reuse, serving latency, held behavior, or production readiness.

Python uses NumPy `a @ weights`; CG V5 uses ordered scalar products. The oracle records threshold and 256/257 cutoff margins. Any restricted-matrix trajectory difference is a failure; no pricing, witness, infinity, or feasibility tolerance may be changed to obtain a pass.
