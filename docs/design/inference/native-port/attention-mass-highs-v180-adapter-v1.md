# HiGHS v1.8.0 mass-CG adapter draft

This is an unapplied adapter design for the solver-neutral mass constraint-generation controller. It is not imported by Antfly, does not link HiGHS, and does not authorize a solver build or model fit.

## Pinned interface

The proposed backend is official HiGHS tag `v1.8.0`, commit `fcfb5341462f8a7db5ef5038613413f97d6cac3d`, under the MIT license. The exact tagged sources used for the design review are:

- `src/interfaces/highs_c_api.h`
- `src/lp_data/HighsCallbackStruct.h`
- `src/lp_data/HighsCallback.cpp`
- `src/simplex/HEkk.cpp`
- `src/lp_data/HighsOptions.h`
- `src/util/HighsInt.h`

The v1.8.0 API provides `Highs_create`, `Highs_destroy`, `Highs_passLp`, `Highs_run`, solution/status/info accessors, typed option setters, and the simplex interrupt callback. Default `HIGHSINT64=OFF` makes `HighsInt` a C `int`; the adapter additionally checks `Highs_getSizeofHighsInt()==4` before passing a model.

The full source audit and official URLs are recorded in `/private/tmp/attention-matching-native-highs-v180-c-abi-integration-readiness-v2.json` (SHA-256 `a4b23dbf7e04525dd7f4a3ea07ee06d92ed4fbddd162d7c8fb0aac2de0baef91`).

## Frozen numerical mapping

`attention_mass_highs_v180_adapter_v1.zig` implements the exact `SolveRestrictedFn` expected by `attention_mass_constraint_generation_v5.zig`. It translates the reviewed dense `attention_mass_lp_v4.Problem` into row-wise CSR without zero elision or coefficient reordering. HiGHS raw row duals are returned unchanged. The controller remains solely responsible for `alpha=-upper_dual` and `beta=-lower_dual` and for the full original-coordinate witness.

The adapter sets only the SciPy 1.16.3 `highs-ds` options material to the frozen recipe:

- `solver=simplex`
- `presolve=on`
- `simplex_strategy=1` (serial dual simplex)
- requested primal and dual feasibility tolerance, restricted to `1e-9` or `1e-10`
- `highs_debug_level=0`
- `output_flag=false`
- `log_to_console=false`
- `time_limit` recomputed from the absolute deadline immediately before `Highs_run`

It deliberately does not set threads, parallelism, random seed, edge weights, scaling, crash strategy, update limits, or a new iteration limit. A bounded build using `HIGHS_NO_DEFAULT_THREADS=ON` is plausible but remains a build-level solver-trajectory variable requiring matched parity evidence.

## Lifecycle and cancellation

A newly created HiGHS instance is used for each solve attempt, so the first `1e-9` attempt and the controller's conditional `1e-10` witness retry do not share HiGHS state or cumulative clocks. `Highs_destroy` is deferred immediately after creation. Every Zig allocation has an error cleanup path, and only optimal output arrays transfer to `RestrictedSolve`.

The callback is allocation-free and cannot unwind through C. It records the exact Zig control error in callback state and sets `user_interrupt=1`; the adapter returns that error after `Highs_run` and cleanup. It also sets the HiGHS time limit and checks control/deadline before assembly, periodically during dense-to-CSR construction, before `passLp`, before `run`, and before accepting an optimal solution. The official simplex callback is a simplex bailout checkpoint and does not promise interruption during presolve or arbitrary solver setup.

## Fixture tests

The module contains an injected exact-shape C API table. Its tests require no HiGHS library and cover:

1. exact row-wise dimensions, starts, coefficient order, options, primal extraction, and unchanged negative row duals;
2. terminal time-limit mapping with owned empty output slices;
3. simplex callback cancellation and exact control-error propagation after destruction;
4. `passLp` failure cleanup plus an allocation-failure sweep of adapter-owned buffers.

Proposed guarded command, not run:

```sh
cd /Users/timkaye/Documents/af/antfly/.worktrees/codex-gemma4-compaction-production/zig
/opt/homebrew/Cellar/zig/0.17.0/bin/zig test \
  -OReleaseSafe \
  --dep attention_mass_lp --dep attention_mass_cg \
  -Mroot=../docs/design/inference/native-port/attention_mass_highs_v180_adapter_v1.zig \
  -OReleaseSafe --dep attention_mass_lp \
  -Mattention_mass_cg=../docs/design/inference/native-port/attention_mass_constraint_generation_v5.zig \
  -OReleaseSafe -Mattention_mass_lp=/private/tmp/attention_mass_lp_v4.zig \
  --cache-dir /Users/timkaye/Documents/af/antfly/.worktrees/codex-gemma4-compaction-production/zig/.zig-cache \
  --global-cache-dir /Users/timkaye/Documents/af/antfly/.worktrees/codex-gemma4-compaction-production/zig/.zig-cache/global
```

## Limits

No HiGHS source archive or library exists in this worktree. No adapter compilation, real C ABI call, optimizer execution, native/SciPy parity, model load, or fitting occurred. A later experiment must bind a complete source-tree receipt, generated `HConfig.h`, compiler flags, static-library and header hashes, architecture, license, and exact link dependencies. It must prove restricted-LP outputs, row-dual signs, full witness results, retry behavior, cancellation, cleanup, memory, and timing before any canonical runtime integration.
