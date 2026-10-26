# HiGHS v1.8.0 real restricted-fixture driver V3

This isolated differential experiment does not integrate HiGHS into Antfly, run full constraint generation, or authorize a model, fit, held evaluation, release, or publication.

The driver consumes the frozen 1128 x 288 fixture from `export_attention_mass_highs_restricted_fixture_v1.py`, rebuilds the LP with `attention_mass_lp_v4.buildPrimaryProblem`, calls the reviewed V3 adapter through explicit HiGHS C pointers, and audits returned weights and raw row duals in exported original row coordinates. Only exact `WitnessRejected` at 1e-9 permits one new HiGHS instance at 1e-10. Every other error, status, cancellation, or deadline crossing stops.

## Required library receipt

Schema `antfly.attention-matching.highs-v180-static-library.v2` must be complete and diagnostic-only with authority false. It binds exact tag v1.8.0 and commit fcfb5341462f8a7db5ef5038613413f97d6cac3d; exact generated runtime git hash; absolute byte-sized SHA-256 edges for the source archive, license, C API/callback/generated configuration headers, libhighs.a, symbol inventory, and link inventory; source-tree hash; compiler and CMake identities and hashes; target, architecture, ordered flags; built_static true; highsint64 false; and highs_no_default_threads false.

The driver requires externally reviewed SHA-256 values for the fixture
manifest, library receipt, and executable-link receipt on argv. The library
receipt separately binds the full source commit and exact generated
`runtime_githash`; runtime compares `Highs_githash()` with the latter. The
executable receipt binds the compiler, full link argv, driver and module
sources, library receipt, exact libhighs archive, and final binary. The driver
rehashes every nested edge
before reading the fixture or creating output. It requires linked runtime
version 1.8.0, reviewed runtime git hash, and four-byte HighsInt. Unresolved
paths, hashes, byte counts, or tool identities fail closed.

## Output and limits

Fresh output contains result.json and weights, raw row duals, alpha, and beta for both attempts as f64-le files. JSON separates fixture `expected` from `observed`, recording vector hashes, objective, t, iterations, total adapter wall time, status, passing PrimaryAudit, fixed options, lifecycle counts, and original-oracle byte comparisons.

The audit covers the 1,128 exported restricted rows. The fixture omits the full CG matrix. Solver-internal timing, full fitter parity, persistent models, and basis reuse remain outside scope.

## Prospective bounded commands

No command below has been executed. It becomes runnable only after independent review binds LIBHIGHS_A, LIBRARY_RECEIPT, FIXTURE_MANIFEST, Zig, and all source hashes. Each module gets ReleaseSafe because flags reset at -M boundaries.

~~~sh
cd /Users/timkaye/Documents/af/antfly/.worktrees/codex-gemma4-compaction-production/zig
zig build-exe -OReleaseSafe -fstrip \
  --dep attention_mass_lp --dep attention_mass_cg --dep attention_mass_highs \
  -Mroot=../docs/design/inference/native-port/attention_mass_highs_v180_real_fixture_driver_v3.zig \
  -OReleaseSafe -Mattention_mass_lp=/private/tmp/attention_mass_lp_v4.zig \
  -OReleaseSafe --dep attention_mass_lp \
  -Mattention_mass_cg=../docs/design/inference/native-port/attention_mass_constraint_generation_v5.zig \
  -OReleaseSafe --dep attention_mass_lp --dep attention_mass_cg \
  -Mattention_mass_highs=../docs/design/inference/native-port/attention_mass_highs_v180_adapter_v3.zig \
  "$LIBHIGHS_A" -lc++ --cache-dir .zig-cache --global-cache-dir .zig-cache \
  -femit-bin=/private/tmp/attention-mass-highs-v180-real-fixture-driver-v3
~~~

The exact build must use the repository bounded-build policy with a reviewed compiler allowance. The run must use a fresh process group under the established guard: 1 GiB process-group RSS, zero swap growth, 600 second outer timeout, driver deadline at most 560 seconds, free-disk gate, and full child drain.

~~~text
/private/tmp/attention-mass-highs-v180-real-fixture-driver-v3
--fixture-manifest <absolute reviewed fixture>/manifest.json
--fixture-sha256 <reviewed manifest SHA-256>
--library-receipt <absolute reviewed library receipt>
--library-receipt-sha256 <reviewed receipt SHA-256>
--executable-receipt <absolute reviewed executable receipt>
--executable-receipt-sha256 <reviewed executable receipt SHA-256>
--output-dir <fresh absolute output>
--deadline-ms 560000
~~~

The first parity build must leave upstream `HIGHS_NO_DEFAULT_THREADS=OFF`.
A no-default-threads build is a separate variant with a separate receipt and
output; it cannot replace the first matched differential.
