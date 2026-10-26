# Attention mass constraint-generation controller V3

This is an unapplied, solver-neutral controller draft. It preserves the frozen
Python policy: deterministic 512-row seed, stable full-row violation ordering,
at most 256 newly selected rows per round, 12 rounds, one exact witness retry
from `1e-9` to `1e-10`, omitted full-coordinate duals filled with zero, and the
reviewed V4 original-coordinate witness.

V3 adds control and ownership closure around that policy:

- cancellation is checked before the initial full input scan;
- finite-input, maximum-absolute-value, and restricted-row gathering scans are
  deadline/cancellation aware;
- all internal deadline crossings return `deadline_exhausted`, while explicit
  cancellation remains a caller error and solver terminal statuses remain
  distinct;
- accepted and failed/retried solver allocations have one owner and tests use
  `std.testing.allocator` to detect leaks;
- geometry is explicitly bounded at 4096 full rows and 512 weight columns,
  covering the frozen 3000-row and 288/352-column profiles. This also bounds
  the reviewed LP builder, witness audit, and candidate sort work that cannot
  call the control callback internally.

V3 also uses Zig 0.17 typed `@splat` arrays and const-correct allocation bindings; this is a compile-hygiene correction only.

The callback is still injected. No HiGHS or other solver is integrated, no
native parity is claimed, and the draft is outside canonical runtime sources.

Root-coordinated test command:

```sh
/opt/homebrew/Cellar/zig/0.17.0/bin/zig test -OReleaseSafe \
  --dep attention_mass_lp \
  -Mroot=docs/design/inference/native-port/attention_mass_constraint_generation_v3.zig \
  -Mattention_mass_lp=/private/tmp/attention_mass_lp_v4.zig \
  --cache-dir .zig-cache --global-cache-dir .zig-cache/global
```
