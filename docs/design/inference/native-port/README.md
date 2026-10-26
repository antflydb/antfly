# Attention mass constraint-generation native-port draft

`attention_mass_constraint_generation.zig` is an unapplied, solver-neutral
controller draft. It imports the reviewed mass-LP V4 domain implementation as
the named module `attention_mass_lp`; it does not select or link an optimizer.

A guarded reviewer may compile and run the embedded synthetic tests with the
pinned Zig toolchain and V4 artifact using the worktree-local cache:

```sh
cd /Users/timkaye/Documents/af/antfly/.worktrees/codex-gemma4-compaction-production
/opt/homebrew/Cellar/zig/0.17.0/bin/zig test -OReleaseSafe \
  --dep attention_mass_lp \
  -Mroot=docs/design/inference/native-port/attention_mass_constraint_generation.zig \
  -Mattention_mass_lp=/private/tmp/attention_mass_lp_v4.zig \
  --cache-dir .zig-cache --global-cache-dir .zig-cache/global
```

This command has not been run as part of the draft. The controller preserves
512 seed rows, 256 additions, 12 rounds, stable violation ordering, full-row
dual zero-fill, original-coordinate witness checks, stored f32-log conversion,
and the current single 1e-10 retry after an exact restricted-witness rejection.
It requires a separately reviewed solver callback. It does not prove HiGHS
parity, mass-recipe parity, all36 parity, resource bounds, serving readiness, or
publication authority.
