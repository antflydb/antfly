# Optimized ARM64 VOPR fiber switch

Base: `3f429d9352`. The broad native transaction gate crashed in
`distributed txn abort fanout preserves durable ordering contact evidence and bounded recovery`.
The already-built production test fails when run alone. Standalone VOPR
ReleaseSafe reproduced 21 crashes out of 167 tests; the existing one-test stack
unwinding regression also reproduces the invalid stack-length access.

The original assembly keeps the task pointer in x30 across an inline switch.
An initial noinline wrapper still crashed: its descriptor arrived in x0 while
assembly read x1 without an intervening move. That failed attempt is retained.
The pinned standard-library assembly describes x1 as a separate input/output
and also as a clobber. The local ARM64 shim ties the input to the output and
omits that duplicate clobber, behind a noinline C calling-convention boundary.
The final disassembly shows the descriptor move and register save/restore.
Both kernel switch directions use it; stack guards and stack size are unchanged.
Other architectures retain the standard-library switch behind the boundary.

Final qualification:

- Native macOS ARM64: `zig build test -Doptimize=ReleaseSafe --cache-dir
  /private/tmp/antfly-vopr-lifetime-native-cache --summary all` from `zig/lib/vopr`:
  exit 0, 167/167 tests, 3/3 steps.
- Pinned Nix/glibc ARM64 Linux: exact Docker argv, source hash and exit-0 result
  in `antfly-vopr-fiber-boundary-tied-linux.json.gz`: 167/167 tests, 3/3 steps.
- Existing narrow unwinding regression: exit 0, 1/1; original source exits 1
  with ABRT in the same regression.

This qualifies the VOPR task kernel and simulator, including existing cancellation,
group join, future, futex and stack unwinding cases. It does not yet qualify the
new production transaction binary, full API archives, mixed-load resource
bounds, or defaults. Broad qualification must run with this newer source.
