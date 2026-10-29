# Tile kernels (prototype)

Describe a kernel once as comptime Zig data and lower it to every backend:

| Lowering | How | Output |
| --- | --- | --- |
| CPU | `cpu.zig` walks the description at comptime and emits `@Vector` code | compiled in, no text |
| Metal | `gpu.zig` renders MSL at comptime | `src/ops/metal/generated/tile_*.metal` (+ `.msl.h` C string) |
| CUDA | `gpu.zig` renders CUDA C++ at comptime | `src/ops/cuda/generated/tile_*.cu` |
| Oracle | `tile.reference`, scalar, f64 sums | tests only |

The only primitive so far is `RowReduce`: one f32 row, per-element masks
(`Expr`: index/param comparisons, boolean logic, "not in list"), and reduction
channels (`argmax` with lowest-index ties, online `logsumexp`). Masks shared by
several channels are `lets`, evaluated once per element. GPU lowerings use one
schedule: a strided per-thread pass, a SIMD-group/warp shuffle tree, a
shared-memory stage across SIMD-groups, and one partial record per block.

## Whisper logits statistics

`whisper_logits.zig` is the first kernel: the per-step statistics Whisper's
decoder needs (best allowed / text / timestamp / raw argmax, and log-sum-exp
over allowed, timestamp and raw logits). It keeps the ABI and record layout of
the hand-written `termite_whisper_logits_partials`, so on Metal it is a
drop-in replacement for that pipeline:

```sh
ANTFLY_TILE_WHISPER_LOGITS=1 ./antfly ...   # uses the generated partials kernel
```

The generated kernel is compiled into its own Metal library at startup; if
that fails the runtime logs it and keeps the hand-written kernel. The
hand-written reduce kernel (which also picks the token and advances the
timestamp grammar on device) is unchanged. The CUDA source (partials plus a
generic combine kernel) is generated but not yet wired into the CUDA runtime.

## Commands

```sh
zig build test-tile-kernels                  # lowerings vs. the scalar oracle
zig build tile-kernel-codegen                # checked-in sources are fresh
zig build tile-kernel-codegen -- --write     # regenerate them
```

`whisper_timestamps.zig` also checks the CPU lowering against
`hostLogitsStats`, the existing host oracle for this kernel.

## Notes

- `@exp` on a `@Vector` lowers to one libm call per lane, so `cpu.zig` has its
  own polynomial `vexp`. With it the 16-wide CPU kernel runs a 51,865-logit
  step in ~120 us versus ~890 us for `hostLogitsStats` (x86-64, AVX-512).
- `logsumexp` here skips `-inf` inputs. The hand-written Metal kernel and
  `hostLogitsStats` turn a leading `-inf` into a NaN sum (`exp(-inf - -inf)`).
- The suppress-list check is a linear scan per element on every backend, as
  in the hand-written kernel; a bitmap lowering would remove most of its cost.
