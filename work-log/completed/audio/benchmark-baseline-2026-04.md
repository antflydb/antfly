# Audio Local Benchmark Baseline (2026-04)

> Relocated verbatim from `zig/lib/audio/AUDIO.md` (lines 167–194 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`AUDIO.md`](../../../zig/lib/audio/AUDIO.md). Durable decisions from this log were folded into that document before the move.

### Local Benchmark Baseline

This is a local single-run baseline for future codec performance work. Compare
new runs on the same machine/toolchain before drawing conclusions.

- Date: 2026-04-14 15:23 PDT, updated after fused AAC/Vorbis
  window/overlap output.
- Commit: `dc9da56` plus current AAC/Vorbis working-tree performance changes.
- Host: Darwin 24.6.0 arm64.
- Zig: `0.16.0`.
- Command:
  `zig build -Doptimize=ReleaseFast bench-audio -- --bench all --warmup-iters 2 --measure-iters 20`

| Benchmark | Iterations | Total ms | ns/iter | MiB/s |
| --- | ---: | ---: | ---: | ---: |
| `mp3_decode` | 20 | 33.550 | 1,677,500 | 5.10 |
| `vorbis_decode` | 20 | 38.695 | 1,934,750 | 2.55 |
| `opus_decode` | 20 | 69.559 | 3,477,950 | 4.16 |
| `aac_adts_decode` | 20 | 45.755 | 2,287,750 | 4.54 |
| `aac_decode` | 20 | 45.749 | 2,287,450 | 4.86 |
| `mp3_synth` | 20 | 0.034 | 1,700 | 5,170.04 |

AAC per-iteration counters from the same run:

| Benchmark | Spectral parse ms | Spectral decode ms | Tools ms | Filterbank ms | IMDCT ms | Window ms | Overlap ms | Access units | Channel decodes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `aac_adts_decode` | 0.114 | 0.206 | 0.379 | 1.297 | 1.201 | 0.001 | 0.091 | 0 | 34 |
| `aac_decode` | 0.111 | 0.196 | 0.371 | 1.303 | 1.206 | 0.002 | 0.090 | 17 | 34 |
