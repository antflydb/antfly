# CUDA TurboQuant L4 Validation History

> Relocated verbatim from `zig/pkg/inference/CUDA.md` (lines 795–826 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`CUDA.md`](../../../../zig/pkg/inference/CUDA.md). Durable decisions from this log were folded into that document before the move.

## History and Evidence

Status checked on 2026-06-21 on an NVIDIA L4 (`sm_89`) with CUDA Toolkit 13.2
and driver R580:

- `zig build -Dcuda=true`, `regen-cuda-artifacts.sh --check --all`,
  `antfly-inference cuda-info --smoke`, and `zig build test -Dcuda=true` pass.
- `polar4` stays fully resident on CUDA with zero host attention fallback in
  the E2B and 12B Q4 checks below.
- `turbo3` is functional and resident, but slower than `polar4` on the L4
  decode workloads tested.
- Deterministic 12B Q4 f32/polar4 output matched in a 32-token raw check, but
  E2B f32/polar4 output diverged.

Measured L4 results from `/tmp/antfly-cuda-turboquant-prod`:

| Workload | Cache | Tokens | Load | Warm TTFT | Cold TTFT | Decode tok/s | CUDA KV status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| E2B Korean summary | f32 | 128 | 8.56s | 0.30s | 8.86s | 17.11 | 4480/4480 device KV successes |
| E2B Korean summary | polar4 | 128 | 8.43s | 0.30s | 8.73s | 16.70 | 1920 compressed-V writes, 4480 reads |
| 12B Q4 Korean summary | f32 | 40 | 17.01s | 2.01s | 19.02s | 8.67 | 1968/1968 device KV successes |
| 12B Q4 Korean summary | polar4 | 30 | 17.10s | 1.98s | 19.08s | 8.66 | 1488 compressed-V writes, 1488 reads |
| 12B Q4 raw repeat 1 | polar4 | 32 | 17.09s | 0.63s | 17.71s | 9.16 | zero fallback |
| 12B Q4 raw repeat 2 | polar4 | 32 | 17.02s | 0.63s | 17.65s | 9.06 | zero fallback |
| 12B Q4 raw repeat 3 | polar4 | 32 | 16.82s | 0.63s | 17.45s | 9.04 | zero fallback |

The measured win at this stage was memory residency and lower metadata
overhead, not higher tok/s. The block-table upload cache reduced E2B
16-token `polar4` block-table uploads to 30, and the longer 128-token E2B
`polar4` stress run used 135 uploads while completing 4480 device-KV reads
with zero fallback.
