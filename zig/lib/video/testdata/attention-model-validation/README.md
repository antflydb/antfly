# Portable CPU vision attention qualification

Native SDPA dispatches independent head slabs on the caller's existing Io owner,
with at most eight lanes, bounded by the cached CPU affinity/cgroup budget.
Sequences shorter than 256 tokens and callers without Io remain serial. Scratch
is allocated before launch and isolated per lane (at most about 512 KiB).
Workers check cancellation at query/key tile boundaries; dispatch drains before
buffers are freed. No new pool or mandatory dependency is introduced.

ARM64 transposed GEMM uses portable Zig four-wide SIMD and a 4x4 register tile.
Ordinary GEMM and other architecture dispatch retain their existing policy.
Paired microbenchmarks compare candidates with the previous eight-wide 6x2
baseline immediately before and after each candidate. The selected tile's median
speedups across five shapes are 1.04–1.36 on macOS and 1.08–1.39 on actual Linux
ARM64. These measurements motivate the tile; emitted register spills were not
audited. The independent F64 tests cover f32/f16 RHS, negative alpha, partial row
slices and M/N/K tails with an absolute tolerance of 2e-5.

## Model and lifecycle measurements

The 53-case linalg suite reports 50 passes / three platform skips on macOS ARM64
and 52 passes / one skip on Linux ARM64. Exact serial/parallel attention tests
cover three bias layouts, sparse and all-masked batches, and a zero-worker
scheduler. Allocation failure rollback, cancellation drain and correct retry
pass on both hosts. Both registered benchmark build targets run successfully.

The old published CPU binary and the new binary ran the same 52-second,
1,253-picture Sintel derivative sequentially after compilation completed.
Both preserve the pinned PyTorch CPU vectors/scores and pass reset/retry,
half-close and four-request concurrency-two load checks. Every request must
return a correct vector, release admission and reconcile server completion
counts. Neither memory/admission limits nor numerical gates were raised.

| macOS native CPU | Published baseline | Optimized |
|---|---:|---:|
| Completed video HTTP latency | 60.96 s | 40.17 s |
| Four-request batch wall time | 147.53 s | 86.40 s |
| Sampled peak process-tree RSS | 1,978,286,080 B | 1,825,210,368 B |

This is a 34% clip-latency and 41% batch-time reduction in this small campaign.
One measured video case and four load requests on a shared host establish no
confidence interval, normalized production throughput or general speedup.
RSS sampling can miss peaks and double-count shared mappings. The single-video
ranking is trivial; the separate GBR corpus supplies multiple alternatives.

Actual Linux ARM64 passes the same four-request load gate under the unchanged
6 GiB container limit and 180-second per-request budgets: all four vectors are
correct, request durations are 118–128 seconds, final active/admitted counts are
zero, and four acknowledged responses match four server completions. Its
246-second batch includes executor queueing. The earlier failed Linux campaign
in `concurrent-model-validation` is retained; this is a fresh passing rerun,
not relabelling historical evidence. Linux uses the recorded local image,
no external network and read-only source/checkpoint/server mounts.

Fresh Metal real-clip regression checks also pass reference preservation,
reset/retry, half-close and four concurrent request completions. Its single clip
time (18.26 seconds) is a diagnostic from a run overlapping Linux qualification,
not a comparable Metal performance measurement. The macOS CPU expanded-format
corpus on both macOS and actual Linux ARM64 preserves all 12 odd/even-sized 8/10/12-bit AV1/VP9 identity-GBR clip
vectors, two query vectors and rankings, with correct lifecycle/load checks.

## Reproduction

With Zig 0.17.0, from `zig/pkg/inference`:

```sh
zig build test-linalg -Doptimize=ReleaseSafe
zig build bench-linalg-transb -Doptimize=ReleaseFast
zig build bench-linalg-attention -Doptimize=ReleaseFast
zig build bench-server -Doptimize=ReleaseFast --prefix /tmp/attention
```

For the Linux server add `-Dtarget=aarch64-linux-musl`, then execute it in the
recorded ARM64 image. From the worktree, with the pinned checkpoint:

```sh
python tools/embeddinggemma2/video_compare.py --backend native --binary /tmp/attention/bin/antfly-inference-bench-server --model-dir .benchmark-assets/native-video/model --corpus tools/embeddinggemma2/testdata/retrieval-long/corpus.json --reference zig/lib/video/testdata/retrieval-validation/long-pytorch-cpu.json --warmups 0 --iterations 1 --concurrency 2 --sustained-rounds 4 --request-timeout 600 --sustained-request-timeout 180 --output /tmp/attention-native.json
```

Use `metal` for the Metal regression check. Expanded-depth identity-GBR checks
use `tools/embeddinggemma2/testdata/gbr-formats/corpus.json` and
`zig/lib/video/testdata/gbr-model-validation/pytorch-cpu.json`, with both request
budgets 180 seconds and one sustained round. Checkpoints/executables are not
published. [qualification.json](qualification.json) binds production and benchmark
sources, executable identities and raw results. Final direct attention samples
overlapped other shared-host work and are diagnostic; the real-clip before/after
campaign ran sequentially. Linux compares against the pinned macOS PyTorch oracle.

Broader codec/conformance/fuzz, display/layer/field parity, sustained load,
unsafe-driver hard-stop recovery, transfer telemetry and Linux x86_64 runtime
remain open. The complete non-CUDA objective remains active.
