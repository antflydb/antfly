# Native chroma reconstruction and pretrained qualification

Native Zig SDR chroma reconstruction now preserves centered sample positions,
fixed-point cubic coefficients, border folding and reference RGB quantization.
CPU filters U/V together with portable SIMD; Metal reconstructs chroma on the
device and reuses uploaded coefficients across matching geometry. Header-only
AV1/VP9 picture metadata carries depth/chroma, so model preflight admits the
correct reconstruction scratch before execution.

The independent 108-case RGB oracle covers odd/even geometries, 4:2:0/4:2:2,
8/10/12-bit samples, three matrices and both ranges. BT.2020 primitive arithmetic
is tested; the existing SDR BT.2020 display rejection remains. Scalar/SIMD/Metal
checks include strides, rotation, coefficient reuse and allocation cleanup.
macOS ReleaseSafe passes 250 video checks (two skips) and 50 model checks.
Actual Linux ARM64 passes 222 video checks (29 skips); that binary predates the
additional allocation-failure test, which passes in the final macOS suite.

All nine expanded AV1 video vectors and both queries now pass the unchanged
pretrained parity gates on macOS native CPU, Metal and actual Linux ARM64 native
CPU. Both query rankings are exact, with top-1/5/10 overlap 1.0. This supersedes
the earlier four-format numerical failures. Linux uses the pinned PyTorch CPU
reference generated on macOS; it does not establish fresh Linux PyTorch parity.
The corpus consists of generated clips and measures reference preservation,
not end-user retrieval usefulness.

Metal completes all numerical comparisons, then its cancellation/retry request
is capacity-denied (retryable HTTP 503) under shared memory admission. Its overall
stress report remains failed; no admission gate is disabled. CPU serial sustained
and disconnect/retry checks complete, but peer disconnect alone does not prove
cancellation completion. Broader display/layer policies, mixed real-world streams,
full-pipeline load/performance and the remaining non-CUDA scope remain active.

Reproduce the pixel oracle with `scripts/generate_rgb_reconstructed_fixture.py`.
Reproduce inference with `tools/embeddinggemma2/video_compare.py`, the maintained
`tools/embeddinggemma2/testdata/av1-formats/corpus.json`, pinned checkpoint,
`--reference zig/lib/video/testdata/av1-model-validation/pytorch-cpu.json`,
`--warmups 0 --iterations 1 --threads 4 --concurrency 1 --sustained-rounds 1`
and `--process-memory-budget-mb 4096`. The Linux run uses the static
`aarch64-linux-musl` server inside the pinned Python image, network disabled,
source/checkpoint read-only. No checkpoint or executable is distributed.
`qualification.json` binds source, fixtures, reports and executable hashes.
