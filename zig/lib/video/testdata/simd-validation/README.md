# Portable integer RGB SIMD qualification

Eight-pixel Zig vectors implement the existing Q13/Q16 integer profiles with
scalar tails. Rotation, HDR, higher depth and floating conversion retain scalar
handling. The scalar entry point remains available as the qualification oracle.
Exact patch comparisons exercise small/odd widths, padded strides, all chroma
layouts and ranges. Vector RGB also matches independent Q16 byte fixtures.

Matched real-clip ABBA runs use one frozen binary and identical decoding,
geometry, samples and resize/patch code. Every prepared F32 byte hashes equally
between modes. Four warm samples per mode show preparation medians reduced by
26.9% at 480p and 29.0% at 1080p on this host; decode-plus-preparation improves
about 1.0% and 1.6%. Timings are diagnostic, background load is uncontrolled, and
there is no production-capacity or cross-architecture speedup claim. No owned
build/test process overlaps these timed runs. SHA verification is outside the
preparation interval and inside completed time.

Reproduce with `zig build install-webm-benchmark -Doptimize=ReleaseSafe
-Dvideo-use-llvm=true`, then run each VP8 corpus input with `cpu_scalar 3 384 24`
and `cpu 3 384 24` in scalar/SIMD/SIMD/scalar order. Inputs and attribution live
in `../benchmarks/webm/corpus`; prior decode/reference-sharing qualification
remains separately pinned. Model resource/lifetime gates pass 24 tests. Producer abandonment exposed a
VP9 view-getter cancellation defect; the two getters are now pure accessors,
with a discovered deterministic regression that fails before and passes after
the fix. The timed frozen binary precedes this cancellation-only follow-up;
its normal-control preparation comparison is independently pinned. Final
combined gates pass 176 macOS tests (two skips), 152 actual Linux ARM64 tests
(26 skips), and all 24 model lifecycle tests. The first focused getter run
contained only import gates; the explicitly discovered regression is the one
recorded in the failing/passing before/after evidence.
