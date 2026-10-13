# Combined 4:4:4 color/profile qualification

The explicit native-depth combined Q13 RGB policy corrects software WebM 4:4:4
SDR preprocessing on CPU and Metal. All three 8/10/12-bit 4:4:4 pretrained vectors
pass the unchanged reference gate (maximum coordinate error below 3.1e-7).
The two monochrome vectors and both queries also retain parity. Four video cases
still fail: 12-bit 4:2:0 and 8/10/12-bit 4:2:2. Full-corpus rankings remain failing;
this phase does not establish complete pretrained format preservation.

The maintained 54-case offline FFmpeg RGB oracle covers three geometries, three
depths, three matrices and both ranges. The explicit compatibility profile also
preserves the reference's signed Q22 output domain for out-of-nominal limited
codes. It does not change ordinary float rendering. Primitive BT.2020 arithmetic
is characterized, while the current SDR BT.2020 display rejection remains
explicitly tested. Supported Metal/scalar/SIMD color paths compare depth, range,
rotation, strides and tails. Every ownership variant now refills producer pixels;
a previous overwrite no longer turns subsequent variants into all-zero inputs.

ReleaseSafe macOS passes 247 video checks (two skips) and 49 model checks.
Actual Linux ARM64 passes 221 video checks (28 skips); final targeted portable
SIMD and display-rejection execution passes as well. The historical failed oracle
and unsupported-policy runs remain in the logs. Pretrained results are macOS ARM64,
not expanded-format Linux model qualification.

A maintained full Sintel AV1 High 4:4:4 derivative reproduces its coded bytes and
all 1,253 independent normalized picture hashes. Every native decoded picture
matches. The source is pinned 4:2:0 material, adding no chroma detail; its CC BY 3.0
attribution and generation/probe identities are in
`../benchmarks/webm/444-corpus/README.md` and `manifest.json`.

The completed preparation benchmark handles all 1,253 decoded pictures and
prepares 53 at 384×384. Scalar/SIMD patch hashes agree exactly. Four warm samples
have preparation medians of about 535 ms scalar and 348 ms SIMD. Two warm Metal
samples record about 85 ms completed preparation, about 11.6 ms GPU execution,
65,177,280 upload bytes, zero warm coefficient uploads, 3,653,632 device allocation
bytes and a 34,816,000-byte process RSS peak. Full model inference is excluded;
scalar and SIMD phases ran sequentially on a shared host, so these diagnostic
samples do not establish a production/full-pipeline speedup.

Both final pretrained reports use an explicit 4 GiB process envelope, retaining
physical-memory admission and unchanged numerical gates. CPU disconnect/retry
and nine serial sustained requests complete; the HTTP peer disconnect does not
prove cancellation completion. Metal completes every corpus vector, then its
retry request is capacity-denied. Complete vector/ranking evidence is now recorded
before stress checks, and the report remains failed. That Metal stress failure is
not repaired or hidden by the numerical correction.

Reproduce RGB generation with `scripts/generate_rgb_combined_fixture.py`, the
real workload with `scripts/generate_av1_444_benchmark.py`, and picture qualification
with `scripts/qualify_webm_benchmarks.py`. Use the pretrained commands from
`../av1-model-validation/README.md`, adding `--process-memory-budget-mb 4096` for
both owned servers. `qualification.json` binds current source, binaries, fixtures,
reports and derived measurements. No checkpoint is distributed.

Filtered chroma reconstruction, full pretrained CPU/Metal/Linux preservation,
layer/wide-gamut policies, complete cancellation/admission/load qualification and
all other active non-CUDA design requirements remain required.
