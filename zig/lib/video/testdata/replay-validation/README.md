# Codec-key replay and RGB24 row qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Codec-verified restart points preserve logical IDs and raw samples while skipping
unneeded prefix reconstruction. Six full-length 480p/1080p excerpts compare 32
selected final-quarter pictures twice on macOS and actual Linux ARM64. Every
selected ordinal, time, geometry, depth/chroma, colour and sample hash agrees.
For the 52-second clips, the final-quarter replay decodes 341 instead of 1,253
packets. Raw timings are diagnostic shared-host measurements, not capacity.

Four independently selected late VP9/AV1 windows at 36 and 44 seconds expose
an RGB24 rounding gap in the previous profile. Failed before-fix reports retain
that evidence. An independent Q16 integer implementation matches reference RGB
bytes for nonaligned rows, with CPU and Metal using the same equations. Recipe
v5 fingerprints the changed policy. The unchanged pinned PyTorch oracle now
matches all six late-window vectors and two exact rankings/top-k sets on CPU,
Metal and actual Linux ARM64, including four concurrent requests per backend.
Ten existing sampling vectors and eight requests each also pass on macOS CPU,
Metal and actual Linux ARM64. The current metadata campaign passes 101,920 runs.

Source clip attribution/pixel oracles are in `../benchmarks/webm`. The model
corpus is `zig/pkg/inference/scripts/embeddinggemma2/testdata/replay/corpus.json`; weights stay outside
these receipts. `webm-benchmark INPUT replay_compare 2` reproduces library checks.
Gate logs trim terminal blank lines. Each receipt records source/artifact/binary
hashes. This does not close broader codec, SIMD/scheduling, display or live scope.
