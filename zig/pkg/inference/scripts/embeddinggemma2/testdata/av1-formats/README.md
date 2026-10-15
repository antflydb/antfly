# Expanded AV1 pretrained preservation corpus

Nine generated, two-second, 127×95 clips exercise monochrome 8/12-bit,
4:2:0 12-bit, and 4:2:2/4:4:4 8/10/12-bit AV1. Their pinned elementary
sources and independent AOM plane goldens are in
`zig/lib/video/testdata/av1-extended`. No application relevance labels are supplied.

Regenerate from the repository root:

```sh
python3 zig/pkg/inference/scripts/embeddinggemma2/generate_av1_model_corpus.py \
  --output zig/pkg/inference/scripts/embeddinggemma2/testdata/av1-formats
```

The offline generator writes deterministic WebM without changing coded temporal
units, checks every packet hash through FFprobe, verifies geometry/depth/chroma
and displayed counts, and compares FFmpeg's complete decoded planes with pinned
AOM frame hashes. `formats.json` retains those checks. FFmpeg is a qualification
tool; inference retains its pure Zig codec runtime.

Use `zig/pkg/inference/scripts/embeddinggemma2/video_compare.py` with this `corpus.json`, a pinned
checkpoint and backends `cpu`, `native` and `metal`. Supply the CPU report through
`--reference` for the native runs. Vector and score gates remain unchanged.
The reports in `zig/lib/video/testdata/av1-model-validation` record failures:
decoded plane agreement does **not** imply pretrained embedding agreement.

The corpus does not cover Annex B model ingestion, scalable layers, HDR,
rotation, Linux execution or real application quality. Those remain separate
requirements in the active design.

The subsequent gray-sample policy correction is recorded in
`zig/lib/video/testdata/av1-gray-model-validation`: monochrome vectors now pass
on CPU/Metal, while the seven color-format cases and full-corpus rankings remain
unqualified. The earlier failing reports remain historical reproduction evidence.

The subsequent combined RGB correction is in
`zig/lib/video/testdata/av1-combined-model-validation`: all three 4:4:4 vectors
now pass CPU/Metal gates. Four 4:2:0/4:2:2 cases and full rankings remain failing.

The native chroma reconstruction correction in
`zig/lib/video/testdata/av1-chroma-model-validation` supersedes those numerical
failures: all nine video vectors and both query rankings pass on macOS CPU/Metal
and actual Linux ARM64 CPU. The Linux comparison uses the pinned macOS PyTorch
reference. Metal's later capacity-denied retry keeps its stress report failed.
Layer/HDR/rotation and application usefulness remain outside this corpus.
