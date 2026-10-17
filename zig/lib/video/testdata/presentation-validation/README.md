# WebM presentation qualification

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

This phase qualifies crop/display/Colour handling through CPU and Metal model
preparation, with portable execution on actual Linux ARM64. It does not complete
the full non-CUDA goal. `qualification.json` pins changed sources, consumed
binaries, fixtures and each report. Earlier v5 receipts retain their original
scope; this phase uses `embeddinggemma2-f32-mean-v6-webm-presentation`.

The three synthetic cases cover VP9/AV1 odd cropping, aspect ratio and opposite
quarter turns, plus VP8 explicit BT.709 full-range Colour. The generator changes
container metadata while preserving compressed Cluster payload hashes. The
PyTorch oracle decodes unmodified source fixtures independently, then applies
explicit RGB conversion/presentation and pinned HF geometry. This is a declared
processor extension, not a claim about upstream automatic metadata handling.
All native backends match vector and prompt-token gates.

Reproduce fixtures with `python3 zig/lib/video/scripts/generate_webm_presentation_fixtures.py`.
Run `zig/pkg/inference/scripts/embeddinggemma2/video_webm_presentation_qualification.py` with
`--backend cpu`, pinned `--model-dir` and `--output`; native/Metal also require
`--binary` and `--reference`. Checkpoints remain local and are not committed.
Container/video/model test commands are `zig build test-media`,
`zig build test-video`, and `zig build test-video-model` in their respective
packages. Linux binaries use `aarch64-linux-musl` and ReleaseSafe. Linux native
model qualification used a pinned offline image, a read-only root and only
staged task inputs after approval review rejected mounting the full worktree.

Logs have trailing empty lines removed. Reports are correctness evidence;
server memory samples under shared host activity are not capacity/performance
qualification. Broader streams, HDR/native-depth model qualification,
scheduling, sustained benchmarks and live protocol coverage remain active.
