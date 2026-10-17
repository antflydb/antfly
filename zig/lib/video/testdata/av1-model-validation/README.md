# Expanded AV1 pretrained failures, macOS ARM64

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

The reports qualify the production decoder/model path at runtime source commit
`8f75e63df4d9e7eb834949bbc46047c6b10b4e39`, built in fresh ReleaseSafe caches
(`server-build.log`). `qualification.json` binds the binary, tools, corpus and
reports with SHA-256 hashes. Checkpoint identity and pinned PyTorch source
provenance are embedded in the inference reports. No checkpoint is included.

All nine generated AV1 format videos fail the existing vector gate against
PyTorch CPU. Native CPU and Metal agree within 1.535e-6 maximum coordinate error;
query vectors pass the existing 1e-4 gate. Both query rankings change, including
non-tied inversions and zero top-1 overlap. Maximum cosine-score differences are
about 0.008009 and 0.006619. Top-5 overlap is 1.0 and 0.8 respectively. Full
vectors, scores and rankings are retained; these are preservation measurements,
not model relevance judgments.

The independently verified plane hashes in
`zig/pkg/inference/scripts/embeddinggemma2/testdata/av1-formats/formats.json` match pinned AOM goldens.
The failure therefore requires preparation-policy investigation. FFmpeg repeats
unexpanded gray samples in the monochrome 8-bit RGB path; native preparation
expands coded limited range. Broader high-depth/chroma conversion differences
are not yet explained or fixed. No threshold was relaxed.

Reproduce from the repository root with the pinned checkpoint and fresh server:

```sh
python3 zig/pkg/inference/scripts/embeddinggemma2/generate_av1_model_corpus.py \
  --output zig/pkg/inference/scripts/embeddinggemma2/testdata/av1-formats
python zig/pkg/inference/scripts/embeddinggemma2/video_compare.py --backend cpu \
  --corpus zig/pkg/inference/scripts/embeddinggemma2/testdata/av1-formats/corpus.json \
  --model-dir .benchmark-assets/native-video/model \
  --output /tmp/av1-reference.json --warmups 0 --iterations 1 \
  --threads 4 --concurrency 1 --sustained-rounds 1
python zig/pkg/inference/scripts/embeddinggemma2/video_compare.py --backend native \
  --corpus zig/pkg/inference/scripts/embeddinggemma2/testdata/av1-formats/corpus.json \
  --model-dir .benchmark-assets/native-video/model \
  --binary zig/pkg/inference/zig-out/bin/antfly-inference-bench-server \
  --reference /tmp/av1-reference.json --output /tmp/av1-native.json \
  --warmups 0 --iterations 1 --threads 4 --concurrency 1 --sustained-rounds 1
```

Repeat the final command with `--backend metal` and a distinct output. Both
native commands intentionally return failure while publishing complete reports.
The harness computes retrieval comparisons before asserting vector gates.

CPU/Metal runs overlapped on a shared host; timings and memory are diagnostic.
No Linux, scalable-layer, Annex B model-input or production-throughput claim is
made. The active goal still requires fixing and verifying expanded-format model
parity and all other remaining non-CUDA design requirements.
