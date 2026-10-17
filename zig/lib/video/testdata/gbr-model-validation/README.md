# Native identity RGB pretrained preservation

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

Native video preparation now accepts full-range 4:4:4 identity-matrix planes in
GBR order. CPU reorders 8-bit channels with portable SIMD; Metal performs color
conversion on device from native integer textures. Ordinary preparation retains
full-range RGB scaling. The explicit `gbr_reference` model profile also matches
the pinned processor's high-depth RGB24 compatibility arithmetic: a native Q15
RGB-to-Q6 intermediate followed by the existing Q13 output transform. No external
scaler/codec runtime is added. VP9 sRGB metadata now supplies BT.709 primaries and
sRGB transfer alongside its identity matrix and full range.

The model and maintained benchmark select this profile consistently. Model
preflight recognizes declared identity color; selected decoding retains metadata
validation. Subsampled/limited-range identity input, incompatible reference modes,
HDR identity and unsupported RGB primaries remain explicit errors. This does not
qualify arbitrary wide-gamut or HDR RGB rendering.

Six AV1 and six VP9 clips cover odd/even geometry and 8/10/12-bit identity RGB.
All 144 native decoded pictures match independent FFmpeg planar hashes, including
color metadata. The independent 16-case RGB oracle spans four geometries and
8/10/12/14-bit codes, including every code value in the largest geometry.
Scalar/SIMD/Metal preparation matches the independent RGB raster through padded
strides, crop and rotation. Ordinary RGB and invalid-policy paths remain covered
by the broader preparation tests. All 12 coded clips and the corpus regenerate
byte-exactly; the independent pixel fixture and its provenance do as well.

The old native server rejects valid identity input with `UnsupportedVideoColor`.
Fresh native CPU, Metal and actual Linux ARM64 runs now preserve all 12 video
vectors, both queries, exact rankings and full top-1/5/10 overlap against pinned
PyTorch CPU. Each also passes observed reset cleanup/retry, legal half-close and
12 serial sustained requests. Linux uses the macOS PyTorch reference; this is
not fresh Linux PyTorch qualification. macOS passes 253 video checks (two skips)
and 52 model checks; actual Linux passes 226 video checks (29 platform skips).

These generated clips establish codec and reference-preservation behavior, not
application relevance or arbitrary real-world color conformance. The separate
encoded-pixel arithmetic prototype is a development diagnostic; production
claims rely on the native decoder, preparation and pretrained gates. Timings are
shared-host diagnostics, without a production throughput claim. Broader display,
codec/fuzz/load/performance and the remaining non-CUDA scope stay active.

Reproduce clips with `zig/pkg/inference/scripts/embeddinggemma2/generate_gbr_model_corpus.py --aomenc
/path/to/pinned/aomenc --output zig/pkg/inference/scripts/embeddinggemma2/testdata/gbr-formats`.
AOM's existing 12-bit non-4:2:0 CLI initialization workaround uses profile zero
with explicit subsampling, letting it promote to Professional; FFprobe verifies
the actual output. The source/tool identities and normalized commands are in
`formats.json`. Reproduce RGB pixels with
`zig/lib/video/scripts/generate_rgb_gbr_fixture.py --output /tmp/gbr.bin`.
Run `zig/pkg/inference/scripts/embeddinggemma2/video_compare.py` with the corpus, pinned checkpoint,
CPU/native/Metal backends, `--warmups 0 --iterations 1 --threads 4 --concurrency 1
--sustained-rounds 1`, and `--process-memory-budget-mb 4096` for owned servers.
Use this `pytorch-cpu.json` as `--reference`. Linux runs static aarch64-linux-musl
binaries inside the pinned ARM64 Python image, network disabled and source/model
read-only. No checkpoint or executable is distributed. `qualification.json`
binds source, fixtures, reports and binaries.
