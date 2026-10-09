# Codec, container and SIMD expansion qualification

Recorded 2026-10-08 with Zig 0.17.0. This is a bounded profile/tool qualification;
no runtime depends on FFmpeg, x264 or JM, and no claim of full H.264 conformance
or comprehensive fuzz/security coverage is made.

## Independent sample evidence

- The original 66-picture 854×480 Sintel stream-copy excerpt matches every FFmpeg
  native sample hash and media PTS in [its differential receipt](h264-sintel-differential-receipt.json).
  Normal portable regressions check four pictures through the dependency span;
  the external qualification harness checks every picture. Source edit-clock PTS
  is independently asserted against the native packet record. FFmpeg movie edits
  are disabled only for the differential media clock, avoiding one-tick edit rounding.
- Four real-content 320×192 Baseline/Main/High10/High444 encodings match all 24
  pictures each. [The corpus receipt](h264-real-corpus-receipt.json) pins exact
  encoder commands, content hashes, native pixel formats and oracle version.
- Nine official JM 19.0 decoder cases each match four native frames: compressed
  monochrome at 8/10 bits, separate colour planes at 8/10/14 bits, data partitions,
  primary SP, switching SP and SI. Checked-in per-case receipts pin MP4 and elementary
  stream bytes. Batch duplicate/unordered slots and three-pass separate-plane receipts
  are tested. Exhaustive allocation failures cover partition and separate-plane output.
- Generated dynamic avc3 cases qualify IDR changes in geometry/depth/chroma and
  predicted-picture PPS changes, registry limits and allocation cleanup. Six monochrome
  PCM vectors and four declared-gap POC1/2 vectors match independent samples/FFmpeg.
  Unit tests qualify gap wrap, DPB eviction and unavailable-reference rejection.
- The new partition registry tests reject truncated prefixes/payloads, duplicate
  partitions, orphan partitions and repeated consumption. Dynamic transitions,
  inferred samples and incomplete fields fail before partial picture publication.

Asset provenance and CC BY 3.0 attribution are in [README](README.md).
JM is an offline independently decoded sample oracle; encoder reconstruction is
not used, particularly for SI. The normative reference is
[ITU-T H.264 (08/2021)](https://www.itu.int/rec/T-REC-H.264-202108-I/en).

## Fuzzing and measured performance

The [mutation receipt](h264-mutation-receipt.jsonl) records 2,000 deterministic
bounded container/packet mutations of the original excerpt. The
[fuzz receipt](h264-fuzz-receipt.json) records fresh-cache Zig coverage-guided
campaigns: reconstruction 2,265 runs / 25.91% instrumented edges, and seeded
parameter parsing 2,259 runs / 7.65%. No failure occurred. Parser and decoder
limits bound memory, source reads, pixels, dependencies and cancellation checks.
Coverage counts refer to each instrumented executable, not a percentage of H.264
syntax combinations. Future corpus/fuzz runs should include more independent
cameras, encoders, resolutions and mixed-tool transitions.

[Raw benchmark receipts](benchmarks/README.md) include full 52.2-second original
Sintel selection/preparation at 384×384 with completed GPU/CPU timing, cold/warm
samples, allocator high-water usage and process RSS. Two warm samples are descriptive,
not a capacity sizing result. Measured portable Zig vectors improve LLVM native
plane packing; the slower vector transform candidate stays disabled. Tests cover
unaligned output, tails, signed wide-coefficient fallback and native endianness.

Metal integer-plane tests cover 8/10/14 bits × 4:2:0/4:2:2/4:4:4 × all rotations
and limited/full range. Both staged host planes and direct producer textures match
CPU patches. Producer textures are released immediately after submission; host
buffers are overwritten before completion to verify ownership. Coefficients remain
cached, and direct texture imports report zero native host staging.

## Bounds that remain deliberate

SPS/geometry changes require IDR; in-band changes use avc3 or an explicit dynamic
Session. B/C partitions must be in the same indexed access-unit packet as A.
Separate colour planes perform independent DPB passes. The new SP/SI qualification
is progressive 8-bit 4:2:0 CAVLC; arbitrary mixed interlace/tool combinations require
additional oracle qualification. Unequal non-monochrome depths and additional
profiles remain unsupported. Native Metal import accepts right-aligned integer
textures, not arbitrary P010/CoreVideo surfaces; VideoToolbox remains 8-bit 4:2:0.

WebM Cues are validated seek hints; VP8/VP9/AV1 decoding remains absent. Sequential
sources spool under explicit byte/read limits. Live fMP4 requires caller-framed
segment boundaries and stable initialization/configuration, with monotonic DTS and
owned immutable snapshots; it does not implement HLS/DASH framing or encryption.
CUDA/NVDEC and EmbeddingGemma 2/model integration remain separate dependencies.

## Platform checks

Native macOS Debug passes 104/106 tests (two platform skips); the additional
Metal ownership check passes all four selected tests. Linux ARM64 ReleaseSafe and
WASI Debug each pass 86/106 tests (20 platform skips). Linux ARM64 and x86-64
ReleaseSafe compile video, benchmark and qualification tools. LLVM SIMD selection
passes four tests. Inference consumer/import checks and all 34 media / three audio
tests pass. Zig formatting, generator lint/format and whitespace checks pass.

Container tests
also cover consecutive unknown-size Clusters, short sequential MP4/WebM reads,
provider cancellation/invalid counts, partial and successive live fragments, replay
rejection, retained earlier leases, shared admission and allocation-failure cleanup.
